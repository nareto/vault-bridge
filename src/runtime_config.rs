use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration as StdDuration, SystemTime};

use chrono::{DateTime, Utc};
use serde::Serialize;
use tokio::sync::RwLock;
use tokio::task::JoinHandle;
use tracing::{info, warn};

use crate::authorization::AuthorizationConfig;
use crate::config::{ApiTokenConfig, AppConfig, ConfigError, McpTokenConfig};

pub const DEFAULT_CONFIG_RELOAD_INTERVAL_SECONDS: u64 = 10;

#[derive(Clone, Debug)]
pub struct AuthConfigSnapshot {
    pub api_tokens: BTreeMap<String, ApiTokenConfig>,
    pub mcp_tokens: BTreeMap<String, McpTokenConfig>,
    pub contexts: AuthorizationConfig,
    pub loaded_at: DateTime<Utc>,
}

impl AuthConfigSnapshot {
    pub fn from_config(config: &AppConfig) -> Self {
        Self::from_config_at(config, Utc::now())
    }

    pub fn from_config_at(config: &AppConfig, loaded_at: DateTime<Utc>) -> Self {
        Self {
            api_tokens: config.api_tokens.clone(),
            mcp_tokens: config.mcp_tokens.clone(),
            contexts: config.contexts.clone(),
            loaded_at,
        }
    }

    pub fn from_contexts(contexts: AuthorizationConfig) -> Self {
        Self {
            api_tokens: BTreeMap::new(),
            mcp_tokens: BTreeMap::new(),
            contexts,
            loaded_at: Utc::now(),
        }
    }
}

impl Default for AuthConfigSnapshot {
    fn default() -> Self {
        Self {
            api_tokens: BTreeMap::new(),
            mcp_tokens: BTreeMap::new(),
            contexts: AuthorizationConfig::default(),
            loaded_at: Utc::now(),
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct RuntimeAuthConfig {
    snapshot: Arc<RwLock<AuthConfigSnapshot>>,
}

impl RuntimeAuthConfig {
    pub fn new(snapshot: AuthConfigSnapshot) -> Self {
        Self {
            snapshot: Arc::new(RwLock::new(snapshot)),
        }
    }

    pub async fn snapshot(&self) -> AuthConfigSnapshot {
        self.snapshot.read().await.clone()
    }

    pub async fn set_snapshot(&self, snapshot: AuthConfigSnapshot) {
        *self.snapshot.write().await = snapshot;
    }

    pub async fn set_contexts(&self, contexts: AuthorizationConfig) {
        let mut snapshot = self.snapshot.write().await;
        snapshot.contexts = contexts;
        snapshot.loaded_at = Utc::now();
    }
}

#[derive(Clone, Debug)]
pub struct RuntimeConfigState {
    auth: RuntimeAuthConfig,
    reload: Arc<RwLock<ConfigReloadStatus>>,
}

impl RuntimeConfigState {
    pub fn new(config: &AppConfig, source_path: Option<PathBuf>) -> Self {
        let loaded_at = Utc::now();
        let auth = RuntimeAuthConfig::new(AuthConfigSnapshot::from_config_at(config, loaded_at));
        let mut reload = ConfigReloadStatus {
            generation: 1,
            last_success_at: Some(loaded_at),
            ..ConfigReloadStatus::default()
        };
        reload.path = source_path.map(display_path);
        Self {
            auth,
            reload: Arc::new(RwLock::new(reload)),
        }
    }

    pub fn for_tests(config: &AppConfig) -> Self {
        Self::new(config, None)
    }

    pub fn auth_config(&self) -> RuntimeAuthConfig {
        self.auth.clone()
    }

    pub async fn reload_status(&self) -> ConfigReloadStatus {
        self.reload.read().await.clone()
    }

    pub async fn enable_reload(&self, path: &Path, poll_interval: Option<StdDuration>) {
        let mut status = self.reload.write().await;
        status.enabled = true;
        status.path = Some(display_path(path.to_path_buf()));
        status.poll_interval_seconds = poll_interval.map(|duration| duration.as_secs());
        status.sighup_enabled = false;
    }

    pub async fn set_sighup_enabled(&self, enabled: bool) {
        self.reload.write().await.sighup_enabled = enabled;
    }

    pub async fn reload_from_path(
        &self,
        path: &Path,
        trigger: ConfigReloadTrigger,
    ) -> Result<(), ConfigError> {
        let attempt_at = Utc::now();
        {
            let mut status = self.reload.write().await;
            status.last_attempt_at = Some(attempt_at);
        }

        match AppConfig::load_from_path(path) {
            Ok(config) => {
                let loaded_at = Utc::now();
                let snapshot = AuthConfigSnapshot::from_config_at(&config, loaded_at);
                self.auth.set_snapshot(snapshot).await;
                let mut status = self.reload.write().await;
                status.generation = status.generation.saturating_add(1);
                status.last_success_at = Some(loaded_at);
                status.last_error = None;
                status.success_count = status.success_count.saturating_add(1);
                info!(
                    path = %path.display(),
                    trigger = trigger.as_str(),
                    generation = status.generation,
                    "vault_bridge config reload applied"
                );
                Ok(())
            }
            Err(error) => {
                let error_message = error.to_string();
                let mut status = self.reload.write().await;
                status.last_failure_at = Some(Utc::now());
                status.last_error = Some(error_message.clone());
                status.failure_count = status.failure_count.saturating_add(1);
                warn!(
                    path = %path.display(),
                    trigger = trigger.as_str(),
                    error = %error_message,
                    generation = status.generation,
                    "vault_bridge config reload failed; keeping previous good auth config"
                );
                Err(error)
            }
        }
    }
}

#[derive(Clone, Debug, Serialize, Default)]
pub struct ConfigReloadStatus {
    pub enabled: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    pub generation: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub poll_interval_seconds: Option<u64>,
    pub sighup_enabled: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_attempt_at: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_success_at: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_failure_at: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
    pub success_count: u64,
    pub failure_count: u64,
}

#[derive(Clone, Copy, Debug)]
pub enum ConfigReloadTrigger {
    Poll,
    Sighup,
}

impl ConfigReloadTrigger {
    fn as_str(self) -> &'static str {
        match self {
            Self::Poll => "poll",
            Self::Sighup => "sighup",
        }
    }
}

pub fn spawn_config_reload_poll_worker(
    runtime_config: RuntimeConfigState,
    path: PathBuf,
    interval: StdDuration,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut last_modified = config_modified_at(&path);
        loop {
            tokio::time::sleep(interval).await;
            let modified = config_modified_at(&path);
            if modified == last_modified {
                continue;
            }
            last_modified = modified;
            let _ = runtime_config
                .reload_from_path(&path, ConfigReloadTrigger::Poll)
                .await;
        }
    })
}

#[cfg(unix)]
pub fn spawn_config_reload_sighup_worker(
    runtime_config: RuntimeConfigState,
    path: PathBuf,
) -> std::io::Result<JoinHandle<()>> {
    let mut signal = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::hangup())?;
    Ok(tokio::spawn(async move {
        while signal.recv().await.is_some() {
            let _ = runtime_config
                .reload_from_path(&path, ConfigReloadTrigger::Sighup)
                .await;
        }
    }))
}

#[cfg(not(unix))]
pub fn spawn_config_reload_sighup_worker(
    _runtime_config: RuntimeConfigState,
    _path: PathBuf,
) -> std::io::Result<JoinHandle<()>> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "SIGHUP config reload is only supported on Unix",
    ))
}

fn config_modified_at(path: &Path) -> Option<SystemTime> {
    std::fs::metadata(path)
        .and_then(|metadata| metadata.modified())
        .ok()
}

fn display_path(path: PathBuf) -> String {
    path.display().to_string()
}

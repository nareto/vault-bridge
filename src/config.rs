use std::collections::BTreeMap;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::authorization::{AccessPolicy, AccessRule};

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct AppConfig {
    pub server: ServerConfig,
    pub couchdb: CouchDbConfig,
    pub database: DatabaseConfig,
    pub api_tokens: BTreeMap<String, ApiTokenConfig>,
    pub mcp_tokens: BTreeMap<String, McpTokenConfig>,
    pub contexts: BTreeMap<String, AccessPolicy>,
    pub indexer: IndexerConfig,
    pub embedding: EmbeddingConfig,
    pub context_assembly: ContextAssemblyConfig,
    pub new_note: NewNoteConfig,
    pub audit: AuditConfig,
}

#[derive(Debug, Clone)]
pub struct LoadedAppConfig {
    pub config: AppConfig,
    pub source_path: Option<PathBuf>,
}

impl Default for AppConfig {
    fn default() -> Self {
        Self {
            server: ServerConfig::default(),
            couchdb: CouchDbConfig::default(),
            database: DatabaseConfig::default(),
            api_tokens: BTreeMap::new(),
            mcp_tokens: BTreeMap::new(),
            contexts: BTreeMap::new(),
            indexer: IndexerConfig::default(),
            embedding: EmbeddingConfig::default(),
            context_assembly: ContextAssemblyConfig::default(),
            new_note: NewNoteConfig::default(),
            audit: AuditConfig::default(),
        }
    }
}

impl AppConfig {
    pub fn load_from_env_or_default() -> Result<Self, ConfigError> {
        Ok(Self::load_with_source_from_env_or_default()?.config)
    }

    pub fn load_with_source_from_env_or_default() -> Result<LoadedAppConfig, ConfigError> {
        if let Ok(path) = env::var("CONFIG_PATH") {
            let source_path = PathBuf::from(path);
            return Ok(LoadedAppConfig {
                config: Self::load_from_path(&source_path)?,
                source_path: Some(source_path),
            });
        }

        let default_path = PathBuf::from("config.yaml");
        let default_path_exists = default_path.exists();
        let mut cfg = if default_path_exists {
            Self::load_from_path(&default_path)?
        } else {
            Self::default()
        };
        cfg.apply_legacy_env_overrides()?;
        cfg.validate()?;
        Ok(LoadedAppConfig {
            config: cfg,
            source_path: default_path_exists.then_some(default_path),
        })
    }

    pub fn load_from_path(path: impl AsRef<Path>) -> Result<Self, ConfigError> {
        let raw = fs::read_to_string(path.as_ref())?;
        let expanded = expand_env_placeholders(&raw)?;
        let mut cfg = serde_yaml::from_str::<Self>(&expanded)?;
        cfg.apply_legacy_env_overrides()?;
        cfg.validate()?;
        Ok(cfg)
    }

    pub fn server_addr(&self) -> String {
        format!("{}:{}", self.server.host, self.server.port)
    }

    fn validate(&self) -> Result<(), ConfigError> {
        validate_context_tokens("API token", &self.api_tokens)?;
        validate_context_tokens("MCP token", &self.mcp_tokens)?;
        self.embedding.validate()?;
        self.validate_contexts()?;
        Ok(())
    }

    fn validate_contexts(&self) -> Result<(), ConfigError> {
        if self.contexts.is_empty() {
            return Err(ConfigError::MissingContexts);
        }
        validate_token_context_references(&self.contexts, self.api_tokens.values())?;
        validate_token_context_references(&self.contexts, self.mcp_tokens.values())?;
        for (name, policy) in &self.contexts {
            validate_policy(name, policy)?;
        }
        Ok(())
    }

    fn apply_legacy_env_overrides(&mut self) -> Result<(), ConfigError> {
        if let Ok(host) = env::var("VAULT_BRIDGE_HOST")
            && !host.trim().is_empty()
        {
            self.server.host = host;
        }

        if let Ok(port) = env::var("VAULT_BRIDGE_PORT") {
            self.server.port = port
                .parse::<u16>()
                .map_err(|_| ConfigError::InvalidPort(port))?;
        }

        if let Ok(log_level) = env::var("RUST_LOG")
            && !log_level.trim().is_empty()
        {
            self.server.log_level = log_level;
        }

        Ok(())
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct ServerConfig {
    pub host: String,
    pub port: u16,
    pub log_level: String,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            host: "0.0.0.0".to_string(),
            port: 8080,
            log_level: "info".to_string(),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct CouchDbConfig {
    pub url: String,
    pub database: String,
    pub username: String,
    pub password: String,
    pub poll_interval_seconds: u64,
    pub feed_mode: FeedMode,
    pub encryption: EncryptionConfig,
    pub longpoll_timeout_grace_seconds: u64,
}

impl Default for CouchDbConfig {
    fn default() -> Self {
        Self {
            url: String::new(),
            database: String::new(),
            username: String::new(),
            password: String::new(),
            poll_interval_seconds: 5,
            feed_mode: FeedMode::Longpoll,
            encryption: EncryptionConfig::default(),
            longpoll_timeout_grace_seconds: 10,
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct EncryptionConfig {
    pub passphrase: String,
}

impl Default for EncryptionConfig {
    fn default() -> Self {
        Self {
            passphrase: String::new(),
        }
    }
}

impl EncryptionConfig {
    pub fn is_enabled(&self) -> bool {
        !self.passphrase.is_empty()
    }
}

impl CouchDbConfig {
    pub fn is_configured(&self) -> bool {
        !self.url.trim().is_empty() && !self.database.trim().is_empty()
    }

    pub fn poll_interval(&self) -> Duration {
        Duration::from_secs(self.poll_interval_seconds.max(1))
    }
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq, Default)]
#[serde(rename_all = "lowercase")]
pub enum FeedMode {
    #[default]
    Longpoll,
    Continuous,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct DatabaseConfig {
    pub url: String,
    pub max_connections: u32,
}

impl Default for DatabaseConfig {
    fn default() -> Self {
        Self {
            url: "postgres://vault_bridge:${PG_PASS}@localhost:5432/vault_bridge".to_string(),
            max_connections: 10,
        }
    }
}

impl DatabaseConfig {
    pub fn is_configured(&self) -> bool {
        let url = self.url.trim();
        !url.is_empty() && !url.contains("${")
    }

    pub fn host_for_diagnostics(&self) -> Option<String> {
        parse_database_host(&self.url)
    }

    pub fn points_to_localhost(&self) -> bool {
        matches!(
            self.host_for_diagnostics().as_deref(),
            Some("localhost" | "127.0.0.1" | "::1")
        )
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct ApiTokenConfig {
    pub context: String,
}

impl Default for ApiTokenConfig {
    fn default() -> Self {
        Self {
            context: String::new(),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct McpTokenConfig {
    pub context: String,
}

impl Default for McpTokenConfig {
    fn default() -> Self {
        Self {
            context: String::new(),
        }
    }
}

impl ApiTokenConfig {
    pub fn context(&self) -> &str {
        self.context.trim()
    }
}

impl McpTokenConfig {
    pub fn context(&self) -> &str {
        self.context.trim()
    }
}

fn parse_database_host(database_url: &str) -> Option<String> {
    let trimmed = database_url.trim();
    let (_, rest) = trimmed.split_once("://")?;
    let host_and_path = rest.rsplit_once('@').map(|(_, rhs)| rhs).unwrap_or(rest);
    let host = if let Some(stripped) = host_and_path.strip_prefix('[') {
        stripped.split_once(']')?.0
    } else {
        host_and_path
            .split([':', '/', '?'])
            .next()
            .unwrap_or_default()
    };
    let host = host.trim();
    if host.is_empty() {
        None
    } else {
        Some(host.to_string())
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct IndexerConfig {
    pub debounce_seconds: u64,
    pub max_changes_per_batch: usize,
    pub max_link_context_chars: usize,
    pub hub_note_threshold: usize,
    pub hub_note_fanout: usize,
    pub hub_note_folders: Vec<String>,
    pub chunk_staging_timeout_seconds: u64,
    pub recovery_batch_size: usize,
    pub recovery_max_failures: usize,
    pub recovery_base_backoff_seconds: u64,
    pub recovery_max_backoff_seconds: u64,
}

impl Default for IndexerConfig {
    fn default() -> Self {
        Self {
            debounce_seconds: 5,
            max_changes_per_batch: 128,
            max_link_context_chars: 250,
            hub_note_threshold: 20,
            hub_note_fanout: 6,
            hub_note_folders: vec!["MOC/".to_string(), "99MOC/".to_string()],
            chunk_staging_timeout_seconds: 60,
            recovery_batch_size: 4,
            recovery_max_failures: 5,
            recovery_base_backoff_seconds: 30,
            recovery_max_backoff_seconds: 3_600,
        }
    }
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq, Default)]
#[serde(rename_all = "lowercase")]
pub enum EmbeddingMode {
    Disabled,
    #[default]
    Local,
    Localai,
}

impl EmbeddingMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Disabled => "disabled",
            Self::Local => "local",
            Self::Localai => "localai",
        }
    }

    fn from_legacy_provider(provider: &str, localai_url: &str) -> Self {
        match provider.trim().to_ascii_lowercase().as_str() {
            "disabled" | "none" | "off" => Self::Disabled,
            "localai" if !localai_url.trim().is_empty() => Self::Localai,
            _ => Self::Local,
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct LocalAiEmbeddingConfig {
    pub url: String,
    pub model: String,
    pub request_dimensions: bool,
}

impl Default for LocalAiEmbeddingConfig {
    fn default() -> Self {
        Self {
            url: String::new(),
            model: "nomic-embed-text".to_string(),
            request_dimensions: false,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct EmbeddingConfig {
    pub mode: EmbeddingMode,
    pub localai: LocalAiEmbeddingConfig,
    pub dimensions: usize,
    pub hnsw_m: usize,
    pub hnsw_ef_construction: usize,
    pub batch_size: usize,
    pub poll_interval_seconds: u64,
    pub timeout_seconds: u64,
    pub note_chunk_bytes: usize,
    pub block_min_chars: usize,
    pub block_chunk_bytes: usize,
    pub block_chunk_overlap_sentences: usize,
    pub block_embedding_enabled: bool,
    pub max_embedding_failures: usize,
}

impl Default for EmbeddingConfig {
    fn default() -> Self {
        Self {
            mode: EmbeddingMode::Local,
            localai: LocalAiEmbeddingConfig::default(),
            dimensions: 768,
            hnsw_m: 16,
            hnsw_ef_construction: 64,
            batch_size: 32,
            poll_interval_seconds: 5,
            timeout_seconds: 30,
            note_chunk_bytes: 800,
            block_min_chars: 200,
            block_chunk_bytes: 800,
            block_chunk_overlap_sentences: 1,
            block_embedding_enabled: true,
            max_embedding_failures: 3,
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(default)]
struct EmbeddingConfigWire {
    mode: Option<EmbeddingMode>,
    localai: LocalAiEmbeddingConfig,
    provider: Option<String>,
    url: Option<String>,
    model: Option<String>,
    dimensions: usize,
    hnsw_m: usize,
    hnsw_ef_construction: usize,
    batch_size: usize,
    poll_interval_seconds: u64,
    timeout_seconds: u64,
    note_chunk_bytes: usize,
    block_min_chars: usize,
    block_chunk_bytes: usize,
    block_chunk_overlap_sentences: usize,
    block_embedding_enabled: bool,
    max_embedding_failures: usize,
}

impl Default for EmbeddingConfigWire {
    fn default() -> Self {
        let defaults = EmbeddingConfig::default();
        Self {
            mode: None,
            localai: defaults.localai,
            provider: None,
            url: None,
            model: None,
            dimensions: defaults.dimensions,
            hnsw_m: defaults.hnsw_m,
            hnsw_ef_construction: defaults.hnsw_ef_construction,
            batch_size: defaults.batch_size,
            poll_interval_seconds: defaults.poll_interval_seconds,
            timeout_seconds: defaults.timeout_seconds,
            note_chunk_bytes: defaults.note_chunk_bytes,
            block_min_chars: defaults.block_min_chars,
            block_chunk_bytes: defaults.block_chunk_bytes,
            block_chunk_overlap_sentences: defaults.block_chunk_overlap_sentences,
            block_embedding_enabled: defaults.block_embedding_enabled,
            max_embedding_failures: defaults.max_embedding_failures,
        }
    }
}

impl<'de> Deserialize<'de> for EmbeddingConfig {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let wire = EmbeddingConfigWire::deserialize(deserializer)?;
        let mut localai = wire.localai;
        if let Some(url) = wire.url {
            localai.url = url;
        }
        if let Some(model) = wire.model {
            localai.model = model;
        }

        let mode = wire.mode.unwrap_or_else(|| {
            wire.provider
                .as_deref()
                .map(|provider| EmbeddingMode::from_legacy_provider(provider, &localai.url))
                .unwrap_or_default()
        });

        Ok(Self {
            mode,
            localai,
            dimensions: wire.dimensions,
            hnsw_m: wire.hnsw_m,
            hnsw_ef_construction: wire.hnsw_ef_construction,
            batch_size: wire.batch_size,
            poll_interval_seconds: wire.poll_interval_seconds,
            timeout_seconds: wire.timeout_seconds,
            note_chunk_bytes: wire.note_chunk_bytes,
            block_min_chars: wire.block_min_chars,
            block_chunk_bytes: wire.block_chunk_bytes,
            block_chunk_overlap_sentences: wire.block_chunk_overlap_sentences,
            block_embedding_enabled: wire.block_embedding_enabled,
            max_embedding_failures: wire.max_embedding_failures,
        })
    }
}

impl EmbeddingConfig {
    pub fn poll_interval(&self) -> Duration {
        Duration::from_secs(self.poll_interval_seconds.max(1))
    }

    pub fn timeout(&self) -> Duration {
        Duration::from_secs(self.timeout_seconds.max(1))
    }

    pub fn note_chunk_bytes(&self) -> usize {
        self.note_chunk_bytes.max(32)
    }

    pub fn block_chunk_bytes(&self) -> usize {
        self.block_chunk_bytes.max(32)
    }

    pub fn localai_url(&self) -> &str {
        self.localai.url.as_str()
    }

    pub fn localai_model(&self) -> &str {
        self.localai.model.as_str()
    }

    pub fn schema_model(&self) -> &str {
        match self.mode {
            EmbeddingMode::Disabled => "disabled",
            EmbeddingMode::Local => "builtin-token-hash",
            EmbeddingMode::Localai => self.localai_model(),
        }
    }

    fn validate(&self) -> Result<(), ConfigError> {
        if self.mode == EmbeddingMode::Localai && self.localai.url.trim().is_empty() {
            return Err(ConfigError::MissingLocalAiUrl);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct ContextAssemblyConfig {
    pub default_max_tokens: usize,
    pub max_max_tokens: usize,
    pub default_max_depth: usize,
}

impl Default for ContextAssemblyConfig {
    fn default() -> Self {
        Self {
            default_max_tokens: 8_000,
            max_max_tokens: 32_000,
            default_max_depth: 2,
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct NewNoteConfig {
    pub base_path: String,
    pub path_template: String,
    pub date_format: String,
    pub max_title_slug_length: usize,
}

impl Default for NewNoteConfig {
    fn default() -> Self {
        Self {
            base_path: "11New".to_string(),
            path_template: "{base}/{date}-{slug}.md".to_string(),
            date_format: "%Y-%m-%d".to_string(),
            max_title_slug_length: 60,
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct AuditConfig {
    pub enabled: bool,
    pub retention_days: u64,
}

impl Default for AuditConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            retention_days: 90,
        }
    }
}

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("failed to read config: {0}")]
    Io(#[from] std::io::Error),
    #[error("failed to parse config yaml: {0}")]
    Yaml(#[from] serde_yaml::Error),
    #[error("missing environment variable '{var}' used in config")]
    MissingEnvVar { var: String },
    #[error("VAULT_BRIDGE_PORT is invalid: '{0}'")]
    InvalidPort(String),
    #[error("{kind} names must not be empty")]
    EmptyTokenName { kind: &'static str },
    #[error("{kind} '{name}' must declare a context")]
    EmptyTokenContext { kind: &'static str, name: String },
    #[error("embedding.localai.url must be set when embedding.mode is localai")]
    MissingLocalAiUrl,
    #[error("at least one context must be configured")]
    MissingContexts,
    #[error("token references unknown context '{0}'")]
    UnknownContext(String),
    #[error("context '{context}' has an invalid {operation} rule at index {index}: {reason}")]
    InvalidContextRule {
        context: String,
        operation: &'static str,
        index: usize,
        reason: String,
    },
}

pub fn expand_env_placeholders(raw: &str) -> Result<String, ConfigError> {
    let mut out = String::with_capacity(raw.len());
    let mut cursor = 0usize;

    while let Some(start_rel) = raw[cursor..].find("${") {
        let start = cursor + start_rel;
        out.push_str(&raw[cursor..start]);

        let var_start = start + 2;
        let Some(end_rel) = raw[var_start..].find('}') else {
            out.push_str(&raw[start..]);
            cursor = raw.len();
            break;
        };

        let end = var_start + end_rel;
        let spec = raw[var_start..end].trim();
        if spec.is_empty() {
            out.push_str("${}");
            cursor = end + 1;
            continue;
        }

        let (var, default) = split_env_placeholder_spec(spec);
        let value = match env::var(var) {
            Ok(value) => {
                if value.is_empty() {
                    default.unwrap_or("").to_string()
                } else {
                    value
                }
            }
            Err(_) => {
                if let Some(default) = default {
                    default.to_string()
                } else {
                    return Err(ConfigError::MissingEnvVar {
                        var: var.to_string(),
                    });
                }
            }
        };
        out.push_str(&value);
        cursor = end + 1;
    }

    if cursor < raw.len() {
        out.push_str(&raw[cursor..]);
    }
    Ok(out)
}

fn split_env_placeholder_spec(spec: &str) -> (&str, Option<&str>) {
    if let Some((var, default)) = spec.split_once(":-") {
        (var.trim(), Some(default))
    } else {
        (spec, None)
    }
}

trait ContextToken {
    fn context(&self) -> &str;
}

impl ContextToken for ApiTokenConfig {
    fn context(&self) -> &str {
        self.context()
    }
}

impl ContextToken for McpTokenConfig {
    fn context(&self) -> &str {
        self.context()
    }
}

fn validate_context_tokens<T>(
    kind: &'static str,
    tokens: &BTreeMap<String, T>,
) -> Result<(), ConfigError>
where
    T: ContextToken,
{
    for (name, token) in tokens {
        if name.trim().is_empty() {
            return Err(ConfigError::EmptyTokenName { kind });
        }
        if token.context().is_empty() {
            return Err(ConfigError::EmptyTokenContext {
                kind,
                name: name.clone(),
            });
        }
    }
    Ok(())
}

fn validate_token_context_references<'a, T>(
    contexts: &BTreeMap<String, AccessPolicy>,
    tokens: impl IntoIterator<Item = &'a T>,
) -> Result<(), ConfigError>
where
    T: ContextToken + 'a,
{
    for token in tokens {
        if !contexts.contains_key(token.context()) {
            return Err(ConfigError::UnknownContext(token.context().to_string()));
        }
    }
    Ok(())
}

fn validate_policy(context: &str, policy: &AccessPolicy) -> Result<(), ConfigError> {
    validate_rules(context, "read", &policy.read)?;
    validate_rules(context, "create", &policy.create)?;
    validate_rules(context, "edit", &policy.edit)?;
    Ok(())
}

fn validate_rules(
    context: &str,
    operation: &'static str,
    rules: &[AccessRule],
) -> Result<(), ConfigError> {
    for (index, rule) in rules.iter().enumerate() {
        if rule.is_allow() == rule.is_deny() {
            return Err(ConfigError::InvalidContextRule {
                context: context.to_string(),
                operation,
                index,
                reason: "rule must contain exactly one of allow or deny".to_string(),
            });
        }
        if rule.is_deny() && rule.has_mutations() {
            return Err(ConfigError::InvalidContextRule {
                context: context.to_string(),
                operation,
                index,
                reason: "deny rules cannot declare mutation fields".to_string(),
            });
        }
        if let Some(matcher) = rule.matcher()
            && let Some(pattern) = matcher.title_regex.as_deref()
            && let Err(error) = regex::Regex::new(pattern)
        {
            return Err(ConfigError::InvalidContextRule {
                context: context.to_string(),
                operation,
                index,
                reason: format!("invalid title_regex: {error}"),
            });
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use tempfile::NamedTempFile;

    use super::{AppConfig, ConfigError, EmbeddingMode, expand_env_placeholders};

    fn run_config_test<T>(test: impl FnOnce() -> T) -> T {
        test()
    }

    #[test]
    fn expands_env_placeholders() {
        let key = "VAULT_BRIDGE_TEST_ENV";
        // SAFETY: unit test runs single-threaded in process context and cleans up.
        unsafe {
            std::env::set_var(key, "expanded-value");
        }
        let expanded = expand_env_placeholders("value: ${VAULT_BRIDGE_TEST_ENV}")
            .expect("placeholder should expand");
        assert_eq!(expanded, "value: expanded-value");
        // SAFETY: see above.
        unsafe {
            std::env::remove_var(key);
        }
    }

    #[test]
    fn missing_env_placeholder_is_error() {
        let error = expand_env_placeholders("value: ${VAULT_BRIDGE_MISSING_ENV}")
            .expect_err("missing variable should error");
        assert!(matches!(error, ConfigError::MissingEnvVar { .. }));
    }

    #[test]
    fn supports_env_placeholder_default_for_missing_value() {
        let expanded = expand_env_placeholders("value: ${VAULT_BRIDGE_OPTIONAL_ENV:-fallback}")
            .expect("missing optional variable should use fallback");
        assert_eq!(expanded, "value: fallback");
    }

    #[test]
    fn supports_env_placeholder_default_for_empty_value() {
        let key = "VAULT_BRIDGE_EMPTY_OPTIONAL_ENV";
        // SAFETY: unit test runs single-threaded in process context and cleans up.
        unsafe {
            std::env::set_var(key, "");
        }
        let expanded =
            expand_env_placeholders("value: ${VAULT_BRIDGE_EMPTY_OPTIONAL_ENV:-fallback}")
                .expect("empty variable should use fallback");
        assert_eq!(expanded, "value: fallback");
        // SAFETY: see above.
        unsafe {
            std::env::remove_var(key);
        }
    }

    #[test]
    fn loads_config_and_preserves_defaults_for_missing_sections() {
        run_config_test(|| {
            let mut file = NamedTempFile::new().expect("temp file");
            writeln!(
                file,
                "server:\n  host: 127.0.0.1\ncouchdb:\n  url: https://example.test\n  database: mainvault\ncontexts:\n  smoke:\n    read: []\n    create: []\n    edit: []\n"
            )
            .expect("write config");

            let cfg = AppConfig::load_from_path(file.path()).expect("load config");
            assert_eq!(cfg.server.host, "127.0.0.1");
            assert_eq!(cfg.server.port, 8080);
            assert_eq!(cfg.couchdb.database, "mainvault");
            assert_eq!(cfg.indexer.max_changes_per_batch, 128);
            assert_eq!(cfg.indexer.max_link_context_chars, 250);
            assert_eq!(cfg.indexer.hub_note_fanout, 6);
            assert_eq!(
                cfg.indexer.hub_note_folders,
                vec!["MOC/".to_string(), "99MOC/".to_string()]
            );
            assert!(cfg.api_tokens.is_empty());
            assert!(cfg.mcp_tokens.is_empty());
            assert!(cfg.contexts.contains_key("smoke"));
            assert_eq!(cfg.embedding.hnsw_m, 16);
            assert_eq!(cfg.embedding.hnsw_ef_construction, 64);
            assert_eq!(cfg.embedding.note_chunk_bytes, 800);
            assert_eq!(cfg.embedding.mode, EmbeddingMode::Local);
        });
    }

    #[test]
    fn loads_nested_localai_embedding_config() {
        run_config_test(|| {
            let mut file = NamedTempFile::new().expect("temp file");
            writeln!(
                file,
                "embedding:\n  mode: localai\n  localai:\n    url: http://localai.test/v1/embeddings\n    model: custom-embed\n  dimensions: 384\ncontexts:\n  smoke:\n    read: []\n    create: []\n    edit: []\n"
            )
            .expect("write config");

            let cfg = AppConfig::load_from_path(file.path()).expect("load config");
            assert_eq!(cfg.embedding.mode, EmbeddingMode::Localai);
            assert_eq!(
                cfg.embedding.localai.url,
                "http://localai.test/v1/embeddings"
            );
            assert_eq!(cfg.embedding.localai.model, "custom-embed");
            assert_eq!(cfg.embedding.schema_model(), "custom-embed");
            assert_eq!(cfg.embedding.dimensions, 384);
        });
    }

    #[test]
    fn rejects_localai_mode_without_url() {
        run_config_test(|| {
            let mut file = NamedTempFile::new().expect("temp file");
            writeln!(file, "embedding:\n  mode: localai\n").expect("write config");

            let error = AppConfig::load_from_path(file.path()).expect_err("load should fail");
            assert!(matches!(error, ConfigError::MissingLocalAiUrl));
        });
    }

    #[test]
    fn accepts_legacy_flat_localai_embedding_config() {
        run_config_test(|| {
            let mut file = NamedTempFile::new().expect("temp file");
            writeln!(
                file,
                "embedding:\n  provider: localai\n  url: http://localai.test/v1/embeddings\n  model: legacy-embed\ncontexts:\n  smoke:\n    read: []\n    create: []\n    edit: []\n"
            )
            .expect("write config");

            let cfg = AppConfig::load_from_path(file.path()).expect("load config");
            assert_eq!(cfg.embedding.mode, EmbeddingMode::Localai);
            assert_eq!(
                cfg.embedding.localai.url,
                "http://localai.test/v1/embeddings"
            );
            assert_eq!(cfg.embedding.localai.model, "legacy-embed");
        });
    }

    #[test]
    fn loads_non_default_hub_and_hnsw_settings() {
        run_config_test(|| {
            let mut file = NamedTempFile::new().expect("temp file");
            writeln!(
                file,
                "indexer:\n  max_changes_per_batch: 64\n  hub_note_fanout: 9\n  hub_note_folders:\n    - Guides/\n    - Maps/\nembedding:\n  hnsw_m: 32\n  hnsw_ef_construction: 120\ncontexts:\n  smoke:\n    read: []\n    create: []\n    edit: []\n"
            )
            .expect("write config");

            let cfg = AppConfig::load_from_path(file.path()).expect("load config");
            assert_eq!(cfg.indexer.max_changes_per_batch, 64);
            assert_eq!(cfg.indexer.hub_note_fanout, 9);
            assert_eq!(
                cfg.indexer.hub_note_folders,
                vec!["Guides/".to_string(), "Maps/".to_string()]
            );
            assert_eq!(cfg.embedding.hnsw_m, 32);
            assert_eq!(cfg.embedding.hnsw_ef_construction, 120);
        });
    }

    #[test]
    fn loads_api_tokens_from_config() {
        run_config_test(|| {
            let mut file = NamedTempFile::new().expect("temp file");
            writeln!(
                file,
                "api_tokens:\n  monitoring:\n    context: non_personal\ncontexts:\n  non_personal:\n    read: []\n    create: []\n    edit: []\n"
            )
            .expect("write config");

            let cfg = AppConfig::load_from_path(file.path()).expect("load config");
            assert_eq!(cfg.api_tokens["monitoring"].context, "non_personal");
            assert_eq!(cfg.api_tokens.len(), 1);
        });
    }

    #[test]
    fn loads_contexts_and_mcp_token_contexts_from_config() {
        run_config_test(|| {
            let mut file = NamedTempFile::new().expect("temp file");
            writeln!(
                file,
                r#"
mcp_tokens:
  claude-work:
    context: work
contexts:
  work:
    read:
      - allow:
          path_prefix: Work/
          tags_any: [work]
    create: []
    edit: []
"#
            )
            .expect("write config");

            let cfg = AppConfig::load_from_path(file.path()).expect("load config");
            assert_eq!(cfg.mcp_tokens["claude-work"].context, "work");
            assert_eq!(cfg.contexts["work"].read.len(), 1);
            assert!(cfg.contexts["work"].read[0].is_allow());
        });
    }

    #[test]
    fn rejects_unknown_context_reference() {
        run_config_test(|| {
            let mut file = NamedTempFile::new().expect("temp file");
            writeln!(
                file,
                "api_tokens:\n  client:\n    context: missing-context\ncontexts:\n  known:\n    read: []\n    create: []\n    edit: []\n"
            )
            .expect("write config");

            let error = AppConfig::load_from_path(file.path())
                .expect_err("unknown context should fail validation");
            assert!(
                matches!(error, ConfigError::UnknownContext(context) if context == "missing-context")
            );
        });
    }

    #[test]
    fn rejects_empty_api_token_context() {
        run_config_test(|| {
            let mut file = NamedTempFile::new().expect("temp file");
            writeln!(file, "api_tokens:\n  client:\n    context: \"\"\ncontexts:\n  known:\n    read: []\n    create: []\n    edit: []\n").expect("write config");

            let error =
                AppConfig::load_from_path(file.path()).expect_err("empty context should fail");
            assert!(matches!(
                error,
                ConfigError::EmptyTokenContext { kind: "API token", name } if name == "client"
            ));
        });
    }

    #[test]
    fn parses_database_host_for_diagnostics() {
        let cfg = super::DatabaseConfig {
            url: "postgres://postgres:5432/vault_bridge?sslmode=disable".to_string(),
            max_connections: 10,
        };
        assert_eq!(cfg.host_for_diagnostics().as_deref(), Some("postgres"));
        assert!(!cfg.points_to_localhost());
    }

    #[test]
    fn detects_localhost_database_urls() {
        let cfg = super::DatabaseConfig {
            url: "postgres://localhost:5432/vault_bridge".to_string(),
            max_connections: 10,
        };
        assert_eq!(cfg.host_for_diagnostics().as_deref(), Some("localhost"));
        assert!(cfg.points_to_localhost());
    }
}

use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration as StdDuration;

use anyhow::Context;
use serde_json::Value;
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;

use vault_bridge::api::{ApiTokenState, AppState, serve};
use vault_bridge::config::AppConfig;
use vault_bridge::encryption::Decryptor;
use vault_bridge::livesync::LivesyncDocument;
use vault_bridge::markdown::{breadcrumb_prefix, split_into_semantic_blocks};
use vault_bridge::new_note::NewNotePathSettings;
use vault_bridge::persistence::{EmbeddingUnblockSelector, PostgresPersistence};
use vault_bridge::runtime_config::{
    DEFAULT_CONFIG_RELOAD_INTERVAL_SECONDS, RuntimeConfigState, spawn_config_reload_poll_worker,
    spawn_config_reload_sighup_worker,
};
use vault_bridge::service::VaultBridgeService;
use vault_bridge::workers::{spawn_embedding_worker, spawn_sync_worker};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RunMode {
    All,
    Api,
    Workers,
}

impl RunMode {
    fn as_str(self) -> &'static str {
        match self {
            Self::All => "all",
            Self::Api => "api",
            Self::Workers => "workers",
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let loaded_config =
        AppConfig::load_with_source_from_env_or_default().context("failed to load config")?;
    let config = loaded_config.config;
    let runtime_config = RuntimeConfigState::new(&config, loaded_config.source_path.clone());
    let args = std::env::args().skip(1).collect::<Vec<_>>();

    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new(config.server.log_level.clone())),
        )
        .compact()
        .init();

    // --reindex-blocks: one-off semantic chunk rebuild mode
    if args.iter().any(|arg| arg == "--reindex-blocks") {
        let only_missing = args.iter().any(|arg| arg == "--only-missing");
        return reindex_blocks(&config, only_missing).await;
    }
    if args.iter().any(|arg| arg == "--embedding-unblock") {
        return unblock_embeddings(&config, &args).await;
    }
    if let Some(note_path) = flag_value(&args, "--debug-note-scan") {
        let include_raw_docs = args.iter().any(|arg| arg == "--include-raw-docs");
        return debug_note_scan(&config, &note_path, include_raw_docs).await;
    }
    if let Some(note_path) = flag_value(&args, "--delete-note-scan") {
        let keep_leaves = args.iter().any(|arg| arg == "--keep-leaves");
        return delete_note_scan(&config, &note_path, !keep_leaves).await;
    }
    let run_mode = parse_run_mode(&args)?;

    let persistence = if config.database.is_configured() {
        let database_host = config
            .database
            .host_for_diagnostics()
            .unwrap_or_else(|| "<unknown>".to_string());
        if Path::new("/.dockerenv").exists() && config.database.points_to_localhost() {
            anyhow::bail!(
                "database.url points to localhost ({database_host}) inside container. For Docker Compose set host to `postgres` in config.yaml"
            );
        }
        let persistence = Arc::new(
            PostgresPersistence::connect_and_migrate(&config.database, config.embedding.dimensions)
                .await
                .with_context(|| {
                    format!(
                        "failed to initialize postgres persistence (database host: {database_host})"
                    )
                })?,
        );
        let sync = persistence
            .ensure_embedding_schema(
                config.embedding.schema_model(),
                config.embedding.dimensions,
                config.embedding.hnsw_m,
                config.embedding.hnsw_ef_construction,
            )
            .await
            .context("failed to align embedding schema with runtime config")?;
        if sync.reset_embeddings {
            info!(
                previous_dimensions = ?sync.previous_dimensions,
                previous_model = ?sync.previous_model,
                target_dimensions = sync.target_dimensions,
                hnsw_m = sync.target_hnsw_m,
                hnsw_ef_construction = sync.target_hnsw_ef_construction,
                cleared_embeddings = sync.cleared_embeddings,
                "embedding schema updated; cleared existing embeddings for Worker B re-index"
            );
        } else if sync.rebuilt_embedding_index {
            info!(
                previous_hnsw_m = ?sync.previous_hnsw_m,
                previous_hnsw_ef_construction = ?sync.previous_hnsw_ef_construction,
                target_hnsw_m = sync.target_hnsw_m,
                target_hnsw_ef_construction = sync.target_hnsw_ef_construction,
                "embedding index rebuilt with updated HNSW settings"
            );
        }
        Some(persistence)
    } else {
        None
    };

    let store = if let Some(persistence) = persistence.clone() {
        let store = vault_bridge::store::VaultStore::new_with_persistence_and_auth_config(
            config.indexer.hub_note_threshold,
            persistence,
            runtime_config.auth_config(),
        );
        store
            .hydrate_from_persistence()
            .await
            .context("failed to hydrate store from postgres")?;
        store
    } else {
        vault_bridge::store::VaultStore::new_with_auth_config(
            config.indexer.hub_note_threshold,
            runtime_config.auth_config(),
        )
    };
    store
        .set_hub_settings(
            config.indexer.hub_note_threshold,
            config.indexer.hub_note_fanout,
            config.indexer.hub_note_folders.clone(),
        )
        .await;
    store
        .set_link_context_chars(config.indexer.max_link_context_chars)
        .await;
    store
        .set_context_settings(
            config.context_assembly.default_max_tokens,
            config.context_assembly.max_max_tokens,
            config.context_assembly.default_max_depth,
        )
        .await;
    store.set_embedding_settings(config.embedding.clone()).await;
    store
        .set_new_note_path_settings(NewNotePathSettings::from(&config.new_note))
        .await;
    store
        .set_audit_settings(config.audit.enabled, config.audit.retention_days)
        .await;
    log_embedding_runtime_config(&config.embedding);

    if std::env::var("VAULT_BRIDGE_SEED_DATA")
        .map(|value| matches!(value.as_str(), "1" | "true" | "TRUE"))
        .unwrap_or(false)
    {
        store.seed_example_data().await;
    }

    let decryptor = build_livesync_decryptor(&config).await?;

    let mut _worker_handles = Vec::new();
    if matches!(run_mode, RunMode::All | RunMode::Workers) {
        if let Some(handle) = spawn_sync_worker(store.clone(), &config, decryptor.clone())
            .context("failed to start sync worker")?
        {
            _worker_handles.push(handle);
        }
        if let Some(handle) = spawn_embedding_worker(store.clone(), &config) {
            _worker_handles.push(handle);
        }
    }

    if let Some(config_path) = loaded_config.source_path.clone() {
        let poll_interval = config_reload_poll_interval()?;
        runtime_config
            .enable_reload(&config_path, poll_interval)
            .await;
        if let Some(interval) = poll_interval {
            _worker_handles.push(spawn_config_reload_poll_worker(
                runtime_config.clone(),
                config_path.clone(),
                interval,
            ));
        }
        match spawn_config_reload_sighup_worker(runtime_config.clone(), config_path.clone()) {
            Ok(handle) => {
                runtime_config.set_sighup_enabled(true).await;
                _worker_handles.push(handle);
            }
            Err(error) => warn!(
                error = %error,
                path = %config_path.display(),
                "SIGHUP config reload disabled"
            ),
        }
        info!(
            path = %config_path.display(),
            poll_interval_seconds = poll_interval.map(|duration| duration.as_secs()),
            "config hot reload enabled for auth policy sections"
        );
    } else {
        info!("config hot reload disabled because no config file was loaded");
    }

    if run_mode == RunMode::Workers {
        info!(
            run_mode = run_mode.as_str(),
            "vault_bridge workers started without API server"
        );
        std::future::pending::<()>().await;
        unreachable!("worker-only mode should not return");
    }

    let host = config.server.host.clone();
    let port = config.server.port;
    let addr: SocketAddr = format!("{}:{}", host, port)
        .parse()
        .context("invalid host/port combination")?;
    let couchdb = if config.couchdb.is_configured() {
        Some(Arc::new(
            vault_bridge::couchdb::CouchDbClient::new(&config.couchdb)
                .context("failed to initialize couchdb write client")?
                .with_livesync_crypto(decryptor),
        ))
    } else {
        None
    };
    let service = VaultBridgeService::new(store.clone(), couchdb);
    let api_tokens = ApiTokenState::from_env_with_auth_config(runtime_config.auth_config());
    let mcp = Some(vault_bridge::mcp::McpState::from_env_with_auth_config(
        service.clone(),
        runtime_config.auth_config(),
    )?);
    let state = AppState {
        service,
        api_tokens,
        mcp,
        runtime_config,
    };
    info!(run_mode = run_mode.as_str(), %addr, "starting vault_bridge API");
    serve(state, addr).await
}

fn flag_value(args: &[String], flag: &str) -> Option<String> {
    args.iter()
        .position(|arg| arg == flag)
        .and_then(|idx| args.get(idx + 1))
        .cloned()
}

fn config_reload_poll_interval() -> anyhow::Result<Option<StdDuration>> {
    let Ok(raw) = std::env::var("CONFIG_RELOAD_INTERVAL_SECONDS") else {
        return Ok(Some(StdDuration::from_secs(
            DEFAULT_CONFIG_RELOAD_INTERVAL_SECONDS,
        )));
    };
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Ok(Some(StdDuration::from_secs(
            DEFAULT_CONFIG_RELOAD_INTERVAL_SECONDS,
        )));
    }
    let seconds = trimmed.parse::<u64>().with_context(|| {
        format!("CONFIG_RELOAD_INTERVAL_SECONDS must be a non-negative integer, got '{raw}'")
    })?;
    if seconds == 0 {
        Ok(None)
    } else {
        Ok(Some(StdDuration::from_secs(seconds)))
    }
}

fn log_embedding_runtime_config(config: &vault_bridge::config::EmbeddingConfig) {
    info!(
        mode = config.mode.as_str(),
        model = config.schema_model(),
        dimensions = config.dimensions.max(1),
        endpoint = embedding_endpoint_for_diagnostics(config.localai_url()).as_deref(),
        request_dimensions = config.localai.request_dimensions,
        block_chunk_bytes = config.block_chunk_bytes(),
        max_embedding_failures = config.max_embedding_failures.max(1),
        "embedding runtime config resolved"
    );
}

fn embedding_endpoint_for_diagnostics(url: &str) -> Option<String> {
    let trimmed = url.trim();
    if trimmed.is_empty() {
        return None;
    }
    let without_scheme = trimmed
        .split_once("://")
        .map(|(_, rest)| rest)
        .unwrap_or(trimmed);
    let without_userinfo = without_scheme
        .rsplit_once('@')
        .map(|(_, rest)| rest)
        .unwrap_or(without_scheme);
    let (host_port, path_query) = without_userinfo
        .split_once('/')
        .map(|(host, path)| (host, format!("/{path}")))
        .unwrap_or((without_userinfo, String::new()));
    let host_port = host_port.trim();
    if host_port.is_empty() {
        return None;
    }
    let path = path_query
        .split_once('?')
        .map(|(path, _)| path)
        .unwrap_or(path_query.as_str());
    Some(format!("{host_port}{path}"))
}

fn parse_run_mode(args: &[String]) -> anyhow::Result<RunMode> {
    let api_only = args.iter().any(|arg| arg == "--api-only");
    let workers_only = args.iter().any(|arg| arg == "--workers-only");
    if api_only && workers_only {
        anyhow::bail!("--api-only and --workers-only are mutually exclusive");
    }

    let has_mode_flag = args.iter().any(|arg| arg == "--mode");
    let explicit_mode = if has_mode_flag {
        let Some(value) = flag_value(args, "--mode") else {
            anyhow::bail!("--mode requires one of: all, api, workers");
        };
        let parsed = match value.as_str() {
            "all" => RunMode::All,
            "api" => RunMode::Api,
            "workers" => RunMode::Workers,
            _ => anyhow::bail!("unsupported --mode value `{value}`; expected all, api, or workers"),
        };
        Some(parsed)
    } else {
        None
    };

    if api_only && explicit_mode.is_some_and(|mode| mode != RunMode::Api) {
        anyhow::bail!("--api-only cannot be combined with a different --mode value");
    }
    if workers_only && explicit_mode.is_some_and(|mode| mode != RunMode::Workers) {
        anyhow::bail!("--workers-only cannot be combined with a different --mode value");
    }

    Ok(explicit_mode.unwrap_or_else(|| {
        if api_only {
            RunMode::Api
        } else if workers_only {
            RunMode::Workers
        } else {
            RunMode::All
        }
    }))
}

async fn build_livesync_decryptor(config: &AppConfig) -> anyhow::Result<Option<Arc<Decryptor>>> {
    if !(config.couchdb.encryption.is_enabled() && config.couchdb.is_configured()) {
        return Ok(None);
    }

    let couch_for_salt = vault_bridge::couchdb::CouchDbClient::new(&config.couchdb)
        .context("failed to build couchdb client for encryption salt")?;
    let salt = couch_for_salt
        .fetch_livesync_pbkdf2_salt()
        .await
        .context("failed to fetch LiveSync PBKDF2 salt from CouchDB")?
        .context(
            "LiveSync sync parameters document missing or has no pbkdf2salt — has LiveSync synced at least once with encryption enabled?",
        )?;
    info!(
        salt_len = salt.len(),
        "derived LiveSync decryption key from passphrase + PBKDF2 salt"
    );
    Ok(Some(Arc::new(Decryptor::new(
        &config.couchdb.encryption.passphrase,
        &salt,
    ))))
}

async fn debug_note_scan(
    config: &AppConfig,
    note_path: &str,
    include_raw_docs: bool,
) -> anyhow::Result<()> {
    anyhow::ensure!(
        config.couchdb.is_configured(),
        "couchdb must be configured for --debug-note-scan"
    );

    let normalized_path = vault_bridge::livesync::normalize_note_path(note_path);
    let decryptor = build_livesync_decryptor(config).await?;
    let couch = vault_bridge::couchdb::CouchDbClient::new(&config.couchdb)
        .context("failed to build couchdb client for note scan")?;
    let source = couch
        .diagnose_note_source(&normalized_path, decryptor.as_deref())
        .await
        .context("failed to classify LiveSync source documents")?;

    let local = if config.database.is_configured() {
        let persistence =
            PostgresPersistence::connect_and_migrate(&config.database, config.embedding.dimensions)
                .await
                .context("failed to connect to postgres for note diagnostics")?;
        serde_json::to_value(
            persistence
                .note_source_diagnostic(&normalized_path, source.file_revision.as_deref())
                .await
                .context("failed to inspect local note state")?,
        )?
    } else {
        serde_json::json!({
            "database_configured": false,
            "state": "unavailable"
        })
    };

    println!(
        "{}",
        serde_json::to_string_pretty(&serde_json::json!({
            "note_path": normalized_path,
            "content_included": false,
            "source": source,
            "local": local
        }))?
    );

    if include_raw_docs {
        eprintln!(
            "WARNING: --include-raw-docs prints decrypted metadata and LiveSync leaf content"
        );
        dump_raw_note_documents(&couch, &normalized_path, decryptor.as_deref()).await?;
    }

    Ok(())
}

async fn dump_raw_note_documents(
    couch: &vault_bridge::couchdb::CouchDbClient,
    note_path: &str,
    decryptor: Option<&Decryptor>,
) -> anyhow::Result<()> {
    let Some(file_doc) = couch
        .find_file_document_by_note_path(note_path, decryptor)
        .await
        .context("failed to scan CouchDB documents for note path")?
    else {
        return Ok(());
    };

    println!("\n=== raw file document ===");
    println!("{}", serde_json::to_string_pretty(&file_doc)?);
    let mut children = Vec::new();
    if let Ok(LivesyncDocument::File(file)) = LivesyncDocument::try_from(file_doc) {
        children = file.children.clone();
        if let Some(decryptor) = decryptor
            && vault_bridge::encryption::is_encrypted_meta_path(&file.path)
            && let Ok(meta) = decryptor.decrypt_meta_document(&file.path)
        {
            println!("\n=== decrypted file metadata ===");
            println!("{}", serde_json::to_string_pretty(&meta)?);
            if let Some(metadata_children) = meta.get("children").and_then(Value::as_array) {
                children = metadata_children.clone();
            }
        }
    }
    for child in children {
        let Some(child_id) = child_doc_id(&child) else {
            continue;
        };
        println!("\n=== raw child document ({child_id}) ===");
        match couch.get_document(&child_id).await? {
            Some(child_doc) => println!("{}", serde_json::to_string_pretty(&child_doc)?),
            None => println!("not found"),
        }
    }
    Ok(())
}

async fn delete_note_scan(
    config: &AppConfig,
    note_path: &str,
    delete_leaf: bool,
) -> anyhow::Result<()> {
    anyhow::ensure!(
        config.couchdb.is_configured(),
        "couchdb must be configured for --delete-note-scan"
    );

    let decryptor = build_livesync_decryptor(config).await?;
    let couch = vault_bridge::couchdb::CouchDbClient::new(&config.couchdb)
        .context("failed to build couchdb client for note deletion")?;

    println!("Delete note scan for path: {note_path}");
    let deleted = couch
        .delete_note_documents_by_note_path(note_path, delete_leaf, decryptor.as_deref())
        .await
        .context("failed to delete couchdb documents for note path")?;

    anyhow::ensure!(
        !deleted.is_empty(),
        "No CouchDB file doc matched the requested note path."
    );

    println!(
        "note_path={note_path} deleted_doc_ids={}",
        deleted.join(" ")
    );
    Ok(())
}

fn child_doc_id(child: &Value) -> Option<String> {
    if let Some(raw) = child.as_str() {
        return Some(raw.to_string());
    }
    if let Some(raw) = child.get("id").and_then(Value::as_str) {
        return Some(raw.to_string());
    }
    child
        .get("_id")
        .and_then(Value::as_str)
        .map(ToString::to_string)
}

async fn reindex_blocks(config: &AppConfig, only_missing: bool) -> anyhow::Result<()> {
    anyhow::ensure!(
        config.database.is_configured(),
        "database must be configured for --reindex-blocks"
    );

    let persistence =
        PostgresPersistence::connect_and_migrate(&config.database, config.embedding.dimensions)
            .await
            .context("failed to connect to postgres")?;

    let notes = if only_missing {
        persistence
            .notes_without_blocks()
            .await
            .context("failed to load notes without blocks")?
    } else {
        persistence
            .load_all_notes_for_block_reindex()
            .await
            .context("failed to load notes for block reindex")?
    };

    let total = notes.len();
    info!(total, "reindex-blocks: backfilling blocks for notes");

    let min_chars = config.embedding.block_min_chars;
    let max_bytes = config.embedding.block_chunk_bytes();
    let overlap_sentences = config.embedding.block_chunk_overlap_sentences;
    let mut block_count = 0usize;

    for (i, (note_id, path, title, content)) in notes.into_iter().enumerate() {
        let blocks = split_into_semantic_blocks(&content, min_chars, max_bytes, overlap_sentences);
        let breadcrumbs: Vec<String> = blocks
            .iter()
            .map(|b| breadcrumb_prefix(&path, &title, &b.heading_path))
            .collect();

        let result = persistence
            .sync_blocks_for_note(&note_id, &blocks, &breadcrumbs)
            .await
            .with_context(|| format!("failed to sync blocks for note {note_id}"))?;
        block_count += result.inserted;

        if (i + 1) % 100 == 0 {
            info!(
                progress = i + 1,
                total, block_count, "reindex-blocks: progress"
            );
        }
    }

    let cleared = persistence
        .clear_all_note_embeddings()
        .await
        .context("failed to clear note embeddings")?;

    info!(
        total,
        block_count,
        cleared_note_embeddings = cleared,
        only_missing,
        "reindex-blocks: done; semantic chunks synced and note embeddings queued for re-embedding with breadcrumbs"
    );

    Ok(())
}

async fn unblock_embeddings(config: &AppConfig, args: &[String]) -> anyhow::Result<()> {
    anyhow::ensure!(
        config.database.is_configured(),
        "database must be configured for --embedding-unblock"
    );

    let selector = EmbeddingUnblockSelector {
        note_id: flag_value(args, "--note-id"),
        path_prefix: flag_value(args, "--path-prefix"),
        block_id: flag_value(args, "--block-id"),
        limit: flag_value(args, "--limit")
            .map(|value| value.parse::<usize>())
            .transpose()
            .context("--limit must be a positive integer")?,
        all: args.iter().any(|arg| arg == "--all"),
    };
    let target_count = usize::from(selector.note_id.is_some())
        + usize::from(selector.path_prefix.is_some())
        + usize::from(selector.block_id.is_some())
        + usize::from(selector.all);
    anyhow::ensure!(
        target_count == 1,
        "choose exactly one unblock target: --note-id, --path-prefix, --block-id, or --all"
    );
    if selector.all {
        anyhow::ensure!(
            selector.limit.is_some(),
            "--all requires --limit; repeat with a larger limit after verifying backend health"
        );
    }

    let dry_run = args.iter().any(|arg| arg == "--dry-run");
    let persistence =
        PostgresPersistence::connect_and_migrate(&config.database, config.embedding.dimensions)
            .await
            .context("failed to connect to postgres")?;
    let result = persistence
        .unblock_embeddings(&selector, dry_run)
        .await
        .context("failed to unblock embeddings")?;

    println!(
        "dry_run={} notes_matched={} blocks_matched={} notes_reset={} blocks_reset={}",
        result.dry_run,
        result.notes_matched,
        result.blocks_matched,
        result.notes_reset,
        result.blocks_reset
    );

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{RunMode, parse_run_mode};

    #[test]
    fn parse_run_mode_defaults_to_all() {
        assert_eq!(parse_run_mode(&[]).expect("default mode"), RunMode::All);
    }

    #[test]
    fn parse_run_mode_accepts_explicit_api_mode() {
        assert_eq!(
            parse_run_mode(&["--mode".to_string(), "api".to_string()]).expect("api mode"),
            RunMode::Api
        );
    }

    #[test]
    fn parse_run_mode_rejects_conflicting_flags() {
        let error = parse_run_mode(&["--api-only".to_string(), "--workers-only".to_string()])
            .expect_err("conflict should error");
        assert!(
            error
                .to_string()
                .contains("--api-only and --workers-only are mutually exclusive")
        );
    }
}

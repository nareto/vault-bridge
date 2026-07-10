use std::collections::HashMap;

use chrono::{DateTime, Utc};
use pgvector::Vector;
use serde_json::Value;
use sqlx::migrate::Migrator;
use sqlx::{PgPool, Row, postgres::PgPoolOptions};
use thiserror::Error;

use crate::config::DatabaseConfig;
static MIGRATOR: Migrator = sqlx::migrate!("./migrations");
const MAX_INDEXED_SEARCH_TEXT_BYTES: usize = 500_000;

#[derive(Clone, Debug)]
pub struct PostgresPersistence {
    pool: PgPool,
}

#[derive(Debug, Clone)]
pub struct PersistedLinkRecord {
    pub target_id: String,
    pub context_text: String,
    pub position: usize,
}

#[derive(Debug, Clone)]
pub struct PersistedNoteRecord {
    pub id: String,
    pub path: String,
    pub title: String,
    pub content: String,
    pub search_text: String,
    pub summary: String,
    pub frontmatter: Value,
    pub tags: Vec<String>,
    pub couchdb_rev: String,
    pub created_at: Option<DateTime<Utc>>,
    pub updated_at: DateTime<Utc>,
    pub indexed_at: DateTime<Utc>,
    pub embedding: Option<Vec<f32>>,
    pub links: Vec<PersistedLinkRecord>,
}

#[derive(Debug, Clone)]
pub struct PersistedSyncState {
    pub last_seq: String,
    pub couchdb_current_seq: String,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct PersistedStagedChunk {
    pub parent_id: String,
    pub chunk_index: usize,
    pub chunk_count: usize,
    pub content: String,
    pub couchdb_rev: String,
    pub received_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Default)]
pub struct PersistedIngestDelta {
    pub note_upserts: Vec<PersistedNoteRecord>,
    pub note_deletes: Vec<String>,
    pub sync_state: Option<PersistedSyncState>,
    pub chunk_upserts: Vec<PersistedStagedChunk>,
    pub chunk_deletes: Vec<String>,
    pub alias_upserts: Vec<PersistedFileAlias>,
    pub alias_deletes: Vec<String>,
    pub vault_file_upserts: Vec<PersistedVaultFile>,
    pub vault_file_deletes: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PersistedRecoveryTarget {
    pub recovery_kind: String,
    pub target_id: String,
    pub failure_count: usize,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RecoveryQueueStats {
    pub pending: usize,
    pub quarantined: usize,
}

#[derive(Debug, Clone)]
pub struct PersistedFileAlias {
    pub file_doc_id: String,
    pub note_path: String,
    pub couchdb_rev: String,
    pub children: Vec<String>,
    pub ctime: i64,
    pub mtime: i64,
}

#[derive(Debug, Clone)]
pub struct PersistedAccessLogEntry {
    pub timestamp: DateTime<Utc>,
    pub context: String,
    pub endpoint: String,
    pub query_params: Value,
    pub notes_returned: Vec<String>,
    pub notes_filtered_count: usize,
}

#[derive(Debug, Clone)]
pub struct PersistedSearchNote {
    pub id: String,
    pub title: String,
    pub content: String,
}

#[derive(Debug, Clone)]
pub struct BlockEmbeddingCandidate {
    pub id: String,
    pub text: String,
}

#[derive(Debug, Clone)]
pub struct BlockSemanticMatch {
    pub block_id: String,
    pub note_id: String,
    pub heading_path: String,
    pub content: String,
    pub score: f32,
}

#[derive(Debug, Clone, Default)]
pub struct BlockEmbeddingStats {
    pub pending: usize,
    pub quarantined: usize,
    pub last_success_at: Option<DateTime<Utc>>,
    pub last_error_at: Option<DateTime<Utc>>,
    pub last_error: Option<String>,
}

#[derive(Debug, Clone, Default)]
pub struct EmbeddingUnblockSelector {
    pub note_id: Option<String>,
    pub path_prefix: Option<String>,
    pub block_id: Option<String>,
    pub limit: Option<usize>,
    pub all: bool,
}

#[derive(Debug, Clone, Default)]
pub struct EmbeddingUnblockResult {
    pub dry_run: bool,
    pub notes_matched: usize,
    pub blocks_matched: usize,
    pub notes_reset: usize,
    pub blocks_reset: usize,
}

#[derive(Debug, Clone)]
pub struct PersistedNoteSummary {
    pub id: String,
    pub title: String,
    pub summary: String,
    pub updated_at: DateTime<Utc>,
    pub tags: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct PersistedLinkContext {
    pub source_id: String,
    pub target_id: String,
    pub context_text: String,
    pub position: usize,
}

#[derive(Debug, Clone)]
pub struct PersistedSnapshot {
    pub generation: u64,
    pub notes: Vec<PersistedNoteRecord>,
    pub sync_state: Option<PersistedSyncState>,
    pub staged_chunks: Vec<PersistedStagedChunk>,
    pub file_aliases: Vec<PersistedFileAlias>,
    pub vault_files: Vec<PersistedVaultFile>,
}

#[derive(Debug, Clone)]
pub struct PersistedVaultFile {
    pub path: String,
    pub content: String,
    pub couchdb_rev: String,
    pub created_at: Option<DateTime<Utc>>,
    pub updated_at: DateTime<Utc>,
    pub indexed_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct EmbeddingSchemaSyncResult {
    pub target_dimensions: usize,
    pub target_hnsw_m: usize,
    pub target_hnsw_ef_construction: usize,
    pub previous_dimensions: Option<usize>,
    pub previous_model: Option<String>,
    pub previous_hnsw_m: Option<usize>,
    pub previous_hnsw_ef_construction: Option<usize>,
    pub column_dimensions_before: Option<usize>,
    pub rebuilt_embedding_index: bool,
    pub reset_embeddings: bool,
    pub cleared_embeddings: usize,
}

#[derive(Debug, Clone)]
pub struct SyncBlocksResult {
    pub inserted: usize,
    pub updated: usize,
    pub deleted: usize,
}

#[derive(Debug, Error)]
pub enum PersistenceError {
    #[error(transparent)]
    Sqlx(#[from] sqlx::Error),
    #[error(transparent)]
    Migration(#[from] sqlx::migrate::MigrateError),
}

impl PostgresPersistence {
    pub async fn connect(config: &DatabaseConfig) -> Result<Self, PersistenceError> {
        Self::connect_with_url(&config.url, config.max_connections).await
    }

    pub async fn connect_with_url(
        database_url: &str,
        max_connections: u32,
    ) -> Result<Self, PersistenceError> {
        let pool = PgPoolOptions::new()
            .max_connections(max_connections.max(1))
            .connect(database_url)
            .await?;
        Ok(Self { pool })
    }

    pub async fn connect_and_migrate(
        config: &DatabaseConfig,
        target_embedding_dimensions: usize,
    ) -> Result<Self, PersistenceError> {
        let persistence = Self::connect(config).await?;
        persistence
            .prepare_embedding_column_for_migrations(target_embedding_dimensions)
            .await?;
        persistence.migrate().await?;
        Ok(persistence)
    }

    pub async fn migrate(&self) -> Result<(), PersistenceError> {
        MIGRATOR.run(&self.pool).await?;
        Ok(())
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    async fn prepare_embedding_column_for_migrations(
        &self,
        target_embedding_dimensions: usize,
    ) -> Result<(), PersistenceError> {
        let target_dimensions = target_embedding_dimensions.max(1).min(i32::MAX as usize);
        let target_dimensions_i32 = target_dimensions as i32;
        let mut tx = self.pool.begin().await?;

        sqlx::query("CREATE EXTENSION IF NOT EXISTS vector")
            .execute(&mut *tx)
            .await?;
        // Bootstraps fresh databases with a typed vector column so migration 0001's
        // HNSW index creation succeeds on pgvector versions that require dimensions.
        sqlx::query(&format!(
            "CREATE TABLE IF NOT EXISTS notes (
                id TEXT PRIMARY KEY,
                path TEXT NOT NULL,
                title TEXT NOT NULL,
                content TEXT NOT NULL,
                summary TEXT NOT NULL DEFAULT '',
                frontmatter JSONB NOT NULL DEFAULT '{{}}'::jsonb,
                sensitivity TEXT NOT NULL DEFAULT 'public',
                embedding vector({target_dimensions_i32}),
                couchdb_rev TEXT NOT NULL,
                created_at TIMESTAMPTZ,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                indexed_at TIMESTAMPTZ NOT NULL DEFAULT now()
            )"
        ))
        .execute(&mut *tx)
        .await?;

        if embedding_column_dimensions_tx(&mut tx).await?.is_none() {
            sqlx::query("UPDATE notes SET embedding = NULL WHERE embedding IS NOT NULL")
                .execute(&mut *tx)
                .await?;
            sqlx::query(&format!(
                "ALTER TABLE notes ALTER COLUMN embedding TYPE vector({target_dimensions_i32})"
            ))
            .execute(&mut *tx)
            .await?;
        }

        tx.commit().await?;
        Ok(())
    }

    pub async fn ensure_embedding_schema(
        &self,
        model: &str,
        dimensions: usize,
        hnsw_m: usize,
        hnsw_ef_construction: usize,
    ) -> Result<EmbeddingSchemaSyncResult, PersistenceError> {
        let target_dimensions = dimensions.max(1).min(i32::MAX as usize);
        let target_dimensions_i32 = target_dimensions as i32;
        let target_hnsw_m = hnsw_m.max(1).min(i32::MAX as usize);
        let target_hnsw_m_i32 = target_hnsw_m as i32;
        let target_hnsw_ef_construction = hnsw_ef_construction.max(1).min(i32::MAX as usize);
        let target_hnsw_ef_construction_i32 = target_hnsw_ef_construction as i32;
        let trimmed_model = model.trim();
        let effective_model = if trimmed_model.is_empty() {
            "unknown"
        } else {
            trimmed_model
        };

        let mut tx = self.pool.begin().await?;

        let metadata_row = sqlx::query(
            "SELECT model, dimensions, hnsw_m, hnsw_ef_construction FROM embedding_schema WHERE id = 1",
        )
        .fetch_optional(&mut *tx)
        .await?;
        let previous_model = metadata_row
            .as_ref()
            .and_then(|row| row.try_get("model").ok());
        let previous_dimensions = metadata_row
            .as_ref()
            .and_then(|row| row.try_get::<i32, _>("dimensions").ok())
            .map(|value| value.max(0) as usize);
        let previous_hnsw_m = metadata_row
            .as_ref()
            .and_then(|row| row.try_get::<i32, _>("hnsw_m").ok())
            .map(|value| value.max(0) as usize);
        let previous_hnsw_ef_construction = metadata_row
            .as_ref()
            .and_then(|row| row.try_get::<i32, _>("hnsw_ef_construction").ok())
            .map(|value| value.max(0) as usize);

        let column_dimensions_before = embedding_column_dimensions_tx(&mut tx).await?;
        let blocks_exists: bool = sqlx::query_scalar(
            "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'blocks')",
        )
        .fetch_one(&mut *tx)
        .await?;
        let block_column_dimensions_before = if blocks_exists {
            embedding_column_dimensions_for_table_tx(&mut tx, "blocks").await?
        } else {
            None
        };
        let dimensions_changed = column_dimensions_before != Some(target_dimensions);
        let block_dimensions_changed =
            blocks_exists && block_column_dimensions_before != Some(target_dimensions);
        let model_changed = previous_model
            .as_deref()
            .is_some_and(|value| value != effective_model);
        let hnsw_changed = previous_hnsw_m != Some(target_hnsw_m)
            || previous_hnsw_ef_construction != Some(target_hnsw_ef_construction);

        let mut cleared_embeddings = 0usize;
        let mut reset_embeddings = false;
        let mut rebuilt_embedding_index = false;

        if dimensions_changed || model_changed {
            let cleared: i64 = sqlx::query_scalar(
                r#"
                WITH cleared AS (
                    UPDATE notes
                    SET embedding = NULL,
                        embedding_failures = 0,
                        embedding_failed_at = NULL
                    WHERE embedding IS NOT NULL
                       OR embedding_failures <> 0
                       OR embedding_failed_at IS NOT NULL
                    RETURNING 1
                )
                SELECT COUNT(*) FROM cleared
                "#,
            )
            .fetch_one(&mut *tx)
            .await?;
            cleared_embeddings = cleared.max(0) as usize;

            reset_embeddings = true;
        }

        if blocks_exists && (block_dimensions_changed || dimensions_changed || model_changed) {
            sqlx::query(
                r#"
                UPDATE blocks
                SET embedding = NULL,
                    embedding_failures = 0,
                    embedding_failed_at = NULL,
                    last_embedding_error = NULL
                WHERE embedding IS NOT NULL
                   OR embedding_failures <> 0
                   OR embedding_failed_at IS NOT NULL
                   OR last_embedding_error IS NOT NULL
                "#,
            )
            .execute(&mut *tx)
            .await?;
        }

        if dimensions_changed {
            // Dimension shifts require a typed vector column; existing embeddings
            // were cleared above and Worker B will repopulate in the background.
            let alter = format!(
                "ALTER TABLE notes ALTER COLUMN embedding TYPE vector({target_dimensions_i32})"
            );
            sqlx::query(&alter).execute(&mut *tx).await?;
        }

        if block_dimensions_changed {
            let alter_blocks = format!(
                "ALTER TABLE blocks ALTER COLUMN embedding TYPE vector({target_dimensions_i32})"
            );
            sqlx::query(&alter_blocks).execute(&mut *tx).await?;
        }

        if dimensions_changed || hnsw_changed {
            sqlx::query("DROP INDEX IF EXISTS idx_notes_embedding")
                .execute(&mut *tx)
                .await?;
            let create_index =
                create_hnsw_embedding_index_sql(target_hnsw_m_i32, target_hnsw_ef_construction_i32);
            sqlx::query(&create_index).execute(&mut *tx).await?;
            rebuilt_embedding_index = true;
        }

        if blocks_exists && (block_dimensions_changed || dimensions_changed || hnsw_changed) {
            sqlx::query("DROP INDEX IF EXISTS idx_blocks_embedding")
                .execute(&mut *tx)
                .await?;
            let create_blocks_index = create_hnsw_block_embedding_index_sql(
                target_hnsw_m_i32,
                target_hnsw_ef_construction_i32,
            );
            sqlx::query(&create_blocks_index).execute(&mut *tx).await?;
            rebuilt_embedding_index = true;
        }

        sqlx::query(
            r#"
            INSERT INTO embedding_schema (id, model, dimensions, hnsw_m, hnsw_ef_construction, updated_at)
            VALUES (1, $1, $2, $3, $4, now())
            ON CONFLICT (id)
            DO UPDATE SET
                model = EXCLUDED.model,
                dimensions = EXCLUDED.dimensions,
                hnsw_m = EXCLUDED.hnsw_m,
                hnsw_ef_construction = EXCLUDED.hnsw_ef_construction,
                updated_at = EXCLUDED.updated_at
            "#,
        )
        .bind(effective_model)
        .bind(target_dimensions_i32)
        .bind(target_hnsw_m_i32)
        .bind(target_hnsw_ef_construction_i32)
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;

        Ok(EmbeddingSchemaSyncResult {
            target_dimensions,
            target_hnsw_m,
            target_hnsw_ef_construction,
            previous_dimensions,
            previous_model,
            previous_hnsw_m,
            previous_hnsw_ef_construction,
            column_dimensions_before,
            rebuilt_embedding_index,
            reset_embeddings,
            cleared_embeddings,
        })
    }

    pub async fn upsert_note(&self, note: &PersistedNoteRecord) -> Result<(), PersistenceError> {
        self.apply_content_delta(vec![note.clone()], Vec::new(), None, Vec::new(), Vec::new())
            .await?;
        Ok(())
    }

    pub async fn delete_note(&self, note_id: &str) -> Result<(), PersistenceError> {
        self.apply_content_delta(
            Vec::new(),
            vec![note_id.to_string()],
            None,
            Vec::new(),
            Vec::new(),
        )
        .await?;
        Ok(())
    }

    pub async fn apply_delta(
        &self,
        upserts: Vec<PersistedNoteRecord>,
        deletes: Vec<String>,
        sync_state: Option<PersistedSyncState>,
    ) -> Result<u64, PersistenceError> {
        self.apply_content_delta(upserts, deletes, sync_state, Vec::new(), Vec::new())
            .await
    }

    pub async fn apply_content_delta(
        &self,
        mut note_upserts: Vec<PersistedNoteRecord>,
        mut note_deletes: Vec<String>,
        sync_state: Option<PersistedSyncState>,
        mut vault_file_upserts: Vec<PersistedVaultFile>,
        mut vault_file_deletes: Vec<String>,
    ) -> Result<u64, PersistenceError> {
        // Concurrent API/worker transactions lock rows in the same order.
        note_upserts.sort_by(|a, b| a.id.cmp(&b.id));
        note_deletes.sort();
        note_deletes.dedup();
        vault_file_upserts.sort_by(|a, b| a.path.cmp(&b.path));
        vault_file_deletes.sort();
        vault_file_deletes.dedup();

        let mut tx = self.pool.begin().await?;

        for note_id in note_deletes {
            sqlx::query("DELETE FROM notes WHERE id = $1")
                .bind(note_id)
                .execute(&mut *tx)
                .await?;
        }

        for note in &note_upserts {
            upsert_note_tx(&mut tx, note).await?;
        }

        for path in vault_file_deletes {
            delete_vault_file_tx(&mut tx, &path).await?;
        }

        for file in &vault_file_upserts {
            upsert_vault_file_tx(&mut tx, file).await?;
        }

        if let Some(sync_state) = sync_state {
            upsert_sync_state_tx(&mut tx, &sync_state).await?;
        }

        let generation = bump_store_generation_tx(&mut tx).await?;
        tx.commit().await?;
        Ok(generation)
    }

    /// Persist one `_changes` batch and its cursor in a single transaction.
    pub async fn apply_ingest_delta(
        &self,
        mut delta: PersistedIngestDelta,
    ) -> Result<u64, PersistenceError> {
        delta.note_upserts.sort_by(|a, b| a.id.cmp(&b.id));
        delta.note_deletes.sort();
        delta.note_deletes.dedup();
        delta.chunk_upserts.sort_by(|a, b| {
            a.parent_id
                .cmp(&b.parent_id)
                .then(a.chunk_index.cmp(&b.chunk_index))
        });
        delta.chunk_deletes.sort();
        delta.chunk_deletes.dedup();
        delta
            .alias_upserts
            .sort_by(|a, b| a.file_doc_id.cmp(&b.file_doc_id));
        delta.alias_deletes.sort();
        delta.alias_deletes.dedup();
        delta.vault_file_upserts.sort_by(|a, b| a.path.cmp(&b.path));
        delta.vault_file_deletes.sort();
        delta.vault_file_deletes.dedup();

        let mut tx = self.pool.begin().await?;

        for note_id in delta.note_deletes {
            sqlx::query("DELETE FROM notes WHERE id = $1")
                .bind(note_id)
                .execute(&mut *tx)
                .await?;
        }
        for note in &delta.note_upserts {
            upsert_note_tx(&mut tx, note).await?;
        }
        for path in delta.vault_file_deletes {
            delete_vault_file_tx(&mut tx, &path).await?;
        }
        for file in &delta.vault_file_upserts {
            upsert_vault_file_tx(&mut tx, file).await?;
        }

        for parent_id in delta.chunk_deletes {
            sqlx::query("DELETE FROM chunk_staging WHERE parent_id = $1")
                .bind(parent_id)
                .execute(&mut *tx)
                .await?;
        }
        for chunk in delta.chunk_upserts {
            let chunk_index = chunk.chunk_index.min(i32::MAX as usize) as i32;
            let chunk_count = chunk.chunk_count.min(i32::MAX as usize).max(1) as i32;
            sqlx::query("DELETE FROM chunk_staging WHERE parent_id = $1 AND couchdb_rev <> $2")
                .bind(&chunk.parent_id)
                .bind(&chunk.couchdb_rev)
                .execute(&mut *tx)
                .await?;
            sqlx::query(
                r#"
                INSERT INTO chunk_staging (
                    parent_id, chunk_index, chunk_count, content, couchdb_rev, received_at
                )
                VALUES ($1, $2, $3, $4, $5, $6)
                ON CONFLICT (parent_id, chunk_index)
                DO UPDATE SET
                    chunk_count = EXCLUDED.chunk_count,
                    content = EXCLUDED.content,
                    couchdb_rev = EXCLUDED.couchdb_rev,
                    received_at = EXCLUDED.received_at
                "#,
            )
            .bind(&chunk.parent_id)
            .bind(chunk_index)
            .bind(chunk_count)
            .bind(&chunk.content)
            .bind(&chunk.couchdb_rev)
            .bind(chunk.received_at)
            .execute(&mut *tx)
            .await?;
        }

        for file_doc_id in delta.alias_deletes {
            sqlx::query("DELETE FROM file_aliases WHERE file_doc_id = $1")
                .bind(file_doc_id)
                .execute(&mut *tx)
                .await?;
        }
        for alias in delta.alias_upserts {
            sqlx::query(
                r#"
                INSERT INTO file_aliases (
                    file_doc_id, note_path, couchdb_rev, children, ctime, mtime, updated_at
                )
                VALUES ($1, $2, $3, $4, $5, $6, now())
                ON CONFLICT (file_doc_id)
                DO UPDATE SET
                    note_path = EXCLUDED.note_path,
                    couchdb_rev = EXCLUDED.couchdb_rev,
                    children = EXCLUDED.children,
                    ctime = EXCLUDED.ctime,
                    mtime = EXCLUDED.mtime,
                    updated_at = EXCLUDED.updated_at
                "#,
            )
            .bind(&alias.file_doc_id)
            .bind(&alias.note_path)
            .bind(&alias.couchdb_rev)
            .bind(&alias.children)
            .bind(alias.ctime)
            .bind(alias.mtime)
            .execute(&mut *tx)
            .await?;
        }

        if let Some(sync_state) = delta.sync_state {
            upsert_sync_state_tx(&mut tx, &sync_state).await?;
        }

        let generation = bump_store_generation_tx(&mut tx).await?;
        tx.commit().await?;
        Ok(generation)
    }

    pub async fn apply_confirmed_vault_file_deletion(
        &self,
        path: &str,
    ) -> Result<u64, PersistenceError> {
        let mut tx = self.pool.begin().await?;

        sqlx::query(
            r#"
            DELETE FROM chunk_staging cs
            USING file_aliases fa
            WHERE fa.note_path = $1
              AND (
                  cs.parent_id = fa.file_doc_id
                  OR cs.parent_id = ANY(fa.children)
              )
            "#,
        )
        .bind(path)
        .execute(&mut *tx)
        .await?;
        sqlx::query("DELETE FROM chunk_staging WHERE parent_id = $1")
            .bind(path)
            .execute(&mut *tx)
            .await?;
        sqlx::query("DELETE FROM notes WHERE id = $1")
            .bind(path)
            .execute(&mut *tx)
            .await?;
        delete_vault_file_tx(&mut tx, path).await?;
        sqlx::query("DELETE FROM file_aliases WHERE note_path = $1")
            .bind(path)
            .execute(&mut *tx)
            .await?;

        let generation = bump_store_generation_tx(&mut tx).await?;
        tx.commit().await?;
        Ok(generation)
    }

    pub async fn apply_chunk_staging_delta(
        &self,
        upserts: Vec<PersistedStagedChunk>,
        delete_parents: Vec<String>,
    ) -> Result<(), PersistenceError> {
        if upserts.is_empty() && delete_parents.is_empty() {
            return Ok(());
        }

        let mut tx = self.pool.begin().await?;

        for chunk in upserts {
            let chunk_index = chunk.chunk_index.min(i32::MAX as usize) as i32;
            let chunk_count = chunk.chunk_count.min(i32::MAX as usize).max(1) as i32;

            sqlx::query("DELETE FROM chunk_staging WHERE parent_id = $1 AND couchdb_rev <> $2")
                .bind(&chunk.parent_id)
                .bind(&chunk.couchdb_rev)
                .execute(&mut *tx)
                .await?;

            sqlx::query(
                r#"
                INSERT INTO chunk_staging (
                    parent_id, chunk_index, chunk_count, content, couchdb_rev, received_at
                )
                VALUES ($1, $2, $3, $4, $5, $6)
                ON CONFLICT (parent_id, chunk_index)
                DO UPDATE SET
                    chunk_count = EXCLUDED.chunk_count,
                    content = EXCLUDED.content,
                    couchdb_rev = EXCLUDED.couchdb_rev,
                    received_at = EXCLUDED.received_at
                "#,
            )
            .bind(&chunk.parent_id)
            .bind(chunk_index)
            .bind(chunk_count)
            .bind(&chunk.content)
            .bind(&chunk.couchdb_rev)
            .bind(chunk.received_at)
            .execute(&mut *tx)
            .await?;
        }

        for parent_id in delete_parents {
            sqlx::query("DELETE FROM chunk_staging WHERE parent_id = $1")
                .bind(parent_id)
                .execute(&mut *tx)
                .await?;
        }

        tx.commit().await?;
        Ok(())
    }

    pub async fn purge_chunk_staging_and_enqueue_recovery(
        &self,
        mut delete_parents: Vec<String>,
        recovery_kind: &str,
        mut recovery_targets: Vec<String>,
    ) -> Result<(), PersistenceError> {
        delete_parents.sort();
        delete_parents.dedup();
        recovery_targets.sort();
        recovery_targets.dedup();
        let mut tx = self.pool.begin().await?;
        for parent_id in delete_parents {
            sqlx::query("DELETE FROM chunk_staging WHERE parent_id = $1")
                .bind(parent_id)
                .execute(&mut *tx)
                .await?;
        }
        if !recovery_targets.is_empty() {
            sqlx::query(
                r#"
                INSERT INTO sync_recovery_queue (recovery_kind, target_id)
                SELECT $1, target_id
                FROM unnest($2::text[]) AS target_id
                ON CONFLICT (recovery_kind, target_id) DO NOTHING
                "#,
            )
            .bind(recovery_kind)
            .bind(&recovery_targets)
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        Ok(())
    }

    pub async fn apply_file_alias_delta(
        &self,
        upserts: Vec<PersistedFileAlias>,
        delete_file_doc_ids: Vec<String>,
    ) -> Result<(), PersistenceError> {
        if upserts.is_empty() && delete_file_doc_ids.is_empty() {
            return Ok(());
        }

        let mut tx = self.pool.begin().await?;

        for alias in upserts {
            sqlx::query(
                r#"
                INSERT INTO file_aliases (
                    file_doc_id, note_path, couchdb_rev, children, ctime, mtime, updated_at
                )
                VALUES ($1, $2, $3, $4, $5, $6, now())
                ON CONFLICT (file_doc_id)
                DO UPDATE SET
                    note_path = EXCLUDED.note_path,
                    couchdb_rev = EXCLUDED.couchdb_rev,
                    children = EXCLUDED.children,
                    ctime = EXCLUDED.ctime,
                    mtime = EXCLUDED.mtime,
                    updated_at = EXCLUDED.updated_at
                "#,
            )
            .bind(&alias.file_doc_id)
            .bind(&alias.note_path)
            .bind(&alias.couchdb_rev)
            .bind(&alias.children)
            .bind(alias.ctime)
            .bind(alias.mtime)
            .execute(&mut *tx)
            .await?;
        }

        for file_doc_id in delete_file_doc_ids {
            sqlx::query("DELETE FROM file_aliases WHERE file_doc_id = $1")
                .bind(file_doc_id)
                .execute(&mut *tx)
                .await?;
        }

        tx.commit().await?;
        Ok(())
    }

    pub async fn upsert_vault_file(
        &self,
        file: &PersistedVaultFile,
    ) -> Result<(), PersistenceError> {
        self.apply_vault_file_delta(vec![file.clone()], Vec::new())
            .await?;
        Ok(())
    }

    pub async fn delete_vault_file(&self, path: &str) -> Result<(), PersistenceError> {
        self.apply_vault_file_delta(Vec::new(), vec![path.to_string()])
            .await?;
        Ok(())
    }

    pub async fn apply_vault_file_delta(
        &self,
        upserts: Vec<PersistedVaultFile>,
        deletes: Vec<String>,
    ) -> Result<u64, PersistenceError> {
        self.apply_content_delta(Vec::new(), Vec::new(), None, upserts, deletes)
            .await
    }

    pub async fn load_store_generation(&self) -> Result<u64, PersistenceError> {
        let generation: i64 = sqlx::query_scalar("SELECT generation FROM store_state WHERE id = 1")
            .fetch_optional(&self.pool)
            .await?
            .unwrap_or(0);
        Ok(generation.max(0) as u64)
    }

    pub async fn vault_path_exists(&self, path: &str) -> Result<bool, PersistenceError> {
        let exists: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM vault_files WHERE path = $1 UNION SELECT 1 FROM notes WHERE id = $1)",
        )
        .bind(path)
        .fetch_one(&self.pool)
        .await?;
        Ok(exists)
    }

    pub async fn pending_embedding_batch(
        &self,
        limit: usize,
        max_failures: i32,
    ) -> Result<Vec<(String, String, String, String)>, PersistenceError> {
        let effective_limit = limit.max(1).min(i32::MAX as usize) as i32;
        let rows = sqlx::query(
            r#"
            SELECT id, path, title, content
            FROM notes
            WHERE embedding IS NULL
              AND embedding_failures < $2
            ORDER BY id
            LIMIT $1
            "#,
        )
        .bind(effective_limit)
        .bind(max_failures)
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter()
            .map(|row| {
                Ok((
                    row.try_get("id")?,
                    row.try_get("path")?,
                    row.try_get("title")?,
                    row.try_get("content")?,
                ))
            })
            .collect::<Result<Vec<_>, sqlx::Error>>()
            .map_err(PersistenceError::Sqlx)
    }

    pub async fn embedding_dimensions(&self) -> Result<Option<usize>, PersistenceError> {
        if let Some(dimensions) = self.embedding_column_dimensions().await? {
            return Ok(Some(dimensions));
        }

        let dimensions: Option<i32> = sqlx::query_scalar(
            r#"
            SELECT vector_dims(embedding)
            FROM notes
            WHERE embedding IS NOT NULL
            LIMIT 1
            "#,
        )
        .fetch_optional(&self.pool)
        .await?
        .flatten();

        Ok(dimensions.map(|value| value.max(0) as usize))
    }

    pub async fn embedding_column_dimensions(&self) -> Result<Option<usize>, PersistenceError> {
        let mut tx = self.pool.begin().await?;
        let dimensions = embedding_column_dimensions_tx(&mut tx).await?;
        tx.commit().await?;
        Ok(dimensions)
    }

    pub async fn search_fulltext_ranking(
        &self,
        query: &str,
        limit: usize,
    ) -> Result<Vec<(String, f32)>, PersistenceError> {
        let effective_limit = limit.max(1).min(i32::MAX as usize) as i32;
        let rows = sqlx::query(
            r#"
            SELECT
                id,
                ts_rank_cd(
                    to_tsvector('english', coalesce(title, '') || ' ' || coalesce(search_text, '')),
                    plainto_tsquery('english', $1)
                ) AS score
            FROM notes
            WHERE to_tsvector('english', coalesce(title, '') || ' ' || coalesce(search_text, ''))
                @@ plainto_tsquery('english', $1)
            ORDER BY score DESC, id ASC
            LIMIT $2
            "#,
        )
        .bind(query)
        .bind(effective_limit)
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter()
            .map(|row| {
                let id: String = row.try_get("id")?;
                let score: f32 = row.try_get("score")?;
                Ok((id, score))
            })
            .collect::<Result<Vec<_>, sqlx::Error>>()
            .map_err(PersistenceError::Sqlx)
    }

    pub async fn search_semantic_ranking(
        &self,
        query_embedding: &[f32],
        limit: usize,
    ) -> Result<Vec<(String, f32)>, PersistenceError> {
        if query_embedding.is_empty() {
            return Ok(Vec::new());
        }

        let effective_limit = limit.max(1).min(i32::MAX as usize) as i32;
        let query_vector = Vector::from(query_embedding.to_vec());
        let rows = sqlx::query(
            r#"
            SELECT
                id,
                (1 - (embedding <=> $1))::real AS score
            FROM notes
            WHERE embedding IS NOT NULL
            ORDER BY embedding <=> $1, id ASC
            LIMIT $2
            "#,
        )
        .bind(query_vector)
        .bind(effective_limit)
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter()
            .map(|row| {
                let id: String = row.try_get("id")?;
                let score: f32 = row.try_get("score")?;
                Ok((id, score))
            })
            .collect::<Result<Vec<_>, sqlx::Error>>()
            .map_err(PersistenceError::Sqlx)
    }

    pub async fn load_search_note_map(
        &self,
        ids: &[String],
    ) -> Result<HashMap<String, PersistedSearchNote>, PersistenceError> {
        if ids.is_empty() {
            return Ok(HashMap::new());
        }

        let rows = sqlx::query(
            r#"
            SELECT id, title, content
            FROM notes
            WHERE id = ANY($1)
            "#,
        )
        .bind(ids)
        .fetch_all(&self.pool)
        .await?;

        let mut map = HashMap::new();
        for row in rows {
            let id: String = row.try_get("id")?;
            let title: String = row.try_get("title")?;
            let content: String = row.try_get("content")?;
            map.insert(id.clone(), PersistedSearchNote { id, title, content });
        }

        Ok(map)
    }

    pub async fn load_note_summaries_by_ids(
        &self,
        ids: &[String],
    ) -> Result<HashMap<String, PersistedNoteSummary>, PersistenceError> {
        if ids.is_empty() {
            return Ok(HashMap::new());
        }

        let rows = sqlx::query(
            r#"
            SELECT
                notes.id,
                notes.title,
                notes.summary,
                notes.updated_at,
                tags.tag
            FROM notes
            LEFT JOIN tags ON tags.note_id = notes.id
            WHERE notes.id = ANY($1)
            ORDER BY notes.id ASC, tags.tag ASC
            "#,
        )
        .bind(ids)
        .fetch_all(&self.pool)
        .await?;

        let mut map = HashMap::new();
        for row in rows {
            let id: String = row.try_get("id")?;
            let title: String = row.try_get("title")?;
            let summary: String = row.try_get("summary")?;
            let updated_at: DateTime<Utc> = row.try_get("updated_at")?;
            let tag: Option<String> = row.try_get("tag")?;

            let entry = map
                .entry(id.clone())
                .or_insert_with(|| PersistedNoteSummary {
                    id,
                    title,
                    summary,
                    updated_at,
                    tags: Vec::new(),
                });

            if let Some(tag) = tag
                && entry.tags.last() != Some(&tag)
            {
                entry.tags.push(tag);
            }
        }

        Ok(map)
    }

    pub async fn load_note_graph_snapshot(
        &self,
    ) -> Result<Vec<PersistedNoteRecord>, PersistenceError> {
        let note_rows = sqlx::query(
            r#"
            SELECT id, path, title, content, search_text, summary, frontmatter,
                   couchdb_rev, created_at, updated_at, indexed_at, embedding
            FROM notes
            ORDER BY id ASC
            "#,
        )
        .fetch_all(&self.pool)
        .await?;

        let mut notes_by_id: HashMap<String, PersistedNoteRecord> = HashMap::new();
        for row in note_rows {
            let embedding: Option<Vector> = row.try_get("embedding")?;
            let id: String = row.try_get("id")?;
            notes_by_id.insert(
                id.clone(),
                PersistedNoteRecord {
                    id,
                    path: row.try_get("path")?,
                    title: row.try_get("title")?,
                    content: row.try_get("content")?,
                    search_text: row.try_get("search_text")?,
                    summary: row.try_get("summary")?,
                    frontmatter: row.try_get("frontmatter")?,
                    tags: Vec::new(),
                    couchdb_rev: row.try_get("couchdb_rev")?,
                    created_at: row.try_get("created_at")?,
                    updated_at: row.try_get("updated_at")?,
                    indexed_at: row.try_get("indexed_at")?,
                    embedding: embedding.map(|v| v.to_vec()),
                    links: Vec::new(),
                },
            );
        }

        let tag_rows = sqlx::query(
            r#"
            SELECT note_id, tag
            FROM tags
            ORDER BY note_id ASC, tag ASC
            "#,
        )
        .fetch_all(&self.pool)
        .await?;
        for row in tag_rows {
            let note_id: String = row.try_get("note_id")?;
            let tag: String = row.try_get("tag")?;
            if let Some(note) = notes_by_id.get_mut(&note_id) {
                note.tags.push(tag);
            }
        }

        let link_rows = sqlx::query(
            r#"
            SELECT source_id, target_id, context_text, position
            FROM links
            ORDER BY source_id ASC, COALESCE(position, 0) ASC, target_id ASC
            "#,
        )
        .fetch_all(&self.pool)
        .await?;
        for row in link_rows {
            let source_id: String = row.try_get("source_id")?;
            let target_id: String = row.try_get("target_id")?;
            let context_text: Option<String> = row.try_get("context_text")?;
            let position: Option<i32> = row.try_get("position")?;

            if let Some(note) = notes_by_id.get_mut(&source_id) {
                note.links.push(PersistedLinkRecord {
                    target_id,
                    context_text: context_text.unwrap_or_default(),
                    position: position.unwrap_or(0).max(0) as usize,
                });
            }
        }

        let mut notes = notes_by_id.into_values().collect::<Vec<_>>();
        notes.sort_by(|a, b| a.id.cmp(&b.id));
        for note in &mut notes {
            note.tags.sort();
            note.tags.dedup();
            note.links.sort_by(|a, b| {
                a.position
                    .cmp(&b.position)
                    .then_with(|| a.target_id.cmp(&b.target_id))
            });
        }

        Ok(notes)
    }

    pub async fn load_note_titles_by_ids(
        &self,
        ids: &[String],
    ) -> Result<HashMap<String, String>, PersistenceError> {
        if ids.is_empty() {
            return Ok(HashMap::new());
        }

        let rows = sqlx::query(
            r#"
            SELECT id, title
            FROM notes
            WHERE id = ANY($1)
            "#,
        )
        .bind(ids)
        .fetch_all(&self.pool)
        .await?;

        let mut map = HashMap::new();
        for row in rows {
            let id: String = row.try_get("id")?;
            let title: String = row.try_get("title")?;
            map.insert(id, title);
        }
        Ok(map)
    }

    pub async fn load_links_between_sets(
        &self,
        source_ids: &[String],
        target_ids: &[String],
    ) -> Result<Vec<PersistedLinkContext>, PersistenceError> {
        if source_ids.is_empty() || target_ids.is_empty() {
            return Ok(Vec::new());
        }

        let rows = sqlx::query(
            r#"
            SELECT
                source_id,
                target_id,
                COALESCE(context_text, '') AS context_text,
                COALESCE(position, 0) AS position
            FROM links
            WHERE source_id = ANY($1) AND target_id = ANY($2)
            ORDER BY source_id ASC, target_id ASC, position ASC
            "#,
        )
        .bind(source_ids)
        .bind(target_ids)
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter()
            .map(|row| {
                let position: i32 = row.try_get("position")?;
                Ok(PersistedLinkContext {
                    source_id: row.try_get("source_id")?,
                    target_id: row.try_get("target_id")?,
                    context_text: row.try_get("context_text")?,
                    position: position.max(0) as usize,
                })
            })
            .collect::<Result<Vec<_>, sqlx::Error>>()
            .map_err(PersistenceError::Sqlx)
    }

    pub async fn pending_chunk_parent_count(&self) -> Result<usize, PersistenceError> {
        let row = sqlx::query(
            r#"
            SELECT COUNT(DISTINCT parent_id) AS pending_parents
            FROM chunk_staging
            "#,
        )
        .fetch_one(&self.pool)
        .await?;
        let pending_parents: i64 = row.try_get("pending_parents")?;
        Ok(pending_parents.max(0) as usize)
    }

    pub async fn orphan_leaf_staging_parent_count(&self) -> Result<usize, PersistenceError> {
        let row = sqlx::query(
            r#"
            SELECT COUNT(DISTINCT cs.parent_id) AS orphan_leaf_parents
            FROM chunk_staging cs
            WHERE cs.parent_id LIKE 'h:%'
              AND NOT EXISTS (
                  SELECT 1
                  FROM file_aliases fa
                  WHERE cs.parent_id = ANY(fa.children)
              )
            "#,
        )
        .fetch_one(&self.pool)
        .await?;
        let orphan_leaf_parents: i64 = row.try_get("orphan_leaf_parents")?;
        Ok(orphan_leaf_parents.max(0) as usize)
    }

    pub async fn stale_file_alias_count(&self) -> Result<usize, PersistenceError> {
        let row = sqlx::query(
            r#"
            SELECT COUNT(*) AS stale_file_aliases
            FROM file_aliases fa
            LEFT JOIN vault_files vf ON vf.path = fa.note_path
            LEFT JOIN notes n ON n.id = fa.note_path
            WHERE vf.path IS NULL
               OR vf.couchdb_rev <> fa.couchdb_rev
               OR (
                    lower(fa.note_path) LIKE '%.md'
                    AND (n.id IS NULL OR n.couchdb_rev <> fa.couchdb_rev)
               )
            "#,
        )
        .fetch_one(&self.pool)
        .await?;
        let stale_file_aliases: i64 = row.try_get("stale_file_aliases")?;
        Ok(stale_file_aliases.max(0) as usize)
    }

    pub async fn quarantined_embedding_count(
        &self,
        max_failures: i32,
    ) -> Result<usize, PersistenceError> {
        let row = sqlx::query(
            r#"
            SELECT COUNT(*) AS quarantined
            FROM notes
            WHERE embedding IS NULL
              AND embedding_failures >= $1
            "#,
        )
        .bind(max_failures)
        .fetch_one(&self.pool)
        .await?;
        let quarantined: i64 = row.try_get("quarantined")?;
        Ok(quarantined.max(0) as usize)
    }

    pub async fn pending_embedding_count(&self) -> Result<usize, PersistenceError> {
        let row = sqlx::query(
            r#"
            SELECT COUNT(*) AS pending
            FROM notes
            WHERE embedding IS NULL
            "#,
        )
        .fetch_one(&self.pool)
        .await?;
        let pending: i64 = row.try_get("pending")?;
        Ok(pending.max(0) as usize)
    }

    pub async fn set_embeddings(
        &self,
        updates: Vec<(String, Vec<f32>)>,
    ) -> Result<(), PersistenceError> {
        if updates.is_empty() {
            return Ok(());
        }

        let mut tx = self.pool.begin().await?;
        let now = Utc::now();
        for (note_id, embedding) in updates {
            sqlx::query(
                r#"
                UPDATE notes
                SET embedding = $2,
                    indexed_at = $3,
                    embedding_failures = 0,
                    embedding_failed_at = NULL
                WHERE id = $1
                "#,
            )
            .bind(note_id)
            .bind(Vector::from(embedding))
            .bind(now)
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        Ok(())
    }

    pub async fn record_embedding_failure(&self, note_id: &str) -> Result<(), PersistenceError> {
        sqlx::query(
            r#"
            UPDATE notes
            SET embedding_failures = embedding_failures + 1,
                embedding_failed_at = now()
            WHERE id = $1
            "#,
        )
        .bind(note_id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn upsert_sync_state(
        &self,
        last_seq: &str,
        couchdb_current_seq: &str,
        updated_at: DateTime<Utc>,
    ) -> Result<(), PersistenceError> {
        let sync_state = PersistedSyncState {
            last_seq: last_seq.to_string(),
            couchdb_current_seq: couchdb_current_seq.to_string(),
            updated_at,
        };
        let mut tx = self.pool.begin().await?;
        upsert_sync_state_tx(&mut tx, &sync_state).await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn load_sync_state(&self) -> Result<Option<PersistedSyncState>, PersistenceError> {
        match sqlx::query(
            "SELECT last_seq, couchdb_current_seq, updated_at FROM sync_state WHERE id = 1",
        )
        .fetch_optional(&self.pool)
        .await?
        {
            Some(row) => Ok(Some(PersistedSyncState {
                last_seq: row.try_get("last_seq")?,
                couchdb_current_seq: row.try_get("couchdb_current_seq")?,
                updated_at: row.try_get("updated_at")?,
            })),
            None => Ok(None),
        }
    }

    pub async fn enqueue_recovery_targets(
        &self,
        recovery_kind: &str,
        target_ids: &[String],
    ) -> Result<(), PersistenceError> {
        if target_ids.is_empty() {
            return Ok(());
        }
        sqlx::query(
            r#"
            INSERT INTO sync_recovery_queue (recovery_kind, target_id)
            SELECT $1, target_id
            FROM unnest($2::text[]) AS target_id
            ON CONFLICT (recovery_kind, target_id) DO NOTHING
            "#,
        )
        .bind(recovery_kind)
        .bind(target_ids)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn due_recovery_targets(
        &self,
        limit: usize,
        now: DateTime<Utc>,
    ) -> Result<Vec<PersistedRecoveryTarget>, PersistenceError> {
        let rows = sqlx::query(
            r#"
            SELECT recovery_kind, target_id, failure_count
            FROM sync_recovery_queue
            WHERE quarantined_at IS NULL
              AND next_retry_at <= $1
            ORDER BY next_retry_at ASC, recovery_kind ASC, target_id ASC
            LIMIT $2
            "#,
        )
        .bind(now)
        .bind(limit.max(1).min(i64::MAX as usize) as i64)
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter()
            .map(|row| {
                let failure_count: i32 = row.try_get("failure_count")?;
                Ok(PersistedRecoveryTarget {
                    recovery_kind: row.try_get("recovery_kind")?,
                    target_id: row.try_get("target_id")?,
                    failure_count: failure_count.max(0) as usize,
                })
            })
            .collect::<Result<Vec<_>, sqlx::Error>>()
            .map_err(PersistenceError::Sqlx)
    }

    pub async fn resolve_recovery_target(
        &self,
        recovery_kind: &str,
        target_id: &str,
    ) -> Result<(), PersistenceError> {
        sqlx::query("DELETE FROM sync_recovery_queue WHERE recovery_kind = $1 AND target_id = $2")
            .bind(recovery_kind)
            .bind(target_id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn fail_recovery_target(
        &self,
        recovery_kind: &str,
        target_id: &str,
        next_retry_at: DateTime<Utc>,
        max_failures: usize,
        failure_kind: &str,
    ) -> Result<bool, PersistenceError> {
        let row = sqlx::query(
            r#"
            UPDATE sync_recovery_queue
            SET failure_count = failure_count + 1,
                next_retry_at = $3,
                quarantined_at = CASE
                    WHEN failure_count + 1 >= $4 THEN now()
                    ELSE NULL
                END,
                last_failure_kind = $5,
                updated_at = now()
            WHERE recovery_kind = $1 AND target_id = $2
            RETURNING quarantined_at IS NOT NULL AS quarantined
            "#,
        )
        .bind(recovery_kind)
        .bind(target_id)
        .bind(next_retry_at)
        .bind(max_failures.max(1).min(i32::MAX as usize) as i32)
        .bind(failure_kind)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row
            .map(|row| row.try_get::<bool, _>("quarantined"))
            .transpose()?
            .unwrap_or(false))
    }

    pub async fn clear_recovery_targets_not_in(
        &self,
        recovery_kind: &str,
        active_target_ids: &[String],
    ) -> Result<(), PersistenceError> {
        sqlx::query(
            r#"
            DELETE FROM sync_recovery_queue
            WHERE recovery_kind = $1
              AND NOT (target_id = ANY($2::text[]))
            "#,
        )
        .bind(recovery_kind)
        .bind(active_target_ids)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn recovery_queue_stats(&self) -> Result<RecoveryQueueStats, PersistenceError> {
        let row = sqlx::query(
            r#"
            SELECT
                COUNT(*) FILTER (WHERE quarantined_at IS NULL) AS pending,
                COUNT(*) FILTER (WHERE quarantined_at IS NOT NULL) AS quarantined
            FROM sync_recovery_queue
            "#,
        )
        .fetch_one(&self.pool)
        .await?;
        let pending: i64 = row.try_get("pending")?;
        let quarantined: i64 = row.try_get("quarantined")?;
        Ok(RecoveryQueueStats {
            pending: pending.max(0) as usize,
            quarantined: quarantined.max(0) as usize,
        })
    }

    pub async fn log_access(
        &self,
        entry: &PersistedAccessLogEntry,
    ) -> Result<(), PersistenceError> {
        let filtered = entry.notes_filtered_count.min(i32::MAX as usize) as i32;
        sqlx::query(
            r#"
            INSERT INTO access_log (timestamp, context, endpoint, query_params, notes_returned, notes_filtered_count)
            VALUES ($1, $2, $3, $4, $5, $6)
            "#,
        )
        .bind(entry.timestamp)
        .bind(&entry.context)
        .bind(&entry.endpoint)
        .bind(&entry.query_params)
        .bind(&entry.notes_returned)
        .bind(filtered)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn prune_access_log(&self, retention_days: u64) -> Result<(), PersistenceError> {
        if retention_days == 0 {
            return Ok(());
        }

        let days = retention_days.min(i32::MAX as u64) as i32;
        sqlx::query(
            r#"
            DELETE FROM access_log
            WHERE timestamp < now() - make_interval(days => $1)
            "#,
        )
        .bind(days)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn load_snapshot(&self) -> Result<PersistedSnapshot, PersistenceError> {
        let mut tx = self.pool.begin().await?;
        sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            .execute(&mut *tx)
            .await?;

        let note_rows = sqlx::query(
            r#"
            SELECT id, path, title, content, search_text, summary, frontmatter,
                   couchdb_rev, created_at, updated_at, indexed_at, embedding
            FROM notes
            "#,
        )
        .fetch_all(&mut *tx)
        .await?;

        let mut notes_by_id: HashMap<String, PersistedNoteRecord> = HashMap::new();
        for row in note_rows {
            let embedding: Option<Vector> = row.try_get("embedding")?;
            let id: String = row.try_get("id")?;
            notes_by_id.insert(
                id.clone(),
                PersistedNoteRecord {
                    id,
                    path: row.try_get("path")?,
                    title: row.try_get("title")?,
                    content: row.try_get("content")?,
                    search_text: row.try_get("search_text")?,
                    summary: row.try_get("summary")?,
                    frontmatter: row.try_get("frontmatter")?,
                    tags: Vec::new(),
                    couchdb_rev: row.try_get("couchdb_rev")?,
                    created_at: row.try_get("created_at")?,
                    updated_at: row.try_get("updated_at")?,
                    indexed_at: row.try_get("indexed_at")?,
                    embedding: embedding.map(|v| v.to_vec()),
                    links: Vec::new(),
                },
            );
        }

        let tag_rows = sqlx::query("SELECT note_id, tag FROM tags")
            .fetch_all(&mut *tx)
            .await?;
        for row in tag_rows {
            let note_id: String = row.try_get("note_id")?;
            let tag: String = row.try_get("tag")?;
            if let Some(note) = notes_by_id.get_mut(&note_id) {
                note.tags.push(tag);
            }
        }

        let link_rows =
            sqlx::query("SELECT source_id, target_id, context_text, position FROM links")
                .fetch_all(&mut *tx)
                .await?;
        for row in link_rows {
            let source_id: String = row.try_get("source_id")?;
            let target_id: String = row.try_get("target_id")?;
            let context_text: Option<String> = row.try_get("context_text")?;
            let position: Option<i32> = row.try_get("position")?;

            if let Some(note) = notes_by_id.get_mut(&source_id) {
                note.links.push(PersistedLinkRecord {
                    target_id,
                    context_text: context_text.unwrap_or_default(),
                    position: position.unwrap_or(0).max(0) as usize,
                });
            }
        }

        for note in notes_by_id.values_mut() {
            note.tags.sort();
            note.tags.dedup();
            note.links.sort_by(|a, b| {
                a.position
                    .cmp(&b.position)
                    .then_with(|| a.target_id.cmp(&b.target_id))
            });
        }

        let sync_state = match sqlx::query(
            "SELECT last_seq, couchdb_current_seq, updated_at FROM sync_state WHERE id = 1",
        )
        .fetch_optional(&mut *tx)
        .await?
        {
            Some(row) => Some(PersistedSyncState {
                last_seq: row.try_get("last_seq")?,
                couchdb_current_seq: row.try_get("couchdb_current_seq")?,
                updated_at: row.try_get("updated_at")?,
            }),
            None => None,
        };

        let chunk_rows = sqlx::query(
            r#"
            SELECT parent_id, chunk_index, chunk_count, content, couchdb_rev, received_at
            FROM chunk_staging
            "#,
        )
        .fetch_all(&mut *tx)
        .await?;
        let mut staged_chunks = chunk_rows
            .into_iter()
            .map(|row| {
                let chunk_index: i32 = row.try_get("chunk_index")?;
                let chunk_count: i32 = row.try_get("chunk_count")?;
                Ok(PersistedStagedChunk {
                    parent_id: row.try_get("parent_id")?,
                    chunk_index: chunk_index.max(0) as usize,
                    chunk_count: chunk_count.max(1) as usize,
                    content: row.try_get("content")?,
                    couchdb_rev: row.try_get("couchdb_rev")?,
                    received_at: row.try_get("received_at")?,
                })
            })
            .collect::<Result<Vec<_>, sqlx::Error>>()?;
        staged_chunks.sort_by(|a, b| {
            a.parent_id
                .cmp(&b.parent_id)
                .then_with(|| a.chunk_index.cmp(&b.chunk_index))
        });

        let alias_rows = sqlx::query(
            r#"
            SELECT file_doc_id, note_path, couchdb_rev, children, ctime, mtime
            FROM file_aliases
            ORDER BY file_doc_id ASC
            "#,
        )
        .fetch_all(&mut *tx)
        .await?;
        let file_aliases = alias_rows
            .into_iter()
            .map(|row| {
                Ok(PersistedFileAlias {
                    file_doc_id: row.try_get("file_doc_id")?,
                    note_path: row.try_get("note_path")?,
                    couchdb_rev: row.try_get("couchdb_rev")?,
                    children: row.try_get("children")?,
                    ctime: row.try_get("ctime")?,
                    mtime: row.try_get("mtime")?,
                })
            })
            .collect::<Result<Vec<_>, sqlx::Error>>()?;

        let vault_file_rows = sqlx::query(
            r#"
            SELECT path, content, couchdb_rev, created_at, updated_at, indexed_at
            FROM vault_files
            ORDER BY path ASC
            "#,
        )
        .fetch_all(&mut *tx)
        .await?;
        let vault_files = vault_file_rows
            .into_iter()
            .map(|row| {
                Ok(PersistedVaultFile {
                    path: row.try_get("path")?,
                    content: row.try_get("content")?,
                    couchdb_rev: row.try_get("couchdb_rev")?,
                    created_at: row.try_get("created_at")?,
                    updated_at: row.try_get("updated_at")?,
                    indexed_at: row.try_get("indexed_at")?,
                })
            })
            .collect::<Result<Vec<_>, sqlx::Error>>()?;

        let generation: i64 = sqlx::query_scalar("SELECT generation FROM store_state WHERE id = 1")
            .fetch_optional(&mut *tx)
            .await?
            .unwrap_or(0);
        tx.commit().await?;

        Ok(PersistedSnapshot {
            generation: generation.max(0) as u64,
            notes: notes_by_id.into_values().collect(),
            sync_state,
            staged_chunks,
            file_aliases,
            vault_files,
        })
    }

    // ------------------------------------------------------------------
    // Block persistence
    // ------------------------------------------------------------------

    pub async fn sync_blocks_for_note(
        &self,
        note_id: &str,
        blocks: &[crate::markdown::MarkdownBlock],
        breadcrumbs: &[String],
    ) -> Result<SyncBlocksResult, PersistenceError> {
        use sha2::{Digest, Sha256};

        let mut tx = self.pool.begin().await?;
        let now = Utc::now();

        // Load existing blocks for this note.
        let existing_rows = sqlx::query("SELECT id, content_hash FROM blocks WHERE note_id = $1")
            .bind(note_id)
            .fetch_all(&mut *tx)
            .await?;

        let mut existing: HashMap<String, String> = HashMap::new();
        for row in &existing_rows {
            let id: String = row.try_get("id")?;
            let hash: String = row.try_get("content_hash")?;
            existing.insert(id, hash);
        }

        let mut new_ids = std::collections::HashSet::new();
        let mut inserted = 0usize;
        let mut updated = 0usize;

        for (block, breadcrumb) in blocks.iter().zip(breadcrumbs.iter()) {
            let block_id = format!("{note_id}##{}", block.block_index);
            new_ids.insert(block_id.clone());

            let heading_path_str = block
                .heading_path
                .iter()
                .map(|h| format!("{} {}", "#".repeat(h.level as usize), h.text))
                .collect::<Vec<_>>()
                .join(" > ");

            let mut hasher = Sha256::new();
            hasher.update(block.content.as_bytes());
            let content_hash = format!("{:x}", hasher.finalize());

            let block_index_i32 = block.block_index.min(i32::MAX as usize) as i32;

            if let Some(old_hash) = existing.get(&block_id) {
                if *old_hash == content_hash {
                    // Content unchanged — skip.
                    continue;
                }
                // Content changed — update and null embedding.
                sqlx::query(
                    r#"
                    UPDATE blocks SET
                        heading_path = $2,
                        breadcrumb = $3,
                        content = $4,
                        content_hash = $5,
                        embedding = NULL,
                        embedding_failures = 0,
                        embedding_failed_at = NULL,
                        last_embedding_error = NULL,
                        updated_at = $6
                    WHERE id = $1
                    "#,
                )
                .bind(&block_id)
                .bind(&heading_path_str)
                .bind(breadcrumb)
                .bind(&block.content)
                .bind(&content_hash)
                .bind(now)
                .execute(&mut *tx)
                .await?;
                updated += 1;
            } else {
                sqlx::query(
                    r#"
                    INSERT INTO blocks (id, note_id, block_index, heading_path, breadcrumb, content, content_hash, created_at, updated_at)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                    "#,
                )
                .bind(&block_id)
                .bind(note_id)
                .bind(block_index_i32)
                .bind(&heading_path_str)
                .bind(breadcrumb)
                .bind(&block.content)
                .bind(&content_hash)
                .bind(now)
                .bind(now)
                .execute(&mut *tx)
                .await?;
                inserted += 1;
            }
        }

        // Delete blocks that no longer exist.
        let mut deleted = 0usize;
        for old_id in existing.keys() {
            if !new_ids.contains(old_id) {
                sqlx::query("DELETE FROM blocks WHERE id = $1")
                    .bind(old_id)
                    .execute(&mut *tx)
                    .await?;
                deleted += 1;
            }
        }

        tx.commit().await?;

        Ok(SyncBlocksResult {
            inserted,
            updated,
            deleted,
        })
    }

    pub async fn pending_block_embedding_batch(
        &self,
        limit: usize,
        max_failures: i32,
    ) -> Result<Vec<BlockEmbeddingCandidate>, PersistenceError> {
        let effective_limit = limit.max(1).min(i32::MAX as usize) as i32;
        let rows = sqlx::query(
            r#"
            SELECT id, breadcrumb || E'\n' || content AS text
            FROM blocks
            WHERE embedding IS NULL
              AND embedding_failures < $2
            ORDER BY note_id, block_index
            LIMIT $1
            "#,
        )
        .bind(effective_limit)
        .bind(max_failures)
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter()
            .map(|row| {
                Ok(BlockEmbeddingCandidate {
                    id: row.try_get("id")?,
                    text: row.try_get("text")?,
                })
            })
            .collect::<Result<Vec<_>, sqlx::Error>>()
            .map_err(PersistenceError::Sqlx)
    }

    pub async fn set_block_embeddings(
        &self,
        updates: Vec<(String, Vec<f32>)>,
    ) -> Result<(), PersistenceError> {
        if updates.is_empty() {
            return Ok(());
        }

        let mut tx = self.pool.begin().await?;
        let now = Utc::now();
        for (block_id, embedding) in updates {
            sqlx::query(
                r#"
                UPDATE blocks
                SET embedding = $2,
                    updated_at = $3,
                    embedding_failures = 0,
                    embedding_failed_at = NULL,
                    last_embedding_error = NULL
                WHERE id = $1
                "#,
            )
            .bind(block_id)
            .bind(Vector::from(embedding))
            .bind(now)
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        Ok(())
    }

    pub async fn record_embedding_runtime_success(&self) -> Result<(), PersistenceError> {
        sqlx::query(
            r#"
            INSERT INTO embedding_runtime (id, last_success_at, updated_at)
            VALUES (1, now(), now())
            ON CONFLICT (id)
            DO UPDATE SET
                last_success_at = EXCLUDED.last_success_at,
                updated_at = EXCLUDED.updated_at
            "#,
        )
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn record_embedding_runtime_error(
        &self,
        error: &str,
    ) -> Result<(), PersistenceError> {
        sqlx::query(
            r#"
            INSERT INTO embedding_runtime (id, last_error_at, last_error, updated_at)
            VALUES (1, now(), $1, now())
            ON CONFLICT (id)
            DO UPDATE SET
                last_error_at = EXCLUDED.last_error_at,
                last_error = EXCLUDED.last_error,
                updated_at = EXCLUDED.updated_at
            "#,
        )
        .bind(truncate_error(error))
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn record_block_embedding_failure(
        &self,
        block_id: &str,
        error: &str,
    ) -> Result<(), PersistenceError> {
        sqlx::query(
            r#"
            UPDATE blocks
            SET embedding_failures = embedding_failures + 1,
                embedding_failed_at = now(),
                last_embedding_error = $2
            WHERE id = $1
            "#,
        )
        .bind(block_id)
        .bind(truncate_error(error))
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn block_embedding_stats(
        &self,
        max_failures: i32,
    ) -> Result<BlockEmbeddingStats, PersistenceError> {
        let pending: i64 = sqlx::query_scalar(
            r#"
            SELECT COUNT(*)
            FROM blocks
            WHERE embedding IS NULL
              AND embedding_failures < $1
            "#,
        )
        .bind(max_failures)
        .fetch_one(&self.pool)
        .await?;

        let quarantined: i64 = sqlx::query_scalar(
            r#"
            SELECT COUNT(*)
            FROM blocks
            WHERE embedding IS NULL
              AND embedding_failures >= $1
            "#,
        )
        .bind(max_failures)
        .fetch_one(&self.pool)
        .await?;

        let last_success_at: Option<DateTime<Utc>> = sqlx::query_scalar(
            r#"
            SELECT max(ts)
            FROM (
                SELECT indexed_at AS ts FROM notes WHERE embedding IS NOT NULL
                UNION ALL
                SELECT updated_at AS ts FROM blocks WHERE embedding IS NOT NULL
                UNION ALL
                SELECT last_success_at AS ts
                FROM embedding_runtime
                WHERE last_success_at IS NOT NULL
            ) successes
            "#,
        )
        .fetch_one(&self.pool)
        .await?;

        let last_error_at: Option<DateTime<Utc>> = sqlx::query_scalar(
            r#"
            SELECT max(ts)
            FROM (
                SELECT embedding_failed_at AS ts FROM notes WHERE embedding_failed_at IS NOT NULL
                UNION ALL
                SELECT embedding_failed_at AS ts FROM blocks WHERE embedding_failed_at IS NOT NULL
                UNION ALL
                SELECT last_error_at AS ts
                FROM embedding_runtime
                WHERE last_error_at IS NOT NULL
            ) failures
            "#,
        )
        .fetch_one(&self.pool)
        .await?;

        let last_error: Option<String> = sqlx::query_scalar(
            r#"
            SELECT last_error
            FROM (
                SELECT last_embedding_error AS last_error, embedding_failed_at AS ts
                FROM blocks
                WHERE last_embedding_error IS NOT NULL
                UNION ALL
                SELECT last_error, last_error_at AS ts
                FROM embedding_runtime
                WHERE last_error IS NOT NULL
            ) errors
            ORDER BY ts DESC NULLS LAST
            LIMIT 1
            "#,
        )
        .fetch_optional(&self.pool)
        .await?;

        Ok(BlockEmbeddingStats {
            pending: pending.max(0) as usize,
            quarantined: quarantined.max(0) as usize,
            last_success_at,
            last_error_at,
            last_error,
        })
    }

    pub async fn search_block_semantic_ranking(
        &self,
        query_embedding: &[f32],
        limit: usize,
    ) -> Result<Vec<BlockSemanticMatch>, PersistenceError> {
        if query_embedding.is_empty() {
            return Ok(Vec::new());
        }

        let effective_limit = limit.max(1).min(i32::MAX as usize) as i32;
        let query_vector = Vector::from(query_embedding.to_vec());
        let rows = sqlx::query(
            r#"
            SELECT
                id,
                note_id,
                heading_path,
                content,
                (1 - (embedding <=> $1))::real AS score
            FROM blocks
            WHERE embedding IS NOT NULL
            ORDER BY embedding <=> $1
            LIMIT $2
            "#,
        )
        .bind(query_vector)
        .bind(effective_limit)
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter()
            .map(|row| {
                let id: String = row.try_get("id")?;
                let note_id: String = row.try_get("note_id")?;
                let heading_path: String = row.try_get("heading_path")?;
                let content: String = row.try_get("content")?;
                let score: f32 = row.try_get("score")?;
                Ok(BlockSemanticMatch {
                    block_id: id,
                    note_id,
                    heading_path,
                    content,
                    score,
                })
            })
            .collect::<Result<Vec<_>, sqlx::Error>>()
            .map_err(PersistenceError::Sqlx)
    }

    pub async fn load_all_notes_for_block_reindex(
        &self,
    ) -> Result<Vec<(String, String, String, String)>, PersistenceError> {
        let rows = sqlx::query(
            r#"
            SELECT id, path, title, content
            FROM notes
            ORDER BY id
            "#,
        )
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter()
            .map(|row| {
                Ok((
                    row.try_get("id")?,
                    row.try_get("path")?,
                    row.try_get("title")?,
                    row.try_get("content")?,
                ))
            })
            .collect::<Result<Vec<_>, sqlx::Error>>()
            .map_err(PersistenceError::Sqlx)
    }

    /// Backfill: load notes that have no blocks yet.
    pub async fn notes_without_blocks(
        &self,
    ) -> Result<Vec<(String, String, String, String)>, PersistenceError> {
        let rows = sqlx::query(
            r#"
            SELECT id, path, title, content
            FROM notes
            WHERE id NOT IN (SELECT DISTINCT note_id FROM blocks)
            ORDER BY id
            "#,
        )
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter()
            .map(|row| {
                Ok((
                    row.try_get("id")?,
                    row.try_get("path")?,
                    row.try_get("title")?,
                    row.try_get("content")?,
                ))
            })
            .collect::<Result<Vec<_>, sqlx::Error>>()
            .map_err(PersistenceError::Sqlx)
    }

    /// Null out all note embeddings so they get re-embedded with breadcrumbs.
    pub async fn clear_all_note_embeddings(&self) -> Result<usize, PersistenceError> {
        let result = sqlx::query(
            r#"
            UPDATE notes
            SET embedding = NULL,
                embedding_failures = 0,
                embedding_failed_at = NULL
            WHERE embedding IS NOT NULL
               OR embedding_failures <> 0
               OR embedding_failed_at IS NOT NULL
            "#,
        )
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected() as usize)
    }

    pub async fn unblock_embeddings(
        &self,
        selector: &EmbeddingUnblockSelector,
        dry_run: bool,
    ) -> Result<EmbeddingUnblockResult, PersistenceError> {
        let note_ids = self.unblock_note_ids(selector).await?;
        let block_ids = self.unblock_block_ids(selector).await?;

        let mut result = EmbeddingUnblockResult {
            dry_run,
            notes_matched: note_ids.len(),
            blocks_matched: block_ids.len(),
            ..Default::default()
        };

        if dry_run {
            return Ok(result);
        }

        if !note_ids.is_empty() {
            let updated = sqlx::query(
                r#"
                UPDATE notes
                SET embedding_failures = 0,
                    embedding_failed_at = NULL
                WHERE id = ANY($1)
                "#,
            )
            .bind(&note_ids)
            .execute(&self.pool)
            .await?;
            result.notes_reset = updated.rows_affected() as usize;
        }

        if !block_ids.is_empty() {
            let updated = sqlx::query(
                r#"
                UPDATE blocks
                SET embedding_failures = 0,
                    embedding_failed_at = NULL,
                    last_embedding_error = NULL
                WHERE id = ANY($1)
                "#,
            )
            .bind(&block_ids)
            .execute(&self.pool)
            .await?;
            result.blocks_reset = updated.rows_affected() as usize;
        }

        Ok(result)
    }

    async fn unblock_note_ids(
        &self,
        selector: &EmbeddingUnblockSelector,
    ) -> Result<Vec<String>, PersistenceError> {
        let limit = selector.limit.unwrap_or(usize::MAX).min(i32::MAX as usize) as i32;

        let rows = if let Some(note_id) = selector.note_id.as_deref() {
            sqlx::query(
                r#"
                SELECT id
                FROM notes
                WHERE id = $1
                  AND embedding_failures > 0
                ORDER BY id
                LIMIT $2
                "#,
            )
            .bind(note_id)
            .bind(limit)
            .fetch_all(&self.pool)
            .await?
        } else if let Some(path_prefix) = selector.path_prefix.as_deref() {
            sqlx::query(
                r#"
                SELECT id
                FROM notes
                WHERE path LIKE ($1 || '%')
                  AND embedding_failures > 0
                ORDER BY id
                LIMIT $2
                "#,
            )
            .bind(path_prefix)
            .bind(limit)
            .fetch_all(&self.pool)
            .await?
        } else if selector.block_id.is_some() {
            Vec::new()
        } else if selector.all {
            sqlx::query(
                r#"
                SELECT id
                FROM notes
                WHERE embedding_failures > 0
                ORDER BY id
                LIMIT $1
                "#,
            )
            .bind(limit)
            .fetch_all(&self.pool)
            .await?
        } else {
            Vec::new()
        };

        rows.into_iter()
            .map(|row| row.try_get("id").map_err(PersistenceError::Sqlx))
            .collect()
    }

    async fn unblock_block_ids(
        &self,
        selector: &EmbeddingUnblockSelector,
    ) -> Result<Vec<String>, PersistenceError> {
        let limit = selector.limit.unwrap_or(usize::MAX).min(i32::MAX as usize) as i32;

        let rows = if let Some(block_id) = selector.block_id.as_deref() {
            sqlx::query(
                r#"
                SELECT id
                FROM blocks
                WHERE id = $1
                  AND embedding_failures > 0
                ORDER BY id
                LIMIT $2
                "#,
            )
            .bind(block_id)
            .bind(limit)
            .fetch_all(&self.pool)
            .await?
        } else if let Some(note_id) = selector.note_id.as_deref() {
            sqlx::query(
                r#"
                SELECT id
                FROM blocks
                WHERE note_id = $1
                  AND embedding_failures > 0
                ORDER BY note_id, block_index
                LIMIT $2
                "#,
            )
            .bind(note_id)
            .bind(limit)
            .fetch_all(&self.pool)
            .await?
        } else if let Some(path_prefix) = selector.path_prefix.as_deref() {
            sqlx::query(
                r#"
                SELECT blocks.id
                FROM blocks
                JOIN notes ON notes.id = blocks.note_id
                WHERE notes.path LIKE ($1 || '%')
                  AND blocks.embedding_failures > 0
                ORDER BY blocks.note_id, blocks.block_index
                LIMIT $2
                "#,
            )
            .bind(path_prefix)
            .bind(limit)
            .fetch_all(&self.pool)
            .await?
        } else if selector.all {
            sqlx::query(
                r#"
                SELECT id
                FROM blocks
                WHERE embedding_failures > 0
                ORDER BY note_id, block_index
                LIMIT $1
                "#,
            )
            .bind(limit)
            .fetch_all(&self.pool)
            .await?
        } else {
            Vec::new()
        };

        rows.into_iter()
            .map(|row| row.try_get("id").map_err(PersistenceError::Sqlx))
            .collect()
    }

    #[cfg(test)]
    pub async fn reset_for_test(&self) -> Result<(), PersistenceError> {
        sqlx::query(
            "TRUNCATE TABLE access_log, api_keys, links, tags, blocks, notes, vault_files, sync_state, store_state, sync_recovery_queue, chunk_staging, file_aliases RESTART IDENTITY CASCADE",
        )
        .execute(&self.pool)
        .await?;
        Ok(())
    }
}

fn create_hnsw_embedding_index_sql(m: i32, ef_construction: i32) -> String {
    format!(
        "CREATE INDEX IF NOT EXISTS idx_notes_embedding ON notes USING hnsw (embedding vector_cosine_ops) WITH (m = {m}, ef_construction = {ef_construction})"
    )
}

fn create_hnsw_block_embedding_index_sql(m: i32, ef_construction: i32) -> String {
    format!(
        "CREATE INDEX IF NOT EXISTS idx_blocks_embedding ON blocks USING hnsw (embedding vector_cosine_ops) WITH (m = {m}, ef_construction = {ef_construction})"
    )
}

fn truncate_error(error: &str) -> String {
    truncate_utf8_to_byte_limit(error, 500).to_string()
}

async fn embedding_column_dimensions_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<Option<usize>, sqlx::Error> {
    embedding_column_dimensions_for_table_tx(tx, "notes").await
}

async fn embedding_column_dimensions_for_table_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    table_name: &str,
) -> Result<Option<usize>, sqlx::Error> {
    let formatted_type: Option<String> = sqlx::query_scalar(
        r#"
        SELECT format_type(atttypid, atttypmod)
        FROM pg_attribute
        WHERE attrelid = to_regclass($1)
          AND attname = 'embedding'
          AND NOT attisdropped
        "#,
    )
    .bind(table_name)
    .fetch_optional(&mut **tx)
    .await?;

    Ok(formatted_type
        .as_deref()
        .and_then(parse_vector_dimensions_from_format_type))
}

fn parse_vector_dimensions_from_format_type(formatted_type: &str) -> Option<usize> {
    let dimensions = formatted_type
        .trim()
        .strip_prefix("vector(")?
        .strip_suffix(')')?
        .trim()
        .parse::<usize>()
        .ok()?;

    (dimensions > 0).then_some(dimensions)
}

async fn upsert_vault_file_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    file: &PersistedVaultFile,
) -> Result<(), PersistenceError> {
    sqlx::query(
        r#"
        INSERT INTO vault_files (path, content, couchdb_rev, created_at, updated_at, indexed_at)
        VALUES ($1, $2, $3, $4, $5, $6)
        ON CONFLICT (path)
        DO UPDATE SET
            content = EXCLUDED.content,
            couchdb_rev = EXCLUDED.couchdb_rev,
            created_at = EXCLUDED.created_at,
            updated_at = EXCLUDED.updated_at,
            indexed_at = EXCLUDED.indexed_at
        "#,
    )
    .bind(&file.path)
    .bind(&file.content)
    .bind(&file.couchdb_rev)
    .bind(file.created_at)
    .bind(file.updated_at)
    .bind(file.indexed_at)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

async fn delete_vault_file_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    path: &str,
) -> Result<(), PersistenceError> {
    sqlx::query("DELETE FROM vault_files WHERE path = $1")
        .bind(path)
        .execute(&mut **tx)
        .await?;
    Ok(())
}

async fn bump_store_generation_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<u64, PersistenceError> {
    let generation: i64 = sqlx::query_scalar(
        r#"
        INSERT INTO store_state (id, generation)
        VALUES (1, 1)
        ON CONFLICT (id)
        DO UPDATE SET generation = store_state.generation + 1
        RETURNING generation
        "#,
    )
    .fetch_one(&mut **tx)
    .await?;
    Ok(generation.max(0) as u64)
}

async fn upsert_note_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    note: &PersistedNoteRecord,
) -> Result<(), PersistenceError> {
    let bounded_search_text =
        truncate_utf8_to_byte_limit(&note.search_text, MAX_INDEXED_SEARCH_TEXT_BYTES);

    sqlx::query(
        r#"
        INSERT INTO notes (
            id, path, title, content, search_text, summary, frontmatter,
            embedding, couchdb_rev, created_at, updated_at, indexed_at,
            embedding_failures, embedding_failed_at
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, 0, NULL)
        ON CONFLICT (id)
        DO UPDATE SET
            path = EXCLUDED.path,
            title = EXCLUDED.title,
            content = EXCLUDED.content,
            search_text = EXCLUDED.search_text,
            summary = EXCLUDED.summary,
            frontmatter = EXCLUDED.frontmatter,
            embedding = EXCLUDED.embedding,
            couchdb_rev = EXCLUDED.couchdb_rev,
            created_at = EXCLUDED.created_at,
            updated_at = EXCLUDED.updated_at,
            indexed_at = EXCLUDED.indexed_at,
            embedding_failures = 0,
            embedding_failed_at = NULL
        "#,
    )
    .bind(&note.id)
    .bind(&note.path)
    .bind(&note.title)
    .bind(&note.content)
    .bind(bounded_search_text)
    .bind(&note.summary)
    .bind(&note.frontmatter)
    .bind(note.embedding.as_ref().map(|v| Vector::from(v.clone())))
    .bind(&note.couchdb_rev)
    .bind(note.created_at)
    .bind(note.updated_at)
    .bind(note.indexed_at)
    .execute(&mut **tx)
    .await?;

    sqlx::query("DELETE FROM links WHERE source_id = $1")
        .bind(&note.id)
        .execute(&mut **tx)
        .await?;

    for link in &note.links {
        let position = link.position.min(i32::MAX as usize) as i32;
        sqlx::query(
            r#"
            INSERT INTO links (source_id, target_id, context_text, position)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (source_id, target_id)
            DO UPDATE SET
                context_text = EXCLUDED.context_text,
                position = EXCLUDED.position
            "#,
        )
        .bind(&note.id)
        .bind(&link.target_id)
        .bind(&link.context_text)
        .bind(position)
        .execute(&mut **tx)
        .await?;
    }

    sqlx::query("DELETE FROM tags WHERE note_id = $1")
        .bind(&note.id)
        .execute(&mut **tx)
        .await?;

    for tag in &note.tags {
        sqlx::query("INSERT INTO tags (note_id, tag) VALUES ($1, $2) ON CONFLICT DO NOTHING")
            .bind(&note.id)
            .bind(tag)
            .execute(&mut **tx)
            .await?;
    }

    Ok(())
}

fn truncate_utf8_to_byte_limit(value: &str, max_bytes: usize) -> &str {
    if value.len() <= max_bytes {
        return value;
    }

    let mut end = max_bytes;
    while end > 0 && !value.is_char_boundary(end) {
        end -= 1;
    }
    &value[..end]
}

async fn upsert_sync_state_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    sync_state: &PersistedSyncState,
) -> Result<(), PersistenceError> {
    sqlx::query(
        r#"
        INSERT INTO sync_state (id, last_seq, couchdb_current_seq, updated_at)
        VALUES (1, $1, $2, $3)
        ON CONFLICT (id)
        DO UPDATE SET
            last_seq = EXCLUDED.last_seq,
            couchdb_current_seq = EXCLUDED.couchdb_current_seq,
            updated_at = EXCLUDED.updated_at
        "#,
    )
    .bind(&sync_state.last_seq)
    .bind(&sync_state.couchdb_current_seq)
    .bind(sync_state.updated_at)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{parse_vector_dimensions_from_format_type, truncate_utf8_to_byte_limit};

    #[test]
    fn truncate_utf8_to_byte_limit_returns_original_when_short_enough() {
        let text = "short text";
        assert_eq!(truncate_utf8_to_byte_limit(text, 64), text);
    }

    #[test]
    fn truncate_utf8_to_byte_limit_preserves_utf8_boundaries() {
        let text = "ab🙂cd";
        let truncated = truncate_utf8_to_byte_limit(text, 5);
        assert_eq!(truncated, "ab");
    }

    #[test]
    fn parse_vector_dimensions_from_format_type_reads_pgvector_dimensions() {
        assert_eq!(
            parse_vector_dimensions_from_format_type("vector(768)"),
            Some(768)
        );
    }

    #[test]
    fn parse_vector_dimensions_from_format_type_rejects_non_vector_types() {
        assert_eq!(parse_vector_dimensions_from_format_type("vector"), None);
        assert_eq!(parse_vector_dimensions_from_format_type("integer"), None);
    }
}

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Weak};
use std::time::Duration;

use chrono::Utc;
use sha2::{Digest, Sha256};
use thiserror::Error;
use tokio::sync::Mutex;
use tracing::{info, warn};

use crate::authorization::AuthContext;
use crate::base_query::{QueryBaseRequest, QueryBaseResponse};
use crate::context::{AssembleContextRequest, AssembleContextResponse};
use crate::couchdb::{CouchDbClient, CouchDbError};
use crate::livesync::{ChangeEvent, LivesyncDocument};
use crate::model::{Note, NoteId, VaultFile};
use crate::new_note::{NewNoteRequest, UpdateNoteRequest, WriteError};
use crate::search::{SearchMode, SearchResponse};
use crate::store::{
    BacklinksResponse, NeighborDirection, NeighborsResponse, NewNoteResponse, NoteTimeFilter,
    NoteVisibility, PathResponse, QueryNotesRequest, RecentNotesResponse, RecoveredVaultFileState,
    StaleFileRecoveryTarget, StatusResponse, TagsResponse, UpdateNoteResponse, VaultFileVisibility,
    VaultStore,
};

#[derive(Clone, Debug)]
pub struct VaultBridgeService {
    pub store: VaultStore,
    pub couchdb: Option<Arc<CouchDbClient>>,
    vault_file_repair_locks: Arc<Mutex<HashMap<String, Weak<Mutex<()>>>>>,
}

impl VaultBridgeService {
    pub fn new(store: VaultStore, couchdb: Option<Arc<CouchDbClient>>) -> Self {
        Self {
            store,
            couchdb,
            vault_file_repair_locks: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub async fn get_note(
        &self,
        auth: &AuthContext,
        note_id: &NoteId,
    ) -> Result<Note, ServiceError> {
        if let Some(note) = self.store.get_note_for_policy(auth, note_id).await {
            return Ok(note);
        }

        let visibility = self.store.note_visibility_for_policy(auth, note_id).await;
        log_note_lookup_miss(auth, "id", note_id.as_str(), visibility);
        Err(ServiceError::NotFound)
    }

    pub async fn get_note_by_title(
        &self,
        auth: &AuthContext,
        title: &str,
    ) -> Result<Note, ServiceError> {
        if let Some(note) = self.store.get_note_by_title_for_policy(auth, title).await {
            return Ok(note);
        }

        let visibility = self.store.title_visibility_for_policy(auth, title).await;
        log_note_lookup_miss(auth, "title", title, visibility);
        Err(ServiceError::NotFound)
    }

    pub async fn search(
        &self,
        auth: &AuthContext,
        query: &str,
        mode: SearchMode,
        limit: usize,
    ) -> SearchResponse {
        self.store.search_for_policy(auth, query, mode, limit).await
    }

    pub async fn recent_notes(
        &self,
        auth: &AuthContext,
        since: Option<chrono::DateTime<Utc>>,
        last_n_days: Option<i64>,
        limit: usize,
    ) -> Result<RecentNotesResponse, ServiceError> {
        self.store
            .recent_notes_for_policy(auth, since, last_n_days, limit)
            .await
            .map_err(|error| ServiceError::BadRequest(error.to_string()))
    }

    pub async fn query_notes(
        &self,
        auth: &AuthContext,
        request: QueryNotesRequest,
    ) -> RecentNotesResponse {
        self.store.query_notes_for_policy(auth, request).await
    }

    pub async fn query_base(
        &self,
        auth: &AuthContext,
        request: QueryBaseRequest,
    ) -> Result<QueryBaseResponse, ServiceError> {
        self.store
            .query_base_for_policy(auth, request)
            .await
            .map_err(|error| ServiceError::BadRequest(error.to_string()))
    }

    pub async fn neighbors(
        &self,
        auth: &AuthContext,
        note_id: &NoteId,
        depth: usize,
        direction: NeighborDirection,
    ) -> Result<NeighborsResponse, ServiceError> {
        self.store
            .neighbors_for_policy(auth, note_id, depth, direction)
            .await
            .ok_or(ServiceError::NotFound)
    }

    pub async fn backlinks(
        &self,
        auth: &AuthContext,
        note_id: &NoteId,
    ) -> Result<BacklinksResponse, ServiceError> {
        self.store
            .backlinks_for_policy(auth, note_id)
            .await
            .ok_or(ServiceError::NotFound)
    }

    pub async fn shortest_path(
        &self,
        auth: &AuthContext,
        from: &NoteId,
        to: &NoteId,
    ) -> PathResponse {
        self.store.shortest_path_for_policy(auth, from, to).await
    }

    pub async fn assemble_context(
        &self,
        auth: &AuthContext,
        request: AssembleContextRequest,
    ) -> AssembleContextResponse {
        self.store.assemble_context_for_policy(auth, request).await
    }

    pub async fn list_tags(&self, auth: &AuthContext, filter: NoteTimeFilter) -> TagsResponse {
        self.store.tags_for_policy(auth, filter).await
    }

    pub async fn create_note(
        &self,
        auth: &AuthContext,
        request: NewNoteRequest,
    ) -> Result<NewNoteResponse, ServiceError> {
        let now = Utc::now();
        let path = self
            .store
            .validate_new_note_write_at(&request, now)
            .await
            .map_err(ServiceError::Write)?;
        let request = self
            .store
            .prepare_create_note_request(auth, request, &path, now)
            .await
            .map_err(ServiceError::Write)?;

        let write = self
            .store
            .prepare_create_vault_write_at(request, now)
            .await
            .map_err(ServiceError::Write)?;
        let response = NewNoteResponse {
            id: NoteId::new(write.path.clone()),
            status: "created",
            file_type: write.file_type,
            indexed_as_note: write.note.is_some(),
        };
        let couchdb_rev = if let Some(couchdb) = self.couchdb.as_ref() {
            couchdb
                .write_livesync_note(&path, &write.content)
                .await
                .map_err(ServiceError::CouchDbWrite)?
                .couchdb_rev
        } else {
            "local-new-note".to_string()
        };
        self.store
            .commit_prepared_vault_write(write, &couchdb_rev)
            .await
            .map_err(ServiceError::Write)?;
        Ok(response)
    }

    pub async fn update_note(
        &self,
        auth: &AuthContext,
        note_id: &NoteId,
        request: UpdateNoteRequest,
    ) -> Result<UpdateNoteResponse, ServiceError> {
        let now = Utc::now();
        let request = self
            .store
            .prepare_update_note_request(auth, note_id, request, now)
            .await
            .map_err(ServiceError::Write)?;
        let write = self
            .store
            .prepare_update_note_write_at(note_id, &request, now)
            .await
            .map_err(ServiceError::Write)?;
        let couchdb_rev = if let Some(couchdb) = self.couchdb.as_ref() {
            couchdb
                .update_livesync_note(note_id.as_str(), &write.content)
                .await
                .map_err(ServiceError::CouchDbUpdate)?
                .couchdb_rev
        } else {
            "local-update-note".to_string()
        };
        self.store
            .commit_prepared_vault_write(write, &couchdb_rev)
            .await
            .map_err(ServiceError::Write)?;
        Ok(UpdateNoteResponse {
            id: note_id.clone(),
            status: "updated",
        })
    }

    pub async fn status(&self) -> StatusResponse {
        let mut status = self.store.status().await;
        let Some(couchdb) = self.couchdb.as_ref() else {
            return status;
        };

        match tokio::time::timeout(Duration::from_secs(3), couchdb.current_sequence()).await {
            Ok(Ok(current_seq)) => {
                status.sync.behind_by = sequence_lag(&status.sync.last_seq, &current_seq);
                status.sync.couchdb_current_seq = current_seq;
                status.sync.current_seq_source = "live";
                status.sync.current_seq_observed_at = Utc::now();
            }
            Ok(Err(error)) => warn!(
                error = %error,
                "status could not refresh live CouchDB sequence; reporting cached watermark"
            ),
            Err(_) => warn!(
                "status timed out refreshing live CouchDB sequence; reporting cached watermark"
            ),
        }
        status
    }

    pub async fn get_vault_file(
        &self,
        auth: &AuthContext,
        file_id: &NoteId,
    ) -> Result<VaultFile, ServiceError> {
        self.ensure_vault_file_available(auth, file_id).await
    }

    pub async fn create_vault_file(
        &self,
        auth: &AuthContext,
        request: NewNoteRequest,
    ) -> Result<NewNoteResponse, ServiceError> {
        let now = Utc::now();
        let path = self
            .store
            .validate_new_note_write_at(&request, now)
            .await
            .map_err(ServiceError::Write)?;
        let request = self
            .store
            .prepare_create_note_request(auth, request, &path, now)
            .await
            .map_err(ServiceError::Write)?;

        let write = self
            .store
            .prepare_create_vault_write_at(request, now)
            .await
            .map_err(ServiceError::Write)?;
        let response = NewNoteResponse {
            id: NoteId::new(write.path.clone()),
            status: "created",
            file_type: write.file_type,
            indexed_as_note: write.note.is_some(),
        };
        let couchdb_rev = if let Some(couchdb) = self.couchdb.as_ref() {
            couchdb
                .write_livesync_note(&path, &write.content)
                .await
                .map_err(ServiceError::CouchDbWrite)?
                .couchdb_rev
        } else {
            "local-new-file".to_string()
        };
        self.store
            .commit_prepared_vault_write(write, &couchdb_rev)
            .await
            .map_err(ServiceError::Write)?;
        Ok(response)
    }

    pub async fn edit_vault_file(
        &self,
        auth: &AuthContext,
        file_id: &NoteId,
        request: UpdateNoteRequest,
    ) -> Result<UpdateNoteResponse, ServiceError> {
        self.ensure_vault_file_available(auth, file_id).await?;
        let now = Utc::now();
        let write = self
            .store
            .prepare_edit_vault_file_write(auth, file_id, request, now)
            .await
            .map_err(ServiceError::Write)?;
        let couchdb_rev = if let Some(couchdb) = self.couchdb.as_ref() {
            couchdb
                .update_livesync_note(file_id.as_str(), &write.content)
                .await
                .map_err(ServiceError::CouchDbUpdate)?
                .couchdb_rev
        } else {
            "local-edit-file".to_string()
        };
        self.store
            .commit_prepared_vault_write(write, &couchdb_rev)
            .await
            .map_err(ServiceError::Write)?;
        Ok(UpdateNoteResponse {
            id: file_id.clone(),
            status: "updated",
        })
    }

    async fn ensure_vault_file_available(
        &self,
        auth: &AuthContext,
        file_id: &NoteId,
    ) -> Result<VaultFile, ServiceError> {
        if let Some(file) = self.store.get_vault_file_for_policy(auth, file_id).await {
            return Ok(file);
        }

        let visibility = self
            .store
            .vault_file_visibility_for_policy(auth, file_id)
            .await;
        if visibility != VaultFileVisibility::MissingRawWithIndexedNote
            || self
                .store
                .get_note_for_policy(auth, file_id)
                .await
                .is_none()
        {
            log_vault_file_lookup_miss(auth, file_id.as_str(), visibility);
            return Err(ServiceError::NotFound);
        }

        let Some(couch) = self.couchdb.as_deref() else {
            log_vault_file_lookup_miss(auth, file_id.as_str(), visibility);
            return Err(ServiceError::NotFound);
        };
        let repair_lock = self.vault_file_repair_lock(file_id.as_str()).await;
        let _repair_guard = repair_lock.lock().await;

        // Another request may have repaired the same path while this one waited.
        if let Some(file) = self.store.get_vault_file_for_policy(auth, file_id).await {
            return Ok(file);
        }

        let target = self.store.vault_file_recovery_target(file_id).await;
        let recovery = self
            .recover_vault_file_from_couch(couch, file_id, target.as_ref())
            .await
            .map_err(ServiceError::VaultFileRepair)?;
        if let Some(recovered) = recovery.recovered {
            self.store
                .commit_recovered_vault_file(recovered)
                .await
                .map_err(ServiceError::Write)?;
            if let Some(file) = self.store.get_vault_file_for_policy(auth, file_id).await {
                info!(
                    lookup_hash = lookup_fingerprint("vault_file", file_id.as_str()).as_str(),
                    "repaired missing raw vault file during policy-visible read"
                );
                return Ok(file);
            }
        }

        let deletion_lookup = target
            .as_ref()
            .map(|target| target.file_doc_id.as_str())
            .unwrap_or_else(|| file_id.as_str());
        let deleted_document_id = if recovery.source_file_seen || recovery.source_deletion_seen {
            None
        } else {
            couch
                .deleted_recovery_document_id(deletion_lookup)
                .await
                .map_err(ServiceError::VaultFileRepair)?
        };
        if recovery.source_deletion_seen || deleted_document_id.is_some() {
            self.store
                .commit_confirmed_vault_file_deletion(file_id)
                .await
                .map_err(ServiceError::Write)?;
            info!(
                lookup_hash = lookup_fingerprint("vault_file", file_id.as_str()).as_str(),
                "removed stale local vault state after confirming source deletion"
            );
            return Err(ServiceError::NotFound);
        }

        if recovery.source_file_seen
            && let Some(recovered) = self
                .store
                .recovered_vault_file_from_index(file_id)
                .await
                .map_err(ServiceError::Write)?
        {
            self.store
                .commit_recovered_vault_file(recovered)
                .await
                .map_err(ServiceError::Write)?;
            if let Some(file) = self.store.get_vault_file_for_policy(auth, file_id).await {
                warn!(
                    lookup_hash = lookup_fingerprint("vault_file", file_id.as_str()).as_str(),
                    "reconstructed raw vault file from visible index after incomplete CouchDB chunk recovery"
                );
                return Ok(file);
            }
        }

        warn!(
            lookup_hash = lookup_fingerprint("vault_file", file_id.as_str()).as_str(),
            "policy-visible vault file could not be reconstructed from couchdb"
        );
        Err(ServiceError::VaultFileTemporarilyUnavailable)
    }

    async fn vault_file_repair_lock(&self, path: &str) -> Arc<Mutex<()>> {
        let mut locks = self.vault_file_repair_locks.lock().await;
        if let Some(lock) = locks.get(path).and_then(Weak::upgrade) {
            return lock;
        }
        locks.retain(|_, lock| lock.strong_count() > 0);
        let lock = Arc::new(Mutex::new(()));
        locks.insert(path.to_string(), Arc::downgrade(&lock));
        lock
    }

    async fn recover_vault_file_from_couch(
        &self,
        couch: &CouchDbClient,
        file_id: &NoteId,
        target: Option<&StaleFileRecoveryTarget>,
    ) -> Result<VaultFileRecoveryAttempt, CouchDbError> {
        let mut source_file_seen = false;
        let mut source_deletion_seen = false;
        if let Some(target) = target {
            let mut document_ids = Vec::with_capacity(target.child_doc_ids.len() + 1);
            document_ids.push(target.file_doc_id.clone());
            document_ids.extend(target.child_doc_ids.iter().cloned());
            let changes = couch.fetch_documents_as_changes(&document_ids).await?;
            source_file_seen |= changes_contain_live_file_document(&changes);
            source_deletion_seen |= changes_contain_deleted_file_document(&changes);
            if let Some(recovered) =
                reconstruct_vault_file(file_id, changes, couch.livesync_decryptor()).await
            {
                return Ok(VaultFileRecoveryAttempt {
                    recovered: Some(recovered),
                    source_file_seen: true,
                    source_deletion_seen: false,
                });
            }

            let changes = couch
                .fetch_parent_recovery_changes(&target.file_doc_id)
                .await?;
            source_file_seen |= changes_contain_live_file_document(&changes);
            source_deletion_seen |= changes_contain_deleted_file_document(&changes);
            if let Some(recovered) =
                reconstruct_vault_file(file_id, changes, couch.livesync_decryptor()).await
            {
                return Ok(VaultFileRecoveryAttempt {
                    recovered: Some(recovered),
                    source_file_seen: true,
                    source_deletion_seen: false,
                });
            }
        }

        let changes = couch
            .fetch_parent_recovery_changes(file_id.as_str())
            .await?;
        source_file_seen |= changes_contain_live_file_document(&changes);
        source_deletion_seen |= changes_contain_deleted_file_document(&changes);
        if let Some(recovered) =
            reconstruct_vault_file(file_id, changes, couch.livesync_decryptor()).await
        {
            return Ok(VaultFileRecoveryAttempt {
                recovered: Some(recovered),
                source_file_seen: true,
                source_deletion_seen: false,
            });
        }

        let path_changes = couch
            .find_file_document_changes_by_note_paths(
                &[file_id.as_str().to_string()],
                couch.livesync_decryptor(),
            )
            .await?;
        source_file_seen |= !path_changes.is_empty();
        let parent_ids = path_changes
            .iter()
            .map(|change| change.id.clone())
            .collect::<Vec<_>>();
        let mut changes = path_changes;
        for parent_id in parent_ids {
            let parent_changes = couch.fetch_parent_recovery_changes(&parent_id).await?;
            source_file_seen |= changes_contain_live_file_document(&parent_changes);
            source_deletion_seen |= changes_contain_deleted_file_document(&parent_changes);
            append_unique_changes(&mut changes, parent_changes);
        }
        Ok(VaultFileRecoveryAttempt {
            recovered: reconstruct_vault_file(file_id, changes, couch.livesync_decryptor()).await,
            source_file_seen,
            source_deletion_seen,
        })
    }
}

struct VaultFileRecoveryAttempt {
    recovered: Option<RecoveredVaultFileState>,
    source_file_seen: bool,
    source_deletion_seen: bool,
}

fn changes_contain_live_file_document(changes: &[ChangeEvent]) -> bool {
    changes.iter().any(|change| {
        change
            .doc
            .clone()
            .and_then(|doc| LivesyncDocument::try_from(doc).ok())
            .is_some_and(|doc| matches!(doc, LivesyncDocument::File(file) if !file.deleted))
    })
}

fn changes_contain_deleted_file_document(changes: &[ChangeEvent]) -> bool {
    changes.iter().any(|change| {
        change.deleted
            && change
                .doc
                .clone()
                .and_then(|doc| LivesyncDocument::try_from(doc).ok())
                .is_some_and(|doc| matches!(doc, LivesyncDocument::File(_)))
    })
}

async fn reconstruct_vault_file(
    file_id: &NoteId,
    changes: Vec<ChangeEvent>,
    decryptor: Option<&crate::encryption::Decryptor>,
) -> Option<RecoveredVaultFileState> {
    if changes.is_empty() {
        return None;
    }
    let recovery_store = VaultStore::new(20);
    recovery_store
        .ingest_changes_batch(changes, "", 250, Duration::MAX, decryptor)
        .await;
    recovery_store.recovered_vault_file_state(file_id).await
}

fn append_unique_changes(existing: &mut Vec<ChangeEvent>, additional: Vec<ChangeEvent>) {
    let mut seen = existing
        .iter()
        .map(|change| change.id.clone())
        .collect::<HashSet<_>>();
    existing.extend(
        additional
            .into_iter()
            .filter(|change| seen.insert(change.id.clone())),
    );
}

fn log_note_lookup_miss(
    auth: &AuthContext,
    lookup_kind: &'static str,
    lookup_value: &str,
    visibility: NoteVisibility,
) {
    info!(
        context = auth.context.as_str(),
        principal = auth.principal.as_str(),
        lookup_kind,
        lookup_hash = lookup_fingerprint(lookup_kind, lookup_value).as_str(),
        visibility = note_visibility_label(visibility),
        "note lookup returned not found"
    );
}

fn note_visibility_label(visibility: NoteVisibility) -> &'static str {
    match visibility {
        NoteVisibility::Missing => "missing_index_row",
        NoteVisibility::Accessible => "accessible",
        NoteVisibility::Filtered => "filtered_by_policy",
    }
}

fn lookup_fingerprint(kind: &str, value: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(kind.as_bytes());
    hasher.update(b":");
    hasher.update(value.trim().as_bytes());
    let digest = hex::encode(hasher.finalize());
    digest.chars().take(16).collect()
}

fn log_vault_file_lookup_miss(
    auth: &AuthContext,
    lookup_value: &str,
    visibility: VaultFileVisibility,
) {
    let visibility = match visibility {
        VaultFileVisibility::Missing => "missing_file",
        VaultFileVisibility::MissingRawWithIndexedNote => "missing_raw_with_indexed_note",
        VaultFileVisibility::MissingIndexWithRawMarkdown => "missing_index_with_raw_markdown",
        VaultFileVisibility::Accessible => "accessible",
        VaultFileVisibility::Filtered => "filtered_by_policy",
    };
    info!(
        context = auth.context.as_str(),
        principal = auth.principal.as_str(),
        lookup_hash = lookup_fingerprint("vault_file", lookup_value).as_str(),
        visibility,
        "vault file lookup returned not found"
    );
}

fn sequence_lag(last_seq: &str, current_seq: &str) -> i64 {
    fn prefix(value: &str) -> i64 {
        value
            .split_once('-')
            .map(|(prefix, _)| prefix)
            .unwrap_or(value)
            .parse::<i64>()
            .unwrap_or(0)
    }

    (prefix(current_seq) - prefix(last_seq)).max(0)
}

#[derive(Debug, Error)]
pub enum ServiceError {
    #[error("not found")]
    NotFound,
    #[error("bad request: {0}")]
    BadRequest(String),
    #[error(transparent)]
    Write(#[from] WriteError),
    #[error("failed to write note to couchdb: {0}")]
    CouchDbWrite(CouchDbError),
    #[error("failed to update note in couchdb: {0}")]
    CouchDbUpdate(CouchDbError),
    #[error("failed to repair raw vault file from couchdb: {0}")]
    VaultFileRepair(CouchDbError),
    #[error("raw vault file is temporarily unavailable while source reconciliation is incomplete")]
    VaultFileTemporarilyUnavailable,
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, HashMap, HashSet};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use axum::extract::{Path, Query, State};
    use axum::http::StatusCode;
    use axum::routing::get;
    use axum::{Json, Router};
    use chrono::Utc;
    use serde::Deserialize;

    use super::{ServiceError, VaultBridgeService};
    use crate::authorization::{AccessPolicy, AuthContext, ContextName};
    use crate::config::{CouchDbConfig, DatabaseConfig, FeedMode};
    use crate::couchdb::{CouchDbClient, build_livesync_note_documents};
    use crate::livesync::ChangeEvent;
    use crate::model::NoteId;
    use crate::new_note::{ContentPatchOperation, UpdateNoteRequest};
    use crate::persistence::PostgresPersistence;
    use crate::store::{NoteInput, VaultStore};

    #[derive(Clone, Default)]
    struct MockCouchState {
        docs: Arc<HashMap<String, serde_json::Value>>,
        deleted: Arc<HashSet<String>>,
        requests: Arc<AtomicUsize>,
    }

    #[derive(Debug, Deserialize)]
    struct AllDocsRequest {
        keys: Vec<String>,
        #[serde(default)]
        include_docs: bool,
    }

    async fn all_docs_by_key(
        State(state): State<MockCouchState>,
        Json(request): Json<AllDocsRequest>,
    ) -> Json<serde_json::Value> {
        state.requests.fetch_add(1, Ordering::SeqCst);
        let rows = request
            .keys
            .into_iter()
            .map(|key| mock_all_docs_row(&state, key, request.include_docs))
            .collect::<Vec<_>>();
        Json(serde_json::json!({ "rows": rows }))
    }

    async fn all_docs_scan(
        State(state): State<MockCouchState>,
        Query(query): Query<HashMap<String, String>>,
    ) -> Json<serde_json::Value> {
        state.requests.fetch_add(1, Ordering::SeqCst);
        let include_docs = query
            .get("include_docs")
            .is_some_and(|value| value == "true");
        let limit = query
            .get("limit")
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or(500);
        let startkey = query
            .get("startkey")
            .and_then(|value| serde_json::from_str::<String>(value).ok());
        let skip = query
            .get("skip")
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or(0);
        let mut keys = state
            .docs
            .keys()
            .chain(state.deleted.iter())
            .cloned()
            .collect::<Vec<_>>();
        keys.sort();
        keys.dedup();
        let start = startkey
            .as_ref()
            .and_then(|key| keys.iter().position(|candidate| candidate == key))
            .map(|index| index.saturating_add(skip))
            .unwrap_or(0);
        let rows = keys
            .into_iter()
            .skip(start)
            .take(limit)
            .map(|key| mock_all_docs_row(&state, key, include_docs))
            .collect::<Vec<_>>();
        Json(serde_json::json!({ "rows": rows }))
    }

    fn mock_all_docs_row(
        state: &MockCouchState,
        key: String,
        include_docs: bool,
    ) -> serde_json::Value {
        if state.deleted.contains(&key) {
            return serde_json::json!({
                "id": key,
                "key": key,
                "value": { "rev": "3-deleted", "deleted": true }
            });
        }
        if let Some(doc) = state.docs.get(&key) {
            let mut row = serde_json::json!({
                "id": key,
                "key": key,
                "value": {
                    "rev": doc.get("_rev").and_then(serde_json::Value::as_str).unwrap_or("1-test")
                }
            });
            if include_docs {
                row["doc"] = doc.clone();
            }
            return row;
        }
        serde_json::json!({ "key": key, "error": "not_found" })
    }

    async fn database_info() -> Json<serde_json::Value> {
        Json(serde_json::json!({"update_seq": "8-g1AAA-live"}))
    }

    async fn get_document(
        State(state): State<MockCouchState>,
        Path((_db, doc_id)): Path<(String, String)>,
    ) -> (StatusCode, Json<serde_json::Value>) {
        state.requests.fetch_add(1, Ordering::SeqCst);
        match state.docs.get(&doc_id) {
            Some(doc) => (StatusCode::OK, Json(doc.clone())),
            None => (
                StatusCode::NOT_FOUND,
                Json(serde_json::json!({"error": "not_found"})),
            ),
        }
    }

    async fn put_document(
        State(state): State<MockCouchState>,
        Path((_db, doc_id)): Path<(String, String)>,
        Json(_doc): Json<serde_json::Value>,
    ) -> Json<serde_json::Value> {
        state.requests.fetch_add(1, Ordering::SeqCst);
        Json(serde_json::json!({"ok": true, "id": doc_id, "rev": "3-mock"}))
    }

    async fn delete_document(
        State(state): State<MockCouchState>,
        Path((_db, doc_id)): Path<(String, String)>,
    ) -> Json<serde_json::Value> {
        state.requests.fetch_add(1, Ordering::SeqCst);
        Json(serde_json::json!({"ok": true, "id": doc_id, "rev": "3-deleted"}))
    }

    fn spawn_mock_couchdb(
        docs: HashMap<String, serde_json::Value>,
        deleted: HashSet<String>,
    ) -> (Arc<CouchDbClient>, MockCouchState) {
        let state = MockCouchState {
            docs: Arc::new(docs),
            deleted: Arc::new(deleted),
            requests: Arc::new(AtomicUsize::new(0)),
        };
        let app = Router::new()
            .route("/{db}", get(database_info))
            .route("/{db}/_all_docs", get(all_docs_scan).post(all_docs_by_key))
            .route(
                "/{db}/{doc_id}",
                get(get_document).put(put_document).delete(delete_document),
            )
            .with_state(state.clone());
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind mock couchdb");
        listener
            .set_nonblocking(true)
            .expect("set mock listener nonblocking");
        let addr = listener.local_addr().expect("mock address");
        tokio::spawn(async move {
            let listener = tokio::net::TcpListener::from_std(listener).expect("tokio listener");
            axum::serve(listener, app)
                .await
                .expect("serve mock couchdb");
        });
        let couch = CouchDbClient::new(&CouchDbConfig {
            url: format!("http://{addr}"),
            database: "mainvault".to_string(),
            username: "user".to_string(),
            password: "pass".to_string(),
            poll_interval_seconds: 1,
            feed_mode: FeedMode::Longpoll,
            ..Default::default()
        })
        .expect("couch client");
        (Arc::new(couch), state)
    }

    #[tokio::test]
    async fn status_uses_live_couchdb_sequence_instead_of_cached_watermark() {
        let store = VaultStore::new(20);
        store.set_sync_state("5-g1AAA", "5-g1AAA").await;
        let (couch, _) = spawn_mock_couchdb(HashMap::new(), HashSet::new());
        let service = VaultBridgeService::new(store, Some(couch));

        let status = service.status().await;

        assert_eq!(status.sync.couchdb_current_seq, "8-g1AAA-live");
        assert_eq!(status.sync.behind_by, 3);
        assert_eq!(status.sync.current_seq_source, "live");
    }

    async fn indexed_store(path: &str, couchdb_rev: &str) -> (VaultStore, AuthContext) {
        let store = VaultStore::new(20);
        let now = Utc::now();
        store
            .upsert_note(NoteInput {
                id: NoteId::new(path),
                title: "Repair Target".to_string(),
                content: "# Repair Target\n\nStale indexed content.\n".to_string(),
                frontmatter: serde_json::json!({"tags": ["shared"]}),
                tags: vec!["shared".to_string()],
                couchdb_rev: couchdb_rev.to_string(),
                created_at: Some(now),
                updated_at: now,
                embedding: None,
                links: Vec::new(),
            })
            .await;
        let mut contexts = BTreeMap::new();
        contexts.insert("admin".to_string(), AccessPolicy::admin());
        store.set_authorization_config(contexts).await;
        (
            store,
            AuthContext::new(ContextName::new("admin"), "test:admin".to_string()),
        )
    }

    #[tokio::test]
    async fn policy_visible_raw_read_repairs_missing_file_from_couchdb() {
        let path = "11Active/repair-target.md";
        let (store, auth) = indexed_store(path, "1-stale").await;
        let docs = build_livesync_note_documents(path, "# Repair Target\n\nAuthoritative body.\n");
        let mut file_doc = docs.file_doc;
        let mut leaf_doc = docs.leaf_doc;
        file_doc["_rev"] = serde_json::json!("2-file");
        leaf_doc["_rev"] = serde_json::json!("2-leaf");
        store
            .ingest_changes_batch(
                vec![ChangeEvent {
                    seq: serde_json::Value::String(String::new()),
                    id: docs.file_id.clone(),
                    deleted: false,
                    doc: Some(file_doc.clone()),
                }],
                "",
                250,
                Duration::from_secs(60),
                None,
            )
            .await;
        let mut source_docs = HashMap::new();
        source_docs.insert(docs.file_id, file_doc);
        source_docs.insert(docs.leaf_id, leaf_doc);
        let (couch, state) = spawn_mock_couchdb(source_docs, HashSet::new());
        let service = VaultBridgeService::new(store.clone(), Some(couch));

        let file = service
            .get_vault_file(&auth, &NoteId::new(path))
            .await
            .expect("targeted repair should satisfy read");

        assert!(file.content.contains("Authoritative body."));
        assert!(state.requests.load(Ordering::SeqCst) > 0);
        assert!(
            store
                .get_vault_file_for_policy(&auth, &NoteId::new(path))
                .await
                .is_some()
        );
    }

    #[tokio::test]
    async fn repaired_raw_file_survives_fresh_postgres_hydration() {
        let Some(database_url) = std::env::var("VAULT_BRIDGE_TEST_DATABASE_URL").ok() else {
            return;
        };
        assert!(
            database_url
                .rsplit('/')
                .next()
                .is_some_and(|name| name.contains("test"))
        );
        let persistence = Arc::new(
            PostgresPersistence::connect_and_migrate(
                &DatabaseConfig {
                    url: database_url,
                    max_connections: 5,
                },
                64,
            )
            .await
            .expect("test postgres"),
        );
        sqlx::query(
            "TRUNCATE TABLE access_log, api_keys, links, tags, blocks, notes, vault_files, sync_state, store_state, sync_recovery_queue, chunk_staging, file_aliases RESTART IDENTITY CASCADE",
        )
        .execute(persistence.pool())
        .await
        .expect("reset test postgres");

        let path = "11Active/durable-read-repair.md";
        let store = VaultStore::new_with_persistence(20, persistence.clone());
        let now = Utc::now();
        store
            .upsert_note(NoteInput {
                id: NoteId::new(path),
                title: "Durable Read Repair".to_string(),
                content: "# Durable Read Repair\n\nStale index.\n".to_string(),
                frontmatter: serde_json::json!({"tags": ["shared"]}),
                tags: vec!["shared".to_string()],
                couchdb_rev: "1-stale".to_string(),
                created_at: Some(now),
                updated_at: now,
                embedding: None,
                links: Vec::new(),
            })
            .await;
        let mut contexts = BTreeMap::new();
        contexts.insert("admin".to_string(), AccessPolicy::admin());
        store.set_authorization_config(contexts.clone()).await;
        let auth = AuthContext::new(ContextName::new("admin"), "test:admin".to_string());
        let docs = build_livesync_note_documents(path, "# Durable Read Repair\n\nSource body.\n");
        let mut file_doc = docs.file_doc;
        let mut leaf_doc = docs.leaf_doc;
        file_doc["_rev"] = serde_json::json!("2-file");
        leaf_doc["_rev"] = serde_json::json!("2-leaf");
        store
            .ingest_changes_batch(
                vec![ChangeEvent {
                    seq: serde_json::Value::String(String::new()),
                    id: docs.file_id.clone(),
                    deleted: false,
                    doc: Some(file_doc.clone()),
                }],
                "",
                250,
                Duration::from_secs(60),
                None,
            )
            .await;
        let mut source_docs = HashMap::new();
        source_docs.insert(docs.file_id, file_doc);
        source_docs.insert(docs.leaf_id, leaf_doc);
        let (couch, _) = spawn_mock_couchdb(source_docs, HashSet::new());
        let service = VaultBridgeService::new(store, Some(couch));

        service
            .get_vault_file(&auth, &NoteId::new(path))
            .await
            .expect("repair raw file");

        let fresh_store = VaultStore::new_with_persistence(20, persistence);
        fresh_store
            .hydrate_from_persistence()
            .await
            .expect("fresh hydration");
        fresh_store.set_authorization_config(contexts).await;
        let raw = fresh_store
            .get_vault_file_for_policy(&auth, &NoteId::new(path))
            .await
            .expect("repaired raw survives hydration");
        assert!(raw.content.contains("Source body."));
    }

    #[tokio::test]
    async fn raw_edit_repairs_missing_file_before_applying_patch() {
        let path = "11Active/edit-repair-target.md";
        let (store, auth) = indexed_store(path, "1-stale").await;
        let docs = build_livesync_note_documents(path, "# Edit Repair Target\n\nOriginal body.\n");
        let mut file_doc = docs.file_doc;
        let mut leaf_doc = docs.leaf_doc;
        file_doc["_rev"] = serde_json::json!("2-file");
        leaf_doc["_rev"] = serde_json::json!("2-leaf");
        store
            .ingest_changes_batch(
                vec![ChangeEvent {
                    seq: serde_json::Value::String(String::new()),
                    id: docs.file_id.clone(),
                    deleted: false,
                    doc: Some(file_doc.clone()),
                }],
                "",
                250,
                Duration::from_secs(60),
                None,
            )
            .await;
        let mut source_docs = HashMap::new();
        source_docs.insert(docs.file_id, file_doc);
        source_docs.insert(docs.leaf_id, leaf_doc);
        let (couch, _) = spawn_mock_couchdb(source_docs, HashSet::new());
        let service = VaultBridgeService::new(store, Some(couch));

        let response = service
            .edit_vault_file(
                &auth,
                &NoteId::new(path),
                UpdateNoteRequest {
                    content: None,
                    content_patch: Some(vec![ContentPatchOperation::Append {
                        text: "\nRepaired and edited.\n".to_string(),
                    }]),
                    tags: None,
                    metadata: None,
                },
            )
            .await
            .expect("edit should repair before applying patch");

        assert_eq!(response.status, "updated");
        let file = service
            .get_vault_file(&auth, &NoteId::new(path))
            .await
            .expect("edited raw file");
        assert!(file.content.contains("Repaired and edited."));
    }

    #[tokio::test]
    async fn incomplete_live_source_falls_back_to_visible_index_content() {
        let path = "11Active/incomplete-live-source.md";
        let (store, auth) = indexed_store(path, "1-stale").await;
        let docs = build_livesync_note_documents(path, "# Incomplete Live Source\n\nNewer body.\n");
        let mut file_doc = docs.file_doc;
        file_doc["_rev"] = serde_json::json!("2-file");
        store
            .ingest_changes_batch(
                vec![ChangeEvent {
                    seq: serde_json::Value::String(String::new()),
                    id: docs.file_id.clone(),
                    deleted: false,
                    doc: Some(file_doc.clone()),
                }],
                "",
                250,
                Duration::from_secs(60),
                None,
            )
            .await;
        let (couch, _) =
            spawn_mock_couchdb(HashMap::from([(docs.file_id, file_doc)]), HashSet::new());
        let service = VaultBridgeService::new(store, Some(couch));

        let file = service
            .get_vault_file(&auth, &NoteId::new(path))
            .await
            .expect("visible index should provide a safe read fallback");

        assert!(file.content.contains("Stale indexed content."));
    }

    #[tokio::test]
    async fn unresolved_policy_visible_raw_read_is_retryable() {
        let path = "11Active/incomplete-source.md";
        let (store, auth) = indexed_store(path, "1-stale").await;
        let (couch, _) = spawn_mock_couchdb(HashMap::new(), HashSet::new());
        let service = VaultBridgeService::new(store, Some(couch));

        let error = service
            .get_vault_file(&auth, &NoteId::new(path))
            .await
            .expect_err("incomplete source should not look like policy denial");

        assert!(matches!(
            error,
            ServiceError::VaultFileTemporarilyUnavailable
        ));
    }

    #[tokio::test]
    async fn filtered_missing_raw_note_does_not_query_couchdb() {
        let path = "11Active/filtered.md";
        let (store, _) = indexed_store(path, "1-stale").await;
        store
            .set_authorization_config(BTreeMap::from([(
                "blocked".to_string(),
                AccessPolicy::default(),
            )]))
            .await;
        let auth = AuthContext::new(ContextName::new("blocked"), "test:blocked".to_string());
        let (couch, state) = spawn_mock_couchdb(HashMap::new(), HashSet::new());
        let service = VaultBridgeService::new(store, Some(couch));

        let error = service
            .get_vault_file(&auth, &NoteId::new(path))
            .await
            .expect_err("filtered note must stay opaque");

        assert!(matches!(error, ServiceError::NotFound));
        assert_eq!(state.requests.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn confirmed_source_tombstone_prunes_stale_local_note() {
        let path = "11Active/deleted-source.md";
        let (store, auth) = indexed_store(path, "1-stale").await;
        let docs = build_livesync_note_documents(path, "# Deleted Source\n");
        let mut file_doc = docs.file_doc;
        file_doc["_rev"] = serde_json::json!("2-file");
        store
            .ingest_changes_batch(
                vec![ChangeEvent {
                    seq: serde_json::Value::String(String::new()),
                    id: docs.file_id.clone(),
                    deleted: false,
                    doc: Some(file_doc),
                }],
                "",
                250,
                Duration::from_secs(60),
                None,
            )
            .await;
        let (couch, _) = spawn_mock_couchdb(HashMap::new(), HashSet::from([docs.file_id.clone()]));
        let service = VaultBridgeService::new(store.clone(), Some(couch));

        let error = service
            .get_vault_file(&auth, &NoteId::new(path))
            .await
            .expect_err("confirmed deletion should remain a genuine 404");

        assert!(matches!(error, ServiceError::NotFound));
        assert!(
            store
                .get_note_for_policy(&auth, &NoteId::new(path))
                .await
                .is_none()
        );
        assert!(
            store
                .vault_file_recovery_target(&NoteId::new(path))
                .await
                .is_none()
        );
    }
}

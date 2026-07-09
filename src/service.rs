use std::sync::Arc;

use chrono::Utc;
use sha2::{Digest, Sha256};
use thiserror::Error;
use tracing::info;

use crate::authorization::AuthContext;
use crate::base_query::{QueryBaseRequest, QueryBaseResponse};
use crate::context::{AssembleContextRequest, AssembleContextResponse};
use crate::couchdb::{CouchDbClient, CouchDbError};
use crate::model::{Note, NoteId, VaultFile};
use crate::new_note::{NewNoteRequest, UpdateNoteRequest, WriteError};
use crate::search::{SearchMode, SearchResponse};
use crate::store::{
    BacklinksResponse, NeighborDirection, NeighborsResponse, NewNoteResponse, NoteTimeFilter,
    NoteVisibility, PathResponse, QueryNotesRequest, RecentNotesResponse, StatusResponse,
    TagsResponse, UpdateNoteResponse, VaultStore,
};

#[derive(Clone, Debug)]
pub struct VaultBridgeService {
    pub store: VaultStore,
    pub couchdb: Option<Arc<CouchDbClient>>,
}

impl VaultBridgeService {
    pub fn new(store: VaultStore, couchdb: Option<Arc<CouchDbClient>>) -> Self {
        Self { store, couchdb }
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

        if let Some(couchdb) = self.couchdb.as_ref() {
            couchdb
                .write_livesync_note(&path, &request.content)
                .await
                .map_err(ServiceError::CouchDbWrite)?;
        }

        self.store
            .create_note_at(request, now)
            .await
            .map_err(ServiceError::Write)
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
        let (response, markdown) = self
            .store
            .update_note_at(note_id, &request, now)
            .await
            .map_err(ServiceError::Write)?;

        if let Some(couchdb) = self.couchdb.as_ref() {
            couchdb
                .update_livesync_note(note_id.as_str(), &markdown)
                .await
                .map_err(ServiceError::CouchDbUpdate)?;
        }

        Ok(response)
    }

    pub async fn status(&self) -> StatusResponse {
        self.store.status().await
    }

    pub async fn get_vault_file(
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
        log_vault_file_lookup_miss(auth, file_id.as_str(), visibility);
        Err(ServiceError::NotFound)
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

        if let Some(couchdb) = self.couchdb.as_ref() {
            couchdb
                .write_livesync_note(&path, &request.content)
                .await
                .map_err(ServiceError::CouchDbWrite)?;
        }

        self.store
            .create_vault_file_at(request, now)
            .await
            .map_err(ServiceError::Write)
    }

    pub async fn edit_vault_file(
        &self,
        auth: &AuthContext,
        file_id: &NoteId,
        request: UpdateNoteRequest,
    ) -> Result<UpdateNoteResponse, ServiceError> {
        let now = Utc::now();
        let (response, new_content) = self
            .store
            .edit_vault_file(auth, file_id, request, now)
            .await
            .map_err(ServiceError::Write)?;

        if let Some(couchdb) = self.couchdb.as_ref() {
            couchdb
                .update_livesync_note(file_id.as_str(), &new_content)
                .await
                .map_err(ServiceError::CouchDbUpdate)?;
        }

        Ok(response)
    }
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
    visibility: NoteVisibility,
) {
    info!(
        context = auth.context.as_str(),
        principal = auth.principal.as_str(),
        lookup_hash = lookup_fingerprint("vault_file", lookup_value).as_str(),
        visibility = note_visibility_label(visibility),
        "vault file lookup returned not found"
    );
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
}

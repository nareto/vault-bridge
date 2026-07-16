use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use hex::encode as hex_encode;
use reqwest::Client;
use reqwest::header::CONTENT_TYPE;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use thiserror::Error;
use tracing::info;

use crate::config::{CouchDbConfig, FeedMode};
use crate::encryption::{Decryptor, EncryptionError};
use crate::livesync::{
    ChangeEvent, ChangesFeed, FileDocument, LivesyncDocument, decode_leaf_chunk,
    normalize_note_path,
};

const COUCHDB_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Debug, Deserialize)]
struct AllDocsPage {
    rows: Vec<AllDocsRow>,
}

#[derive(Debug, Deserialize)]
struct AllDocsRow {
    id: String,
    #[serde(default)]
    doc: Option<Value>,
}

#[derive(Debug, Deserialize)]
struct BulkAllDocsPage {
    rows: Vec<BulkAllDocsRow>,
}

#[derive(Debug, Deserialize)]
struct BulkAllDocsRow {
    #[serde(default)]
    id: Option<String>,
    key: String,
    #[serde(default)]
    value: Option<BulkAllDocsValue>,
    #[serde(default)]
    doc: Option<Value>,
    #[serde(default)]
    error: Option<String>,
}

#[derive(Debug, Deserialize)]
struct BulkAllDocsValue {
    rev: String,
    #[serde(default)]
    deleted: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LiveSyncWriteReceipt {
    pub couchdb_rev: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SourceDocumentState {
    Live,
    Missing,
    Tombstoned,
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SourceChildState {
    pub ordinal: usize,
    pub document_id: String,
    pub state: SourceDocumentState,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct LiveSyncSourceDiagnostic {
    pub file_document_exists: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub file_document_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub file_revision: Option<String>,
    pub expected_child_count: usize,
    pub live_child_count: usize,
    pub missing_child_count: usize,
    pub tombstoned_child_count: usize,
    pub unknown_child_count: usize,
    pub children: Vec<SourceChildState>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct TombstonedChildRepairResult {
    pub expected_parent_revision: String,
    pub parent_revision_changed: bool,
    pub attempted_child_count: usize,
    pub repaired_child_count: usize,
    pub unrecoverable_child_count: usize,
    pub diagnostic: LiveSyncSourceDiagnostic,
}

impl LiveSyncSourceDiagnostic {
    pub fn failure_kind(&self) -> Option<&'static str> {
        match (
            self.missing_child_count > 0,
            self.tombstoned_child_count > 0,
        ) {
            (true, true) => Some("mixed_unavailable_children"),
            (true, false) => Some("missing_children"),
            (false, true) => Some("tombstoned_children"),
            (false, false) => None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct CouchDbClient {
    http: Client,
    base_url: String,
    database: String,
    username: String,
    password: String,
    feed_mode: FeedMode,
    longpoll_timeout_grace: Duration,
    livesync_crypto: Option<Arc<Decryptor>>,
    livesync_passphrase: Option<String>,
}

impl CouchDbClient {
    pub fn new(config: &CouchDbConfig) -> Result<Self, CouchDbError> {
        if !config.is_configured() {
            return Err(CouchDbError::NotConfigured);
        }

        let base_url = config.url.trim_end_matches('/').to_string();
        let http = Client::builder()
            .user_agent("vault-bridge/0.1")
            .build()
            .map_err(CouchDbError::HttpClientBuild)?;

        let grace_secs = config.longpoll_timeout_grace_seconds.max(1);
        Ok(Self {
            http,
            base_url,
            database: config.database.clone(),
            username: config.username.clone(),
            password: config.password.clone(),
            feed_mode: config.feed_mode,
            longpoll_timeout_grace: Duration::from_secs(grace_secs),
            livesync_crypto: None,
            livesync_passphrase: (!config.encryption.passphrase.is_empty())
                .then(|| config.encryption.passphrase.clone()),
        })
    }

    pub fn with_livesync_crypto(mut self, livesync_crypto: Option<Arc<Decryptor>>) -> Self {
        self.livesync_crypto = livesync_crypto;
        self
    }

    pub fn db_base_url(&self) -> String {
        format!("{}/{}", self.base_url, self.database)
    }

    pub fn livesync_decryptor(&self) -> Option<&Decryptor> {
        self.livesync_crypto.as_deref()
    }

    pub fn changes_url(&self) -> String {
        format!("{}/_changes", self.db_base_url())
    }

    async fn send_request_with_timeout(
        &self,
        request: reqwest::RequestBuilder,
        timeout: Duration,
    ) -> Result<reqwest::Response, CouchDbError> {
        tokio::time::timeout(timeout, request.send())
            .await
            .map_err(|_| Self::request_timeout_error(timeout))?
            .map_err(CouchDbError::Http)
    }

    fn request_timeout_error(timeout: Duration) -> CouchDbError {
        CouchDbError::RequestTimeout {
            timeout_ms: timeout.as_millis().min(u64::MAX as u128) as u64,
        }
    }

    async fn read_json_with_timeout<T: DeserializeOwned>(
        &self,
        response: reqwest::Response,
        timeout: Duration,
    ) -> Result<T, CouchDbError> {
        tokio::time::timeout(timeout, response.json())
            .await
            .map_err(|_| Self::request_timeout_error(timeout))?
            .map_err(CouchDbError::Http)
    }

    async fn read_text_with_timeout(
        &self,
        response: reqwest::Response,
        timeout: Duration,
    ) -> Result<String, CouchDbError> {
        tokio::time::timeout(timeout, response.text())
            .await
            .map_err(|_| Self::request_timeout_error(timeout))?
            .map_err(CouchDbError::Http)
    }

    pub async fn current_sequence(&self) -> Result<String, CouchDbError> {
        let url = self.db_base_url();
        let response = self
            .send_request_with_timeout(
                self.http
                    .get(url)
                    .basic_auth(&self.username, Some(&self.password)),
                COUCHDB_REQUEST_TIMEOUT,
            )
            .await?
            .error_for_status()?;

        let body: Value = self
            .read_json_with_timeout(response, COUCHDB_REQUEST_TIMEOUT)
            .await?;
        let seq = body
            .get("update_seq")
            .and_then(|value| match value {
                Value::String(raw) => Some(raw.clone()),
                Value::Number(number) => Some(number.to_string()),
                other => serde_json::to_string(other).ok(),
            })
            .unwrap_or_else(|| "0".to_string());
        Ok(seq)
    }

    pub async fn poll_changes(
        &self,
        since: &str,
        timeout: Duration,
    ) -> Result<ChangesFeed, CouchDbError> {
        if self.feed_mode == FeedMode::Continuous {
            return self.poll_changes_continuous(since, timeout).await;
        }

        let timeout_ms = timeout.as_millis().max(1).to_string();
        let request_timeout = timeout.saturating_add(self.longpoll_timeout_grace);
        let body_timeout = request_timeout;

        let response = self
            .send_request_with_timeout(
                self.http
                    .get(self.changes_url())
                    .basic_auth(&self.username, Some(&self.password))
                    .query(&[
                        ("feed", "longpoll"),
                        ("since", since),
                        ("include_docs", "true"),
                        ("timeout", timeout_ms.as_str()),
                    ]),
                request_timeout,
            )
            .await?
            .error_for_status()?;

        let content_type = response
            .headers()
            .get(CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .map(ToString::to_string);
        let body = self.read_text_with_timeout(response, body_timeout).await?;

        decode_changes_body(self.feed_mode, content_type.as_deref(), &body)
    }

    async fn poll_changes_continuous(
        &self,
        since: &str,
        timeout: Duration,
    ) -> Result<ChangesFeed, CouchDbError> {
        let timeout_ms = timeout.as_millis().max(1).to_string();
        let mut response = self
            .send_request_with_timeout(
                self.http
                    .get(self.changes_url())
                    .basic_auth(&self.username, Some(&self.password))
                    .query(&[
                        ("feed", "continuous"),
                        ("since", since),
                        ("include_docs", "true"),
                        ("heartbeat", timeout_ms.as_str()),
                    ]),
                COUCHDB_REQUEST_TIMEOUT,
            )
            .await?
            .error_for_status()?;

        let mut buffer = Vec::new();
        let mut results = Vec::new();
        let mut last_seq = Value::String(since.to_string());

        loop {
            let chunk = match tokio::time::timeout(timeout, response.chunk()).await {
                Ok(result) => result?,
                Err(_) => break,
            };
            let Some(chunk) = chunk else {
                break;
            };

            buffer.extend_from_slice(&chunk);

            while let Some(line_end) = buffer.iter().position(|byte| *byte == b'\n') {
                let line_bytes = buffer.drain(..=line_end).collect::<Vec<_>>();
                let line = std::str::from_utf8(&line_bytes).map_err(|error| {
                    CouchDbError::InvalidResponse(format!(
                        "continuous feed line is not valid utf-8: {error}"
                    ))
                })?;
                parse_continuous_line(line, &mut results, &mut last_seq)?;
            }
        }

        if !buffer.is_empty() {
            let line = std::str::from_utf8(&buffer).map_err(|error| {
                CouchDbError::InvalidResponse(format!(
                    "continuous feed trailing line is not valid utf-8: {error}"
                ))
            })?;
            parse_continuous_line(line, &mut results, &mut last_seq)?;
        }

        Ok(ChangesFeed { last_seq, results })
    }

    pub async fn put_document(
        &self,
        document_id: &str,
        document: &Value,
    ) -> Result<Value, CouchDbError> {
        let encoded_id = urlencoding::encode(document_id);
        let url = format!("{}/{}", self.db_base_url(), encoded_id);
        let response = self
            .send_request_with_timeout(
                self.http
                    .put(url)
                    .basic_auth(&self.username, Some(&self.password))
                    .json(document),
                COUCHDB_REQUEST_TIMEOUT,
            )
            .await?;

        if response.status() == reqwest::StatusCode::CONFLICT {
            return Err(CouchDbError::Conflict {
                document_id: document_id.to_string(),
            });
        }

        let response = response.error_for_status()?;

        self.read_json_with_timeout(response, COUCHDB_REQUEST_TIMEOUT)
            .await
    }

    pub async fn delete_document(&self, document_id: &str) -> Result<bool, CouchDbError> {
        let Some(document) = self.get_document(document_id).await? else {
            return Ok(false);
        };
        let rev = document
            .get("_rev")
            .and_then(Value::as_str)
            .ok_or_else(|| {
                CouchDbError::InvalidResponse(format!(
                    "document {document_id} is missing _rev; cannot delete"
                ))
            })?;

        let encoded_id = urlencoding::encode(document_id);
        let encoded_rev = urlencoding::encode(rev);
        let url = format!("{}/{}?rev={}", self.db_base_url(), encoded_id, encoded_rev);
        let response = self
            .send_request_with_timeout(
                self.http
                    .delete(url)
                    .basic_auth(&self.username, Some(&self.password)),
                COUCHDB_REQUEST_TIMEOUT,
            )
            .await?;

        if response.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(false);
        }

        response.error_for_status()?;
        Ok(true)
    }

    pub async fn get_document(&self, document_id: &str) -> Result<Option<Value>, CouchDbError> {
        let encoded_id = urlencoding::encode(document_id);
        let url = format!("{}/{}", self.db_base_url(), encoded_id);
        let response = self
            .send_request_with_timeout(
                self.http
                    .get(url)
                    .basic_auth(&self.username, Some(&self.password)),
                COUCHDB_REQUEST_TIMEOUT,
            )
            .await?;

        if response.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(None);
        }

        let response = response.error_for_status()?;
        let document = self
            .read_json_with_timeout(response, COUCHDB_REQUEST_TIMEOUT)
            .await?;
        Ok(Some(document))
    }

    async fn get_document_revision(
        &self,
        document_id: &str,
        revision: &str,
        include_revision_history: bool,
    ) -> Result<Option<Value>, CouchDbError> {
        let encoded_id = urlencoding::encode(document_id);
        let encoded_revision = urlencoding::encode(revision);
        let url = format!(
            "{}/{}?rev={}&revs={}",
            self.db_base_url(),
            encoded_id,
            encoded_revision,
            include_revision_history
        );
        let response = self
            .send_request_with_timeout(
                self.http
                    .get(url)
                    .basic_auth(&self.username, Some(&self.password)),
                COUCHDB_REQUEST_TIMEOUT,
            )
            .await?;

        if response.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(None);
        }

        let response = response.error_for_status()?;
        self.read_json_with_timeout(response, COUCHDB_REQUEST_TIMEOUT)
            .await
            .map(Some)
    }

    pub async fn fetch_document_revisions(
        &self,
        document_ids: &[String],
    ) -> Result<HashMap<String, String>, CouchDbError> {
        if document_ids.is_empty() {
            return Ok(HashMap::new());
        }

        let mut revisions = HashMap::new();

        for chunk in document_ids.chunks(500) {
            let response = self
                .send_request_with_timeout(
                    self.http
                        .post(format!("{}/_all_docs", self.db_base_url()))
                        .basic_auth(&self.username, Some(&self.password))
                        .json(&serde_json::json!({ "keys": chunk })),
                    COUCHDB_REQUEST_TIMEOUT,
                )
                .await?
                .error_for_status()?;
            let page: BulkAllDocsPage = self
                .read_json_with_timeout(response, COUCHDB_REQUEST_TIMEOUT)
                .await?;

            for row in page.rows {
                if row.error.as_deref() == Some("not_found") {
                    continue;
                }
                let Some(value) = row.value else {
                    continue;
                };
                let doc_id = row.id.unwrap_or(row.key);
                revisions.insert(doc_id, value.rev);
            }
        }

        Ok(revisions)
    }

    async fn fetch_existing_documents(
        &self,
        document_ids: &[String],
    ) -> Result<HashMap<String, Value>, CouchDbError> {
        if document_ids.is_empty() {
            return Ok(HashMap::new());
        }

        let mut documents = HashMap::new();

        for chunk in document_ids.chunks(500) {
            let response = self
                .send_request_with_timeout(
                    self.http
                        .post(format!("{}/_all_docs", self.db_base_url()))
                        .basic_auth(&self.username, Some(&self.password))
                        .json(&serde_json::json!({
                            "keys": chunk,
                            "include_docs": true
                        })),
                    COUCHDB_REQUEST_TIMEOUT,
                )
                .await?
                .error_for_status()?;
            let page: BulkAllDocsPage = self
                .read_json_with_timeout(response, COUCHDB_REQUEST_TIMEOUT)
                .await?;
            for row in page.rows {
                if row.error.is_some() {
                    continue;
                }
                let Some(doc) = row.doc else {
                    continue;
                };
                documents.insert(row.id.unwrap_or(row.key), doc);
            }
        }

        Ok(documents)
    }

    pub async fn classify_document_states(
        &self,
        document_ids: &[String],
    ) -> Result<Vec<SourceChildState>, CouchDbError> {
        let mut states = Vec::with_capacity(document_ids.len());
        for (chunk_index, chunk) in document_ids.chunks(500).enumerate() {
            let response = self
                .send_request_with_timeout(
                    self.http
                        .post(format!("{}/_all_docs", self.db_base_url()))
                        .basic_auth(&self.username, Some(&self.password))
                        .json(&serde_json::json!({ "keys": chunk, "include_docs": false })),
                    COUCHDB_REQUEST_TIMEOUT,
                )
                .await?
                .error_for_status()?;
            let page: BulkAllDocsPage = self
                .read_json_with_timeout(response, COUCHDB_REQUEST_TIMEOUT)
                .await?;
            for (row_index, row) in page.rows.into_iter().enumerate() {
                let state = if row.value.as_ref().is_some_and(|value| value.deleted) {
                    SourceDocumentState::Tombstoned
                } else if row.error.as_deref() == Some("not_found") {
                    SourceDocumentState::Missing
                } else if row.value.is_some() {
                    SourceDocumentState::Live
                } else {
                    SourceDocumentState::Unknown
                };
                states.push(SourceChildState {
                    ordinal: chunk_index * 500 + row_index,
                    document_id: row.id.unwrap_or(row.key),
                    state,
                });
            }
        }
        Ok(states)
    }

    pub async fn diagnose_note_source(
        &self,
        note_path: &str,
        decryptor: Option<&Decryptor>,
    ) -> Result<LiveSyncSourceDiagnostic, CouchDbError> {
        let Some(file_doc) = self
            .find_file_document_by_note_path(note_path, decryptor)
            .await?
        else {
            return Ok(LiveSyncSourceDiagnostic {
                file_document_exists: false,
                file_document_id: None,
                file_revision: None,
                expected_child_count: 0,
                live_child_count: 0,
                missing_child_count: 0,
                tombstoned_child_count: 0,
                unknown_child_count: 0,
                children: Vec::new(),
            });
        };

        let mut file = match LivesyncDocument::try_from(file_doc) {
            Ok(LivesyncDocument::File(file)) => file,
            _ => {
                return Err(CouchDbError::InvalidResponse(
                    "matched note path did not decode as a LiveSync file document".to_string(),
                ));
            }
        };
        hydrate_file_from_encrypted_metadata(&mut file, decryptor)?;
        let child_ids = file
            .children
            .iter()
            .filter_map(child_doc_id)
            .collect::<Vec<_>>();
        let children = self.classify_document_states(&child_ids).await?;
        let live_child_count = children
            .iter()
            .filter(|child| child.state == SourceDocumentState::Live)
            .count();
        let missing_child_count = children
            .iter()
            .filter(|child| child.state == SourceDocumentState::Missing)
            .count();
        let tombstoned_child_count = children
            .iter()
            .filter(|child| child.state == SourceDocumentState::Tombstoned)
            .count();
        let unknown_child_count = children
            .iter()
            .filter(|child| child.state == SourceDocumentState::Unknown)
            .count();

        Ok(LiveSyncSourceDiagnostic {
            file_document_exists: true,
            file_document_id: Some(file.id),
            file_revision: Some(file.rev),
            expected_child_count: child_ids.len(),
            live_child_count,
            missing_child_count,
            tombstoned_child_count,
            unknown_child_count,
            children,
        })
    }

    pub async fn repair_tombstoned_note_children(
        &self,
        note_path: &str,
        expected_parent_revision: &str,
        decryptor: Option<&Decryptor>,
    ) -> Result<TombstonedChildRepairResult, CouchDbError> {
        let initial = self.diagnose_note_source(note_path, decryptor).await?;
        let Some(parent_id) = initial.file_document_id.as_deref() else {
            return Ok(TombstonedChildRepairResult {
                expected_parent_revision: expected_parent_revision.to_string(),
                parent_revision_changed: true,
                attempted_child_count: 0,
                repaired_child_count: 0,
                unrecoverable_child_count: 0,
                diagnostic: initial,
            });
        };
        if initial.file_revision.as_deref() != Some(expected_parent_revision) {
            return Ok(TombstonedChildRepairResult {
                expected_parent_revision: expected_parent_revision.to_string(),
                parent_revision_changed: true,
                attempted_child_count: 0,
                repaired_child_count: 0,
                unrecoverable_child_count: 0,
                diagnostic: initial,
            });
        }

        let tombstoned_ids = initial
            .children
            .iter()
            .filter(|child| child.state == SourceDocumentState::Tombstoned)
            .map(|child| child.document_id.clone())
            .collect::<Vec<_>>();
        let mut repaired_child_count = 0usize;
        let mut unrecoverable_child_count = 0usize;
        let mut parent_revision_changed = false;

        for child_id in &tombstoned_ids {
            let Some(parent_doc) = self.get_document(parent_id).await? else {
                parent_revision_changed = true;
                break;
            };
            let Ok(LivesyncDocument::File(mut parent)) = LivesyncDocument::try_from(parent_doc)
            else {
                return Err(CouchDbError::InvalidResponse(format!(
                    "document {parent_id} no longer decodes as a LiveSync file"
                )));
            };
            hydrate_file_from_encrypted_metadata(&mut parent, decryptor)?;
            if parent.rev != expected_parent_revision
                || !parent
                    .children
                    .iter()
                    .filter_map(child_doc_id)
                    .any(|current_id| current_id == *child_id)
            {
                parent_revision_changed = true;
                break;
            }

            let current_state = self
                .classify_document_states(std::slice::from_ref(child_id))
                .await?
                .into_iter()
                .next()
                .map(|child| child.state)
                .unwrap_or(SourceDocumentState::Unknown);
            if current_state == SourceDocumentState::Live {
                continue;
            }
            if current_state != SourceDocumentState::Tombstoned {
                unrecoverable_child_count += 1;
                continue;
            }

            let Some(tombstone_revision) = self
                .fetch_document_revisions(std::slice::from_ref(child_id))
                .await?
                .remove(child_id)
            else {
                unrecoverable_child_count += 1;
                continue;
            };
            let Some(mut recovered) = self
                .recover_live_leaf_revision(child_id, &tombstone_revision, decryptor)
                .await?
            else {
                unrecoverable_child_count += 1;
                continue;
            };
            let Some(object) = recovered.as_object_mut() else {
                unrecoverable_child_count += 1;
                continue;
            };
            object.remove("_deleted");
            object.remove("deleted");
            object.remove("_revisions");
            object.remove("_revs_info");
            object.remove("_conflicts");
            object.remove("_rev");
            match self.put_document(child_id, &recovered).await {
                Ok(_) => repaired_child_count += 1,
                Err(CouchDbError::Conflict { .. }) => {
                    let state = self
                        .classify_document_states(std::slice::from_ref(child_id))
                        .await?
                        .into_iter()
                        .next()
                        .map(|child| child.state)
                        .unwrap_or(SourceDocumentState::Unknown);
                    if state != SourceDocumentState::Live {
                        return Err(CouchDbError::Conflict {
                            document_id: child_id.clone(),
                        });
                    }
                }
                Err(error) => return Err(error),
            }
        }

        let diagnostic = self.diagnose_note_source(note_path, decryptor).await?;
        parent_revision_changed |=
            diagnostic.file_revision.as_deref() != Some(expected_parent_revision);
        Ok(TombstonedChildRepairResult {
            expected_parent_revision: expected_parent_revision.to_string(),
            parent_revision_changed,
            attempted_child_count: tombstoned_ids.len(),
            repaired_child_count,
            unrecoverable_child_count,
            diagnostic,
        })
    }

    async fn recover_live_leaf_revision(
        &self,
        document_id: &str,
        tombstone_revision: &str,
        decryptor: Option<&Decryptor>,
    ) -> Result<Option<Value>, CouchDbError> {
        let Some(tombstone) = self
            .get_document_revision(document_id, tombstone_revision, true)
            .await?
        else {
            return Ok(None);
        };
        let Some(revisions) = tombstone.get("_revisions") else {
            return Ok(None);
        };
        let Some(start) = revisions.get("start").and_then(Value::as_u64) else {
            return Ok(None);
        };
        let Some(revision_ids) = revisions.get("ids").and_then(Value::as_array) else {
            return Ok(None);
        };

        let Some(revision_id) = revision_ids.get(1).and_then(Value::as_str) else {
            return Ok(None);
        };
        let Some(generation) = start.checked_sub(1) else {
            return Ok(None);
        };
        let revision = format!("{generation}-{revision_id}");
        let Some(document) = self
            .get_document_revision(document_id, &revision, false)
            .await?
        else {
            return Ok(None);
        };
        if recoverable_leaf_document(document_id, &document, decryptor) {
            Ok(Some(document))
        } else {
            Ok(None)
        }
    }

    pub async fn find_file_document_by_note_path(
        &self,
        note_path: &str,
        decryptor: Option<&Decryptor>,
    ) -> Result<Option<Value>, CouchDbError> {
        let note_paths = vec![note_path.to_string()];
        let mut documents = self
            .find_file_documents_by_note_paths(&note_paths, decryptor)
            .await?;
        Ok(documents.remove(&normalize_note_path(note_path)))
    }

    pub async fn find_file_documents_by_note_paths(
        &self,
        note_paths: &[String],
        decryptor: Option<&Decryptor>,
    ) -> Result<HashMap<String, Value>, CouchDbError> {
        let targets = note_paths
            .iter()
            .map(|path| normalize_note_path(path))
            .collect::<HashSet<_>>();
        if targets.is_empty() {
            return Ok(HashMap::new());
        }

        let mut found = HashMap::new();
        let page_size = 500usize;
        let mut startkey: Option<String> = None;

        loop {
            let limit = page_size.to_string();
            let mut query = vec![("include_docs", "true".to_string()), ("limit", limit)];
            if let Some(start) = startkey.as_ref() {
                query.push((
                    "startkey",
                    serde_json::to_string(start).expect("document ID should serialize as JSON"),
                ));
                query.push(("skip", "1".to_string()));
            }

            let response = self
                .send_request_with_timeout(
                    self.http
                        .get(format!("{}/_all_docs", self.db_base_url()))
                        .basic_auth(&self.username, Some(&self.password))
                        .query(&query),
                    COUCHDB_REQUEST_TIMEOUT,
                )
                .await?
                .error_for_status()?;
            let page: AllDocsPage = self
                .read_json_with_timeout(response, COUCHDB_REQUEST_TIMEOUT)
                .await?;
            let row_count = page.rows.len();
            if row_count == 0 {
                return Ok(found);
            }

            let mut last_seen_id = None;

            for row in page.rows {
                last_seen_id = Some(row.id.clone());
                let Some(doc) = row.doc else {
                    continue;
                };
                let Ok(LivesyncDocument::File(mut file)) = LivesyncDocument::try_from(doc.clone())
                else {
                    continue;
                };
                if crate::encryption::is_encrypted_meta_path(&file.path) {
                    let Ok(decrypted_path) =
                        crate::encryption::maybe_decrypt_meta_path(decryptor, &file.path)
                    else {
                        continue;
                    };
                    file.path = decrypted_path;
                }
                let normalized_path = normalize_note_path(&file.path);
                if targets.contains(&normalized_path) {
                    found.entry(normalized_path).or_insert(doc);
                    if found.len() == targets.len() {
                        return Ok(found);
                    }
                }
            }

            if row_count < page_size {
                return Ok(found);
            }
            let Some(last_id) = last_seen_id else {
                return Ok(found);
            };
            startkey = Some(last_id);
        }
    }

    pub async fn delete_note_file_document_by_note_path(
        &self,
        note_path: &str,
        decryptor: Option<&Decryptor>,
    ) -> Result<Vec<String>, CouchDbError> {
        let Some(file_doc) = self
            .find_file_document_by_note_path(note_path, decryptor)
            .await?
        else {
            return Ok(Vec::new());
        };

        let Ok(LivesyncDocument::File(file)) = LivesyncDocument::try_from(file_doc) else {
            return Err(CouchDbError::InvalidResponse(
                "matched note path did not decode as a LiveSync file document".to_string(),
            ));
        };

        if self.delete_document(&file.id).await? {
            return Ok(vec![file.id]);
        }
        Ok(Vec::new())
    }

    /// Fetch the PBKDF2 salt from the LiveSync sync parameters document.
    ///
    /// Returns the raw salt bytes, or `None` if the document doesn't exist or
    /// has no `pbkdf2salt` field.
    pub async fn fetch_livesync_pbkdf2_salt(&self) -> Result<Option<Vec<u8>>, CouchDbError> {
        let doc = self
            .get_document("_local/obsidian_livesync_sync_parameters")
            .await?;
        let Some(doc) = doc else {
            return Ok(None);
        };
        let Some(salt_b64) = doc.get("pbkdf2salt").and_then(Value::as_str) else {
            return Ok(None);
        };
        if salt_b64.is_empty() {
            return Ok(None);
        }
        use base64::Engine;
        use base64::engine::general_purpose::STANDARD as BASE64;
        let salt = BASE64.decode(salt_b64).map_err(|e| {
            CouchDbError::InvalidResponse(format!("invalid base64 in pbkdf2salt: {e}"))
        })?;
        Ok(Some(salt))
    }

    /// Write a new markdown note to CouchDB using the Livesync-compatible
    /// file/leaf document split modeled in Appendix C.
    pub async fn write_livesync_note(
        &self,
        note_path: &str,
        markdown: &str,
    ) -> Result<LiveSyncWriteReceipt, CouchDbError> {
        let docs = match self.livesync_crypto.as_deref() {
            Some(crypto) => build_native_encrypted_livesync_note_documents(
                note_path,
                markdown,
                crypto,
                self.livesync_passphrase.as_deref(),
            )?,
            None => build_livesync_note_documents_with_crypto(note_path, markdown, None)?,
        };
        for (leaf_id, leaf_doc) in &docs.leaf_docs {
            self.put_document(leaf_id, leaf_doc)
                .await
                .map_err(|error| match error {
                    CouchDbError::Conflict { .. } => CouchDbError::NoteAlreadyExists {
                        note_path: note_path.to_string(),
                    },
                    other => other,
                })?;
        }
        let file_response = self
            .put_document(&docs.file_id, &docs.file_doc)
            .await
            .map_err(|error| match error {
                CouchDbError::Conflict { .. } => CouchDbError::NoteAlreadyExists {
                    note_path: note_path.to_string(),
                },
                other => other,
            })?;
        let couchdb_rev = write_response_revision(&file_response, &docs.file_id)?;
        info!(
            note_path,
            file_id = %docs.file_id,
            leaf_id = %docs.leaf_id,
            leaf_count = docs.leaf_docs.len(),
            encrypted = self.livesync_crypto.is_some(),
            bytes = markdown.len(),
            "wrote LiveSync note to CouchDB"
        );
        Ok(LiveSyncWriteReceipt { couchdb_rev })
    }

    /// Update an existing markdown note in CouchDB.
    ///
    /// Fetches the current file document and its leaf children, rebuilds
    /// them with the new markdown content, and merges `_rev` values so CouchDB
    /// accepts the updates. Superseded content-addressed leaves are retained
    /// because they may be shared or reused by concurrent LiveSync writers.
    pub async fn update_livesync_note(
        &self,
        note_path: &str,
        markdown: &str,
    ) -> Result<LiveSyncWriteReceipt, CouchDbError> {
        self.update_livesync_note_if_revision(note_path, markdown, None)
            .await
    }

    pub async fn update_livesync_note_if_revision(
        &self,
        note_path: &str,
        markdown: &str,
        expected_revision: Option<&str>,
    ) -> Result<LiveSyncWriteReceipt, CouchDbError> {
        let new_docs = match self.livesync_crypto.as_deref() {
            Some(crypto) => build_native_encrypted_livesync_note_documents(
                note_path,
                markdown,
                crypto,
                self.livesync_passphrase.as_deref(),
            )?,
            None => build_livesync_note_documents_with_crypto(note_path, markdown, None)?,
        };

        // Fetch the existing file document to get its _rev and current child list.
        let existing_file = self.get_document(&new_docs.file_id).await?.ok_or_else(|| {
            CouchDbError::NoteNotFound {
                note_path: note_path.to_string(),
            }
        })?;

        let file_rev = existing_file
            .get("_rev")
            .and_then(Value::as_str)
            .ok_or_else(|| {
                CouchDbError::InvalidResponse(format!(
                    "file document {} is missing _rev",
                    new_docs.file_id
                ))
            })?
            .to_string();
        if let Some(expected_revision) = expected_revision
            && expected_revision != file_rev
        {
            return Err(CouchDbError::RevisionConflict {
                note_path: note_path.to_string(),
                expected_revision: expected_revision.to_string(),
                actual_revision: file_rev,
            });
        }

        let old_leaf_ids = self.existing_file_leaf_ids(&existing_file)?;

        // Fetch revisions for all old leaves so we can update or delete them.
        let mut all_doc_ids = old_leaf_ids.clone();
        for (leaf_id, _) in &new_docs.leaf_docs {
            if !all_doc_ids.contains(leaf_id) {
                all_doc_ids.push(leaf_id.clone());
            }
        }
        let revisions = self.fetch_document_revisions(&all_doc_ids).await?;

        let new_leaf_ids: HashSet<String> = new_docs
            .leaf_docs
            .iter()
            .map(|(id, _)| id.clone())
            .collect();

        // Write updated leaf documents (with _rev merged in).
        for (leaf_id, mut leaf_doc) in new_docs.leaf_docs {
            if let Some(rev) = revisions.get(&leaf_id) {
                leaf_doc["_rev"] = Value::String(rev.clone());
            }
            self.put_document(&leaf_id, &leaf_doc).await?;
        }

        // Update the file document before deleting stale leaves so clients can
        // observe the new child list before any now-obsolete chunks disappear.
        let mut file_doc = new_docs.file_doc;
        file_doc["_rev"] = Value::String(file_rev);
        let file_response = self.put_document(&new_docs.file_id, &file_doc).await?;
        let couchdb_rev = write_response_revision(&file_response, &new_docs.file_id)?;

        // LiveSync leaves are content-addressed and may be shared by other
        // parents or reused by a newer revision. Online deletion cannot be
        // made race-free while external LiveSync writers are active.

        info!(
            note_path,
            file_id = %new_docs.file_id,
            leaf_id = %new_docs.leaf_id,
            leaf_count = new_leaf_ids.len(),
            old_leaf_count = old_leaf_ids.len(),
            encrypted = self.livesync_crypto.is_some(),
            bytes = markdown.len(),
            "updated LiveSync note in CouchDB"
        );
        Ok(LiveSyncWriteReceipt { couchdb_rev })
    }

    fn existing_file_leaf_ids(&self, existing_file: &Value) -> Result<Vec<String>, CouchDbError> {
        let mut leaf_ids = Vec::new();
        let mut seen = HashSet::new();

        if let Ok(LivesyncDocument::File(mut file)) =
            LivesyncDocument::try_from(existing_file.clone())
        {
            hydrate_file_from_encrypted_metadata(&mut file, self.livesync_crypto.as_deref())?;
            for child in &file.children {
                if let Some(child_id) = child_doc_id(child)
                    && seen.insert(child_id.clone())
                {
                    leaf_ids.push(child_id);
                }
            }
            return Ok(leaf_ids);
        }

        if let Some(children) = existing_file.get("children").and_then(Value::as_array) {
            for child in children {
                if let Some(child_id) = child_doc_id(child)
                    && seen.insert(child_id.clone())
                {
                    leaf_ids.push(child_id);
                }
            }
        }

        Ok(leaf_ids)
    }

    /// Best-effort recovery helper for stale chunk staging parents.
    ///
    /// When chunk staging times out we re-fetch the parent file document and
    /// its leaf chunks so Worker A can attempt reassembly again.
    pub async fn fetch_parent_recovery_changes(
        &self,
        parent_id: &str,
    ) -> Result<Vec<ChangeEvent>, CouchDbError> {
        let mut queue = VecDeque::from(self.recovery_candidate_doc_ids(parent_id));
        let mut visited = HashSet::new();
        let mut queued_ids = HashSet::new();
        let mut emitted = HashSet::new();
        let mut events = Vec::new();

        for id in queue.iter() {
            queued_ids.insert(id.clone());
        }

        while let Some(doc_id) = queue.pop_front() {
            if !visited.insert(doc_id.clone()) {
                continue;
            }

            let Some(doc) = self
                .fetch_existing_documents(std::slice::from_ref(&doc_id))
                .await?
                .remove(&doc_id)
            else {
                continue;
            };

            let resolved_id = doc
                .get("_id")
                .and_then(Value::as_str)
                .map(ToString::to_string)
                .unwrap_or_else(|| doc_id.clone());
            let parsed_doc = LivesyncDocument::try_from(doc.clone()).ok();
            let suppress_standalone_leaf = parent_id.starts_with("h:")
                && resolved_id == parent_id
                && matches!(parsed_doc, Some(LivesyncDocument::Leaf(_)));
            if !suppress_standalone_leaf && emitted.insert(resolved_id.clone()) {
                let deleted = doc
                    .get("deleted")
                    .or_else(|| doc.get("_deleted"))
                    .and_then(Value::as_bool)
                    .unwrap_or(false);
                events.push(ChangeEvent {
                    // Recovery events are synthetic and must not advance
                    // sync_state.last_seq when ingested.
                    seq: Value::String(String::new()),
                    id: resolved_id,
                    deleted,
                    doc: Some(doc.clone()),
                });
            }

            match parsed_doc {
                Some(LivesyncDocument::File(mut file)) => {
                    if let Some(decryptor) = self.livesync_crypto.as_deref()
                        && crate::encryption::is_encrypted_meta_path(&file.path)
                        && let Ok(meta) = decryptor.decrypt_meta_document(&file.path)
                        && let Some(children) = meta.get("children").and_then(Value::as_array)
                    {
                        file.children = children.clone();
                    }
                    for child in file.children {
                        let Some(child_id) = child_doc_id(&child) else {
                            continue;
                        };
                        if visited.contains(&child_id) || !queued_ids.insert(child_id.clone()) {
                            continue;
                        }
                        queue.push_back(child_id);
                    }
                }
                Some(LivesyncDocument::Leaf(leaf)) => {
                    if let Ok(chunk) = decode_leaf_chunk(&leaf, Utc::now()) {
                        for candidate in self.recovery_candidate_doc_ids(&chunk.parent_id) {
                            if visited.contains(&candidate) || !queued_ids.insert(candidate.clone())
                            {
                                continue;
                            }
                            queue.push_back(candidate);
                        }
                    }
                }
                _ => {}
            }
        }

        events.sort_by(|a, b| a.id.cmp(&b.id));
        Ok(events)
    }

    pub async fn fetch_documents_as_changes(
        &self,
        document_ids: &[String],
    ) -> Result<Vec<ChangeEvent>, CouchDbError> {
        if document_ids.is_empty() {
            return Ok(Vec::new());
        }

        let documents = self.fetch_existing_documents(document_ids).await?;
        let mut events = Vec::new();
        let mut emitted = HashSet::new();

        for doc_id in document_ids {
            let Some(doc) = documents.get(doc_id) else {
                continue;
            };
            let resolved_id = doc
                .get("_id")
                .and_then(Value::as_str)
                .unwrap_or(doc_id.as_str())
                .to_string();
            if !emitted.insert(resolved_id.clone()) {
                continue;
            }
            let deleted = doc
                .get("deleted")
                .or_else(|| doc.get("_deleted"))
                .and_then(Value::as_bool)
                .unwrap_or(false);
            events.push(ChangeEvent {
                seq: Value::String(String::new()),
                id: resolved_id,
                deleted,
                doc: Some(doc.clone()),
            });
        }

        Ok(events)
    }

    pub async fn deleted_recovery_document_id(
        &self,
        parent_id: &str,
    ) -> Result<Option<String>, CouchDbError> {
        let document_ids = self.recovery_candidate_doc_ids(parent_id);
        if document_ids.is_empty() {
            return Ok(None);
        }

        let response = self
            .send_request_with_timeout(
                self.http
                    .post(format!("{}/_all_docs", self.db_base_url()))
                    .basic_auth(&self.username, Some(&self.password))
                    .json(&serde_json::json!({
                        "keys": document_ids,
                        "include_docs": false
                    })),
                COUCHDB_REQUEST_TIMEOUT,
            )
            .await?
            .error_for_status()?;
        let page: BulkAllDocsPage = self
            .read_json_with_timeout(response, COUCHDB_REQUEST_TIMEOUT)
            .await?;

        let mut deleted_document_id = None;
        for row in page.rows {
            let Some(value) = row.value else {
                continue;
            };
            if !value.deleted {
                return Ok(None);
            }
            deleted_document_id.get_or_insert_with(|| row.id.unwrap_or(row.key));
        }
        Ok(deleted_document_id)
    }

    pub async fn find_file_document_changes_by_note_paths(
        &self,
        note_paths: &[String],
        decryptor: Option<&Decryptor>,
    ) -> Result<Vec<ChangeEvent>, CouchDbError> {
        let documents = self
            .find_file_documents_by_note_paths(note_paths, decryptor)
            .await?;
        if documents.is_empty() {
            return Ok(Vec::new());
        }

        let mut events = Vec::new();
        let mut emitted = HashSet::new();
        for doc in documents.into_values() {
            let resolved_id = doc
                .get("_id")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string();
            if !emitted.insert(resolved_id.clone()) {
                continue;
            }
            let deleted = doc
                .get("deleted")
                .or_else(|| doc.get("_deleted"))
                .and_then(Value::as_bool)
                .unwrap_or(false);
            events.push(ChangeEvent {
                seq: Value::String(String::new()),
                id: resolved_id,
                deleted,
                doc: Some(doc),
            });
        }

        events.sort_by(|a, b| a.id.cmp(&b.id));
        Ok(events)
    }

    fn recovery_candidate_doc_ids(&self, parent_id: &str) -> Vec<String> {
        recovery_candidate_doc_ids(parent_id, self.livesync_passphrase.as_deref())
    }
}

fn write_response_revision(response: &Value, document_id: &str) -> Result<String, CouchDbError> {
    response
        .get("rev")
        .and_then(Value::as_str)
        .map(ToString::to_string)
        .ok_or_else(|| {
            CouchDbError::InvalidResponse(format!(
                "write response for {document_id} is missing rev"
            ))
        })
}

#[derive(Debug, Clone)]
pub struct LivesyncWriteDocuments {
    pub file_id: String,
    pub leaf_id: String,
    pub file_doc: Value,
    pub leaf_doc: Value,
    pub leaf_docs: Vec<(String, Value)>,
}

pub fn build_livesync_note_documents(note_path: &str, markdown: &str) -> LivesyncWriteDocuments {
    build_livesync_note_documents_with_crypto(note_path, markdown, None)
        .expect("plaintext Livesync document generation should not fail")
}

type LeafDocumentParts = (String, Value, Vec<(String, Value)>, Vec<Value>);

fn build_leaf_documents<FId, FPayload>(
    markdown: &str,
    mut leaf_id_for_chunk: FId,
    mut encode_chunk: FPayload,
) -> Result<LeafDocumentParts, EncryptionError>
where
    FId: FnMut(&str) -> String,
    FPayload: FnMut(&str) -> Result<String, EncryptionError>,
{
    let chunks = split_markdown_chunks(markdown, 4_096);
    let mut leaf_docs = Vec::new();
    let mut metadata_children = Vec::with_capacity(chunks.len());
    let mut seen_leaf_ids = HashSet::new();

    for chunk in chunks {
        let leaf_id = leaf_id_for_chunk(&chunk);
        metadata_children.push(Value::String(leaf_id.clone()));

        if seen_leaf_ids.insert(leaf_id.clone()) {
            leaf_docs.push((
                leaf_id.clone(),
                serde_json::json!({
                    "_id": leaf_id,
                    "data": encode_chunk(&chunk)?,
                    "type": "leaf",
                    "e_": true
                }),
            ));
        }
    }

    let (leaf_id, leaf_doc) = leaf_docs
        .first()
        .cloned()
        .expect("split_markdown_chunks should produce at least one chunk");

    Ok((leaf_id, leaf_doc, leaf_docs, metadata_children))
}

fn build_native_encrypted_livesync_note_documents(
    note_path: &str,
    markdown: &str,
    livesync_crypto: &Decryptor,
    livesync_passphrase: Option<&str>,
) -> Result<LivesyncWriteDocuments, EncryptionError> {
    let normalized_path = normalize_note_path(note_path);
    let now = Utc::now();
    let ctime = now.timestamp_millis();
    let mtime = ctime;
    let size = markdown.len() as i64;
    let file_id = native_file_doc_id_for_path(&normalized_path, livesync_passphrase);
    let (leaf_id, leaf_doc, leaf_docs, metadata_children) = build_leaf_documents(
        markdown,
        |chunk| native_leaf_doc_id_for_content(chunk, livesync_passphrase),
        |chunk| livesync_crypto.encrypt(chunk),
    )?;

    let file_doc = serde_json::json!({
        "_id": file_id.clone(),
        "children": [],
        "path": livesync_crypto.encrypt_meta_path(
            &normalized_path,
            mtime,
            ctime,
            size,
            &metadata_children,
        )?,
        "ctime": 0,
        "mtime": 0,
        "size": 0,
        "type": "newnote",
        "eden": {}
    });

    Ok(LivesyncWriteDocuments {
        file_id,
        leaf_id,
        file_doc,
        leaf_doc,
        leaf_docs,
    })
}

pub fn build_livesync_note_documents_with_crypto(
    note_path: &str,
    markdown: &str,
    livesync_crypto: Option<&Decryptor>,
) -> Result<LivesyncWriteDocuments, EncryptionError> {
    let normalized_path = note_path.trim().trim_start_matches('/').replace('\\', "/");
    let file_id = file_doc_id_for_path(&normalized_path);
    let ctime = 0;
    let mtime = 0;
    let size = markdown.len() as i64;

    // LiveSync external integrations expect file docs keyed by lowercase vault
    // path and leaf docs containing note chunks referenced from file.children.
    let (leaf_id, leaf_doc, leaf_docs, children) = build_leaf_documents(
        markdown,
        plain_leaf_doc_id_for_content,
        |chunk| match livesync_crypto {
            Some(crypto) => crypto.encrypt(chunk),
            None => Ok(chunk.to_string()),
        },
    )?;
    let file_path = match livesync_crypto {
        Some(crypto) => {
            crypto.encrypt_meta_path(&normalized_path, mtime, ctime, size, &children)?
        }
        None => normalized_path.clone(),
    };

    let file_doc = serde_json::json!({
        "_id": file_id.clone(),
        "children": children,
        "path": file_path,
        "ctime": ctime,
        "mtime": mtime,
        "size": size,
        "type": "plain",
        "eden": {}
    });

    Ok(LivesyncWriteDocuments {
        file_id,
        leaf_id,
        file_doc,
        leaf_doc,
        leaf_docs,
    })
}

fn file_doc_id_for_path(note_path: &str) -> String {
    normalize_note_path(note_path).to_lowercase()
}

fn leaf_doc_id_for_path(note_path: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(normalize_note_path(note_path).to_lowercase().as_bytes());
    let digest = hex_encode(hasher.finalize());
    format!("h:{}", &digest[..16])
}

fn plain_leaf_doc_id_for_content(chunk: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"leaf:");
    hasher.update(chunk.as_bytes());
    hasher.update(b"-");
    hasher.update(chunk.len().to_string().as_bytes());
    format!("h:{}", hex_encode(hasher.finalize()))
}

fn native_file_doc_id_for_path(note_path: &str, livesync_passphrase: Option<&str>) -> String {
    let normalized = normalize_note_path(note_path).to_ascii_lowercase();
    let Some(passphrase) = livesync_passphrase.filter(|passphrase| !passphrase.is_empty()) else {
        return normalized;
    };

    // Match LiveSync's path2id_base contract for case-insensitive,
    // path-obfuscated note IDs: f:sha256(sha256(passphrase):lowercased_path).
    let hashed_passphrase = sha256_hex(passphrase.as_bytes());
    let digest = sha256_hex(format!("{hashed_passphrase}:{normalized}").as_bytes());
    format!("f:{digest}")
}

fn native_leaf_doc_id_for_content(chunk: &str, livesync_passphrase: Option<&str>) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"native-leaf:");
    if let Some(passphrase) = livesync_passphrase.filter(|passphrase| !passphrase.is_empty()) {
        hasher.update(sha256_hex(passphrase.as_bytes()).as_bytes());
        hasher.update(b":");
    }
    hasher.update(chunk.as_bytes());
    hasher.update(b"-");
    hasher.update(chunk.len().to_string().as_bytes());
    format!("h:+{}", hex_encode(hasher.finalize()))
}

fn sha256_hex(input: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(input);
    hex_encode(hasher.finalize())
}

fn legacy_file_doc_id_for_path(note_path: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"file:");
    hasher.update(note_path.as_bytes());
    format!("f:{}", hex_encode(hasher.finalize()))
}

fn legacy_leaf_doc_id_for_path(note_path: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"leaf:");
    hasher.update(note_path.as_bytes());
    let digest = hex_encode(hasher.finalize());
    format!("h:+{}", &digest[..16])
}

fn split_markdown_chunks(markdown: &str, max_bytes: usize) -> Vec<String> {
    if markdown.is_empty() {
        return vec![String::new()];
    }

    let mut chunks = Vec::new();
    let mut start = 0usize;
    while start < markdown.len() {
        let mut end = (start + max_bytes).min(markdown.len());
        while !markdown.is_char_boundary(end) {
            end -= 1;
        }
        if end == start {
            end = markdown[start..]
                .char_indices()
                .nth(1)
                .map(|(idx, _)| start + idx)
                .unwrap_or(markdown.len());
        }
        chunks.push(markdown[start..end].to_string());
        start = end;
    }
    chunks
}

fn hydrate_file_from_encrypted_metadata(
    file: &mut FileDocument,
    decryptor: Option<&Decryptor>,
) -> Result<(), CouchDbError> {
    if !crate::encryption::is_encrypted_meta_path(&file.path) {
        return Ok(());
    }

    let Some(decryptor) = decryptor else {
        return Err(CouchDbError::InvalidResponse(
            "file metadata path is encrypted but no decryptor is configured".to_string(),
        ));
    };

    let meta = decryptor
        .decrypt_meta_document(&file.path)
        .map_err(|error| CouchDbError::InvalidResponse(error.to_string()))?;

    let Some(path) = meta.get("path").and_then(Value::as_str) else {
        return Err(CouchDbError::InvalidResponse(
            "decrypted file metadata is missing `path`".to_string(),
        ));
    };
    file.path = path.to_string();

    if let Some(children) = meta.get("children").and_then(Value::as_array) {
        file.children = children.clone();
    }
    if let Some(ctime) = meta.get("ctime").and_then(Value::as_i64) {
        file.ctime = ctime;
    }
    if let Some(mtime) = meta.get("mtime").and_then(Value::as_i64) {
        file.mtime = mtime;
    }
    if let Some(size) = meta.get("size").and_then(Value::as_i64) {
        file.size = size;
    }

    Ok(())
}

fn recoverable_leaf_document(
    document_id: &str,
    document: &Value,
    decryptor: Option<&Decryptor>,
) -> bool {
    if document
        .get("_deleted")
        .or_else(|| document.get("deleted"))
        .and_then(Value::as_bool)
        .unwrap_or(false)
    {
        return false;
    }
    let Ok(LivesyncDocument::Leaf(mut leaf)) = LivesyncDocument::try_from(document.clone()) else {
        return false;
    };
    if leaf.id != document_id {
        return false;
    }
    if leaf.e_ && crate::encryption::is_hkdf_encrypted(&leaf.data) {
        let Some(decryptor) = decryptor else {
            return false;
        };
        let Ok(decrypted) = decryptor.decrypt(&leaf.data) else {
            return false;
        };
        leaf.data = decrypted;
        leaf.e_ = false;
    }
    decode_leaf_chunk(&leaf, Utc::now()).is_ok()
}

fn recovery_candidate_doc_ids(parent_id: &str, livesync_passphrase: Option<&str>) -> Vec<String> {
    let normalized = normalize_note_path(parent_id);
    let mut ids = Vec::new();

    if normalized.to_ascii_lowercase().ends_with(".md") {
        ids.push(native_file_doc_id_for_path(
            &normalized,
            livesync_passphrase,
        ));
        ids.push(file_doc_id_for_path(&normalized));
        ids.push(legacy_file_doc_id_for_path(&normalized));
        ids.push(leaf_doc_id_for_path(&normalized));
        ids.push(legacy_leaf_doc_id_for_path(&normalized));
    }

    ids.push(parent_id.to_string());

    let mut seen = HashSet::new();
    ids.retain(|id| seen.insert(id.clone()));
    ids
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

pub fn decode_changes_body(
    mode: FeedMode,
    content_type: Option<&str>,
    body: &str,
) -> Result<ChangesFeed, CouchDbError> {
    let trimmed = body.trim();
    if trimmed.is_empty() {
        return Ok(ChangesFeed {
            last_seq: Value::String("0".to_string()),
            results: Vec::new(),
        });
    }

    if trimmed.starts_with('{')
        || content_type
            .map(|ct| ct.contains("application/json"))
            .unwrap_or(false)
    {
        match serde_json::from_str::<ChangesFeed>(trimmed) {
            Ok(parsed) => return Ok(parsed),
            Err(_) if mode == FeedMode::Continuous => {
                // Continuous feed lines are also JSON objects, but not in the
                // aggregated `ChangesFeed` shape.
                debug_assert!(!trimmed.is_empty(), "continuous body should be non-empty");
            }
            Err(error) => return Err(CouchDbError::Json(error)),
        }
    }

    // Continuous feed returns one JSON object per line.
    if mode == FeedMode::Continuous {
        return decode_continuous_lines(trimmed);
    }

    Err(CouchDbError::InvalidResponse(
        "unexpected _changes response shape".to_string(),
    ))
}

fn decode_continuous_lines(body: &str) -> Result<ChangesFeed, CouchDbError> {
    let mut results = Vec::new();
    let mut last_seq = Value::String("0".to_string());

    for line in body.lines().map(str::trim).filter(|line| !line.is_empty()) {
        parse_continuous_line(line, &mut results, &mut last_seq)?;
    }

    Ok(ChangesFeed { last_seq, results })
}

fn parse_continuous_line(
    line: &str,
    results: &mut Vec<ChangeEvent>,
    last_seq: &mut Value,
) -> Result<(), CouchDbError> {
    #[derive(Debug, Deserialize)]
    struct SeqOnly {
        last_seq: Value,
    }

    let trimmed = line.trim();
    if trimmed.is_empty() {
        return Ok(());
    }

    if let Ok(change) = serde_json::from_str::<ChangeEvent>(trimmed) {
        *last_seq = change.seq.clone();
        results.push(change);
        return Ok(());
    }

    if let Ok(seq_only) = serde_json::from_str::<SeqOnly>(trimmed) {
        *last_seq = seq_only.last_seq;
        return Ok(());
    }

    Err(CouchDbError::InvalidResponse(format!(
        "failed to decode continuous feed line: {trimmed}"
    )))
}

#[derive(Debug, Error)]
pub enum CouchDbError {
    #[error("couchdb integration is not configured")]
    NotConfigured,
    #[error("failed to build HTTP client: {0}")]
    HttpClientBuild(reqwest::Error),
    #[error("couchdb request timed out after {timeout_ms}ms")]
    RequestTimeout { timeout_ms: u64 },
    #[error("http error: {0}")]
    Http(#[from] reqwest::Error),
    #[error("json decode error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("failed to build LiveSync payload: {0}")]
    Encryption(#[from] EncryptionError),
    #[error("invalid couchdb response: {0}")]
    InvalidResponse(String),
    #[error("couchdb document already exists: {document_id}")]
    Conflict { document_id: String },
    #[error("note already exists: {note_path}")]
    NoteAlreadyExists { note_path: String },
    #[error(
        "source revision conflict for {note_path}: expected {expected_revision}, found {actual_revision}"
    )]
    RevisionConflict {
        note_path: String,
        expected_revision: String,
        actual_revision: String,
    },
    #[error("note not found: {note_path}")]
    NoteNotFound { note_path: String },
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::convert::Infallible;
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use axum::body::{Body, Bytes};
    use axum::extract::{Path, Query, State};
    use axum::http::StatusCode;
    use axum::response::Response;
    use axum::routing::get;
    use axum::{Json, Router};
    use serde::Deserialize;
    use serde_json::Value;
    use tokio::sync::Mutex;
    use tokio_stream::StreamExt;

    use super::{
        CouchDbClient, CouchDbError, SourceDocumentState, build_livesync_note_documents,
        build_livesync_note_documents_with_crypto, build_native_encrypted_livesync_note_documents,
        decode_changes_body,
    };
    use crate::config::{CouchDbConfig, EncryptionConfig, FeedMode};
    use crate::encryption::Decryptor;

    #[derive(Clone, Default)]
    struct MockCouchState {
        docs: Arc<Mutex<HashMap<String, Value>>>,
        revisions: Arc<Mutex<HashMap<(String, String), Value>>>,
        requested: Arc<Mutex<Vec<String>>>,
        operations: Arc<Mutex<Vec<String>>>,
    }

    #[derive(Debug, Default, Deserialize)]
    struct MockGetParams {
        rev: Option<String>,
        #[allow(dead_code)]
        revs: Option<bool>,
    }

    #[derive(Debug, Deserialize)]
    struct MockDeleteParams {
        rev: Option<String>,
    }

    async fn mock_get_document(
        State(state): State<MockCouchState>,
        Path((_db, doc_id)): Path<(String, String)>,
        Query(params): Query<MockGetParams>,
    ) -> (StatusCode, Json<Value>) {
        state.requested.lock().await.push(doc_id.clone());
        if let Some(revision) = params.rev.as_deref() {
            if let Some(doc) = state
                .revisions
                .lock()
                .await
                .get(&(doc_id.clone(), revision.to_string()))
                .cloned()
            {
                return (StatusCode::OK, Json(doc));
            }
            if let Some(doc) = state.docs.lock().await.get(&doc_id).cloned()
                && doc.get("_rev").and_then(Value::as_str) == Some(revision)
            {
                return (StatusCode::OK, Json(doc));
            }
            return (
                StatusCode::NOT_FOUND,
                Json(serde_json::json!({ "error": "not_found" })),
            );
        }
        if let Some(doc) = state.docs.lock().await.get(&doc_id).cloned()
            && doc.get("_deleted").and_then(Value::as_bool) != Some(true)
        {
            return (StatusCode::OK, Json(doc));
        }
        (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "error": "not_found" })),
        )
    }

    async fn mock_put_document(
        State(state): State<MockCouchState>,
        Path((_db, doc_id)): Path<(String, String)>,
        Json(mut doc): Json<Value>,
    ) -> (StatusCode, Json<Value>) {
        let current = state.docs.lock().await.get(&doc_id).cloned();
        if let Some(current) = current.as_ref() {
            let current_deleted = current
                .get("_deleted")
                .and_then(Value::as_bool)
                .unwrap_or(false);
            let current_revision = current.get("_rev").and_then(Value::as_str);
            let supplied_revision = doc.get("_rev").and_then(Value::as_str);
            let accepted = if current_deleted {
                supplied_revision.is_none()
            } else {
                supplied_revision.is_some() && supplied_revision == current_revision
            };
            if !accepted {
                return (
                    StatusCode::CONFLICT,
                    Json(serde_json::json!({ "error": "conflict" })),
                );
            }
        }

        let next_generation = current
            .as_ref()
            .and_then(|current| current.get("_rev").and_then(Value::as_str))
            .and_then(|revision| revision.split_once('-'))
            .and_then(|(generation, _)| generation.parse::<u64>().ok())
            .unwrap_or(0)
            + 1;
        let next_revision = format!("{next_generation}-mock");
        doc["_rev"] = Value::String(next_revision.clone());
        state.operations.lock().await.push(format!("put:{doc_id}"));
        state.docs.lock().await.insert(doc_id, doc);
        (
            StatusCode::CREATED,
            Json(serde_json::json!({ "ok": true, "rev": next_revision })),
        )
    }

    async fn mock_delete_document(
        State(state): State<MockCouchState>,
        Path((_db, doc_id)): Path<(String, String)>,
        Query(params): Query<MockDeleteParams>,
    ) -> (StatusCode, Json<Value>) {
        let Some(document) = state.docs.lock().await.get(&doc_id).cloned() else {
            return (
                StatusCode::NOT_FOUND,
                Json(serde_json::json!({ "error": "not_found" })),
            );
        };
        let Some(expected_rev) = document.get("_rev").and_then(Value::as_str) else {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({ "error": "missing_rev" })),
            );
        };
        if params.rev.as_deref() != Some(expected_rev) {
            return (
                StatusCode::CONFLICT,
                Json(serde_json::json!({ "error": "conflict" })),
            );
        }

        state
            .operations
            .lock()
            .await
            .push(format!("delete:{doc_id}"));
        state.docs.lock().await.remove(&doc_id);
        (StatusCode::OK, Json(serde_json::json!({ "ok": true })))
    }

    async fn mock_all_docs_scan(
        State(state): State<MockCouchState>,
        Query(query): Query<HashMap<String, String>>,
    ) -> Json<Value> {
        let include_docs = query
            .get("include_docs")
            .is_some_and(|value| value == "true");
        let docs = state.docs.lock().await;
        let mut keys = docs.keys().cloned().collect::<Vec<_>>();
        keys.sort();
        let rows = keys
            .into_iter()
            .filter_map(|key| {
                let doc = docs.get(&key)?;
                if doc.get("_deleted").and_then(Value::as_bool) == Some(true) {
                    return None;
                }
                let mut row = serde_json::json!({
                    "id": key,
                    "key": key,
                    "value": {"rev": doc.get("_rev").and_then(Value::as_str).unwrap_or("")}
                });
                if include_docs {
                    row["doc"] = doc.clone();
                }
                Some(row)
            })
            .collect::<Vec<_>>();
        Json(serde_json::json!({"rows": rows}))
    }

    async fn mock_all_docs(
        State(state): State<MockCouchState>,
        Path(_db): Path<String>,
        Json(body): Json<Value>,
    ) -> Json<Value> {
        let include_docs = body
            .get("include_docs")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        let keys = body
            .get("keys")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        let key_strings = keys
            .into_iter()
            .map(|key| key.as_str().unwrap_or_default().to_string())
            .collect::<Vec<_>>();
        state.requested.lock().await.extend(key_strings.clone());
        let docs = state.docs.lock().await;
        let rows = key_strings
            .into_iter()
            .map(|key_string| match docs.get(&key_string) {
                Some(doc) => {
                    let rev = doc.get("_rev").and_then(Value::as_str).unwrap_or("");
                    let deleted = doc
                        .get("_deleted")
                        .and_then(Value::as_bool)
                        .unwrap_or(false);
                    let mut row = serde_json::json!({
                        "id": key_string,
                        "key": key_string,
                        "value": { "rev": rev, "deleted": deleted }
                    });
                    if include_docs && !deleted {
                        row["doc"] = doc.clone();
                    }
                    row
                }
                None => serde_json::json!({
                    "key": key_string,
                    "error": "not_found"
                }),
            })
            .collect::<Vec<_>>();
        Json(serde_json::json!({ "rows": rows }))
    }

    fn spawn_mock_couchdb(docs: HashMap<String, Value>) -> (String, MockCouchState) {
        spawn_mock_couchdb_with_revisions(docs, HashMap::new())
    }

    fn spawn_mock_couchdb_with_revisions(
        docs: HashMap<String, Value>,
        revisions: HashMap<(String, String), Value>,
    ) -> (String, MockCouchState) {
        let state = MockCouchState {
            docs: Arc::new(Mutex::new(docs)),
            revisions: Arc::new(Mutex::new(revisions)),
            requested: Arc::new(Mutex::new(Vec::new())),
            operations: Arc::new(Mutex::new(Vec::new())),
        };
        let app = Router::new()
            .route(
                "/{db}/_all_docs",
                get(mock_all_docs_scan).post(mock_all_docs),
            )
            .route(
                "/{db}/{doc_id}",
                get(mock_get_document)
                    .put(mock_put_document)
                    .delete(mock_delete_document),
            )
            .with_state(state.clone());

        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind mock couchdb");
        listener
            .set_nonblocking(true)
            .expect("set mock couchdb listener non-blocking");
        let addr = listener.local_addr().expect("mock couchdb local addr");
        tokio::spawn(async move {
            let listener =
                tokio::net::TcpListener::from_std(listener).expect("tokio listener from std");
            axum::serve(listener, app)
                .await
                .expect("serve mock couchdb");
        });

        (format!("http://{addr}"), state)
    }

    #[tokio::test]
    async fn classifies_live_missing_and_tombstoned_documents_without_content() {
        let docs = HashMap::from([
            (
                "h:live".to_string(),
                serde_json::json!({"_id": "h:live", "_rev": "1-live", "data": "private"}),
            ),
            (
                "h:deleted".to_string(),
                serde_json::json!({"_id": "h:deleted", "_rev": "2-deleted", "_deleted": true}),
            ),
        ]);
        let (url, _) = spawn_mock_couchdb(docs);
        let client = client_for(url);
        let states = client
            .classify_document_states(&[
                "h:live".to_string(),
                "h:missing".to_string(),
                "h:deleted".to_string(),
            ])
            .await
            .expect("classify documents");

        assert_eq!(states[0].state, SourceDocumentState::Live);
        assert_eq!(states[1].state, SourceDocumentState::Missing);
        assert_eq!(states[2].state, SourceDocumentState::Tombstoned);
        assert_eq!(states[0].ordinal, 0);
    }

    #[tokio::test]
    async fn diagnoses_file_metadata_with_missing_and_tombstoned_children() {
        let docs = HashMap::from([
            (
                "f:test".to_string(),
                serde_json::json!({
                    "_id": "f:test",
                    "_rev": "7-file",
                    "type": "newnote",
                    "path": "Notes/partial.md",
                    "children": ["h:live", "h:missing", "h:deleted"],
                    "ctime": 0,
                    "mtime": 0,
                    "size": 10,
                    "eden": {}
                }),
            ),
            (
                "h:live".to_string(),
                serde_json::json!({"_id": "h:live", "_rev": "1-live", "type": "leaf", "data": "secret"}),
            ),
            (
                "h:deleted".to_string(),
                serde_json::json!({"_id": "h:deleted", "_rev": "2-deleted", "_deleted": true}),
            ),
        ]);
        let (url, _) = spawn_mock_couchdb(docs);
        let diagnostic = client_for(url)
            .diagnose_note_source("Notes/partial.md", None)
            .await
            .expect("diagnose partial source");

        assert!(diagnostic.file_document_exists);
        assert_eq!(diagnostic.expected_child_count, 3);
        assert_eq!(diagnostic.live_child_count, 1);
        assert_eq!(diagnostic.missing_child_count, 1);
        assert_eq!(diagnostic.tombstoned_child_count, 1);
        assert_eq!(
            diagnostic.failure_kind(),
            Some("mixed_unavailable_children")
        );
        let safe_output = serde_json::to_string(&diagnostic).expect("serialize diagnostic");
        assert!(!safe_output.contains("secret"));
        assert!(!safe_output.contains("data"));
    }

    #[tokio::test]
    async fn repairs_referenced_tombstone_from_available_leaf_revision() {
        let note_path = "Notes/repair.md";
        let built = build_livesync_note_documents(note_path, "# Repair\n\nRecovered body");
        let mut file_doc = built.file_doc.clone();
        file_doc["_rev"] = Value::String("7-file".to_string());
        let mut live_leaf = built.leaf_doc.clone();
        live_leaf["_rev"] = Value::String("1-live".to_string());
        let tombstone = serde_json::json!({
            "_id": built.leaf_id,
            "_rev": "2-deleted",
            "_deleted": true,
            "_revisions": {
                "start": 2,
                "ids": ["deleted", "live"]
            }
        });
        let docs = HashMap::from([
            (built.file_id.clone(), file_doc),
            (built.leaf_id.clone(), tombstone),
        ]);
        let revisions = HashMap::from([((built.leaf_id.clone(), "1-live".to_string()), live_leaf)]);
        let (url, state) = spawn_mock_couchdb_with_revisions(docs, revisions);

        let result = client_for(url)
            .repair_tombstoned_note_children(note_path, "7-file", None)
            .await
            .expect("repair tombstoned child");

        assert!(!result.parent_revision_changed);
        assert_eq!(result.attempted_child_count, 1);
        assert_eq!(result.repaired_child_count, 1);
        assert_eq!(result.unrecoverable_child_count, 0);
        assert_eq!(result.diagnostic.live_child_count, 1);
        assert_eq!(result.diagnostic.tombstoned_child_count, 0);
        let repaired = state
            .docs
            .lock()
            .await
            .get(&built.leaf_id)
            .cloned()
            .expect("repaired leaf");
        assert_ne!(repaired.get("_deleted"), Some(&Value::Bool(true)));
        assert_eq!(repaired["_rev"], "3-mock");
        assert!(
            state
                .operations
                .lock()
                .await
                .contains(&format!("put:{}", built.leaf_id))
        );
    }

    #[tokio::test]
    async fn leaves_compacted_tombstone_unresolved_without_writing() {
        let note_path = "Notes/compacted.md";
        let built = build_livesync_note_documents(note_path, "# Compacted");
        let mut file_doc = built.file_doc.clone();
        file_doc["_rev"] = Value::String("4-file".to_string());
        let tombstone = serde_json::json!({
            "_id": built.leaf_id,
            "_rev": "2-deleted",
            "_deleted": true,
            "_revisions": {
                "start": 2,
                "ids": ["deleted", "live"]
            }
        });
        let docs = HashMap::from([
            (built.file_id, file_doc),
            (built.leaf_id.clone(), tombstone),
        ]);
        let (url, state) = spawn_mock_couchdb(docs);

        let result = client_for(url)
            .repair_tombstoned_note_children(note_path, "4-file", None)
            .await
            .expect("classify compacted child");

        assert_eq!(result.repaired_child_count, 0);
        assert_eq!(result.unrecoverable_child_count, 1);
        assert_eq!(result.diagnostic.tombstoned_child_count, 1);
        assert!(state.operations.lock().await.is_empty());
    }

    #[tokio::test]
    async fn refuses_older_leaf_when_immediate_predecessor_is_invalid() {
        let note_path = "Notes/ambiguous-history.md";
        let built = build_livesync_note_documents(note_path, "# Historical body");
        let mut file_doc = built.file_doc.clone();
        file_doc["_rev"] = Value::String("8-file".to_string());
        let tombstone = serde_json::json!({
            "_id": built.leaf_id,
            "_rev": "3-deleted",
            "_deleted": true,
            "_revisions": {
                "start": 3,
                "ids": ["deleted", "invalid", "live"]
            }
        });
        let mut older_live_leaf = built.leaf_doc.clone();
        older_live_leaf["_rev"] = Value::String("1-live".to_string());
        let docs = HashMap::from([
            (built.file_id, file_doc),
            (built.leaf_id.clone(), tombstone),
        ]);
        let revisions = HashMap::from([
            (
                (built.leaf_id.clone(), "2-invalid".to_string()),
                serde_json::json!({
                    "_id": built.leaf_id,
                    "_rev": "2-invalid",
                    "type": "unsupported",
                    "data": "not a leaf"
                }),
            ),
            (
                (built.leaf_id.clone(), "1-live".to_string()),
                older_live_leaf,
            ),
        ]);
        let (url, state) = spawn_mock_couchdb_with_revisions(docs, revisions);

        let result = client_for(url)
            .repair_tombstoned_note_children(note_path, "8-file", None)
            .await
            .expect("classify invalid immediate predecessor");

        assert_eq!(result.repaired_child_count, 0);
        assert_eq!(result.unrecoverable_child_count, 1);
        assert_eq!(result.diagnostic.tombstoned_child_count, 1);
        assert!(state.operations.lock().await.is_empty());
    }

    fn client_for(base_url: String) -> CouchDbClient {
        CouchDbClient::new(&CouchDbConfig {
            url: base_url,
            database: "mainvault".to_string(),
            username: "user".to_string(),
            password: "pass".to_string(),
            poll_interval_seconds: 1,
            feed_mode: FeedMode::Longpoll,
            ..Default::default()
        })
        .expect("build couchdb client")
    }

    fn client_for_with_passphrase(base_url: String, passphrase: &str) -> CouchDbClient {
        CouchDbClient::new(&CouchDbConfig {
            url: base_url,
            database: "mainvault".to_string(),
            username: "user".to_string(),
            password: "pass".to_string(),
            poll_interval_seconds: 1,
            feed_mode: FeedMode::Longpoll,
            encryption: EncryptionConfig {
                passphrase: passphrase.to_string(),
            },
            ..Default::default()
        })
        .expect("build couchdb client")
    }

    async fn mock_continuous_changes_stream() -> Response {
        let stream = tokio_stream::iter(vec![
            Ok::<_, Infallible>(Bytes::from(
                r#"{"seq":"41-g1AAA","id":"h:+a","deleted":false,"doc":{"_id":"h:+a"}}
{"seq":"42-g1AAA","id":"h:+b","deleted":false,"doc":{"_id":"h:+b"}}
"#,
            )),
            Ok::<_, Infallible>(Bytes::from(r#"{"last_seq":"42-g1AAA"}"#)),
        ])
        .throttle(Duration::from_millis(400));

        Response::builder()
            .status(StatusCode::OK)
            .header("content-type", "text/plain")
            .body(Body::from_stream(stream))
            .expect("streaming changes response")
    }

    fn spawn_mock_changes_stream_server() -> String {
        let app = Router::new().route("/mainvault/_changes", get(mock_continuous_changes_stream));
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind stream server");
        listener
            .set_nonblocking(true)
            .expect("set stream server listener non-blocking");
        let addr = listener.local_addr().expect("stream server local addr");
        tokio::spawn(async move {
            let listener =
                tokio::net::TcpListener::from_std(listener).expect("tokio listener from std");
            axum::serve(listener, app)
                .await
                .expect("serve stream server");
        });

        format!("http://{addr}")
    }

    async fn mock_current_sequence_stalled_body() -> Response {
        let stream = tokio_stream::iter(vec![
            (Duration::ZERO, Bytes::from(r#"{"update_seq":"#)),
            (Duration::from_millis(400), Bytes::from(r#""42-g1AAA"}"#)),
        ])
        .then(|(delay, chunk)| async move {
            tokio::time::sleep(delay).await;
            Ok::<_, Infallible>(chunk)
        });

        Response::builder()
            .status(StatusCode::OK)
            .header("content-type", "application/json")
            .body(Body::from_stream(stream))
            .expect("stalled current sequence response")
    }

    async fn mock_longpoll_changes_stalled_body() -> Response {
        let stream = tokio_stream::iter(vec![
            (
                Duration::ZERO,
                Bytes::from(r#"{"last_seq":"42-g1AAA","results":"#),
            ),
            (Duration::from_secs(30), Bytes::from("[] }")),
        ])
        .then(|(delay, chunk)| async move {
            tokio::time::sleep(delay).await;
            Ok::<_, Infallible>(chunk)
        });

        Response::builder()
            .status(StatusCode::OK)
            .header("content-type", "application/json")
            .body(Body::from_stream(stream))
            .expect("stalled longpoll changes response")
    }

    fn spawn_mock_stalled_body_server() -> String {
        let app = Router::new()
            .route("/mainvault", get(mock_current_sequence_stalled_body))
            .route(
                "/mainvault/_changes",
                get(mock_longpoll_changes_stalled_body),
            );
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind stalled server");
        listener
            .set_nonblocking(true)
            .expect("set stalled server listener non-blocking");
        let addr = listener.local_addr().expect("stalled server local addr");
        tokio::spawn(async move {
            let listener =
                tokio::net::TcpListener::from_std(listener).expect("tokio listener from std");
            axum::serve(listener, app)
                .await
                .expect("serve stalled server");
        });

        format!("http://{addr}")
    }

    #[test]
    fn longpoll_changes_body_decodes_json_payload() {
        let payload = r#"
        {
          "last_seq": "42-g1AAA",
          "results": [
            { "seq": "41-g1AAA", "id": "h:+a", "deleted": false, "doc": { "_id": "h:+a" } }
          ]
        }
        "#;

        let feed = decode_changes_body(FeedMode::Longpoll, Some("application/json"), payload)
            .expect("decode longpoll body");
        assert_eq!(feed.results.len(), 1);
        assert_eq!(feed.results[0].id, "h:+a");
    }

    #[test]
    fn continuous_changes_body_decodes_line_delimited_events() {
        let payload = r#"
{"seq":"41-g1AAA","id":"h:+a","deleted":false,"doc":{"_id":"h:+a"}}
{"seq":"42-g1AAA","id":"h:+b","deleted":true}
{"last_seq":"42-g1AAA"}
        "#;

        let feed = decode_changes_body(FeedMode::Continuous, Some("text/plain"), payload)
            .expect("decode continuous body");
        assert_eq!(feed.results.len(), 2);
        assert_eq!(feed.results[1].id, "h:+b");
    }

    #[tokio::test]
    async fn continuous_poll_changes_streams_without_waiting_for_response_close() {
        let base_url = spawn_mock_changes_stream_server();
        let client = CouchDbClient::new(&CouchDbConfig {
            url: base_url,
            database: "mainvault".to_string(),
            username: "user".to_string(),
            password: "pass".to_string(),
            poll_interval_seconds: 1,
            feed_mode: FeedMode::Continuous,
            ..Default::default()
        })
        .expect("build continuous client");

        let started = Instant::now();
        let feed = client
            .poll_changes("40-g1AAA", Duration::from_millis(120))
            .await
            .expect("poll continuous changes");
        let elapsed = started.elapsed();

        assert_eq!(feed.results.len(), 2);
        assert_eq!(feed.results[0].id, "h:+a");
        assert_eq!(feed.results[1].id, "h:+b");
        assert_eq!(feed.last_seq, Value::String("42-g1AAA".to_string()));
        assert!(
            elapsed < Duration::from_millis(300),
            "continuous polling should return on read-timeout after parsing streamed lines, elapsed={elapsed:?}"
        );
    }

    #[tokio::test]
    async fn read_json_with_timeout_times_out_when_body_stalls_after_headers() {
        let base_url = spawn_mock_stalled_body_server();
        let client = client_for(base_url);
        let response = client
            .send_request_with_timeout(
                client
                    .http
                    .get(client.db_base_url())
                    .basic_auth(&client.username, Some(&client.password)),
                Duration::from_millis(120),
            )
            .await
            .expect("receive stalled response headers");

        let started = Instant::now();
        let error = client
            .read_json_with_timeout::<Value>(response, Duration::from_millis(120))
            .await
            .expect_err("stalled body read should time out");
        let elapsed = started.elapsed();

        assert!(matches!(error, CouchDbError::RequestTimeout { .. }));
        assert!(
            elapsed < Duration::from_millis(300),
            "json body timeout should fail quickly after headers, elapsed={elapsed:?}"
        );
    }

    #[tokio::test]
    async fn longpoll_poll_changes_times_out_when_body_stalls_after_headers() {
        let base_url = spawn_mock_stalled_body_server();
        // Use a tiny grace so the body timeout (timeout + grace = 170ms)
        // is exceeded by the mock's 400ms stall without a 10s wait.
        let client = CouchDbClient::new(&CouchDbConfig {
            url: base_url,
            database: "mainvault".to_string(),
            username: "user".to_string(),
            password: "pass".to_string(),
            poll_interval_seconds: 1,
            feed_mode: FeedMode::Longpoll,
            longpoll_timeout_grace_seconds: 1,
            ..Default::default()
        })
        .expect("build couchdb client");

        let started = Instant::now();
        let error = client
            .poll_changes("41-g1AAA", Duration::from_millis(120))
            .await
            .expect_err("stalled longpoll body should time out");
        let elapsed = started.elapsed();

        assert!(matches!(error, CouchDbError::RequestTimeout { .. }));
        assert!(
            elapsed < Duration::from_millis(2000),
            "longpoll body timeout should fail after grace expires, elapsed={elapsed:?}"
        );
    }

    #[test]
    fn couchdb_client_builds_expected_urls() {
        let config = CouchDbConfig {
            url: "https://couch.example.com/".to_string(),
            database: "mainvault".to_string(),
            username: "user".to_string(),
            password: "pass".to_string(),
            poll_interval_seconds: 5,
            feed_mode: FeedMode::Longpoll,
            ..Default::default()
        };
        let client = CouchDbClient::new(&config).expect("client");
        assert_eq!(
            client.db_base_url(),
            "https://couch.example.com/mainvault".to_string()
        );
        assert_eq!(
            client.changes_url(),
            "https://couch.example.com/mainvault/_changes".to_string()
        );
    }

    #[test]
    fn build_livesync_note_documents_produces_file_and_leaf_shape() {
        let docs =
            build_livesync_note_documents("11New/2026-02-26-new-note.md", "# New Note\n\nBody");

        assert_eq!(docs.file_id, "11new/2026-02-26-new-note.md");
        assert!(docs.leaf_id.starts_with("h:"));
        assert!(!docs.leaf_id.starts_with("h:+"));

        assert_eq!(docs.file_doc["path"], "11New/2026-02-26-new-note.md");
        assert_eq!(docs.file_doc["type"], "plain");
        assert_eq!(docs.file_doc["children"][0], docs.leaf_id);

        assert_eq!(docs.leaf_doc["type"], "leaf");
        assert_eq!(docs.leaf_doc["e_"], true);
        let payload = docs.leaf_doc["data"].as_str().expect("leaf payload string");
        assert_eq!(payload, "# New Note\n\nBody");
    }

    #[test]
    fn build_livesync_note_documents_encrypts_payloads_when_crypto_configured() {
        let decryptor = Decryptor::new("test-passphrase", &[0x42u8; 32]);
        let docs = build_livesync_note_documents_with_crypto(
            "00New/2026-02-26-new-note.md",
            "# New Note\n\nBody",
            Some(&decryptor),
        )
        .expect("encrypted docs");

        let path = docs.file_doc["path"].as_str().expect("encrypted path");
        assert!(path.starts_with("/\\:%="));
        assert_eq!(
            decryptor
                .decrypt_meta_path(path)
                .expect("decrypt meta path"),
            "00New/2026-02-26-new-note.md"
        );

        let encrypted_meta = path.strip_prefix("/\\:").expect("meta prefix");
        let meta_json = decryptor
            .decrypt(encrypted_meta)
            .expect("decrypt meta json");
        let meta: Value = serde_json::from_str(&meta_json).expect("meta json");
        assert_eq!(meta["path"], "00New/2026-02-26-new-note.md");
        assert_eq!(meta["children"][0], docs.leaf_id);

        let leaf_payload = docs.leaf_doc["data"].as_str().expect("leaf payload");
        assert!(leaf_payload.starts_with("%="));
        let payload = decryptor
            .decrypt(leaf_payload)
            .expect("decrypt leaf payload");
        assert_eq!(payload, "# New Note\n\nBody");
    }

    #[test]
    fn build_native_encrypted_livesync_note_documents_hides_metadata_fields() {
        let decryptor = Decryptor::new("test-passphrase", &[0x42u8; 32]);
        let docs = build_native_encrypted_livesync_note_documents(
            "00New/2026-02-26-new-note.md",
            "# New Note\n\nBody",
            &decryptor,
            Some("test-passphrase"),
        )
        .expect("native encrypted docs");

        assert_eq!(
            docs.file_id,
            "f:f47eb7c286c9b0740f1897938de60d3c18359c49d5d5a9fea8bc30fc34648079"
        );
        assert!(docs.leaf_id.starts_with("h:+"));
        assert_eq!(docs.leaf_docs.len(), 1);
        assert_eq!(docs.file_doc["children"], serde_json::json!([]));
        assert_eq!(docs.file_doc["ctime"], 0);
        assert_eq!(docs.file_doc["mtime"], 0);
        assert_eq!(docs.file_doc["size"], 0);

        let meta = decryptor
            .decrypt_meta_document(docs.file_doc["path"].as_str().expect("path"))
            .expect("decrypt metadata");
        assert_eq!(meta["path"], "00New/2026-02-26-new-note.md");
        assert_eq!(meta["children"][0], docs.leaf_id);
        assert!(meta["mtime"].as_i64().unwrap_or_default() > 0);
        assert_eq!(meta["size"], 16);

        let payload = decryptor
            .decrypt(docs.leaf_doc["data"].as_str().expect("leaf payload"))
            .expect("decrypt leaf payload");
        assert_eq!(payload, "# New Note\n\nBody");
    }

    #[test]
    fn build_native_encrypted_livesync_note_documents_chunks_large_notes() {
        let decryptor = Decryptor::new("test-passphrase", &[0x42u8; 32]);
        let markdown = "A".repeat(10_000);
        let docs = build_native_encrypted_livesync_note_documents(
            "00New/2026-02-26-large-note.md",
            &markdown,
            &decryptor,
            Some("test-passphrase"),
        )
        .expect("native encrypted docs");

        assert!(docs.leaf_docs.len() > 1);
        let meta = decryptor
            .decrypt_meta_document(docs.file_doc["path"].as_str().expect("path"))
            .expect("decrypt metadata");
        let children = meta["children"].as_array().expect("metadata children");
        assert!(children.len() >= docs.leaf_docs.len());

        let leaf_docs_by_id = docs
            .leaf_docs
            .iter()
            .map(|(id, doc)| (id.as_str(), doc))
            .collect::<HashMap<_, _>>();
        let reconstructed = children
            .iter()
            .map(|child| {
                let child_id = child.as_str().expect("child id string");
                let doc = leaf_docs_by_id
                    .get(child_id)
                    .expect("child id should resolve to a leaf doc");
                decryptor
                    .decrypt(doc["data"].as_str().expect("leaf payload"))
                    .expect("decrypt leaf payload")
            })
            .collect::<String>();
        assert_eq!(reconstructed, markdown);
    }

    #[test]
    fn build_native_encrypted_livesync_note_documents_uses_stable_ids() {
        let decryptor = Decryptor::new("test-passphrase", &[0x42u8; 32]);
        let first = build_native_encrypted_livesync_note_documents(
            "00New/2026-02-26-stable.md",
            "alpha\nbeta\ngamma",
            &decryptor,
            Some("test-passphrase"),
        )
        .expect("first native encrypted docs");
        let second = build_native_encrypted_livesync_note_documents(
            "00New/2026-02-26-stable.md",
            "alpha\nbeta\ngamma",
            &decryptor,
            Some("test-passphrase"),
        )
        .expect("second native encrypted docs");

        assert_eq!(first.file_id, second.file_id);
        let first_leaf_ids: Vec<_> = first.leaf_docs.iter().map(|(id, _)| id.clone()).collect();
        let second_leaf_ids: Vec<_> = second.leaf_docs.iter().map(|(id, _)| id.clone()).collect();
        assert_eq!(first_leaf_ids, second_leaf_ids);
    }

    #[test]
    fn build_livesync_note_documents_changes_leaf_ids_when_content_changes() {
        let first = build_livesync_note_documents("11New/changed.md", "alpha\nbeta\ngamma");
        let second = build_livesync_note_documents("11New/changed.md", "alpha\nbeta\ndelta");

        let first_leaf_ids: Vec<_> = first.leaf_docs.iter().map(|(id, _)| id.clone()).collect();
        let second_leaf_ids: Vec<_> = second.leaf_docs.iter().map(|(id, _)| id.clone()).collect();
        assert_ne!(first_leaf_ids, second_leaf_ids);
    }

    #[tokio::test]
    async fn update_livesync_note_rejects_stale_expected_revision_before_writes() {
        let note_path = "11New/revision-conflict.md";
        let mut original = build_livesync_note_documents(note_path, "original");
        original.file_doc["_rev"] = Value::String("3-current".to_string());
        let mut docs = HashMap::from([(original.file_id.clone(), original.file_doc)]);
        for (leaf_id, mut leaf_doc) in original.leaf_docs {
            leaf_doc["_rev"] = Value::String("1-leaf".to_string());
            docs.insert(leaf_id, leaf_doc);
        }
        let (url, state) = spawn_mock_couchdb(docs);
        let client = CouchDbClient::new(&CouchDbConfig {
            url,
            database: "mainvault".to_string(),
            username: "user".to_string(),
            password: "pass".to_string(),
            poll_interval_seconds: 1,
            feed_mode: FeedMode::Longpoll,
            ..Default::default()
        })
        .expect("build client");

        let error = client
            .update_livesync_note_if_revision(note_path, "new body", Some("2-stale"))
            .await
            .expect_err("stale revision must fail before a source mutation");

        assert!(matches!(error, CouchDbError::RevisionConflict { .. }));
        assert!(state.operations.lock().await.is_empty());
    }

    #[tokio::test]
    async fn update_livesync_note_keeps_content_addressed_stale_leaves() {
        let note_path = "00New/2026-02-26-update-regression.md";
        let passphrase = "test-passphrase";
        let decryptor = Arc::new(Decryptor::new(passphrase, &[0x42u8; 32]));

        let original_markdown = format!("{}{}", "A".repeat(4_096), "B".repeat(4_096));
        let updated_markdown = format!("{}{}", "C".repeat(4_096), "D".repeat(4_096));

        let mut original = build_native_encrypted_livesync_note_documents(
            note_path,
            &original_markdown,
            decryptor.as_ref(),
            Some(passphrase),
        )
        .expect("original native encrypted docs");
        let updated = build_native_encrypted_livesync_note_documents(
            note_path,
            &updated_markdown,
            decryptor.as_ref(),
            Some(passphrase),
        )
        .expect("updated native encrypted docs");

        let original_leaf_ids = original
            .leaf_docs
            .iter()
            .map(|(id, _)| id.clone())
            .collect::<Vec<_>>();
        let updated_leaf_ids = updated
            .leaf_docs
            .iter()
            .map(|(id, _)| id.clone())
            .collect::<Vec<_>>();
        assert_ne!(original_leaf_ids, updated_leaf_ids);

        original.file_doc["_rev"] = Value::String("1-file".to_string());
        let mut db_docs = HashMap::new();
        db_docs.insert(original.file_id.clone(), original.file_doc.clone());
        for (index, (leaf_id, leaf_doc)) in original.leaf_docs.iter_mut().enumerate() {
            leaf_doc["_rev"] = Value::String(format!("1-leaf-{index}"));
            db_docs.insert(leaf_id.clone(), leaf_doc.clone());
        }

        let (url, state) = spawn_mock_couchdb(db_docs);
        let client = CouchDbClient::new(&CouchDbConfig {
            url,
            database: "mainvault".to_string(),
            username: "user".to_string(),
            password: "pass".to_string(),
            poll_interval_seconds: 1,
            feed_mode: FeedMode::Longpoll,
            encryption: crate::config::EncryptionConfig {
                passphrase: passphrase.to_string(),
            },
            ..Default::default()
        })
        .expect("build encrypted client")
        .with_livesync_crypto(Some(decryptor.clone()));

        client
            .update_livesync_note(note_path, &updated_markdown)
            .await
            .expect("update encrypted note");

        let docs = state.docs.lock().await.clone();
        for old_leaf_id in &original_leaf_ids {
            assert!(
                docs.contains_key(old_leaf_id),
                "content-addressed leaf {old_leaf_id} must remain available for other parents"
            );
        }
        for new_leaf_id in &updated_leaf_ids {
            assert!(
                docs.contains_key(new_leaf_id),
                "updated encrypted leaf {new_leaf_id} should exist after update"
            );
        }

        let stored_file = docs
            .get(&updated.file_id)
            .cloned()
            .expect("updated file doc should be written");
        let stored_meta = decryptor
            .decrypt_meta_document(stored_file["path"].as_str().expect("stored path"))
            .expect("decrypt stored metadata");
        let expected_meta = decryptor
            .decrypt_meta_document(updated.file_doc["path"].as_str().expect("expected path"))
            .expect("decrypt expected metadata");
        assert_eq!(stored_meta["children"], expected_meta["children"]);

        let operations = state.operations.lock().await.clone();
        assert!(
            operations
                .iter()
                .any(|operation| operation == &format!("put:{}", updated.file_id)),
            "file put operation should be recorded"
        );
        assert!(
            operations
                .iter()
                .all(|operation| !operation.starts_with("delete:h:")),
            "online writes must never delete reusable LiveSync leaves"
        );
    }

    #[tokio::test]
    async fn fetch_parent_recovery_changes_resolves_note_path_to_file_and_leaf_docs() {
        let note_path = "11New/recovery.md";
        let docs = build_livesync_note_documents(note_path, "# Recovery");
        let mut file_doc = docs.file_doc.clone();
        let mut leaf_doc = docs.leaf_doc.clone();
        file_doc["_rev"] = Value::String("1-file".to_string());
        leaf_doc["_rev"] = Value::String("1-leaf".to_string());

        let mut db_docs = HashMap::new();
        db_docs.insert(docs.file_id.clone(), file_doc);
        db_docs.insert(docs.leaf_id.clone(), leaf_doc);

        let (url, _state) = spawn_mock_couchdb(db_docs);
        let client = client_for(url);

        let events = client
            .fetch_parent_recovery_changes(note_path)
            .await
            .expect("fetch parent recovery changes");

        let ids = events
            .iter()
            .map(|event| event.id.clone())
            .collect::<Vec<_>>();
        assert_eq!(ids.len(), 2);
        assert!(ids.contains(&docs.file_id));
        assert!(ids.contains(&docs.leaf_id));
        assert!(events.iter().all(|event| event.doc.is_some()));
        assert!(
            events
                .iter()
                .all(|event| event.seq.as_str().unwrap_or_default().is_empty())
        );
    }

    #[tokio::test]
    async fn fetch_parent_recovery_changes_returns_empty_for_missing_parent() {
        let (url, _state) = spawn_mock_couchdb(HashMap::new());
        let client = client_for(url);

        let events = client
            .fetch_parent_recovery_changes("11New/missing.md")
            .await
            .expect("fetch parent recovery changes");
        assert!(events.is_empty());
    }

    #[tokio::test]
    async fn fetch_parent_recovery_changes_does_not_emit_standalone_leaf_root() {
        let leaf_id = "h:+orphan-recovery";
        let mut db_docs = HashMap::new();
        db_docs.insert(
            leaf_id.to_string(),
            serde_json::json!({
                "_id": leaf_id,
                "_rev": "1-leaf",
                "type": "leaf",
                "e_": true,
                "data": "# Orphan Recovery\n\nNo parent metadata is available."
            }),
        );
        let (url, _state) = spawn_mock_couchdb(db_docs);
        let client = client_for(url);

        let events = client
            .fetch_parent_recovery_changes(leaf_id)
            .await
            .expect("fetch parent recovery changes");

        assert!(
            events.is_empty(),
            "recovery from a bare opaque leaf id must not emit the same leaf again"
        );
    }

    #[tokio::test]
    async fn fetch_parent_recovery_changes_reads_native_encrypted_children_with_crypto() {
        let decryptor = Arc::new(Decryptor::new("test-passphrase", &[0x42u8; 32]));
        let docs = build_native_encrypted_livesync_note_documents(
            "11New/native-encrypted-recovery.md",
            &"A".repeat(10_000),
            &decryptor,
            Some("test-passphrase"),
        )
        .expect("native encrypted docs");
        let leaf_count = docs.leaf_docs.len();

        let mut file_doc = docs.file_doc.clone();
        file_doc["_rev"] = Value::String("1-file".to_string());

        let mut db_docs = HashMap::new();
        db_docs.insert(docs.file_id.clone(), file_doc);
        for (index, (leaf_id, mut leaf_doc)) in docs.leaf_docs.iter().cloned().enumerate() {
            leaf_doc["_rev"] = Value::String(format!("1-leaf-{index}"));
            db_docs.insert(leaf_id, leaf_doc);
        }

        let (url, _state) = spawn_mock_couchdb(db_docs);
        let client = client_for(url).with_livesync_crypto(Some(decryptor));

        let events = client
            .fetch_parent_recovery_changes(&docs.file_id)
            .await
            .expect("fetch parent recovery changes");

        assert_eq!(events.len(), 1 + leaf_count);
        let ids = events
            .iter()
            .map(|event| event.id.clone())
            .collect::<Vec<_>>();
        assert!(ids.contains(&docs.file_id));
        for (leaf_id, _) in &docs.leaf_docs {
            assert!(ids.contains(leaf_id));
        }
    }

    #[tokio::test]
    async fn fetch_parent_recovery_changes_resolves_native_encrypted_path_with_passphrase() {
        let passphrase = "test-passphrase";
        let note_path = "11New/native-encrypted-path-recovery.md";
        let decryptor = Arc::new(Decryptor::new(passphrase, &[0x42u8; 32]));
        let docs = build_native_encrypted_livesync_note_documents(
            note_path,
            &"B".repeat(10_000),
            &decryptor,
            Some(passphrase),
        )
        .expect("native encrypted docs");
        let leaf_count = docs.leaf_docs.len();

        let mut file_doc = docs.file_doc.clone();
        file_doc["_rev"] = Value::String("1-file".to_string());

        let mut db_docs = HashMap::new();
        db_docs.insert(docs.file_id.clone(), file_doc);
        for (index, (leaf_id, mut leaf_doc)) in docs.leaf_docs.iter().cloned().enumerate() {
            leaf_doc["_rev"] = Value::String(format!("1-leaf-{index}"));
            db_docs.insert(leaf_id, leaf_doc);
        }

        let (url, state) = spawn_mock_couchdb(db_docs);
        let client =
            client_for_with_passphrase(url, passphrase).with_livesync_crypto(Some(decryptor));

        let events = client
            .fetch_parent_recovery_changes(note_path)
            .await
            .expect("fetch parent recovery changes");

        assert_eq!(events.len(), 1 + leaf_count);
        let ids = events
            .iter()
            .map(|event| event.id.clone())
            .collect::<Vec<_>>();
        assert!(ids.contains(&docs.file_id));
        for (leaf_id, _) in &docs.leaf_docs {
            assert!(ids.contains(leaf_id));
        }

        let requested = state.requested.lock().await.clone();
        assert!(requested.contains(&docs.file_id));
    }
}

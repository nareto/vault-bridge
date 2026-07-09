use std::collections::{BTreeMap, HashMap};
use std::time::Duration;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ChangesFeed {
    pub last_seq: Value,
    #[serde(default)]
    pub results: Vec<ChangeEvent>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ChangeEvent {
    pub seq: Value,
    pub id: String,
    #[serde(default)]
    pub deleted: bool,
    #[serde(default)]
    pub doc: Option<Value>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct FileDocument {
    #[serde(rename = "_id")]
    pub id: String,
    #[serde(rename = "_rev")]
    pub rev: String,
    pub children: Vec<Value>,
    pub path: String,
    pub ctime: i64,
    pub mtime: i64,
    pub size: i64,
    #[serde(rename = "type")]
    pub doc_type: String,
    pub eden: Value,
    #[serde(default)]
    pub deleted: bool,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct LeafDocument {
    #[serde(rename = "_id")]
    pub id: String,
    #[serde(rename = "_rev")]
    pub rev: String,
    pub data: String,
    #[serde(rename = "type")]
    pub doc_type: String,
    #[serde(default)]
    pub e_: bool,
}

#[derive(Debug, Clone)]
pub enum LivesyncDocument {
    File(FileDocument),
    Leaf(LeafDocument),
    Unknown(Value),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecodedChunk {
    pub parent_id: String,
    pub chunk_index: usize,
    pub chunk_count: usize,
    pub content: String,
    pub couchdb_rev: String,
    pub received_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReassembledNote {
    pub parent_id: String,
    pub content: String,
    pub couchdb_rev: String,
}

#[derive(Debug, Error)]
pub enum DecoderError {
    #[error("invalid livesync document shape")]
    InvalidDocument,
    #[error("missing required field '{0}' in leaf payload")]
    MissingLeafField(&'static str),
    #[error("failed to parse leaf payload json: {0}")]
    InvalidLeafJson(String),
}

impl TryFrom<Value> for LivesyncDocument {
    type Error = DecoderError;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        if value.get("path").is_some() && value.get("children").is_some() {
            return serde_json::from_value::<FileDocument>(value)
                .map(Self::File)
                .map_err(|_| DecoderError::InvalidDocument);
        }

        if value.get("data").is_some()
            && (value.get("e_").is_some()
                || value
                    .get("type")
                    .and_then(Value::as_str)
                    .is_some_and(|kind| kind == "leaf"))
        {
            return serde_json::from_value::<LeafDocument>(value)
                .map(Self::Leaf)
                .map_err(|_| DecoderError::InvalidDocument);
        }

        Ok(Self::Unknown(value))
    }
}

pub fn is_deletion(change: &ChangeEvent) -> bool {
    if change.deleted {
        return true;
    }

    change
        .doc
        .as_ref()
        .and_then(|doc| doc.get("deleted").or_else(|| doc.get("_deleted")))
        .and_then(Value::as_bool)
        .unwrap_or(false)
}

/// Convert CouchDB sequence values into stable strings for sync_state tracking.
pub fn sequence_to_string(seq: &Value) -> String {
    match seq {
        Value::String(raw) => raw.clone(),
        Value::Number(number) => number.to_string(),
        other => serde_json::to_string(other).unwrap_or_default(),
    }
}

/// Normalize a note path emitted by Livesync documents.
///
/// Appendix C samples show `path` on `f:*` documents. The exact format can vary,
/// so normalization keeps this conservative and reversible.
pub fn normalize_note_path(path: &str) -> String {
    path.trim().trim_start_matches('/').replace('\\', "/")
}

/// Best-effort vault file id extraction from a file metadata document.
///
/// Returns a vault-relative path for supported text file types (.md, .base).
pub fn file_document_vault_file_id(file: &FileDocument) -> Option<String> {
    if !is_supported_note_file_type(&file.doc_type) {
        return None;
    }

    let normalized = normalize_note_path(&file.path);
    is_supported_vault_file_path(&normalized).then_some(normalized)
}

/// Legacy alias for sync code that only needs markdown note IDs.
pub fn file_document_note_id(file: &FileDocument) -> Option<String> {
    let normalized = normalize_note_path(&file.path);
    (is_markdown_note_path(&normalized)).then_some(normalized)
}

/// Decode a leaf payload into a staging chunk.
///
/// Appendix C only shows that `data` is a string blob. The decoder supports two modes:
/// 1. Structured JSON payloads in `data` with explicit chunk metadata.
/// 2. Fallback single-chunk mode for opaque payloads.
pub fn decode_leaf_chunk(
    doc: &LeafDocument,
    now: DateTime<Utc>,
) -> Result<DecodedChunk, DecoderError> {
    if let Ok(payload) = serde_json::from_str::<Value>(&doc.data)
        && let Ok(decoded) = decode_json_leaf_chunk(doc, &payload, now)
    {
        return Ok(decoded);
    }

    Ok(DecodedChunk {
        parent_id: doc.id.clone(),
        chunk_index: 0,
        chunk_count: 1,
        content: doc.data.clone(),
        couchdb_rev: doc.rev.clone(),
        received_at: now,
    })
}

fn decode_json_leaf_chunk(
    doc: &LeafDocument,
    payload: &Value,
    now: DateTime<Utc>,
) -> Result<DecodedChunk, DecoderError> {
    let parent_id = find_string(
        payload,
        &[
            "parent_id",
            "parentId",
            "parent",
            "path",
            "note_id",
            "noteId",
            "file_id",
            "fileId",
        ],
    )
    .map(|raw| normalize_note_path(&raw))
    .filter(|raw| !raw.is_empty())
    .unwrap_or_else(|| doc.id.clone());

    let chunk_index = find_u64(
        payload,
        &["chunk_index", "chunkIndex", "index", "chunkNo", "seq"],
    )
    .unwrap_or(0) as usize;

    let chunk_count = find_u64(
        payload,
        &[
            "chunk_count",
            "chunkCount",
            "total_chunks",
            "total",
            "count",
        ],
    )
    .unwrap_or(1)
    .max(1) as usize;

    let content = find_content(payload).ok_or(DecoderError::MissingLeafField(
        "content|text|body|markdown|data",
    ))?;

    Ok(DecodedChunk {
        parent_id,
        chunk_index,
        chunk_count,
        content,
        couchdb_rev: doc.rev.clone(),
        received_at: now,
    })
}

fn find_string(payload: &Value, keys: &[&str]) -> Option<String> {
    payload_variants(payload).into_iter().find_map(|candidate| {
        keys.iter().find_map(|key| {
            candidate
                .get(*key)
                .and_then(Value::as_str)
                .map(ToString::to_string)
        })
    })
}

fn find_u64(payload: &Value, keys: &[&str]) -> Option<u64> {
    payload_variants(payload).into_iter().find_map(|candidate| {
        keys.iter().find_map(|key| {
            candidate.get(*key).and_then(|value| {
                value
                    .as_u64()
                    .or_else(|| value.as_str().and_then(|raw| raw.parse::<u64>().ok()))
            })
        })
    })
}

fn find_content(payload: &Value) -> Option<String> {
    if let Some(raw) = payload.as_str() {
        return Some(raw.to_string());
    }

    payload_variants(payload).into_iter().find_map(|candidate| {
        ["content", "text", "body", "markdown", "data", "chunk"]
            .iter()
            .find_map(|key| {
                candidate
                    .get(*key)
                    .and_then(Value::as_str)
                    .map(ToString::to_string)
            })
    })
}

fn payload_variants(payload: &Value) -> Vec<&Value> {
    let mut variants = vec![payload];

    for key in ["payload", "chunk", "leaf", "data"] {
        if let Some(candidate) = payload.get(key)
            && candidate.is_object()
        {
            variants.push(candidate);
        }
    }

    variants
}

#[derive(Debug, Clone)]
struct StagedParent {
    chunk_count: usize,
    couchdb_rev: String,
    chunks: BTreeMap<usize, String>,
    last_received: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StageResult {
    Pending { received: usize, expected: usize },
    Complete(ReassembledNote),
}

#[derive(Debug, Default)]
pub struct ChunkStagingBuffer {
    parents: HashMap<String, StagedParent>,
}

impl ChunkStagingBuffer {
    /// Restore a persisted pending chunk without attempting reassembly.
    ///
    /// Startup hydration uses this path so previously persisted staging rows can
    /// be reconstructed exactly as pending state.
    pub fn restore_pending_chunk(&mut self, chunk: DecodedChunk) {
        let parent = self
            .parents
            .entry(chunk.parent_id)
            .or_insert_with(|| StagedParent {
                chunk_count: chunk.chunk_count.max(1),
                couchdb_rev: chunk.couchdb_rev.clone(),
                chunks: BTreeMap::new(),
                last_received: chunk.received_at,
            });

        if parent.couchdb_rev != chunk.couchdb_rev {
            *parent = StagedParent {
                chunk_count: chunk.chunk_count.max(1),
                couchdb_rev: chunk.couchdb_rev.clone(),
                chunks: BTreeMap::new(),
                last_received: chunk.received_at,
            };
        }

        parent.last_received = chunk.received_at;
        parent.chunks.insert(chunk.chunk_index, chunk.content);

        let min_index = parent.chunks.keys().next().copied().unwrap_or(0);
        let max_index = parent
            .chunks
            .keys()
            .next_back()
            .copied()
            .unwrap_or(min_index);
        let required_by_indexes = if min_index == 0 {
            max_index + 1
        } else {
            max_index.max(1)
        };
        let advertised = chunk.chunk_count.max(1);
        parent.chunk_count = advertised.max(required_by_indexes);
    }

    pub fn take_parent_chunks(&mut self, parent_id: &str) -> Option<Vec<DecodedChunk>> {
        let parent = self.parents.remove(parent_id)?;
        Some(
            parent
                .chunks
                .into_iter()
                .map(|(chunk_index, content)| DecodedChunk {
                    parent_id: parent_id.to_string(),
                    chunk_index,
                    chunk_count: parent.chunk_count,
                    content,
                    couchdb_rev: parent.couchdb_rev.clone(),
                    received_at: parent.last_received,
                })
                .collect(),
        )
    }

    pub fn stage(&mut self, chunk: DecodedChunk) -> StageResult {
        let parent = self
            .parents
            .entry(chunk.parent_id.clone())
            .or_insert_with(|| StagedParent {
                chunk_count: chunk.chunk_count.max(1),
                couchdb_rev: chunk.couchdb_rev.clone(),
                chunks: BTreeMap::new(),
                last_received: chunk.received_at,
            });

        if parent.couchdb_rev != chunk.couchdb_rev {
            *parent = StagedParent {
                chunk_count: chunk.chunk_count.max(1),
                couchdb_rev: chunk.couchdb_rev.clone(),
                chunks: BTreeMap::new(),
                last_received: chunk.received_at,
            };
        }

        parent.last_received = chunk.received_at;
        parent.chunks.insert(chunk.chunk_index, chunk.content);

        // Keep expected chunk count consistent with the latest payload while
        // still respecting any observed chunk indexes.
        let min_index = parent.chunks.keys().next().copied().unwrap_or(0);
        let max_index = parent
            .chunks
            .keys()
            .next_back()
            .copied()
            .unwrap_or(min_index);
        let required_by_indexes = if min_index == 0 {
            max_index + 1
        } else {
            max_index.max(1)
        };
        let advertised = chunk.chunk_count.max(1);
        parent.chunk_count = advertised.max(required_by_indexes);

        let received = parent.chunks.len();
        let expected = parent.chunk_count;

        let zero_based_complete =
            received == expected && (0..expected).all(|idx| parent.chunks.contains_key(&idx));
        let one_based_complete =
            received == expected && (1..=expected).all(|idx| parent.chunks.contains_key(&idx));

        if zero_based_complete || one_based_complete {
            let order = if zero_based_complete {
                (0..expected).collect::<Vec<_>>()
            } else {
                (1..=expected).collect::<Vec<_>>()
            };
            let content = order
                .into_iter()
                .filter_map(|idx| parent.chunks.get(&idx))
                .cloned()
                .collect::<String>();
            let note = ReassembledNote {
                parent_id: chunk.parent_id.clone(),
                content,
                couchdb_rev: parent.couchdb_rev.clone(),
            };
            self.parents.remove(&chunk.parent_id);
            StageResult::Complete(note)
        } else {
            StageResult::Pending { received, expected }
        }
    }

    pub fn pending_count(&self) -> usize {
        self.parents.len()
    }

    pub fn parent_ids(&self) -> impl Iterator<Item = &str> {
        self.parents.keys().map(String::as_str)
    }

    pub fn purge_older_than(&mut self, threshold: Duration, now: DateTime<Utc>) -> Vec<String> {
        let mut stale = Vec::new();
        self.parents.retain(|parent_id, parent| {
            let age = now.signed_duration_since(parent.last_received);
            if age.to_std().ok().is_some_and(|d| d > threshold) {
                stale.push(parent_id.clone());
                false
            } else {
                true
            }
        });
        stale.sort();
        stale
    }
}

fn is_supported_note_file_type(doc_type: &str) -> bool {
    matches!(
        doc_type.to_ascii_lowercase().as_str(),
        "plain" | "newnote" | "markdown"
    )
}

pub(crate) fn is_markdown_note_path(path: &str) -> bool {
    path.to_ascii_lowercase().ends_with(".md")
}

/// Returns true for supported vault text file paths (.md, .base).
pub(crate) fn is_supported_vault_file_path(path: &str) -> bool {
    let lower = path.to_ascii_lowercase();
    lower.ends_with(".md") || lower.ends_with(".base")
}

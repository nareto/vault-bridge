use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration as StdDuration;

use chrono::{DateTime, Duration, Utc};
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;
use sha2::{Digest, Sha256};
use tokio::sync::{Mutex, RwLock};
use tracing::warn;

use crate::authorization::{
    AccessRule, AuthContext, AuthorizationConfig, ContextName, PolicyNote, add_unique_tag,
    owner_from_frontmatter,
};
use crate::base_query::{
    BaseQueryCandidate, BaseQueryError, QueryBaseRequest, QueryBaseResponse, execute_query_base,
};
use crate::config::EmbeddingConfig;
use crate::context::{
    AssembleContextRequest, AssembleContextResponse, ContextCandidate, assemble_context,
};
use crate::graph::FilteredGraph;
use crate::livesync::{
    ChangeEvent, ChunkStagingBuffer, DecodedChunk, FileDocument, LivesyncDocument, ReassembledNote,
    StageResult, decode_leaf_chunk, file_document_vault_file_id, is_deletion,
    is_markdown_note_path, is_supported_vault_file_path, sequence_to_string,
};
use crate::markdown::{
    breadcrumb_prefix, extract_frontmatter_tags, extract_tags, first_h1_title, markdown_plain_text,
    parse_frontmatter, parse_markdown, split_into_semantic_blocks,
};
use crate::model::{Note, NoteId, UnscopedNote, VaultFile};
use crate::new_note::{
    NewNoteFileType, NewNotePathSettings, NewNoteRequest, PersistenceFailureKind,
    UpdateNoteRequest, WriteError, apply_content_patch,
};
use crate::persistence::{
    BlockSemanticMatch, PersistedAccessLogEntry, PersistedFileAlias, PersistedIngestDelta,
    PersistedLinkRecord, PersistedNoteRecord, PersistedRecoveryTarget, PersistedSearchNote,
    PersistedStagedChunk, PersistedSyncState, PersistedVaultFile, PostgresPersistence,
    RecoveryQueueStats,
};
use crate::runtime_config::{ConfigReloadStatus, RuntimeAuthConfig};
use crate::search::{
    CandidateSearchDoc, MatchType, SearchMode, SearchResponse, UnscopedSearchHit,
    cosine_similarity, embed_text, fulltext_ranking, hybrid_ranking, semantic_ranking,
};
use crate::summary::structural_summary;

pub const MAX_NOTE_LIST_LIMIT: usize = 500;
pub const MAX_GRAPH_TRAVERSAL_DEPTH: usize = 5;

#[derive(Debug, Clone)]
pub struct LinkRecord {
    pub source_id: NoteId,
    pub target_id: NoteId,
    pub context_text: String,
    pub position: usize,
}

#[derive(Debug, Clone)]
pub struct StoredNote {
    pub id: NoteId,
    pub path: String,
    pub title: String,
    pub heading_title: Option<String>,
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
}

/// Raw vault file stored alongside the parsed note index.
///
/// For .md files, content is the full markdown including YAML frontmatter.
/// For .base files, content is the raw YAML.
#[derive(Debug, Clone)]
pub struct StoredVaultFile {
    pub path: String,
    pub content: String,
    pub couchdb_rev: String,
    pub created_at: Option<DateTime<Utc>>,
    pub updated_at: DateTime<Utc>,
    pub indexed_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct NoteInput {
    pub id: NoteId,
    pub title: String,
    pub content: String,
    pub frontmatter: Value,
    pub tags: Vec<String>,
    pub couchdb_rev: String,
    pub created_at: Option<DateTime<Utc>>,
    pub updated_at: DateTime<Utc>,
    pub embedding: Option<Vec<f32>>,
    pub links: Vec<LinkInput>,
}

#[derive(Debug, Clone)]
pub struct LinkInput {
    pub target_id: NoteId,
    pub context_text: String,
    pub position: usize,
}

#[derive(Debug, Clone)]
pub struct PreparedVaultWrite {
    pub(crate) path: String,
    pub(crate) content: String,
    pub(crate) file_type: NewNoteFileType,
    pub(crate) created_at: Option<DateTime<Utc>>,
    pub(crate) updated_at: DateTime<Utc>,
    pub(crate) note: Option<NoteInput>,
    pub(crate) mark_created: bool,
    pub(crate) expected_couchdb_rev: Option<String>,
    pub(crate) operation_id: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LocalProjectionOutcome {
    Applied,
    Pending {
        failure_kind: PersistenceFailureKind,
    },
}

impl LocalProjectionOutcome {
    pub fn state(self) -> &'static str {
        match self {
            Self::Applied => "applied",
            Self::Pending { .. } => "pending",
        }
    }

    pub fn response_status(self, completed: &'static str) -> &'static str {
        match self {
            Self::Applied => completed,
            Self::Pending { .. } => "accepted",
        }
    }
}

#[derive(Debug, Clone, Default)]
struct ProjectionHealthState {
    pending: HashMap<String, String>,
    last_success_at: Option<DateTime<Utc>>,
    last_failure_at: Option<DateTime<Utc>>,
    last_failure_kind: Option<PersistenceFailureKind>,
}

#[derive(Debug, Clone)]
pub(crate) struct RecoveredVaultFileState {
    path: String,
    content: String,
    file_type: NewNoteFileType,
    couchdb_rev: String,
    created_at: Option<DateTime<Utc>>,
    updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
struct PreparedNoteUpsert {
    note: StoredNote,
    links: Vec<LinkRecord>,
}

#[derive(Debug, Clone)]
pub struct UnscopedRecentNoteSummary {
    pub id: NoteId,
    pub title: String,
    pub heading_title: Option<String>,
    pub summary: String,
    pub tags: Vec<String>,
    pub updated_at: DateTime<Utc>,
    pub link_count: usize,
    pub backlink_count: usize,
    pub search_score: Option<f32>,
    pub search_match_type: Option<MatchType>,
    pub search_snippet: Option<String>,
    pub matched_chunk_id: Option<String>,
    pub matched_heading_path: Option<String>,
    pub matched_snippet: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct RecentNoteSummary {
    pub id: NoteId,
    pub title: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub heading_title: Option<String>,
    pub summary: String,
    pub tags: Vec<String>,
    pub updated_at: DateTime<Utc>,
    pub link_count: usize,
    pub backlink_count: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub search_score: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub search_match_type: Option<MatchType>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub search_snippet: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub matched_chunk_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub matched_heading_path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub matched_snippet: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct RecentNotesResponse {
    pub notes: Vec<RecentNoteSummary>,
    pub total: usize,
    pub total_filtered: usize,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum NoteSortField {
    Relevance,
    UpdatedAt,
    CreatedAt,
    Title,
}

#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum SortOrder {
    Asc,
    #[default]
    Desc,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct NoteTimeFilter {
    #[serde(default)]
    pub created_after: Option<DateTime<Utc>>,
    #[serde(default)]
    pub created_before: Option<DateTime<Utc>>,
    #[serde(default)]
    pub updated_after: Option<DateTime<Utc>>,
    #[serde(default)]
    pub updated_before: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct QueryNotesRequest {
    #[serde(default)]
    pub tags_all: Vec<String>,
    #[serde(default)]
    pub tags_any: Vec<String>,
    #[serde(default)]
    pub tags_none: Vec<String>,
    #[serde(flatten)]
    pub time_filter: NoteTimeFilter,
    #[serde(default)]
    pub has_frontmatter: Vec<String>,
    #[serde(default)]
    pub missing_frontmatter: Vec<String>,
    #[serde(default)]
    pub path_prefix: Option<String>,
    #[serde(default)]
    pub title_exact: Option<String>,
    #[serde(default)]
    pub text_query: Option<String>,
    #[serde(default)]
    pub search_mode: Option<SearchMode>,
    #[serde(default)]
    pub sort_by: Option<NoteSortField>,
    #[serde(default)]
    pub sort_order: Option<SortOrder>,
    #[serde(default)]
    pub limit: Option<usize>,
}

#[derive(Debug, Clone)]
struct RankedNoteHit {
    id: NoteId,
    score: f32,
    match_type: MatchType,
    chunk_match: Option<ChunkMatchMetadata>,
}

#[derive(Debug, Clone)]
struct ChunkMatchMetadata {
    block_id: String,
    heading_path: String,
    snippet: String,
}

#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum NeighborDirection {
    Outgoing,
    Incoming,
    #[default]
    Both,
}

#[derive(Debug, Clone)]
pub struct UnscopedNeighborNode {
    pub id: NoteId,
    pub title: String,
    pub depth: usize,
    pub link_context: String,
    pub is_hub: bool,
    pub direction: NeighborDirection,
}

#[derive(Debug, Clone, Serialize)]
pub struct NeighborNode {
    pub id: NoteId,
    pub title: String,
    pub depth: usize,
    pub link_context: String,
    pub is_hub: bool,
    pub direction: NeighborDirection,
}

#[derive(Debug, Clone, Serialize)]
pub struct NeighborEdge {
    pub from: NoteId,
    pub to: NoteId,
}

#[derive(Debug, Clone, Serialize)]
pub struct NeighborsResponse {
    pub center: NoteId,
    pub direction: NeighborDirection,
    pub nodes: Vec<NeighborNode>,
    pub edges: Vec<NeighborEdge>,
}

#[derive(Debug, Clone)]
pub struct UnscopedBacklinkEntry {
    pub id: NoteId,
    pub title: String,
    pub context: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct BacklinkEntry {
    pub id: NoteId,
    pub title: String,
    pub context: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct BacklinksResponse {
    pub target: NoteId,
    pub backlinks: Vec<BacklinkEntry>,
}

#[derive(Debug, Clone, Serialize)]
pub struct PathResponse {
    pub from: NoteId,
    pub to: NoteId,
    pub path: Option<Vec<NoteId>>,
    pub length: Option<usize>,
}

#[derive(Debug, Clone, Serialize)]
pub struct TagCount {
    pub tag: String,
    pub count: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct TagsResponse {
    pub tags: Vec<TagCount>,
}

impl UnscopedRecentNoteSummary {
    pub fn into_summary(self) -> RecentNoteSummary {
        RecentNoteSummary {
            id: self.id,
            title: self.title,
            heading_title: self.heading_title,
            summary: self.summary,
            tags: self.tags,
            updated_at: self.updated_at,
            link_count: self.link_count,
            backlink_count: self.backlink_count,
            search_score: self.search_score,
            search_match_type: self.search_match_type,
            search_snippet: self.search_snippet,
            matched_chunk_id: self.matched_chunk_id,
            matched_heading_path: self.matched_heading_path,
            matched_snippet: self.matched_snippet,
        }
    }
}

impl RecentNotesResponse {
    fn new(notes: Vec<RecentNoteSummary>, total: usize, total_filtered: usize) -> Self {
        Self {
            notes,
            total,
            total_filtered,
        }
    }
}

impl UnscopedNeighborNode {
    pub fn into_node(self) -> NeighborNode {
        NeighborNode {
            id: self.id,
            title: self.title,
            depth: self.depth,
            link_context: self.link_context,
            is_hub: self.is_hub,
            direction: self.direction,
        }
    }
}

impl NeighborsResponse {
    fn new(
        center: NoteId,
        direction: NeighborDirection,
        nodes: Vec<NeighborNode>,
        edges: Vec<NeighborEdge>,
    ) -> Self {
        Self {
            center,
            direction,
            nodes,
            edges,
        }
    }
}

impl UnscopedBacklinkEntry {
    pub fn into_entry(self) -> BacklinkEntry {
        BacklinkEntry {
            id: self.id,
            title: self.title,
            context: self.context,
        }
    }
}

impl BacklinksResponse {
    fn new(target: NoteId, backlinks: Vec<BacklinkEntry>) -> Self {
        Self { target, backlinks }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct NewNoteResponse {
    pub id: NoteId,
    pub status: &'static str,
    pub file_type: NewNoteFileType,
    pub indexed_as_note: bool,
    pub local_projection: &'static str,
    pub operation_id: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct UpdateNoteResponse {
    pub id: NoteId,
    pub status: &'static str,
    pub local_projection: &'static str,
    pub operation_id: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct IndexStats {
    pub total_notes: usize,
    pub total_links: usize,
    pub total_tags: usize,
    pub pending_embeddings: usize,
    pub quarantined_embeddings: usize,
    pub pending_chunk_embeddings: usize,
    pub quarantined_chunk_embeddings: usize,
    pub pending_chunks: usize,
    pub orphan_leaf_staging_count: usize,
    pub stale_file_aliases: usize,
    pub pending_sync_recoveries: usize,
    pub quarantined_sync_recoveries: usize,
    pub stale_aliases_blocked_by_unavailable_children: usize,
    pub missing_livesync_children: usize,
    pub tombstoned_livesync_children: usize,
    pub missing_vault_files_for_notes: usize,
    pub unindexed_markdown_vault_files: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct EmbeddingStatus {
    pub mode: String,
    pub model: String,
    pub dimensions: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub endpoint: Option<String>,
    pub pending_notes: usize,
    pub quarantined_notes: usize,
    pub pending_chunks: usize,
    pub quarantined_chunks: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_success_at: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_error_at: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
    pub backend_state: &'static str,
}

#[derive(Debug, Clone, Serialize)]
pub struct SyncStats {
    pub last_seq: String,
    pub couchdb_current_seq: String,
    pub behind_by: i64,
    pub current_seq_source: &'static str,
    pub current_seq_observed_at: DateTime<Utc>,
    pub last_sync_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ContextStats {
    pub accessible_notes: usize,
    pub filtered_notes: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct StatusResponse {
    pub status: &'static str,
    pub dependencies: DependencyStatus,
    pub write_projection: WriteProjectionStatus,
    pub index: IndexStats,
    pub embedding: EmbeddingStatus,
    pub sync: SyncStats,
    pub context_stats: HashMap<String, ContextStats>,
    pub config_reload: ConfigReloadStatus,
}

#[derive(Debug, Clone, Serialize)]
pub struct DependencyStatus {
    pub postgres: &'static str,
    pub couchdb: &'static str,
}

#[derive(Debug, Clone, Serialize)]
pub struct WriteProjectionStatus {
    pub pending: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_success_at: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_failure_at: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_failure_kind: Option<PersistenceFailureKind>,
}

#[derive(Debug, Clone)]
pub struct SyncBatchResult {
    pub indexed_notes: usize,
    pub deleted_notes: usize,
    pub pending_chunks: usize,
    pub purged_parent_ids: Vec<String>,
    pub recovery_parent_ids: Vec<String>,
    pub orphan_leaf_parent_ids: Vec<String>,
    pub last_seq: Option<String>,
    pub durably_applied: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ChunkStagingPurgeResult {
    pub purged_parent_ids: Vec<String>,
    pub recovery_parent_ids: Vec<String>,
    pub orphan_leaf_parent_ids: Vec<String>,
}

impl ChunkStagingPurgeResult {
    pub fn is_empty(&self) -> bool {
        self.purged_parent_ids.is_empty()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NoteVisibility {
    Missing,
    Accessible,
    Filtered,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VaultFileVisibility {
    Missing,
    MissingRawWithIndexedNote,
    MissingIndexWithRawMarkdown,
    Accessible,
    Filtered,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StaleFileRecoveryTarget {
    pub file_doc_id: String,
    pub note_path: String,
    pub child_doc_ids: Vec<String>,
    pub needs_file_document: bool,
}

#[derive(Debug, Clone)]
pub struct AccessLogEntry {
    pub timestamp: DateTime<Utc>,
    pub context: String,
    pub endpoint: String,
    pub query_params: Value,
    pub notes_returned: Vec<NoteId>,
    pub notes_filtered_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ChildChunkHint {
    note_path: String,
    chunk_index: usize,
    chunk_count: usize,
    couchdb_rev: String,
}

#[derive(Debug, Clone, Default)]
struct FileTimestampHint {
    created_at: Option<DateTime<Utc>>,
    updated_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone)]
struct StoreSettings {
    max_link_context_chars: usize,
    hub_note_threshold: usize,
    hub_note_fanout: usize,
    hub_note_folders: Vec<String>,
    context_default_max_tokens: usize,
    context_max_max_tokens: usize,
    context_default_max_depth: usize,
    embedding_provider: String,
    embedding_url: String,
    embedding_model: String,
    embedding_dimensions: usize,
    embedding_timeout_seconds: u64,
    new_note_path_settings: NewNotePathSettings,
    block_min_chars: usize,
    block_chunk_bytes: usize,
    block_chunk_overlap_sentences: usize,
    block_embedding_enabled: bool,
    max_embedding_failures: usize,
}

impl StoreSettings {
    fn new(hub_note_threshold: usize) -> Self {
        Self {
            max_link_context_chars: 250,
            hub_note_threshold,
            hub_note_fanout: 6,
            hub_note_folders: vec!["MOC/".to_string(), "99MOC/".to_string()],
            context_default_max_tokens: 8_000,
            context_max_max_tokens: 32_000,
            context_default_max_depth: 2,
            embedding_provider: "local".to_string(),
            embedding_url: String::new(),
            embedding_model: "nomic-embed-text".to_string(),
            embedding_dimensions: 768,
            embedding_timeout_seconds: 30,
            new_note_path_settings: NewNotePathSettings::default(),
            block_min_chars: 200,
            block_chunk_bytes: 800,
            block_chunk_overlap_sentences: 1,
            block_embedding_enabled: true,
            max_embedding_failures: 3,
        }
    }
}

#[derive(Debug, Clone)]
struct StoreAuditState {
    enabled: bool,
    retention_days: u64,
    access_log: Vec<AccessLogEntry>,
}

impl Default for StoreAuditState {
    fn default() -> Self {
        Self {
            enabled: true,
            retention_days: 90,
            access_log: Vec::new(),
        }
    }
}

#[derive(Debug, Clone)]
struct RuntimeSyncState {
    last_seq: String,
    couchdb_current_seq: String,
    last_sync_at: DateTime<Utc>,
}

impl RuntimeSyncState {
    fn new(now: DateTime<Utc>) -> Self {
        Self {
            last_seq: "0".to_string(),
            couchdb_current_seq: "0".to_string(),
            last_sync_at: now,
        }
    }
}

fn persisted_sync_state_is_newer(
    runtime: &RuntimeSyncState,
    persisted: &PersistedSyncState,
) -> bool {
    persisted.updated_at > runtime.last_sync_at
        || (persisted.updated_at == runtime.last_sync_at
            && (persisted.last_seq != runtime.last_seq
                || persisted.couchdb_current_seq != runtime.couchdb_current_seq))
}

#[derive(Debug)]
pub struct StoreInner {
    notes: HashMap<NoteId, StoredNote>,
    vault_files: HashMap<String, StoredVaultFile>,
    links: Vec<LinkRecord>,
    created_file_paths: HashSet<String>,
    file_doc_paths: HashMap<String, String>,
    file_doc_revs: HashMap<String, String>,
    file_children: HashMap<String, Vec<String>>,
    child_doc_paths: HashMap<String, Vec<String>>,
    child_chunk_hints: HashMap<String, Vec<ChildChunkHint>>,
    note_timestamp_hints: HashMap<String, FileTimestampHint>,
    access_log: Vec<AccessLogEntry>,
    pub chunk_staging: ChunkStagingBuffer,
    graph_cache_generation: u64,
    pub last_seq: String,
    pub couchdb_current_seq: String,
    pub last_sync_at: DateTime<Utc>,
    pub max_link_context_chars: usize,
    pub hub_note_threshold: usize,
    pub hub_note_fanout: usize,
    pub hub_note_folders: Vec<String>,
    pub context_default_max_tokens: usize,
    pub context_max_max_tokens: usize,
    pub context_default_max_depth: usize,
    pub embedding_provider: String,
    pub embedding_url: String,
    pub embedding_model: String,
    pub embedding_dimensions: usize,
    pub embedding_timeout_seconds: u64,
    pub new_note_path_settings: NewNotePathSettings,
    pub audit_enabled: bool,
    pub audit_retention_days: u64,
    pub block_min_chars: usize,
    pub block_chunk_bytes: usize,
    pub block_chunk_overlap_sentences: usize,
    pub block_embedding_enabled: bool,
}

#[derive(Clone, Debug)]
pub struct VaultStore {
    inner: Arc<RwLock<StoreInner>>,
    settings: Arc<RwLock<StoreSettings>>,
    authorization: RuntimeAuthConfig,
    audit: Arc<RwLock<StoreAuditState>>,
    sync_state: Arc<RwLock<RuntimeSyncState>>,
    cache_generation: Arc<AtomicU64>,
    read_refresh_lock: Arc<Mutex<()>>,
    projection_health: Arc<RwLock<ProjectionHealthState>>,
    #[cfg(test)]
    forced_projection_failure: Arc<RwLock<Option<PersistenceFailureKind>>>,
    persistence: Option<Arc<PostgresPersistence>>,
}

impl Default for VaultStore {
    fn default() -> Self {
        Self::new(20)
    }
}

impl VaultStore {
    pub fn new(hub_note_threshold: usize) -> Self {
        Self::new_with_optional_persistence_and_auth_config(
            hub_note_threshold,
            None,
            RuntimeAuthConfig::default(),
        )
    }

    pub fn new_with_auth_config(
        hub_note_threshold: usize,
        authorization: RuntimeAuthConfig,
    ) -> Self {
        Self::new_with_optional_persistence_and_auth_config(hub_note_threshold, None, authorization)
    }

    pub fn new_with_persistence(
        hub_note_threshold: usize,
        persistence: Arc<PostgresPersistence>,
    ) -> Self {
        Self::new_with_optional_persistence_and_auth_config(
            hub_note_threshold,
            Some(persistence),
            RuntimeAuthConfig::default(),
        )
    }

    pub fn new_with_persistence_and_auth_config(
        hub_note_threshold: usize,
        persistence: Arc<PostgresPersistence>,
        authorization: RuntimeAuthConfig,
    ) -> Self {
        Self::new_with_optional_persistence_and_auth_config(
            hub_note_threshold,
            Some(persistence),
            authorization,
        )
    }

    fn new_with_optional_persistence_and_auth_config(
        hub_note_threshold: usize,
        persistence: Option<Arc<PostgresPersistence>>,
        authorization: RuntimeAuthConfig,
    ) -> Self {
        let now = Utc::now();
        let settings = StoreSettings::new(hub_note_threshold);
        let audit = StoreAuditState::default();
        let sync_state = RuntimeSyncState::new(now);

        Self {
            inner: Arc::new(RwLock::new(StoreInner {
                notes: HashMap::new(),
                vault_files: HashMap::new(),
                links: Vec::new(),
                created_file_paths: HashSet::new(),
                file_doc_paths: HashMap::new(),
                file_doc_revs: HashMap::new(),
                file_children: HashMap::new(),
                child_doc_paths: HashMap::new(),
                child_chunk_hints: HashMap::new(),
                note_timestamp_hints: HashMap::new(),
                access_log: Vec::new(),
                chunk_staging: ChunkStagingBuffer::default(),
                graph_cache_generation: 0,
                last_seq: sync_state.last_seq.clone(),
                couchdb_current_seq: sync_state.couchdb_current_seq.clone(),
                last_sync_at: sync_state.last_sync_at,
                max_link_context_chars: settings.max_link_context_chars,
                hub_note_threshold: settings.hub_note_threshold,
                hub_note_fanout: settings.hub_note_fanout,
                hub_note_folders: settings.hub_note_folders.clone(),
                context_default_max_tokens: settings.context_default_max_tokens,
                context_max_max_tokens: settings.context_max_max_tokens,
                context_default_max_depth: settings.context_default_max_depth,
                embedding_provider: settings.embedding_provider.clone(),
                embedding_url: settings.embedding_url.clone(),
                embedding_model: settings.embedding_model.clone(),
                embedding_dimensions: settings.embedding_dimensions,
                embedding_timeout_seconds: settings.embedding_timeout_seconds,
                new_note_path_settings: settings.new_note_path_settings.clone(),
                audit_enabled: audit.enabled,
                audit_retention_days: audit.retention_days,
                block_min_chars: settings.block_min_chars,
                block_chunk_bytes: settings.block_chunk_bytes,
                block_chunk_overlap_sentences: settings.block_chunk_overlap_sentences,
                block_embedding_enabled: settings.block_embedding_enabled,
            })),
            settings: Arc::new(RwLock::new(settings)),
            authorization,
            audit: Arc::new(RwLock::new(audit)),
            sync_state: Arc::new(RwLock::new(sync_state)),
            cache_generation: Arc::new(AtomicU64::new(0)),
            read_refresh_lock: Arc::new(Mutex::new(())),
            projection_health: Arc::new(RwLock::new(ProjectionHealthState::default())),
            #[cfg(test)]
            forced_projection_failure: Arc::new(RwLock::new(None)),
            persistence,
        }
    }

    async fn ensure_read_cache_fresh(&self) -> Result<(), crate::persistence::PersistenceError> {
        let Some(persistence) = self.persistence.clone() else {
            return Ok(());
        };

        let persisted_generation = persistence.load_store_generation().await?;
        let persisted_sync_state = persistence.load_sync_state().await?;
        if !self
            .persisted_state_is_newer(persisted_generation, persisted_sync_state.as_ref())
            .await
        {
            return Ok(());
        }

        let _refresh_guard = self.read_refresh_lock.lock().await;
        let persisted_generation = persistence.load_store_generation().await?;
        let persisted_sync_state = persistence.load_sync_state().await?;
        if !self
            .persisted_state_is_newer(persisted_generation, persisted_sync_state.as_ref())
            .await
        {
            return Ok(());
        }

        self.hydrate_from_persistence().await
    }

    async fn persisted_state_is_newer(
        &self,
        persisted_generation: u64,
        persisted_sync_state: Option<&PersistedSyncState>,
    ) -> bool {
        if persisted_generation > self.cache_generation.load(Ordering::Acquire) {
            return true;
        }
        let Some(persisted_sync_state) = persisted_sync_state else {
            return false;
        };
        let runtime_sync_state = self.sync_state.read().await;
        persisted_sync_state_is_newer(&runtime_sync_state, persisted_sync_state)
    }

    async fn prepare_cached_read(&self, read_context: &'static str) {
        if self.persistence.is_none() {
            return;
        }

        if let Err(error) = self.ensure_read_cache_fresh().await {
            warn!(
                error = %error,
                read_context,
                "failed postgres-backed read cache refresh; using in-memory cache"
            );
        }
    }

    pub async fn hydrate_from_persistence(
        &self,
    ) -> Result<(), crate::persistence::PersistenceError> {
        let Some(persistence) = self.persistence.as_ref() else {
            return Ok(());
        };

        let snapshot = persistence.load_snapshot().await?;
        let snapshot_generation = snapshot.generation;
        let mut guard = self.inner.write().await;

        guard.notes.clear();
        guard.vault_files.clear();
        guard.links.clear();
        guard.file_doc_paths.clear();
        guard.file_doc_revs.clear();
        guard.file_children.clear();
        guard.child_doc_paths.clear();
        guard.child_chunk_hints.clear();
        guard.note_timestamp_hints.clear();
        guard.chunk_staging = ChunkStagingBuffer::default();

        for note in snapshot.notes {
            let note_id = NoteId::new(note.id.clone());
            guard.notes.insert(
                note_id.clone(),
                StoredNote {
                    id: note_id.clone(),
                    path: note.path,
                    title: note.title,
                    heading_title: first_h1_title(&note.content),
                    content: note.content,
                    search_text: note.search_text,
                    summary: note.summary,
                    frontmatter: note.frontmatter,
                    tags: note.tags,
                    couchdb_rev: note.couchdb_rev,
                    created_at: note.created_at,
                    updated_at: note.updated_at,
                    indexed_at: note.indexed_at,
                    embedding: note.embedding,
                },
            );

            for link in note.links {
                guard.links.push(LinkRecord {
                    source_id: note_id.clone(),
                    target_id: NoteId::new(link.target_id),
                    context_text: link.context_text,
                    position: link.position,
                });
            }
        }

        for chunk in snapshot.staged_chunks {
            guard.chunk_staging.restore_pending_chunk(DecodedChunk {
                parent_id: chunk.parent_id,
                chunk_index: chunk.chunk_index,
                chunk_count: chunk.chunk_count,
                content: chunk.content,
                couchdb_rev: chunk.couchdb_rev,
                received_at: chunk.received_at,
            });
        }

        for vf in snapshot.vault_files {
            guard.vault_files.insert(
                vf.path.clone(),
                StoredVaultFile {
                    path: vf.path,
                    content: vf.content,
                    couchdb_rev: vf.couchdb_rev,
                    created_at: vf.created_at,
                    updated_at: vf.updated_at,
                    indexed_at: vf.indexed_at,
                },
            );
        }

        let mut repaired_note_ids: HashSet<NoteId> = HashSet::new();
        let mut repaired_deleted_ids: HashSet<NoteId> = HashSet::new();
        let mut _repaired_staged_upserts: HashMap<(String, usize), PersistedStagedChunk> =
            HashMap::new();
        let mut _repaired_staged_deletes: HashSet<String> = HashSet::new();
        let hydration_now = Utc::now();
        for alias in snapshot.file_aliases {
            let note_path = alias.note_path.clone();
            let file_doc_id = alias.file_doc_id.clone();
            let child_ids = alias.children.clone();
            let couchdb_rev = alias.couchdb_rev.clone();
            register_file_alias_persisted_locked(&mut guard, alias);
            rehome_staged_chunks_locked(
                &mut guard,
                &note_path,
                &file_doc_id,
                &child_ids,
                &couchdb_rev,
                250,
                hydration_now,
                &mut repaired_note_ids,
                &mut repaired_deleted_ids,
                &mut _repaired_staged_upserts,
                &mut _repaired_staged_deletes,
            );
            repair_note_alias_locked(
                &mut guard,
                &note_path,
                &file_doc_id,
                &child_ids,
                &mut repaired_note_ids,
                &mut repaired_deleted_ids,
            );
        }

        if let Some(sync_state) = snapshot.sync_state {
            guard.last_seq = sync_state.last_seq.clone();
            guard.couchdb_current_seq = sync_state.couchdb_current_seq;
            guard.last_sync_at = sync_state.updated_at;
        }

        rebuild_graph_cache_locked(&mut guard);
        let runtime_sync_state = RuntimeSyncState {
            last_seq: guard.last_seq.clone(),
            couchdb_current_seq: guard.couchdb_current_seq.clone(),
            last_sync_at: guard.last_sync_at,
        };

        let repaired_batch = if !repaired_note_ids.is_empty() || !repaired_deleted_ids.is_empty() {
            let upserts = repaired_note_ids
                .iter()
                .filter_map(|note_id| note_for_persistence_locked(&guard, note_id))
                .collect::<Vec<_>>();
            let deletes = repaired_deleted_ids
                .iter()
                .map(|note_id| note_id.as_str().to_string())
                .collect::<Vec<_>>();
            let block_sync_notes = repaired_note_ids
                .iter()
                .filter_map(|note_id| {
                    let note = guard.notes.get(note_id)?;
                    Some((
                        note_id.as_str().to_string(),
                        note.path.clone(),
                        note.title.clone(),
                        note.content.clone(),
                    ))
                })
                .collect::<Vec<_>>();
            Some((upserts, deletes, block_sync_notes))
        } else {
            None
        };
        drop(guard);
        *self.sync_state.write().await = runtime_sync_state;

        let mut cache_generation = snapshot_generation;
        if let Some((upserts, deletes, block_sync_notes)) = repaired_batch {
            cache_generation = persistence.apply_delta(upserts, deletes, None).await?;
            for (note_id, path, title, content) in block_sync_notes {
                self.sync_blocks_for_note(&note_id, &path, &title, &content)
                    .await;
            }
        }
        self.cache_generation
            .store(cache_generation, Ordering::Release);
        self.reconcile_pending_projections_from_cache().await;

        Ok(())
    }

    async fn reconcile_pending_projections_from_cache(&self) {
        let revisions = {
            let guard = self.inner.read().await;
            guard
                .vault_files
                .iter()
                .map(|(path, file)| (path.clone(), file.couchdb_rev.clone()))
                .collect::<HashMap<_, _>>()
        };
        let mut health = self.projection_health.write().await;
        let before = health.pending.len();
        health
            .pending
            .retain(|path, revision| revisions.get(path) != Some(revision));
        if health.pending.len() < before {
            health.last_success_at = Some(Utc::now());
        }
    }

    #[cfg(test)]
    pub(crate) async fn force_projection_failure_for_test(
        &self,
        failure: Option<PersistenceFailureKind>,
    ) {
        *self.forced_projection_failure.write().await = failure;
    }

    pub async fn set_authorization_config(&self, config: AuthorizationConfig) {
        self.authorization.set_contexts(config).await;
    }

    pub async fn authorization_config(&self) -> AuthorizationConfig {
        self.authorization.snapshot().await.contexts
    }

    pub async fn set_hub_settings(&self, threshold: usize, fanout: usize, folders: Vec<String>) {
        let mut settings = self.settings.write().await;
        settings.hub_note_threshold = threshold;
        settings.hub_note_fanout = fanout.max(1);
        settings.hub_note_folders = folders.clone();
        drop(settings);

        let mut guard = self.inner.write().await;
        guard.hub_note_threshold = threshold;
        guard.hub_note_fanout = fanout.max(1);
        guard.hub_note_folders = folders;
    }

    pub async fn set_link_context_chars(&self, max_link_context_chars: usize) {
        self.settings.write().await.max_link_context_chars = max_link_context_chars.max(1);
        let mut guard = self.inner.write().await;
        guard.max_link_context_chars = max_link_context_chars.max(1);
    }

    pub async fn set_context_settings(
        &self,
        default_max_tokens: usize,
        max_max_tokens: usize,
        default_max_depth: usize,
    ) {
        let clamped_default_depth = default_max_depth.clamp(1, MAX_GRAPH_TRAVERSAL_DEPTH);
        let clamped_default_tokens = default_max_tokens.max(1);
        let clamped_max_tokens = max_max_tokens.max(clamped_default_tokens);
        let mut settings = self.settings.write().await;
        settings.context_default_max_depth = clamped_default_depth;
        settings.context_default_max_tokens = clamped_default_tokens;
        settings.context_max_max_tokens = clamped_max_tokens;
        drop(settings);

        let mut guard = self.inner.write().await;
        guard.context_default_max_depth = clamped_default_depth;
        guard.context_default_max_tokens = clamped_default_tokens;
        guard.context_max_max_tokens = clamped_max_tokens;
    }

    pub async fn set_embedding_settings(&self, config: EmbeddingConfig) {
        let embedding_mode = config.mode.as_str().to_string();
        let localai_url = config.localai.url.clone();
        let localai_model = config.localai.model.clone();
        let mut settings = self.settings.write().await;
        settings.embedding_provider = embedding_mode.clone();
        settings.embedding_url = localai_url.clone();
        settings.embedding_model = localai_model.clone();
        settings.embedding_dimensions = config.dimensions.max(1);
        settings.embedding_timeout_seconds = config.timeout_seconds.max(1);
        settings.block_min_chars = config.block_min_chars;
        settings.block_chunk_bytes = config.block_chunk_bytes();
        settings.block_chunk_overlap_sentences = config.block_chunk_overlap_sentences;
        settings.block_embedding_enabled = config.block_embedding_enabled;
        settings.max_embedding_failures = config.max_embedding_failures.max(1);
        drop(settings);

        let mut guard = self.inner.write().await;
        guard.embedding_provider = embedding_mode;
        guard.embedding_url = localai_url;
        guard.embedding_model = localai_model;
        guard.embedding_dimensions = config.dimensions.max(1);
        guard.embedding_timeout_seconds = config.timeout_seconds.max(1);
        guard.block_min_chars = config.block_min_chars;
        guard.block_chunk_bytes = config.block_chunk_bytes();
        guard.block_chunk_overlap_sentences = config.block_chunk_overlap_sentences;
        guard.block_embedding_enabled = config.block_embedding_enabled;
    }

    pub async fn set_new_note_path_settings(&self, settings: NewNotePathSettings) {
        self.settings.write().await.new_note_path_settings = settings.clone();
        let mut guard = self.inner.write().await;
        guard.new_note_path_settings = settings;
    }

    pub async fn set_audit_settings(&self, enabled: bool, retention_days: u64) {
        let mut audit = self.audit.write().await;
        audit.enabled = enabled;
        audit.retention_days = retention_days;
        if !enabled {
            audit.access_log.clear();
        }
        drop(audit);

        let mut guard = self.inner.write().await;
        guard.audit_enabled = enabled;
        guard.audit_retention_days = retention_days;
        if !enabled {
            guard.access_log.clear();
        }
    }

    pub async fn upsert_note(&self, note: NoteInput) {
        let note_id = note.id.clone();
        let mut guard = self.inner.write().await;
        let now = Utc::now();
        upsert_note_locked(&mut guard, note, now);
        rebuild_graph_cache_locked(&mut guard);
        guard.last_sync_at = now;
        let runtime_sync_state = RuntimeSyncState {
            last_seq: guard.last_seq.clone(),
            couchdb_current_seq: guard.couchdb_current_seq.clone(),
            last_sync_at: guard.last_sync_at,
        };
        let persisted_note = if self.persistence.is_some() {
            note_for_persistence_locked(&guard, &note_id)
        } else {
            None
        };
        let sync_state = if self.persistence.is_some() {
            Some(sync_state_for_persistence_locked(&guard))
        } else {
            None
        };
        let block_data = guard
            .notes
            .get(&note_id)
            .map(|n| (n.path.clone(), n.title.clone(), n.content.clone()));
        drop(guard);
        *self.sync_state.write().await = runtime_sync_state;

        if let Some(persistence) = self.persistence.as_ref()
            && let (Some(note), Some(sync_state)) = (persisted_note, sync_state)
        {
            match persistence
                .apply_delta(vec![note], Vec::new(), Some(sync_state))
                .await
            {
                Ok(generation) => self.cache_generation.store(generation, Ordering::Release),
                Err(error) => {
                    warn!(error = %error, note_id = %note_id, "failed to persist note upsert")
                }
            }
        }

        if let Some((path, title, content)) = block_data {
            self.sync_blocks_for_note(note_id.as_str(), &path, &title, &content)
                .await;
        }
    }

    pub async fn delete_note(&self, note_id: &NoteId) {
        let mut guard = self.inner.write().await;
        let now = Utc::now();
        let existed = delete_note_locked(&mut guard, note_id);
        if existed {
            rebuild_graph_cache_locked(&mut guard);
        }
        guard.last_sync_at = now;
        let runtime_sync_state = RuntimeSyncState {
            last_seq: guard.last_seq.clone(),
            couchdb_current_seq: guard.couchdb_current_seq.clone(),
            last_sync_at: guard.last_sync_at,
        };
        let sync_state = if self.persistence.is_some() {
            Some(sync_state_for_persistence_locked(&guard))
        } else {
            None
        };
        drop(guard);
        *self.sync_state.write().await = runtime_sync_state;

        if existed
            && let Some(persistence) = self.persistence.as_ref()
            && let Some(sync_state) = sync_state
        {
            match persistence
                .apply_delta(
                    Vec::new(),
                    vec![note_id.as_str().to_string()],
                    Some(sync_state),
                )
                .await
            {
                Ok(generation) => self.cache_generation.store(generation, Ordering::Release),
                Err(error) => {
                    warn!(error = %error, note_id = %note_id, "failed to persist note deletion")
                }
            }
        }
    }

    /// Sync Worker A primitive: ingest a debounced batch of CouchDB changes.
    ///
    /// The full batch is applied while holding a single write lock, mirroring
    /// the PRD requirement that rename cascades become atomic from API readers'
    /// perspective.
    pub async fn ingest_changes_batch(
        &self,
        changes: Vec<ChangeEvent>,
        couchdb_current_seq: &str,
        context_window: usize,
        chunk_staging_timeout: StdDuration,
        decryptor: Option<&crate::encryption::Decryptor>,
    ) -> SyncBatchResult {
        self.ingest_changes_batch_at(
            changes,
            couchdb_current_seq,
            context_window,
            chunk_staging_timeout,
            Utc::now(),
            decryptor,
        )
        .await
    }

    pub async fn ingest_changes_batch_at(
        &self,
        changes: Vec<ChangeEvent>,
        couchdb_current_seq: &str,
        context_window: usize,
        chunk_staging_timeout: StdDuration,
        now: DateTime<Utc>,
        decryptor: Option<&crate::encryption::Decryptor>,
    ) -> SyncBatchResult {
        let mut guard = self.inner.write().await;
        let mut result = SyncBatchResult {
            indexed_notes: 0,
            deleted_notes: 0,
            pending_chunks: 0,
            purged_parent_ids: Vec::new(),
            recovery_parent_ids: Vec::new(),
            orphan_leaf_parent_ids: Vec::new(),
            last_seq: None,
            durably_applied: true,
        };
        let mut changed_note_ids: HashSet<NoteId> = HashSet::new();
        let mut deleted_note_ids: HashSet<NoteId> = HashSet::new();
        let mut graph_dirty = false;
        let mut staged_chunk_upserts: HashMap<(String, usize), PersistedStagedChunk> =
            HashMap::new();
        let mut staged_parent_deletes: HashSet<String> = HashSet::new();
        let mut file_alias_upserts: HashMap<String, PersistedFileAlias> = HashMap::new();
        let mut file_alias_deletes: HashSet<String> = HashSet::new();
        let mut vault_file_changed_paths: HashSet<String> = HashSet::new();
        let mut vault_file_deleted_paths: HashSet<String> = HashSet::new();

        // Pre-register file metadata so leaf chunks can be resolved to stable
        // vault paths even when `_changes` delivers file/leaf docs out-of-order.
        for change in &changes {
            if is_deletion(change) {
                continue;
            }
            let Some(doc) = change.doc.clone() else {
                continue;
            };
            let Ok(parsed_doc) = LivesyncDocument::try_from(doc) else {
                continue;
            };
            if let LivesyncDocument::File(file) = parsed_doc
                && !file.deleted
            {
                let mut file = file;
                if let Err(e) = hydrate_file_from_encrypted_metadata(&mut file, decryptor) {
                    warn!(file_id = %file.id, error = %e, "failed to decrypt file metadata, skipping");
                    continue;
                }
                if let Some(note_path) = file_document_vault_file_id(&file) {
                    let persisted_alias = persisted_file_alias_from_file(&file, &note_path);
                    let child_ids = persisted_alias.children.clone();
                    let removed_file_aliases =
                        register_file_aliases_locked(&mut guard, &file, note_path.clone());
                    for removed_file_alias in removed_file_aliases {
                        file_alias_upserts.remove(&removed_file_alias);
                        file_alias_deletes.insert(removed_file_alias);
                    }
                    let rehomed_notes = rehome_staged_chunks_locked(
                        &mut guard,
                        &note_path,
                        &file.id,
                        &child_ids,
                        &file.rev,
                        context_window,
                        now,
                        &mut changed_note_ids,
                        &mut deleted_note_ids,
                        &mut staged_chunk_upserts,
                        &mut staged_parent_deletes,
                    );
                    if rehomed_notes > 0 {
                        graph_dirty = true;
                        result.indexed_notes += rehomed_notes;
                    }
                    graph_dirty |= repair_note_alias_locked(
                        &mut guard,
                        &note_path,
                        &file.id,
                        &child_ids,
                        &mut changed_note_ids,
                        &mut deleted_note_ids,
                    );
                    result.pending_chunks = guard.chunk_staging.pending_count();
                    file_alias_upserts.insert(file.id.clone(), persisted_alias);
                    file_alias_deletes.remove(&file.id);
                }
            }
        }

        for change in changes {
            let seq = sequence_to_string(&change.seq);
            if !seq.is_empty() {
                result.last_seq = Some(seq.clone());
                guard.last_seq = seq;
            }

            if is_deletion(&change) {
                if let Some(note_id) = note_id_from_deletion_change(&change) {
                    let resolved_note_id =
                        NoteId::new(resolve_note_alias_locked(&guard, note_id.as_str()));
                    staged_parent_deletes.insert(resolved_note_id.as_str().to_string());
                    if delete_note_locked(&mut guard, &resolved_note_id) {
                        graph_dirty = true;
                        result.deleted_notes += 1;
                        deleted_note_ids.insert(resolved_note_id.clone());
                        changed_note_ids.remove(&resolved_note_id);
                    }
                    // Also remove the raw vault file for all supported paths.
                    delete_vault_file_locked(&mut guard, resolved_note_id.as_str());
                    vault_file_deleted_paths.insert(resolved_note_id.as_str().to_string());
                }

                if let Some(file_doc_id) = deletion_file_doc_id(&change) {
                    staged_parent_deletes.insert(file_doc_id.clone());
                    // Remove vault file for the note path before unregistering aliases.
                    if let Some(note_path) = guard.file_doc_paths.get(&file_doc_id).cloned() {
                        delete_vault_file_locked(&mut guard, &note_path);
                        vault_file_deleted_paths.insert(note_path);
                    }
                    unregister_file_aliases_locked(&mut guard, &file_doc_id);
                    file_alias_upserts.remove(&file_doc_id);
                    file_alias_deletes.insert(file_doc_id);
                }
                continue;
            }

            let Some(doc) = change.doc else {
                continue;
            };

            let Ok(parsed_doc) = LivesyncDocument::try_from(doc) else {
                continue;
            };

            match parsed_doc {
                LivesyncDocument::Leaf(leaf) => {
                    let leaf = if leaf.e_ && crate::encryption::is_hkdf_encrypted(&leaf.data) {
                        if let Some(d) = decryptor {
                            match d.decrypt(&leaf.data) {
                                Ok(decrypted_data) => {
                                    let mut decrypted_leaf = leaf;
                                    decrypted_leaf.data = decrypted_data;
                                    decrypted_leaf.e_ = false;
                                    decrypted_leaf
                                }
                                Err(e) => {
                                    warn!(leaf_id = %leaf.id, error = %e, "failed to decrypt leaf, skipping");
                                    continue;
                                }
                            }
                        } else {
                            warn!(leaf_id = %leaf.id, "leaf is HKDF-encrypted but no decryptor configured, skipping");
                            continue;
                        }
                    } else {
                        leaf
                    };
                    let Ok(chunk) = decode_leaf_chunk(&leaf, now) else {
                        continue;
                    };

                    for chunk in apply_chunk_aliases_locked(&guard, &leaf.id, chunk) {
                        if !is_supported_vault_file_path(&chunk.parent_id) {
                            let pending_chunk = chunk.clone();
                            guard.chunk_staging.restore_pending_chunk(chunk);
                            result.pending_chunks = guard.chunk_staging.pending_count();
                            staged_chunk_upserts.insert(
                                (pending_chunk.parent_id.clone(), pending_chunk.chunk_index),
                                persisted_staged_chunk_from_decoded(&pending_chunk),
                            );
                            continue;
                        }
                        let pending_chunk = chunk.clone();
                        let is_md = is_markdown_note_path(&chunk.parent_id);

                        match guard.chunk_staging.stage(chunk) {
                            StageResult::Pending { .. } => {
                                result.pending_chunks = guard.chunk_staging.pending_count();
                                staged_chunk_upserts.insert(
                                    (pending_chunk.parent_id.clone(), pending_chunk.chunk_index),
                                    persisted_staged_chunk_from_decoded(&pending_chunk),
                                );
                            }
                            StageResult::Complete(note) => {
                                staged_parent_deletes.insert(note.parent_id.clone());
                                staged_chunk_upserts.retain(|(parent_id, _), _| {
                                    parent_id != note.parent_id.as_str()
                                });

                                // Store raw vault file for all supported paths.
                                let file_timestamps =
                                    note_timestamp_hints_for_parent_locked(&guard, &note.parent_id);
                                let created_at = file_timestamps.created_at;
                                let updated_at = file_timestamps.updated_at.unwrap_or(now);
                                upsert_vault_file_locked(
                                    &mut guard,
                                    &note.parent_id,
                                    &note.content,
                                    &note.couchdb_rev,
                                    created_at,
                                    updated_at,
                                    now,
                                );
                                vault_file_changed_paths.insert(note.parent_id.clone());

                                if is_md {
                                    index_reassembled_note_locked(
                                        &mut guard,
                                        note,
                                        context_window,
                                        now,
                                        &mut changed_note_ids,
                                        &mut deleted_note_ids,
                                    );
                                    graph_dirty = true;
                                    result.indexed_notes += 1;
                                }
                                result.pending_chunks = guard.chunk_staging.pending_count();
                            }
                        }
                    }
                }
                LivesyncDocument::File(mut file) => {
                    if let Err(e) = hydrate_file_from_encrypted_metadata(&mut file, decryptor) {
                        warn!(file_id = %file.id, error = %e, "failed to decrypt file metadata, skipping");
                        continue;
                    }
                    if file.deleted
                        && let Some(note_id) = file_document_vault_file_id(&file).map(NoteId::new)
                    {
                        staged_parent_deletes.insert(note_id.as_str().to_string());
                        delete_vault_file_locked(&mut guard, note_id.as_str());
                        vault_file_deleted_paths.insert(note_id.as_str().to_string());
                        if delete_note_locked(&mut guard, &note_id) {
                            graph_dirty = true;
                            result.deleted_notes += 1;
                            deleted_note_ids.insert(note_id.clone());
                            changed_note_ids.remove(&note_id);
                        }
                        file_alias_upserts.remove(&file.id);
                        file_alias_deletes.insert(file.id.clone());
                    }
                }
                LivesyncDocument::Unknown(_) => {}
            }
        }

        let purged = guard
            .chunk_staging
            .purge_older_than(chunk_staging_timeout, now);
        let purge_result = classify_purged_chunk_staging_locked(&guard, purged);
        result.purged_parent_ids = purge_result.purged_parent_ids;
        result.recovery_parent_ids = purge_result.recovery_parent_ids;
        result.orphan_leaf_parent_ids = purge_result.orphan_leaf_parent_ids;
        if !result.purged_parent_ids.is_empty() {
            warn!(
                purged_parent_count = result.purged_parent_ids.len(),
                recovery_parent_count = result.recovery_parent_ids.len(),
                orphan_leaf_parent_count = result.orphan_leaf_parent_ids.len(),
                timeout_seconds = chunk_staging_timeout.as_secs(),
                "discarded stale chunk staging parents after timeout"
            );
        }
        for parent_id in &result.purged_parent_ids {
            staged_parent_deletes.insert(parent_id.clone());
            staged_chunk_upserts.retain(|(staged_parent, _), _| staged_parent != parent_id);
        }
        result.pending_chunks = guard.chunk_staging.pending_count();

        if !couchdb_current_seq.is_empty() {
            guard.couchdb_current_seq = couchdb_current_seq.to_string();
        }

        if result.indexed_notes > 0
            || result.deleted_notes > 0
            || !result.purged_parent_ids.is_empty()
            || result.last_seq.is_some()
        {
            guard.last_sync_at = now;
        }
        if graph_dirty {
            rebuild_graph_cache_locked(&mut guard);
        }

        let persistence_batch = if self.persistence.is_some() {
            let upserts = changed_note_ids
                .iter()
                .filter_map(|note_id| note_for_persistence_locked(&guard, note_id))
                .collect::<Vec<_>>();
            let deletes = deleted_note_ids
                .iter()
                .map(|note_id| note_id.as_str().to_string())
                .collect::<Vec<_>>();
            let sync_state = if result.last_seq.is_some()
                || !changed_note_ids.is_empty()
                || !deleted_note_ids.is_empty()
                || !result.purged_parent_ids.is_empty()
            {
                Some(sync_state_for_persistence_locked(&guard))
            } else {
                None
            };
            let mut chunk_staging_upserts = staged_chunk_upserts.into_values().collect::<Vec<_>>();
            chunk_staging_upserts.sort_by(|a, b| {
                a.parent_id
                    .cmp(&b.parent_id)
                    .then_with(|| a.chunk_index.cmp(&b.chunk_index))
            });
            let mut chunk_staging_deletes = staged_parent_deletes.into_iter().collect::<Vec<_>>();
            chunk_staging_deletes.sort();
            chunk_staging_deletes.dedup();
            let mut alias_upserts = file_alias_upserts.into_values().collect::<Vec<_>>();
            alias_upserts.sort_by(|a, b| a.file_doc_id.cmp(&b.file_doc_id));
            let mut alias_deletes = file_alias_deletes.into_iter().collect::<Vec<_>>();
            alias_deletes.sort();
            alias_deletes.dedup();

            let mut vault_file_persisted_upserts = vault_file_changed_paths
                .iter()
                .filter_map(|path| guard.vault_files.get(path))
                .map(|vf| PersistedVaultFile {
                    path: vf.path.clone(),
                    content: vf.content.clone(),
                    couchdb_rev: vf.couchdb_rev.clone(),
                    created_at: vf.created_at,
                    updated_at: vf.updated_at,
                    indexed_at: vf.indexed_at,
                })
                .collect::<Vec<_>>();
            vault_file_persisted_upserts.sort_by(|a, b| a.path.cmp(&b.path));
            let mut vault_file_persisted_deletes =
                vault_file_deleted_paths.into_iter().collect::<Vec<_>>();
            vault_file_persisted_deletes.sort();
            vault_file_persisted_deletes.dedup();

            Some((
                upserts,
                deletes,
                sync_state,
                chunk_staging_upserts,
                chunk_staging_deletes,
                alias_upserts,
                alias_deletes,
                vault_file_persisted_upserts,
                vault_file_persisted_deletes,
            ))
        } else {
            None
        };

        // Collect block sync data while lock is held.
        let block_sync_notes: Vec<(String, String, String, String)> = changed_note_ids
            .iter()
            .filter_map(|note_id| {
                let note = guard.notes.get(note_id)?;
                Some((
                    note_id.as_str().to_string(),
                    note.path.clone(),
                    note.title.clone(),
                    note.content.clone(),
                ))
            })
            .collect();
        let runtime_sync_state = RuntimeSyncState {
            last_seq: guard.last_seq.clone(),
            couchdb_current_seq: guard.couchdb_current_seq.clone(),
            last_sync_at: guard.last_sync_at,
        };
        drop(guard);
        *self.sync_state.write().await = runtime_sync_state;

        if let Some(persistence) = self.persistence.as_ref()
            && let Some((
                upserts,
                deletes,
                sync_state,
                chunk_upserts,
                chunk_deletes,
                alias_upserts,
                alias_deletes,
                vault_file_upserts,
                vault_file_deletes,
            )) = persistence_batch
        {
            let delta = PersistedIngestDelta {
                note_upserts: upserts,
                note_deletes: deletes,
                sync_state,
                chunk_upserts,
                chunk_deletes,
                alias_upserts,
                alias_deletes,
                vault_file_upserts,
                vault_file_deletes,
            };
            match persistence.apply_ingest_delta(delta).await {
                Ok(generation) => self.cache_generation.store(generation, Ordering::Release),
                Err(error) => {
                    result.durably_applied = false;
                    warn!(error = %error, "failed to persist atomic sync ingest batch");
                }
            }
        }

        // Blocks reference persisted notes, so do not advance them after a failed ingest.
        if result.durably_applied {
            for (note_id, path, title, content) in block_sync_notes {
                self.sync_blocks_for_note(&note_id, &path, &title, &content)
                    .await;
            }
        }

        result
    }

    pub async fn get_note_for_policy(&self, auth: &AuthContext, note_id: &NoteId) -> Option<Note> {
        self.prepare_cached_read("get_note lookup").await;
        let guard = self.inner.read().await;
        let config = self.authorization_config().await;
        if !note_readable_for_policy_from_inner(&guard, &config, auth, note_id, Utc::now()) {
            return None;
        }
        get_note_from_inner(&guard, note_id, Some(&config), Some(auth), Utc::now())
    }

    pub async fn get_note_by_title_for_policy(
        &self,
        auth: &AuthContext,
        title: &str,
    ) -> Option<Note> {
        self.prepare_cached_read("get_note title lookup").await;
        let guard = self.inner.read().await;
        let config = self.authorization_config().await;
        let now = Utc::now();
        let title = title.trim();
        let mut candidates = guard
            .notes
            .values()
            .filter(|note| note.title.eq_ignore_ascii_case(title))
            .filter(|note| {
                note_readable_for_policy_from_inner(&guard, &config, auth, &note.id, now)
            })
            .map(|note| (note.id.clone(), note.updated_at))
            .collect::<Vec<_>>();
        candidates.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.as_str().cmp(b.0.as_str())));
        let note_id = candidates.first().map(|(id, _)| id.clone())?;
        get_note_from_inner(&guard, &note_id, Some(&config), Some(auth), now)
    }

    pub async fn note_visibility_for_policy(
        &self,
        auth: &AuthContext,
        note_id: &NoteId,
    ) -> NoteVisibility {
        self.prepare_cached_read("note visibility lookup").await;
        let guard = self.inner.read().await;
        match guard.notes.get(note_id) {
            None => NoteVisibility::Missing,
            Some(_) => {
                let config = self.authorization_config().await;
                if note_readable_for_policy_from_inner(&guard, &config, auth, note_id, Utc::now()) {
                    NoteVisibility::Accessible
                } else {
                    NoteVisibility::Filtered
                }
            }
        }
    }

    pub async fn title_visibility_for_policy(
        &self,
        auth: &AuthContext,
        title: &str,
    ) -> NoteVisibility {
        self.prepare_cached_read("note title visibility lookup")
            .await;
        let guard = self.inner.read().await;
        let title = title.trim();
        let candidates = guard
            .notes
            .values()
            .filter(|note| note.title.eq_ignore_ascii_case(title))
            .collect::<Vec<_>>();
        if candidates.is_empty() {
            return NoteVisibility::Missing;
        }

        let config = self.authorization_config().await;
        let now = Utc::now();
        if candidates
            .iter()
            .any(|note| note_readable_for_policy_from_inner(&guard, &config, auth, &note.id, now))
        {
            NoteVisibility::Accessible
        } else {
            NoteVisibility::Filtered
        }
    }

    async fn search_raw(&self, query: &str, mode: SearchMode, limit: usize) -> SearchResponse {
        if let Some(persistence) = self.persistence.as_ref() {
            match self
                .search_via_persistence(persistence, query, mode, limit)
                .await
            {
                Ok(response) => return response,
                Err(error) => {
                    warn!(
                        error = %error,
                        "failed postgres-backed search; falling back to in-memory ranking"
                    );
                }
            }
        }

        let semantic_enabled = self.semantic_embeddings_enabled().await;
        let guard = self.inner.read().await;
        let docs = guard
            .notes
            .values()
            .map(|note| CandidateSearchDoc {
                id: note.id.clone(),
                title: title_from_note_id(note.id.as_str()),
                content: note.search_text.clone(),
                embedding: note.embedding.clone(),
            })
            .collect::<Vec<_>>();

        let fulltext = fulltext_ranking(&docs, query);
        let semantic = if semantic_enabled {
            semantic_ranking_for_query(&docs, query)
        } else {
            Vec::new()
        };

        let ranked = match mode {
            SearchMode::Fulltext => fulltext
                .iter()
                .map(|(id, score)| (id.clone(), *score, MatchType::Fulltext))
                .collect::<Vec<_>>(),
            SearchMode::Semantic => semantic
                .iter()
                .map(|(id, score)| (id.clone(), *score, MatchType::Semantic))
                .collect::<Vec<_>>(),
            SearchMode::Hybrid => hybrid_ranking(&fulltext, &semantic),
        };

        let mut results = Vec::new();

        for (id, score, match_type) in ranked {
            if let Some(note) = guard.notes.get(&id) {
                let unscoped = UnscopedSearchHit {
                    id: id.clone(),
                    title: title_from_note_id(id.as_str()),
                    snippet: snippet_for(&note.content, query),
                    score,
                    match_type,
                    matched_chunk_id: None,
                    matched_heading_path: None,
                    matched_snippet: None,
                };

                if results.len() < limit {
                    results.push(unscoped.into_hit());
                }
            }
        }

        SearchResponse::new(results, 0)
    }

    pub async fn search_for_policy(
        &self,
        auth: &AuthContext,
        query: &str,
        mode: SearchMode,
        limit: usize,
    ) -> SearchResponse {
        let ranking_limit = search_ranking_limit(limit);
        let mut response = self.search_raw(query, mode, ranking_limit).await;
        let ids = response
            .results
            .iter()
            .map(|hit| hit.id.clone())
            .collect::<Vec<_>>();
        let readable = self.readable_note_ids_for_policy(auth, &ids).await;
        response.results.retain(|hit| readable.contains(&hit.id));
        response.total_filtered = 0;
        response.results.truncate(limit);
        response
    }

    async fn search_via_persistence(
        &self,
        persistence: &PostgresPersistence,
        query: &str,
        mode: SearchMode,
        limit: usize,
    ) -> Result<SearchResponse, crate::persistence::PersistenceError> {
        let ranking_limit = search_ranking_limit(limit);

        let fulltext = if matches!(mode, SearchMode::Fulltext | SearchMode::Hybrid) {
            persistence
                .search_fulltext_ranking(query, ranking_limit)
                .await?
        } else {
            Vec::new()
        };

        let mut chunk_matches: HashMap<String, ChunkMatchMetadata> = HashMap::new();
        let semantic = if matches!(mode, SearchMode::Semantic | SearchMode::Hybrid)
            && self.semantic_embeddings_enabled().await
        {
            match persistence.embedding_dimensions().await? {
                Some(dimensions) if dimensions > 0 => {
                    let query_embedding = self.query_embedding_for_search(query, dimensions).await;
                    let note_results = persistence
                        .search_semantic_ranking(&query_embedding, ranking_limit)
                        .await?;
                    let block_results = persistence
                        .search_block_semantic_ranking(&query_embedding, ranking_limit)
                        .await?;
                    let mut merged: HashMap<String, f32> = HashMap::new();
                    for (id, score) in &note_results {
                        let entry = merged.entry(id.clone()).or_insert(0.0);
                        if *score > *entry {
                            *entry = *score;
                        }
                    }
                    let mut chunk_scores: HashMap<String, f32> = HashMap::new();
                    for block in &block_results {
                        let entry = merged.entry(block.note_id.clone()).or_insert(0.0);
                        if block.score > *entry {
                            *entry = block.score;
                        }
                        let replace = chunk_scores
                            .get(&block.note_id)
                            .is_none_or(|score| block.score > *score);
                        if replace {
                            chunk_scores.insert(block.note_id.clone(), block.score);
                            chunk_matches
                                .insert(block.note_id.clone(), chunk_match_metadata(block, query));
                        }
                    }
                    let mut merged_vec: Vec<(String, f32)> = merged.into_iter().collect();
                    merged_vec.sort_by(|a, b| {
                        b.1.partial_cmp(&a.1)
                            .unwrap_or(std::cmp::Ordering::Equal)
                            .then_with(|| a.0.cmp(&b.0))
                    });
                    merged_vec.truncate(ranking_limit);
                    merged_vec
                }
                _ => Vec::new(),
            }
        } else {
            Vec::new()
        };

        let fulltext_ids = fulltext
            .into_iter()
            .map(|(id, score)| (NoteId::new(id), score))
            .collect::<Vec<_>>();
        let semantic_ids = semantic
            .into_iter()
            .map(|(id, score)| (NoteId::new(id), score))
            .collect::<Vec<_>>();

        let ranked = match mode {
            SearchMode::Fulltext => fulltext_ids
                .iter()
                .map(|(id, score)| (id.clone(), *score, MatchType::Fulltext))
                .collect::<Vec<_>>(),
            SearchMode::Semantic => semantic_ids
                .iter()
                .map(|(id, score)| (id.clone(), *score, MatchType::Semantic))
                .collect::<Vec<_>>(),
            SearchMode::Hybrid => hybrid_ranking(&fulltext_ids, &semantic_ids),
        };

        let mut ordered_ids = Vec::new();
        let mut seen = HashSet::new();
        for (id, _, _) in &ranked {
            let id_string = id.as_str().to_string();
            if seen.insert(id_string.clone()) {
                ordered_ids.push(id_string);
            }
        }
        let note_map = persistence.load_search_note_map(&ordered_ids).await?;

        let mut results = Vec::new();
        for (id, score, match_type) in ranked {
            let Some(note) = note_map.get(id.as_str()) else {
                continue;
            };
            let unscoped = search_hit_from_persisted(
                note,
                query,
                score,
                match_type,
                &id,
                chunk_matches.get(id.as_str()).cloned(),
            );
            if results.len() < limit {
                results.push(unscoped.into_hit());
            }
        }

        Ok(SearchResponse::new(results, 0))
    }

    async fn semantic_embeddings_enabled(&self) -> bool {
        let settings = self.settings.read().await;
        !settings.embedding_provider.eq_ignore_ascii_case("disabled")
    }

    async fn query_embedding_for_search(&self, query: &str, dimensions: usize) -> Vec<f32> {
        let effective_dimensions = dimensions.max(1);
        let settings = {
            let guard = self.settings.read().await;
            SearchEmbeddingSettings {
                provider: guard.embedding_provider.clone(),
                url: guard.embedding_url.clone(),
                model: guard.embedding_model.clone(),
                timeout_seconds: guard.embedding_timeout_seconds.max(1),
            }
        };

        if settings.provider.eq_ignore_ascii_case("disabled") {
            return Vec::new();
        }

        if !settings.provider.eq_ignore_ascii_case("localai") || settings.url.trim().is_empty() {
            return embed_text(query, effective_dimensions);
        }

        match fetch_localai_search_embedding(&settings, query, effective_dimensions).await {
            Ok(embedding) => embedding,
            Err(error) => {
                warn!(
                    error = %error,
                    dimensions = effective_dimensions,
                    "semantic search: LocalAI query embedding failed; using deterministic fallback"
                );
                embed_text(query, effective_dimensions)
            }
        }
    }

    async fn semantic_ranking_for_query_with_store(
        &self,
        docs: &[CandidateSearchDoc],
        query: &str,
    ) -> Vec<(NoteId, f32)> {
        let mut docs_by_dimensions: HashMap<usize, Vec<CandidateSearchDoc>> = HashMap::new();
        for doc in docs {
            let Some(embedding) = doc.embedding.as_ref() else {
                continue;
            };
            if embedding.is_empty() {
                continue;
            }
            docs_by_dimensions
                .entry(embedding.len())
                .or_default()
                .push(doc.clone());
        }

        let mut ranked = Vec::new();
        for (dimensions, docs_for_dimension) in docs_by_dimensions {
            let query_embedding = self.query_embedding_for_search(query, dimensions).await;
            ranked.extend(semantic_ranking(&docs_for_dimension, &query_embedding));
        }

        ranked.sort_by(|a, b| {
            b.1.total_cmp(&a.1)
                .then_with(|| a.0.as_str().cmp(b.0.as_str()))
        });
        ranked
    }

    async fn ranked_note_hits_for_query(
        &self,
        docs: &[CandidateSearchDoc],
        query: &str,
        mode: SearchMode,
    ) -> Vec<RankedNoteHit> {
        let fulltext = if matches!(mode, SearchMode::Fulltext | SearchMode::Hybrid) {
            fulltext_ranking(docs, query)
        } else {
            Vec::new()
        };
        let semantic = if matches!(mode, SearchMode::Semantic | SearchMode::Hybrid)
            && self.semantic_embeddings_enabled().await
        {
            let allowed_ids = docs
                .iter()
                .map(|doc| doc.id.clone())
                .collect::<HashSet<_>>();
            let mut semantic_scores = self
                .semantic_ranking_for_query_with_store(docs, query)
                .await
                .into_iter()
                .collect::<HashMap<_, _>>();
            let mut chunk_matches = HashMap::new();
            if let Some(persistence) = self.persistence.as_ref() {
                match persistence.embedding_dimensions().await {
                    Ok(Some(dimensions)) if dimensions > 0 => {
                        let query_embedding =
                            self.query_embedding_for_search(query, dimensions).await;
                        match persistence
                            .search_block_semantic_ranking(
                                &query_embedding,
                                search_ranking_limit(docs.len().max(20)),
                            )
                            .await
                        {
                            Ok(block_results) => {
                                let mut chunk_scores: HashMap<NoteId, f32> = HashMap::new();
                                for block in block_results {
                                    let note_id = NoteId::new(block.note_id.clone());
                                    if !allowed_ids.contains(&note_id) {
                                        continue;
                                    }
                                    let entry =
                                        semantic_scores.entry(note_id.clone()).or_insert(0.0);
                                    if block.score > *entry {
                                        *entry = block.score;
                                    }
                                    let replace = chunk_scores
                                        .get(&note_id)
                                        .is_none_or(|score| block.score > *score);
                                    if replace {
                                        chunk_scores.insert(note_id.clone(), block.score);
                                        chunk_matches
                                            .insert(note_id, chunk_match_metadata(&block, query));
                                    }
                                }
                            }
                            Err(error) => {
                                warn!(
                                    error = %error,
                                    "query_notes semantic chunk ranking failed; using note embeddings only"
                                );
                            }
                        }
                    }
                    Ok(_) => {}
                    Err(error) => {
                        warn!(
                            error = %error,
                            "query_notes failed to read embedding dimensions; using note embeddings only"
                        );
                    }
                }
            }

            let mut ranked = semantic_scores.into_iter().collect::<Vec<_>>();
            ranked.sort_by(|a, b| {
                b.1.total_cmp(&a.1)
                    .then_with(|| a.0.as_str().cmp(b.0.as_str()))
            });
            ranked
                .into_iter()
                .map(|(id, score)| {
                    let chunk_match = chunk_matches.remove(&id);
                    (id, score, chunk_match)
                })
                .collect::<Vec<_>>()
        } else {
            Vec::new()
        };

        let ranked = match mode {
            SearchMode::Fulltext => fulltext
                .into_iter()
                .map(|(id, score)| (id, score, MatchType::Fulltext, None))
                .collect::<Vec<_>>(),
            SearchMode::Semantic => semantic
                .into_iter()
                .map(|(id, score, chunk_match)| (id, score, MatchType::Semantic, chunk_match))
                .collect::<Vec<_>>(),
            SearchMode::Hybrid => {
                let semantic_pairs = semantic
                    .iter()
                    .map(|(id, score, _)| (id.clone(), *score))
                    .collect::<Vec<_>>();
                let chunk_matches = semantic
                    .into_iter()
                    .filter_map(|(id, _, chunk_match)| chunk_match.map(|m| (id, m)))
                    .collect::<HashMap<_, _>>();
                hybrid_ranking(&fulltext, &semantic_pairs)
                    .into_iter()
                    .map(|(id, score, match_type)| {
                        let chunk_match = chunk_matches.get(&id).cloned();
                        (id, score, match_type, chunk_match)
                    })
                    .collect::<Vec<_>>()
            }
        };

        let mut hits = Vec::new();
        let mut seen = HashSet::new();
        for (id, score, match_type, chunk_match) in ranked {
            if seen.insert(id.clone()) {
                hits.push(RankedNoteHit {
                    id,
                    score,
                    match_type,
                    chunk_match,
                });
            }
        }
        hits
    }

    pub async fn recent_notes_for_policy(
        &self,
        auth: &AuthContext,
        since: Option<DateTime<Utc>>,
        last_n_days: Option<i64>,
        limit: usize,
    ) -> Result<RecentNotesResponse, &'static str> {
        let threshold = if let Some(since_value) = since {
            since_value
        } else if let Some(days) = last_n_days {
            if days <= 0 {
                return Err("last_n_days must be positive");
            }
            Utc::now() - Duration::days(days)
        } else {
            return Err("one of since or last_n_days is required");
        };
        let effective_limit = limit.min(MAX_NOTE_LIST_LIMIT);
        self.prepare_cached_read("recent_notes policy query").await;
        let guard = self.inner.read().await;
        let config = self.authorization_config().await;
        let now = Utc::now();

        let mut candidates = guard
            .notes
            .values()
            .filter(|note| note.updated_at > threshold)
            .map(|note| (note.id.clone(), note.updated_at))
            .collect::<Vec<_>>();
        candidates.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.as_str().cmp(b.0.as_str())));

        let mut total = 0usize;
        let mut notes = Vec::new();

        for (note_id, _) in candidates {
            let Some(note) = guard.notes.get(&note_id) else {
                continue;
            };
            let unscoped = build_recent_note_summary(&guard, note);
            if policy_allows(&config, auth, "read", &policy_note_from_stored(note), now) {
                total += 1;
                if notes.len() < effective_limit {
                    notes.push(unscoped.into_summary());
                }
            }
        }

        Ok(RecentNotesResponse::new(notes, total, 0))
    }

    pub async fn query_notes_for_policy(
        &self,
        auth: &AuthContext,
        request: QueryNotesRequest,
    ) -> RecentNotesResponse {
        self.prepare_cached_read("query_notes policy query").await;
        let guard = self.inner.read().await;
        self.query_notes_from_inner(&guard, &request, auth).await
    }

    pub async fn query_base_for_policy(
        &self,
        auth: &AuthContext,
        request: QueryBaseRequest,
    ) -> Result<QueryBaseResponse, BaseQueryError> {
        self.prepare_cached_read("query_base policy query").await;
        let guard = self.inner.read().await;
        let config = self.authorization_config().await;
        let now = Utc::now();
        let mut outgoing_links: HashMap<NoteId, Vec<String>> = HashMap::new();
        for link in &guard.links {
            outgoing_links
                .entry(link.source_id.clone())
                .or_default()
                .push(link.target_id.as_str().to_string());
        }

        let candidates = guard
            .notes
            .values()
            .filter(|note| {
                policy_allows(&config, auth, "read", &policy_note_from_stored(note), now)
            })
            .map(|note| BaseQueryCandidate {
                id: note.id.clone(),
                path: note.path.clone(),
                title: note.title.clone(),
                frontmatter: note.frontmatter.clone(),
                tags: note.tags.clone(),
                created_at: note.created_at,
                updated_at: note.updated_at,
                links: outgoing_links.get(&note.id).cloned().unwrap_or_default(),
            })
            .collect::<Vec<_>>();

        execute_query_base(request, candidates, MAX_NOTE_LIST_LIMIT, now)
    }

    pub async fn neighbors_for_policy(
        &self,
        auth: &AuthContext,
        center: &NoteId,
        depth: usize,
        direction: NeighborDirection,
    ) -> Option<NeighborsResponse> {
        self.prepare_cached_read("neighbors query").await;
        let guard = self.inner.read().await;
        let config = self.authorization_config().await;
        neighbors_from_inner(&guard, &config, auth, center, depth, direction)
    }

    pub async fn backlinks_for_policy(
        &self,
        auth: &AuthContext,
        target: &NoteId,
    ) -> Option<BacklinksResponse> {
        self.prepare_cached_read("backlinks query").await;
        let guard = self.inner.read().await;
        let config = self.authorization_config().await;
        backlinks_from_inner(&guard, &config, auth, target)
    }

    pub async fn shortest_path_for_policy(
        &self,
        auth: &AuthContext,
        from: &NoteId,
        to: &NoteId,
    ) -> PathResponse {
        self.prepare_cached_read("path lookup").await;
        let guard = self.inner.read().await;
        let config = self.authorization_config().await;
        shortest_path_from_inner(&guard, &config, auth, from, to)
    }

    pub async fn tags_for_policy(
        &self,
        auth: &AuthContext,
        filter: NoteTimeFilter,
    ) -> TagsResponse {
        self.prepare_cached_read("policy tag listing").await;
        let guard = self.inner.read().await;
        let config = self.authorization_config().await;
        let now = Utc::now();
        let mut counts: HashMap<String, usize> = HashMap::new();

        for note in guard.notes.values() {
            if !note_matches_time_filter(note, &filter)
                || !policy_allows(&config, auth, "read", &policy_note_from_stored(note), now)
            {
                continue;
            }
            for tag in &note.tags {
                *counts.entry(tag.clone()).or_default() += 1;
            }
        }

        let mut tags = counts
            .into_iter()
            .map(|(tag, count)| TagCount { tag, count })
            .collect::<Vec<_>>();
        tags.sort_by(|a, b| b.count.cmp(&a.count).then_with(|| a.tag.cmp(&b.tag)));
        TagsResponse { tags }
    }

    pub async fn note_readable_for_policy(&self, auth: &AuthContext, note_id: &NoteId) -> bool {
        self.prepare_cached_read("policy note read").await;
        let guard = self.inner.read().await;
        let config = self.authorization_config().await;
        note_readable_for_policy_from_inner(&guard, &config, auth, note_id, Utc::now())
    }

    pub async fn readable_note_ids_for_policy(
        &self,
        auth: &AuthContext,
        note_ids: &[NoteId],
    ) -> HashSet<NoteId> {
        self.prepare_cached_read("policy note set read").await;
        let guard = self.inner.read().await;
        let config = self.authorization_config().await;
        let now = Utc::now();
        note_ids
            .iter()
            .filter(|note_id| {
                note_readable_for_policy_from_inner(&guard, &config, auth, note_id, now)
            })
            .cloned()
            .collect()
    }

    async fn query_notes_from_inner(
        &self,
        guard: &StoreInner,
        request: &QueryNotesRequest,
        auth: &AuthContext,
    ) -> RecentNotesResponse {
        let text_query = request
            .text_query
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty());
        let ranking_mode = request.search_mode.unwrap_or_default();
        let limit = request.limit.unwrap_or(20).min(MAX_NOTE_LIST_LIMIT);
        let mut ranked_hits: HashMap<NoteId, RankedNoteHit> = HashMap::new();

        let auth_config = self.authorization_config().await;
        let now = Utc::now();
        let mut note_ids = guard
            .notes
            .values()
            .filter(|note| note_matches_query_filters(note, request))
            .filter_map(|note| {
                if !policy_allows(
                    &auth_config,
                    auth,
                    "read",
                    &policy_note_from_stored(note),
                    now,
                ) {
                    None
                } else {
                    Some(note.id.clone())
                }
            })
            .collect::<Vec<_>>();

        if let Some(query) = text_query {
            let docs = note_ids
                .iter()
                .filter_map(|note_id| guard.notes.get(note_id))
                .map(|note| CandidateSearchDoc {
                    id: note.id.clone(),
                    title: title_from_note_id(note.id.as_str()),
                    content: note.search_text.clone(),
                    embedding: note.embedding.clone(),
                })
                .collect::<Vec<_>>();
            let hits = self
                .ranked_note_hits_for_query(&docs, query, ranking_mode)
                .await;
            note_ids = hits.iter().map(|hit| hit.id.clone()).collect::<Vec<_>>();
            ranked_hits = hits
                .into_iter()
                .map(|hit| (hit.id.clone(), hit))
                .collect::<HashMap<_, _>>();
        }

        let sort_field = effective_query_sort_field(request.sort_by, text_query.is_some());
        let sort_order = effective_sort_order(sort_field, request.sort_order);
        if !matches!(sort_field, NoteSortField::Relevance) {
            note_ids
                .sort_by(|a, b| compare_note_ids_for_query(guard, a, b, sort_field, sort_order));
        } else if matches!(sort_order, SortOrder::Asc) {
            note_ids.reverse();
        }

        let mut total = 0usize;
        let mut notes = Vec::new();

        for note_id in note_ids {
            let Some(note) = guard.notes.get(&note_id) else {
                continue;
            };
            let ranked_hit = ranked_hits.get(&note_id);
            let mut unscoped = build_recent_note_summary(guard, note);
            if let Some(hit) = ranked_hit {
                unscoped.search_score = Some(hit.score);
                unscoped.search_match_type = Some(hit.match_type.clone());
                if let Some(chunk_match) = hit.chunk_match.clone() {
                    unscoped.matched_chunk_id = Some(chunk_match.block_id);
                    unscoped.matched_heading_path =
                        (!chunk_match.heading_path.is_empty()).then_some(chunk_match.heading_path);
                    unscoped.matched_snippet = Some(chunk_match.snippet);
                }
            }
            let mut summary = unscoped.into_summary();
            total += 1;
            if notes.len() < limit {
                if ranked_hit.is_some()
                    && let Some(query) = text_query
                    && summary.search_snippet.is_none()
                {
                    summary.search_snippet = Some(snippet_for(&note.content, query));
                }
                notes.push(summary);
            }
        }

        RecentNotesResponse::new(notes, total, 0)
    }

    pub async fn assemble_context_for_policy(
        &self,
        auth: &AuthContext,
        request: AssembleContextRequest,
    ) -> AssembleContextResponse {
        self.prepare_cached_read("context assembly").await;
        let guard = self.inner.read().await;
        self.assemble_context_from_inner(&guard, request, auth)
            .await
    }

    async fn assemble_context_from_inner(
        &self,
        guard: &StoreInner,
        request: AssembleContextRequest,
        auth: &AuthContext,
    ) -> AssembleContextResponse {
        let settings = self.settings.read().await.clone();
        let auth_config = self.authorization_config().await;
        let now = Utc::now();
        let readable_ids = readable_note_ids_from_inner(guard, &auth_config, auth, now);
        let graph = filtered_graph_from_ids(guard, readable_ids.clone());
        let readable = |id: &NoteId| graph.is_accessible(id);

        let max_depth = request
            .max_depth
            .unwrap_or(settings.context_default_max_depth)
            .clamp(1, MAX_GRAPH_TRAVERSAL_DEPTH);
        let max_tokens = request
            .max_tokens
            .unwrap_or(settings.context_default_max_tokens)
            .min(settings.context_max_max_tokens.max(1));
        let include_graph_summary = request.include_graph_summary.unwrap_or(true);
        let format = request.format.unwrap_or_default();
        let seed_query = request.seed_query;
        let semantic_enabled = !settings.embedding_provider.eq_ignore_ascii_case("disabled");

        let mut seed_ids = request
            .seeds
            .into_iter()
            .filter(|id| readable(id))
            .collect::<HashSet<_>>();

        let query_dimensions = guard
            .notes
            .values()
            .filter(|note| readable(&note.id))
            .filter_map(|note| note.embedding.as_ref().map(Vec::len))
            .find(|dim| *dim > 0)
            .unwrap_or(64);
        let query_embedding = if semantic_enabled && let Some(seed_query) = seed_query.as_deref() {
            self.query_embedding_for_search(seed_query, query_dimensions)
                .await
        } else {
            Vec::new()
        };
        let query_embedding_ref = if query_embedding.is_empty() {
            None
        } else {
            Some(query_embedding.as_slice())
        };

        if semantic_enabled && let Some(seed_query) = seed_query.as_deref() {
            let semantic_hits = self
                .search_for_policy(auth, seed_query, SearchMode::Semantic, 8)
                .await;
            seed_ids.extend(
                semantic_hits
                    .results
                    .into_iter()
                    .map(|hit| hit.id)
                    .filter(|id| readable(id)),
            );
        }

        if seed_ids.is_empty() {
            return assemble_context(Vec::new(), max_tokens, include_graph_summary, None, format);
        }

        let mut distances: HashMap<NoteId, usize> = HashMap::new();
        let mut queue = VecDeque::new();

        for seed in &seed_ids {
            distances.insert(seed.clone(), 0);
            queue.push_back(seed.clone());
        }

        while let Some(node) = queue.pop_front() {
            let depth = distances.get(&node).copied().unwrap_or(0);
            if depth >= max_depth {
                continue;
            }

            let neighbors = guard.notes.get(&node).map_or_else(Vec::new, |note| {
                if is_hub_note_with_settings(guard, &settings, note) {
                    limited_hub_neighbors(
                        guard,
                        &graph,
                        &node,
                        query_embedding_ref,
                        settings.hub_note_fanout,
                    )
                } else {
                    graph.get_neighbors(&node)
                }
            });

            for next in neighbors {
                if !readable(&next) {
                    continue;
                }
                if let std::collections::hash_map::Entry::Vacant(entry) =
                    distances.entry(next.clone())
                {
                    entry.insert(depth + 1);
                    queue.push_back(next);
                }
            }
        }

        let candidates = distances
            .into_iter()
            .filter_map(|(id, depth)| {
                let note = guard.notes.get(&id)?;
                let backlinks = graph
                    .get_backlinks(&id)
                    .into_iter()
                    .filter(|link_id| readable(link_id))
                    .collect::<Vec<_>>();
                let links_to = if is_hub_note_with_settings(guard, &settings, note) {
                    limited_hub_neighbors(
                        guard,
                        &graph,
                        &id,
                        query_embedding_ref,
                        settings.hub_note_fanout,
                    )
                    .into_iter()
                    .filter(|link_id| readable(link_id))
                    .collect::<Vec<_>>()
                } else {
                    graph
                        .get_neighbors(&id)
                        .into_iter()
                        .filter(|link_id| readable(link_id))
                        .collect::<Vec<_>>()
                };
                Some(ContextCandidate {
                    id: id.clone(),
                    title: title_from_note_id(id.as_str()),
                    content: note.content.clone(),
                    summary: note.summary.clone(),
                    links_to,
                    linked_from: backlinks,
                    embedding: note.embedding.clone(),
                    depth,
                    is_seed: seed_ids.contains(&id),
                    is_hub: is_hub_note_with_settings(guard, &settings, note),
                })
            })
            .collect::<Vec<_>>();

        assemble_context(
            candidates,
            max_tokens,
            include_graph_summary,
            query_embedding_ref,
            format,
        )
    }

    pub async fn create_note(
        &self,
        request: NewNoteRequest,
    ) -> Result<NewNoteResponse, WriteError> {
        self.create_note_at(request, Utc::now()).await
    }

    pub async fn validate_new_note_write_at(
        &self,
        request: &NewNoteRequest,
        now: DateTime<Utc>,
    ) -> Result<String, WriteError> {
        let path_settings = self.settings.read().await.new_note_path_settings.clone();
        let path = request.validate_write_with_settings_at(now, &path_settings)?;
        let note_id = NoteId::new(path.clone());

        if let Some(persistence) = self.persistence.as_ref() {
            match persistence.vault_path_exists(&path).await {
                Ok(true) => return Err(WriteError::AlreadyExists { path }),
                Ok(false) => {}
                Err(error) => {
                    warn!(
                        error = %error,
                        note_id = %note_id,
                        "failed postgres-backed vault path existence check; using in-memory fallback"
                    );
                }
            }
        }

        let guard = self.inner.read().await;
        if guard.notes.contains_key(&note_id)
            || guard.vault_files.contains_key(&path)
            || guard.created_file_paths.contains(&path)
        {
            return Err(WriteError::AlreadyExists { path });
        }

        Ok(path)
    }

    pub async fn prepare_create_note_request(
        &self,
        auth: &AuthContext,
        mut request: NewNoteRequest,
        path: &str,
        now: DateTime<Utc>,
    ) -> Result<NewNoteRequest, WriteError> {
        validate_create_template_reference(&request)?;
        if request.file_type == NewNoteFileType::Base {
            validate_base_file_content(&request.content)?;
        }
        let note = policy_note_from_create_request(&request, path, now)?;
        let config = self.authorization_config().await;
        let Some(decision) = policy_decision_for(&config, auth, "create", &note, now) else {
            return Err(WriteError::PolicyDenied {
                operation: "create",
                path: path.to_string(),
                reason: "no matching create rule in the effective authorization policy".to_string(),
            });
        };

        let owner_principal = decision.set_owner.then_some(auth.principal.as_str());
        request.apply_markdown_create_metadata(now, &decision.add_tags, owner_principal)?;
        self.validate_create_template_content(auth, &request)
            .await?;

        let note = policy_note_from_create_request(&request, path, now)?;
        if !policy_allows(&config, auth, "create", &note, now) {
            return Err(WriteError::PolicyDenied {
                operation: "create",
                path: path.to_string(),
                reason:
                    "created note metadata would not satisfy the effective authorization policy"
                        .to_string(),
            });
        }

        Ok(request)
    }

    async fn validate_create_template_content(
        &self,
        auth: &AuthContext,
        request: &NewNoteRequest,
    ) -> Result<(), WriteError> {
        let Some(template_id) = request.template_id.as_deref() else {
            return Ok(());
        };

        match request.file_type {
            NewNoteFileType::Md => {
                let template = self
                    .get_note_for_policy(auth, &NoteId::new(template_id.to_string()))
                    .await
                    .ok_or_else(|| WriteError::TemplateNotFound {
                        path: template_id.to_string(),
                    })?;
                validate_markdown_content_against_template(&request.content, &template.frontmatter)
            }
            NewNoteFileType::Base => validate_base_file_content(&request.content),
        }
    }

    pub async fn create_note_at(
        &self,
        request: NewNoteRequest,
        now: DateTime<Utc>,
    ) -> Result<NewNoteResponse, WriteError> {
        let write = self.prepare_create_vault_write_at(request, now).await?;
        let response = NewNoteResponse {
            id: NoteId::new(write.path.clone()),
            status: "created",
            file_type: write.file_type,
            indexed_as_note: write.note.is_some(),
            local_projection: "applied",
            operation_id: write.operation_id.clone(),
        };
        self.commit_prepared_vault_write(write, "local-new-note")
            .await?;
        Ok(response)
    }

    pub async fn prepare_create_vault_write_at(
        &self,
        request: NewNoteRequest,
        now: DateTime<Utc>,
    ) -> Result<PreparedVaultWrite, WriteError> {
        let path = self.validate_new_note_write_at(&request, now).await?;
        if request.file_type == NewNoteFileType::Base {
            validate_base_file_content(&request.content)?;
        }
        let note = if request.file_type.is_markdown() {
            let max_link_context_chars = self.settings.read().await.max_link_context_chars;
            Some(note_input_from_raw_markdown(
                &path,
                &request.content,
                "",
                Some(now),
                now,
                max_link_context_chars,
            ))
        } else {
            None
        };

        let operation_id = write_operation_id(&path, &request.content, now);
        Ok(PreparedVaultWrite {
            path,
            content: request.content,
            file_type: request.file_type,
            created_at: Some(now),
            updated_at: now,
            note,
            mark_created: true,
            expected_couchdb_rev: None,
            operation_id,
        })
    }

    pub async fn commit_prepared_vault_write(
        &self,
        write: PreparedVaultWrite,
        couchdb_rev: &str,
    ) -> Result<(), WriteError> {
        match self
            .apply_prepared_vault_write(write, couchdb_rev, false)
            .await?
        {
            LocalProjectionOutcome::Applied => Ok(()),
            LocalProjectionOutcome::Pending { failure_kind } => {
                Err(WriteError::Persistence { kind: failure_kind })
            }
        }
    }

    pub async fn project_source_committed_vault_write(
        &self,
        write: PreparedVaultWrite,
        couchdb_rev: &str,
    ) -> Result<LocalProjectionOutcome, WriteError> {
        self.apply_prepared_vault_write(write, couchdb_rev, true)
            .await
    }

    async fn apply_prepared_vault_write(
        &self,
        mut write: PreparedVaultWrite,
        couchdb_rev: &str,
        publish_after_persistence_failure: bool,
    ) -> Result<LocalProjectionOutcome, WriteError> {
        let couchdb_rev = couchdb_rev.to_string();
        if let Some(note) = write.note.as_mut() {
            note.couchdb_rev = couchdb_rev.clone();
        }
        let prepared_note = write
            .note
            .take()
            .map(|note| prepare_note_upsert(note, write.updated_at));
        let persisted_note = prepared_note.as_ref().map(persisted_note_from_prepared);
        let stored_file = StoredVaultFile {
            path: write.path.clone(),
            content: write.content.clone(),
            couchdb_rev: couchdb_rev.clone(),
            created_at: write.created_at,
            updated_at: write.updated_at,
            indexed_at: write.updated_at,
        };
        let persisted_file = PersistedVaultFile {
            path: stored_file.path.clone(),
            content: stored_file.content.clone(),
            couchdb_rev: stored_file.couchdb_rev.clone(),
            created_at: stored_file.created_at,
            updated_at: stored_file.updated_at,
            indexed_at: stored_file.indexed_at,
        };
        let target_path = write.path.clone();
        let target_hash = write_target_fingerprint(&target_path);
        let operation_id = write.operation_id.clone();

        let refresh_guard = self.read_refresh_lock.lock().await;
        let mut persistence_failure = None;
        #[cfg(test)]
        let forced_failure = *self.forced_projection_failure.read().await;
        #[cfg(not(test))]
        let forced_failure: Option<PersistenceFailureKind> = None;
        let generation = if let Some(failure_kind) = forced_failure {
            if !publish_after_persistence_failure {
                return Err(WriteError::Persistence { kind: failure_kind });
            }
            persistence_failure = Some(failure_kind);
            None
        } else if let Some(persistence) = self.persistence.as_ref() {
            match persistence
                .apply_content_delta(
                    persisted_note.into_iter().collect(),
                    Vec::new(),
                    None,
                    vec![persisted_file],
                    Vec::new(),
                )
                .await
            {
                Ok(generation) => Some(generation),
                Err(error) => {
                    let failure_kind = error.failure_kind();
                    warn!(
                        error = %error,
                        failure_kind = failure_kind.as_str(),
                        operation_id,
                        target_hash,
                        source_committed = publish_after_persistence_failure,
                        "failed to persist direct vault write"
                    );
                    if !publish_after_persistence_failure {
                        return Err(WriteError::Persistence { kind: failure_kind });
                    }
                    persistence_failure = Some(failure_kind);
                    None
                }
            }
        } else {
            None
        };

        let block_data = prepared_note.as_ref().map(|prepared| {
            (
                prepared.note.id.as_str().to_string(),
                prepared.note.path.clone(),
                prepared.note.title.clone(),
                prepared.note.content.clone(),
            )
        });
        {
            let mut guard = self.inner.write().await;
            if write.mark_created {
                guard.created_file_paths.insert(write.path.clone());
            }
            guard.vault_files.insert(write.path, stored_file);
            if let Some(prepared_note) = prepared_note {
                apply_prepared_note_upsert_locked(&mut guard, prepared_note);
                rebuild_graph_cache_locked(&mut guard);
            }
        }
        if let Some(generation) = generation {
            self.cache_generation.store(generation, Ordering::Release);
        }
        drop(refresh_guard);

        let outcome = if let Some(failure_kind) = persistence_failure {
            let mut health = self.projection_health.write().await;
            health.pending.insert(target_path, couchdb_rev);
            health.last_failure_at = Some(Utc::now());
            health.last_failure_kind = Some(failure_kind);
            LocalProjectionOutcome::Pending { failure_kind }
        } else {
            let mut health = self.projection_health.write().await;
            health.pending.remove(&target_path);
            health.last_success_at = Some(Utc::now());
            LocalProjectionOutcome::Applied
        };

        if matches!(outcome, LocalProjectionOutcome::Applied)
            && let Some((note_id, path, title, content)) = block_data
        {
            self.sync_blocks_for_note(&note_id, &path, &title, &content)
                .await;
        }
        Ok(outcome)
    }

    pub async fn prepare_update_note_request(
        &self,
        auth: &AuthContext,
        note_id: &NoteId,
        mut request: UpdateNoteRequest,
        now: DateTime<Utc>,
    ) -> Result<UpdateNoteRequest, WriteError> {
        self.prepare_cached_read("prepare note update").await;
        let path = note_id.as_str().to_string();
        let note = {
            let guard = self.inner.read().await;
            let Some(note) = guard.notes.get(note_id) else {
                return Err(WriteError::NotFound { path });
            };
            policy_note_from_stored(note)
        };

        let config = self.authorization_config().await;
        let Some(decision) = policy_decision_for(&config, auth, "edit", &note, now) else {
            return Err(WriteError::PolicyDenied {
                operation: "edit",
                path: note.path.to_string(),
                reason: "no matching edit rule in the effective authorization policy".to_string(),
            });
        };

        if let Some(metadata) = request.metadata.as_mut() {
            let Some(metadata) = metadata.as_object_mut() else {
                return Err(WriteError::InvalidUpdate {
                    reason: "metadata must be an object".to_string(),
                });
            };
            for managed_key in ["created", "created_by", "tags", "updated"] {
                metadata.remove(managed_key);
            }
        }

        if let Some(tags) = request.tags.as_mut() {
            for tag in decision.preserve_tags {
                if note.tags.iter().any(|candidate| {
                    crate::authorization::normalize_tag(candidate)
                        .eq_ignore_ascii_case(&crate::authorization::normalize_tag(&tag))
                }) {
                    add_unique_tag(tags, &tag);
                }
            }
            tags.sort();
            tags.dedup_by(|a, b| a.eq_ignore_ascii_case(b));
        }

        Ok(request)
    }

    /// Check whether a note exists and has a given tag (case-insensitive).
    pub async fn note_has_tag(&self, note_id: &NoteId, tag: &str) -> Option<bool> {
        let guard = self.inner.read().await;
        let note = guard.notes.get(note_id)?;
        Some(note.tags.iter().any(|t| t.eq_ignore_ascii_case(tag)))
    }

    pub async fn prepare_update_note_write_at(
        &self,
        note_id: &NoteId,
        request: &UpdateNoteRequest,
        now: DateTime<Utc>,
    ) -> Result<PreparedVaultWrite, WriteError> {
        let path = note_id.as_str().to_string();
        let (
            existing_frontmatter,
            existing_body,
            existing_tags,
            existing_created_at,
            expected_couchdb_rev,
        ) = {
            let guard = self.inner.read().await;
            let note = guard
                .notes
                .get(note_id)
                .ok_or_else(|| WriteError::NotFound { path: path.clone() })?;
            (
                note.frontmatter.clone(),
                note.content.clone(),
                note.tags.clone(),
                note.created_at,
                note.couchdb_rev.clone(),
            )
        };
        let markdown =
            request.rebuild_markdown(&existing_frontmatter, &existing_body, &existing_tags, now)?;
        let max_link_context_chars = self.settings.read().await.max_link_context_chars;
        let note = note_input_from_raw_markdown(
            &path,
            &markdown,
            "",
            existing_created_at,
            now,
            max_link_context_chars,
        );

        let operation_id = write_operation_id(&path, &markdown, now);
        Ok(PreparedVaultWrite {
            path,
            content: markdown,
            file_type: NewNoteFileType::Md,
            created_at: existing_created_at,
            updated_at: now,
            note: Some(note),
            mark_created: false,
            expected_couchdb_rev: Some(expected_couchdb_rev),
            operation_id,
        })
    }

    pub async fn update_note_at(
        &self,
        note_id: &NoteId,
        request: &UpdateNoteRequest,
        now: DateTime<Utc>,
    ) -> Result<(UpdateNoteResponse, String), WriteError> {
        let write = self
            .prepare_update_note_write_at(note_id, request, now)
            .await?;
        let markdown = write.content.clone();
        let operation_id = write.operation_id.clone();
        self.commit_prepared_vault_write(write, "local-update-note")
            .await?;
        Ok((
            UpdateNoteResponse {
                id: note_id.clone(),
                status: "updated",
                local_projection: "applied",
                operation_id,
            },
            markdown,
        ))
    }

    /// Read a raw vault file by exact path with policy enforcement.
    pub async fn get_vault_file_for_policy(
        &self,
        auth: &AuthContext,
        file_id: &NoteId,
    ) -> Option<VaultFile> {
        self.prepare_cached_read("vault file lookup").await;
        let guard = self.inner.read().await;
        let config = self.authorization_config().await;
        let now = Utc::now();
        let path = file_id.as_str();
        if !vault_file_readable_for_policy_from_inner(&guard, &config, auth, path, now) {
            return None;
        }
        get_vault_file_from_inner(&guard, path)
    }

    pub async fn vault_file_visibility_for_policy(
        &self,
        auth: &AuthContext,
        file_id: &NoteId,
    ) -> VaultFileVisibility {
        self.prepare_cached_read("vault file visibility lookup")
            .await;
        let guard = self.inner.read().await;
        let path = file_id.as_str();
        let Some(_) = guard.vault_files.get(path) else {
            return if is_markdown_note_path(path) && guard.notes.contains_key(&NoteId::new(path)) {
                VaultFileVisibility::MissingRawWithIndexedNote
            } else {
                VaultFileVisibility::Missing
            };
        };
        if is_markdown_note_path(path) && !guard.notes.contains_key(&NoteId::new(path)) {
            return VaultFileVisibility::MissingIndexWithRawMarkdown;
        }

        let config = self.authorization_config().await;
        if vault_file_readable_for_policy_from_inner(&guard, &config, auth, path, Utc::now()) {
            VaultFileVisibility::Accessible
        } else {
            VaultFileVisibility::Filtered
        }
    }

    pub async fn vault_file_recovery_target(
        &self,
        file_id: &NoteId,
    ) -> Option<StaleFileRecoveryTarget> {
        self.prepare_cached_read("vault file recovery target lookup")
            .await;
        let guard = self.inner.read().await;
        stale_file_recovery_targets_locked(&guard)
            .into_iter()
            .find(|target| target.note_path == file_id.as_str())
    }

    pub(crate) async fn recovered_vault_file_state(
        &self,
        file_id: &NoteId,
    ) -> Option<RecoveredVaultFileState> {
        let guard = self.inner.read().await;
        let file = guard.vault_files.get(file_id.as_str())?;
        Some(RecoveredVaultFileState {
            path: file.path.clone(),
            content: file.content.clone(),
            file_type: if is_markdown_note_path(&file.path) {
                NewNoteFileType::Md
            } else {
                NewNoteFileType::Base
            },
            couchdb_rev: file.couchdb_rev.clone(),
            created_at: file.created_at,
            updated_at: file.updated_at,
        })
    }

    pub(crate) async fn recovered_vault_file_from_index(
        &self,
        file_id: &NoteId,
    ) -> Result<Option<RecoveredVaultFileState>, WriteError> {
        let (frontmatter, body, tags, couchdb_rev, created_at, updated_at) = {
            let guard = self.inner.read().await;
            let Some(note) = guard.notes.get(file_id) else {
                return Ok(None);
            };
            (
                note.frontmatter.clone(),
                note.content.clone(),
                note.tags.clone(),
                note.couchdb_rev.clone(),
                note.created_at,
                note.updated_at,
            )
        };
        let content = UpdateNoteRequest {
            content: None,
            content_patch: None,
            tags: None,
            metadata: None,
        }
        .rebuild_markdown(&frontmatter, &body, &tags, updated_at)?;
        Ok(Some(RecoveredVaultFileState {
            path: file_id.as_str().to_string(),
            content,
            file_type: NewNoteFileType::Md,
            couchdb_rev,
            created_at,
            updated_at,
        }))
    }

    pub(crate) async fn project_recovered_vault_file(
        &self,
        recovered: RecoveredVaultFileState,
    ) -> Result<LocalProjectionOutcome, WriteError> {
        let couchdb_rev = recovered.couchdb_rev.clone();
        let write = self.prepared_recovered_vault_file(recovered).await;
        self.project_source_committed_vault_write(write, &couchdb_rev)
            .await
    }

    async fn prepared_recovered_vault_file(
        &self,
        recovered: RecoveredVaultFileState,
    ) -> PreparedVaultWrite {
        let note = if recovered.file_type.is_markdown() {
            let max_link_context_chars = self.settings.read().await.max_link_context_chars;
            Some(note_input_from_raw_markdown(
                &recovered.path,
                &recovered.content,
                &recovered.couchdb_rev,
                recovered.created_at,
                recovered.updated_at,
                max_link_context_chars,
            ))
        } else {
            None
        };
        let operation_id =
            write_operation_id(&recovered.path, &recovered.content, recovered.updated_at);
        PreparedVaultWrite {
            path: recovered.path,
            content: recovered.content,
            file_type: recovered.file_type,
            created_at: recovered.created_at,
            updated_at: recovered.updated_at,
            note,
            mark_created: false,
            expected_couchdb_rev: None,
            operation_id,
        }
    }

    pub(crate) async fn commit_confirmed_vault_file_deletion(
        &self,
        file_id: &NoteId,
    ) -> Result<(), WriteError> {
        self.prepare_cached_read("confirmed vault file deletion")
            .await;
        let path = file_id.as_str().to_string();
        let refresh_guard = self.read_refresh_lock.lock().await;
        let (file_doc_ids, child_doc_ids) = {
            let guard = self.inner.read().await;
            let file_doc_ids = guard
                .file_doc_paths
                .iter()
                .filter(|(_, note_path)| *note_path == &path)
                .map(|(file_doc_id, _)| file_doc_id.clone())
                .collect::<Vec<_>>();
            let child_doc_ids = file_doc_ids
                .iter()
                .flat_map(|file_doc_id| {
                    guard
                        .file_children
                        .get(file_doc_id)
                        .into_iter()
                        .flatten()
                        .cloned()
                })
                .collect::<Vec<_>>();
            (file_doc_ids, child_doc_ids)
        };

        let generation = if let Some(persistence) = self.persistence.as_ref() {
            match persistence.apply_confirmed_vault_file_deletion(&path).await {
                Ok(generation) => Some(generation),
                Err(error) => {
                    let kind = error.failure_kind();
                    warn!(error = %error, failure_kind = kind.as_str(), "failed to persist confirmed vault file deletion");
                    return Err(WriteError::Persistence { kind });
                }
            }
        } else {
            None
        };

        {
            let mut guard = self.inner.write().await;
            let note_deleted = delete_note_locked(&mut guard, file_id);
            delete_vault_file_locked(&mut guard, &path);
            guard.created_file_paths.remove(&path);
            for parent_id in std::iter::once(path.clone())
                .chain(file_doc_ids.iter().cloned())
                .chain(child_doc_ids)
            {
                guard.chunk_staging.take_parent_chunks(&parent_id);
            }
            for file_doc_id in file_doc_ids {
                unregister_file_aliases_locked(&mut guard, &file_doc_id);
            }
            if note_deleted {
                rebuild_graph_cache_locked(&mut guard);
            }
        }
        if let Some(generation) = generation {
            self.cache_generation.store(generation, Ordering::Release);
        }
        drop(refresh_guard);
        Ok(())
    }

    /// Create a raw vault file (md or base) with policy enforcement.
    pub async fn create_vault_file(
        &self,
        auth: &AuthContext,
        mut request: NewNoteRequest,
        now: DateTime<Utc>,
    ) -> Result<NewNoteResponse, WriteError> {
        let path = self.validate_new_note_write_at(&request, now).await?;
        validate_create_template_reference(&request)?;
        if request.file_type == NewNoteFileType::Base {
            validate_base_file_content(&request.content)?;
        }

        let note = policy_note_from_create_request(&request, &path, now)?;
        let config = self.authorization_config().await;
        let Some(decision) = policy_decision_for(&config, auth, "create", &note, now) else {
            return Err(WriteError::PolicyDenied {
                operation: "create",
                path: path.clone(),
                reason: "no matching create rule in the effective authorization policy".to_string(),
            });
        };

        let owner_principal = decision.set_owner.then_some(auth.principal.as_str());
        if request.file_type.is_markdown() {
            request.apply_markdown_create_metadata(now, &decision.add_tags, owner_principal)?;
        }
        self.validate_create_template_content(auth, &request)
            .await?;

        let note = policy_note_from_create_request(&request, &path, now)?;
        if !policy_allows(&config, auth, "create", &note, now) {
            return Err(WriteError::PolicyDenied {
                operation: "create",
                path: path.clone(),
                reason: "created content would not satisfy the effective authorization policy"
                    .to_string(),
            });
        }

        self.create_vault_file_at(request, now).await
    }

    pub async fn create_vault_file_at(
        &self,
        request: NewNoteRequest,
        now: DateTime<Utc>,
    ) -> Result<NewNoteResponse, WriteError> {
        let write = self.prepare_create_vault_write_at(request, now).await?;
        let response = NewNoteResponse {
            id: NoteId::new(write.path.clone()),
            status: "created",
            file_type: write.file_type,
            indexed_as_note: write.note.is_some(),
            local_projection: "applied",
            operation_id: write.operation_id.clone(),
        };
        self.commit_prepared_vault_write(write, "local-new-file")
            .await?;
        Ok(response)
    }

    pub async fn prepare_edit_vault_file_write(
        &self,
        auth: &AuthContext,
        file_id: &NoteId,
        request: UpdateNoteRequest,
        now: DateTime<Utc>,
    ) -> Result<PreparedVaultWrite, WriteError> {
        self.prepare_cached_read("prepare vault file edit").await;
        let path = file_id.as_str().to_string();
        let file_type = file_type_from_path(&path);
        let (
            existing_content,
            existing_created_at,
            existing_frontmatter,
            policy_note,
            expected_couchdb_rev,
        ) = {
            let guard = self.inner.read().await;
            let file = guard
                .vault_files
                .get(&path)
                .ok_or_else(|| WriteError::NotFound { path: path.clone() })?;
            let stored_note = if file_type.is_markdown() {
                Some(
                    guard
                        .notes
                        .get(file_id)
                        .ok_or_else(|| WriteError::NotFound { path: path.clone() })?,
                )
            } else {
                None
            };
            let policy_note =
                stored_note
                    .map(policy_note_from_stored)
                    .unwrap_or_else(|| PolicyNote {
                        path: path.clone(),
                        title: title_from_note_id(&path),
                        tags: Vec::new(),
                        created_at: file.created_at,
                        updated_at: file.updated_at,
                        owner: None,
                    });
            (
                file.content.clone(),
                file.created_at,
                stored_note.map(|note| note.frontmatter.clone()),
                policy_note,
                file.couchdb_rev.clone(),
            )
        };

        let config = self.authorization_config().await;
        let Some(decision) = policy_decision_for(&config, auth, "edit", &policy_note, now) else {
            return Err(WriteError::PolicyDenied {
                operation: "edit",
                path: path.clone(),
                reason: "no matching edit rule in the effective authorization policy".to_string(),
            });
        };
        if request.content.is_some() && request.content_patch.is_some() {
            return Err(WriteError::InvalidUpdate {
                reason: "content and content_patch are mutually exclusive".to_string(),
            });
        }
        if !file_type.is_markdown() && (request.tags.is_some() || request.metadata.is_some()) {
            return Err(WriteError::InvalidUpdate {
                reason: "tags and metadata are only valid for markdown (.md) files".to_string(),
            });
        }

        let candidate_content = match (&request.content, &request.content_patch) {
            (Some(content), None) => content.clone(),
            (None, Some(patch)) => apply_content_patch(&existing_content, patch)?,
            (None, None) => existing_content,
            (Some(_), Some(_)) => unreachable!("mutual exclusion checked above"),
        };

        let (content, note) = if file_type.is_markdown() {
            let existing_frontmatter = existing_frontmatter
                .as_ref()
                .expect("markdown edit requires indexed frontmatter");
            let content = apply_markdown_raw_update(
                &candidate_content,
                &request,
                existing_frontmatter,
                &decision.preserve_tags,
                &policy_note.tags,
                now,
            )?;
            let max_link_context_chars = self.settings.read().await.max_link_context_chars;
            let note = note_input_from_raw_markdown(
                &path,
                &content,
                "",
                existing_created_at,
                now,
                max_link_context_chars,
            );
            (content, Some(note))
        } else {
            validate_base_file_content(&candidate_content)?;
            (candidate_content, None)
        };

        let operation_id = write_operation_id(&path, &content, now);
        Ok(PreparedVaultWrite {
            path,
            content,
            file_type,
            created_at: existing_created_at,
            updated_at: now,
            note,
            mark_created: false,
            expected_couchdb_rev: Some(expected_couchdb_rev),
            operation_id,
        })
    }

    pub async fn edit_vault_file(
        &self,
        auth: &AuthContext,
        file_id: &NoteId,
        request: UpdateNoteRequest,
        now: DateTime<Utc>,
    ) -> Result<(UpdateNoteResponse, String), WriteError> {
        let write = self
            .prepare_edit_vault_file_write(auth, file_id, request, now)
            .await?;
        let content = write.content.clone();
        let operation_id = write.operation_id.clone();
        self.commit_prepared_vault_write(write, "local-edit-file")
            .await?;
        Ok((
            UpdateNoteResponse {
                id: file_id.clone(),
                status: "updated",
                local_projection: "applied",
                operation_id,
            },
            content,
        ))
    }

    /// Worker B primitive: claim a batch of notes that still need embeddings.
    pub async fn pending_embedding_ids(&self, limit: usize) -> Vec<NoteId> {
        if let Some(persistence) = self.persistence.as_ref() {
            match persistence
                .pending_embedding_batch(limit.max(1), i32::MAX)
                .await
            {
                Ok(rows) => {
                    return rows
                        .into_iter()
                        .map(|(id, _, _, _)| NoteId::new(id))
                        .collect::<Vec<_>>();
                }
                Err(error) => {
                    warn!(
                        error = %error,
                        "failed to load pending embedding ids from postgres; falling back to in-memory state"
                    );
                }
            }
        }

        let guard = self.inner.read().await;
        let mut ids = guard
            .notes
            .values()
            .filter(|note| note.embedding.is_none())
            .map(|note| note.id.clone())
            .collect::<Vec<_>>();
        ids.sort_by(|a, b| a.as_str().cmp(b.as_str()));
        ids.truncate(limit);
        ids
    }

    /// Worker B primitive: fetch note bodies needing embeddings (with breadcrumb prefix).
    pub async fn pending_embedding_batch(
        &self,
        limit: usize,
        max_failures: usize,
    ) -> Vec<(NoteId, String)> {
        if let Some(persistence) = self.persistence.as_ref() {
            let max_failures_i32 = max_failures.min(i32::MAX as usize) as i32;
            match persistence
                .pending_embedding_batch(limit.max(1), max_failures_i32)
                .await
            {
                Ok(rows) => {
                    return rows
                        .into_iter()
                        .map(|(id, path, title, content)| {
                            let prefix = breadcrumb_prefix(&path, &title, &[]);
                            let text = if prefix.is_empty() {
                                content
                            } else {
                                format!("{prefix}\n{content}")
                            };
                            (NoteId::new(id), text)
                        })
                        .collect::<Vec<_>>();
                }
                Err(error) => {
                    warn!(
                        error = %error,
                        "failed to load pending embedding batch from postgres; falling back to in-memory state"
                    );
                }
            }
        }

        let guard = self.inner.read().await;
        let mut rows = guard
            .notes
            .values()
            .filter(|note| note.embedding.is_none())
            .map(|note| {
                let prefix = breadcrumb_prefix(&note.path, &note.title, &[]);
                let text = if prefix.is_empty() {
                    note.content.clone()
                } else {
                    format!("{prefix}\n{}", note.content)
                };
                (note.id.clone(), text)
            })
            .collect::<Vec<_>>();
        rows.sort_by(|a, b| a.0.as_str().cmp(b.0.as_str()));
        rows.truncate(limit.max(1));
        rows
    }

    /// Record an embedding failure for a note (increments failure counter).
    pub async fn record_embedding_failure(&self, note_id: &NoteId) {
        if let Some(persistence) = self.persistence.as_ref()
            && let Err(error) = persistence.record_embedding_failure(note_id.as_str()).await
        {
            warn!(
                error = %error,
                note_id = note_id.as_str(),
                "failed to record embedding failure in postgres"
            );
        }
    }

    /// Worker B primitive: persist embeddings from an external provider.
    pub async fn set_embeddings(&self, updates: Vec<(NoteId, Vec<f32>)>) -> usize {
        let persistence = self.persistence.clone();
        let persisted_updates = updates
            .iter()
            .map(|(note_id, embedding)| (note_id.as_str().to_string(), embedding.clone()))
            .collect::<Vec<_>>();
        let persisted_count = persisted_updates.len();

        let mut guard = self.inner.write().await;
        let mut in_memory_updated = 0usize;
        let now = Utc::now();

        for (note_id, embedding) in &updates {
            if let Some(note) = guard.notes.get_mut(note_id) {
                note.embedding = Some(embedding.clone());
                note.indexed_at = now;
                in_memory_updated += 1;
            }
        }

        if persisted_count > 0 || in_memory_updated > 0 {
            guard.last_sync_at = now;
        }
        drop(guard);
        if persisted_count > 0 || in_memory_updated > 0 {
            self.sync_state.write().await.last_sync_at = now;
        }

        if let Some(persistence) = persistence {
            if persisted_count == 0 {
                return 0;
            }
            if let Err(error) = persistence.set_embeddings(persisted_updates).await {
                warn!(error = %error, "failed to persist embedding updates");
                return 0;
            }
            return persisted_count;
        }

        in_memory_updated
    }

    /// Worker B primitive: fetch blocks needing embeddings (persistence-only).
    pub async fn pending_block_embedding_batch(
        &self,
        limit: usize,
        max_failures: usize,
    ) -> Vec<(String, String)> {
        let Some(persistence) = self.persistence.as_ref() else {
            return Vec::new();
        };
        let max_failures_i32 = max_failures.min(i32::MAX as usize) as i32;
        match persistence
            .pending_block_embedding_batch(limit.max(1), max_failures_i32)
            .await
        {
            Ok(rows) => rows.into_iter().map(|row| (row.id, row.text)).collect(),
            Err(error) => {
                warn!(error = %error, "failed to load pending block embedding batch");
                Vec::new()
            }
        }
    }

    /// Worker B primitive: persist block embeddings from an external provider.
    pub async fn set_block_embeddings(&self, updates: Vec<(String, Vec<f32>)>) -> usize {
        let count = updates.len();
        let Some(persistence) = self.persistence.as_ref() else {
            return 0;
        };
        if let Err(error) = persistence.set_block_embeddings(updates).await {
            warn!(error = %error, "failed to persist block embedding updates");
            return 0;
        }
        count
    }

    pub async fn record_block_embedding_failure(&self, block_id: &str, message: &str) {
        if let Some(persistence) = self.persistence.as_ref()
            && let Err(error) = persistence
                .record_block_embedding_failure(block_id, message)
                .await
        {
            warn!(
                error = %error,
                block_id = block_id,
                "failed to record block embedding failure in postgres"
            );
        }
    }

    pub async fn record_embedding_provider_success(&self) {
        if let Some(persistence) = self.persistence.as_ref()
            && let Err(error) = persistence.record_embedding_runtime_success().await
        {
            warn!(
                error = %error,
                "failed to record embedding provider success in postgres"
            );
        }
    }

    pub async fn record_embedding_provider_error(&self, message: &str) {
        if let Some(persistence) = self.persistence.as_ref()
            && let Err(error) = persistence.record_embedding_runtime_error(message).await
        {
            warn!(
                error = %error,
                "failed to record embedding provider error in postgres"
            );
        }
    }

    /// Sync blocks for a note after upsert. Persistence-only, no in-memory state.
    async fn sync_blocks_for_note(&self, note_id: &str, path: &str, title: &str, content: &str) {
        let Some(persistence) = self.persistence.as_ref() else {
            return;
        };
        let (block_min_chars, block_chunk_bytes, block_chunk_overlap_sentences) = {
            let settings = self.settings.read().await;
            if !settings.block_embedding_enabled {
                return;
            }
            (
                settings.block_min_chars,
                settings.block_chunk_bytes,
                settings.block_chunk_overlap_sentences,
            )
        };

        let blocks = split_into_semantic_blocks(
            content,
            block_min_chars,
            block_chunk_bytes,
            block_chunk_overlap_sentences,
        );
        let breadcrumbs: Vec<String> = blocks
            .iter()
            .map(|b| breadcrumb_prefix(path, title, &b.heading_path))
            .collect();

        if let Err(error) = persistence
            .sync_blocks_for_note(note_id, &blocks, &breadcrumbs)
            .await
        {
            warn!(error = %error, note_id = %note_id, "failed to sync blocks for note");
        }
    }

    /// Worker B primitive: embed up to `batch_size` pending notes.
    /// Returns the number of notes updated in this pass.
    pub async fn run_embedding_pass(&self, batch_size: usize, dimensions: usize) -> usize {
        let effective_batch = batch_size.max(1);
        let effective_dimensions = dimensions.max(1);

        let pending = {
            let guard = self.inner.read().await;
            let mut pending = guard
                .notes
                .values()
                .filter(|note| note.embedding.is_none())
                .map(|note| (note.id.clone(), note.content.clone()))
                .collect::<Vec<_>>();
            pending.sort_by(|a, b| a.0.as_str().cmp(b.0.as_str()));
            pending
        };

        let updates = pending
            .into_iter()
            .take(effective_batch)
            .map(|(note_id, content)| (note_id, embed_text(&content, effective_dimensions)))
            .collect::<Vec<_>>();

        self.set_embeddings(updates).await
    }

    pub async fn status(&self) -> StatusResponse {
        let Some(persistence) = self.persistence.as_ref() else {
            return self.in_memory_status("disabled").await;
        };
        if let Err(error) = persistence.health_check().await {
            warn!(
                error = %error,
                failure_kind = error.failure_kind().as_str(),
                "postgres dependency unavailable; reporting degraded cached status"
            );
            return self.in_memory_status("unavailable").await;
        }

        self.prepare_cached_read("status query").await;
        let pending_chunks = persistence.pending_chunk_parent_count().await.unwrap_or(0);
        let orphan_leaf_staging_count = persistence
            .orphan_leaf_staging_parent_count()
            .await
            .unwrap_or(0);
        let stale_file_aliases = persistence.stale_file_alias_count().await.unwrap_or(0);
        let recovery_queue_stats = persistence.recovery_queue_stats().await.unwrap_or_default();
        let max_failures = self.settings.read().await.max_embedding_failures;
        let pending_embeddings = persistence.pending_embedding_count().await.unwrap_or(0);
        let quarantined = persistence
            .quarantined_embedding_count(max_failures.max(1) as i32)
            .await
            .unwrap_or(0);
        let block_embedding_stats = persistence
            .block_embedding_stats(max_failures.max(1) as i32)
            .await
            .unwrap_or_default();
        let sync_state = self.sync_state.read().await.clone();
        let guard = self.inner.read().await;
        let mut status = status_from_inner(
            &guard,
            pending_chunks,
            orphan_leaf_staging_count,
            stale_file_aliases,
            &recovery_queue_stats,
            &sync_state,
        );
        let auth_config = self.authorization_config().await;
        status.context_stats = context_stats_from_inner(&guard, &auth_config);
        status.index.pending_embeddings = pending_embeddings;
        status.index.quarantined_embeddings = quarantined;
        status.index.pending_chunk_embeddings = block_embedding_stats.pending;
        status.index.quarantined_chunk_embeddings = block_embedding_stats.quarantined;
        status.embedding = embedding_status_from_inner(
            &guard,
            status.index.pending_embeddings,
            status.index.quarantined_embeddings,
            block_embedding_stats.pending,
            block_embedding_stats.quarantined,
            block_embedding_stats.last_success_at,
            block_embedding_stats.last_error_at,
            block_embedding_stats.last_error,
        );
        drop(guard);
        self.apply_projection_health_to_status(&mut status, "healthy")
            .await;
        status
    }

    async fn in_memory_status(&self, postgres_state: &'static str) -> StatusResponse {
        let guard = self.inner.read().await;
        let sync_state = RuntimeSyncState {
            last_seq: guard.last_seq.clone(),
            couchdb_current_seq: guard.couchdb_current_seq.clone(),
            last_sync_at: guard.last_sync_at,
        };
        let mut status = status_from_inner(
            &guard,
            guard.chunk_staging.pending_count(),
            orphan_leaf_staging_count_locked(&guard),
            stale_file_doc_ids_for_recovery_locked(&guard).len(),
            &RecoveryQueueStats::default(),
            &sync_state,
        );
        let auth_config = self.authorization_config().await;
        status.context_stats = context_stats_from_inner(&guard, &auth_config);
        drop(guard);
        self.apply_projection_health_to_status(&mut status, postgres_state)
            .await;
        status
    }

    async fn apply_projection_health_to_status(
        &self,
        status: &mut StatusResponse,
        postgres_state: &'static str,
    ) {
        let health = self.projection_health.read().await;
        status.dependencies.postgres = postgres_state;
        status.write_projection = WriteProjectionStatus {
            pending: health.pending.len(),
            last_success_at: health.last_success_at,
            last_failure_at: health.last_failure_at,
            last_failure_kind: health.last_failure_kind,
        };
        if postgres_state == "unavailable" || !health.pending.is_empty() {
            status.status = "degraded";
        }
    }

    pub async fn graph_cache_generation(&self) -> u64 {
        self.inner.read().await.graph_cache_generation
    }

    pub async fn note_timestamps(
        &self,
        note_id: &NoteId,
    ) -> Option<(Option<DateTime<Utc>>, DateTime<Utc>)> {
        let guard = self.inner.read().await;
        guard
            .notes
            .get(note_id)
            .map(|note| (note.created_at, note.updated_at))
    }

    pub async fn recover_stale_chunk_staging_at(
        &self,
        chunk_staging_timeout: StdDuration,
        now: DateTime<Utc>,
    ) -> ChunkStagingPurgeResult {
        let mut guard = self.inner.write().await;
        let purged_parent_ids = guard
            .chunk_staging
            .purge_older_than(chunk_staging_timeout, now);
        if purged_parent_ids.is_empty() {
            return ChunkStagingPurgeResult::default();
        }
        let purge_result = classify_purged_chunk_staging_locked(&guard, purged_parent_ids);
        guard.last_sync_at = now;
        drop(guard);
        self.sync_state.write().await.last_sync_at = now;

        if let Some(persistence) = self.persistence.as_ref()
            && let Err(error) = persistence
                .purge_chunk_staging_and_enqueue_recovery(
                    purge_result.purged_parent_ids.clone(),
                    "chunk_parent",
                    purge_result.recovery_parent_ids.clone(),
                )
                .await
        {
            warn!(error = %error, "failed to atomically persist chunk purge and recovery queue");
        }

        purge_result
    }

    pub async fn stale_file_doc_ids_for_recovery(&self) -> Vec<String> {
        self.stale_file_recovery_targets()
            .await
            .into_iter()
            .map(|target| target.file_doc_id)
            .collect()
    }

    pub async fn stale_file_recovery_targets(&self) -> Vec<StaleFileRecoveryTarget> {
        self.prepare_cached_read("stale_file_recovery_targets")
            .await;
        let guard = self.inner.read().await;
        stale_file_recovery_targets_locked(&guard)
    }

    pub async fn sync_recovery_target_is_unresolved(&self, target_id: &str) -> bool {
        self.prepare_cached_read("sync_recovery_target_state").await;
        let guard = self.inner.read().await;
        guard.chunk_staging.parent_ids().any(|id| id == target_id)
            || stale_file_recovery_targets_locked(&guard)
                .iter()
                .any(|target| target.file_doc_id == target_id || target.note_path == target_id)
    }

    pub async fn tracked_file_doc_revs(&self) -> Vec<(String, String)> {
        let guard = self.inner.read().await;
        let mut tracked = guard
            .file_doc_revs
            .iter()
            .map(|(file_doc_id, couchdb_rev)| (file_doc_id.clone(), couchdb_rev.clone()))
            .collect::<Vec<_>>();
        tracked.sort_by(|a, b| a.0.cmp(&b.0));
        tracked
    }

    pub async fn set_sync_state(&self, last_seq: &str, couchdb_current_seq: &str) -> bool {
        let mut guard = self.inner.write().await;
        let now = Utc::now();
        guard.last_seq = last_seq.to_string();
        guard.couchdb_current_seq = couchdb_current_seq.to_string();
        guard.last_sync_at = now;
        drop(guard);
        *self.sync_state.write().await = RuntimeSyncState {
            last_seq: last_seq.to_string(),
            couchdb_current_seq: couchdb_current_seq.to_string(),
            last_sync_at: now,
        };

        if let Some(persistence) = self.persistence.as_ref()
            && let Err(error) = persistence
                .upsert_sync_state(last_seq, couchdb_current_seq, now)
                .await
        {
            warn!(error = %error, "failed to persist sync_state");
            return false;
        }
        true
    }

    pub async fn enqueue_sync_recoveries(
        &self,
        recovery_kind: &str,
        target_ids: &[String],
    ) -> Result<(), crate::persistence::PersistenceError> {
        let Some(persistence) = self.persistence.as_ref() else {
            return Ok(());
        };
        persistence
            .enqueue_recovery_targets(recovery_kind, target_ids)
            .await
    }

    pub async fn reactivate_sync_recovery(
        &self,
        recovery_kind: &str,
        target_id: &str,
        source_revision: &str,
        cooldown_seconds: u64,
    ) -> Result<bool, crate::persistence::PersistenceError> {
        let Some(persistence) = self.persistence.as_ref() else {
            return Ok(false);
        };
        persistence
            .reactivate_recovery_target(
                recovery_kind,
                target_id,
                source_revision,
                Utc::now(),
                cooldown_seconds,
            )
            .await
    }

    pub async fn due_sync_recoveries(
        &self,
        limit: usize,
        now: DateTime<Utc>,
    ) -> Result<Vec<PersistedRecoveryTarget>, crate::persistence::PersistenceError> {
        let Some(persistence) = self.persistence.as_ref() else {
            return Ok(Vec::new());
        };
        persistence.due_recovery_targets(limit, now).await
    }

    pub async fn resolve_sync_recovery(
        &self,
        recovery_kind: &str,
        target_id: &str,
    ) -> Result<(), crate::persistence::PersistenceError> {
        let Some(persistence) = self.persistence.as_ref() else {
            return Ok(());
        };
        persistence
            .resolve_recovery_target(recovery_kind, target_id)
            .await
    }

    pub async fn fail_sync_recovery(
        &self,
        recovery_kind: &str,
        target_id: &str,
        next_retry_at: DateTime<Utc>,
        max_failures: usize,
        failure_kind: &str,
        child_diagnosis: Option<&crate::persistence::RecoveryChildDiagnosis>,
    ) -> Result<bool, crate::persistence::PersistenceError> {
        let Some(persistence) = self.persistence.as_ref() else {
            return Ok(false);
        };
        persistence
            .fail_recovery_target(
                recovery_kind,
                target_id,
                next_retry_at,
                max_failures,
                failure_kind,
                child_diagnosis,
            )
            .await
    }

    pub async fn clear_resolved_sync_recoveries(
        &self,
        recovery_kind: &str,
        active_target_ids: &[String],
    ) -> Result<(), crate::persistence::PersistenceError> {
        let Some(persistence) = self.persistence.as_ref() else {
            return Ok(());
        };
        persistence
            .clear_recovery_targets_not_in(recovery_kind, active_target_ids)
            .await
    }

    pub async fn log_access(
        &self,
        context: &str,
        endpoint: &str,
        query_params: &Value,
        returned_ids: &[NoteId],
        filtered_count: usize,
    ) {
        let mut audit = self.audit.write().await;
        if !audit.enabled {
            return;
        }

        let now = Utc::now();
        let retention_days = audit.retention_days;
        if audit.retention_days > 0 {
            let days = audit.retention_days.min(i64::MAX as u64) as i64;
            let cutoff = now - Duration::days(days);
            audit.access_log.retain(|entry| entry.timestamp >= cutoff);
        }

        let access_entry = AccessLogEntry {
            timestamp: now,
            context: context.to_string(),
            endpoint: endpoint.to_string(),
            query_params: query_params.clone(),
            notes_returned: returned_ids.to_vec(),
            notes_filtered_count: filtered_count,
        };
        audit.access_log.push(access_entry.clone());
        drop(audit);

        if let Some(persistence) = self.persistence.as_ref() {
            let persisted = PersistedAccessLogEntry {
                timestamp: access_entry.timestamp,
                context: access_entry.context,
                endpoint: access_entry.endpoint,
                query_params: access_entry.query_params,
                notes_returned: access_entry
                    .notes_returned
                    .into_iter()
                    .map(|id| id.as_str().to_string())
                    .collect(),
                notes_filtered_count: access_entry.notes_filtered_count,
            };

            if let Err(error) = persistence.log_access(&persisted).await {
                warn!(error = %error, "failed to persist access log entry");
            }

            if retention_days > 0
                && let Err(error) = persistence.prune_access_log(retention_days).await
            {
                warn!(error = %error, "failed to prune persisted access logs");
            }
        }
    }

    pub async fn access_log_len(&self) -> usize {
        self.audit.read().await.access_log.len()
    }

    pub async fn access_log_snapshot(&self) -> Vec<AccessLogEntry> {
        self.audit.read().await.access_log.clone()
    }

    pub async fn seed_example_data(&self) {
        let now = Utc::now();
        let notes = vec![
            NoteInput {
                id: NoteId::new("03Concepts/rust-phantom-types.md"),
                title: "Rust Phantom Types".to_string(),
                content: "# Rust Phantom Types\n\nIn the context of [[03Concepts/type-systems.md]], phantom types encode invariants.\n#rust #type-theory".to_string(),
                frontmatter: serde_json::json!({"created": now.to_rfc3339()}),
                tags: vec!["rust".to_string(), "type-theory".to_string()],
                couchdb_rev: "1-a".to_string(),
                created_at: Some(now),
                updated_at: now,
                embedding: Some(embed_text("rust phantom types", 64)),
                links: vec![LinkInput {
                    target_id: NoteId::new("03Concepts/type-systems.md"),
                    context_text: "In the context of type systems, phantom types encode invariants.".to_string(),
                    position: 20,
                }],
            },
            NoteInput {
                id: NoteId::new("03Concepts/type-systems.md"),
                title: "Type Systems".to_string(),
                content: "# Type Systems\n\nType systems connect to [[03Concepts/generics.md]].\n#rust".to_string(),
                frontmatter: serde_json::json!({}),
                tags: vec!["rust".to_string()],
                couchdb_rev: "1-b".to_string(),
                created_at: Some(now),
                updated_at: now,
                embedding: Some(embed_text("type systems", 64)),
                links: vec![LinkInput {
                    target_id: NoteId::new("03Concepts/generics.md"),
                    context_text: "Type systems connect to generics.".to_string(),
                    position: 15,
                }],
            },
            NoteInput {
                id: NoteId::new("03Concepts/generics.md"),
                title: "Generics in Rust".to_string(),
                content: "# Generics in Rust\n\n## Type Parameters".to_string(),
                frontmatter: serde_json::json!({}),
                tags: vec!["rust".to_string()],
                couchdb_rev: "1-c".to_string(),
                created_at: Some(now),
                updated_at: now,
                embedding: Some(embed_text("generics rust", 64)),
                links: vec![],
            },
            NoteInput {
                id: NoteId::new("00Journal/private.md"),
                title: "Private Journal".to_string(),
                content: "# Personal\n\nLinked to [[03Concepts/type-systems.md]]".to_string(),
                frontmatter: serde_json::json!({}),
                tags: vec!["personal".to_string()],
                couchdb_rev: "1-d".to_string(),
                created_at: Some(now),
                updated_at: now,
                embedding: Some(embed_text("journal private", 64)),
                links: vec![LinkInput {
                    target_id: NoteId::new("03Concepts/type-systems.md"),
                    context_text: "Private note references type systems".to_string(),
                    position: 10,
                }],
            },
        ];

        for note in notes {
            let path = note.id.as_str().to_string();
            let raw_content = markdown_from_seed_note(&note.frontmatter, &note.content);
            let created_at = note.created_at;
            let updated_at = note.updated_at;
            let operation_id = write_operation_id(&path, &raw_content, updated_at);
            let write = PreparedVaultWrite {
                path,
                content: raw_content,
                file_type: NewNoteFileType::Md,
                created_at,
                updated_at,
                note: Some(note),
                mark_created: false,
                expected_couchdb_rev: None,
                operation_id,
            };
            if let Err(error) = self.commit_prepared_vault_write(write, "1-seed").await {
                warn!(error = %error, "failed to persist seeded vault file");
            }
        }

        self.set_sync_state("1547", "1549").await;
    }
}

#[derive(Debug, Deserialize)]
struct LocalAiSearchEmbeddingResponse {
    data: Vec<LocalAiSearchEmbeddingData>,
}

#[derive(Debug, Deserialize)]
struct LocalAiSearchEmbeddingData {
    embedding: Vec<f32>,
}

#[derive(Debug, Clone)]
struct SearchEmbeddingSettings {
    provider: String,
    url: String,
    model: String,
    timeout_seconds: u64,
}

async fn fetch_localai_search_embedding(
    settings: &SearchEmbeddingSettings,
    query: &str,
    expected_dimensions: usize,
) -> Result<Vec<f32>, String> {
    let client = reqwest::Client::builder()
        .timeout(StdDuration::from_secs(settings.timeout_seconds.max(1)))
        .user_agent("vault-bridge/0.1")
        .build()
        .map_err(|error| format!("failed to build LocalAI client: {error}"))?;

    let response = client
        .post(&settings.url)
        .json(&serde_json::json!({
            "model": settings.model,
            "input": [query],
        }))
        .send()
        .await
        .map_err(|error| format!("LocalAI request failed: {error}"))?
        .error_for_status()
        .map_err(|error| format!("LocalAI returned non-success status: {error}"))?;

    let payload = response
        .json::<LocalAiSearchEmbeddingResponse>()
        .await
        .map_err(|error| format!("failed to parse LocalAI embedding response: {error}"))?;

    let embedding = payload
        .data
        .into_iter()
        .next()
        .map(|item| item.embedding)
        .ok_or_else(|| "LocalAI response had no embedding items".to_string())?;

    if embedding.len() != expected_dimensions {
        return Err(format!(
            "invalid embedding dimensions from LocalAI (expected {expected_dimensions}, got {})",
            embedding.len()
        ));
    }

    Ok(embedding)
}

fn markdown_from_seed_note(frontmatter: &Value, body: &str) -> String {
    let Some(frontmatter_obj) = frontmatter.as_object() else {
        return body.to_string();
    };
    if frontmatter_obj.is_empty() {
        return body.to_string();
    }

    let yaml_mapping: serde_yaml::Mapping = frontmatter_obj
        .iter()
        .filter_map(|(key, value)| {
            let yaml_value = serde_yaml::to_value(value).ok()?;
            Some((serde_yaml::Value::String(key.clone()), yaml_value))
        })
        .collect();
    let yaml = serde_yaml::to_string(&yaml_mapping).unwrap_or_default();
    let yaml = yaml.strip_prefix("---\n").unwrap_or(&yaml);
    let mut markdown = format!("---\n{}---\n\n{}", yaml, body);
    if !markdown.ends_with('\n') {
        markdown.push('\n');
    }
    markdown
}

fn prepare_note_upsert(note: NoteInput, indexed_at: DateTime<Utc>) -> PreparedNoteUpsert {
    let note_id = note.id.clone();
    let title = note.title;
    let summary = summary_with_title_fallback(&title, &note.content);
    let heading_title = first_h1_title(&note.content);
    let search_text = markdown_plain_text(&note.content);

    let stored = StoredNote {
        id: note_id.clone(),
        path: note_id.as_str().to_string(),
        title,
        heading_title,
        content: note.content.clone(),
        search_text,
        summary,
        frontmatter: note.frontmatter,
        tags: note.tags,
        couchdb_rev: note.couchdb_rev,
        created_at: note.created_at,
        updated_at: note.updated_at,
        indexed_at,
        embedding: note.embedding,
    };

    let mut unique_links: HashMap<NoteId, LinkInput> = HashMap::new();
    for link in note.links {
        let target = link.target_id.clone();
        match unique_links.get_mut(&target) {
            Some(existing) if link.position < existing.position => *existing = link,
            Some(_) => {}
            None => {
                unique_links.insert(target, link);
            }
        }
    }

    let mut ordered_links = unique_links.into_values().collect::<Vec<_>>();
    ordered_links.sort_by(|a, b| {
        a.position
            .cmp(&b.position)
            .then_with(|| a.target_id.as_str().cmp(b.target_id.as_str()))
    });
    let links = ordered_links
        .into_iter()
        .map(|link| LinkRecord {
            source_id: note_id.clone(),
            target_id: link.target_id,
            context_text: link.context_text,
            position: link.position,
        })
        .collect();

    PreparedNoteUpsert {
        note: stored,
        links,
    }
}

fn apply_prepared_note_upsert_locked(guard: &mut StoreInner, prepared: PreparedNoteUpsert) {
    let note_id = prepared.note.id.clone();
    guard.links.retain(|link| link.source_id != note_id);
    guard.links.extend(prepared.links);
    guard.notes.insert(note_id, prepared.note);
}

fn upsert_note_locked(guard: &mut StoreInner, note: NoteInput, indexed_at: DateTime<Utc>) {
    apply_prepared_note_upsert_locked(guard, prepare_note_upsert(note, indexed_at));
}

fn upsert_vault_file_locked(
    guard: &mut StoreInner,
    path: &str,
    content: &str,
    couchdb_rev: &str,
    created_at: Option<DateTime<Utc>>,
    updated_at: DateTime<Utc>,
    indexed_at: DateTime<Utc>,
) {
    guard.vault_files.insert(
        path.to_string(),
        StoredVaultFile {
            path: path.to_string(),
            content: content.to_string(),
            couchdb_rev: couchdb_rev.to_string(),
            created_at,
            updated_at,
            indexed_at,
        },
    );
}

fn delete_vault_file_locked(guard: &mut StoreInner, path: &str) -> bool {
    guard.vault_files.remove(path).is_some()
}

fn get_vault_file_from_inner(guard: &StoreInner, path: &str) -> Option<VaultFile> {
    let file = guard.vault_files.get(path)?;
    Some(VaultFile {
        id: NoteId::new(path),
        path: path.to_string(),
        file_type: file_type_from_path(path),
        content: file.content.clone(),
        created_at: file.created_at,
        updated_at: file.updated_at,
        size_bytes: file.content.len(),
    })
}

fn vault_file_readable_for_policy_from_inner(
    guard: &StoreInner,
    config: &AuthorizationConfig,
    auth: &AuthContext,
    path: &str,
    now: DateTime<Utc>,
) -> bool {
    let Some(file) = guard.vault_files.get(path) else {
        return false;
    };
    let policy_note = if is_markdown_note_path(path) {
        let Some(note) = guard.notes.get(&NoteId::new(path)) else {
            return false;
        };
        policy_note_from_stored(note)
    } else {
        PolicyNote {
            path: path.to_string(),
            title: title_from_note_id(path),
            tags: Vec::new(),
            created_at: file.created_at,
            updated_at: file.updated_at,
            owner: None,
        }
    };
    policy_allows(config, auth, "read", &policy_note, now)
}

fn file_type_from_path(path: &str) -> NewNoteFileType {
    if path.to_ascii_lowercase().ends_with(".md") {
        NewNoteFileType::Md
    } else {
        NewNoteFileType::Base
    }
}

fn note_input_from_raw_markdown(
    path: &str,
    content: &str,
    couchdb_rev: &str,
    created_at: Option<DateTime<Utc>>,
    updated_at: DateTime<Utc>,
    max_link_context_chars: usize,
) -> NoteInput {
    let parsed = parse_markdown(content, max_link_context_chars);
    let links = parsed
        .links
        .into_iter()
        .map(|link| LinkInput {
            target_id: NoteId::new(link.target),
            context_text: link.context,
            position: link.byte_offset,
        })
        .collect::<Vec<_>>();
    let mut tags = parsed.tags;
    tags.sort();
    tags.dedup_by(|a, b| a.eq_ignore_ascii_case(b));

    NoteInput {
        id: NoteId::new(path),
        title: title_from_note_id(path),
        content: parsed.body,
        frontmatter: parsed.frontmatter,
        tags,
        couchdb_rev: couchdb_rev.to_string(),
        created_at,
        updated_at,
        embedding: None,
        links,
    }
}

fn apply_markdown_raw_update(
    candidate_content: &str,
    request: &UpdateNoteRequest,
    existing_frontmatter: &Value,
    preserve_tags: &[String],
    existing_tags: &[String],
    now: DateTime<Utc>,
) -> Result<String, WriteError> {
    let (mut frontmatter, body) = parse_frontmatter(candidate_content);
    if !frontmatter.is_object() {
        return Err(WriteError::InvalidUpdate {
            reason: "markdown frontmatter must be a YAML mapping".to_string(),
        });
    }

    if let Some(metadata) = request.metadata.as_ref() {
        let Some(metadata) = metadata.as_object() else {
            return Err(WriteError::InvalidUpdate {
                reason: "metadata must be an object".to_string(),
            });
        };
        let map = frontmatter
            .as_object_mut()
            .expect("frontmatter object checked above");
        for (key, value) in metadata {
            if matches!(key.as_str(), "created" | "created_by" | "tags" | "updated") {
                continue;
            }
            map.insert(key.clone(), value.clone());
        }
    }

    let mut tags = request
        .tags
        .clone()
        .unwrap_or_else(|| extract_frontmatter_tags(&frontmatter));
    for tag in preserve_tags {
        if existing_tags.iter().any(|candidate| {
            crate::authorization::normalize_tag(candidate)
                .eq_ignore_ascii_case(&crate::authorization::normalize_tag(tag))
        }) {
            add_unique_tag(&mut tags, tag);
        }
    }
    tags.sort();
    tags.dedup_by(|a, b| a.eq_ignore_ascii_case(b));

    {
        let map = frontmatter
            .as_object_mut()
            .expect("frontmatter object checked above");
        map.insert(
            "tags".to_string(),
            Value::Array(tags.into_iter().map(Value::String).collect()),
        );
        map.insert("updated".to_string(), Value::String(now.to_rfc3339()));
        if let Some(created) = existing_frontmatter.get("created") {
            map.insert("created".to_string(), created.clone());
        } else {
            map.remove("created");
        }
        map.remove("created_by");
        if let Some(legacy) = map.get_mut("vault_bridge").and_then(Value::as_object_mut) {
            legacy.remove("created_by");
        }
    }
    if let Some(owner) = owner_from_frontmatter(existing_frontmatter) {
        crate::authorization::set_owner_metadata(&mut frontmatter, owner);
    }

    markdown_with_updated_frontmatter(&frontmatter, &body)
}

fn markdown_with_updated_frontmatter(
    frontmatter: &Value,
    body: &str,
) -> Result<String, WriteError> {
    let Some(frontmatter) = frontmatter.as_object() else {
        return Err(WriteError::InvalidUpdate {
            reason: "markdown frontmatter must be a YAML mapping".to_string(),
        });
    };
    let yaml_mapping: serde_yaml::Mapping = frontmatter
        .iter()
        .filter_map(|(key, value)| {
            serde_yaml::to_value(value)
                .ok()
                .map(|value| (serde_yaml::Value::String(key.clone()), value))
        })
        .collect();
    let yaml = serde_yaml::to_string(&yaml_mapping).map_err(|error| WriteError::InvalidUpdate {
        reason: format!("failed to serialize markdown frontmatter: {error}"),
    })?;
    let yaml = yaml.strip_prefix("---\n").unwrap_or(&yaml);
    let mut content = format!("---\n{}---\n\n{}", yaml, body.trim());
    if !content.ends_with('\n') {
        content.push('\n');
    }
    Ok(content)
}

fn delete_note_locked(guard: &mut StoreInner, note_id: &NoteId) -> bool {
    let existed = guard.notes.remove(note_id).is_some();
    guard
        .links
        .retain(|link| &link.source_id != note_id && &link.target_id != note_id);
    existed
}

fn persisted_note_from_prepared(prepared: &PreparedNoteUpsert) -> PersistedNoteRecord {
    PersistedNoteRecord {
        id: prepared.note.id.as_str().to_string(),
        path: prepared.note.path.clone(),
        title: prepared.note.title.clone(),
        content: prepared.note.content.clone(),
        search_text: prepared.note.search_text.clone(),
        summary: prepared.note.summary.clone(),
        frontmatter: prepared.note.frontmatter.clone(),
        tags: prepared.note.tags.clone(),
        couchdb_rev: prepared.note.couchdb_rev.clone(),
        created_at: prepared.note.created_at,
        updated_at: prepared.note.updated_at,
        indexed_at: prepared.note.indexed_at,
        embedding: prepared.note.embedding.clone(),
        links: prepared
            .links
            .iter()
            .map(|link| PersistedLinkRecord {
                target_id: link.target_id.as_str().to_string(),
                context_text: link.context_text.clone(),
                position: link.position,
            })
            .collect(),
    }
}

fn note_for_persistence_locked(
    guard: &StoreInner,
    note_id: &NoteId,
) -> Option<PersistedNoteRecord> {
    let note = guard.notes.get(note_id)?;
    let links = outgoing_links(guard, note_id)
        .into_iter()
        .map(|link| PersistedLinkRecord {
            target_id: link.target_id.as_str().to_string(),
            context_text: link.context_text,
            position: link.position,
        })
        .collect::<Vec<_>>();

    Some(PersistedNoteRecord {
        id: note.id.as_str().to_string(),
        path: note.path.clone(),
        title: note.title.clone(),
        content: note.content.clone(),
        search_text: note.search_text.clone(),
        summary: note.summary.clone(),
        frontmatter: note.frontmatter.clone(),
        tags: note.tags.clone(),
        couchdb_rev: note.couchdb_rev.clone(),
        created_at: note.created_at,
        updated_at: note.updated_at,
        indexed_at: note.indexed_at,
        embedding: note.embedding.clone(),
        links,
    })
}

fn persisted_staged_chunk_from_decoded(chunk: &DecodedChunk) -> PersistedStagedChunk {
    PersistedStagedChunk {
        parent_id: chunk.parent_id.clone(),
        chunk_index: chunk.chunk_index,
        chunk_count: chunk.chunk_count.max(1),
        content: chunk.content.clone(),
        couchdb_rev: chunk.couchdb_rev.clone(),
        received_at: chunk.received_at,
    }
}

fn sync_state_for_persistence_locked(guard: &StoreInner) -> PersistedSyncState {
    PersistedSyncState {
        last_seq: guard.last_seq.clone(),
        couchdb_current_seq: guard.couchdb_current_seq.clone(),
        updated_at: guard.last_sync_at,
    }
}

fn rebuild_graph_cache_locked(guard: &mut StoreInner) {
    guard.graph_cache_generation = guard.graph_cache_generation.saturating_add(1);
}

fn build_unscoped_note(guard: &StoreInner, note_id: &NoteId) -> Option<UnscopedNote> {
    let note = guard.notes.get(note_id)?;
    let links = outgoing_links(guard, note_id)
        .into_iter()
        .map(|l| l.target_id)
        .collect::<Vec<_>>();
    let backlinks = backlinks_for_note(guard, note_id)
        .into_iter()
        .map(|l| l.source_id)
        .collect::<Vec<_>>();

    Some(UnscopedNote {
        id: note.id.clone(),
        path: note.path.clone(),
        title: title_from_note_id(note.path.as_str()),
        heading_title: note.heading_title.clone(),
        content: note.content.clone(),
        summary: note.summary.clone(),
        frontmatter: note.frontmatter.clone(),
        links,
        backlinks,
        tags: note.tags.clone(),
        updated_at: note.updated_at,
    })
}

#[cfg(test)]
fn store_inner_from_persisted_notes(
    persisted_notes: Vec<PersistedNoteRecord>,
    settings: &StoreSettings,
    sync_state: &RuntimeSyncState,
) -> StoreInner {
    let mut notes = HashMap::new();
    let mut links = Vec::new();

    for note in persisted_notes {
        let note_id = NoteId::new(note.id.clone());
        for link in note.links {
            links.push(LinkRecord {
                source_id: note_id.clone(),
                target_id: NoteId::new(link.target_id),
                context_text: link.context_text,
                position: link.position,
            });
        }

        notes.insert(
            note_id.clone(),
            StoredNote {
                id: note_id,
                path: note.path,
                title: note.title,
                heading_title: first_h1_title(&note.content),
                content: note.content,
                search_text: note.search_text,
                summary: note.summary,
                frontmatter: note.frontmatter,
                tags: note.tags,
                couchdb_rev: note.couchdb_rev,
                created_at: note.created_at,
                updated_at: note.updated_at,
                indexed_at: note.indexed_at,
                embedding: note.embedding,
            },
        );
    }

    StoreInner {
        notes,
        vault_files: HashMap::new(),
        links,
        created_file_paths: HashSet::new(),
        file_doc_paths: HashMap::new(),
        file_doc_revs: HashMap::new(),
        file_children: HashMap::new(),
        child_doc_paths: HashMap::new(),
        child_chunk_hints: HashMap::new(),
        note_timestamp_hints: HashMap::new(),
        access_log: Vec::new(),
        chunk_staging: ChunkStagingBuffer::default(),
        graph_cache_generation: 0,
        last_seq: sync_state.last_seq.clone(),
        couchdb_current_seq: sync_state.couchdb_current_seq.clone(),
        last_sync_at: sync_state.last_sync_at,
        max_link_context_chars: settings.max_link_context_chars,
        hub_note_threshold: settings.hub_note_threshold,
        hub_note_fanout: settings.hub_note_fanout,
        hub_note_folders: settings.hub_note_folders.clone(),
        context_default_max_tokens: settings.context_default_max_tokens,
        context_max_max_tokens: settings.context_max_max_tokens,
        context_default_max_depth: settings.context_default_max_depth,
        embedding_provider: settings.embedding_provider.clone(),
        embedding_url: settings.embedding_url.clone(),
        embedding_model: settings.embedding_model.clone(),
        embedding_dimensions: settings.embedding_dimensions,
        embedding_timeout_seconds: settings.embedding_timeout_seconds,
        new_note_path_settings: settings.new_note_path_settings.clone(),
        audit_enabled: false,
        audit_retention_days: 0,
        block_min_chars: settings.block_min_chars,
        block_chunk_bytes: settings.block_chunk_bytes,
        block_chunk_overlap_sentences: settings.block_chunk_overlap_sentences,
        block_embedding_enabled: settings.block_embedding_enabled,
    }
}

fn get_note_from_inner(
    guard: &StoreInner,
    note_id: &NoteId,
    config: Option<&AuthorizationConfig>,
    auth: Option<&AuthContext>,
    now: DateTime<Utc>,
) -> Option<Note> {
    let mut note = build_unscoped_note(guard, note_id)?.into_note();
    if let (Some(config), Some(auth)) = (config, auth) {
        note.links
            .retain(|id| note_readable_for_policy_from_inner(guard, config, auth, id, now));
        note.backlinks
            .retain(|id| note_readable_for_policy_from_inner(guard, config, auth, id, now));
    }
    Some(note)
}

fn build_recent_note_summary(guard: &StoreInner, note: &StoredNote) -> UnscopedRecentNoteSummary {
    UnscopedRecentNoteSummary {
        id: note.id.clone(),
        title: title_from_note_id(note.id.as_str()),
        heading_title: note.heading_title.clone(),
        summary: note.summary.clone(),
        tags: note.tags.clone(),
        updated_at: note.updated_at,
        link_count: outgoing_links(guard, &note.id).len(),
        backlink_count: backlinks_for_note(guard, &note.id).len(),
        search_score: None,
        search_match_type: None,
        search_snippet: None,
        matched_chunk_id: None,
        matched_heading_path: None,
        matched_snippet: None,
    }
}

fn note_matches_time_filter(note: &StoredNote, filter: &NoteTimeFilter) -> bool {
    if let Some(created_after) = filter.created_after {
        let Some(created_at) = note.created_at else {
            return false;
        };
        if created_at < created_after {
            return false;
        }
    }

    if let Some(created_before) = filter.created_before {
        let Some(created_at) = note.created_at else {
            return false;
        };
        if created_at > created_before {
            return false;
        }
    }

    if let Some(updated_after) = filter.updated_after
        && note.updated_at < updated_after
    {
        return false;
    }

    if let Some(updated_before) = filter.updated_before
        && note.updated_at > updated_before
    {
        return false;
    }

    true
}

fn note_matches_query_filters(note: &StoredNote, request: &QueryNotesRequest) -> bool {
    if !note_matches_time_filter(note, &request.time_filter) {
        return false;
    }

    if !request
        .tags_all
        .iter()
        .all(|tag| note.tags.iter().any(|note_tag| note_tag == tag))
    {
        return false;
    }

    if !request.tags_any.is_empty()
        && !request
            .tags_any
            .iter()
            .any(|tag| note.tags.iter().any(|note_tag| note_tag == tag))
    {
        return false;
    }

    if request
        .tags_none
        .iter()
        .any(|tag| note.tags.iter().any(|note_tag| note_tag == tag))
    {
        return false;
    }

    if !request
        .has_frontmatter
        .iter()
        .all(|key| note.frontmatter.get(key).is_some())
    {
        return false;
    }

    if request
        .missing_frontmatter
        .iter()
        .any(|key| note.frontmatter.get(key).is_some())
    {
        return false;
    }

    if let Some(path_prefix) = request.path_prefix.as_deref()
        && !note.path.starts_with(path_prefix)
    {
        return false;
    }

    if let Some(title_exact) = request.title_exact.as_deref()
        && !note_title_matches_exact(note, title_exact)
    {
        return false;
    }

    true
}

fn effective_query_sort_field(
    sort_by: Option<NoteSortField>,
    has_text_query: bool,
) -> NoteSortField {
    match sort_by {
        Some(NoteSortField::Relevance) if !has_text_query => NoteSortField::UpdatedAt,
        Some(value) => value,
        None if has_text_query => NoteSortField::Relevance,
        None => NoteSortField::UpdatedAt,
    }
}

fn effective_sort_order(sort_by: NoteSortField, requested: Option<SortOrder>) -> SortOrder {
    requested.unwrap_or(match sort_by {
        NoteSortField::Title => SortOrder::Asc,
        _ => SortOrder::Desc,
    })
}

fn compare_optional_datetimes(
    left: Option<DateTime<Utc>>,
    right: Option<DateTime<Utc>>,
    order: SortOrder,
) -> std::cmp::Ordering {
    match (left, right) {
        (Some(left), Some(right)) => match order {
            SortOrder::Asc => left.cmp(&right),
            SortOrder::Desc => right.cmp(&left),
        },
        (Some(_), None) => std::cmp::Ordering::Less,
        (None, Some(_)) => std::cmp::Ordering::Greater,
        (None, None) => std::cmp::Ordering::Equal,
    }
}

fn compare_note_ids_for_query(
    guard: &StoreInner,
    left: &NoteId,
    right: &NoteId,
    sort_by: NoteSortField,
    order: SortOrder,
) -> std::cmp::Ordering {
    let left_note = guard.notes.get(left).expect("left note");
    let right_note = guard.notes.get(right).expect("right note");

    let cmp = match sort_by {
        NoteSortField::UpdatedAt => match order {
            SortOrder::Asc => left_note.updated_at.cmp(&right_note.updated_at),
            SortOrder::Desc => right_note.updated_at.cmp(&left_note.updated_at),
        },
        NoteSortField::CreatedAt => {
            compare_optional_datetimes(left_note.created_at, right_note.created_at, order)
        }
        NoteSortField::Title => match order {
            SortOrder::Asc => left_note.title.cmp(&right_note.title),
            SortOrder::Desc => right_note.title.cmp(&left_note.title),
        },
        NoteSortField::Relevance => std::cmp::Ordering::Equal,
    };

    cmp.then_with(|| left.as_str().cmp(right.as_str()))
}

fn neighbors_from_inner(
    guard: &StoreInner,
    config: &AuthorizationConfig,
    auth: &AuthContext,
    center: &NoteId,
    depth: usize,
    direction: NeighborDirection,
) -> Option<NeighborsResponse> {
    let now = Utc::now();
    let readable_ids = readable_note_ids_from_inner(guard, config, auth, now);
    let graph = filtered_graph_from_ids(guard, readable_ids);
    if !graph.is_accessible(center) {
        return None;
    }

    let max_depth = depth.clamp(1, MAX_GRAPH_TRAVERSAL_DEPTH);
    let mut queue = VecDeque::from([(center.clone(), 0usize)]);
    let mut visited: HashSet<NoteId> = HashSet::from([center.clone()]);
    let mut levels: HashMap<NoteId, usize> = HashMap::new();
    let mut parents: HashMap<NoteId, (NoteId, NeighborDirection)> = HashMap::new();

    while let Some((node, current_depth)) = queue.pop_front() {
        if current_depth >= max_depth {
            continue;
        }

        for (next, traversal_direction) in filtered_traversal_neighbors(&graph, &node, direction) {
            if visited.insert(next.clone()) {
                let next_depth = current_depth + 1;
                levels.insert(next.clone(), next_depth);
                parents.insert(next.clone(), (node.clone(), traversal_direction));
                queue.push_back((next, next_depth));
            }
        }
    }

    let mut node_specs = levels
        .iter()
        .filter_map(|(id, node_depth)| {
            let (parent, node_direction) = parents.get(id)?;
            let note = guard.notes.get(id);
            let title = title_from_note_id(id.as_str());
            let is_hub = note.is_some_and(|n| is_hub_note(guard, n));
            let context = match node_direction {
                NeighborDirection::Outgoing | NeighborDirection::Both => {
                    link_context_between(guard, parent, id).unwrap_or_default()
                }
                NeighborDirection::Incoming => {
                    link_context_between(guard, id, parent).unwrap_or_default()
                }
            };
            Some((
                id.clone(),
                *node_depth,
                title,
                context,
                is_hub,
                *node_direction,
            ))
        })
        .collect::<Vec<_>>();
    node_specs.sort_by(|a, b| a.1.cmp(&b.1).then_with(|| a.0.as_str().cmp(b.0.as_str())));

    let nodes = node_specs
        .into_iter()
        .map(|(id, node_depth, title, context, is_hub, node_direction)| {
            UnscopedNeighborNode {
                id,
                title,
                depth: node_depth,
                link_context: context,
                is_hub,
                direction: node_direction,
            }
            .into_node()
        })
        .collect::<Vec<_>>();

    let mut edges = guard
        .links
        .iter()
        .filter(|link| visited.contains(&link.source_id) && visited.contains(&link.target_id))
        .map(|link| NeighborEdge {
            from: link.source_id.clone(),
            to: link.target_id.clone(),
        })
        .collect::<Vec<_>>();
    edges.sort_by(|a, b| {
        a.from
            .as_str()
            .cmp(b.from.as_str())
            .then_with(|| a.to.as_str().cmp(b.to.as_str()))
    });
    edges.dedup_by(|a, b| a.from == b.from && a.to == b.to);

    Some(NeighborsResponse::new(
        center.clone(),
        direction,
        nodes,
        edges,
    ))
}

fn backlinks_from_inner(
    guard: &StoreInner,
    config: &AuthorizationConfig,
    auth: &AuthContext,
    target: &NoteId,
) -> Option<BacklinksResponse> {
    let now = Utc::now();
    let readable_ids = readable_note_ids_from_inner(guard, config, auth, now);
    let graph = filtered_graph_from_ids(guard, readable_ids);
    if !graph.is_accessible(target) {
        return None;
    }

    let backlinks = graph
        .get_backlinks(target)
        .into_iter()
        .map(|source_id| {
            UnscopedBacklinkEntry {
                title: title_from_note_id(source_id.as_str()),
                context: link_context_between(guard, &source_id, target).unwrap_or_default(),
                id: source_id,
            }
            .into_entry()
        })
        .collect::<Vec<_>>();

    Some(BacklinksResponse::new(target.clone(), backlinks))
}

fn shortest_path_from_inner(
    guard: &StoreInner,
    config: &AuthorizationConfig,
    auth: &AuthContext,
    from: &NoteId,
    to: &NoteId,
) -> PathResponse {
    let now = Utc::now();
    let readable_ids = readable_note_ids_from_inner(guard, config, auth, now);
    let graph = filtered_graph_from_ids(guard, readable_ids);
    let path = graph.shortest_path(from, to);
    let length = path.as_ref().map(|p| p.len().saturating_sub(1));

    PathResponse {
        from: from.clone(),
        to: to.clone(),
        path,
        length,
    }
}

fn readable_note_ids_from_inner(
    guard: &StoreInner,
    config: &AuthorizationConfig,
    auth: &AuthContext,
    now: DateTime<Utc>,
) -> HashSet<NoteId> {
    guard
        .notes
        .values()
        .filter(|note| policy_allows(config, auth, "read", &policy_note_from_stored(note), now))
        .map(|note| note.id.clone())
        .collect()
}

fn filtered_graph_from_ids(guard: &StoreInner, readable_ids: HashSet<NoteId>) -> FilteredGraph {
    let all_links = guard
        .links
        .iter()
        .map(|link| (link.source_id.clone(), link.target_id.clone()))
        .collect::<Vec<_>>();
    FilteredGraph::from_accessible_notes(readable_ids, &all_links)
}

fn note_readable_for_policy_from_inner(
    guard: &StoreInner,
    config: &AuthorizationConfig,
    auth: &AuthContext,
    note_id: &NoteId,
    now: DateTime<Utc>,
) -> bool {
    let Some(note) = guard.notes.get(note_id) else {
        return false;
    };
    policy_allows(config, auth, "read", &policy_note_from_stored(note), now)
}

fn policy_note_from_create_request(
    request: &NewNoteRequest,
    path: &str,
    now: DateTime<Utc>,
) -> Result<PolicyNote, WriteError> {
    let (tags, owner) = create_request_tags_and_owner(request)?;
    Ok(PolicyNote {
        path: path.to_string(),
        title: request.title.trim().to_string(),
        tags,
        created_at: Some(now),
        updated_at: now,
        owner,
    })
}

fn create_request_tags_and_owner(
    request: &NewNoteRequest,
) -> Result<(Vec<String>, Option<String>), WriteError> {
    if !request.file_type.is_markdown() {
        return Ok((Vec::new(), None));
    }

    let (frontmatter, body) = parse_frontmatter(&request.content);
    if !frontmatter.is_object() {
        return Err(WriteError::InvalidCreate {
            reason: "markdown frontmatter must be a YAML mapping".to_string(),
        });
    }
    let mut tags = extract_tags(&body);
    tags.extend(extract_frontmatter_tags(&frontmatter));
    tags.sort();
    tags.dedup_by(|a, b| a.eq_ignore_ascii_case(b));
    let owner = owner_from_frontmatter(&frontmatter).map(str::to_string);

    Ok((tags, owner))
}

fn validate_create_template_reference(request: &NewNoteRequest) -> Result<(), WriteError> {
    let Some(template_id) = request.template_id.as_deref() else {
        return Ok(());
    };
    if !is_safe_template_id(template_id) {
        return Err(WriteError::InvalidCreate {
            reason: format!("template_id '{template_id}' is not a safe vault-relative path"),
        });
    }
    let expected_suffix = format!(".{}", request.file_type.extension());
    if !template_id.to_ascii_lowercase().ends_with(&expected_suffix) {
        return Err(WriteError::InvalidCreate {
            reason: format!("template_id must end with '{expected_suffix}' for this file_type"),
        });
    }
    Ok(())
}

fn is_safe_template_id(template_id: &str) -> bool {
    let template_id = template_id.trim();
    !template_id.is_empty()
        && !template_id.starts_with('/')
        && template_id
            .split('/')
            .all(|segment| !segment.is_empty() && segment != "." && segment != "..")
}

fn validate_markdown_content_against_template(
    content: &str,
    template_frontmatter: &Value,
) -> Result<(), WriteError> {
    let Some(template_obj) = template_frontmatter.as_object() else {
        return Ok(());
    };
    let (frontmatter, _) = parse_frontmatter(content);
    let Some(content_obj) = frontmatter.as_object() else {
        return Err(WriteError::InvalidCreate {
            reason: "markdown content must have YAML mapping frontmatter when the template does"
                .to_string(),
        });
    };

    for (key, template_value) in template_obj {
        if matches!(key.as_str(), "created" | "created_by" | "tags") {
            continue;
        }
        let Some(content_value) = content_obj.get(key) else {
            return Err(WriteError::InvalidCreate {
                reason: format!("content is missing template frontmatter key '{key}'"),
            });
        };
        // Null is an unset default; it still requires the key without constraining its type.
        if !template_value.is_null()
            && json_type_name(template_value) != json_type_name(content_value)
        {
            return Err(WriteError::InvalidCreate {
                reason: format!(
                    "content frontmatter key '{key}' has type {}, expected {} from template",
                    json_type_name(content_value),
                    json_type_name(template_value)
                ),
            });
        }
    }

    Ok(())
}

fn validate_base_file_content(content: &str) -> Result<(), WriteError> {
    let value = serde_yaml::from_str::<serde_yaml::Value>(content).map_err(|error| {
        WriteError::InvalidCreate {
            reason: format!("base content is not valid YAML: {error}"),
        }
    })?;
    if !matches!(value, serde_yaml::Value::Mapping(_)) {
        return Err(WriteError::InvalidCreate {
            reason: "base content must be a YAML mapping".to_string(),
        });
    }
    Ok(())
}

fn json_type_name(value: &Value) -> &'static str {
    match value {
        Value::Null => "null",
        Value::Bool(_) => "boolean",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

#[derive(Debug, Default)]
struct PolicyDecision {
    add_tags: Vec<String>,
    preserve_tags: Vec<String>,
    set_owner: bool,
}

fn policy_allows(
    config: &AuthorizationConfig,
    auth: &AuthContext,
    operation: &'static str,
    note: &PolicyNote,
    now: DateTime<Utc>,
) -> bool {
    policy_decision_for(config, auth, operation, note, now).is_some()
}

fn policy_decision_for(
    config: &AuthorizationConfig,
    auth: &AuthContext,
    operation: &'static str,
    note: &PolicyNote,
    now: DateTime<Utc>,
) -> Option<PolicyDecision> {
    let policy = config.get(auth.context.as_str())?;
    let rules = match operation {
        "read" => &policy.read,
        "create" => &policy.create,
        "edit" => &policy.edit,
        _ => return None,
    };

    for rule in rules {
        let Some(matcher) = rule.matcher() else {
            continue;
        };
        if rule.is_deny() && matcher.matches(note, &auth.principal, now) {
            return None;
        }
    }

    let mut decision = PolicyDecision::default();
    let mut allowed = false;
    for rule in rules {
        let Some(matcher) = rule.matcher() else {
            continue;
        };
        if rule.is_allow() && matcher.matches(note, &auth.principal, now) {
            allowed = true;
            merge_rule_decision(&mut decision, rule);
        }
    }

    allowed.then_some(decision)
}

fn write_operation_id(path: &str, content: &str, timestamp: DateTime<Utc>) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"vault-write:");
    hasher.update(path.as_bytes());
    hasher.update(b":");
    hasher.update(
        timestamp
            .timestamp_nanos_opt()
            .unwrap_or_default()
            .to_string(),
    );
    hasher.update(b":");
    hasher.update(content.as_bytes());
    format!("write-{}", &hex::encode(hasher.finalize())[..16])
}

fn write_target_fingerprint(path: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"vault-write-target:");
    hasher.update(path.as_bytes());
    hex::encode(hasher.finalize())[..16].to_string()
}

fn merge_rule_decision(decision: &mut PolicyDecision, rule: &AccessRule) {
    for tag in &rule.add_tags {
        add_unique_tag(&mut decision.add_tags, tag);
    }
    for tag in &rule.preserve_tags {
        add_unique_tag(&mut decision.preserve_tags, tag);
    }
    decision.set_owner |= rule.set_owner;
}

fn policy_note_from_stored(note: &StoredNote) -> PolicyNote {
    PolicyNote {
        path: note.path.clone(),
        title: note.title.clone(),
        tags: note.tags.clone(),
        created_at: note.created_at,
        updated_at: note.updated_at,
        owner: owner_from_frontmatter(&note.frontmatter).map(str::to_string),
    }
}

fn status_from_inner(
    guard: &StoreInner,
    pending_chunks: usize,
    orphan_leaf_staging_count: usize,
    stale_file_aliases: usize,
    recovery_queue_stats: &RecoveryQueueStats,
    sync_state: &RuntimeSyncState,
) -> StatusResponse {
    let total_notes = guard.notes.len();
    let total_links = guard.links.len();
    let total_tags = guard
        .notes
        .values()
        .flat_map(|note| note.tags.iter().cloned())
        .collect::<HashSet<_>>()
        .len();
    let pending_embeddings = guard
        .notes
        .values()
        .filter(|note| note.embedding.is_none())
        .count();
    let missing_vault_files_for_notes = guard
        .notes
        .keys()
        .filter(|note_id| !guard.vault_files.contains_key(note_id.as_str()))
        .count();
    let unindexed_markdown_vault_files = guard
        .vault_files
        .keys()
        .filter(|path| {
            is_markdown_note_path(path) && !guard.notes.contains_key(&NoteId::new(path.as_str()))
        })
        .count();

    let last_seq_n = parse_seq_value(&sync_state.last_seq);
    let current_seq_n = parse_seq_value(&sync_state.couchdb_current_seq);

    StatusResponse {
        status: "ok",
        dependencies: DependencyStatus {
            postgres: "disabled",
            couchdb: "disabled",
        },
        write_projection: WriteProjectionStatus {
            pending: 0,
            last_success_at: None,
            last_failure_at: None,
            last_failure_kind: None,
        },
        index: IndexStats {
            total_notes,
            total_links,
            total_tags,
            pending_embeddings,
            quarantined_embeddings: 0,
            pending_chunk_embeddings: 0,
            quarantined_chunk_embeddings: 0,
            pending_chunks,
            orphan_leaf_staging_count,
            stale_file_aliases,
            pending_sync_recoveries: recovery_queue_stats.pending,
            quarantined_sync_recoveries: recovery_queue_stats.quarantined,
            stale_aliases_blocked_by_unavailable_children: recovery_queue_stats
                .aliases_blocked_by_unavailable_children,
            missing_livesync_children: recovery_queue_stats.missing_children,
            tombstoned_livesync_children: recovery_queue_stats.tombstoned_children,
            missing_vault_files_for_notes,
            unindexed_markdown_vault_files,
        },
        embedding: embedding_status_from_inner(
            guard,
            pending_embeddings,
            0,
            0,
            0,
            guard
                .notes
                .values()
                .filter(|note| note.embedding.is_some())
                .map(|note| note.indexed_at)
                .max(),
            None,
            None,
        ),
        sync: SyncStats {
            last_seq: sync_state.last_seq.clone(),
            couchdb_current_seq: sync_state.couchdb_current_seq.clone(),
            behind_by: (current_seq_n - last_seq_n).max(0),
            current_seq_source: "cached",
            current_seq_observed_at: sync_state.last_sync_at,
            last_sync_at: sync_state.last_sync_at,
        },
        context_stats: HashMap::new(),
        config_reload: ConfigReloadStatus::default(),
    }
}

fn context_stats_from_inner(
    guard: &StoreInner,
    config: &AuthorizationConfig,
) -> HashMap<String, ContextStats> {
    let now = Utc::now();
    let total_notes = guard.notes.len();
    config
        .keys()
        .map(|context| {
            let auth = AuthContext::new(
                ContextName::new(context.clone()),
                format!("status:{context}"),
            );
            let accessible_notes = guard
                .notes
                .values()
                .filter(|note| {
                    policy_allows(config, &auth, "read", &policy_note_from_stored(note), now)
                })
                .count();
            (
                context.clone(),
                ContextStats {
                    accessible_notes,
                    filtered_notes: total_notes.saturating_sub(accessible_notes),
                },
            )
        })
        .collect()
}

#[allow(clippy::too_many_arguments)]
fn embedding_status_from_inner(
    guard: &StoreInner,
    pending_notes: usize,
    quarantined_notes: usize,
    pending_chunks: usize,
    quarantined_chunks: usize,
    last_success_at: Option<DateTime<Utc>>,
    last_error_at: Option<DateTime<Utc>>,
    last_error: Option<String>,
) -> EmbeddingStatus {
    let backend_state = embedding_backend_state(
        &guard.embedding_provider,
        pending_notes + pending_chunks,
        quarantined_notes + quarantined_chunks,
        last_success_at,
        last_error_at,
    );

    EmbeddingStatus {
        mode: guard.embedding_provider.clone(),
        model: guard.embedding_model.clone(),
        dimensions: guard.embedding_dimensions.max(1),
        endpoint: embedding_endpoint_for_diagnostics(&guard.embedding_url),
        pending_notes,
        quarantined_notes,
        pending_chunks,
        quarantined_chunks,
        last_success_at,
        last_error_at,
        last_error,
        backend_state,
    }
}

fn embedding_backend_state(
    mode: &str,
    pending: usize,
    quarantined: usize,
    last_success_at: Option<DateTime<Utc>>,
    last_error_at: Option<DateTime<Utc>>,
) -> &'static str {
    if mode.eq_ignore_ascii_case("disabled") {
        return "disabled";
    }
    if quarantined > 0 {
        return "degraded";
    }
    if let Some(last_error) = last_error_at
        && last_success_at.is_none_or(|last_success| last_error > last_success)
        && pending > 0
    {
        return "degraded";
    }
    if last_success_at.is_some() {
        return "available";
    }
    "unknown"
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

fn outgoing_links(guard: &StoreInner, source: &NoteId) -> Vec<LinkRecord> {
    let mut links = guard
        .links
        .iter()
        .filter(|link| &link.source_id == source)
        .cloned()
        .collect::<Vec<_>>();
    links.sort_by(|a, b| {
        a.position
            .cmp(&b.position)
            .then_with(|| a.target_id.as_str().cmp(b.target_id.as_str()))
    });
    links
}

fn filtered_traversal_neighbors(
    graph: &FilteredGraph,
    source: &NoteId,
    direction: NeighborDirection,
) -> Vec<(NoteId, NeighborDirection)> {
    let mut neighbors = Vec::new();
    let mut seen = HashSet::new();

    if matches!(
        direction,
        NeighborDirection::Outgoing | NeighborDirection::Both
    ) {
        for target in graph.get_neighbors(source) {
            if seen.insert(target.clone()) {
                neighbors.push((target, NeighborDirection::Outgoing));
            }
        }
    }

    if matches!(
        direction,
        NeighborDirection::Incoming | NeighborDirection::Both
    ) {
        for source_id in graph.get_backlinks(source) {
            if seen.insert(source_id.clone()) {
                neighbors.push((source_id, NeighborDirection::Incoming));
            }
        }
    }

    neighbors
}

fn backlinks_for_note(guard: &StoreInner, target: &NoteId) -> Vec<LinkRecord> {
    let mut links = guard
        .links
        .iter()
        .filter(|link| &link.target_id == target)
        .cloned()
        .collect::<Vec<_>>();
    links.sort_by(|a, b| {
        a.position
            .cmp(&b.position)
            .then_with(|| a.source_id.as_str().cmp(b.source_id.as_str()))
    });
    links
}

fn link_context_between(guard: &StoreInner, from: &NoteId, to: &NoteId) -> Option<String> {
    guard
        .links
        .iter()
        .find(|link| &link.source_id == from && &link.target_id == to)
        .map(|link| link.context_text.clone())
}

fn is_hub_note(guard: &StoreInner, note: &StoredNote) -> bool {
    if outgoing_links(guard, &note.id).len() > guard.hub_note_threshold {
        return true;
    }

    if guard
        .hub_note_folders
        .iter()
        .any(|prefix| note.path.starts_with(prefix))
    {
        return true;
    }

    frontmatter_marks_hub(&note.frontmatter)
}

fn is_hub_note_with_settings(
    guard: &StoreInner,
    settings: &StoreSettings,
    note: &StoredNote,
) -> bool {
    if outgoing_links(guard, &note.id).len() > settings.hub_note_threshold {
        return true;
    }

    if settings
        .hub_note_folders
        .iter()
        .any(|prefix| note.path.starts_with(prefix))
    {
        return true;
    }

    frontmatter_marks_hub(&note.frontmatter)
}

fn frontmatter_marks_hub(frontmatter: &Value) -> bool {
    let obj = match frontmatter.as_object() {
        Some(value) => value,
        None => return false,
    };

    if let Some(label) = obj.get("type").and_then(Value::as_str)
        && is_moc_label(label)
    {
        return true;
    }

    if obj.get("moc").and_then(Value::as_bool).unwrap_or(false)
        || obj.get("is_hub").and_then(Value::as_bool).unwrap_or(false)
    {
        return true;
    }

    obj.get("tags")
        .and_then(Value::as_array)
        .is_some_and(|tags| tags.iter().filter_map(Value::as_str).any(is_moc_label))
}

fn is_moc_label(label: &str) -> bool {
    let normalized = label.trim().to_ascii_lowercase().replace('-', "_");
    matches!(
        normalized.as_str(),
        "moc" | "map_of_content" | "map_of_contents"
    )
}

fn limited_hub_neighbors(
    guard: &StoreInner,
    graph: &FilteredGraph,
    source: &NoteId,
    query_embedding: Option<&[f32]>,
    fanout: usize,
) -> Vec<NoteId> {
    let mut neighbors = graph.get_neighbors(source);
    if neighbors.len() <= fanout.max(1) {
        return neighbors;
    }

    let mut positions: HashMap<NoteId, usize> = HashMap::new();
    for link in &guard.links {
        if &link.source_id != source {
            continue;
        }
        positions
            .entry(link.target_id.clone())
            .and_modify(|current| *current = (*current).min(link.position))
            .or_insert(link.position);
    }

    neighbors.sort_by(|a, b| {
        let score_a = note_semantic_score(guard, a, query_embedding);
        let score_b = note_semantic_score(guard, b, query_embedding);
        score_b
            .total_cmp(&score_a)
            .then_with(|| {
                let pos_a = positions.get(a).copied().unwrap_or(usize::MAX);
                let pos_b = positions.get(b).copied().unwrap_or(usize::MAX);
                pos_a.cmp(&pos_b)
            })
            .then_with(|| a.as_str().cmp(b.as_str()))
    });

    neighbors.truncate(fanout.max(1));
    neighbors
}

fn note_semantic_score(
    guard: &StoreInner,
    note_id: &NoteId,
    query_embedding: Option<&[f32]>,
) -> f32 {
    query_embedding
        .and_then(|query| {
            guard
                .notes
                .get(note_id)
                .and_then(|note| note.embedding.as_ref())
                .map(|embedding| crate::search::cosine_similarity(embedding, query))
        })
        .unwrap_or(0.0)
}

/// Semantic ranking that honors per-note embedding dimensions.
///
/// The system is designed around a single embedding dimensionality at a time,
/// but this keeps search functional during migration windows and in tests that
/// seed custom-sized vectors.
fn semantic_ranking_for_query(docs: &[CandidateSearchDoc], query: &str) -> Vec<(NoteId, f32)> {
    let mut query_embeddings: HashMap<usize, Vec<f32>> = HashMap::new();
    let mut ranked = Vec::new();

    for doc in docs {
        let Some(embedding) = doc.embedding.as_ref() else {
            continue;
        };
        let dimensions = embedding.len();
        if dimensions == 0 {
            continue;
        }

        let query_embedding = query_embeddings
            .entry(dimensions)
            .or_insert_with(|| embed_text(query, dimensions));

        let score = cosine_similarity(embedding, query_embedding);
        if score > 0.0 {
            ranked.push((doc.id.clone(), score));
        }
    }

    ranked.sort_by(|a, b| {
        b.1.total_cmp(&a.1)
            .then_with(|| a.0.as_str().cmp(b.0.as_str()))
    });
    ranked
}

fn summary_with_title_fallback(title: &str, content: &str) -> String {
    let summary = structural_summary(content);
    let title_text = if title.trim().is_empty() {
        "Untitled"
    } else {
        title.trim()
    };
    let title_header = format!("# {}", title_text);

    if summary.is_empty() {
        title_header
    } else {
        let has_h1 = summary.lines().any(|line| line.starts_with("# "));
        if has_h1 {
            summary
        } else {
            format!("{title_header}\n{summary}")
        }
    }
}

fn title_from_note_id(note_id: &str) -> String {
    let filename = note_id.rsplit('/').next().unwrap_or(note_id);
    let stem = filename.strip_suffix(".md").unwrap_or(filename).trim();
    if stem.is_empty() {
        "Untitled".to_string()
    } else {
        stem.to_string()
    }
}

fn note_title_matches_exact(note: &StoredNote, title: &str) -> bool {
    let requested = title.trim();
    !requested.is_empty() && note.title.eq_ignore_ascii_case(requested)
}

fn frontmatter_datetime(frontmatter: &Value, key: &str) -> Option<DateTime<Utc>> {
    frontmatter
        .get(key)
        .and_then(Value::as_str)
        .and_then(|raw| DateTime::parse_from_rfc3339(raw).ok())
        .map(|dt| dt.with_timezone(&Utc))
}

fn note_id_from_deletion_change(change: &ChangeEvent) -> Option<NoteId> {
    if let Some(doc) = change.doc.as_ref() {
        if let Ok(LivesyncDocument::File(file)) = LivesyncDocument::try_from(doc.clone())
            && let Some(path) = file_document_vault_file_id(&file)
        {
            return Some(NoteId::new(path));
        }

        if let Some(path) = doc
            .get("path")
            .and_then(Value::as_str)
            .map(crate::livesync::normalize_note_path)
            .filter(|value| {
                let lower = value.to_ascii_lowercase();
                lower.ends_with(".md") || lower.ends_with(".base")
            })
            .filter(|value| !value.is_empty())
        {
            return Some(NoteId::new(path));
        }
    }

    Some(NoteId::new(change.id.clone()))
}

fn repair_note_alias_locked(
    guard: &mut StoreInner,
    note_path: &str,
    file_doc_id: &str,
    child_ids: &[String],
    changed_note_ids: &mut HashSet<NoteId>,
    deleted_note_ids: &mut HashSet<NoteId>,
) -> bool {
    let canonical_id = NoteId::new(note_path);
    let canonical_title = title_from_note_id(note_path);
    let timestamp_hints = note_timestamp_hints_for_parent_locked(guard, note_path);

    let alias_note_ids = std::iter::once(file_doc_id.to_string())
        .chain(child_ids.iter().cloned())
        .filter(|id| id != note_path)
        .map(NoteId::new)
        .collect::<HashSet<_>>();
    let raw_note_ids = alias_note_ids
        .iter()
        .filter(|id| guard.notes.contains_key(*id))
        .cloned()
        .collect::<Vec<_>>();

    let canonical_needs_repair = guard.notes.get(&canonical_id).is_some_and(|note| {
        let created_at = frontmatter_datetime(&note.frontmatter, "created")
            .or(timestamp_hints.created_at)
            .or(note.created_at);
        let updated_at = frontmatter_datetime(&note.frontmatter, "updated")
            .or(timestamp_hints.updated_at)
            .unwrap_or(note.updated_at);
        note.path != note_path
            || note.title != canonical_title
            || note.created_at != created_at
            || note.updated_at != updated_at
    });

    if raw_note_ids.is_empty() && !canonical_needs_repair {
        return false;
    }

    let mut candidate_note_ids = raw_note_ids.clone();
    if guard.notes.contains_key(&canonical_id) {
        candidate_note_ids.push(canonical_id.clone());
    }
    if candidate_note_ids.is_empty() {
        return false;
    }

    let mut candidates = Vec::new();
    for note_id in &candidate_note_ids {
        let outgoing = outgoing_links(guard, note_id)
            .into_iter()
            .map(|link| LinkInput {
                target_id: if alias_note_ids.contains(&link.target_id) {
                    canonical_id.clone()
                } else {
                    link.target_id
                },
                context_text: link.context_text,
                position: link.position,
            })
            .collect::<Vec<_>>();
        if let Some(note) = guard.notes.remove(note_id) {
            candidates.push((note_id.clone(), note, outgoing));
        }
    }
    if candidates.is_empty() {
        return false;
    }

    let candidate_sources = candidate_note_ids.into_iter().collect::<HashSet<_>>();
    guard
        .links
        .retain(|link| !candidate_sources.contains(&link.source_id));

    let mut graph_dirty = false;
    for link in &mut guard.links {
        if alias_note_ids.contains(&link.target_id) {
            link.target_id = canonical_id.clone();
            graph_dirty = true;
        }
    }
    if graph_dirty {
        dedupe_links_locked(guard);
    }

    let mut merged_links = Vec::new();
    let mut selected_note: Option<StoredNote> = None;
    let mut removed_raw_ids = Vec::new();
    for (note_id, note, outgoing) in candidates {
        if note_id != canonical_id {
            removed_raw_ids.push(note_id.clone());
        }
        let replace_selected = selected_note.as_ref().is_none_or(|current| {
            note.indexed_at > current.indexed_at
                || (note.indexed_at == current.indexed_at && note.updated_at > current.updated_at)
        });
        if replace_selected {
            selected_note = Some(note.clone());
        }
        merged_links.extend(outgoing);
    }

    let Some(base_note) = selected_note else {
        return graph_dirty;
    };

    let created_at = frontmatter_datetime(&base_note.frontmatter, "created")
        .or(timestamp_hints.created_at)
        .or(base_note.created_at);
    let updated_at = frontmatter_datetime(&base_note.frontmatter, "updated")
        .or(timestamp_hints.updated_at)
        .unwrap_or(base_note.updated_at);

    upsert_note_locked(
        guard,
        NoteInput {
            id: canonical_id.clone(),
            title: canonical_title,
            content: base_note.content,
            frontmatter: base_note.frontmatter,
            tags: base_note.tags,
            couchdb_rev: base_note.couchdb_rev,
            created_at,
            updated_at,
            embedding: base_note.embedding,
            links: merged_links,
        },
        base_note.indexed_at,
    );
    changed_note_ids.insert(canonical_id);
    deleted_note_ids.extend(removed_raw_ids);

    graph_dirty || !raw_note_ids.is_empty()
}

fn index_reassembled_note_locked(
    guard: &mut StoreInner,
    note: ReassembledNote,
    context_window: usize,
    now: DateTime<Utc>,
    changed_note_ids: &mut HashSet<NoteId>,
    deleted_note_ids: &mut HashSet<NoteId>,
) {
    let note_parent_id = note.parent_id.clone();
    let parsed = parse_markdown(&note.content, context_window);
    let title = title_from_note_id(&note_parent_id);
    let file_timestamps = note_timestamp_hints_for_parent_locked(guard, &note_parent_id);
    // recent_notes/get_note expose these stored timestamps directly:
    // frontmatter RFC3339 values win, then Livesync file metadata,
    // then ingest time if neither source is available.
    let created_at =
        frontmatter_datetime(&parsed.frontmatter, "created").or(file_timestamps.created_at);
    let updated_at = frontmatter_datetime(&parsed.frontmatter, "updated")
        .or(file_timestamps.updated_at)
        .unwrap_or(now);

    let links = parsed
        .links
        .into_iter()
        .map(|link| LinkInput {
            target_id: NoteId::new(link.target),
            context_text: link.context,
            position: link.byte_offset,
        })
        .collect::<Vec<_>>();

    upsert_note_locked(
        guard,
        NoteInput {
            id: NoteId::new(note_parent_id.clone()),
            title,
            // Persist markdown body only; frontmatter is stored separately.
            content: parsed.body,
            frontmatter: parsed.frontmatter,
            tags: parsed.tags,
            couchdb_rev: note.couchdb_rev,
            created_at,
            updated_at,
            embedding: None,
            links,
        },
        now,
    );

    let note_id = NoteId::new(note_parent_id);
    changed_note_ids.insert(note_id.clone());
    deleted_note_ids.remove(&note_id);
}

#[allow(clippy::too_many_arguments)]
fn rehome_staged_chunks_locked(
    guard: &mut StoreInner,
    note_path: &str,
    file_doc_id: &str,
    child_ids: &[String],
    couchdb_rev: &str,
    context_window: usize,
    now: DateTime<Utc>,
    changed_note_ids: &mut HashSet<NoteId>,
    deleted_note_ids: &mut HashSet<NoteId>,
    staged_chunk_upserts: &mut HashMap<(String, usize), PersistedStagedChunk>,
    staged_parent_deletes: &mut HashSet<String>,
) -> usize {
    let mut indexed_notes = 0;
    let child_positions = child_ids
        .iter()
        .enumerate()
        .map(|(idx, child_id)| (child_id.as_str(), idx))
        .collect::<HashMap<_, _>>();
    let candidate_parent_ids = std::iter::once(file_doc_id.to_string())
        .chain(child_ids.iter().cloned())
        .collect::<HashSet<_>>();

    for candidate_parent_id in candidate_parent_ids {
        let Some(chunks) = guard.chunk_staging.take_parent_chunks(&candidate_parent_id) else {
            continue;
        };
        staged_parent_deletes.insert(candidate_parent_id.clone());

        for mut chunk in chunks {
            if let Some(chunk_index) = child_positions.get(candidate_parent_id.as_str())
                && chunk.chunk_index == 0
                && chunk.chunk_count <= 1
            {
                chunk.chunk_index = *chunk_index;
                chunk.chunk_count = child_ids.len().max(1);
            }

            chunk.parent_id = note_path.to_string();
            chunk.couchdb_rev = couchdb_rev.to_string();
            let pending_chunk = chunk.clone();

            match guard.chunk_staging.stage(chunk) {
                StageResult::Pending { .. } => {
                    staged_chunk_upserts.insert(
                        (pending_chunk.parent_id.clone(), pending_chunk.chunk_index),
                        persisted_staged_chunk_from_decoded(&pending_chunk),
                    );
                }
                StageResult::Complete(note) => {
                    staged_parent_deletes.insert(note.parent_id.clone());
                    staged_chunk_upserts
                        .retain(|(parent_id, _), _| parent_id != note.parent_id.as_str());
                    index_reassembled_note_locked(
                        guard,
                        note,
                        context_window,
                        now,
                        changed_note_ids,
                        deleted_note_ids,
                    );
                    indexed_notes += 1;
                }
            }
        }
    }

    indexed_notes
}

fn register_file_aliases_locked(
    guard: &mut StoreInner,
    file: &FileDocument,
    note_path: String,
) -> Vec<String> {
    let removed_file_aliases =
        unregister_file_aliases_for_note_path_locked(guard, &note_path, Some(&file.id));
    unregister_file_aliases_locked(guard, &file.id);
    guard
        .file_doc_paths
        .insert(file.id.clone(), note_path.clone());
    guard
        .file_doc_revs
        .insert(file.id.clone(), file.rev.clone());
    if let Some(timestamps) = file_timestamp_hint(file) {
        guard
            .note_timestamp_hints
            .insert(note_path.clone(), timestamps);
    } else {
        guard.note_timestamp_hints.remove(&note_path);
    }

    let children = extract_child_doc_ids_ordered(&file.children);
    if children.is_empty() {
        return removed_file_aliases;
    }

    let chunk_count = children.len();
    for (chunk_index, child_id) in children.iter().enumerate() {
        let note_paths = guard.child_doc_paths.entry(child_id.clone()).or_default();
        if !note_paths.iter().any(|existing| existing == &note_path) {
            note_paths.push(note_path.clone());
        }

        let hints = guard.child_chunk_hints.entry(child_id.clone()).or_default();
        let hint = ChildChunkHint {
            note_path: note_path.clone(),
            chunk_index,
            chunk_count,
            couchdb_rev: file.rev.clone(),
        };
        if !hints.iter().any(|existing| existing == &hint) {
            hints.push(hint);
        }
    }
    guard.file_children.insert(file.id.clone(), children);
    removed_file_aliases
}

fn hydrate_file_from_encrypted_metadata(
    file: &mut FileDocument,
    decryptor: Option<&crate::encryption::Decryptor>,
) -> Result<(), String> {
    if !crate::encryption::is_encrypted_meta_path(&file.path) {
        return Ok(());
    }

    let Some(decryptor) = decryptor else {
        return Err("file metadata path is encrypted but no decryptor is configured".to_string());
    };

    let meta = decryptor
        .decrypt_meta_document(&file.path)
        .map_err(|error| error.to_string())?;

    let Some(path) = meta.get("path").and_then(Value::as_str) else {
        return Err("decrypted file metadata is missing `path`".to_string());
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

fn persisted_file_alias_from_file(file: &FileDocument, note_path: &str) -> PersistedFileAlias {
    PersistedFileAlias {
        file_doc_id: file.id.clone(),
        note_path: note_path.to_string(),
        couchdb_rev: file.rev.clone(),
        children: extract_child_doc_ids_ordered(&file.children),
        ctime: file.ctime,
        mtime: file.mtime,
    }
}

fn register_file_alias_persisted_locked(guard: &mut StoreInner, alias: PersistedFileAlias) {
    let note_path = crate::livesync::normalize_note_path(&alias.note_path);
    unregister_file_aliases_locked(guard, &alias.file_doc_id);
    guard
        .file_doc_paths
        .insert(alias.file_doc_id.clone(), note_path.clone());
    guard
        .file_doc_revs
        .insert(alias.file_doc_id.clone(), alias.couchdb_rev.clone());

    let timestamps = FileTimestampHint {
        created_at: couchdb_epoch_to_datetime(alias.ctime)
            .or_else(|| couchdb_epoch_to_datetime(alias.mtime)),
        updated_at: couchdb_epoch_to_datetime(alias.mtime)
            .or_else(|| couchdb_epoch_to_datetime(alias.ctime)),
    };
    if timestamps.created_at.is_some() || timestamps.updated_at.is_some() {
        guard
            .note_timestamp_hints
            .insert(note_path.clone(), timestamps);
    } else {
        guard.note_timestamp_hints.remove(&note_path);
    }

    if alias.children.is_empty() {
        return;
    }

    let chunk_count = alias.children.len();
    for (chunk_index, child_id) in alias.children.iter().enumerate() {
        let note_paths = guard.child_doc_paths.entry(child_id.clone()).or_default();
        if !note_paths.iter().any(|existing| existing == &note_path) {
            note_paths.push(note_path.clone());
        }

        let hints = guard.child_chunk_hints.entry(child_id.clone()).or_default();
        let hint = ChildChunkHint {
            note_path: note_path.clone(),
            chunk_index,
            chunk_count,
            couchdb_rev: alias.couchdb_rev.clone(),
        };
        if !hints.iter().any(|existing| existing == &hint) {
            hints.push(hint);
        }
    }
    guard
        .file_children
        .insert(alias.file_doc_id.clone(), alias.children);
}

fn unregister_file_aliases_locked(guard: &mut StoreInner, file_id: &str) {
    let removed_note_path = guard.file_doc_paths.remove(file_id);
    if let Some(note_path) = removed_note_path.as_ref() {
        guard.note_timestamp_hints.remove(note_path.as_str());
    }
    let removed_rev = guard.file_doc_revs.remove(file_id);
    if let Some(children) = guard.file_children.remove(file_id) {
        for child_id in children {
            if let Some(paths) = guard.child_doc_paths.get_mut(&child_id) {
                if let Some(note_path) = removed_note_path.as_ref() {
                    paths.retain(|existing| existing != note_path);
                } else {
                    paths.clear();
                }
                if paths.is_empty() {
                    guard.child_doc_paths.remove(&child_id);
                }
            }

            if let Some(hints) = guard.child_chunk_hints.get_mut(&child_id) {
                if let Some(note_path) = removed_note_path.as_ref() {
                    if let Some(rev) = removed_rev.as_ref() {
                        hints.retain(|hint| {
                            !(hint.note_path == *note_path && hint.couchdb_rev == *rev)
                        });
                    } else {
                        hints.retain(|hint| hint.note_path != *note_path);
                    }
                } else {
                    hints.clear();
                }
                if hints.is_empty() {
                    guard.child_chunk_hints.remove(&child_id);
                }
            }
        }
    }
}

fn unregister_file_aliases_for_note_path_locked(
    guard: &mut StoreInner,
    note_path: &str,
    except_file_id: Option<&str>,
) -> Vec<String> {
    let file_doc_ids = guard
        .file_doc_paths
        .iter()
        .filter(|(file_doc_id, existing_note_path)| {
            existing_note_path.as_str() == note_path && except_file_id != Some(file_doc_id.as_str())
        })
        .map(|(file_doc_id, _)| file_doc_id.clone())
        .collect::<Vec<_>>();

    for file_doc_id in &file_doc_ids {
        unregister_file_aliases_locked(guard, file_doc_id);
    }

    file_doc_ids
}

fn dedupe_links_locked(guard: &mut StoreInner) {
    let mut unique = HashMap::<(NoteId, NoteId), LinkRecord>::new();
    for link in guard.links.drain(..) {
        let key = (link.source_id.clone(), link.target_id.clone());
        match unique.get_mut(&key) {
            Some(existing) => {
                if link.position < existing.position {
                    *existing = link;
                }
            }
            None => {
                unique.insert(key, link);
            }
        }
    }

    let mut deduped = unique.into_values().collect::<Vec<_>>();
    deduped.sort_by(|a, b| {
        a.source_id
            .as_str()
            .cmp(b.source_id.as_str())
            .then_with(|| a.target_id.as_str().cmp(b.target_id.as_str()))
            .then_with(|| a.position.cmp(&b.position))
    });
    guard.links = deduped;
}

fn extract_child_doc_ids_ordered(children: &[Value]) -> Vec<String> {
    let mut out = Vec::new();
    let mut seen = HashSet::new();
    for child in children {
        if let Some(child_id) = parse_child_doc_id(child)
            && seen.insert(child_id.clone())
        {
            out.push(child_id);
        }
    }
    out
}

fn parse_child_doc_id(value: &Value) -> Option<String> {
    match value {
        Value::String(raw) => {
            let child_id = raw.trim();
            (!child_id.is_empty()).then_some(child_id.to_string())
        }
        Value::Object(map) => ["id", "_id", "child", "doc_id", "docId"]
            .iter()
            .find_map(|key| map.get(*key).and_then(Value::as_str))
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToString::to_string),
        _ => None,
    }
}

fn stale_file_doc_ids_for_recovery_locked(guard: &StoreInner) -> Vec<String> {
    stale_file_recovery_targets_locked(guard)
        .into_iter()
        .map(|target| target.file_doc_id)
        .collect()
}

fn stale_file_recovery_targets_locked(guard: &StoreInner) -> Vec<StaleFileRecoveryTarget> {
    let mut stale_file_targets = Vec::new();

    for (file_doc_id, note_path) in &guard.file_doc_paths {
        let Some(file_doc_rev) = guard.file_doc_revs.get(file_doc_id) else {
            continue;
        };

        let vault_file_is_stale = guard
            .vault_files
            .get(note_path)
            .is_none_or(|file| file.couchdb_rev != *file_doc_rev);
        let note_is_stale = is_markdown_note_path(note_path)
            && guard
                .notes
                .get(&NoteId::new(note_path))
                .is_none_or(|note| note.couchdb_rev != *file_doc_rev);
        if vault_file_is_stale || note_is_stale {
            stale_file_targets.push(StaleFileRecoveryTarget {
                file_doc_id: file_doc_id.clone(),
                note_path: note_path.clone(),
                child_doc_ids: guard
                    .file_children
                    .get(file_doc_id)
                    .cloned()
                    .unwrap_or_default(),
                needs_file_document: note_is_stale,
            });
        }
    }

    let targeted_paths = stale_file_targets
        .iter()
        .map(|target| target.note_path.clone())
        .collect::<HashSet<_>>();
    for note in guard.notes.values() {
        if !guard.vault_files.contains_key(note.path.as_str())
            && !targeted_paths.contains(note.path.as_str())
        {
            stale_file_targets.push(StaleFileRecoveryTarget {
                file_doc_id: note.path.clone(),
                note_path: note.path.clone(),
                child_doc_ids: Vec::new(),
                needs_file_document: true,
            });
        }
    }

    stale_file_targets.sort_by(|a, b| {
        a.file_doc_id
            .cmp(&b.file_doc_id)
            .then_with(|| a.note_path.cmp(&b.note_path))
    });
    stale_file_targets.dedup();
    stale_file_targets
}

fn classify_purged_chunk_staging_locked(
    guard: &StoreInner,
    mut purged_parent_ids: Vec<String>,
) -> ChunkStagingPurgeResult {
    purged_parent_ids.sort();
    purged_parent_ids.dedup();

    let mut recovery_parent_ids = Vec::new();
    let mut orphan_leaf_parent_ids = Vec::new();

    for parent_id in &purged_parent_ids {
        let recovery_ids = recovery_parent_ids_for_purged_parent_locked(guard, parent_id);
        if recovery_ids.is_empty() {
            orphan_leaf_parent_ids.push(parent_id.clone());
        } else {
            recovery_parent_ids.extend(recovery_ids);
        }
    }

    recovery_parent_ids.sort();
    recovery_parent_ids.dedup();
    orphan_leaf_parent_ids.sort();
    orphan_leaf_parent_ids.dedup();

    ChunkStagingPurgeResult {
        purged_parent_ids,
        recovery_parent_ids,
        orphan_leaf_parent_ids,
    }
}

fn recovery_parent_ids_for_purged_parent_locked(
    guard: &StoreInner,
    parent_id: &str,
) -> Vec<String> {
    if !parent_id.starts_with("h:") {
        return vec![parent_id.to_string()];
    }

    let mut recovery_parent_ids = Vec::new();

    for (file_doc_id, children) in &guard.file_children {
        if children.iter().any(|child_id| child_id == parent_id) {
            recovery_parent_ids.push(file_doc_id.clone());
        }
    }

    if let Some(note_paths) = guard.child_doc_paths.get(parent_id) {
        recovery_parent_ids.extend(note_paths.iter().cloned());
    }

    if let Some(hints) = guard.child_chunk_hints.get(parent_id) {
        recovery_parent_ids.extend(hints.iter().map(|hint| hint.note_path.clone()));
    }

    recovery_parent_ids.sort();
    recovery_parent_ids.dedup();
    recovery_parent_ids
}

fn orphan_leaf_staging_count_locked(guard: &StoreInner) -> usize {
    guard
        .chunk_staging
        .parent_ids()
        .filter(|parent_id| is_orphan_leaf_staging_parent_locked(guard, parent_id))
        .count()
}

fn is_orphan_leaf_staging_parent_locked(guard: &StoreInner, parent_id: &str) -> bool {
    parent_id.starts_with("h:")
        && recovery_parent_ids_for_purged_parent_locked(guard, parent_id).is_empty()
}

fn apply_chunk_aliases_locked(
    guard: &StoreInner,
    leaf_doc_id: &str,
    chunk: DecodedChunk,
) -> Vec<DecodedChunk> {
    let raw_parent_id = chunk.parent_id.clone();
    let chunk_hints = guard
        .child_chunk_hints
        .get(&raw_parent_id)
        .or_else(|| guard.child_chunk_hints.get(leaf_doc_id));

    if let Some(hints) = chunk_hints
        && !hints.is_empty()
    {
        let mut aliased: Vec<DecodedChunk> = Vec::with_capacity(hints.len());
        for hint in hints {
            let mut aliased_chunk = chunk.clone();
            // Opaque chunks often have no chunk metadata; infer order/count from
            // the parent file document's `children` list.
            if aliased_chunk.chunk_index == 0
                && aliased_chunk.chunk_count <= 1
                && hint.chunk_count > 1
            {
                aliased_chunk.chunk_index = hint.chunk_index;
                aliased_chunk.chunk_count = hint.chunk_count;
            }
            aliased_chunk.parent_id = hint.note_path.clone();
            aliased_chunk.couchdb_rev = hint.couchdb_rev.clone();

            if !aliased.iter().any(|existing| {
                existing.parent_id == aliased_chunk.parent_id
                    && existing.chunk_index == aliased_chunk.chunk_index
                    && existing.couchdb_rev == aliased_chunk.couchdb_rev
            }) {
                aliased.push(aliased_chunk);
            }
        }
        if !aliased.is_empty() {
            return aliased;
        }
    }

    if let Some(note_path) = guard.file_doc_paths.get(&raw_parent_id) {
        let mut aliased = chunk;
        aliased.parent_id = note_path.clone();
        if let Some(rev) = guard.file_doc_revs.get(&raw_parent_id) {
            aliased.couchdb_rev = rev.clone();
        }
        return vec![aliased];
    }
    if let Some(note_paths) = guard.child_doc_paths.get(&raw_parent_id)
        && !note_paths.is_empty()
    {
        return note_paths
            .iter()
            .map(|note_path| {
                let mut aliased = chunk.clone();
                aliased.parent_id = note_path.clone();
                aliased
            })
            .collect();
    }
    if let Some(note_paths) = guard.child_doc_paths.get(leaf_doc_id)
        && !note_paths.is_empty()
    {
        return note_paths
            .iter()
            .map(|note_path| {
                let mut aliased = chunk.clone();
                aliased.parent_id = note_path.clone();
                aliased
            })
            .collect();
    }

    vec![chunk]
}

fn resolve_note_alias_locked(guard: &StoreInner, note_id: &str) -> String {
    // Deletion events should only alias file metadata IDs to note paths.
    // Never map `h:*` child IDs here; chunk tombstones are not note deletions.
    guard
        .file_doc_paths
        .get(note_id)
        .cloned()
        .unwrap_or_else(|| note_id.to_string())
}

fn note_timestamp_hints_for_parent_locked(
    guard: &StoreInner,
    parent_id: &str,
) -> FileTimestampHint {
    guard
        .note_timestamp_hints
        .get(parent_id)
        .cloned()
        .unwrap_or_default()
}

fn file_timestamp_hint(file: &FileDocument) -> Option<FileTimestampHint> {
    let created_at =
        couchdb_epoch_to_datetime(file.ctime).or_else(|| couchdb_epoch_to_datetime(file.mtime));
    let updated_at =
        couchdb_epoch_to_datetime(file.mtime).or_else(|| couchdb_epoch_to_datetime(file.ctime));
    if created_at.is_none() && updated_at.is_none() {
        return None;
    }

    Some(FileTimestampHint {
        created_at,
        updated_at,
    })
}

fn couchdb_epoch_to_datetime(raw: i64) -> Option<DateTime<Utc>> {
    if raw <= 0 {
        return None;
    }

    // Livesync metadata is typically unix milliseconds; keep seconds support
    // for schema variants and tests.
    let (seconds, nanos) = if raw >= 10_000_000_000 {
        let seconds = raw / 1_000;
        let millis = (raw % 1_000) as u32;
        (seconds, millis.saturating_mul(1_000_000))
    } else {
        (raw, 0)
    };

    DateTime::<Utc>::from_timestamp(seconds, nanos)
}

fn deletion_file_doc_id(change: &ChangeEvent) -> Option<String> {
    if let Some(file_doc_id) = change
        .doc
        .as_ref()
        .filter(|doc| doc.get("path").is_some() && doc.get("children").is_some())
        .and_then(|doc| doc.get("_id").and_then(Value::as_str))
    {
        return Some(file_doc_id.to_string());
    }

    if change.id.starts_with("h:") {
        return None;
    }

    (change.id.starts_with("f:") || {
        let lower = change.id.to_ascii_lowercase();
        lower.ends_with(".md") || lower.ends_with(".base")
    })
    .then(|| change.id.clone())
}

fn snippet_for(content: &str, query: &str) -> String {
    if let Some(pos) = find_case_insensitive(content, query) {
        let start = pos.saturating_sub(48);
        let end = (pos + query.len() + 80).min(content.len());
        safe_slice(content, start, end).replace('\n', " ")
    } else {
        content
            .lines()
            .next()
            .unwrap_or_default()
            .chars()
            .take(120)
            .collect()
    }
}

fn search_hit_from_persisted(
    note: &PersistedSearchNote,
    query: &str,
    score: f32,
    match_type: MatchType,
    fallback_id: &NoteId,
    chunk_match: Option<ChunkMatchMetadata>,
) -> UnscopedSearchHit {
    let id = if note.id.trim().is_empty() {
        fallback_id.clone()
    } else {
        NoteId::new(note.id.clone())
    };
    let (matched_chunk_id, matched_heading_path, matched_snippet) = chunk_match
        .map(|matched| {
            (
                Some(matched.block_id),
                (!matched.heading_path.is_empty()).then_some(matched.heading_path),
                Some(matched.snippet),
            )
        })
        .unwrap_or((None, None, None));
    UnscopedSearchHit {
        id,
        title: title_from_note_id(note.id.as_str()),
        snippet: snippet_for(&note.content, query),
        score,
        match_type,
        matched_chunk_id,
        matched_heading_path,
        matched_snippet,
    }
}

fn chunk_match_metadata(block: &BlockSemanticMatch, query: &str) -> ChunkMatchMetadata {
    ChunkMatchMetadata {
        block_id: block.block_id.clone(),
        heading_path: block.heading_path.clone(),
        snippet: snippet_for(&block.content, query),
    }
}

fn search_ranking_limit(limit: usize) -> usize {
    limit.max(20).saturating_mul(10).clamp(100, 5_000)
}

fn find_case_insensitive(content: &str, query: &str) -> Option<usize> {
    if query.is_empty() {
        return None;
    }

    regex::RegexBuilder::new(&regex::escape(query))
        .case_insensitive(true)
        .build()
        .ok()
        .and_then(|regex| regex.find(content).map(|m| m.start()))
}

fn safe_slice(content: &str, start: usize, end: usize) -> &str {
    let mut safe_start = start.min(content.len());
    while safe_start > 0 && !content.is_char_boundary(safe_start) {
        safe_start -= 1;
    }

    let mut safe_end = end.min(content.len());
    while safe_end < content.len() && !content.is_char_boundary(safe_end) {
        safe_end += 1;
    }

    if safe_end < safe_start {
        safe_end = safe_start;
    }

    &content[safe_start..safe_end]
}

fn parse_seq_value(value: &str) -> i64 {
    value
        .split('-')
        .next()
        .and_then(|v| v.parse::<i64>().ok())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use chrono::{Duration, Utc};
    use serde_json::{Value, json};
    use static_assertions::assert_not_impl_any;

    use super::{
        NeighborDirection, NoteInput, QueryBaseRequest, QueryNotesRequest, RuntimeSyncState,
        StoreSettings, StoredVaultFile, UnscopedBacklinkEntry, UnscopedNeighborNode,
        UnscopedRecentNoteSummary, VaultStore, find_case_insensitive, get_note_from_inner,
        neighbors_from_inner, note_readable_for_policy_from_inner,
        stale_file_recovery_targets_locked, status_from_inner, store_inner_from_persisted_notes,
        upsert_note_locked,
    };
    use crate::authorization::{
        AccessMatcher, AccessPolicy, AccessRule, AuthContext, AuthorizationConfig, ContextName,
    };
    use crate::model::NoteId;
    use crate::persistence::{PersistedLinkRecord, PersistedNoteRecord};

    #[test]
    fn unscoped_store_payloads_are_not_serializable() {
        assert_not_impl_any!(UnscopedRecentNoteSummary: serde::Serialize);
        assert_not_impl_any!(UnscopedNeighborNode: serde::Serialize);
        assert_not_impl_any!(UnscopedBacklinkEntry: serde::Serialize);
    }

    #[test]
    fn find_case_insensitive_matches_mixed_case_query() {
        let content = "Alpha beta DEF gamma";
        assert_eq!(find_case_insensitive(content, "def"), content.find("DEF"));
    }

    #[test]
    fn base_alias_freshness_depends_on_raw_file_not_markdown_index() {
        let now = Utc::now();
        let settings = StoreSettings::new(20);
        let sync_state = RuntimeSyncState::new(now);
        let mut inner = store_inner_from_persisted_notes(Vec::new(), &settings, &sync_state);
        let path = "11New/dashboard.base";
        inner
            .file_doc_paths
            .insert("f:base".to_string(), path.to_string());
        inner
            .file_doc_revs
            .insert("f:base".to_string(), "2-base".to_string());
        inner.vault_files.insert(
            path.to_string(),
            StoredVaultFile {
                path: path.to_string(),
                content: "views: []\n".to_string(),
                couchdb_rev: "2-base".to_string(),
                created_at: Some(now),
                updated_at: now,
                indexed_at: now,
            },
        );

        assert!(stale_file_recovery_targets_locked(&inner).is_empty());
        inner.vault_files.remove(path);
        assert_eq!(stale_file_recovery_targets_locked(&inner).len(), 1);
    }

    #[test]
    fn indexed_markdown_without_raw_file_is_recoverable_without_alias() {
        let now = Utc::now();
        let settings = StoreSettings::new(20);
        let sync_state = RuntimeSyncState::new(now);
        let mut inner = store_inner_from_persisted_notes(Vec::new(), &settings, &sync_state);
        let path = "11New/missing-raw.md";
        upsert_note_locked(
            &mut inner,
            note_input(path, "Missing Raw", vec!["shared"], now),
            now,
        );

        let targets = stale_file_recovery_targets_locked(&inner);
        assert_eq!(targets.len(), 1);
        assert_eq!(targets[0].file_doc_id, path);
        assert_eq!(targets[0].note_path, path);
        assert!(targets[0].child_doc_ids.is_empty());
        assert!(targets[0].needs_file_document);
    }

    #[test]
    fn persisted_snapshot_context_filters_private_links_and_backlinks() {
        let root_id = NoteId::new("root.md");
        let secret_id = NoteId::new("secret.md");
        let inner = persisted_inner_fixture();
        let config = external_config();
        let auth = external_auth();
        let now = Utc::now();

        let root = get_note_from_inner(&inner, &root_id, Some(&config), Some(&auth), now)
            .expect("root note");
        assert_eq!(root.links, vec![NoteId::new("public.md")]);
        assert_eq!(root.backlinks, vec![NoteId::new("public.md")]);
        assert!(
            !note_readable_for_policy_from_inner(&inner, &config, &auth, &secret_id, now),
            "secret note should be filtered"
        );
    }

    #[test]
    fn incoming_neighbors_return_backlink_context() {
        let inner = persisted_inner_fixture();
        let root_id = NoteId::new("root.md");
        let config = external_config();
        let auth = external_auth();

        let response = neighbors_from_inner(
            &inner,
            &config,
            &auth,
            &root_id,
            1,
            NeighborDirection::Incoming,
        )
        .expect("neighbors");

        assert_eq!(response.direction, NeighborDirection::Incoming);
        assert_eq!(response.nodes.len(), 1);
        assert_eq!(response.nodes[0].id, NoteId::new("public.md"));
        assert_eq!(response.nodes[0].direction, NeighborDirection::Incoming);
        assert_eq!(response.nodes[0].link_context, "public to root");
    }

    #[tokio::test]
    async fn title_lookup_prefers_latest_visible_duplicate() {
        let now = Utc::now();
        let store = VaultStore::new(20);
        store.set_authorization_config(external_config()).await;
        store
            .upsert_note(note_input(
                "public-old.md",
                "Duplicate",
                vec!["shared"],
                now - Duration::days(2),
            ))
            .await;
        store
            .upsert_note(note_input(
                "public-new.md",
                "Duplicate",
                vec!["shared"],
                now - Duration::days(1),
            ))
            .await;
        store
            .upsert_note(note_input("secret.md", "Duplicate", vec!["private"], now))
            .await;

        let note = store
            .get_note_by_title_for_policy(&external_auth(), "duplicate")
            .await
            .expect("visible duplicate title");

        assert_eq!(note.id, NoteId::new("public-new.md"));
    }

    #[tokio::test]
    async fn exact_id_and_title_lookup_support_space_hyphen_path_shape() {
        let now = Utc::now();
        let store = VaultStore::new(20);
        store.set_authorization_config(external_config()).await;
        let note_id = NoteId::new("Public Notes/synthetic-hyphen draft 5.md");
        store
            .upsert_note(note_input(
                note_id.as_str(),
                "synthetic-hyphen draft 5",
                vec!["shared"],
                now,
            ))
            .await;

        let by_id = store
            .get_note_for_policy(&external_auth(), &note_id)
            .await
            .expect("visible note should resolve by exact id");
        assert_eq!(by_id.id, note_id);

        let by_title = store
            .get_note_by_title_for_policy(&external_auth(), "SYNTHETIC-HYPHEN DRAFT 5")
            .await
            .expect("visible note should resolve by case-insensitive exact title");
        assert_eq!(
            by_title.id,
            NoteId::new("Public Notes/synthetic-hyphen draft 5.md")
        );
    }

    #[tokio::test]
    async fn query_notes_filters_visible_notes_by_exact_tag() {
        let now = Utc::now();
        let store = VaultStore::new(20);
        store.set_authorization_config(external_config()).await;
        store
            .upsert_note(note_input(
                "public-old.md",
                "Public Old",
                vec!["shared"],
                now - Duration::days(2),
            ))
            .await;
        store
            .upsert_note(note_input(
                "public-new.md",
                "Public New",
                vec!["shared"],
                now - Duration::days(1),
            ))
            .await;
        store
            .upsert_note(note_input(
                "public-case.md",
                "Public Case",
                vec!["Shared"],
                now,
            ))
            .await;
        store
            .upsert_note(note_input("secret.md", "Secret", vec!["shared"], now))
            .await;

        let response = store
            .query_notes_for_policy(
                &external_auth(),
                QueryNotesRequest {
                    tags_all: vec!["shared".to_string()],
                    ..Default::default()
                },
            )
            .await;

        let ids = response
            .notes
            .iter()
            .map(|note| note.id.clone())
            .collect::<Vec<_>>();
        assert_eq!(
            ids,
            vec![NoteId::new("public-new.md"), NoteId::new("public-old.md")]
        );
        assert_eq!(response.total, 2);

        let case_response = store
            .query_notes_for_policy(
                &external_auth(),
                QueryNotesRequest {
                    tags_all: vec!["Shared".to_string()],
                    ..Default::default()
                },
            )
            .await;
        assert_eq!(case_response.notes.len(), 1);
        assert_eq!(case_response.notes[0].id, NoteId::new("public-case.md"));
    }

    #[tokio::test]
    async fn query_base_projects_base_table_rows_and_hides_denied_notes() {
        let now = Utc::now();
        let store = VaultStore::new(20);
        store.set_authorization_config(external_config()).await;
        store
            .upsert_note(note_input_with_frontmatter(
                "public-rings.md",
                "Public Rings",
                vec!["workout-exercise"],
                now,
                json!({
                    "uses": ["mini_wod"],
                    "equipment": ["rings"],
                    "exercise_status": ["unassessed"]
                }),
            ))
            .await;
        store
            .upsert_note(note_input_with_frontmatter(
                "public-bike.md",
                "Public Bike",
                vec!["workout-exercise"],
                now - Duration::minutes(1),
                json!({
                    "uses": ["mini_wod"],
                    "equipment": ["bike"],
                    "exercise_status": ["active"]
                }),
            ))
            .await;
        store
            .upsert_note(note_input_with_frontmatter(
                "secret.md",
                "Secret Rings",
                vec!["workout-exercise"],
                now,
                json!({
                    "uses": ["mini_wod"],
                    "equipment": ["rings"],
                    "exercise_status": ["unassessed"]
                }),
            ))
            .await;

        let response = store
            .query_base_for_policy(
                &external_auth(),
                QueryBaseRequest {
                    base_query: r#"
filters:
  and:
    - file.hasTag("workout-exercise")
    - list(uses).contains("mini_wod")
    - list(equipment).contains("rings")
properties:
  note.exercise_status:
    displayName: Status
views:
  - type: table
    name: Candidates
    limit: 25
    order:
      - file.name
      - note.equipment
      - note.exercise_status
"#
                    .to_string(),
                    view: Some("Candidates".to_string()),
                    limit: None,
                },
            )
            .await
            .expect("base query succeeds");

        assert_eq!(response.total, 1);
        assert_eq!(response.returned, 1);
        assert_eq!(response.rows[0].note_id, NoteId::new("public-rings.md"));
        assert_eq!(response.rows[0].cells["note.equipment"], json!(["rings"]));
        assert_eq!(response.columns[2].label, "Status");
    }

    #[test]
    fn persisted_snapshot_status_uses_runtime_sync_metadata() {
        let now = Utc::now();
        let settings = StoreSettings::new(20);
        let sync_state = RuntimeSyncState {
            last_seq: "10".to_string(),
            couchdb_current_seq: "15".to_string(),
            last_sync_at: now,
        };
        let inner =
            store_inner_from_persisted_notes(persisted_notes_fixture(now), &settings, &sync_state);
        let status = status_from_inner(
            &inner,
            7,
            0,
            0,
            &crate::persistence::RecoveryQueueStats::default(),
            &sync_state,
        );

        assert_eq!(status.index.total_notes, 3);
        assert_eq!(status.index.total_links, 4);
        assert_eq!(status.index.total_tags, 2);
        assert_eq!(status.index.pending_embeddings, 3);
        assert_eq!(status.index.pending_chunks, 7);
        assert_eq!(status.index.orphan_leaf_staging_count, 0);
        assert_eq!(status.index.stale_file_aliases, 0);
        assert_eq!(status.sync.last_seq, "10");
        assert_eq!(status.sync.couchdb_current_seq, "15");
        assert_eq!(status.sync.behind_by, 5);
        assert_eq!(status.sync.last_sync_at, now);
        assert!(status.context_stats.is_empty());
    }

    fn persisted_inner_fixture() -> super::StoreInner {
        let now = Utc::now();
        let settings = StoreSettings::new(20);
        let sync_state = RuntimeSyncState::new(now);
        store_inner_from_persisted_notes(persisted_notes_fixture(now), &settings, &sync_state)
    }

    fn persisted_notes_fixture(now: chrono::DateTime<Utc>) -> Vec<PersistedNoteRecord> {
        vec![
            persisted_note(
                "root.md",
                "Root",
                vec![
                    persisted_link("public.md", "root to public", 0),
                    persisted_link("secret.md", "root to secret", 1),
                ],
                vec!["shared"],
                now,
            ),
            persisted_note(
                "public.md",
                "Public",
                vec![persisted_link("root.md", "public to root", 0)],
                vec!["shared"],
                now,
            ),
            persisted_note(
                "secret.md",
                "Secret",
                vec![persisted_link("root.md", "secret to root", 0)],
                vec!["private"],
                now,
            ),
        ]
    }

    fn persisted_note(
        id: &str,
        title: &str,
        links: Vec<PersistedLinkRecord>,
        tags: Vec<&str>,
        now: chrono::DateTime<Utc>,
    ) -> PersistedNoteRecord {
        PersistedNoteRecord {
            id: id.to_string(),
            path: id.to_string(),
            title: title.to_string(),
            content: format!("# {title}\n\nBody for {title}."),
            search_text: format!("{title} body"),
            summary: format!("Summary for {title}"),
            frontmatter: json!({}),
            tags: tags.into_iter().map(ToString::to_string).collect(),
            couchdb_rev: "1-test".to_string(),
            created_at: Some(now),
            updated_at: now,
            indexed_at: now,
            embedding: None,
            links,
        }
    }

    fn note_input(
        id: &str,
        title: &str,
        tags: Vec<&str>,
        updated_at: chrono::DateTime<Utc>,
    ) -> NoteInput {
        note_input_with_frontmatter(id, title, tags, updated_at, json!({}))
    }

    fn note_input_with_frontmatter(
        id: &str,
        title: &str,
        tags: Vec<&str>,
        updated_at: chrono::DateTime<Utc>,
        frontmatter: Value,
    ) -> NoteInput {
        NoteInput {
            id: NoteId::new(id),
            title: title.to_string(),
            content: format!("# {title}\n\nBody for {id}."),
            frontmatter,
            tags: tags.into_iter().map(ToString::to_string).collect(),
            couchdb_rev: "1-test".to_string(),
            created_at: Some(updated_at),
            updated_at,
            embedding: None,
            links: Vec::new(),
        }
    }

    fn persisted_link(target_id: &str, context_text: &str, position: usize) -> PersistedLinkRecord {
        PersistedLinkRecord {
            target_id: target_id.to_string(),
            context_text: context_text.to_string(),
            position,
        }
    }

    fn external_auth() -> AuthContext {
        AuthContext::new(ContextName::new("external"), "test-principal".to_string())
    }

    fn external_config() -> AuthorizationConfig {
        let mut policy = AccessPolicy::default_agent();
        policy.read = vec![
            AccessRule::deny(AccessMatcher {
                path_prefix: Some("secret.md".to_string()),
                ..Default::default()
            }),
            AccessRule::allow(AccessMatcher::allow_all()),
        ];
        let mut config = BTreeMap::new();
        config.insert("external".to_string(), policy);
        config
    }
}

use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use once_cell::sync::Lazy;
use regex::Regex;
use serde::Deserialize;
use serde_json::json;
use sha2::{Digest, Sha256};
use thiserror::Error;
use tokio::task::{JoinHandle, yield_now};
use tokio::time::{Instant, sleep};
use tracing::{debug, info, warn};

use crate::config::{AppConfig, EmbeddingConfig, EmbeddingMode};
use crate::couchdb::{CouchDbClient, CouchDbError};
use crate::encryption::Decryptor;
use crate::livesync::{ChangeEvent, LivesyncDocument, is_deletion};
use crate::store::{StaleFileRecoveryTarget, SyncBatchResult, VaultStore};

const LOCALAI_HEALTH_PROBE_INPUT: &str = "vault bridge embedding health probe";
const STALE_FILE_RECOVERY_TARGET_LIMIT: usize = 16;

static DATA_URI_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r#"data:[^\s)>"']{128,}"#).expect("valid data URI regex"));
static OPAQUE_TOKEN_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"[A-Za-z0-9_+/=-]{160,}").expect("valid opaque token regex"));

pub fn spawn_sync_worker(
    store: VaultStore,
    config: &AppConfig,
    decryptor: Option<Arc<Decryptor>>,
) -> Result<Option<JoinHandle<()>>, WorkerError> {
    if !config.couchdb.is_configured() {
        return Ok(None);
    }

    let couch = CouchDbClient::new(&config.couchdb)?.with_livesync_crypto(decryptor.clone());
    let context_window = config.indexer.max_link_context_chars;
    let max_changes_per_batch = config.indexer.max_changes_per_batch.max(1);
    let chunk_timeout = Duration::from_secs(config.indexer.chunk_staging_timeout_seconds.max(1));
    let stale_file_alias_recovery_interval = chunk_timeout.max(Duration::from_secs(60));
    let poll_interval = config.couchdb.poll_interval();
    let debounce_window = Duration::from_secs(config.indexer.debounce_seconds.max(1));

    Ok(Some(tokio::spawn(async move {
        let mut backoff = Duration::from_secs(1);
        let mut poll_since = store.status().await.sync.last_seq;
        let mut pending = Vec::new();
        let mut pending_current_seq = String::new();
        let mut last_event_at: Option<Instant> = None;
        let mut last_stale_file_alias_recovery_at = Instant::now();

        let flush_pending = |pending: &mut Vec<crate::livesync::ChangeEvent>,
                             pending_current_seq: &mut String,
                             last_event_at: &mut Option<Instant>| {
            let drained = std::mem::take(pending);
            let current_seq = if pending_current_seq.is_empty() {
                "0".to_string()
            } else {
                pending_current_seq.clone()
            };
            pending_current_seq.clear();
            *last_event_at = None;
            (drained, current_seq)
        };

        let startup_current_seq = match couch.current_sequence().await {
            Ok(seq) => seq,
            Err(error) => {
                warn!(
                    error = %error,
                    "sync worker: failed to read current sequence during startup chunk recovery"
                );
                poll_since.clone()
            }
        };

        let startup_purged = store
            .recover_stale_chunk_staging_at(chunk_timeout, Utc::now())
            .await;
        if !startup_purged.is_empty() {
            warn!(
                purged_parent_count = startup_purged.purged_parent_ids.len(),
                recovery_parent_count = startup_purged.recovery_parent_ids.len(),
                orphan_leaf_parent_count = startup_purged.orphan_leaf_parent_ids.len(),
                timeout_seconds = chunk_timeout.as_secs(),
                "sync worker: startup chunk staging recovery purged stale parents"
            );
            queue_parent_recovery(
                &couch,
                &startup_purged.recovery_parent_ids,
                &mut pending,
                &mut pending_current_seq,
                &mut last_event_at,
                &startup_current_seq,
            )
            .await;
        }

        let startup_stale_file_targets = store.stale_file_recovery_targets().await;
        let startup_stale_file_total = startup_stale_file_targets.len();
        let startup_stale_file_targets = take_stale_file_recovery_targets(
            startup_stale_file_targets,
            STALE_FILE_RECOVERY_TARGET_LIMIT,
        );
        let startup_stale_file_docs = startup_stale_file_targets
            .iter()
            .map(|target| target.file_doc_id.clone())
            .collect::<Vec<_>>();
        let startup_remote_drift_file_docs = detect_remote_stale_file_docs(&store, &couch).await;
        let mut startup_recovery_lookup_ids =
            recovery_lookup_ids_for_stale_file_targets(&startup_stale_file_targets);
        startup_recovery_lookup_ids.extend(
            startup_remote_drift_file_docs
                .iter()
                .take(STALE_FILE_RECOVERY_TARGET_LIMIT)
                .cloned(),
        );
        startup_recovery_lookup_ids.sort();
        startup_recovery_lookup_ids.dedup();
        let startup_recovery_child_ids =
            recovery_child_doc_ids_for_stale_file_targets(&startup_stale_file_targets);

        if !startup_recovery_lookup_ids.is_empty() || !startup_recovery_child_ids.is_empty() {
            warn!(
                stale_file_alias_total = startup_stale_file_total,
                stale_file_doc_count = startup_stale_file_docs.len(),
                stale_file_recovery_limit = STALE_FILE_RECOVERY_TARGET_LIMIT,
                remote_drift_file_doc_count = startup_remote_drift_file_docs.len(),
                recovery_lookup_count = startup_recovery_lookup_ids.len(),
                recovery_child_count = startup_recovery_child_ids.len(),
                "sync worker: startup detected stale file aliases or remote drift; queuing bounded parent recovery"
            );
            queue_parent_recovery(
                &couch,
                &startup_recovery_lookup_ids,
                &mut pending,
                &mut pending_current_seq,
                &mut last_event_at,
                &startup_current_seq,
            )
            .await;
            queue_exact_recovery(
                &couch,
                &startup_recovery_child_ids,
                &mut pending,
                &mut pending_current_seq,
                &mut last_event_at,
                &startup_current_seq,
            )
            .await;
            queue_path_scan_recovery(
                &couch,
                &startup_stale_file_targets,
                decryptor.as_deref(),
                &mut pending,
                &mut pending_current_seq,
                &mut last_event_at,
                &startup_current_seq,
            )
            .await;
        }

        if !pending.is_empty() {
            let recovered = std::mem::take(&mut pending);
            pending_current_seq.clear();
            last_event_at = None;

            let batch = ingest_changes_cooperatively(
                &store,
                &couch,
                recovered,
                &startup_current_seq,
                context_window,
                chunk_timeout,
                decryptor.as_deref(),
                max_changes_per_batch,
            )
            .await;
            info!(
                indexed = batch.indexed_notes,
                deleted = batch.deleted_notes,
                pending_chunks = batch.pending_chunks,
                purged_parent_count = batch.purged_parent_ids.len(),
                recovery_parent_count = batch.recovery_parent_ids.len(),
                orphan_leaf_parent_count = batch.orphan_leaf_parent_ids.len(),
                stale_file_alias_total = startup_stale_file_total,
                stale_file_doc_count = startup_stale_file_docs.len(),
                remote_drift_file_doc_count = startup_remote_drift_file_docs.len(),
                "sync worker: bounded startup recovery replay completed"
            );
        }

        loop {
            let current_seq = match couch.current_sequence().await {
                Ok(seq) => seq,
                Err(error) => {
                    if should_flush_pending(pending.len(), last_event_at, debounce_window) {
                        let (drained, current_seq) = flush_pending(
                            &mut pending,
                            &mut pending_current_seq,
                            &mut last_event_at,
                        );
                        let drained_count = drained.len();
                        let batch = ingest_changes_cooperatively(
                            &store,
                            &couch,
                            drained,
                            &current_seq,
                            context_window,
                            chunk_timeout,
                            decryptor.as_deref(),
                            max_changes_per_batch,
                        )
                        .await;
                        debug!(
                            input_changes = drained_count,
                            indexed = batch.indexed_notes,
                            deleted = batch.deleted_notes,
                            pending_chunks = batch.pending_chunks,
                            purged_parent_count = batch.purged_parent_ids.len(),
                            recovery_parent_count = batch.recovery_parent_ids.len(),
                            orphan_leaf_parent_count = batch.orphan_leaf_parent_ids.len(),
                            "sync worker: flushed pending batch during current_seq backoff"
                        );
                    }
                    warn!(error = %error, "sync worker: failed to fetch couchdb current sequence");
                    sleep(backoff).await;
                    backoff = next_backoff(backoff);
                    continue;
                }
            };

            match couch.poll_changes(&poll_since, poll_interval).await {
                Ok(feed) => {
                    let next_since = crate::livesync::sequence_to_string(&feed.last_seq);
                    if !next_since.is_empty() {
                        poll_since = next_since;
                    }

                    if !feed.results.is_empty() {
                        pending.extend(feed.results);
                        pending_current_seq = current_seq.clone();
                        last_event_at = Some(Instant::now());
                    }

                    if should_flush_pending(pending.len(), last_event_at, debounce_window) {
                        let (drained, current_seq) = flush_pending(
                            &mut pending,
                            &mut pending_current_seq,
                            &mut last_event_at,
                        );
                        let drained_count = drained.len();
                        let batch = ingest_changes_cooperatively(
                            &store,
                            &couch,
                            drained,
                            &current_seq,
                            context_window,
                            chunk_timeout,
                            decryptor.as_deref(),
                            max_changes_per_batch,
                        )
                        .await;
                        debug!(
                            input_changes = drained_count,
                            indexed = batch.indexed_notes,
                            deleted = batch.deleted_notes,
                            pending_chunks = batch.pending_chunks,
                            last_seq = ?batch.last_seq,
                            purged_parent_count = batch.purged_parent_ids.len(),
                            recovery_parent_count = batch.recovery_parent_ids.len(),
                            orphan_leaf_parent_count = batch.orphan_leaf_parent_ids.len(),
                            "sync worker: debounced batch applied"
                        );
                    }

                    if pending.is_empty() {
                        let batch = recover_stale_chunk_staging_cooperatively(
                            &store,
                            &couch,
                            &current_seq,
                            context_window,
                            chunk_timeout,
                            decryptor.as_deref(),
                            max_changes_per_batch,
                        )
                        .await;
                        if !batch.purged_parent_ids.is_empty() {
                            debug!(
                                indexed = batch.indexed_notes,
                                deleted = batch.deleted_notes,
                                pending_chunks = batch.pending_chunks,
                                purged_parent_count = batch.purged_parent_ids.len(),
                                recovery_parent_count = batch.recovery_parent_ids.len(),
                                orphan_leaf_parent_count = batch.orphan_leaf_parent_ids.len(),
                                "sync worker: idle chunk staging recovery completed"
                            );
                        }

                        if last_stale_file_alias_recovery_at.elapsed()
                            >= stale_file_alias_recovery_interval
                        {
                            last_stale_file_alias_recovery_at = Instant::now();
                            let batch = recover_stale_file_aliases_cooperatively(
                                &store,
                                &couch,
                                &current_seq,
                                context_window,
                                chunk_timeout,
                                decryptor.as_deref(),
                                max_changes_per_batch,
                            )
                            .await;
                            if batch.indexed_notes > 0 || batch.deleted_notes > 0 {
                                info!(
                                    indexed = batch.indexed_notes,
                                    deleted = batch.deleted_notes,
                                    pending_chunks = batch.pending_chunks,
                                    purged_parent_count = batch.purged_parent_ids.len(),
                                    recovery_parent_count = batch.recovery_parent_ids.len(),
                                    orphan_leaf_parent_count = batch.orphan_leaf_parent_ids.len(),
                                    "sync worker: idle stale file alias recovery completed"
                                );
                            }
                        }
                    }

                    backoff = Duration::from_secs(1);
                }
                Err(error) => {
                    if should_flush_pending(pending.len(), last_event_at, debounce_window) {
                        let (drained, current_seq) = flush_pending(
                            &mut pending,
                            &mut pending_current_seq,
                            &mut last_event_at,
                        );
                        let drained_count = drained.len();
                        let batch = ingest_changes_cooperatively(
                            &store,
                            &couch,
                            drained,
                            &current_seq,
                            context_window,
                            chunk_timeout,
                            decryptor.as_deref(),
                            max_changes_per_batch,
                        )
                        .await;
                        debug!(
                            input_changes = drained_count,
                            indexed = batch.indexed_notes,
                            deleted = batch.deleted_notes,
                            pending_chunks = batch.pending_chunks,
                            purged_parent_count = batch.purged_parent_ids.len(),
                            recovery_parent_count = batch.recovery_parent_ids.len(),
                            orphan_leaf_parent_count = batch.orphan_leaf_parent_ids.len(),
                            "sync worker: flushed pending batch after poll error"
                        );
                    }
                    warn!(error = %error, "sync worker: failed to poll _changes feed");
                    sleep(backoff).await;
                    backoff = next_backoff(backoff);
                }
            }
        }
    })))
}

async fn ingest_changes_cooperatively(
    store: &VaultStore,
    couch: &CouchDbClient,
    mut pending: Vec<ChangeEvent>,
    current_seq: &str,
    context_window: usize,
    chunk_timeout: Duration,
    decryptor: Option<&Decryptor>,
    max_changes_per_batch: usize,
) -> SyncBatchResult {
    let mut aggregate = SyncBatchResult {
        indexed_notes: 0,
        deleted_notes: 0,
        pending_chunks: 0,
        purged_parent_ids: Vec::new(),
        recovery_parent_ids: Vec::new(),
        orphan_leaf_parent_ids: Vec::new(),
        last_seq: None,
    };
    let mut seen_purged_parent_ids = HashSet::new();
    let mut seen_recovery_parent_ids = HashSet::new();
    let mut seen_orphan_leaf_parent_ids = HashSet::new();

    while !pending.is_empty() {
        let mut next_batch = take_change_batch(&mut pending, max_changes_per_batch);
        refresh_file_change_batch(couch, &mut next_batch).await;
        let batch = store
            .ingest_changes_batch(
                next_batch,
                current_seq,
                context_window,
                chunk_timeout,
                decryptor,
            )
            .await;

        aggregate.indexed_notes += batch.indexed_notes;
        aggregate.deleted_notes += batch.deleted_notes;
        aggregate.pending_chunks = batch.pending_chunks;
        if batch.last_seq.is_some() {
            aggregate.last_seq = batch.last_seq.clone();
        }

        for parent_id in &batch.purged_parent_ids {
            if seen_purged_parent_ids.insert(parent_id.clone()) {
                aggregate.purged_parent_ids.push(parent_id.clone());
            }
        }
        for parent_id in &batch.recovery_parent_ids {
            if seen_recovery_parent_ids.insert(parent_id.clone()) {
                aggregate.recovery_parent_ids.push(parent_id.clone());
            }
        }
        for parent_id in &batch.orphan_leaf_parent_ids {
            if seen_orphan_leaf_parent_ids.insert(parent_id.clone()) {
                aggregate.orphan_leaf_parent_ids.push(parent_id.clone());
            }
        }

        let recovered = fetch_parent_recovery_changes(couch, &batch.recovery_parent_ids).await;
        if !recovered.is_empty() {
            pending.extend(recovered);
        }

        if !pending.is_empty() {
            yield_now().await;
        }
    }

    aggregate
}

async fn recover_stale_chunk_staging_cooperatively(
    store: &VaultStore,
    couch: &CouchDbClient,
    current_seq: &str,
    context_window: usize,
    chunk_timeout: Duration,
    decryptor: Option<&Decryptor>,
    max_changes_per_batch: usize,
) -> SyncBatchResult {
    let purged = store
        .recover_stale_chunk_staging_at(chunk_timeout, Utc::now())
        .await;
    let mut aggregate = SyncBatchResult {
        indexed_notes: 0,
        deleted_notes: 0,
        pending_chunks: 0,
        purged_parent_ids: purged.purged_parent_ids,
        recovery_parent_ids: purged.recovery_parent_ids,
        orphan_leaf_parent_ids: purged.orphan_leaf_parent_ids,
        last_seq: None,
    };
    if aggregate.purged_parent_ids.is_empty() {
        return aggregate;
    }

    aggregate.pending_chunks = store.status().await.index.pending_chunks;
    warn!(
        purged_parent_count = aggregate.purged_parent_ids.len(),
        recovery_parent_count = aggregate.recovery_parent_ids.len(),
        orphan_leaf_parent_count = aggregate.orphan_leaf_parent_ids.len(),
        timeout_seconds = chunk_timeout.as_secs(),
        "sync worker: idle chunk staging recovery purged stale parents"
    );

    let recovered = fetch_parent_recovery_changes(couch, &aggregate.recovery_parent_ids).await;
    if recovered.is_empty() {
        return aggregate;
    }

    let replay = ingest_changes_cooperatively(
        store,
        couch,
        recovered,
        current_seq,
        context_window,
        chunk_timeout,
        decryptor,
        max_changes_per_batch,
    )
    .await;

    aggregate.indexed_notes += replay.indexed_notes;
    aggregate.deleted_notes += replay.deleted_notes;
    aggregate.pending_chunks = replay.pending_chunks;
    append_unique_ids(&mut aggregate.purged_parent_ids, replay.purged_parent_ids);
    append_unique_ids(
        &mut aggregate.recovery_parent_ids,
        replay.recovery_parent_ids,
    );
    append_unique_ids(
        &mut aggregate.orphan_leaf_parent_ids,
        replay.orphan_leaf_parent_ids,
    );
    if replay.last_seq.is_some() {
        aggregate.last_seq = replay.last_seq;
    }

    aggregate
}

async fn recover_stale_file_aliases_cooperatively(
    store: &VaultStore,
    couch: &CouchDbClient,
    current_seq: &str,
    context_window: usize,
    chunk_timeout: Duration,
    decryptor: Option<&Decryptor>,
    max_changes_per_batch: usize,
) -> SyncBatchResult {
    let stale_file_targets = store.stale_file_recovery_targets().await;
    let stale_file_alias_total = stale_file_targets.len();
    let stale_file_targets =
        take_stale_file_recovery_targets(stale_file_targets, STALE_FILE_RECOVERY_TARGET_LIMIT);
    let stale_file_doc_ids = stale_file_targets
        .iter()
        .map(|target| target.file_doc_id.clone())
        .collect::<Vec<_>>();
    let mut aggregate = SyncBatchResult {
        indexed_notes: 0,
        deleted_notes: 0,
        pending_chunks: 0,
        purged_parent_ids: Vec::new(),
        recovery_parent_ids: stale_file_doc_ids.clone(),
        orphan_leaf_parent_ids: Vec::new(),
        last_seq: None,
    };
    if stale_file_doc_ids.is_empty() {
        return aggregate;
    }

    let recovery_lookup_ids = recovery_lookup_ids_for_stale_file_targets(&stale_file_targets);
    let recovery_child_doc_ids = recovery_child_doc_ids_for_stale_file_targets(&stale_file_targets);
    warn!(
        stale_file_alias_total,
        stale_file_alias_count = stale_file_doc_ids.len(),
        stale_file_recovery_limit = STALE_FILE_RECOVERY_TARGET_LIMIT,
        recovery_lookup_count = recovery_lookup_ids.len(),
        recovery_child_count = recovery_child_doc_ids.len(),
        "sync worker: idle detected stale file aliases; queuing bounded parent recovery"
    );

    let mut recovered = fetch_parent_recovery_changes(couch, &recovery_lookup_ids).await;
    let child_recovered = fetch_exact_recovery_changes(couch, &recovery_child_doc_ids).await;
    append_missing_changes(&mut recovered, child_recovered);
    let path_recovered =
        fetch_path_scan_recovery_changes(couch, &stale_file_targets, decryptor).await;
    append_missing_changes(&mut recovered, path_recovered);
    if recovered.is_empty() {
        return aggregate;
    }

    let replay = ingest_changes_cooperatively(
        store,
        couch,
        recovered,
        current_seq,
        context_window,
        chunk_timeout,
        decryptor,
        max_changes_per_batch,
    )
    .await;

    aggregate.indexed_notes += replay.indexed_notes;
    aggregate.deleted_notes += replay.deleted_notes;
    aggregate.pending_chunks = replay.pending_chunks;
    append_unique_ids(&mut aggregate.purged_parent_ids, replay.purged_parent_ids);
    append_unique_ids(
        &mut aggregate.recovery_parent_ids,
        replay.recovery_parent_ids,
    );
    append_unique_ids(
        &mut aggregate.orphan_leaf_parent_ids,
        replay.orphan_leaf_parent_ids,
    );
    if replay.last_seq.is_some() {
        aggregate.last_seq = replay.last_seq;
    }

    aggregate
}

fn append_unique_ids(target: &mut Vec<String>, incoming: Vec<String>) {
    let mut seen = target.iter().cloned().collect::<HashSet<_>>();
    for id in incoming {
        if seen.insert(id.clone()) {
            target.push(id);
        }
    }
}

async fn refresh_file_change_batch(couch: &CouchDbClient, batch: &mut Vec<ChangeEvent>) {
    let parent_ids = file_change_parent_ids(batch);
    if parent_ids.is_empty() {
        return;
    }

    let recovered = fetch_parent_recovery_changes(couch, &parent_ids).await;
    if recovered.is_empty() {
        return;
    }

    let recovered_count = append_missing_changes(batch, recovered);
    if recovered_count > 0 {
        debug!(
            file_parent_count = parent_ids.len(),
            recovered_events = recovered_count,
            "sync worker: fetched current child docs for file changes"
        );
    }
}

fn take_change_batch<T>(pending: &mut Vec<T>, max_changes_per_batch: usize) -> Vec<T> {
    if pending.len() <= max_changes_per_batch {
        return std::mem::take(pending);
    }

    let tail = pending.split_off(max_changes_per_batch);
    std::mem::replace(pending, tail)
}

fn file_change_parent_ids(changes: &[ChangeEvent]) -> Vec<String> {
    let mut parent_ids = Vec::new();
    let mut seen_ids = HashSet::new();

    for change in changes {
        if is_deletion(change) {
            continue;
        }

        let Some(doc) = change.doc.clone() else {
            continue;
        };
        let Ok(LivesyncDocument::File(file)) = LivesyncDocument::try_from(doc) else {
            continue;
        };
        if file.deleted || !seen_ids.insert(file.id.clone()) {
            continue;
        }
        parent_ids.push(file.id);
    }

    parent_ids
}

async fn fetch_parent_recovery_changes(
    couch: &CouchDbClient,
    purged_parent_ids: &[String],
) -> Vec<ChangeEvent> {
    if purged_parent_ids.is_empty() {
        return Vec::new();
    }

    let mut recovered = Vec::new();
    let mut seen_ids = HashSet::new();
    for parent_id in purged_parent_ids {
        match couch.fetch_parent_recovery_changes(parent_id).await {
            Ok(events) => {
                for event in events {
                    if seen_ids.insert(event.id.clone()) {
                        recovered.push(event);
                    }
                }
            }
            Err(error) => warn!(
                recovery_lookup_hash = recovery_lookup_fingerprint(parent_id).as_str(),
                error = %error,
                "sync worker: failed to refetch stale chunk parent from couchdb"
            ),
        }
    }

    recovered
}

fn take_stale_file_recovery_targets(
    mut targets: Vec<StaleFileRecoveryTarget>,
    limit: usize,
) -> Vec<StaleFileRecoveryTarget> {
    targets.truncate(limit.max(1));
    targets
}

fn recovery_lookup_ids_for_stale_file_targets(targets: &[StaleFileRecoveryTarget]) -> Vec<String> {
    let mut lookup_ids = Vec::with_capacity(targets.len() * 2);
    for target in targets {
        lookup_ids.push(target.note_path.clone());
        lookup_ids.push(target.file_doc_id.clone());
    }
    let mut seen = HashSet::new();
    lookup_ids.retain(|id| seen.insert(id.clone()));
    lookup_ids
}

fn recovery_child_doc_ids_for_stale_file_targets(
    targets: &[StaleFileRecoveryTarget],
) -> Vec<String> {
    let mut child_doc_ids = Vec::new();
    for target in targets {
        child_doc_ids.extend(target.child_doc_ids.iter().cloned());
    }
    let mut seen = HashSet::new();
    child_doc_ids.retain(|id| seen.insert(id.clone()));
    child_doc_ids
}

fn recovery_lookup_fingerprint(value: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(value.trim().as_bytes());
    let digest = hex::encode(hasher.finalize());
    digest.chars().take(16).collect()
}

async fn detect_remote_stale_file_docs(store: &VaultStore, couch: &CouchDbClient) -> Vec<String> {
    let tracked_file_doc_revs = store.tracked_file_doc_revs().await;
    if tracked_file_doc_revs.is_empty() {
        return Vec::new();
    }

    let file_doc_ids = tracked_file_doc_revs
        .iter()
        .map(|(file_doc_id, _)| file_doc_id.clone())
        .collect::<Vec<_>>();
    let remote_revisions = match couch.fetch_document_revisions(&file_doc_ids).await {
        Ok(revisions) => revisions,
        Err(error) => {
            warn!(
                error = %error,
                tracked_file_doc_count = tracked_file_doc_revs.len(),
                "sync worker: failed to compare tracked file docs with remote revs"
            );
            return Vec::new();
        }
    };

    let mut stale_file_doc_ids = tracked_file_doc_revs
        .into_iter()
        .filter_map(|(file_doc_id, local_rev)| {
            remote_revisions
                .get(&file_doc_id)
                .filter(|remote_rev| **remote_rev != local_rev)
                .map(|_| file_doc_id)
        })
        .collect::<Vec<_>>();
    stale_file_doc_ids.sort();
    stale_file_doc_ids.dedup();
    stale_file_doc_ids
}

fn append_missing_changes(batch: &mut Vec<ChangeEvent>, recovered: Vec<ChangeEvent>) -> usize {
    let mut seen_ids = batch
        .iter()
        .map(|event| event.id.clone())
        .collect::<HashSet<_>>();
    let mut appended = 0;

    for event in recovered {
        if seen_ids.insert(event.id.clone()) {
            batch.push(event);
            appended += 1;
        }
    }

    appended
}

async fn queue_parent_recovery(
    couch: &CouchDbClient,
    purged_parent_ids: &[String],
    pending: &mut Vec<crate::livesync::ChangeEvent>,
    pending_current_seq: &mut String,
    last_event_at: &mut Option<Instant>,
    current_seq: &str,
) {
    let recovered = fetch_parent_recovery_changes(couch, purged_parent_ids).await;
    if recovered.is_empty() {
        return;
    }

    let recovered_count = recovered.len();
    pending.extend(recovered);
    *pending_current_seq = current_seq.to_string();
    *last_event_at = Some(Instant::now());
    debug!(
        recovered_events = recovered_count,
        purged_parent_count = purged_parent_ids.len(),
        "sync worker: queued refetched stale chunk parents for reprocessing"
    );
}

async fn fetch_exact_recovery_changes(
    couch: &CouchDbClient,
    document_ids: &[String],
) -> Vec<ChangeEvent> {
    if document_ids.is_empty() {
        return Vec::new();
    }

    match couch.fetch_documents_as_changes(document_ids).await {
        Ok(events) => events,
        Err(error) => {
            warn!(
                recovery_lookup_count = document_ids.len(),
                error = %error,
                "sync worker: failed to refetch exact recovery documents from couchdb"
            );
            Vec::new()
        }
    }
}

async fn queue_exact_recovery(
    couch: &CouchDbClient,
    document_ids: &[String],
    pending: &mut Vec<crate::livesync::ChangeEvent>,
    pending_current_seq: &mut String,
    last_event_at: &mut Option<Instant>,
    current_seq: &str,
) {
    let recovered = fetch_exact_recovery_changes(couch, document_ids).await;
    if recovered.is_empty() {
        return;
    }

    let recovered_count = recovered.len();
    append_missing_changes(pending, recovered);
    *pending_current_seq = current_seq.to_string();
    *last_event_at = Some(Instant::now());
    debug!(
        recovered_events = recovered_count,
        recovery_lookup_count = document_ids.len(),
        "sync worker: queued refetched exact recovery documents for reprocessing"
    );
}

async fn fetch_path_scan_recovery_changes(
    couch: &CouchDbClient,
    targets: &[StaleFileRecoveryTarget],
    decryptor: Option<&Decryptor>,
) -> Vec<ChangeEvent> {
    if targets.is_empty() {
        return Vec::new();
    }

    let mut note_paths = targets
        .iter()
        .map(|target| target.note_path.clone())
        .collect::<Vec<_>>();
    note_paths.sort();
    note_paths.dedup();

    match couch
        .find_file_document_changes_by_note_paths(&note_paths, decryptor)
        .await
    {
        Ok(events) => {
            if events.is_empty() {
                debug!(
                    stale_file_alias_count = targets.len(),
                    note_path_count = note_paths.len(),
                    "sync worker: stale alias path scan found no file documents"
                );
            } else {
                info!(
                    stale_file_alias_count = targets.len(),
                    note_path_count = note_paths.len(),
                    recovered_events = events.len(),
                    "sync worker: stale alias path scan recovered file documents"
                );
            }
            events
        }
        Err(error) => {
            warn!(
                stale_file_alias_count = targets.len(),
                note_path_count = note_paths.len(),
                error = %error,
                "sync worker: failed to scan file documents for stale alias recovery"
            );
            Vec::new()
        }
    }
}

async fn queue_path_scan_recovery(
    couch: &CouchDbClient,
    targets: &[StaleFileRecoveryTarget],
    decryptor: Option<&Decryptor>,
    pending: &mut Vec<crate::livesync::ChangeEvent>,
    pending_current_seq: &mut String,
    last_event_at: &mut Option<Instant>,
    current_seq: &str,
) {
    let recovered = fetch_path_scan_recovery_changes(couch, targets, decryptor).await;
    if recovered.is_empty() {
        return;
    }

    let recovered_count = append_missing_changes(pending, recovered);
    if recovered_count == 0 {
        return;
    }

    *pending_current_seq = current_seq.to_string();
    *last_event_at = Some(Instant::now());
    info!(
        stale_file_alias_count = targets.len(),
        recovered_events = recovered_count,
        "sync worker: queued scanned stale alias file documents for reprocessing"
    );
}

pub fn spawn_embedding_worker(store: VaultStore, config: &AppConfig) -> Option<JoinHandle<()>> {
    let embedding_config = config.embedding.clone();
    if embedding_config.mode == EmbeddingMode::Disabled {
        return None;
    }

    Some(tokio::spawn(async move {
        match embedding_config.mode {
            EmbeddingMode::Disabled => {}
            EmbeddingMode::Local => run_simulated_embedding_loop(store, embedding_config).await,
            EmbeddingMode::Localai => run_localai_embedding_loop(store, embedding_config).await,
        }
    }))
}

#[derive(Debug)]
struct ChunkedNoteEmbedding {
    embedding: Vec<f32>,
    chunk_count: usize,
    skipped_chunk_count: usize,
    max_chunk_bytes: usize,
    failed_chunk_count: usize,
}

#[derive(Debug, Default)]
struct BlockEmbeddingOutcome {
    updated: usize,
    failed: usize,
    provider_degraded: bool,
    provider_error: Option<String>,
}

fn normalize_localai_embedding_input(input: &str) -> String {
    let without_data_uris = DATA_URI_RE.replace_all(input, "[embedded-data]");
    OPAQUE_TOKEN_RE
        .replace_all(&without_data_uris, "[opaque-token]")
        .into_owned()
}

async fn embed_note_with_chunks(
    client: &LocalAiEmbeddingClient,
    note_text: &str,
    chunk_bytes: usize,
    request_batch_size: usize,
) -> Result<ChunkedNoteEmbedding, WorkerError> {
    const MAX_NOTE_EMBEDDING_CHUNKS: usize = 256;

    let chunks = split_note_for_localai(note_text, chunk_bytes.max(1));
    let total_chunk_count = chunks.len();
    let (chunks, skipped_chunk_count) =
        sample_note_chunks_for_embedding(chunks, MAX_NOTE_EMBEDDING_CHUNKS);
    let mut vectors = Vec::with_capacity(chunks.len());
    let mut failed_chunk_count = 0usize;
    let mut last_chunk_error = None;

    for chunk_batch in chunks.chunks(request_batch_size.max(1)) {
        let mut queue = vec![chunk_batch.to_vec()];
        while let Some(mut batch) = queue.pop() {
            if batch.is_empty() {
                continue;
            }

            match client.embed_batch(&batch).await {
                Ok(batch_vectors) if batch_vectors.len() == batch.len() => {
                    vectors.extend(batch.into_iter().zip(batch_vectors.into_iter()));
                }
                Ok(batch_vectors) => {
                    return Err(WorkerError::LocalAiBatchSizeMismatch {
                        expected: batch.len(),
                        got: batch_vectors.len(),
                    });
                }
                Err(error) if should_split_note_chunk_failure(&error, batch.len()) => {
                    let right = batch.split_off(batch.len() / 2);
                    queue.push(right);
                    queue.push(batch);
                }
                Err(error) if chunks.len() > 1 && error.may_be_payload_specific() => {
                    failed_chunk_count += 1;
                    last_chunk_error = Some(error.to_string());
                    warn!(
                        error = %error,
                        "embedding worker: skipping isolated failed note chunk"
                    );
                }
                Err(error) => return Err(error),
            }
        }
    }

    if vectors.is_empty() {
        return Err(WorkerError::LocalAiNoteChunksFailed {
            failed: failed_chunk_count,
            total: chunks.len(),
            last_error: last_chunk_error.unwrap_or_else(|| "unknown LocalAI error".to_string()),
        });
    }

    let (weights, embeddings): (Vec<_>, Vec<_>) = vectors
        .into_iter()
        .map(|(chunk, embedding)| (chunk.len(), embedding))
        .unzip();
    let embedding = aggregate_chunk_embeddings(&embeddings, &weights, client.dimensions)?;

    Ok(ChunkedNoteEmbedding {
        embedding,
        chunk_count: total_chunk_count,
        skipped_chunk_count,
        max_chunk_bytes: chunks.iter().map(|chunk| chunk.len()).max().unwrap_or(0),
        failed_chunk_count,
    })
}

fn should_split_note_chunk_failure(error: &WorkerError, batch_len: usize) -> bool {
    batch_len > 1 && error.may_be_payload_specific()
}

fn sample_note_chunks_for_embedding(
    chunks: Vec<String>,
    max_chunks: usize,
) -> (Vec<String>, usize) {
    let total = chunks.len();
    if max_chunks == 0 || total <= max_chunks {
        return (chunks, 0);
    }
    if max_chunks == 1 {
        return (
            chunks.into_iter().take(1).collect(),
            total.saturating_sub(1),
        );
    }

    let mut sampled = Vec::with_capacity(max_chunks);
    let mut previous = None;
    for slot in 0..max_chunks {
        let index = slot * (total - 1) / (max_chunks - 1);
        if previous == Some(index) {
            continue;
        }
        sampled.push(chunks[index].clone());
        previous = Some(index);
    }
    let skipped = total.saturating_sub(sampled.len());
    (sampled, skipped)
}

fn split_note_for_localai(text: &str, max_bytes: usize) -> Vec<String> {
    let limit = max_bytes.max(1);
    if text.len() <= limit {
        return vec![text.to_string()];
    }

    let mut chunks = Vec::new();
    let mut start = 0usize;

    while start < text.len() {
        let remaining = &text[start..];
        if remaining.len() <= limit {
            let tail = remaining.trim_end();
            if !tail.is_empty() || chunks.is_empty() {
                chunks.push(tail.to_string());
            }
            break;
        }

        let mut end = preferred_chunk_boundary(remaining, limit);
        if end == 0 {
            end = floor_char_boundary(remaining, limit);
        }
        if end == 0 {
            end = remaining
                .chars()
                .next()
                .map(|ch| ch.len_utf8())
                .unwrap_or(remaining.len());
        }

        let chunk = remaining[..end].trim_end();
        if !chunk.is_empty() {
            chunks.push(chunk.to_string());
        }

        start += end;
        while start < text.len() {
            let Some(ch) = text[start..].chars().next() else {
                break;
            };
            if !ch.is_whitespace() {
                break;
            }
            start += ch.len_utf8();
        }
    }

    if chunks.is_empty() {
        vec![String::new()]
    } else {
        chunks
    }
}

fn preferred_chunk_boundary(text: &str, max_bytes: usize) -> usize {
    let bounded = floor_char_boundary(text, max_bytes.min(text.len()));
    if bounded == 0 {
        return 0;
    }

    let min_boundary = bounded / 2;

    for (idx, ch) in text[..bounded].char_indices().rev() {
        if idx <= min_boundary {
            break;
        }
        if ch == '\n' {
            return idx;
        }
    }

    for (idx, ch) in text[..bounded].char_indices().rev() {
        if idx <= min_boundary {
            break;
        }
        if ch.is_whitespace() {
            return idx;
        }
    }

    bounded
}

fn floor_char_boundary(text: &str, idx: usize) -> usize {
    let mut boundary = idx.min(text.len());
    while boundary > 0 && !text.is_char_boundary(boundary) {
        boundary -= 1;
    }
    boundary
}

fn aggregate_chunk_embeddings(
    vectors: &[Vec<f32>],
    weights: &[usize],
    dimensions: usize,
) -> Result<Vec<f32>, WorkerError> {
    if vectors.len() != weights.len() {
        return Err(WorkerError::LocalAiBatchSizeMismatch {
            expected: weights.len(),
            got: vectors.len(),
        });
    }

    if vectors.is_empty() {
        return Ok(vec![0.0; dimensions]);
    }

    let mut aggregated = vec![0.0; dimensions];
    let mut total_weight = 0.0f32;

    for (embedding, weight) in vectors.iter().zip(weights.iter().copied()) {
        let effective_weight = weight.max(1) as f32;
        total_weight += effective_weight;
        for (slot, value) in aggregated.iter_mut().zip(embedding.iter().copied()) {
            *slot += value * effective_weight;
        }
    }

    if total_weight > 0.0 {
        for value in &mut aggregated {
            *value /= total_weight;
        }
    }

    Ok(aggregated)
}

async fn embed_block_batch_safely(
    client: &LocalAiEmbeddingClient,
    store: &VaultStore,
    pending_blocks: Vec<(String, String)>,
) -> BlockEmbeddingOutcome {
    let mut outcome = BlockEmbeddingOutcome::default();
    let mut queue = vec![pending_blocks];

    while let Some(mut batch) = queue.pop() {
        if batch.is_empty() {
            continue;
        }

        let inputs = batch
            .iter()
            .map(|(_, text)| text.clone())
            .collect::<Vec<_>>();

        match client.embed_batch(&inputs).await {
            Ok(vectors) if vectors.len() == batch.len() => {
                let updates = batch
                    .into_iter()
                    .zip(vectors.into_iter())
                    .map(|((id, _), vector)| (id, vector))
                    .collect::<Vec<_>>();
                outcome.updated += store.set_block_embeddings(updates).await;
            }
            Ok(vectors) => {
                let error = WorkerError::LocalAiBatchSizeMismatch {
                    expected: batch.len(),
                    got: vectors.len(),
                };
                warn!(error = %error, "embedding worker: LocalAI returned invalid block batch response");
                outcome.provider_degraded = true;
                outcome.provider_error = Some(error.to_string());
                break;
            }
            Err(error) => {
                let error_text = error.to_string();
                let payload_specific = if error.should_isolate_payload() {
                    true
                } else if error.may_be_payload_specific() {
                    match client.health_probe().await {
                        Ok(()) => {
                            store.record_embedding_provider_success().await;
                            true
                        }
                        Err(probe_error) => {
                            warn!(
                                block_error = %error_text,
                                probe_error = %probe_error,
                                "embedding worker: LocalAI health probe failed after block error"
                            );
                            outcome.provider_error = Some(format!(
                                "LocalAI health probe failed after block embedding error ({error_text}): {probe_error}"
                            ));
                            false
                        }
                    }
                } else {
                    false
                };

                if payload_specific && batch.len() > 1 {
                    warn!(
                        error = %error_text,
                        batch_len = batch.len(),
                        "embedding worker: LocalAI block batch failed, bisecting"
                    );
                    let right = batch.split_off(batch.len() / 2);
                    queue.push(right);
                    queue.push(batch);
                } else if payload_specific {
                    let block_id = batch[0].0.clone();
                    warn!(
                        block_id = block_id,
                        error = %error_text,
                        "embedding worker: LocalAI block request failed for isolated block"
                    );
                    store
                        .record_block_embedding_failure(&block_id, &error_text)
                        .await;
                    outcome.failed += 1;
                } else {
                    warn!(error = %error_text, "embedding worker: LocalAI block provider unavailable");
                    outcome.provider_degraded = true;
                    if outcome.provider_error.is_none() {
                        outcome.provider_error = Some(error_text);
                    }
                    break;
                }
            }
        }
    }

    outcome
}

async fn run_localai_embedding_loop(store: VaultStore, config: EmbeddingConfig) {
    let client = match LocalAiEmbeddingClient::new(&config) {
        Ok(client) => client,
        Err(error) => {
            warn!(error = %error, "embedding worker: failed to initialize LocalAI client");
            return;
        }
    };

    let poll_interval = config.poll_interval();
    let block_embedding_enabled = config.block_embedding_enabled;
    let note_chunk_bytes = config.note_chunk_bytes();
    let request_batch_size = config.batch_size.max(1);
    let max_failures = config.max_embedding_failures.max(1);
    let mut consecutive_all_fail = 0u32;
    let mut consecutive_block_provider_fail = 0u32;

    loop {
        // Priority 1: note embeddings (with breadcrumb prefix). Process notes
        // one by one so a single LocalAI failure cannot poison the whole pass.
        let pending = store
            .pending_embedding_batch(request_batch_size, max_failures)
            .await;
        if !pending.is_empty() {
            let batch_len = pending.len();
            let mut updated = 0usize;
            let mut failures = Vec::new();
            let mut last_failure_error = None;
            let mut chunked_notes = 0usize;
            let mut max_chunks = 1usize;
            let mut max_emitted_chunk_bytes = 0usize;

            for (note_id, content) in pending {
                match embed_note_with_chunks(
                    &client,
                    &content,
                    note_chunk_bytes,
                    request_batch_size,
                )
                .await
                {
                    Ok(chunked) => {
                        if chunked.chunk_count > 1 {
                            chunked_notes += 1;
                            max_chunks = max_chunks.max(chunked.chunk_count);
                            max_emitted_chunk_bytes =
                                max_emitted_chunk_bytes.max(chunked.max_chunk_bytes);
                        }
                        if chunked.failed_chunk_count > 0 {
                            warn!(
                                note_id = note_id.as_str(),
                                failed_chunks = chunked.failed_chunk_count,
                                total_chunks = chunked.chunk_count,
                                "embedding worker: note embedding used partial LocalAI chunks"
                            );
                        }
                        if chunked.skipped_chunk_count > 0 {
                            warn!(
                                note_id = note_id.as_str(),
                                embedded_chunks = chunked
                                    .chunk_count
                                    .saturating_sub(chunked.skipped_chunk_count),
                                skipped_chunks = chunked.skipped_chunk_count,
                                total_chunks = chunked.chunk_count,
                                "embedding worker: sampled oversized note for LocalAI"
                            );
                        }
                        updated += store
                            .set_embeddings(vec![(note_id, chunked.embedding)])
                            .await;
                    }
                    Err(error) => {
                        let error_text = error.to_string();
                        warn!(
                            note_id = note_id.as_str(),
                            error = %error_text,
                            "embedding worker: LocalAI note request failed"
                        );
                        last_failure_error = Some(error_text);
                        failures.push(note_id);
                    }
                }
            }

            // Record failures so notes are quarantined after max_failures.
            for note_id in &failures {
                store.record_embedding_failure(note_id).await;
            }

            if chunked_notes > 0 {
                info!(
                    chunked_notes,
                    max_chunks,
                    chunk_bytes_limit = note_chunk_bytes,
                    max_emitted_chunk_bytes,
                    "embedding worker: chunked oversized notes for LocalAI"
                );
            }

            if updated > 0 {
                store.record_embedding_provider_success().await;
                debug!(
                    updated,
                    "embedding worker: updated note embeddings from LocalAI"
                );
            }

            // Exponential backoff when every note in a batch fails (provider
            // likely down or overloaded). Reset on any partial success.
            if failures.len() == batch_len {
                if let Some(error) = last_failure_error.as_deref() {
                    store.record_embedding_provider_error(error).await;
                }
                consecutive_all_fail = consecutive_all_fail.saturating_add(1);
                let backoff_secs = poll_interval
                    .as_secs()
                    .saturating_mul(1 << consecutive_all_fail.min(6));
                let backoff = Duration::from_secs(backoff_secs.clamp(1, 300));
                warn!(
                    failed = failures.len(),
                    backoff_secs = backoff.as_secs(),
                    "embedding worker: all notes in batch failed, backing off"
                );
                sleep(backoff).await;
            } else {
                consecutive_all_fail = 0;
                sleep(poll_interval).await;
            }
            continue; // notes always take priority
        }

        // Priority 2: block embeddings.
        if block_embedding_enabled {
            let pending_blocks = store
                .pending_block_embedding_batch(config.batch_size, max_failures)
                .await;
            if !pending_blocks.is_empty() {
                let outcome = embed_block_batch_safely(&client, &store, pending_blocks).await;
                if outcome.updated > 0 || outcome.failed > 0 {
                    debug!(
                        updated = outcome.updated,
                        failed = outcome.failed,
                        "embedding worker: processed block embedding batch"
                    );
                }
                if outcome.provider_degraded {
                    if let Some(error) = outcome.provider_error.as_deref() {
                        store.record_embedding_provider_error(error).await;
                    }
                    consecutive_block_provider_fail =
                        consecutive_block_provider_fail.saturating_add(1);
                    let backoff_secs = poll_interval
                        .as_secs()
                        .saturating_mul(1 << consecutive_block_provider_fail.min(6));
                    let backoff = Duration::from_secs(backoff_secs.clamp(1, 300));
                    warn!(
                        backoff_secs = backoff.as_secs(),
                        "embedding worker: block embedding provider degraded, backing off"
                    );
                    sleep(backoff).await;
                } else {
                    if outcome.updated > 0 {
                        store.record_embedding_provider_success().await;
                    }
                    consecutive_block_provider_fail = 0;
                    sleep(poll_interval).await;
                }
                continue;
            }
        }

        sleep(poll_interval).await;
    }
}

async fn run_simulated_embedding_loop(store: VaultStore, config: EmbeddingConfig) {
    let poll_interval = config.poll_interval();
    let block_embedding_enabled = config.block_embedding_enabled;
    let dimensions = config.dimensions.max(1);
    let max_failures = config.max_embedding_failures.max(1);
    loop {
        // Priority 1: note embeddings.
        let updated = store
            .run_embedding_pass(config.batch_size, dimensions)
            .await;
        if updated > 0 {
            debug!(updated, "embedding worker: updated simulated embeddings");
            continue;
        }

        // Priority 2: block embeddings (simulated).
        if block_embedding_enabled {
            let pending_blocks = store
                .pending_block_embedding_batch(config.batch_size, max_failures)
                .await;
            if !pending_blocks.is_empty() {
                let updates: Vec<(String, Vec<f32>)> = pending_blocks
                    .into_iter()
                    .map(|(id, text)| {
                        let embedding = crate::search::embed_text(&text, dimensions);
                        (id, embedding)
                    })
                    .collect();
                let count = store.set_block_embeddings(updates).await;
                debug!(
                    count,
                    "embedding worker: updated simulated block embeddings"
                );
                continue;
            }
        }

        sleep(poll_interval).await;
    }
}

fn next_backoff(current: Duration) -> Duration {
    let doubled = current.as_secs().saturating_mul(2).clamp(1, 60);
    Duration::from_secs(doubled)
}

fn should_flush_pending(
    pending_len: usize,
    last_event_at: Option<Instant>,
    debounce_window: Duration,
) -> bool {
    pending_len > 0 && last_event_at.is_some_and(|instant| instant.elapsed() >= debounce_window)
}

#[derive(Debug, Clone)]
pub struct LocalAiEmbeddingClient {
    http: reqwest::Client,
    url: String,
    model: String,
    dimensions: usize,
    request_dimensions: bool,
}

impl LocalAiEmbeddingClient {
    pub fn new(config: &EmbeddingConfig) -> Result<Self, WorkerError> {
        let http = reqwest::Client::builder()
            .timeout(config.timeout())
            .user_agent("vault-bridge/0.1")
            .build()
            .map_err(WorkerError::HttpClientBuild)?;
        Ok(Self {
            http,
            url: config.localai_url().to_string(),
            model: config.localai_model().to_string(),
            dimensions: config.dimensions.max(1),
            request_dimensions: config.localai.request_dimensions,
        })
    }

    pub async fn embed_batch(&self, inputs: &[String]) -> Result<Vec<Vec<f32>>, WorkerError> {
        let normalized = inputs
            .iter()
            .map(|input| normalize_localai_embedding_input(input))
            .collect::<Vec<_>>();
        self.embed_batch_raw(&normalized).await
    }

    async fn embed_batch_raw(&self, inputs: &[String]) -> Result<Vec<Vec<f32>>, WorkerError> {
        if inputs.is_empty() {
            return Ok(Vec::new());
        }

        let mut body = json!({
            "model": self.model,
            "input": inputs,
        });
        if self.request_dimensions
            && let Some(map) = body.as_object_mut()
        {
            map.insert("dimensions".to_string(), json!(self.dimensions));
        }

        let response = self
            .http
            .post(&self.url)
            .json(&body)
            .send()
            .await?
            .error_for_status()?;

        let payload = response.json::<LocalAiEmbeddingResponse>().await?;
        parse_localai_embeddings(payload, self.dimensions)
    }

    pub async fn health_probe(&self) -> Result<(), WorkerError> {
        let inputs = [LOCALAI_HEALTH_PROBE_INPUT.to_string()];
        let vectors = self.embed_batch_raw(&inputs).await?;
        if vectors.len() == 1 {
            Ok(())
        } else {
            Err(WorkerError::LocalAiBatchSizeMismatch {
                expected: 1,
                got: vectors.len(),
            })
        }
    }
}

#[derive(Debug, Deserialize)]
struct LocalAiEmbeddingResponse {
    data: Vec<LocalAiEmbeddingData>,
}

#[derive(Debug, Deserialize)]
struct LocalAiEmbeddingData {
    embedding: Vec<f32>,
    #[serde(default)]
    index: Option<usize>,
}

fn parse_localai_embeddings(
    response: LocalAiEmbeddingResponse,
    expected_dimensions: usize,
) -> Result<Vec<Vec<f32>>, WorkerError> {
    let mut indexed = response
        .data
        .into_iter()
        .enumerate()
        .map(|(fallback_index, item)| {
            let index = item.index.unwrap_or(fallback_index);
            (index, item.embedding)
        })
        .collect::<Vec<_>>();

    indexed.sort_by_key(|(index, _)| *index);

    let mut deduped = HashMap::new();
    for (index, embedding) in indexed {
        if embedding.len() != expected_dimensions {
            return Err(WorkerError::InvalidEmbeddingDimensions {
                expected: expected_dimensions,
                got: embedding.len(),
            });
        }
        deduped.entry(index).or_insert(embedding);
    }

    let mut pairs = deduped.into_iter().collect::<Vec<_>>();
    pairs.sort_by_key(|(index, _)| *index);
    Ok(pairs.into_iter().map(|(_, embedding)| embedding).collect())
}

#[derive(Debug, Error)]
pub enum WorkerError {
    #[error(transparent)]
    CouchDb(#[from] CouchDbError),
    #[error("failed to build HTTP client: {0}")]
    HttpClientBuild(reqwest::Error),
    #[error("http error: {0}")]
    Http(#[from] reqwest::Error),
    #[error("json decode error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("invalid embedding dimensions (expected {expected}, got {got})")]
    InvalidEmbeddingDimensions { expected: usize, got: usize },
    #[error("LocalAI returned mismatched batch size (expected {expected}, got {got})")]
    LocalAiBatchSizeMismatch { expected: usize, got: usize },
    #[error("LocalAI failed all note chunks ({failed}/{total}); last error: {last_error}")]
    LocalAiNoteChunksFailed {
        failed: usize,
        total: usize,
        last_error: String,
    },
}

impl WorkerError {
    fn should_isolate_payload(&self) -> bool {
        match self {
            Self::Http(error) => error
                .status()
                .is_some_and(|status| status.is_client_error()),
            _ => false,
        }
    }

    fn may_be_payload_specific(&self) -> bool {
        match self {
            Self::Http(error) => error
                .status()
                .is_some_and(|status| status.is_client_error() || status.is_server_error()),
            _ => false,
        }
    }
}

#[cfg(test)]
pub(crate) fn parse_localai_embeddings_for_test(
    payload: serde_json::Value,
    expected_dimensions: usize,
) -> Result<Vec<Vec<f32>>, WorkerError> {
    let parsed: LocalAiEmbeddingResponse = serde_json::from_value(payload)?;
    parse_localai_embeddings(parsed, expected_dimensions)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use axum::body::Bytes;
    use axum::extract::{Path, Query, State};
    use axum::http::StatusCode;
    use axum::routing::{get, post};
    use axum::{Json, Router};
    use chrono::Utc;
    use serde::Deserialize;
    use tokio::sync::Mutex;
    use tokio::time::Instant;

    use super::{
        LocalAiEmbeddingClient, aggregate_chunk_embeddings, detect_remote_stale_file_docs,
        embed_note_with_chunks, ingest_changes_cooperatively, normalize_localai_embedding_input,
        parse_localai_embeddings_for_test, queue_parent_recovery,
        recover_stale_chunk_staging_cooperatively, recover_stale_file_aliases_cooperatively,
        sample_note_chunks_for_embedding, should_flush_pending, spawn_embedding_worker,
        split_note_for_localai, take_change_batch, take_stale_file_recovery_targets,
    };
    use crate::authorization::{AccessPolicy, AuthContext, ContextName};
    use crate::config::{
        AppConfig, CouchDbConfig, EmbeddingConfig, EmbeddingMode, FeedMode, LocalAiEmbeddingConfig,
    };
    use crate::couchdb::{CouchDbClient, build_livesync_note_documents};
    use crate::livesync::ChangeEvent;
    use crate::model::NoteId;
    use crate::store::{NoteInput, StaleFileRecoveryTarget, VaultStore};

    #[derive(Clone, Default)]
    struct MockCouchState {
        docs: Arc<HashMap<String, serde_json::Value>>,
        requested: Arc<Mutex<Vec<String>>>,
    }

    #[derive(Clone)]
    struct MockLocalAiState {
        max_input_bytes: usize,
        fail_substring: Option<String>,
        requests: Arc<AtomicUsize>,
        chunk_lengths: Arc<Mutex<Vec<usize>>>,
        requested_dimensions: Arc<Mutex<Vec<Option<u64>>>>,
    }

    async fn mock_get_document(
        State(state): State<MockCouchState>,
        Path((_db, doc_id)): Path<(String, String)>,
    ) -> (StatusCode, Json<serde_json::Value>) {
        state.requested.lock().await.push(doc_id.clone());
        if let Some(doc) = state.docs.get(&doc_id) {
            return (StatusCode::OK, Json(doc.clone()));
        }
        (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({"error": "not_found"})),
        )
    }

    #[derive(Debug, Deserialize)]
    struct MockAllDocsRequest {
        keys: Vec<String>,
        #[serde(default)]
        include_docs: bool,
    }

    async fn mock_all_docs(
        State(state): State<MockCouchState>,
        Json(request): Json<MockAllDocsRequest>,
    ) -> Json<serde_json::Value> {
        for key in &request.keys {
            state.requested.lock().await.push(key.clone());
        }

        let rows = request
            .keys
            .into_iter()
            .map(|key| {
                if let Some(doc) = state.docs.get(&key) {
                    let mut row = serde_json::json!({
                        "id": key,
                        "key": key,
                        "value": { "rev": doc.get("_rev").and_then(|value| value.as_str()).unwrap_or_default() }
                    });
                    if request.include_docs {
                        row["doc"] = doc.clone();
                    }
                    row
                } else {
                    serde_json::json!({
                        "key": key,
                        "error": "not_found"
                    })
                }
            })
            .collect::<Vec<_>>();
        Json(serde_json::json!({ "rows": rows }))
    }

    async fn mock_all_docs_scan(
        State(state): State<MockCouchState>,
        Query(query): Query<HashMap<String, String>>,
    ) -> Json<serde_json::Value> {
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

        let mut keys = state.docs.keys().cloned().collect::<Vec<_>>();
        keys.sort();
        let start_index = startkey
            .as_ref()
            .and_then(|start| keys.iter().position(|key| key == start))
            .map(|index| index.saturating_add(skip))
            .unwrap_or(0);

        let docs = keys
            .into_iter()
            .skip(start_index)
            .take(limit)
            .filter_map(|key| {
                let doc = state.docs.get(&key)?;
                let mut row = serde_json::json!({
                    "id": key,
                    "key": key,
                    "value": { "rev": doc.get("_rev").and_then(|value| value.as_str()).unwrap_or_default() }
                });
                if include_docs {
                    row["doc"] = doc.clone();
                }
                Some(row)
            })
            .collect::<Vec<_>>();

        Json(serde_json::json!({ "rows": docs }))
    }

    fn spawn_mock_couchdb(docs: HashMap<String, serde_json::Value>) -> (String, MockCouchState) {
        let state = MockCouchState {
            docs: Arc::new(docs),
            requested: Arc::new(Mutex::new(Vec::new())),
        };
        let app = Router::new()
            .route(
                "/{db}/_all_docs",
                get(mock_all_docs_scan).post(mock_all_docs),
            )
            .route("/{db}/{doc_id}", get(mock_get_document))
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

    fn couchdb_client(url: String) -> CouchDbClient {
        CouchDbClient::new(&CouchDbConfig {
            url,
            database: "mainvault".to_string(),
            username: "user".to_string(),
            password: "pass".to_string(),
            poll_interval_seconds: 1,
            feed_mode: FeedMode::Longpoll,
            ..Default::default()
        })
        .expect("build couchdb client")
    }

    async fn get_local_note(store: &VaultStore, note_path: &str) -> Option<crate::model::Note> {
        let mut config = AppConfig::default();
        config
            .contexts
            .insert("local".to_string(), AccessPolicy::default_agent());
        store.set_authorization_config(config.contexts).await;
        let auth = AuthContext::new(ContextName::new("local"), "test-principal".to_string());
        store
            .get_note_for_policy(&auth, &NoteId::new(note_path))
            .await
    }

    async fn mock_localai_embeddings(
        State(state): State<MockLocalAiState>,
        body: Bytes,
    ) -> (StatusCode, Json<serde_json::Value>) {
        let payload: serde_json::Value =
            serde_json::from_slice(&body).expect("parse mock localai body");
        state.requests.fetch_add(1, Ordering::SeqCst);

        let inputs = payload
            .get("input")
            .and_then(serde_json::Value::as_array)
            .cloned()
            .unwrap_or_default();
        let inputs = inputs
            .into_iter()
            .map(|value| {
                value
                    .as_str()
                    .expect("mock localai input should be a string")
                    .to_string()
            })
            .collect::<Vec<_>>();

        {
            let mut recorded = state.chunk_lengths.lock().await;
            recorded.extend(inputs.iter().map(|input| input.len()));
        }
        {
            let mut recorded = state.requested_dimensions.lock().await;
            recorded.push(
                payload
                    .get("dimensions")
                    .and_then(serde_json::Value::as_u64),
            );
        }

        if inputs
            .iter()
            .any(|input| input.len() > state.max_input_bytes)
            || state
                .fail_substring
                .as_ref()
                .is_some_and(|needle| inputs.iter().any(|input| input.contains(needle)))
        {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({
                    "error": "input_too_large",
                })),
            );
        }

        let data = inputs
            .iter()
            .enumerate()
            .map(|(index, input)| {
                serde_json::json!({
                    "index": index,
                    "embedding": [input.len() as f32, 1.0],
                })
            })
            .collect::<Vec<_>>();

        (
            StatusCode::OK,
            Json(serde_json::json!({
                "data": data,
            })),
        )
    }

    async fn spawn_mock_localai(max_input_bytes: usize) -> (String, MockLocalAiState) {
        spawn_mock_localai_with_options(max_input_bytes, None).await
    }

    async fn spawn_mock_localai_with_options(
        max_input_bytes: usize,
        fail_substring: Option<String>,
    ) -> (String, MockLocalAiState) {
        let state = MockLocalAiState {
            max_input_bytes,
            fail_substring,
            requests: Arc::new(AtomicUsize::new(0)),
            chunk_lengths: Arc::new(Mutex::new(Vec::new())),
            requested_dimensions: Arc::new(Mutex::new(Vec::new())),
        };
        let app = Router::new()
            .route("/v1/embeddings", post(mock_localai_embeddings))
            .with_state(state.clone());

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind mock localai");
        let addr = listener.local_addr().expect("mock localai addr");

        tokio::spawn(async move {
            axum::serve(listener, app)
                .await
                .expect("serve mock localai");
        });

        (format!("http://{addr}/v1/embeddings"), state)
    }

    async fn spawn_mock_localai_with_failure(
        max_input_bytes: usize,
        fail_substring: &str,
    ) -> (String, MockLocalAiState) {
        spawn_mock_localai_with_options(max_input_bytes, Some(fail_substring.to_string())).await
    }

    #[test]
    fn localai_embeddings_sort_by_index_and_validate_dimensions() {
        let payload = serde_json::json!({
            "data": [
                { "index": 1, "embedding": [0.0, 1.0] },
                { "index": 0, "embedding": [1.0, 0.0] }
            ]
        });

        let parsed = parse_localai_embeddings_for_test(payload, 2).expect("parse embeddings");
        assert_eq!(parsed, vec![vec![1.0, 0.0], vec![0.0, 1.0]]);
    }

    #[test]
    fn take_change_batch_preserves_order_and_leaves_tail_pending() {
        let mut pending = vec![1, 2, 3, 4, 5];

        let first = take_change_batch(&mut pending, 2);
        assert_eq!(first, vec![1, 2]);
        assert_eq!(pending, vec![3, 4, 5]);

        let rest = take_change_batch(&mut pending, 10);
        assert_eq!(rest, vec![3, 4, 5]);
        assert!(pending.is_empty());
    }

    #[test]
    fn stale_file_recovery_targets_are_bounded_without_reordering() {
        let targets = (0..5)
            .map(|index| StaleFileRecoveryTarget {
                file_doc_id: format!("f:{index}"),
                note_path: format!("note-{index}.md"),
                child_doc_ids: Vec::new(),
            })
            .collect();

        let selected = take_stale_file_recovery_targets(targets, 2);
        assert_eq!(selected.len(), 2);
        assert_eq!(selected[0].file_doc_id, "f:0");
        assert_eq!(selected[1].file_doc_id, "f:1");
    }

    #[test]
    fn localai_embeddings_dimension_mismatch_is_error() {
        let payload = serde_json::json!({
            "data": [
                { "index": 0, "embedding": [1.0, 0.0, 2.0] }
            ]
        });
        let error = parse_localai_embeddings_for_test(payload, 2).expect_err("should fail");
        assert!(error.to_string().contains("invalid embedding dimensions"));
    }

    #[tokio::test]
    async fn localai_embedding_client_sends_dimensions_when_configured() {
        let (mock_url, state) = spawn_mock_localai(1024).await;
        let config = EmbeddingConfig {
            mode: EmbeddingMode::Localai,
            localai: LocalAiEmbeddingConfig {
                url: mock_url,
                model: "nomic-embed-text".to_string(),
                request_dimensions: true,
            },
            dimensions: 2,
            ..EmbeddingConfig::default()
        };
        let client = LocalAiEmbeddingClient::new(&config).expect("build LocalAI client");

        client
            .embed_batch(&["dimension probe".to_string()])
            .await
            .expect("mock embedding request");

        let requested_dimensions = state.requested_dimensions.lock().await.clone();
        assert_eq!(requested_dimensions, vec![Some(2)]);
    }

    #[test]
    fn split_note_for_localai_respects_max_bytes() {
        let text = "folder > long-note\nThis is a deliberately long paragraph that should be split before it reaches the LocalAI backend hard limit.";
        let chunks = split_note_for_localai(text, 48);

        assert!(chunks.len() > 1);
        assert!(chunks.iter().all(|chunk| chunk.len() <= 48));
        assert_eq!(
            chunks.join(" ").split_whitespace().collect::<Vec<_>>(),
            text.split_whitespace().collect::<Vec<_>>()
        );
    }

    #[test]
    fn sample_note_chunks_for_embedding_preserves_coverage() {
        let chunks = (0..10)
            .map(|idx| format!("chunk-{idx}"))
            .collect::<Vec<_>>();

        let (sampled, skipped) = sample_note_chunks_for_embedding(chunks, 4);

        assert_eq!(sampled, vec!["chunk-0", "chunk-3", "chunk-6", "chunk-9"]);
        assert_eq!(skipped, 6);
    }

    #[test]
    fn localai_embedding_input_redacts_opaque_payloads() {
        let opaque_token = "a".repeat(220);
        let image_uri = format!("data:image/png;base64,{}", "b".repeat(220));
        let normalized = normalize_localai_embedding_input(&format!(
            "useful heading {opaque_token} useful tail {image_uri}"
        ));

        assert!(normalized.contains("useful heading"));
        assert!(normalized.contains("[opaque-token]"));
        assert!(normalized.contains("[embedded-data]"));
        assert!(!normalized.contains(&opaque_token));
        assert!(!normalized.contains(&image_uri));
    }

    #[tokio::test]
    async fn localai_note_embedding_skips_isolated_failed_chunk() {
        let (mock_url, state) = spawn_mock_localai_with_failure(1024, "BADCHUNK").await;
        let config = EmbeddingConfig {
            mode: EmbeddingMode::Localai,
            localai: LocalAiEmbeddingConfig {
                url: mock_url,
                model: "nomic-embed-text".to_string(),
                request_dimensions: false,
            },
            dimensions: 2,
            ..EmbeddingConfig::default()
        };
        let client = LocalAiEmbeddingClient::new(&config).expect("build LocalAI client");
        let text = "first useful sentence.\nBADCHUNK\nsecond useful sentence.";

        let embedded = embed_note_with_chunks(&client, text, 24, 8)
            .await
            .expect("partial note embedding should succeed");

        assert!(embedded.chunk_count > 1);
        assert_eq!(embedded.failed_chunk_count, 1);
        assert_eq!(embedded.embedding.len(), 2);
        assert!(state.requests.load(Ordering::SeqCst) > 1);
    }

    #[test]
    fn aggregate_chunk_embeddings_weights_by_chunk_size() {
        let aggregated = aggregate_chunk_embeddings(&[vec![1.0, 0.0], vec![0.0, 1.0]], &[3, 1], 2)
            .expect("aggregate chunk embeddings");
        assert_eq!(aggregated, vec![0.75, 0.25]);
    }

    #[test]
    fn should_flush_pending_requires_events() {
        assert!(!should_flush_pending(
            0,
            Some(Instant::now() - Duration::from_secs(10)),
            Duration::from_secs(5),
        ));
    }

    #[test]
    fn should_flush_pending_after_debounce_window() {
        assert!(should_flush_pending(
            2,
            Some(Instant::now() - Duration::from_secs(6)),
            Duration::from_secs(5),
        ));
    }

    #[test]
    fn should_not_flush_pending_before_debounce_window() {
        assert!(!should_flush_pending(
            2,
            Some(Instant::now() - Duration::from_secs(2)),
            Duration::from_secs(5),
        ));
    }

    #[tokio::test]
    async fn localai_embedding_worker_chunks_oversized_notes() {
        let (mock_url, state) = spawn_mock_localai(64).await;

        let mut config = AppConfig::default();
        config.embedding.mode = EmbeddingMode::Localai;
        config.embedding.localai = LocalAiEmbeddingConfig {
            url: mock_url,
            model: "nomic-embed-text".to_string(),
            request_dimensions: false,
        };
        config.embedding.dimensions = 2;
        config.embedding.batch_size = 2;
        config.embedding.poll_interval_seconds = 1;
        config.embedding.timeout_seconds = 5;
        config.embedding.note_chunk_bytes = 64;
        config.embedding.block_embedding_enabled = false;

        let store = VaultStore::new(20);
        let note_id = NoteId::new("03Concepts/chunked-localai.md");
        store
            .upsert_note(NoteInput {
                id: note_id.clone(),
                title: "chunked-localai".to_string(),
                content: "This note is intentionally much larger than the mock LocalAI limit so the worker has to split it into several bounded chunks before requesting embeddings from the provider.".to_string(),
                frontmatter: serde_json::json!({}),
                tags: vec![],
                couchdb_rev: "1-test".to_string(),
                created_at: Some(Utc::now()),
                updated_at: Utc::now(),
                embedding: None,
                links: vec![],
            })
            .await;

        let handle = spawn_embedding_worker(store.clone(), &config)
            .expect("localai embedding mode should start worker");
        let deadline = Instant::now() + Duration::from_secs(5);
        while Instant::now() < deadline {
            if store.pending_embedding_ids(10).await.is_empty() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        handle.abort();
        let _ = handle.await;

        assert!(
            store.pending_embedding_ids(10).await.is_empty(),
            "embedding worker should clear the pending oversized note"
        );

        let chunk_lengths = state.chunk_lengths.lock().await.clone();
        assert!(state.requests.load(Ordering::SeqCst) >= 2);
        assert!(chunk_lengths.len() > 1);
        assert!(chunk_lengths.iter().all(|len| *len <= 64));
    }

    #[tokio::test]
    async fn queue_parent_recovery_enqueues_refetched_docs_for_reprocessing() {
        let note_path = "11New/recovery-queue.md";
        let docs = build_livesync_note_documents(note_path, "# Recovery Queue");
        let mut file_doc = docs.file_doc.clone();
        let mut leaf_doc = docs.leaf_doc.clone();
        file_doc["_rev"] = serde_json::Value::String("1-file".to_string());
        leaf_doc["_rev"] = serde_json::Value::String("1-leaf".to_string());

        let mut server_docs = HashMap::new();
        server_docs.insert(docs.file_id.clone(), file_doc);
        server_docs.insert(docs.leaf_id.clone(), leaf_doc);

        let (url, _state) = spawn_mock_couchdb(server_docs);
        let couch = couchdb_client(url);

        let mut pending = Vec::new();
        let mut pending_seq = String::new();
        let mut last_event_at = None;
        queue_parent_recovery(
            &couch,
            &[note_path.to_string()],
            &mut pending,
            &mut pending_seq,
            &mut last_event_at,
            "42-g1AAA",
        )
        .await;

        let ids = pending
            .into_iter()
            .map(|event| event.id)
            .collect::<Vec<_>>();
        assert_eq!(ids.len(), 2);
        assert!(ids.contains(&docs.file_id));
        assert!(ids.contains(&docs.leaf_id));
        assert_eq!(pending_seq, "42-g1AAA");
        assert!(last_event_at.is_some());
    }

    #[tokio::test]
    async fn ingest_changes_cooperatively_does_not_refetch_orphan_leaf_after_timeout() {
        let store = VaultStore::new(20);
        let leaf_id = "h:+worker-orphan";
        let stale_at = Utc::now() - chrono::Duration::seconds(70);
        let leaf_doc = serde_json::json!({
            "_id": leaf_id,
            "_rev": "1-leaf",
            "type": "leaf",
            "e_": true,
            "data": "# Worker Orphan\n\nNo file metadata currently references this leaf."
        });

        let initial = store
            .ingest_changes_batch_at(
                vec![ChangeEvent {
                    seq: serde_json::Value::String("43-g1AAA".to_string()),
                    id: leaf_id.to_string(),
                    deleted: false,
                    doc: Some(leaf_doc.clone()),
                }],
                "43-g1AAA",
                250,
                Duration::from_secs(60),
                stale_at,
                None,
            )
            .await;
        assert_eq!(initial.pending_chunks, 1);
        assert_eq!(store.status().await.index.orphan_leaf_staging_count, 1);

        let mut server_docs = HashMap::new();
        server_docs.insert(leaf_id.to_string(), leaf_doc);
        let (url, state) = spawn_mock_couchdb(server_docs);
        let couch = couchdb_client(url);

        let batch = ingest_changes_cooperatively(
            &store,
            &couch,
            vec![ChangeEvent {
                seq: serde_json::Value::String("44-g1AAA".to_string()),
                id: "noop".to_string(),
                deleted: false,
                doc: Some(serde_json::json!({"_id": "noop", "type": "unknown"})),
            }],
            "44-g1AAA",
            250,
            Duration::from_secs(60),
            None,
            1,
        )
        .await;

        assert_eq!(batch.pending_chunks, 0);
        assert_eq!(batch.purged_parent_ids, vec![leaf_id.to_string()]);
        assert!(batch.recovery_parent_ids.is_empty());
        assert_eq!(batch.orphan_leaf_parent_ids, vec![leaf_id.to_string()]);
        assert_eq!(store.status().await.index.orphan_leaf_staging_count, 0);

        let requested = state.requested.lock().await.clone();
        assert!(
            !requested.iter().any(|requested_id| requested_id == leaf_id),
            "orphan leaf parent should not be refetched and restaged"
        );
    }

    #[tokio::test]
    async fn idle_chunk_recovery_purges_orphan_leaf_without_incoming_changes() {
        let store = VaultStore::new(20);
        let leaf_id = "h:+idle-worker-orphan";
        let stale_at = Utc::now() - chrono::Duration::seconds(70);
        let leaf_doc = serde_json::json!({
            "_id": leaf_id,
            "_rev": "1-leaf",
            "type": "leaf",
            "e_": true,
            "data": "# Idle Worker Orphan\n\nNo file metadata currently references this leaf."
        });

        let initial = store
            .ingest_changes_batch_at(
                vec![ChangeEvent {
                    seq: serde_json::Value::String("45-g1AAA".to_string()),
                    id: leaf_id.to_string(),
                    deleted: false,
                    doc: Some(leaf_doc.clone()),
                }],
                "45-g1AAA",
                250,
                Duration::from_secs(60),
                stale_at,
                None,
            )
            .await;
        assert_eq!(initial.pending_chunks, 1);
        assert_eq!(store.status().await.index.orphan_leaf_staging_count, 1);

        let mut server_docs = HashMap::new();
        server_docs.insert(leaf_id.to_string(), leaf_doc);
        let (url, state) = spawn_mock_couchdb(server_docs);
        let couch = couchdb_client(url);

        let batch = recover_stale_chunk_staging_cooperatively(
            &store,
            &couch,
            "46-g1AAA",
            250,
            Duration::from_secs(60),
            None,
            1,
        )
        .await;

        assert_eq!(batch.pending_chunks, 0);
        assert_eq!(batch.purged_parent_ids, vec![leaf_id.to_string()]);
        assert!(batch.recovery_parent_ids.is_empty());
        assert_eq!(batch.orphan_leaf_parent_ids, vec![leaf_id.to_string()]);
        assert_eq!(store.status().await.index.orphan_leaf_staging_count, 0);

        let requested = state.requested.lock().await.clone();
        assert!(
            !requested.iter().any(|requested_id| requested_id == leaf_id),
            "idle recovery should not refetch an orphan leaf parent"
        );
    }

    #[tokio::test]
    async fn idle_chunk_recovery_refetches_recoverable_parent_without_incoming_changes() {
        let store = VaultStore::new(20);
        let note_path = "11New/idle-recovery.md";
        let stale_at = Utc::now() - chrono::Duration::seconds(70);
        let stale_leaf_doc = serde_json::json!({
            "_id": "h:+idle-recoverable-stale",
            "_rev": "1-stale",
            "type": "leaf",
            "e_": true,
            "data": serde_json::json!({
                "parent_id": note_path,
                "chunk_index": 0,
                "chunk_count": 2,
                "content": "# stale partial"
            }).to_string()
        });

        let initial = store
            .ingest_changes_batch_at(
                vec![ChangeEvent {
                    seq: serde_json::Value::String("47-g1AAA".to_string()),
                    id: "h:+idle-recoverable-stale".to_string(),
                    deleted: false,
                    doc: Some(stale_leaf_doc),
                }],
                "47-g1AAA",
                250,
                Duration::from_secs(60),
                stale_at,
                None,
            )
            .await;
        assert_eq!(initial.pending_chunks, 1);

        let docs =
            build_livesync_note_documents(note_path, "# Idle Recovery\n\nRebuilt from CouchDB.");
        let mut file_doc = docs.file_doc.clone();
        let mut leaf_doc = docs.leaf_doc.clone();
        file_doc["_rev"] = serde_json::Value::String("2-file".to_string());
        leaf_doc["_rev"] = serde_json::Value::String("2-leaf".to_string());
        let mut server_docs = HashMap::new();
        server_docs.insert(docs.file_id.clone(), file_doc);
        server_docs.insert(docs.leaf_id.clone(), leaf_doc);
        let (url, state) = spawn_mock_couchdb(server_docs);
        let couch = couchdb_client(url);

        let batch = recover_stale_chunk_staging_cooperatively(
            &store,
            &couch,
            "48-g1AAA",
            250,
            Duration::from_secs(60),
            None,
            100,
        )
        .await;

        assert_eq!(batch.pending_chunks, 0);
        assert_eq!(batch.purged_parent_ids, vec![note_path.to_string()]);
        assert_eq!(batch.recovery_parent_ids, vec![note_path.to_string()]);
        assert!(batch.orphan_leaf_parent_ids.is_empty());
        assert_eq!(batch.indexed_notes, 1);

        let note = get_local_note(&store, note_path)
            .await
            .expect("idle recovery should re-index recovered note");
        assert!(note.content.contains("Rebuilt from CouchDB."));

        let requested = state.requested.lock().await.clone();
        assert!(
            requested
                .iter()
                .any(|requested_id| requested_id == &docs.file_id)
        );
        assert!(
            requested
                .iter()
                .any(|requested_id| requested_id == &docs.leaf_id)
        );
    }

    #[tokio::test]
    async fn idle_file_alias_recovery_repairs_missing_note_row_without_staged_chunks() {
        let store = VaultStore::new(20);
        let note_path = "Public Notes/synthetic-hyphen draft 5.md";
        let docs = build_livesync_note_documents(
            note_path,
            "# Synthetic Hyphen Draft 5\n\nRebuilt from a stale file alias.",
        );
        let mut file_doc = docs.file_doc.clone();
        let mut leaf_doc = docs.leaf_doc.clone();
        file_doc["_rev"] = serde_json::Value::String("2-file".to_string());
        leaf_doc["_rev"] = serde_json::Value::String("2-leaf".to_string());

        let stale_alias = store
            .ingest_changes_batch_at(
                vec![ChangeEvent {
                    seq: serde_json::Value::String("49-g1AAA".to_string()),
                    id: docs.file_id.clone(),
                    deleted: false,
                    doc: Some(file_doc.clone()),
                }],
                "49-g1AAA",
                250,
                Duration::from_secs(60),
                Utc::now(),
                None,
            )
            .await;
        assert_eq!(stale_alias.indexed_notes, 0);
        assert_eq!(stale_alias.pending_chunks, 0);
        assert_eq!(store.status().await.index.stale_file_aliases, 1);
        assert_eq!(
            store.stale_file_doc_ids_for_recovery().await,
            vec![docs.file_id.clone()]
        );
        assert!(
            get_local_note(&store, note_path).await.is_none(),
            "file-only ingest should leave the note row missing"
        );

        let mut server_docs = HashMap::new();
        server_docs.insert(docs.file_id.clone(), file_doc);
        server_docs.insert(docs.leaf_id.clone(), leaf_doc);
        let (url, state) = spawn_mock_couchdb(server_docs);
        let couch = couchdb_client(url);

        let recovered = recover_stale_file_aliases_cooperatively(
            &store,
            &couch,
            "50-g1AAA",
            250,
            Duration::from_secs(60),
            None,
            64,
        )
        .await;

        assert_eq!(recovered.indexed_notes, 1);
        assert_eq!(recovered.pending_chunks, 0);
        assert_eq!(store.status().await.index.stale_file_aliases, 0);
        assert!(store.stale_file_doc_ids_for_recovery().await.is_empty());

        let note = get_local_note(&store, note_path)
            .await
            .expect("idle stale alias recovery should materialize missing note row");
        assert_eq!(note.id, NoteId::new(note_path));
        assert!(note.content.contains("Rebuilt from a stale file alias."));

        let requested = state.requested.lock().await.clone();
        assert!(requested.contains(&docs.file_id));
        assert!(requested.contains(&docs.leaf_id));
    }

    #[tokio::test]
    async fn idle_file_alias_recovery_uses_note_path_when_stored_file_doc_is_missing() {
        let store = VaultStore::new(20);
        let note_path = "Public Notes/synthetic-rekeyed draft 5.md";
        let docs = build_livesync_note_documents(
            note_path,
            "# Synthetic Rekeyed Draft 5\n\nRecovered through the note path fallback.",
        );
        let stale_file_id = "f:stale-synthetic-rekeyed-draft-5";
        let mut stale_file_doc = docs.file_doc.clone();
        stale_file_doc["_id"] = serde_json::Value::String(stale_file_id.to_string());
        stale_file_doc["_rev"] = serde_json::Value::String("2-stale-file".to_string());

        let stale_alias = store
            .ingest_changes_batch_at(
                vec![ChangeEvent {
                    seq: serde_json::Value::String("49-g1AAA".to_string()),
                    id: stale_file_id.to_string(),
                    deleted: false,
                    doc: Some(stale_file_doc),
                }],
                "49-g1AAA",
                250,
                Duration::from_secs(60),
                Utc::now(),
                None,
            )
            .await;
        assert_eq!(stale_alias.indexed_notes, 0);
        assert_eq!(store.status().await.index.stale_file_aliases, 1);
        assert_eq!(
            store.stale_file_doc_ids_for_recovery().await,
            vec![stale_file_id.to_string()]
        );

        let mut file_doc = docs.file_doc.clone();
        let mut leaf_doc = docs.leaf_doc.clone();
        file_doc["_rev"] = serde_json::Value::String("3-current-file".to_string());
        leaf_doc["_rev"] = serde_json::Value::String("3-current-leaf".to_string());

        let mut server_docs = HashMap::new();
        server_docs.insert(docs.file_id.clone(), file_doc);
        server_docs.insert(docs.leaf_id.clone(), leaf_doc);
        let (url, state) = spawn_mock_couchdb(server_docs);
        let couch = couchdb_client(url);

        let recovered = recover_stale_file_aliases_cooperatively(
            &store,
            &couch,
            "50-g1AAA",
            250,
            Duration::from_secs(60),
            None,
            64,
        )
        .await;

        assert_eq!(recovered.indexed_notes, 1);
        assert_eq!(store.status().await.index.stale_file_aliases, 0);
        assert!(store.stale_file_doc_ids_for_recovery().await.is_empty());

        let note = get_local_note(&store, note_path)
            .await
            .expect("path fallback should materialize missing note row");
        assert_eq!(note.id, NoteId::new(note_path));
        assert!(note.content.contains("note path fallback"));

        let requested = state.requested.lock().await.clone();
        assert!(requested.contains(&docs.file_id));
        assert!(requested.contains(&docs.leaf_id));
    }

    #[tokio::test]
    async fn idle_file_alias_recovery_uses_persisted_children_when_file_doc_lookup_misses() {
        let store = VaultStore::new(20);
        let note_path = "Public Notes/synthetic-child-recovery draft 5.md";
        let docs = build_livesync_note_documents(
            note_path,
            "# Synthetic Child Recovery Draft 5\n\nRecovered through persisted child IDs.",
        );
        let mut file_doc = docs.file_doc.clone();
        let mut leaf_doc = docs.leaf_doc.clone();
        file_doc["_rev"] = serde_json::Value::String("2-file".to_string());
        leaf_doc["_rev"] = serde_json::Value::String("2-leaf".to_string());

        let stale_alias = store
            .ingest_changes_batch_at(
                vec![ChangeEvent {
                    seq: serde_json::Value::String("49-g1AAA".to_string()),
                    id: docs.file_id.clone(),
                    deleted: false,
                    doc: Some(file_doc),
                }],
                "49-g1AAA",
                250,
                Duration::from_secs(60),
                Utc::now(),
                None,
            )
            .await;
        assert_eq!(stale_alias.indexed_notes, 0);
        assert_eq!(
            store.stale_file_doc_ids_for_recovery().await,
            vec![docs.file_id.clone()]
        );

        let mut server_docs = HashMap::new();
        server_docs.insert(docs.leaf_id.clone(), leaf_doc);
        let (url, state) = spawn_mock_couchdb(server_docs);
        let couch = couchdb_client(url);

        let recovered = recover_stale_file_aliases_cooperatively(
            &store,
            &couch,
            "50-g1AAA",
            250,
            Duration::from_secs(60),
            None,
            64,
        )
        .await;

        assert_eq!(recovered.indexed_notes, 1);
        assert_eq!(store.status().await.index.stale_file_aliases, 0);
        assert!(store.stale_file_doc_ids_for_recovery().await.is_empty());

        let note = get_local_note(&store, note_path)
            .await
            .expect("persisted child fallback should materialize missing note row");
        assert_eq!(note.id, NoteId::new(note_path));
        assert!(note.content.contains("persisted child IDs"));

        let requested = state.requested.lock().await.clone();
        assert!(requested.contains(&docs.file_id));
        assert!(requested.contains(&docs.leaf_id));
    }

    #[tokio::test]
    async fn idle_file_alias_recovery_scans_by_path_when_alias_id_and_children_are_stale() {
        let store = VaultStore::new(20);
        let note_path = "Public Notes/synthetic-scan-recovery draft 5.md";
        let docs = build_livesync_note_documents(
            note_path,
            "# Synthetic Scan Recovery Draft 5\n\nRecovered through a path scan.",
        );
        let stale_file_id = "f:stale-synthetic-scan-recovery";
        let current_file_id = "f:current-synthetic-scan-recovery";
        let mut stale_file_doc = docs.file_doc.clone();
        stale_file_doc["_id"] = serde_json::Value::String(stale_file_id.to_string());
        stale_file_doc["_rev"] = serde_json::Value::String("2-stale-file".to_string());
        stale_file_doc["children"] = serde_json::json!(["h:stale-child-a", "h:stale-child-b"]);

        let stale_alias = store
            .ingest_changes_batch_at(
                vec![ChangeEvent {
                    seq: serde_json::Value::String("49-g1AAA".to_string()),
                    id: stale_file_id.to_string(),
                    deleted: false,
                    doc: Some(stale_file_doc),
                }],
                "49-g1AAA",
                250,
                Duration::from_secs(60),
                Utc::now(),
                None,
            )
            .await;
        assert_eq!(stale_alias.indexed_notes, 0);
        assert_eq!(
            store.stale_file_doc_ids_for_recovery().await,
            vec![stale_file_id.to_string()]
        );

        let mut current_file_doc = docs.file_doc.clone();
        let mut current_leaf_doc = docs.leaf_doc.clone();
        current_file_doc["_id"] = serde_json::Value::String(current_file_id.to_string());
        current_file_doc["_rev"] = serde_json::Value::String("3-current-file".to_string());
        current_leaf_doc["_rev"] = serde_json::Value::String("3-current-leaf".to_string());

        let mut server_docs = HashMap::new();
        server_docs.insert(current_file_id.to_string(), current_file_doc);
        server_docs.insert(docs.leaf_id.clone(), current_leaf_doc);
        let (url, state) = spawn_mock_couchdb(server_docs);
        let couch = couchdb_client(url);

        let recovered = recover_stale_file_aliases_cooperatively(
            &store,
            &couch,
            "50-g1AAA",
            250,
            Duration::from_secs(60),
            None,
            64,
        )
        .await;

        assert_eq!(recovered.indexed_notes, 1);
        assert_eq!(store.status().await.index.stale_file_aliases, 0);
        assert!(store.stale_file_doc_ids_for_recovery().await.is_empty());

        let note = get_local_note(&store, note_path)
            .await
            .expect("path scan fallback should materialize stale alias note");
        assert_eq!(note.id, NoteId::new(note_path));
        assert!(note.content.contains("path scan"));

        let requested = state.requested.lock().await.clone();
        assert!(requested.contains(&stale_file_id.to_string()));
        assert!(requested.contains(&current_file_id.to_string()));
        assert!(requested.contains(&docs.leaf_id));
    }

    #[tokio::test]
    async fn ingest_changes_cooperatively_refetches_current_child_docs_for_file_updates() {
        let store = VaultStore::new(20);
        let now = Utc::now();
        let note_path = "11New/file-refresh.md";

        let original_docs = build_livesync_note_documents(note_path, "# Prompt\n\n```\nold\n```");
        let mut original_file_doc = original_docs.file_doc.clone();
        let mut original_leaf_doc = original_docs.leaf_doc.clone();
        original_file_doc["_rev"] = serde_json::Value::String("1-file".to_string());
        original_leaf_doc["_rev"] = serde_json::Value::String("1-leaf".to_string());

        let indexed = store
            .ingest_changes_batch_at(
                vec![
                    ChangeEvent {
                        seq: serde_json::Value::String("50-g1AAA".to_string()),
                        id: original_docs.file_id.clone(),
                        deleted: false,
                        doc: Some(original_file_doc),
                    },
                    ChangeEvent {
                        seq: serde_json::Value::String("51-g1AAA".to_string()),
                        id: original_docs.leaf_id.clone(),
                        deleted: false,
                        doc: Some(original_leaf_doc),
                    },
                ],
                "51-g1AAA",
                250,
                Duration::from_secs(60),
                now,
                None,
            )
            .await;
        assert_eq!(indexed.indexed_notes, 1);
        let original = get_local_note(&store, note_path)
            .await
            .expect("original note should be indexed");
        assert!(!original.content.contains("asdf"));

        let refreshed_docs =
            build_livesync_note_documents(note_path, "# Prompt\n\n```\nold\nasdf\n```");
        let mut refreshed_file_doc = refreshed_docs.file_doc.clone();
        let mut refreshed_leaf_doc = refreshed_docs.leaf_doc.clone();
        refreshed_file_doc["_rev"] = serde_json::Value::String("2-file".to_string());
        refreshed_leaf_doc["_rev"] = serde_json::Value::String("2-leaf".to_string());

        let mut server_docs = HashMap::new();
        server_docs.insert(refreshed_docs.file_id.clone(), refreshed_file_doc.clone());
        server_docs.insert(refreshed_docs.leaf_id.clone(), refreshed_leaf_doc);

        let (url, state) = spawn_mock_couchdb(server_docs);
        let couch = couchdb_client(url);

        let refreshed = ingest_changes_cooperatively(
            &store,
            &couch,
            vec![ChangeEvent {
                seq: serde_json::Value::String("52-g1AAA".to_string()),
                id: refreshed_docs.file_id.clone(),
                deleted: false,
                doc: Some(refreshed_file_doc),
            }],
            "52-g1AAA",
            250,
            Duration::from_secs(60),
            None,
            1,
        )
        .await;

        assert_eq!(refreshed.indexed_notes, 1);
        let updated = get_local_note(&store, note_path)
            .await
            .expect("file refresh should keep note indexed");
        assert!(updated.content.contains("asdf"));

        let requested = state.requested.lock().await.clone();
        assert!(requested.contains(&refreshed_docs.file_id));
        assert!(requested.contains(&refreshed_docs.leaf_id));
    }

    #[tokio::test]
    async fn startup_recovery_refetches_stale_file_aliases() {
        let store = VaultStore::new(20);
        let now = Utc::now();
        let note_path = "11New/startup-recovery.md";

        let original_docs = build_livesync_note_documents(note_path, "# Prompt\n\n```\nold\n```");
        let mut original_file_doc = original_docs.file_doc.clone();
        let mut original_leaf_doc = original_docs.leaf_doc.clone();
        original_file_doc["_rev"] = serde_json::Value::String("1-file".to_string());
        original_leaf_doc["_rev"] = serde_json::Value::String("1-leaf".to_string());

        let indexed = store
            .ingest_changes_batch_at(
                vec![
                    ChangeEvent {
                        seq: serde_json::Value::String("60-g1AAA".to_string()),
                        id: original_docs.file_id.clone(),
                        deleted: false,
                        doc: Some(original_file_doc),
                    },
                    ChangeEvent {
                        seq: serde_json::Value::String("61-g1AAA".to_string()),
                        id: original_docs.leaf_id.clone(),
                        deleted: false,
                        doc: Some(original_leaf_doc),
                    },
                ],
                "61-g1AAA",
                250,
                Duration::from_secs(60),
                now,
                None,
            )
            .await;
        assert_eq!(indexed.indexed_notes, 1);
        assert!(store.stale_file_doc_ids_for_recovery().await.is_empty());

        let refreshed_docs =
            build_livesync_note_documents(note_path, "# Prompt\n\n```\nold\nasdf\n```");
        let mut refreshed_file_doc = refreshed_docs.file_doc.clone();
        let mut refreshed_leaf_doc = refreshed_docs.leaf_doc.clone();
        refreshed_file_doc["_rev"] = serde_json::Value::String("2-file".to_string());
        refreshed_leaf_doc["_rev"] = serde_json::Value::String("2-leaf".to_string());

        let stale = store
            .ingest_changes_batch_at(
                vec![ChangeEvent {
                    seq: serde_json::Value::String("62-g1AAA".to_string()),
                    id: refreshed_docs.file_id.clone(),
                    deleted: false,
                    doc: Some(refreshed_file_doc.clone()),
                }],
                "62-g1AAA",
                250,
                Duration::from_secs(60),
                now + chrono::Duration::seconds(1),
                None,
            )
            .await;
        assert_eq!(stale.indexed_notes, 0);

        let stale_note = get_local_note(&store, note_path)
            .await
            .expect("stale note should remain readable");
        assert!(!stale_note.content.contains("asdf"));
        assert_eq!(
            store.stale_file_doc_ids_for_recovery().await,
            vec![refreshed_docs.file_id.clone()]
        );

        let mut server_docs = HashMap::new();
        server_docs.insert(refreshed_docs.file_id.clone(), refreshed_file_doc);
        server_docs.insert(refreshed_docs.leaf_id.clone(), refreshed_leaf_doc);

        let (url, state) = spawn_mock_couchdb(server_docs);
        let couch = couchdb_client(url);

        let mut pending = Vec::new();
        let mut pending_seq = String::new();
        let mut last_event_at = None;
        let stale_file_doc_ids = store.stale_file_doc_ids_for_recovery().await;
        queue_parent_recovery(
            &couch,
            &stale_file_doc_ids,
            &mut pending,
            &mut pending_seq,
            &mut last_event_at,
            "63-g1AAA",
        )
        .await;

        let recovered = ingest_changes_cooperatively(
            &store,
            &couch,
            pending,
            "63-g1AAA",
            250,
            Duration::from_secs(60),
            None,
            64,
        )
        .await;

        assert_eq!(recovered.indexed_notes, 1);
        let repaired = get_local_note(&store, note_path)
            .await
            .expect("startup recovery should repair stale note");
        assert!(repaired.content.contains("asdf"));
        assert!(store.stale_file_doc_ids_for_recovery().await.is_empty());

        let requested = state.requested.lock().await.clone();
        assert!(requested.contains(&refreshed_docs.file_id));
        assert!(requested.contains(&refreshed_docs.leaf_id));
    }

    #[tokio::test]
    async fn detect_remote_stale_file_docs_finds_equal_local_rows_stuck_on_old_rev() {
        let store = VaultStore::new(20);
        let now = Utc::now();
        let note_path = "11New/remote-drift.md";

        let original_docs = build_livesync_note_documents(note_path, "# Prompt\n\n```\nold\n```");
        let mut original_file_doc = original_docs.file_doc.clone();
        let mut original_leaf_doc = original_docs.leaf_doc.clone();
        original_file_doc["_rev"] = serde_json::Value::String("1-file".to_string());
        original_leaf_doc["_rev"] = serde_json::Value::String("1-leaf".to_string());

        let indexed = store
            .ingest_changes_batch_at(
                vec![
                    ChangeEvent {
                        seq: serde_json::Value::String("70-g1AAA".to_string()),
                        id: original_docs.file_id.clone(),
                        deleted: false,
                        doc: Some(original_file_doc),
                    },
                    ChangeEvent {
                        seq: serde_json::Value::String("71-g1AAA".to_string()),
                        id: original_docs.leaf_id.clone(),
                        deleted: false,
                        doc: Some(original_leaf_doc),
                    },
                ],
                "71-g1AAA",
                250,
                Duration::from_secs(60),
                now,
                None,
            )
            .await;
        assert_eq!(indexed.indexed_notes, 1);
        assert!(store.stale_file_doc_ids_for_recovery().await.is_empty());

        let refreshed_docs =
            build_livesync_note_documents(note_path, "# Prompt\n\n```\nold\nasdf\n```");
        let mut refreshed_file_doc = refreshed_docs.file_doc.clone();
        let mut refreshed_leaf_doc = refreshed_docs.leaf_doc.clone();
        refreshed_file_doc["_rev"] = serde_json::Value::String("2-file".to_string());
        refreshed_leaf_doc["_rev"] = serde_json::Value::String("2-leaf".to_string());

        let mut server_docs = HashMap::new();
        server_docs.insert(refreshed_docs.file_id.clone(), refreshed_file_doc.clone());
        server_docs.insert(refreshed_docs.leaf_id.clone(), refreshed_leaf_doc);

        let (url, state) = spawn_mock_couchdb(server_docs);
        let couch = couchdb_client(url);

        assert_eq!(
            detect_remote_stale_file_docs(&store, &couch).await,
            vec![refreshed_docs.file_id.clone()]
        );

        let mut pending = Vec::new();
        let mut pending_seq = String::new();
        let mut last_event_at = None;
        queue_parent_recovery(
            &couch,
            &[refreshed_docs.file_id.clone()],
            &mut pending,
            &mut pending_seq,
            &mut last_event_at,
            "72-g1AAA",
        )
        .await;

        let recovered = ingest_changes_cooperatively(
            &store,
            &couch,
            pending,
            "72-g1AAA",
            250,
            Duration::from_secs(60),
            None,
            64,
        )
        .await;

        assert_eq!(recovered.indexed_notes, 1);
        let repaired = get_local_note(&store, note_path)
            .await
            .expect("remote drift recovery should repair stale note");
        assert!(repaired.content.contains("asdf"));
        assert!(store.stale_file_doc_ids_for_recovery().await.is_empty());

        let requested = state.requested.lock().await.clone();
        assert!(requested.contains(&refreshed_docs.file_id));
        assert!(requested.contains(&refreshed_docs.leaf_id));
    }

    #[tokio::test]
    async fn ingest_handles_shared_leaf_ids_across_multiple_notes() {
        let store = VaultStore::new(20);

        let note_a = "00New/shared-leaf-a.md";
        let note_b = "00New/shared-leaf-b.md";
        let file_a = "f:shared-a";
        let file_b = "f:shared-b";
        let shared_leaf = "h:+shared";
        let leaf_a = "h:+leaf-a";
        let leaf_b = "h:+leaf-b";

        let file_a_doc = serde_json::json!({
            "_id": file_a,
            "_rev": "1-a",
            "children": [shared_leaf, leaf_a],
            "path": note_a,
            "ctime": 0,
            "mtime": 0,
            "size": 0,
            "type": "plain",
            "eden": {}
        });
        let file_b_doc = serde_json::json!({
            "_id": file_b,
            "_rev": "1-b",
            "children": [shared_leaf, leaf_b],
            "path": note_b,
            "ctime": 0,
            "mtime": 0,
            "size": 0,
            "type": "plain",
            "eden": {}
        });
        let shared_leaf_doc = serde_json::json!({
            "_id": shared_leaf,
            "_rev": "1-shared",
            "data": "Shared prefix\n",
            "type": "leaf",
            "e_": false
        });
        let leaf_a_doc = serde_json::json!({
            "_id": leaf_a,
            "_rev": "1-leaf-a",
            "data": "Alpha tail\n",
            "type": "leaf",
            "e_": false
        });
        let leaf_b_doc = serde_json::json!({
            "_id": leaf_b,
            "_rev": "1-leaf-b",
            "data": "Beta tail\n",
            "type": "leaf",
            "e_": false
        });

        let changes = vec![
            ChangeEvent {
                seq: serde_json::Value::String("100-g1AAA".to_string()),
                id: file_a.to_string(),
                deleted: false,
                doc: Some(file_a_doc.clone()),
            },
            ChangeEvent {
                seq: serde_json::Value::String("101-g1AAA".to_string()),
                id: file_b.to_string(),
                deleted: false,
                doc: Some(file_b_doc.clone()),
            },
            ChangeEvent {
                seq: serde_json::Value::String("102-g1AAA".to_string()),
                id: shared_leaf.to_string(),
                deleted: false,
                doc: Some(shared_leaf_doc.clone()),
            },
            ChangeEvent {
                seq: serde_json::Value::String("103-g1AAA".to_string()),
                id: leaf_a.to_string(),
                deleted: false,
                doc: Some(leaf_a_doc.clone()),
            },
            ChangeEvent {
                seq: serde_json::Value::String("104-g1AAA".to_string()),
                id: leaf_b.to_string(),
                deleted: false,
                doc: Some(leaf_b_doc.clone()),
            },
        ];

        let mut server_docs = HashMap::new();
        server_docs.insert(file_a.to_string(), file_a_doc);
        server_docs.insert(file_b.to_string(), file_b_doc);
        server_docs.insert(shared_leaf.to_string(), shared_leaf_doc);
        server_docs.insert(leaf_a.to_string(), leaf_a_doc);
        server_docs.insert(leaf_b.to_string(), leaf_b_doc);

        let (url, _state) = spawn_mock_couchdb(server_docs);
        let couch = couchdb_client(url);

        let result = ingest_changes_cooperatively(
            &store,
            &couch,
            changes,
            "104-g1AAA",
            250,
            Duration::from_secs(60),
            None,
            64,
        )
        .await;

        assert_eq!(result.indexed_notes, 2);

        let indexed_a = get_local_note(&store, note_a)
            .await
            .expect("note A should be indexed");
        assert!(indexed_a.content.contains("Shared prefix"));
        assert!(indexed_a.content.contains("Alpha tail"));

        let indexed_b = get_local_note(&store, note_b)
            .await
            .expect("note B should be indexed");
        assert!(indexed_b.content.contains("Shared prefix"));
        assert!(indexed_b.content.contains("Beta tail"));
    }
}

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use serde_json::json;

use vault_bridge::authorization::{AccessPolicy, AuthContext, ContextName};
use vault_bridge::config::DatabaseConfig;
use vault_bridge::livesync::ChangeEvent;
use vault_bridge::model::NoteId;
use vault_bridge::new_note::{
    ContentPatchOperation, NewNoteFileType, NewNoteRequest, UpdateNoteRequest, WriteError,
};
use vault_bridge::persistence::{PostgresPersistence, RecoveryChildDiagnosis};
use vault_bridge::store::{VaultFileVisibility, VaultStore};

fn test_database_url() -> Option<String> {
    let url = std::env::var("VAULT_BRIDGE_TEST_DATABASE_URL").ok()?;
    let database_name = url.rsplit('/').next().unwrap_or_default();
    assert!(
        database_name.contains("test"),
        "VAULT_BRIDGE_TEST_DATABASE_URL must name a test database"
    );
    Some(url)
}

async fn hydrated_store(persistence: Arc<PostgresPersistence>) -> VaultStore {
    let store = VaultStore::new_with_persistence(20, persistence);
    store
        .hydrate_from_persistence()
        .await
        .expect("hydrate store");
    let mut contexts = BTreeMap::new();
    contexts.insert("admin".to_string(), AccessPolicy::admin());
    store.set_authorization_config(contexts).await;
    store
}

fn admin_auth() -> AuthContext {
    AuthContext::new(ContextName::new("admin"), "test:admin".to_string())
}

#[tokio::test]
async fn direct_vault_writes_survive_reload_and_refresh_other_processes() {
    let Some(database_url) = test_database_url() else {
        eprintln!(
            "skipping postgres vault-file persistence test; set VAULT_BRIDGE_TEST_DATABASE_URL"
        );
        return;
    };
    let persistence = Arc::new(
        PostgresPersistence::connect_and_migrate(
            &DatabaseConfig {
                url: database_url,
                max_connections: 5,
            },
            64,
        )
        .await
        .expect("connect and migrate test postgres"),
    );
    sqlx::query(
        "TRUNCATE TABLE access_log, api_keys, links, tags, blocks, notes, vault_files, sync_state, store_state, sync_recovery_queue, chunk_staging, file_aliases RESTART IDENTITY CASCADE",
    )
    .execute(persistence.pool())
    .await
    .expect("reset test postgres");

    let store_a = hydrated_store(persistence.clone()).await;
    let now = Utc::now();
    let created = store_a
        .create_vault_file_at(
            NewNoteRequest {
                title: "Persistent Raw Markdown".to_string(),
                content: "---\ntags: [durable]\nstatus: draft\n---\n\n# Persistent Raw Markdown\n\nOriginal body.\n"
                    .to_string(),
                template_id: None,
                file_type: NewNoteFileType::Md,
            },
            now,
        )
        .await
        .expect("create markdown");
    let markdown_id = created.id;

    let store_b = hydrated_store(persistence.clone()).await;
    let auth = admin_auth();
    let raw = store_b
        .get_vault_file_for_policy(&auth, &markdown_id)
        .await
        .expect("raw markdown after fresh hydration");
    assert!(raw.content.contains("Original body."));
    assert!(
        store_b
            .get_note_for_policy(&auth, &markdown_id)
            .await
            .is_some()
    );

    store_a
        .update_note_at(
            &markdown_id,
            &UpdateNoteRequest {
                content: None,
                content_patch: Some(vec![ContentPatchOperation::Replace {
                    old: "Original body.".to_string(),
                    new: "Updated through note API.".to_string(),
                }]),
                tags: None,
                metadata: Some(json!({"status": "reviewed"})),
            },
            now + chrono::Duration::seconds(1),
        )
        .await
        .expect("update note");

    let refreshed_raw = store_b
        .get_vault_file_for_policy(&auth, &markdown_id)
        .await
        .expect("other store refreshes updated raw markdown");
    assert!(refreshed_raw.content.contains("Updated through note API."));
    let refreshed_note = store_b
        .get_note_for_policy(&auth, &markdown_id)
        .await
        .expect("other store refreshes updated note");
    assert_eq!(refreshed_note.frontmatter["status"], "reviewed");

    store_a
        .edit_vault_file(
            &auth,
            &markdown_id,
            UpdateNoteRequest {
                content: None,
                content_patch: Some(vec![ContentPatchOperation::Replace {
                    old: "Updated through note API.".to_string(),
                    new: "Updated through raw API.".to_string(),
                }]),
                tags: Some(vec!["durable".to_string(), "raw-edited".to_string()]),
                metadata: Some(json!({"status": "done"})),
            },
            now + chrono::Duration::seconds(2),
        )
        .await
        .expect("edit raw markdown");

    let store_c = hydrated_store(persistence.clone()).await;
    let raw = store_c
        .get_vault_file_for_policy(&auth, &markdown_id)
        .await
        .expect("raw edit after fresh hydration");
    assert!(raw.content.contains("Updated through raw API."));
    let note = store_c
        .get_note_for_policy(&auth, &markdown_id)
        .await
        .expect("indexed edit after fresh hydration");
    assert_eq!(note.frontmatter["status"], "done");
    assert!(note.tags.iter().any(|tag| tag == "raw-edited"));

    let base = store_a
        .create_vault_file_at(
            NewNoteRequest {
                title: "Persistent Dashboard".to_string(),
                content: "views:\n  - type: table\n    name: Initial\n".to_string(),
                template_id: None,
                file_type: NewNoteFileType::Base,
            },
            now + chrono::Duration::seconds(3),
        )
        .await
        .expect("create base file");
    let base_id = base.id;
    let base_raw = store_c
        .get_vault_file_for_policy(&auth, &base_id)
        .await
        .expect("other store refreshes base create");
    assert!(base_raw.content.contains("name: Initial"));

    store_a
        .edit_vault_file(
            &auth,
            &base_id,
            UpdateNoteRequest {
                content: None,
                content_patch: Some(vec![ContentPatchOperation::Replace {
                    old: "name: Initial".to_string(),
                    new: "name: Updated".to_string(),
                }]),
                tags: None,
                metadata: None,
            },
            now + chrono::Duration::seconds(4),
        )
        .await
        .expect("edit base file");
    let store_d = hydrated_store(persistence.clone()).await;
    let base_raw = store_d
        .get_vault_file_for_policy(&auth, &base_id)
        .await
        .expect("base edit after fresh hydration");
    assert!(base_raw.content.contains("name: Updated"));

    sqlx::query(
        "INSERT INTO file_aliases (file_doc_id, note_path, couchdb_rev, children, ctime, mtime) VALUES ('base-file-doc', $1, 'local-edit-file', ARRAY[]::TEXT[], 0, 0)",
    )
    .bind(base_id.as_str())
    .execute(persistence.pool())
    .await
    .expect("insert matching base alias");
    assert_eq!(
        persistence
            .stale_file_alias_count()
            .await
            .expect("count matching base alias"),
        0,
        "a base alias does not require a Markdown note row"
    );
    persistence
        .delete_vault_file(base_id.as_str())
        .await
        .expect("remove base raw row");
    assert_eq!(
        persistence
            .stale_file_alias_count()
            .await
            .expect("count base alias missing raw row"),
        1
    );

    persistence
        .delete_vault_file(markdown_id.as_str())
        .await
        .expect("remove raw row to test diagnostics");
    let store_e = hydrated_store(persistence.clone()).await;
    assert_eq!(
        store_e
            .vault_file_visibility_for_policy(&auth, &NoteId::new(markdown_id.as_str()))
            .await,
        VaultFileVisibility::MissingRawWithIndexedNote
    );

    sqlx::query(
        "INSERT INTO file_aliases (file_doc_id, note_path, couchdb_rev, children, ctime, mtime) VALUES ('deleted-file-doc', $1, 'local-edit-file', ARRAY['h:deleted-child']::TEXT[], 0, 0)",
    )
    .bind(markdown_id.as_str())
    .execute(persistence.pool())
    .await
    .expect("insert alias before confirmed deletion");
    sqlx::query(
        "INSERT INTO chunk_staging (parent_id, chunk_index, chunk_count, content, couchdb_rev) VALUES ('h:deleted-child', 0, 2, 'partial', 'local-edit-file')",
    )
    .execute(persistence.pool())
    .await
    .expect("insert staged child before confirmed deletion");
    sqlx::query(
        "INSERT INTO chunk_staging (parent_id, chunk_index, chunk_count, content, couchdb_rev) VALUES ($1, 0, 2, 'current partial', 'remote-new')",
    )
    .bind(markdown_id.as_str())
    .execute(persistence.pool())
    .await
    .expect("insert current-generation staged child");
    let source_diagnostic = persistence
        .note_source_diagnostic(markdown_id.as_str(), Some("remote-new"))
        .await
        .expect("load local source diagnostic");
    assert_eq!(source_diagnostic.index_state, "stale");
    assert_eq!(source_diagnostic.raw_file_state, "missing");
    assert_eq!(source_diagnostic.staged_current_child_count, 1);
    assert_eq!(source_diagnostic.aliases.len(), 1);
    assert_eq!(source_diagnostic.aliases[0].expected_child_count, 1);

    persistence
        .apply_confirmed_vault_file_deletion(markdown_id.as_str())
        .await
        .expect("persist confirmed source deletion");
    let deleted_snapshot = persistence
        .load_snapshot()
        .await
        .expect("load snapshot after confirmed deletion");
    assert!(
        deleted_snapshot
            .notes
            .iter()
            .all(|note| note.id != markdown_id.as_str())
    );
    assert!(
        deleted_snapshot
            .file_aliases
            .iter()
            .all(|alias| alias.note_path != markdown_id.as_str())
    );
    assert!(
        deleted_snapshot
            .staged_chunks
            .iter()
            .all(|chunk| chunk.parent_id != "h:deleted-child")
    );

    let recovery_targets = vec!["parent-a".to_string(), "parent-b".to_string()];
    persistence
        .enqueue_recovery_targets("chunk_parent", &recovery_targets)
        .await
        .expect("enqueue recovery targets");
    let due = persistence
        .due_recovery_targets(10, Utc::now())
        .await
        .expect("load due recovery targets");
    assert_eq!(due.len(), 2);
    assert!(
        !persistence
            .fail_recovery_target(
                "chunk_parent",
                "parent-a",
                Utc::now() + chrono::Duration::seconds(30),
                2,
                "incomplete_source",
                None,
            )
            .await
            .expect("defer first recovery failure")
    );
    assert!(
        persistence
            .fail_recovery_target(
                "chunk_parent",
                "parent-a",
                Utc::now() + chrono::Duration::seconds(60),
                2,
                "incomplete_source",
                None,
            )
            .await
            .expect("quarantine repeated recovery failure")
    );
    persistence
        .resolve_recovery_target("chunk_parent", "parent-b")
        .await
        .expect("resolve recovery target");
    let recovery_stats = persistence
        .recovery_queue_stats()
        .await
        .expect("read recovery queue stats");
    assert_eq!(recovery_stats.pending, 0);
    assert_eq!(recovery_stats.quarantined, 1);

    persistence
        .enqueue_recovery_targets("file_alias", &["deleted-file-doc".to_string()])
        .await
        .expect("enqueue unavailable-child alias");
    persistence
        .fail_recovery_target(
            "file_alias",
            "deleted-file-doc",
            Utc::now() + chrono::Duration::seconds(30),
            5,
            "mixed_unavailable_children",
            Some(&RecoveryChildDiagnosis {
                expected: 4,
                live: 2,
                missing: 1,
                tombstoned: 1,
            }),
        )
        .await
        .expect("persist unavailable-child diagnosis");
    let diagnosed_stats = persistence
        .recovery_queue_stats()
        .await
        .expect("read diagnosed recovery stats");
    assert_eq!(diagnosed_stats.aliases_blocked_by_unavailable_children, 1);
    assert_eq!(diagnosed_stats.missing_children, 1);
    assert_eq!(diagnosed_stats.tombstoned_children, 1);

    sqlx::query(
        "INSERT INTO chunk_staging (parent_id, chunk_index, chunk_count, content, couchdb_rev) VALUES ('queued-parent', 0, 2, 'partial', '1-test')",
    )
    .execute(persistence.pool())
    .await
    .expect("insert chunk before atomic recovery enqueue");
    persistence
        .purge_chunk_staging_and_enqueue_recovery(
            vec!["queued-parent".to_string()],
            "chunk_parent",
            vec!["queued-parent".to_string()],
        )
        .await
        .expect("atomically purge and enqueue recovery");
    let staged_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM chunk_staging WHERE parent_id = 'queued-parent'")
            .fetch_one(persistence.pool())
            .await
            .expect("count purged staging rows");
    assert_eq!(staged_count, 0);
    assert!(
        persistence
            .due_recovery_targets(10, Utc::now())
            .await
            .expect("load atomically queued target")
            .iter()
            .any(|target| target.target_id == "queued-parent")
    );

    let failed_write = store_e
        .prepare_create_vault_write_at(
            NewNoteRequest {
                title: "Persistence Failure".to_string(),
                content: "# Persistence Failure\n".to_string(),
                template_id: None,
                file_type: NewNoteFileType::Md,
            },
            now + chrono::Duration::seconds(5),
        )
        .await
        .expect("prepare write before closing pool");
    persistence.pool().close().await;
    let failed_ingest = store_e
        .ingest_changes_batch(
            vec![ChangeEvent {
                seq: json!("999-g1AAA"),
                id: "unknown-doc".to_string(),
                deleted: false,
                doc: Some(json!({"_id": "unknown-doc", "type": "unknown"})),
            }],
            "999-g1AAA",
            250,
            Duration::from_secs(60),
            None,
        )
        .await;
    assert!(
        !failed_ingest.durably_applied,
        "worker must retain its previous cursor after an atomic ingest fails"
    );
    assert!(matches!(
        store_e
            .commit_prepared_vault_write(failed_write, "1-test")
            .await,
        Err(WriteError::Persistence)
    ));
}

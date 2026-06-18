# Live Runtime Integration Validation Results

- Started at (UTC): 2026-03-01T19:30:06Z
- Finished at (UTC): 2026-03-01T19:30:24Z
- Command: `cargo test --test live_runtime_integration_tests -- --nocapture --test-threads=1`
- Command exit code: 0
- Strict mode: yes
- Skip lines detected: 0
- Result: **passed**
- Notes: Live integration suite executed with runtime credentials.
- Execution context: Local harness run on 2026-03-01 using an ephemeral CouchDB container and a temporary embedding endpoint compatible with the LocalAI API contract. This is strict non-skip test evidence, but it is not a production Obsidian/Livesync deployment run.

## Environment Variable Presence

| Variable | Present |
|---|---|
| `VAULT_BRIDGE_RUN_LIVE_TESTS` | yes |
| `VAULT_BRIDGE_LIVE_COUCHDB_URL` | yes |
| `VAULT_BRIDGE_LIVE_COUCHDB_DATABASE` | yes |
| `VAULT_BRIDGE_LIVE_COUCHDB_USER` | yes |
| `VAULT_BRIDGE_LIVE_COUCHDB_PASS` | yes |
| `VAULT_BRIDGE_LIVE_LOCALAI_URL` | yes |
| `VAULT_BRIDGE_LIVE_LOCALAI_MODEL` | yes |
| `VAULT_BRIDGE_LIVE_LOCALAI_DIMENSIONS` | yes |

## Raw Command Output

```text
    Finished `test` profile [unoptimized + debuginfo] target(s) in 0.69s
     Running tests/live_runtime_integration_tests.rs (target/debug/deps/live_runtime_integration_tests-36b10f7db0d8ad13)

running 5 tests
test live_couchdb_changes_feed_reassembles_chunked_note ... ok
test live_localai_embedding_worker_backfills_pending_notes ... ok
test live_post_notes_round_trip_writes_livesync_docs_and_indexes_note ... ok
test live_rename_cascade_applies_as_atomic_batch_without_fractured_graph_state ... ok
test live_sync_worker_indexes_written_note ... ok

test result: ok. 5 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 16.91s

```

## Manual Obsidian Verification

- [ ] Confirm the note created by `live_post_notes_round_trip_writes_livesync_docs_and_indexes_note` appears in Obsidian.
- [ ] Open the note and verify markdown/frontmatter render correctly.
- [ ] Confirm updates sync back through Livesync without conflicts.

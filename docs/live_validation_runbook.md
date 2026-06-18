# Live Runtime Validation Runbook

This runbook executes Vault Bridge's env-gated live integration suite against a
real Livesync/CouchDB + LocalAI environment, then records an auditable markdown
artifact.

Use this for PRD validation requiring:

- `_changes` ingestion + chunk reassembly
- Sync Worker indexing from live CouchDB updates
- `POST /api/v1/notes` write-through round-trip
- Rename-cascade atomicity under debounce batching
- Embedding Worker backfill against live LocalAI

## Tests covered

`tests/live_runtime_integration_tests.rs`:

- `live_couchdb_changes_feed_reassembles_chunked_note`
- `live_sync_worker_indexes_written_note`
- `live_post_notes_round_trip_writes_livesync_docs_and_indexes_note`
- `live_rename_cascade_applies_as_atomic_batch_without_fractured_graph_state`
- `live_localai_embedding_worker_backfills_pending_notes`

## Prerequisites

1. A test CouchDB database used by Obsidian Livesync.
2. LocalAI embeddings endpoint reachable from this machine.
3. Credentials exported as environment variables:
   - `VAULT_BRIDGE_RUN_LIVE_TESTS=1`
   - `VAULT_BRIDGE_LIVE_COUCHDB_URL`
   - `VAULT_BRIDGE_LIVE_COUCHDB_DATABASE`
   - `VAULT_BRIDGE_LIVE_COUCHDB_USER`
   - `VAULT_BRIDGE_LIVE_COUCHDB_PASS`
   - `VAULT_BRIDGE_LIVE_LOCALAI_URL`
   - `VAULT_BRIDGE_LIVE_LOCALAI_MODEL` (optional, defaults to `nomic-embed-text`)
   - `VAULT_BRIDGE_LIVE_LOCALAI_DIMENSIONS` (optional, defaults to `768`)
4. Strict mode behavior:
   - When `VAULT_BRIDGE_RUN_LIVE_TESTS=1`, missing required env vars fail tests (no skip fallback).
   - The report script also fails if any `skipping live_...` lines appear while strict mode is active.
   - Optional override: `VAULT_BRIDGE_LIVE_STRICT_MODE=1` forces strict skip-detection even if the run flag is not set.

## Execute and capture report

From repo root:

```bash
./scripts/run_live_validation.sh
```

Optional custom report path:

```bash
VAULT_BRIDGE_LIVE_REPORT_PATH=docs/live_validation_results_YYYY-MM-DD.md \
./scripts/run_live_validation.sh
```

Strict mode can also be forced explicitly:

```bash
VAULT_BRIDGE_LIVE_STRICT_MODE=1 ./scripts/run_live_validation.sh
```

The script runs:

```bash
cargo test --test live_runtime_integration_tests -- --nocapture --test-threads=1
```

and writes a markdown report containing:

- UTC start/end timestamps
- test command and exit code
- environment-variable presence matrix (without leaking secret values)
- raw test output
- manual Obsidian verification checklist

## Manual Obsidian check

After a passing live run:

1. Open Obsidian connected to the same Livesync target database.
2. Search for note titles starting with `Live API Write`.
3. Open the newly created note and verify frontmatter/body integrity.
4. Confirm no conflict artifacts were created by the test writes.

## Committed artifacts

- Runbook: `docs/live_validation_runbook.md`
- Latest captured harness status: `docs/live_validation_results_2026-03-01.md`

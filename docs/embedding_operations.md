# Embedding Operations

Vault Bridge keeps permanent embedding quarantine for notes and semantic chunks.
That protects the embedding backend from retry storms, while still giving
operators a bounded way to recover after a backend outage or config fix.

## Verify Health

Check API status:

```bash
API_TOKEN="$(tr -d '\r\n' < .secrets/api/admin.token)"
curl -fsS -H "X-Api-Key: ${API_TOKEN}" \
  http://127.0.0.1:8080/api/v1/status | jq '.embedding, .index'
```

Useful fields:

- `embedding.mode`, `embedding.model`, `embedding.dimensions`
- `embedding.endpoint`, sanitized to host/path only
- `embedding.pending_notes` and `embedding.quarantined_notes`
- `embedding.pending_chunks` and `embedding.quarantined_chunks`
- `embedding.last_success_at`, `embedding.last_error_at`, `embedding.last_error`
- `embedding.backend_state`

Metrics include:

- `vault_bridge_pending_embeddings`
- `vault_bridge_quarantined_embeddings`
- `vault_bridge_pending_chunk_embeddings`
- `vault_bridge_quarantined_chunk_embeddings`
- `vault_bridge_embedding_backend_degraded`
- `vault_bridge_embedding_last_success_timestamp_seconds`
- `vault_bridge_embedding_last_error_timestamp_seconds`

## Rebuild Semantic Chunks

Use this after changing chunking settings or after upgrading from heading-sized
blocks to semantic chunks:

```bash
vault_bridge --reindex-blocks
```

This rebuilds chunks for all notes and clears note embedding/failure state so
the worker can repopulate embeddings. For a missing-only backfill:

```bash
vault_bridge --reindex-blocks --only-missing
```

## Unblock Quarantined Work

Always start with a dry run:

```bash
vault_bridge --embedding-unblock --dry-run --path-prefix 03Concepts/ --limit 20
```

Then unblock a bounded batch:

```bash
vault_bridge --embedding-unblock --path-prefix 03Concepts/ --limit 20
```

Supported selectors:

```bash
vault_bridge --embedding-unblock --note-id path/to/note.md --limit 20
vault_bridge --embedding-unblock --block-id 'path/to/note.md##12'
vault_bridge --embedding-unblock --path-prefix 03Concepts/ --limit 20
vault_bridge --embedding-unblock --all --limit 20
```

`--all` requires `--limit`. Increase the limit gradually only after status,
metrics, and backend logs show successful progress.

## LocalAI Dimensions

Keep top-level `embedding.dimensions` aligned with the actual model output.
When the backend supports the OpenAI-compatible `dimensions` request field, set:

```yaml
embedding:
  localai:
    request_dimensions: true
```

Changing model or dimensions clears incompatible note and chunk embeddings,
resets their failure counters, and rebuilds HNSW indexes so the worker can
re-embed against the new schema.

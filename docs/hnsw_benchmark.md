# HNSW Benchmark Workflow

This procedure benchmarks `idx_notes_embedding` tuning for Vault Bridge's
read-heavy + background-write workload.

Latest committed benchmark run:

- [docs/hnsw_benchmark_results_2026-03-01.md](hnsw_benchmark_results_2026-03-01.md)

It focuses on two goals:

1. Keep semantic/hybrid query latency stable.
2. Avoid excessive write slowdown while Worker B is backfilling embeddings.

## Config knobs

`config.yaml`:

```yaml
embedding:
  hnsw_m: 16
  hnsw_ef_construction: 64
```

- `hnsw_m`: graph connectivity. Higher values usually improve recall at higher
  index memory/build cost.
- `hnsw_ef_construction`: candidate list size during index construction.
  Higher values usually improve recall at higher index build/write cost.

## Recommended baseline

Start from:

- `hnsw_m = 16`
- `hnsw_ef_construction = 64`

The latest matrix run kept these values as the locked defaults.

## Automated runner

Use the scripted matrix runner to reproduce and update benchmark evidence:

```bash
BENCH_PG_CONTAINER=vault-bridge-db ./scripts/benchmark_hnsw_matrix.sh
```

By default this writes `docs/hnsw_benchmark_latest.csv` with:

- write throughput while the HNSW index is present (Worker B simulation)
- index rebuild duration on populated vectors
- semantic query latency across the candidate matrix

## Benchmark setup

1. Use a representative Postgres dataset (same note count + average note size
   as production).
2. Ensure embeddings exist for most notes.
3. Run Vault Bridge with the candidate HNSW settings.
4. Restart Vault Bridge after each settings change so startup can rebuild
   `idx_notes_embedding`.

## Metrics to capture

Collect for each candidate pair:

1. Index rebuild duration at startup.
2. Worker B throughput (notes embedded per minute).
3. API query latency for semantic/hybrid search.
4. Sync lag and embedding backlog behavior during backfill.

Useful commands:

```bash
API_TOKEN="$(tr -d '\r\n' < .secrets/api/admin.token)"

# Index definition confirms active parameters
psql "$VAULT_BRIDGE_PG_TEST_URL" -c \
  "SELECT indexdef FROM pg_indexes WHERE indexname = 'idx_notes_embedding';"

# Worker B backlog + sync lag (while benchmark runs)
curl -s http://127.0.0.1:8080/api/v1/status \
  -H "X-Api-Key: ${API_TOKEN}" | jq '.index.pending_embeddings, .sync.behind_by'

# Query latency sample (semantic mode)
for i in {1..20}; do
  /usr/bin/time -f "%e" curl -s \
    "http://127.0.0.1:8080/api/v1/search?q=rust%20types&mode=semantic&limit=10" \
    -H "X-Api-Key: ${API_TOKEN}" > /dev/null
done
```

## Candidate matrix

Test at least:

- `(m=16, ef=64)` baseline
- `(m=16, ef=96)`
- `(m=24, ef=96)`
- `(m=32, ef=128)`

Stop increasing when write throughput degrades materially or query latency gains
flatten.

## Decision and rollout

Choose the smallest parameter pair that meets your latency/recall target.

Then:

1. Set values in `config.yaml`.
2. Restart Vault Bridge.
3. Verify index definition (`pg_indexes`) and `/api/v1/status` stability.

## Rollback

If query latency or Worker B throughput regresses:

1. Restore baseline values:
   - `hnsw_m = 16`
   - `hnsw_ef_construction = 64`
2. Restart Vault Bridge (it rebuilds `idx_notes_embedding` automatically).
3. Re-check `/api/v1/status` and semantic query latency.

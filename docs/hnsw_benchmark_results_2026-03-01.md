# HNSW Benchmark Results (2026-03-01)

This run closes the PRD phase-2 requirement to benchmark HNSW parameters under
background embedding writes and lock an initial production setting.

## Environment

- Host OS: `Linux 6.18.9-arch1-2`
- CPU threads available to container host: `9`
- Postgres image: `pgvector/pgvector:pg16`
- Corpus: `8,000` synthetic notes
- Embedding dimensions: `768`
- Candidate matrix: `(16,64)`, `(16,96)`, `(24,96)`, `(32,128)`

## Raw Results

Source CSV: [docs/hnsw_benchmark_results_2026-03-01.csv](hnsw_benchmark_results_2026-03-01.csv)

| Candidate | Write seconds (8k updates with index present) | Write throughput (rows/s) | Rebuild seconds (populated vectors) | Semantic avg/query (ms) |
|---|---:|---:|---:|---:|
| `m=16, ef=64` | 41.45 | 192.98 | 7.25 | 55.45 |
| `m=16, ef=96` | 47.88 | 167.10 | 9.29 | 55.96 |
| `m=24, ef=96` | 99.62 | 80.31 | 14.70 | 55.63 |
| `m=32, ef=128` | 178.60 | 44.79 | 23.20 | 55.79 |

## Decision

Keep and lock the current defaults:

- `embedding.hnsw_m = 16`
- `embedding.hnsw_ef_construction = 64`

Reasoning:

- Query latency is effectively flat across candidates (~55-56ms/query in this run).
- Increasing `m`/`ef_construction` materially harms Worker B write throughput and
  index rebuild time without measurable read-latency benefit.
- Relative to baseline (`16/64`):
  - `16/96` write throughput is `13.4%` slower, rebuild `28.2%` slower.
  - `24/96` write throughput is `58.4%` slower, rebuild `102.8%` slower.
  - `32/128` write throughput is `76.8%` slower, rebuild `220.1%` slower.

## Reproduce

Run:

```bash
BENCH_PG_CONTAINER=vault-bridge-db ./scripts/benchmark_hnsw_matrix.sh
```

The runner writes CSV output to `docs/hnsw_benchmark_latest.csv` by default.

#!/usr/bin/env bash
set -euo pipefail

# Benchmark HNSW settings for Vault Bridge using a pgvector Postgres container.
#
# Usage:
#   BENCH_PG_CONTAINER=vault-bridge-db ./scripts/benchmark_hnsw_matrix.sh
#
# Tunables (env):
#   BENCH_PG_CONTAINER    Docker container name (default: vault-bridge-db)
#   BENCH_DB_NAME         Database name (default: vault_bridge)
#   BENCH_DB_USER         Database user (default: vault_bridge)
#   BENCH_NOTE_COUNT      Number of synthetic notes (default: 8000)
#   BENCH_QUERY_COUNT     Number of semantic queries per candidate (default: 80)
#   BENCH_DIMENSIONS      Embedding dimensions (default: 768)
#   BENCH_CANDIDATES      Space-separated m:ef pairs (default: "16:64 16:96 24:96 32:128")
#   BENCH_RESULTS_FILE    CSV output path (default: docs/hnsw_benchmark_latest.csv)

CONTAINER="${BENCH_PG_CONTAINER:-vault-bridge-db}"
DB_NAME="${BENCH_DB_NAME:-vault_bridge}"
DB_USER="${BENCH_DB_USER:-vault_bridge}"
NOTE_COUNT="${BENCH_NOTE_COUNT:-8000}"
QUERY_COUNT="${BENCH_QUERY_COUNT:-80}"
DIMENSIONS="${BENCH_DIMENSIONS:-768}"
CANDIDATES="${BENCH_CANDIDATES:-16:64 16:96 24:96 32:128}"
RESULTS_FILE="${BENCH_RESULTS_FILE:-docs/hnsw_benchmark_latest.csv}"

if ! docker ps --format '{{.Names}}' | grep -qx "$CONTAINER"; then
  echo "Postgres container '$CONTAINER' is not running." >&2
  exit 1
fi

psql_exec() {
  docker exec -i "$CONTAINER" psql -U "$DB_USER" -d "$DB_NAME" -v ON_ERROR_STOP=1 "$@"
}

psql_exec -q <<SQL
CREATE EXTENSION IF NOT EXISTS vector;
ALTER TABLE notes ALTER COLUMN embedding TYPE vector(${DIMENSIONS});
CREATE OR REPLACE FUNCTION random_vector(dim integer)
RETURNS vector
LANGUAGE SQL
VOLATILE
AS \$\$
  SELECT ARRAY(SELECT random() FROM generate_series(1, dim))::vector;
\$\$;
SQL

mkdir -p "$(dirname "$RESULTS_FILE")"
printf 'candidate,write_rows,write_seconds,write_rows_per_second,rebuild_seconds,semantic_queries,semantic_total_seconds,semantic_avg_ms\n' > "$RESULTS_FILE"

run_candidate() {
  local m="$1"
  local ef="$2"
  local label="m${m}_ef${ef}"

  psql_exec -q -c "DROP INDEX IF EXISTS idx_notes_embedding;"
  psql_exec -q -c "DROP TABLE IF EXISTS bench_query_vectors;"
  psql_exec -q -c "TRUNCATE TABLE tags, links, notes CASCADE;"

  psql_exec -q <<SQL
INSERT INTO notes (id, path, title, content, summary, frontmatter, couchdb_rev, created_at, updated_at, indexed_at)
SELECT
  format('bench/%s.md', gs),
  format('bench/%s.md', gs),
  format('Benchmark Note %s', gs),
  format('# Benchmark Note %s\n\nSynthetic benchmark body.', gs),
  format('# Benchmark Note %s', gs),
  '{}'::jsonb,
  format('bench-rev-%s', gs),
  now(),
  now(),
  now()
FROM generate_series(1, ${NOTE_COUNT}) AS gs;
SQL

  # Simulate Worker B write path: updates occur while HNSW index exists.
  psql_exec -q -c "CREATE INDEX idx_notes_embedding ON notes USING hnsw (embedding vector_cosine_ops) WITH (m = ${m}, ef_construction = ${ef});" >/dev/null

  local t0 t1 write_seconds
  t0=$(date +%s.%N)
  psql_exec -q -c "UPDATE notes SET embedding = random_vector(${DIMENSIONS}) WHERE embedding IS NULL;" >/dev/null
  t1=$(date +%s.%N)
  write_seconds=$(awk -v start="$t0" -v end="$t1" 'BEGIN { printf "%.6f", end-start }')
  local write_rows_per_second
  write_rows_per_second=$(awk -v rows="$NOTE_COUNT" -v sec="$write_seconds" 'BEGIN { if (sec <= 0) print 0; else printf "%.2f", rows/sec }')

  # Rebuild on populated vectors for startup/index-maintenance cost.
  psql_exec -q -c "DROP INDEX IF EXISTS idx_notes_embedding;" >/dev/null
  t0=$(date +%s.%N)
  psql_exec -q -c "CREATE INDEX idx_notes_embedding ON notes USING hnsw (embedding vector_cosine_ops) WITH (m = ${m}, ef_construction = ${ef});" >/dev/null
  t1=$(date +%s.%N)
  local rebuild_seconds
  rebuild_seconds=$(awk -v start="$t0" -v end="$t1" 'BEGIN { printf "%.6f", end-start }')

  psql_exec -q <<SQL
CREATE TABLE bench_query_vectors AS
SELECT gs AS qid, random_vector(${DIMENSIONS}) AS q
FROM generate_series(1, ${QUERY_COUNT}) AS gs;
SQL

  t0=$(date +%s.%N)
  psql_exec -q <<SQL
SELECT COUNT(*)
FROM (
  SELECT q.qid, n.id
  FROM bench_query_vectors q
  JOIN LATERAL (
    SELECT id
    FROM notes
    WHERE embedding IS NOT NULL
    ORDER BY embedding <=> q.q
    LIMIT 10
  ) AS n ON true
) ranked;
SQL
  t1=$(date +%s.%N)

  local semantic_total_seconds
  semantic_total_seconds=$(awk -v start="$t0" -v end="$t1" 'BEGIN { printf "%.6f", end-start }')
  local semantic_avg_ms
  semantic_avg_ms=$(awk -v sec="$semantic_total_seconds" -v q="$QUERY_COUNT" 'BEGIN { if (q <= 0) print 0; else printf "%.3f", (sec*1000)/q }')

  printf '%s,%s,%s,%s,%s,%s,%s,%s\n' \
    "$label" "$NOTE_COUNT" "$write_seconds" "$write_rows_per_second" "$rebuild_seconds" "$QUERY_COUNT" "$semantic_total_seconds" "$semantic_avg_ms" \
    >> "$RESULTS_FILE"

  echo "Completed $label"
}

for pair in $CANDIDATES; do
  m="${pair%%:*}"
  ef="${pair##*:}"
  run_candidate "$m" "$ef"
done

echo "Results written to $RESULTS_FILE"
cat "$RESULTS_FILE"

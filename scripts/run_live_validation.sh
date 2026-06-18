#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

DEFAULT_REPORT_PATH="docs/live_validation_results_$(date -u +%Y-%m-%d).md"
REPORT_PATH="${VAULT_BRIDGE_LIVE_REPORT_PATH:-$DEFAULT_REPORT_PATH}"
TMP_LOG="$(mktemp -t vault-bridge-live-validation.XXXXXX.log)"
STARTED_AT="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

readonly REQUIRED_ENV_VARS=(
  "VAULT_BRIDGE_RUN_LIVE_TESTS"
  "VAULT_BRIDGE_LIVE_COUCHDB_URL"
  "VAULT_BRIDGE_LIVE_COUCHDB_DATABASE"
  "VAULT_BRIDGE_LIVE_COUCHDB_USER"
  "VAULT_BRIDGE_LIVE_COUCHDB_PASS"
  "VAULT_BRIDGE_LIVE_LOCALAI_URL"
  "VAULT_BRIDGE_LIVE_LOCALAI_MODEL"
  "VAULT_BRIDGE_LIVE_LOCALAI_DIMENSIONS"
)

is_truthy() {
  case "${1:-}" in
    1|true|TRUE|True|yes|YES|on|ON) return 0 ;;
    *) return 1 ;;
  esac
}

# Force single-threaded test execution to keep raw artifact logs readable and deterministic.
COMMAND=(cargo test --test live_runtime_integration_tests -- --nocapture --test-threads=1)

set +e
"${COMMAND[@]}" 2>&1 | tee "$TMP_LOG"
COMMAND_EXIT="${PIPESTATUS[0]}"
set -e

FINISHED_AT="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
SKIP_LINES="$(grep -Eic "skipping live_" "$TMP_LOG" || true)"
STRICT_MODE="no"
if is_truthy "${VAULT_BRIDGE_LIVE_STRICT_MODE:-}" || is_truthy "${VAULT_BRIDGE_RUN_LIVE_TESTS:-}"; then
  STRICT_MODE="yes"
fi

if [[ "$STRICT_MODE" == "yes" && "$SKIP_LINES" -gt 0 ]]; then
  COMMAND_EXIT=1
fi

RESULT_STATUS="failed"
RESULT_NOTE="One or more live integration assertions failed."
if [[ "$STRICT_MODE" == "yes" && "$SKIP_LINES" -gt 0 ]]; then
  RESULT_STATUS="failed"
  RESULT_NOTE="Strict mode is enabled and skip output was detected."
elif [[ "$COMMAND_EXIT" -eq 0 ]]; then
  if grep -q "set VAULT_BRIDGE_RUN_LIVE_TESTS=1" "$TMP_LOG"; then
    RESULT_STATUS="blocked"
    RESULT_NOTE="Live credentials were not provided; tests executed in skip mode."
  else
    RESULT_STATUS="passed"
    RESULT_NOTE="Live integration suite executed with runtime credentials."
  fi
fi

mkdir -p "$(dirname "$REPORT_PATH")"

{
  echo "# Live Runtime Integration Validation Results"
  echo
  echo "- Started at (UTC): $STARTED_AT"
  echo "- Finished at (UTC): $FINISHED_AT"
  echo "- Command: \`${COMMAND[*]}\`"
  echo "- Command exit code: $COMMAND_EXIT"
  echo "- Strict mode: $STRICT_MODE"
  echo "- Skip lines detected: $SKIP_LINES"
  echo "- Result: **$RESULT_STATUS**"
  echo "- Notes: $RESULT_NOTE"
  echo
  echo "## Environment Variable Presence"
  echo
  echo "| Variable | Present |"
  echo "|---|---|"
  for var_name in "${REQUIRED_ENV_VARS[@]}"; do
    if [[ -n "${!var_name-}" ]]; then
      present="yes"
    else
      present="no"
    fi
    printf '| `%s` | %s |\n' "$var_name" "$present"
  done
  echo
  echo "## Raw Command Output"
  echo
  echo '```text'
  cat "$TMP_LOG"
  echo '```'
  echo
  echo "## Manual Obsidian Verification"
  echo
  echo "- [ ] Confirm the note created by \`live_post_notes_round_trip_writes_livesync_docs_and_indexes_note\` appears in Obsidian."
  echo "- [ ] Open the note and verify markdown/frontmatter render correctly."
  echo "- [ ] Confirm updates sync back through Livesync without conflicts."
} > "$REPORT_PATH"

rm -f "$TMP_LOG"
echo "Wrote live validation report: $REPORT_PATH"

exit "$COMMAND_EXIT"

#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

DEFAULT_REPORT_PATH="docs/live_mcp_validation_results_$(date -u +%Y-%m-%d).md"
REPORT_PATH="${VAULT_BRIDGE_LIVE_MCP_REPORT_PATH:-$DEFAULT_REPORT_PATH}"
TMP_LOG="$(mktemp -t vault-bridge-live-mcp-validation.XXXXXX.log)"
STARTED_AT="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

readonly ENV_VARS=(
  "VAULT_BRIDGE_RUN_LIVE_MCP_TESTS"
  "VAULT_BRIDGE_LIVE_MCP_EXTERNAL_URL"
  "VAULT_BRIDGE_LIVE_MCP_LOCAL_URL"
  "VAULT_BRIDGE_LIVE_MCP_EXTERNAL_BEARER_TOKEN"
  "VAULT_BRIDGE_LIVE_MCP_LOCAL_BEARER_TOKEN"
  "VAULT_BRIDGE_LIVE_MCP_PUBLIC_NOTE_ID"
  "VAULT_BRIDGE_LIVE_MCP_PERSONAL_NOTE_ID"
  "VAULT_BRIDGE_LIVE_MCP_QUERY"
)

is_truthy() {
  case "${1:-}" in
    1|true|TRUE|True|yes|YES|on|ON) return 0 ;;
    *) return 1 ;;
  esac
}

COMMAND=(cargo test --test live_mcp_validation_tests -- --nocapture --test-threads=1)

set +e
"${COMMAND[@]}" 2>&1 | tee "$TMP_LOG"
COMMAND_EXIT="${PIPESTATUS[0]}"
set -e

FINISHED_AT="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
SKIP_LINES="$(grep -Eic "skipping live_mcp_" "$TMP_LOG" || true)"
STRICT_MODE="no"
if is_truthy "${VAULT_BRIDGE_LIVE_MCP_STRICT_MODE:-}" || is_truthy "${VAULT_BRIDGE_RUN_LIVE_MCP_TESTS:-}"; then
  STRICT_MODE="yes"
fi

if [[ "$STRICT_MODE" == "yes" && "$SKIP_LINES" -gt 0 ]]; then
  COMMAND_EXIT=1
fi

RESULT_STATUS="failed"
RESULT_NOTE="One or more live MCP integration assertions failed."
if [[ "$STRICT_MODE" == "yes" && "$SKIP_LINES" -gt 0 ]]; then
  RESULT_STATUS="failed"
  RESULT_NOTE="Strict mode is enabled and skip output was detected."
elif [[ "$COMMAND_EXIT" -eq 0 ]]; then
  if grep -q "set VAULT_BRIDGE_RUN_LIVE_MCP_TESTS=1" "$TMP_LOG"; then
    RESULT_STATUS="blocked"
    RESULT_NOTE="Live MCP credentials/targets were not provided; tests executed in skip mode."
  else
    RESULT_STATUS="passed"
    RESULT_NOTE="Live MCP integration suite executed against runtime endpoints."
  fi
fi

mkdir -p "$(dirname "$REPORT_PATH")"

{
  echo "# Live MCP Integration Validation Results"
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
  for var_name in "${ENV_VARS[@]}"; do
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
  echo "## Manual Claude Code Validation Checklist"
  echo
  echo "- [ ] Configure Claude Code to use the external MCP endpoint (\`/sse\`) under test."
  echo "- [ ] In one session, confirm tool discovery and execute: \`query_notes\`, \`get_note\`, \`get_neighbors\`, \`list_tags\`, and \`new_note\`."
  echo "- [ ] Verify \`new_note\` appears in Obsidian under \`11New/\` after sync."
  echo "- [ ] If a local-context MCP token is configured, confirm a personal note is hidden from external context but readable from local context."
} > "$REPORT_PATH"

rm -f "$TMP_LOG"
echo "Wrote live MCP validation report: $REPORT_PATH"

exit "$COMMAND_EXIT"

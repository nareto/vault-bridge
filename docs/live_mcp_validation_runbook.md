# Live MCP Validation Runbook

This runbook executes Vault Bridge's env-gated live MCP integration suite and
captures an auditable markdown artifact.

Use this when validating PRD Phase 4 MCP requirements:

- MCP handshake and transport (`/sse` + `/mcp`)
- Tool discovery (`tools/list`)
- Tool-call flow for the PRD tool surface
- Context behavior across external/local MCP tokens
- End-to-end `new_note` creation through MCP

## Tests covered

`tests/live_mcp_validation_tests.rs`:

- `live_mcp_initialize_and_sse_endpoint_are_available`
- `live_mcp_external_context_tool_flow_matches_prd_surface`
- `live_mcp_context_opacity_blocks_personal_notes_for_external_context`

## Prerequisites

1. Running Vault Bridge + MCP deployment (for example `docker compose up -d`).
2. MCP endpoint URL and bearer tokens configured in deployment.
3. Optional local-context MCP token for cross-context checks.
4. Environment variables:
   - `VAULT_BRIDGE_RUN_LIVE_MCP_TESTS=1`
   - `VAULT_BRIDGE_LIVE_MCP_EXTERNAL_URL` (for example `http://127.0.0.1:8080`)
   - `VAULT_BRIDGE_LIVE_MCP_LOCAL_URL` (optional, usually the same URL with a local-context token)
   - `VAULT_BRIDGE_LIVE_MCP_EXTERNAL_BEARER_TOKEN` (optional, required if external MCP has Bearer auth enabled)
   - `VAULT_BRIDGE_LIVE_MCP_LOCAL_BEARER_TOKEN` (optional, required if local MCP has Bearer auth enabled)
   - `VAULT_BRIDGE_LIVE_MCP_PUBLIC_NOTE_ID` (optional readable note for smoke read)
   - `VAULT_BRIDGE_LIVE_MCP_PERSONAL_NOTE_ID` (optional personal note ID for context opacity)
   - `VAULT_BRIDGE_LIVE_MCP_QUERY` (optional search seed)
5. Strict mode behavior:
   - When `VAULT_BRIDGE_RUN_LIVE_MCP_TESTS=1`, missing required env vars fail tests (no skip fallback).
   - The context-opacity test requires both `VAULT_BRIDGE_LIVE_MCP_LOCAL_URL` and
     `VAULT_BRIDGE_LIVE_MCP_PERSONAL_NOTE_ID` when strict mode is enabled.
   - The report script fails if any `skipping live_mcp_...` lines appear while strict mode is active.
   - Optional override: `VAULT_BRIDGE_LIVE_MCP_STRICT_MODE=1` forces strict skip-detection.

`VAULT_BRIDGE_LIVE_MCP_PERSONAL_NOTE_ID` should point to a note hidden from the
external context so external is expected to fail and local is expected to
succeed.

## Execute and capture report

From repo root:

```bash
./scripts/run_live_mcp_validation.sh
```

Optional custom report path:

```bash
VAULT_BRIDGE_LIVE_MCP_REPORT_PATH=docs/live_mcp_validation_results_YYYY-MM-DD.md \
./scripts/run_live_mcp_validation.sh
```

Strict mode can also be forced explicitly:

```bash
VAULT_BRIDGE_LIVE_MCP_STRICT_MODE=1 ./scripts/run_live_mcp_validation.sh
```

The script runs:

```bash
cargo test --test live_mcp_validation_tests -- --nocapture --test-threads=1
```

and writes a report including:

- UTC start/end timestamps
- command and exit code
- environment-variable presence matrix (without leaking secret values)
- raw test output
- manual Claude Code validation checklist

## Manual Claude Code verification

After a passing live run:

1. Configure Claude Code with the tested MCP endpoint (`/sse`).
2. In a single session, execute:
   - `query_notes`
   - `get_note`
   - `get_neighbors`
   - `list_tags`
   - `new_note`
3. Verify the created note appears in Obsidian under `11New/`.
4. If both contexts are available, verify:
   - external MCP cannot read the configured personal note
   - local MCP can read the same personal note

## Committed artifacts

- Runbook: `docs/live_mcp_validation_runbook.md`
- Latest captured harness status: `docs/live_mcp_validation_results_2026-03-01.md`

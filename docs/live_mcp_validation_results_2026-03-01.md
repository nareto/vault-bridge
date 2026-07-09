# Live MCP Integration Validation Results

- Started at (UTC): 2026-03-01T19:33:55Z
- Finished at (UTC): 2026-03-01T19:34:05Z
- Command: `cargo test --test live_mcp_validation_tests -- --nocapture --test-threads=1`
- Command exit code: 0
- Strict mode: yes
- Skip lines detected: 0
- Result: **passed**
- Notes: Live MCP integration suite executed against runtime endpoints.
- Execution context: Local harness run on 2026-03-01 against ephemeral Vault Bridge + MCP processes with seeded data. This is strict non-skip MCP protocol/tool evidence, but it is not a manual Claude Code client session against a long-lived production deployment.

## Environment Variable Presence

| Variable | Present |
|---|---|
| `VAULT_BRIDGE_RUN_LIVE_MCP_TESTS` | yes |
| `VAULT_BRIDGE_LIVE_MCP_EXTERNAL_URL` | yes |
| `VAULT_BRIDGE_LIVE_MCP_LOCAL_URL` | yes |
| `VAULT_BRIDGE_LIVE_MCP_PUBLIC_NOTE_ID` | yes |
| `VAULT_BRIDGE_LIVE_MCP_PERSONAL_NOTE_ID` | yes |
| `VAULT_BRIDGE_LIVE_MCP_QUERY` | yes |

## Raw Command Output

```text
    Finished `test` profile [unoptimized + debuginfo] target(s) in 0.26s
     Running tests/live_mcp_validation_tests.rs (target/debug/deps/live_mcp_validation_tests-54c4c761e3df1d0a)

running 3 tests
test live_mcp_external_context_tool_flow_matches_prd_surface ... ok
test live_mcp_initialize_and_sse_endpoint_are_available ... ok
test live_mcp_context_opacity_blocks_personal_notes_for_external_context ... ok

test result: ok. 3 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 9.64s

```

## Manual Claude Code Validation Checklist

- [ ] Configure Claude Code to use the external MCP endpoint (`/sse`) under test.
- [ ] In one session, confirm tool discovery and execute: `query_notes`, `get_vault_file`, `get_neighbors`, `list_tags`, and `create_vault_file`.
- [ ] Verify `create_vault_file` output appears in Obsidian under `11New/` after sync.
- [ ] If local MCP endpoint is configured, confirm a personal note is hidden from external context but readable from local context.

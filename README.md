# Vault Bridge

Vault Bridge turns an Obsidian LiveSync vault into a self-hosted knowledge
backend for AI agents, automations, dashboards, and local tools. It watches the
LiveSync CouchDB, indexes notes into Postgres, builds search and graph metadata,
and exposes the result through both a documented REST API and an embedded MCP
endpoint.

The goal is practical: give AI clients useful access to your notes without
handing every client the raw vault, raw CouchDB credentials, or a one-size-fits-all
view of private and public material.

## Status and Limitations

Vault Bridge is alpha software and a productized personal project. It is useful
for self-hosted agent workflows today, but it is not a general-purpose Obsidian
backend or a generic note-sync abstraction.

The supported shape is intentionally narrow:

- Obsidian LiveSync/CouchDB is the source of truth.
- PostgreSQL with pgvector is the derived search, graph, and embedding index.
- REST and MCP are the application and agent surfaces.
- LocalAI can be used for embeddings, while deterministic local embeddings are
  available for development and smaller deployments.
- LocalAI-backed LLM summaries are planned, not a stable production feature.
- Prometheus and Grafana-style observability is supported for self-hosted
  deployments, but this repo does not own environment-specific monitoring
  rollout.

Expect breaking configuration and API changes before a stable release. Run it
against vaults you can back up and recover, keep tokens scoped to least-privilege
contexts, and review access rules before exposing MCP or REST endpoints to any
client you do not fully control.

## What You Get

- **Obsidian-native ingestion** from the existing LiveSync CouchDB database,
  including optional LiveSync E2EE decryption when a passphrase is configured.
- **Fast retrieval over a real index** with metadata filters, full-text search,
  semantic search, hybrid ranking, link graph traversal, backlinks, tags, and
  REST context assembly.
- **Config-defined access contexts** so cloud AI clients can see only the notes
  you allow while trusted local agents can receive broader access.
- **REST and MCP surfaces**: REST is the stable backend contract for apps,
  scripts, monitoring, and generated clients; MCP is the agent-facing adapter for
  Claude, Codex, n8n, and other MCP-capable clients.
- **Write-through note creation and editing** with server-side path validation,
  CouchDB synchronization, and extra MCP guardrails for agent-created or
  explicitly editable notes.
- **Self-hosted operations** with Docker Compose, Swagger UI, Prometheus metrics,
  named API credentials, MCP bearer tokens, and separate API/worker modes.

## How It Works

Vault Bridge is intentionally split into a few small responsibilities:

| Layer | Role |
|---|---|
| LiveSync CouchDB | Source of truth for synced Obsidian notes |
| Workers | Poll CouchDB changes, parse Markdown, update indexes and embeddings |
| Postgres + pgvector | Persistent note metadata, search text, links, tags, sync state, and vectors |
| REST API | Stable backend API under `/api/v1` with `X-Api-Key` context selection |
| MCP endpoint | Agent-facing `/mcp` and `/sse` transports in the same app process |

This split keeps the expensive and security-sensitive note logic in one backend
while still giving agent clients a clean MCP interface. Non-agent software can
use REST directly instead of pretending to be an MCP client.

## Core Features

| Feature | Description |
|---|---|
| Context policy visibility | YAML contexts define allow/deny rules for every read and write |
| Search | Supports full-text, semantic, and hybrid search modes |
| Structured note queries | Filters by tags, paths, frontmatter, timestamps, exact titles, and text relevance |
| Graph exploration | Returns neighbors, backlinks, shortest paths, hub-aware graph expansion, and link context |
| REST context assembly | Builds compact bundles from note seeds, semantic queries, and graph expansion for REST clients |
| Write support | Creates notes in a configured inbox path and updates eligible notes back through CouchDB |
| MCP resources | Exposes a tooling catalog, OpenAPI schema, note resource templates, and primitive note/query/graph tools to agents |
| Observability | Publishes status and Prometheus metrics for sync lag, embeddings, notes, links, tags, and context counts |

## Contexts

Access is configured with named contexts in `config.yaml`. REST API token names
and MCP bearer-token names point at a context. Each context has explicit `read`,
`create`, and `edit` rule lists. Deny rules win, and a request with no matching
allow rule is denied.

```yaml
api_tokens:
  monitoring:
    context: non_personal

mcp_tokens:
  claude-work:
    context: work

contexts:
  non_personal:
    read:
      - deny:
          path_prefix: "66Journal/"
      - deny:
          tags_any: ["personal"]
      - allow:
          default: true
    create: []
    edit: []
```

No context names or token names are built in. Missing `api_tokens` or
`mcp_tokens` sections mean no tokens of that type are configured. Use names that
match your deployment and point each token at an explicit context.

## Runtime Surfaces

| Surface | Default URL | Auth | Primary use |
|---|---|---|---|
| REST API | `http://127.0.0.1:8080/api/v1` | `X-Api-Key` | Apps, scripts, dashboards, monitoring, generated clients |
| Swagger UI | `http://127.0.0.1:8080/swagger-ui/` | `X-Api-Key` inside UI | Human API exploration |
| OpenAPI JSON | `http://127.0.0.1:8080/api-doc/openapi.json` | None for schema | Client generation and MCP schema resource |
| MCP | `http://127.0.0.1:8080/mcp` or `/sse` | Bearer token | Agent clients, authorized by token context |

The REST API and MCP endpoint share the same in-process service and policy
engine. MCP authenticates agent clients, exposes MCP tool/resource discovery,
and sanitizes large embedded image payloads by default.

## Images and Deployment

This repository intentionally keeps deployment environment details out of
source control. The Dockerfiles and Compose file here are generic app assets:

- `docker-compose.yml` builds from source
- `.env.example` documents local dev/runtime env
- `config.example.yaml` documents the app's general config schema

Build and publish production images from your release system, then let your
deployment repository or orchestrator consume an explicit image tag or app
revision. Keep environment-specific rollout automation, registry names, and
runtime secret locations outside this application repository.

## A) Local Deploy

1. Prepare runtime files:

```bash
cp config.example.yaml config.yaml
cp .env.example .env
```

2. Edit `config.yaml`:

- Set real `couchdb.url` and `couchdb.database`.
- Keep `couchdb.username`/`couchdb.password` as `${COUCHDB_USER}` / `${COUCHDB_PASS}`.
- Ensure Postgres host is `postgres` when running in Docker Compose:

```yaml
database:
  url: "postgres://vault_bridge:${PG_PASS}@postgres:5432/vault_bridge"
```

- Set named API token mappings and bind each one to a context. Token contents
  live in `.secrets/api/<name>.token`:

```yaml
api_tokens:
  monitoring:
    context: non_personal
```

All REST endpoints, including `GET /api/v1/metrics`, use `X-Api-Key` with one
of the configured file-backed API tokens.

- Adjust `contexts` to match your vault's folder structure. Rules can gate
  read/create/edit by path prefix, tags, title regex, owner, and
  created/updated time. Created notes can receive policy-driven tags and owner
  metadata; editable notes can preserve configured tags such as `ai-editable`.

- Configure `new_note.path_template` for server-generated create paths.
  Supported tokens are `{base}`, `{date}`, `{slug}`, and `{title}`. Use
  `{title}` for human-readable filenames that preserve spaces and capitalization
  after safety cleanup, or `{slug}` for lowercase hyphenated filenames.

- Choose an embedding mode. Default `config.example.yaml` uses local CPU embeddings:

```yaml
embedding:
  mode: "local" # disabled | local | localai
  localai:
    url: "${LOCALAI_URL:-}"
    model: "nomic-embed-text"
  note_chunk_bytes: 800
```

`local` computes deterministic CPU embeddings inside the Vault Bridge API and
worker containers. `localai` calls the configured LocalAI embeddings endpoint and
requires `LOCALAI_URL`. `disabled` skips embedding workers and semantic ranking;
hybrid text queries behave as full-text ranking only. When LocalAI is enabled,
note-level embeddings are chunked to `note_chunk_bytes` and aggregated back into
a single note vector so oversized notes do not wedge the worker. Heading-aware
semantic chunks are split to `block_chunk_bytes` with bounded sentence overlap
and embedded independently for section-level retrieval. See
[`docs/embedding_operations.md`](docs/embedding_operations.md) for health checks,
safe unblocking, and reindexing.

3. Edit `.env`:

Required:

- `COUCHDB_USER`
- `COUCHDB_PASS`
- `PG_PASS`

Optional:

- `LIVESYNC_PASSPHRASE` (only when LiveSync E2EE decryption is needed)
- `LOCALAI_URL` (required when `embedding.mode` is `localai`; example: `http://10.0.0.20:8080/v1/embeddings`)

REST API tokens are file-backed under `.secrets/api/*.token`; MCP bearer tokens
are file-backed under `.secrets/mcp/*.token`. The token filename stem must exist
under `api_tokens` or `mcp_tokens` in `config.yaml`, where it maps to a context.
File-backed token contents are read at auth time, so rotating an
already-configured token takes effect without a container restart.

Vault Bridge hot-reloads the auth policy sections of the loaded config file:
`api_tokens`, `mcp_tokens`, and `contexts`. Reload polling runs every 10 seconds
by default; set `CONFIG_RELOAD_INTERVAL_SECONDS=0` to disable polling. On Unix,
send `SIGHUP` to request an immediate reload. Invalid reload attempts keep the
previous good auth config active and are exposed in status, metrics, and logs.
Startup-only settings such as server bind address, database, CouchDB,
encryption, embedding, and worker/indexer runtime settings still require a
restart/redeploy.

Clients must send `Authorization: Bearer <token>` to `/mcp` and `/sse`. REST API
clients must send `X-Api-Key: <token>`.

4. Materialize token files before first start:

Docker bind-mounts `./.secrets/api` and `./.secrets/mcp` into the app container.
`just up` creates any missing token files declared by `config.yaml`
automatically, without printing token values. You can run the same step
explicitly:

```bash
just materialize-config-tokens
```

To add a token, edit `api_tokens` or `mcp_tokens` in `config.yaml` and map the
new token name to an existing context. Then rerun `just materialize-config-tokens`.
Existing token files are preserved, and the new mapping becomes active after the
next successful config reload.

For a single declared token, these commands create the missing file without
changing `config.yaml`:

```bash
just issue-api-token monitoring
just issue-mcp-token cloud-agent
```

5. Start:

```bash
just up
```

6. Verify:

```bash
docker compose ps
```

Expected: `vault-bridge-db` healthy and `vault-bridge` running.

## B) Test It Works

Run the smoke suite:

```bash
just test
```

`just test` reads secrets from `.env`, starts/updates the stack, waits for API
readiness, then validates status/metrics/MCP endpoints.

Manual checks (if needed):

1. API status:

```bash
API_TOKEN="$(tr -d '\r\n' < .secrets/api/monitoring.token)"
curl -sS -H "X-Api-Key: ${API_TOKEN}" http://127.0.0.1:8080/api/v1/status | jq
```

2. Metrics endpoint:

```bash
API_TOKEN="$(tr -d '\r\n' < .secrets/api/monitoring.token)"
curl -sS -H "X-Api-Key: ${API_TOKEN}" http://127.0.0.1:8080/api/v1/metrics | sed -n '1,40p'
```

3. MCP transport:

```bash
TOKEN="$(find .secrets/mcp -maxdepth 1 -type f -name '*.token' 2>/dev/null | sort | head -n1 | xargs -r tr -d '\r\n')"
curl -sN \
  -H "Authorization: Bearer ${TOKEN}" \
  http://127.0.0.1:8080/sse | sed -n '1,6p'
```

4. Logs:

```bash
just logs vault-bridge
just logs vault-bridge-db
```

`just logs` with no args tails all services. That includes verbose Postgres startup output.

### Log Notes

- First boot of a new Postgres volume prints a long `initdb` sequence. That is normal.
- Postgres briefly starts/stops during initialization. That is normal.
- `string is too long for tsvector` means one note body was too large for PostgreSQL full-text indexing.
  Vault Bridge now caps indexed `search_text` size during persistence; full note content is still stored.
- `pool timed out while waiting for an open connection` usually means one of:

1. `.env` missing or `PG_PASS` empty.
2. `config.yaml` has `localhost` in `database.url` instead of `postgres`.
3. `PG_PASS` changed after the volume was initialized.

- `vault-bridge API did not become ready within 60s (last /api/v1/status HTTP=401)` usually means:

1. The token file used by the smoke check does not match a configured `api_tokens.<name>` entry.
2. The latest auth config reload has not run yet or failed validation. Check
   `/api/v1/status`, `/api/v1/metrics`, or container logs for reload state.

If password changed after first boot:

```bash
just down-v
just up
```

## Sanitized Demo Path

A safe public demo should use a throwaway vault or sample notes, never a
personal production vault. Configure at least two contexts:

- A restricted external context that denies a private path or tag and allows a
  small public/demo folder.
- A write-controlled agent context that can create notes only under the
  configured inbox path and edit only notes that match explicit edit policy.

The demo sequence should prove the security boundary first, then show useful
retrieval:

1. Use the external REST or MCP token to request a known private note and confirm
   it is returned as not found or not visible.
2. Use REST `POST /api/v1/assemble-context` with public seed notes, a search
   query, and graph expansion enabled to show combined search and graph context.
3. Use the MCP `query_notes`, `get_note`, and `get_neighbors` tools from the
   same restricted context to show the agent-facing read surface.
4. Use MCP `new_note` to create a controlled demo note, then use `edit_note`
   only if the note matches the configured edit policy.
5. Confirm the created or edited note syncs through CouchDB into Obsidian.

`assemble_context` is intentionally a REST endpoint rather than an MCP tool.
MCP clients should compose the smaller note, query, and graph tools unless the
calling application deliberately uses the REST API.

## C) Add MCP Server to Claude Desktop, Claude Code, and Codex

Cloud AI clients (Claude Desktop, Claude Code, Codex) should use tokens mapped
to a restricted context.
See [Contexts](#contexts) above for why.

In the examples below, `non_personal` means the restricted cloud-agent view of
your vault. Rename it to whatever fits your config.

Replace `<host>` below with the hostname or IP of the machine running the Docker
stack, for example `vault.example.com`. Use `127.0.0.1` only when the
client runs on the same machine.

There are two separate steps:

1. On the Vault Bridge host, declare one token per client in `config.yaml`.
   Map each token name to the context it should use:

```yaml
mcp_tokens:
  claude-desktop:
    context: non_personal
  claude:
    context: non_personal
  n8n:
    context: non_personal
  codex:
    context: non_personal
```

Then run `just materialize-config-tokens` or deploy the updated config. The
materializer creates any missing `.secrets/mcp/<name>.token` files and preserves
existing files. It logs only file paths and never prints token contents.

Adding a new `mcp_tokens` entry or changing a context policy takes effect after
the next successful config reload. Rotating an existing token file does not
require a reload because token file contents are read at auth time.

Useful commands:

```bash
just list-mcp-tokens
just revoke-mcp-token claude
just issue-mcp-token home-agent
```

2. On the client machine, feed the token to the matching install helper over
stdin, or pass an explicit token file with `TOKEN_FILE=/path/to/token`. The
second positional argument is the host and defaults to `127.0.0.1`.

### n8n

Use the MCP endpoint with Bearer auth:

- URL: `http://<host>:8080/mcp` or `http://<host>:8080/sse`
- Auth: `Bearer`
- Token: contents of `.secrets/mcp/n8n.token` on the Vault Bridge host

### Claude Code

Streamable HTTP transport (`/mcp`) — preferred over SSE because Claude Code's
SSE health check can time out on remote hosts. Run this on the client machine:

```bash
just client-mcp-claude claude <host> < /path/to/claude.token
```

That installs a global Claude MCP entry in user scope by default. Override as
needed:

```bash
TOKEN_FILE=/path/to/claude.token just client-mcp-claude claude <host>
SERVER_NAME=vault-local just client-mcp-claude home-agent <host> < /path/to/home-agent.token
```

Manual equivalent:

```bash
claude mcp add --scope user --transport http vault-notes http://<host>:8080/mcp \
  --header "Authorization: Bearer <token>"
claude mcp get vault-notes
```

### Claude Desktop on Arch Linux (AUR)

Anthropic's MCP quickstart still documents Claude Desktop on macOS and Windows.
On Arch Linux, the practical route is the unofficial `claude-desktop-bin` AUR
package plus a manual MCP config that shells out to `mcp-remote`.

Install Claude Desktop with your AUR helper:

```bash
yay -S claude-desktop-bin
```

Because the Claude Desktop config below runs `npx`, make sure `npx` exists on
your Arch box. If it does not, install `npm`:

```bash
sudo pacman -S --needed npm
```

Declare and materialize a dedicated restricted-context token on the Vault Bridge
host:

```yaml
mcp_tokens:
  claude-desktop:
    context: non_personal
```

```bash
just materialize-config-tokens
```

Then on the Arch client, update Claude Desktop's Linux config file directly.
This helper creates or updates `~/.config/Claude/claude_desktop_config.json`
and preserves any other top-level keys already in the file.

Feed the token over stdin:

```bash
just client-mcp-claude-desktop claude-desktop <host> < /path/to/claude-desktop.token
```

That writes a `vault-claude-desktop` entry pointing at
`http://<host>:8080/sse`.

Useful overrides:

```bash
TOKEN_FILE=/path/to/claude-desktop.token just client-mcp-claude-desktop claude-desktop <host>
SERVER_NAME=vault-local-desktop just client-mcp-claude-desktop home-agent <host> < /path/to/home-agent.token
CONFIG_PATH="$HOME/.config/Claude/claude_desktop_config.json" just client-mcp-claude-desktop claude-desktop <host> < /path/to/claude-desktop.token
```

On Arch/AUR, Claude Desktop's `Settings -> Developer -> Edit Config` flow can
misbehave and hand a `.json` file to your default app instead of opening the
real config. If that happens, edit this file yourself:

```text
~/.config/Claude/claude_desktop_config.json
```

Config example:

```json
{
  "mcpServers": {
    "vault-claude-desktop": {
      "command": "/usr/bin/bash",
      "args": [
        "-lc",
        "exec /usr/bin/npx -y mcp-remote http://vault.example.com:8080/sse --transport sse-only --allow-http --header 'Authorization: Bearer <token>'"
      ]
    }
  }
}
```

Notes:

- Use a restricted context token for Claude Desktop.
- Use `/sse` for Claude Desktop via `mcp-remote`; Vault Bridge advertises `/mcp`
  from that SSE handshake.
- `-y` avoids an interactive `npx` install prompt when Claude launches the MCP bridge.
- `--allow-http` is required here when the MCP endpoint is plain HTTP, not HTTPS.
- The helper writes `/usr/bin/bash -lc ...` with an absolute `/usr/bin/npx` path because GUI-launched Claude Desktop sessions on Arch can have a thinner environment than your terminal.
- Fully restart Claude Desktop after changing the config file.
- This stores the bearer token in Claude Desktop's config file, so revoke and reissue it if the machine is lost or you rotate credentials:

```bash
just revoke-mcp-token claude-desktop
just issue-mcp-token claude-desktop
```

To configure a client that connects to a remote Vault Bridge host, pass the host
explicitly:

```bash
just client-mcp-claude-desktop claude-desktop vault.example.com < /path/to/claude-desktop.token
```

After restart, use Claude Desktop's Connectors UI or Developer settings to
confirm the server is connected.

### Codex CLI

Run this on the client machine and feed the token over stdin:

```bash
just client-mcp-codex codex <host> < /path/to/codex.token
```

The helper writes a persisted `http_headers` Authorization entry into
`~/.codex/config.toml`. That is the important change: for streamable HTTP MCP
servers, Codex's `--bearer-token-env-var` flow only stores an env var name, so
it is not enough by itself unless you manage that env var outside Codex.

Override the token file or server name as needed:

```bash
TOKEN_FILE=/path/to/codex.token just client-mcp-codex codex <host>
SERVER_NAME=vault-local just client-mcp-codex home-agent <host> < /path/to/home-agent.token
```

Manual equivalent:

```toml
[mcp_servers.vault-notes]
url = "http://<host>:8080/mcp"
http_headers = { Authorization = "Bearer <token>" }
```

## Prometheus and Alertmanager

`docker-compose.yml` no longer runs Prometheus, Alertmanager, or LocalAI.

What Vault Bridge sends:

- To Prometheus: nothing (push model is not used). Prometheus must scrape `GET /api/v1/metrics`.
- To Alertmanager: nothing directly. Alertmanager only receives alerts from your Prometheus rules.

Prometheus scrapes must send one of the configured file-backed API tokens as an
`X-Api-Key` header:

```yaml
scrape_configs:
  - job_name: vault_bridge
    metrics_path: /api/v1/metrics
    http_headers:
      X-Api-Key:
        files:
          - /run/secrets/vault_bridge_api_token
    static_configs:
      - targets:
          - vault-bridge:8080
```

Metrics exposed by `GET /api/v1/metrics` include:

- `vault_bridge_sync_behind_by`
- `vault_bridge_pending_embeddings`
- `vault_bridge_quarantined_embeddings`
- `vault_bridge_pending_chunk_embeddings`
- `vault_bridge_quarantined_chunk_embeddings`
- `vault_bridge_embedding_backend_degraded`
- `vault_bridge_embedding_last_success_timestamp_seconds`
- `vault_bridge_embedding_last_error_timestamp_seconds`
- `vault_bridge_pending_chunks`
- `vault_bridge_orphan_leaf_staging`
- `vault_bridge_total_notes`
- `vault_bridge_total_links`
- `vault_bridge_total_tags`
- `vault_bridge_config_reload_enabled`
- `vault_bridge_config_reload_generation`
- `vault_bridge_config_reload_success_total`
- `vault_bridge_config_reload_failure_total`
- `vault_bridge_config_reload_last_success_timestamp_seconds`
- `vault_bridge_config_reload_last_failure_timestamp_seconds`
- `vault_bridge_accessible_notes{context="..."}`
- `vault_bridge_filtered_notes{context="..."}`

### Grafana dashboard

An importable Grafana dashboard is available at
`monitoring/grafana/dashboards/vault-bridge-health.json`.

The dashboard uses only the Prometheus metrics listed above. It does not require
HTTP request counters or latency histograms. During import, select your
Prometheus datasource and set the `job` variable to the scrape job that collects
Vault Bridge metrics. The example Prometheus config in this repo uses
`vault_bridge`.

For provisioned Grafana installs, copy the JSON file into your dashboard
provisioning path and point its datasource variable at the Prometheus datasource
used for Vault Bridge.

## Common Commands

```bash
just up
just down
just down-v
just test
just test-status
just test-metrics
just test-mcp
just logs vault-bridge
just logs vault-bridge-db
```

## Development Validation

```bash
cargo clippy --all-targets --all-features -- -D warnings
cargo test
```

Optional live validation harnesses:

```bash
./scripts/run_live_validation.sh
./scripts/run_live_mcp_validation.sh
```

## Advanced Docs

- `docs/live_validation_runbook.md`
- `docs/live_mcp_validation_runbook.md`
- `docs/hnsw_benchmark.md`

## License

Vault Bridge is distributed under the Apache License, Version 2.0. See
[LICENSE](LICENSE).

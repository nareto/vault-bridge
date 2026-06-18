set dotenv-load := true

# Local source-build helpers

up:
    python3 scripts/materialize_config_tokens.py
    docker compose up -d --build

deploy: up

down:
    docker compose down

down-v:
    docker compose down -v

logs *args:
    docker compose logs --tail=200 -f {{args}}

materialize-config-tokens:
    python3 scripts/materialize_config_tokens.py

test: _require-test-env
    just up
    just _wait-for-api
    just test-status
    just test-metrics
    just test-mcp
    @echo "All smoke checks passed."

test-status: _require-test-env
    api_token="$(tr -d '\r\n' < "${API_TOKEN_FILE:-.secrets/api/monitoring.token}")"; curl -fsS -H "X-Api-Key: ${api_token}" http://127.0.0.1:8080/api/v1/status

test-metrics: _require-test-env
    api_token="$(tr -d '\r\n' < "${API_TOKEN_FILE:-.secrets/api/monitoring.token}")"; curl -fsS -H "X-Api-Key: ${api_token}" http://127.0.0.1:8080/api/v1/metrics | grep -q '^vault_bridge_total_notes'

test-mcp:
    #!/usr/bin/env bash
    set -euo pipefail

    first_token() {
      local dir=".secrets/mcp"
      [[ -d "${dir}" ]] || return 0
      local token_file
      token_file="$(find "${dir}" -maxdepth 1 -type f -name '*.token' | sort | head -n1)"
      [[ -n "${token_file}" ]] || return 0
      tr -d '\r\n' < "${token_file}"
    }

    check_mcp() {
      local token="${1:-}"
      local -a args=(-fsS --max-time 5)
      if [[ -n "${token}" ]]; then
        args+=(-H "Authorization: Bearer ${token}")
      fi
      curl "${args[@]}" "http://127.0.0.1:8080/sse" 2>/dev/null | sed -n '1,6p' | grep -q '/mcp'
    }

    check_mcp "$(first_token)"

issue-api-token NAME:
    #!/usr/bin/env bash
    set -euo pipefail

    name="{{NAME}}"
    name="${name#NAME=}"
    name="${name#\"}"
    name="${name%\"}"
    if [[ -z "${name}" ]]; then
      echo "NAME is required, e.g. just issue-api-token monitoring" >&2
      exit 1
    fi
    if [[ ! "${name}" =~ ^[A-Za-z0-9._-]+$ ]]; then
      echo "NAME may contain only letters, digits, dot, underscore, and hyphen" >&2
      exit 1
    fi

    TOKEN_KIND=api TOKEN_NAME="${name}" python3 scripts/materialize_config_tokens.py

issue-api-tokens NAME:
    @just issue-api-token NAME={{NAME}}

revoke-api-token NAME:
    #!/usr/bin/env bash
    set -euo pipefail

    name="{{NAME}}"
    name="${name#\"}"
    name="${name%\"}"
    if [[ -z "${name}" ]]; then
      echo "NAME is required, e.g. just revoke-api-token monitoring" >&2
      exit 1
    fi

    path=".secrets/api/${name}.token"
    rm -f "${path}"

list-api-tokens:
    #!/usr/bin/env bash
    set -euo pipefail

    dir=".secrets/api"
    if [[ ! -d "${dir}" ]]; then
      exit 0
    fi
    find "${dir}" -maxdepth 1 -type f -name '*.token' -printf '%f\n' | sed 's/\.token$//' | sort

issue-mcp-token NAME:
    #!/usr/bin/env bash
    set -euo pipefail

    name="{{NAME}}"
    name="${name#NAME=}"
    name="${name#\"}"
    name="${name%\"}"
    if [[ -z "${name}" ]]; then
      echo "NAME is required, e.g. just issue-mcp-token claude" >&2
      exit 1
    fi
    if [[ ! "${name}" =~ ^[A-Za-z0-9._-]+$ ]]; then
      echo "NAME may contain only letters, digits, dot, underscore, and hyphen" >&2
      exit 1
    fi

    TOKEN_KIND=mcp TOKEN_NAME="${name}" python3 scripts/materialize_config_tokens.py

issue-mcp-tokens NAME:
    @just issue-mcp-token NAME={{NAME}}

revoke-mcp-token NAME:
    #!/usr/bin/env bash
    set -euo pipefail

    name="{{NAME}}"
    name="${name#\"}"
    name="${name%\"}"
    if [[ -z "${name}" ]]; then
      echo "NAME is required, e.g. just revoke-mcp-token NAME=claude" >&2
      exit 1
    fi

    path=".secrets/mcp/${name}.token"
    rm -f "${path}"

list-mcp-tokens:
    #!/usr/bin/env bash
    set -euo pipefail

    dir=".secrets/mcp"
    if [[ ! -d "${dir}" ]]; then
      exit 0
    fi
    find "${dir}" -maxdepth 1 -type f -name '*.token' -printf '%f\n' | sed 's/\.token$//' | sort

reindex-blocks:
    docker compose exec vault-bridge /usr/local/bin/vault_bridge --reindex-blocks

[positional-arguments]
delete-note *ARGS:
    #!/usr/bin/env bash
    set -euo pipefail

    note_path=""
    delete_leaf=""
    config_file=""

    assign_delete_arg() {
      local arg="$1"
      [[ -n "${arg}" ]] || return 0
      case "${arg}" in
        NOTE_PATH=*|NOTE=*)
          note_path="${arg#*=}"
          ;;
        DELETE_LEAF=*)
          delete_leaf="${arg#DELETE_LEAF=}"
          ;;
        CONFIG_FILE=*)
          config_file="${arg#CONFIG_FILE=}"
          ;;
        *)
          if [[ -z "${note_path}" ]]; then
            note_path="${arg}"
          elif [[ -z "${delete_leaf}" ]]; then
            delete_leaf="${arg}"
          elif [[ -z "${config_file}" ]]; then
            config_file="${arg}"
          fi
          ;;
      esac
    }

    for arg in "$@"; do
      assign_delete_arg "${arg}"
    done

    note_path="${note_path#\"}"
    note_path="${note_path%\"}"
    if [[ -z "${note_path}" ]]; then
      echo "NOTE_PATH is required, e.g. just delete-note NOTE_PATH='00New/2026-03-15-bad-note.md'" >&2
      exit 1
    fi

    delete_leaf="${delete_leaf#\"}"
    delete_leaf="${delete_leaf%\"}"
    [[ -n "${delete_leaf}" ]] || delete_leaf="true"

    case "${delete_leaf,,}" in
      1|true|yes) delete_leaf=true ;;
      0|false|no) delete_leaf=false ;;
      *)
        echo "DELETE_LEAF must be one of: true, false" >&2
        exit 1
        ;;
    esac

    config_file="${config_file#\"}"
    config_file="${config_file%\"}"
    [[ -n "${config_file}" ]] || config_file="${CONFIG_PATH:-config.yaml}"
    if [[ ! -f "${config_file}" ]]; then
      echo "Config file not found: ${config_file}" >&2
      exit 1
    fi

    strip_quotes() {
      local value="$1"
      if [[ "${value}" == \"*\" && "${value}" == *\" ]]; then
        value="${value:1:${#value}-2}"
      elif [[ "${value}" == \'*\' && "${value}" == *\' ]]; then
        value="${value:1:${#value}-2}"
      fi
      printf '%s' "${value}"
    }

    yaml_value() {
      local section="$1"
      local key="$2"
      awk -v section="${section}" -v key="${key}" '
        $0 ~ "^[[:space:]]*" section ":[[:space:]]*$" { in_section = 1; next }
        in_section && $0 ~ "^[^[:space:]]" { in_section = 0 }
        in_section && $0 ~ "^[[:space:]][[:space:]]" key ":[[:space:]]*" {
          line = $0
          sub("^[[:space:]][[:space:]]" key ":[[:space:]]*", "", line)
          sub(/[[:space:]]+#.*$/, "", line)
          print line
          exit
        }
      ' "${config_file}"
    }

    expand_config_value() {
      local value="$1"
      local prefix var_name has_default default_value suffix replacement
      while [[ "${value}" =~ ^(.*)\$\{([A-Za-z_][A-Za-z0-9_]*)(:-([^}]*))?\}(.*)$ ]]; do
        prefix="${BASH_REMATCH[1]}"
        var_name="${BASH_REMATCH[2]}"
        has_default="${BASH_REMATCH[3]}"
        default_value="${BASH_REMATCH[4]}"
        suffix="${BASH_REMATCH[5]}"
        replacement="${!var_name-}"
        if [[ -z "${replacement}" && -n "${has_default}" ]]; then
          replacement="${default_value}"
        fi
        value="${prefix}${replacement}${suffix}"
      done
      printf '%s' "${value}"
    }

    config_value() {
      local key="$1"
      local raw
      raw="$(yaml_value couchdb "${key}")"
      if [[ -z "${raw}" ]]; then
        echo "Missing couchdb.${key} in ${config_file}" >&2
        exit 1
      fi
      raw="$(strip_quotes "${raw}")"
      expand_config_value "${raw}"
    }

    urlencode() {
      python3 -c 'import sys, urllib.parse; print(urllib.parse.quote(sys.argv[1], safe=""), end="")' "$1"
    }

    json_rev() {
      python3 -c 'import json, sys; print(json.load(sys.stdin).get("_rev", ""), end="")'
    }

    fetch_rev() {
      local doc_id="$1"
      local encoded_id response_file status
      encoded_id="$(urlencode "${doc_id}")"
      response_file="$(mktemp)"
      status="$(curl -sS -u "${couchdb_username}:${couchdb_password}" -o "${response_file}" -w '%{http_code}' "${db_base_url}/${encoded_id}")"
      case "${status}" in
        200)
          json_rev < "${response_file}"
          rm -f "${response_file}"
          return 0
          ;;
        404)
          rm -f "${response_file}"
          return 1
          ;;
        *)
          cat "${response_file}" >&2
          rm -f "${response_file}"
          echo "Failed to fetch ${doc_id} from CouchDB (HTTP ${status})" >&2
          return 2
          ;;
      esac
    }

    delete_doc() {
      local doc_id="$1"
      local rev="$2"
      local encoded_id encoded_rev
      encoded_id="$(urlencode "${doc_id}")"
      encoded_rev="$(urlencode "${rev}")"
      curl -fsS -u "${couchdb_username}:${couchdb_password}" -X DELETE \
        "${db_base_url}/${encoded_id}?rev=${encoded_rev}" >/dev/null
    }

    couchdb_url="$(config_value url)"
    couchdb_database="$(config_value database)"
    couchdb_username="$(config_value username)"
    couchdb_password="$(config_value password)"
    db_base_url="${couchdb_url%/}/${couchdb_database}"

    note_path="${note_path#/}"
    note_path="${note_path//\\//}"
    lowercase_note_path="${note_path,,}"
    file_ids=(
      "${lowercase_note_path}"
      "f:$(printf 'file:%s' "${note_path}" | sha256sum | awk '{print $1}')"
    )
    leaf_ids=(
      "h:$(printf '%s' "${lowercase_note_path}" | sha256sum | awk '{print substr($1,1,16)}')"
      "h:+$(printf 'leaf:%s' "${note_path}" | sha256sum | awk '{print substr($1,1,16)}')"
    )

    deleted_any=false
    deleted_doc_ids=()

    for doc_id in "${file_ids[@]}"; do
      if file_rev="$(fetch_rev "${doc_id}")"; then
        delete_doc "${doc_id}" "${file_rev}"
        deleted_any=true
        deleted_doc_ids+=("${doc_id}")
      fi
    done

    if [[ "${delete_leaf}" == "true" ]]; then
      for doc_id in "${leaf_ids[@]}"; do
        if leaf_rev="$(fetch_rev "${doc_id}")"; then
          delete_doc "${doc_id}" "${leaf_rev}"
          deleted_any=true
          deleted_doc_ids+=("${doc_id}")
        fi
      done
    fi

    if [[ "${deleted_any}" != "true" ]]; then
      fallback_args=(run --rm --build --no-deps vault-bridge --delete-note-scan "${note_path}")
      if [[ "${delete_leaf}" != "true" ]]; then
        fallback_args+=(--keep-leaves)
      fi
      if fallback_output="$(docker compose "${fallback_args[@]}" 2>&1)"; then
        printf '%s\n' "${fallback_output}"
        exit 0
      fi
      printf '%s\n' "${fallback_output}" >&2
      echo "No matching LiveSync docs found for ${note_path}" >&2
      exit 1
    fi

    printf 'note_path=%s deleted_doc_ids=%s\n' \
      "${note_path}" "${deleted_doc_ids[*]}"

[positional-arguments]
debug-note *ARGS:
    #!/usr/bin/env bash
    set -euo pipefail

    note_path=""
    config_file=""
    api_base_url=""
    force_scan=""

    assign_debug_arg() {
      local arg="$1"
      [[ -n "${arg}" ]] || return 0
      case "${arg}" in
        NOTE_PATH=*|NOTE=*)
          note_path="${arg#*=}"
          ;;
        CONFIG_FILE=*)
          config_file="${arg#CONFIG_FILE=}"
          ;;
        API_BASE_URL=*)
          api_base_url="${arg#API_BASE_URL=}"
          ;;
        FORCE_SCAN=*)
          force_scan="${arg#FORCE_SCAN=}"
          ;;
        *)
          if [[ -z "${note_path}" ]]; then
            note_path="${arg}"
          elif [[ -z "${config_file}" ]]; then
            config_file="${arg}"
          elif [[ -z "${api_base_url}" ]]; then
            api_base_url="${arg}"
          elif [[ -z "${force_scan}" ]]; then
            force_scan="${arg}"
          fi
          ;;
      esac
    }

    for arg in "$@"; do
      assign_debug_arg "${arg}"
    done

    note_path="${note_path#\"}"
    note_path="${note_path%\"}"
    if [[ -z "${note_path}" ]]; then
      echo "NOTE_PATH is required, e.g. just debug-note NOTE_PATH='00New/2026-03-15-daily-newsletter.md'" >&2
      exit 1
    fi

    config_file="${config_file#\"}"
    config_file="${config_file%\"}"
    [[ -n "${config_file}" ]] || config_file="${CONFIG_PATH:-config.yaml}"
    if [[ ! -f "${config_file}" ]]; then
      echo "Config file not found: ${config_file}" >&2
      exit 1
    fi

    api_base_url="${api_base_url#\"}"
    api_base_url="${api_base_url%\"}"

    force_scan="${force_scan#\"}"
    force_scan="${force_scan%\"}"
    [[ -n "${force_scan}" ]] || force_scan="false"

    strip_quotes() {
      local value="$1"
      if [[ "${value}" == \"*\" && "${value}" == *\" ]]; then
        value="${value:1:${#value}-2}"
      elif [[ "${value}" == \'*\' && "${value}" == *\' ]]; then
        value="${value:1:${#value}-2}"
      fi
      printf '%s' "${value}"
    }

    yaml_value() {
      local section="$1"
      local key="$2"
      awk -v section="${section}" -v key="${key}" '
        $0 ~ "^[[:space:]]*" section ":[[:space:]]*$" { in_section = 1; next }
        in_section && $0 ~ "^[^[:space:]]" { in_section = 0 }
        in_section && $0 ~ "^[[:space:]][[:space:]]" key ":[[:space:]]*" {
          line = $0
          sub("^[[:space:]][[:space:]]" key ":[[:space:]]*", "", line)
          sub(/[[:space:]]+#.*$/, "", line)
          print line
          exit
        }
      ' "${config_file}"
    }

    expand_config_value() {
      local value="$1"
      local prefix var_name has_default default_value suffix replacement
      while [[ "${value}" =~ ^(.*)\$\{([A-Za-z_][A-Za-z0-9_]*)(:-([^}]*))?\}(.*)$ ]]; do
        prefix="${BASH_REMATCH[1]}"
        var_name="${BASH_REMATCH[2]}"
        has_default="${BASH_REMATCH[3]}"
        default_value="${BASH_REMATCH[4]}"
        suffix="${BASH_REMATCH[5]}"
        replacement="${!var_name-}"
        if [[ -z "${replacement}" && -n "${has_default}" ]]; then
          replacement="${default_value}"
        fi
        value="${prefix}${replacement}${suffix}"
      done
      printf '%s' "${value}"
    }

    config_value() {
      local section="$1"
      local key="$2"
      local raw
      raw="$(yaml_value "${section}" "${key}")"
      if [[ -z "${raw}" ]]; then
        echo "Missing ${section}.${key} in ${config_file}" >&2
        exit 1
      fi
      raw="$(strip_quotes "${raw}")"
      expand_config_value "${raw}"
    }

    optional_config_value() {
      local section="$1"
      local key="$2"
      local raw
      raw="$(yaml_value "${section}" "${key}")"
      if [[ -z "${raw}" ]]; then
        return 0
      fi
      raw="$(strip_quotes "${raw}")"
      expand_config_value "${raw}"
    }

    urlencode() {
      python3 -c 'import sys, urllib.parse; print(urllib.parse.quote(sys.argv[1], safe=""), end="")' "$1"
    }

    print_json_or_raw() {
      local file="$1"
      if command -v jq >/dev/null 2>&1; then
        jq . < "${file}" 2>/dev/null || cat "${file}"
      else
        cat "${file}"
      fi
    }

    print_doc() {
      local label="$1"
      local doc_id="$2"
      local encoded_id response_file status
      encoded_id="$(urlencode "${doc_id}")"
      response_file="$(mktemp)"
      status="$(curl -sS -u "${couchdb_username}:${couchdb_password}" -o "${response_file}" -w '%{http_code}' "${db_base_url}/${encoded_id}")"

      echo
      echo "=== ${label} (${doc_id}) HTTP ${status} ==="
      case "${status}" in
        200)
          print_json_or_raw "${response_file}"
          ;;
        404)
          echo "not found"
          ;;
        *)
          cat "${response_file}"
          ;;
      esac
      rm -f "${response_file}"
    }

    couchdb_url="$(config_value couchdb url)"
    couchdb_database="$(config_value couchdb database)"
    couchdb_username="$(config_value couchdb username)"
    couchdb_password="$(config_value couchdb password)"
    api_token_file="${API_TOKEN_FILE:-.secrets/api/monitoring.token}"
    if [[ ! -f "${api_token_file}" ]]; then
      echo "Missing ${api_token_file}. Add the matching api_tokens entry to config.yaml and run: just materialize-config-tokens" >&2
      exit 1
    fi
    api_key="$(tr -d '\r\n' < "${api_token_file}")"
    server_port="$(optional_config_value server port)"
    new_note_base_path="$(optional_config_value new_note base_path)"
    new_note_path_template="$(optional_config_value new_note path_template)"

    [[ -n "${server_port}" ]] || server_port="8080"
    [[ -n "${api_base_url}" ]] || api_base_url="http://127.0.0.1:${server_port}"
    db_base_url="${couchdb_url%/}/${couchdb_database}"

    note_path="${note_path#/}"
    note_path="${note_path//\\//}"
    encoded_note_path="$(urlencode "${note_path}")"
    lowercase_note_path="${note_path,,}"
    file_id="${lowercase_note_path}"
    leaf_id="h:$(printf '%s' "${lowercase_note_path}" | sha256sum | awk '{print substr($1,1,16)}')"
    legacy_file_id="f:$(printf 'file:%s' "${note_path}" | sha256sum | awk '{print $1}')"
    legacy_leaf_id="h:+$(printf 'leaf:%s' "${note_path}" | sha256sum | awk '{print substr($1,1,16)}')"

    api_response_file="$(mktemp)"
    trap 'rm -f "${api_response_file}"' EXIT
    api_status="$(curl -sS -H "X-Api-Key: ${api_key}" -o "${api_response_file}" -w '%{http_code}' "${api_base_url%/}/api/v1/notes/${encoded_note_path}")"

    echo "Config file: ${config_file}"
    echo "API base URL: ${api_base_url%/}"
    echo "CouchDB database: ${db_base_url}"
    if [[ -n "${new_note_base_path}" || -n "${new_note_path_template}" ]]; then
      echo "new_note.base_path: ${new_note_base_path:-<unset>}"
      echo "new_note.path_template: ${new_note_path_template:-<unset>}"
    fi
    echo "note_path: ${note_path}"
    echo "file_id: ${file_id}"
    echo "leaf_id: ${leaf_id}"
    echo "legacy_file_id: ${legacy_file_id}"
    echo "legacy_leaf_id: ${legacy_leaf_id}"

    echo
    echo "=== API note lookup HTTP ${api_status} ==="
    print_json_or_raw "${api_response_file}"

    print_doc "file doc" "${file_id}"
    print_doc "leaf doc" "${leaf_id}"
    print_doc "legacy file doc" "${legacy_file_id}"
    print_doc "legacy leaf doc" "${legacy_leaf_id}"

    if [[ "${api_status}" == "404" || "${force_scan,,}" == "true" ]]; then
      echo
      echo "=== path scan via Rust decryptor ==="
      docker compose run --rm --build --no-deps vault-bridge --debug-note-scan "${note_path}"
    fi

    trap - EXIT
    rm -f "${api_response_file}"

client-mcp-claude NAME HOST="127.0.0.1":
    #!/usr/bin/env bash
    set -euo pipefail

    name="{{NAME}}"
    name="${name#NAME=}"
    name="${name#\"}"
    name="${name%\"}"
    [[ -n "${name}" ]] || { echo "NAME is required" >&2; exit 1; }
    if [[ ! "${name}" =~ ^[A-Za-z0-9._-]+$ ]]; then
      echo "NAME may contain only letters, digits, dot, underscore, and hyphen" >&2
      exit 1
    fi

    host="{{HOST}}"
    host="${host#HOST=}"
    host="${host#\"}"
    host="${host%\"}"
    [[ -n "${host}" ]] || { echo "HOST is required" >&2; exit 1; }

    server_name="${SERVER_NAME:-}"
    claude_scope="${CLAUDE_SCOPE:-user}"
    port="${PORT:-8080}"
    case "${claude_scope}" in
      local|user|project) ;;
      *)
        echo "CLAUDE_SCOPE must be one of: local, user, project" >&2
        exit 1
        ;;
    esac

    if [[ -z "${server_name}" ]]; then
      server_name="vault-${name}"
    fi
    if [[ ! "${server_name}" =~ ^[A-Za-z0-9_-]+$ ]]; then
      echo "SERVER_NAME may contain only letters, digits, underscore, and hyphen" >&2
      exit 1
    fi

    read_mcp_token() {
      local token=""
      if [[ -n "${TOKEN_FILE:-}" ]]; then
        if [[ ! -f "${TOKEN_FILE}" ]]; then
          echo "TOKEN_FILE not found: ${TOKEN_FILE}" >&2
          exit 1
        fi
        token="$(tr -d '\r\n' < "${TOKEN_FILE}")"
      elif [[ -t 0 ]]; then
        printf 'Paste MCP bearer token for %s, then press Enter: ' "${name}" >&2
        IFS= read -r -s token || true
        printf '\n' >&2
      else
        IFS= read -r token || true
      fi
      token="${token//$'\r'/}"
      token="${token//$'\n'/}"
      if [[ -z "${token}" ]]; then
        echo "MCP bearer token is required on stdin or via TOKEN_FILE=/path/to/token" >&2
        exit 1
      fi
      printf '%s' "${token}"
    }
    token="$(read_mcp_token)"

    url="http://${host}:${port}/mcp"
    claude mcp remove "${server_name}" 2>/dev/null || true
    claude mcp add --scope "${claude_scope}" --transport http "${server_name}" "${url}" \
      --header "Authorization: Bearer ${token}"
    printf 'Updated Claude MCP server %s -> %s\n' "${server_name}" "${url}"

client-mcp-claude-desktop NAME HOST="127.0.0.1":
    #!/usr/bin/env bash
    set -euo pipefail

    name="{{NAME}}"
    name="${name#NAME=}"
    name="${name#\"}"
    name="${name%\"}"
    [[ -n "${name}" ]] || { echo "NAME is required" >&2; exit 1; }
    if [[ ! "${name}" =~ ^[A-Za-z0-9._-]+$ ]]; then
      echo "NAME may contain only letters, digits, dot, underscore, and hyphen" >&2
      exit 1
    fi

    host="{{HOST}}"
    host="${host#HOST=}"
    host="${host#\"}"
    host="${host%\"}"
    [[ -n "${host}" ]] || { echo "HOST is required" >&2; exit 1; }

    server_name="${SERVER_NAME:-}"
    config_path="${CONFIG_PATH:-}"
    port="${PORT:-8080}"

    if [[ -z "${server_name}" ]]; then
      server_name="vault-${name}"
    fi
    if [[ ! "${server_name}" =~ ^[A-Za-z0-9._-]+$ ]]; then
      echo "SERVER_NAME may contain only letters, digits, dot, underscore, and hyphen" >&2
      exit 1
    fi

    if [[ -z "${config_path}" ]]; then
      config_home="${XDG_CONFIG_HOME:-${HOME}/.config}"
      config_path="${config_home}/Claude/claude_desktop_config.json"
    fi

    read_mcp_token() {
      local token=""
      if [[ -n "${TOKEN_FILE:-}" ]]; then
        if [[ ! -f "${TOKEN_FILE}" ]]; then
          echo "TOKEN_FILE not found: ${TOKEN_FILE}" >&2
          exit 1
        fi
        token="$(tr -d '\r\n' < "${TOKEN_FILE}")"
      elif [[ -t 0 ]]; then
        printf 'Paste MCP bearer token for %s, then press Enter: ' "${name}" >&2
        IFS= read -r -s token || true
        printf '\n' >&2
      else
        IFS= read -r token || true
      fi
      token="${token//$'\r'/}"
      token="${token//$'\n'/}"
      if [[ -z "${token}" ]]; then
        echo "MCP bearer token is required on stdin or via TOKEN_FILE=/path/to/token" >&2
        exit 1
      fi
      printf '%s' "${token}"
    }
    token="$(read_mcp_token)"

    node_bin="${NODE_BIN:-/usr/bin/node}"
    if [[ ! -x "${node_bin}" ]]; then
      node_bin="$(command -v node || true)"
    fi
    [[ -n "${node_bin}" ]] || { echo "Could not find node" >&2; exit 1; }

    npx_cli="${NPX_CLI:-/usr/lib/node_modules/npm/bin/npx-cli.js}"
    if [[ ! -f "${npx_cli}" ]]; then
      npx_bin="${NPX_BIN:-}"
      if [[ -z "${npx_bin}" ]]; then
        npx_bin="$(command -v npx || true)"
      fi
      [[ -n "${npx_bin}" ]] || { echo "Could not find npx" >&2; exit 1; }
      npx_cli="${npx_bin}"
    fi

    url="http://${host}:${port}/sse"

    claude_desktop_path="${CLAUDE_DESKTOP_PATH:-/usr/bin:/bin}"

    CONFIG_PATH="${config_path}" SERVER_NAME="${server_name}" URL="${url}" AUTH_TOKEN="${token}" NODE_BIN="${node_bin}" NPX_CLI="${npx_cli}" CLAUDE_DESKTOP_PATH="${claude_desktop_path}" \
      python3 -c $'import json\nimport os\nimport sys\nfrom pathlib import Path\n\nconfig_path = Path(os.environ["CONFIG_PATH"]).expanduser()\nserver_name = os.environ["SERVER_NAME"]\nurl = os.environ["URL"]\nauth_token = os.environ["AUTH_TOKEN"]\nnode_bin = os.environ["NODE_BIN"]\nnpx_cli = os.environ["NPX_CLI"]\nclaude_desktop_path = os.environ["CLAUDE_DESKTOP_PATH"]\n\nif config_path.exists():\n    raw = config_path.read_text()\n    if raw.strip():\n        try:\n            payload = json.loads(raw)\n        except json.JSONDecodeError as exc:\n            print(f"Existing Claude Desktop config is not valid JSON: {exc}", file=sys.stderr)\n            sys.exit(1)\n    else:\n        payload = {}\nelse:\n    payload = {}\n\nif not isinstance(payload, dict):\n    print("Claude Desktop config root must be a JSON object", file=sys.stderr)\n    sys.exit(1)\n\nmcp_servers = payload.get("mcpServers")\nif mcp_servers is None:\n    mcp_servers = {}\n    payload["mcpServers"] = mcp_servers\nelif not isinstance(mcp_servers, dict):\n    print("Claude Desktop config field mcpServers must be a JSON object", file=sys.stderr)\n    sys.exit(1)\n\nmcp_servers[server_name] = {\n    "command": node_bin,\n    "args": [\n        npx_cli,\n        "-y",\n        "mcp-remote",\n        url,\n        "--transport",\n        "sse-only",\n        "--allow-http",\n        "--silent",\n        "--header",\n        "Authorization:${AUTH_HEADER}",\n    ],\n    "env": {\n        "AUTH_HEADER": f"Bearer {auth_token}",\n        "PATH": claude_desktop_path,\n    },\n}\n\nconfig_path.parent.mkdir(parents=True, exist_ok=True)\nconfig_path.write_text(json.dumps(payload, indent=2) + "\\n")\nprint(f"Updated {config_path} with MCP server {server_name}")\n'

client-mcp-codex NAME HOST="127.0.0.1":
    #!/usr/bin/env bash
    set -euo pipefail

    name="{{NAME}}"
    name="${name#NAME=}"
    name="${name#\"}"
    name="${name%\"}"
    [[ -n "${name}" ]] || { echo "NAME is required" >&2; exit 1; }
    if [[ ! "${name}" =~ ^[A-Za-z0-9._-]+$ ]]; then
      echo "NAME may contain only letters, digits, dot, underscore, and hyphen" >&2
      exit 1
    fi

    host="{{HOST}}"
    host="${host#HOST=}"
    host="${host#\"}"
    host="${host%\"}"
    [[ -n "${host}" ]] || { echo "HOST is required" >&2; exit 1; }

    server_name="${SERVER_NAME:-}"
    port="${PORT:-8080}"

    if [[ -z "${server_name}" ]]; then
      server_name="vault-${name}"
    fi
    if [[ ! "${server_name}" =~ ^[A-Za-z0-9_-]+$ ]]; then
      echo "SERVER_NAME may contain only letters, digits, underscore, and hyphen" >&2
      exit 1
    fi

    read_mcp_token() {
      local token=""
      if [[ -n "${TOKEN_FILE:-}" ]]; then
        if [[ ! -f "${TOKEN_FILE}" ]]; then
          echo "TOKEN_FILE not found: ${TOKEN_FILE}" >&2
          exit 1
        fi
        token="$(tr -d '\r\n' < "${TOKEN_FILE}")"
      elif [[ -t 0 ]]; then
        printf 'Paste MCP bearer token for %s, then press Enter: ' "${name}" >&2
        IFS= read -r -s token || true
        printf '\n' >&2
      else
        IFS= read -r token || true
      fi
      token="${token//$'\r'/}"
      token="${token//$'\n'/}"
      if [[ -z "${token}" ]]; then
        echo "MCP bearer token is required on stdin or via TOKEN_FILE=/path/to/token" >&2
        exit 1
      fi
      printf '%s' "${token}"
    }
    token="$(read_mcp_token)"

    config_dir="${HOME}/.codex"
    config_path="${config_dir}/config.toml"
    url="http://${host}:${port}/mcp"

    mkdir -p "${config_dir}"
    touch "${config_path}"
    codex mcp remove "${server_name}" >/dev/null 2>&1 || true

    if [[ -s "${config_path}" ]] && [[ "$(tail -c1 "${config_path}")" != $'\n' ]]; then
      printf '\n' >> "${config_path}"
    fi

    {
      printf '\n[mcp_servers.%s]\n' "${server_name}"
      printf 'url = "%s"\n' "${url}"
      printf 'http_headers = { Authorization = "Bearer %s" }\n' "${token}"
    } >> "${config_path}"

    printf 'Updated %s with MCP server %s -> %s\n' "${config_path}" "${server_name}" "${url}"

client-mcp-opencode NAME HOST="127.0.0.1":
    #!/usr/bin/env bash
    set -euo pipefail

    name="{{NAME}}"
    name="${name#NAME=}"
    name="${name#\"}"
    name="${name%\"}"
    [[ -n "${name}" ]] || { echo "NAME is required" >&2; exit 1; }
    if [[ ! "${name}" =~ ^[A-Za-z0-9._-]+$ ]]; then
      echo "NAME may contain only letters, digits, dot, underscore, and hyphen" >&2
      exit 1
    fi

    host="{{HOST}}"
    host="${host#HOST=}"
    host="${host#\"}"
    host="${host%\"}"
    [[ -n "${host}" ]] || { echo "HOST is required" >&2; exit 1; }

    server_name="${SERVER_NAME:-}"
    config_path_override="${CONFIG_PATH:-}"
    port="${PORT:-8080}"

    if [[ -z "${server_name}" ]]; then
      server_name="vault-${name}"
    fi
    if [[ ! "${server_name}" =~ ^[A-Za-z0-9._-]+$ ]]; then
      echo "SERVER_NAME may contain only letters, digits, dot, underscore, and hyphen" >&2
      exit 1
    fi

    config_home="${XDG_CONFIG_HOME:-${HOME}/.config}"
    default_config_path="${config_home}/opencode/opencode.json"
    if [[ -n "${config_path_override}" ]]; then
      config_path="${config_path_override}"
    else
      config_path="${default_config_path}"
    fi

    read_mcp_token() {
      local token=""
      if [[ -n "${TOKEN_FILE:-}" ]]; then
        if [[ ! -f "${TOKEN_FILE}" ]]; then
          echo "TOKEN_FILE not found: ${TOKEN_FILE}" >&2
          exit 1
        fi
        token="$(tr -d '\r\n' < "${TOKEN_FILE}")"
      elif [[ -t 0 ]]; then
        printf 'Paste MCP bearer token for %s, then press Enter: ' "${name}" >&2
        IFS= read -r -s token || true
        printf '\n' >&2
      else
        IFS= read -r token || true
      fi
      token="${token//$'\r'/}"
      token="${token//$'\n'/}"
      if [[ -z "${token}" ]]; then
        echo "MCP bearer token is required on stdin or via TOKEN_FILE=/path/to/token" >&2
        exit 1
      fi
      printf '%s' "${token}"
    }
    token="$(read_mcp_token)"

    mcp_url="http://${host}:${port}/mcp"

    CONFIG_PATH="${config_path}" SERVER_NAME="${server_name}" MCP_URL="${mcp_url}" TOKEN="${token}" \
      python3 -c $'import json\nimport os\nimport sys\nfrom pathlib import Path\n\nconfig_path = Path(os.environ["CONFIG_PATH"]).expanduser()\nserver_name = os.environ["SERVER_NAME"]\nmcp_url = os.environ["MCP_URL"]\ntoken = os.environ["TOKEN"]\n\nconfig_path.parent.mkdir(parents=True, exist_ok=True)\n\nif config_path.exists() and config_path.stat().st_size > 0:\n    try:\n        data = json.loads(config_path.read_text())\n    except json.JSONDecodeError as exc:\n        print(f"Existing OpenCode config is not valid JSON: {exc}", file=sys.stderr)\n        sys.exit(1)\nelse:\n    data = {}\n\nif not isinstance(data, dict):\n    print("OpenCode config root must be a JSON object", file=sys.stderr)\n    sys.exit(1)\n\nlegacy_mcp_servers = data.pop("mcpServers", None)\nif legacy_mcp_servers is not None and not isinstance(legacy_mcp_servers, dict):\n    print("OpenCode config field mcpServers must be a JSON object", file=sys.stderr)\n    sys.exit(1)\n\nmcp_field = "mcp"\nmcp_servers = data.get(mcp_field)\nif mcp_servers is None:\n    mcp_servers = {}\n    data[mcp_field] = mcp_servers\nelif not isinstance(mcp_servers, dict):\n    print("OpenCode config field mcp must be a JSON object", file=sys.stderr)\n    sys.exit(1)\n\nmcp_servers[server_name] = {\n    "type": "remote",\n    "url": mcp_url,\n    "enabled": True,\n    "headers": {\n        "Authorization": f"Bearer {token}",\n    },\n}\n\nconfig_path.write_text(json.dumps(data, indent=2) + "\\n")\nprint(f"Updated {config_path} with MCP server '{server_name}'")'

    echo "Saved configuration to ${config_path}"

client-mcp-pi NAME HOST="127.0.0.1":
    #!/usr/bin/env bash
    set -euo pipefail

    name="{{NAME}}"
    name="${name#NAME=}"
    name="${name#\"}"
    name="${name%\"}"
    [[ -n "${name}" ]] || { echo "NAME is required" >&2; exit 1; }
    if [[ ! "${name}" =~ ^[A-Za-z0-9._-]+$ ]]; then
      echo "NAME may contain only letters, digits, dot, underscore, and hyphen" >&2
      exit 1
    fi

    host="{{HOST}}"
    host="${host#HOST=}"
    host="${host#\"}"
    host="${host%\"}"
    [[ -n "${host}" ]] || { echo "HOST is required" >&2; exit 1; }

    server_name="${SERVER_NAME:-}"
    config_path_override="${CONFIG_PATH:-}"
    port="${PORT:-8080}"

    if [[ -z "${server_name}" ]]; then
      server_name="vault-${name}"
    fi
    if [[ ! "${server_name}" =~ ^[A-Za-z0-9._-]+$ ]]; then
      echo "SERVER_NAME may contain only letters, digits, dot, underscore, and hyphen" >&2
      exit 1
    fi

    if [[ -n "${config_path_override}" ]]; then
      config_path="${config_path_override}"
    else
      config_path="${HOME}/.pi/agent/mcp.json"
    fi

    read_mcp_token() {
      local token=""
      if [[ -n "${TOKEN_FILE:-}" ]]; then
        if [[ ! -f "${TOKEN_FILE}" ]]; then
          echo "TOKEN_FILE not found: ${TOKEN_FILE}" >&2
          exit 1
        fi
        token="$(tr -d '\r\n' < "${TOKEN_FILE}")"
      elif [[ -t 0 ]]; then
        printf 'Paste MCP bearer token for %s, then press Enter: ' "${name}" >&2
        IFS= read -r -s token || true
        printf '\n' >&2
      else
        IFS= read -r token || true
      fi
      token="${token//$'\r'/}"
      token="${token//$'\n'/}"
      if [[ -z "${token}" ]]; then
        echo "MCP bearer token is required on stdin or via TOKEN_FILE=/path/to/token" >&2
        exit 1
      fi
      printf '%s' "${token}"
    }
    token="$(read_mcp_token)"

    mcp_url="http://${host}:${port}/mcp"

    CONFIG_PATH="${config_path}" SERVER_NAME="${server_name}" MCP_URL="${mcp_url}" TOKEN="${token}" python3 -c 'import json, os, sys; from pathlib import Path; config_path = Path(os.environ["CONFIG_PATH"]).expanduser(); server_name = os.environ["SERVER_NAME"]; mcp_url = os.environ["MCP_URL"]; token = os.environ["TOKEN"]; config_path.parent.mkdir(parents=True, exist_ok=True); raw = config_path.read_text() if config_path.exists() and config_path.stat().st_size > 0 else ""; data = json.loads(raw) if raw.strip() else {}; assert isinstance(data, dict), "pi MCP config root must be a JSON object"; mcp_servers = data.get("mcpServers"); assert mcp_servers is None or isinstance(mcp_servers, dict), "pi MCP config field mcpServers must be a JSON object"; mcp_servers = {} if mcp_servers is None else mcp_servers; data["mcpServers"] = mcp_servers; mcp_servers[server_name] = {"url": mcp_url, "auth": "bearer", "bearerToken": token}; config_path.write_text(json.dumps(data, indent=2) + "\n"); print(f"Updated {config_path} with MCP server {server_name}")'

    echo "Saved configuration to ${config_path}"

_require-test-env:
    #!/usr/bin/env bash
    set -euo pipefail

    if [[ ! -f .env ]]; then
      echo "Missing .env. Run: cp .env.example .env" >&2
      exit 1
    fi

    if [[ ! -f config.yaml ]]; then
      echo "Missing config.yaml. Run: cp config.example.yaml config.yaml" >&2
      exit 1
    fi

    api_token_file="${API_TOKEN_FILE:-.secrets/api/monitoring.token}"
    if [[ ! -f "${api_token_file}" ]]; then
      echo "Missing ${api_token_file}. Add the matching api_tokens entry to config.yaml and run: just materialize-config-tokens" >&2
      exit 1
    fi

_wait-for-api:
    #!/usr/bin/env bash
    set -euo pipefail

    last_status="000"
    api_token_file="${API_TOKEN_FILE:-.secrets/api/monitoring.token}"
    api_token="$(tr -d '\r\n' < "${api_token_file}")"
    for _ in {1..30}; do
      status="$(curl -sS -o /dev/null -w "%{http_code}" -H "X-Api-Key: ${api_token}" http://127.0.0.1:8080/api/v1/status 2>/dev/null || true)"
      if [[ -z "${status}" ]]; then
        status="000"
      fi
      last_status="${status}"
      if [[ "${status}" == "200" ]]; then
        exit 0
      fi
      sleep 2
    done

    echo "vault-bridge API did not become ready within 60s (last /api/v1/status HTTP=${last_status})" >&2
    if [[ "${last_status}" == "401" ]]; then
      echo "Hint: ${api_token_file} may not match a configured api_tokens entry, or the latest config reload may have failed." >&2
    fi
    exit 1

use std::collections::HashSet;
use std::time::Duration;

use chrono::Utc;
use reqwest::StatusCode;
use serde_json::{Value, json};
use tokio::time::{Instant, sleep, timeout};

const LIVE_MCP_TESTS_FLAG: &str = "VAULT_BRIDGE_RUN_LIVE_MCP_TESTS";
const LIVE_MCP_EXTERNAL_URL: &str = "VAULT_BRIDGE_LIVE_MCP_EXTERNAL_URL";
const LIVE_MCP_LOCAL_URL: &str = "VAULT_BRIDGE_LIVE_MCP_LOCAL_URL";
const LIVE_MCP_EXTERNAL_BEARER_TOKEN: &str = "VAULT_BRIDGE_LIVE_MCP_EXTERNAL_BEARER_TOKEN";
const LIVE_MCP_LOCAL_BEARER_TOKEN: &str = "VAULT_BRIDGE_LIVE_MCP_LOCAL_BEARER_TOKEN";
const LIVE_MCP_PUBLIC_NOTE_ID: &str = "VAULT_BRIDGE_LIVE_MCP_PUBLIC_NOTE_ID";
const LIVE_MCP_PERSONAL_NOTE_ID: &str = "VAULT_BRIDGE_LIVE_MCP_PERSONAL_NOTE_ID";
const LIVE_MCP_QUERY: &str = "VAULT_BRIDGE_LIVE_MCP_QUERY";

#[derive(Debug, Clone)]
struct LiveMcpConfig {
    external_url: String,
    local_url: Option<String>,
    external_bearer_token: Option<String>,
    local_bearer_token: Option<String>,
    public_note_id: Option<String>,
    personal_note_id: Option<String>,
    query: String,
}

fn live_tests_enabled() -> bool {
    std::env::var(LIVE_MCP_TESTS_FLAG)
        .map(|value| matches!(value.as_str(), "1" | "true" | "TRUE"))
        .unwrap_or(false)
}

fn required_env(name: &str) -> Result<String, String> {
    std::env::var(name)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .ok_or_else(|| format!("missing {name}"))
}

fn optional_env(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn load_live_mcp_config() -> Result<LiveMcpConfig, String> {
    Ok(LiveMcpConfig {
        external_url: required_env(LIVE_MCP_EXTERNAL_URL)?,
        local_url: optional_env(LIVE_MCP_LOCAL_URL),
        external_bearer_token: optional_env(LIVE_MCP_EXTERNAL_BEARER_TOKEN),
        local_bearer_token: optional_env(LIVE_MCP_LOCAL_BEARER_TOKEN),
        public_note_id: optional_env(LIVE_MCP_PUBLIC_NOTE_ID),
        personal_note_id: optional_env(LIVE_MCP_PERSONAL_NOTE_ID),
        query: optional_env(LIVE_MCP_QUERY).unwrap_or_else(|| "vault bridge".to_string()),
    })
}

fn live_mcp_config_or_skip(test_name: &str) -> Option<LiveMcpConfig> {
    if !live_tests_enabled() {
        eprintln!(
            "skipping {test_name}: set {LIVE_MCP_TESTS_FLAG}=1 to run live MCP integration tests"
        );
        return None;
    }

    Some(load_live_mcp_config().unwrap_or_else(|reason| {
        panic!("{test_name} requires live MCP env when {LIVE_MCP_TESTS_FLAG}=1: {reason}")
    }))
}

fn endpoint_url(base_url: &str, path: &str) -> String {
    format!(
        "{}/{}",
        base_url.trim_end_matches('/'),
        path.trim_start_matches('/')
    )
}

fn rpc_error_message(error: &Value) -> String {
    let code = error
        .get("code")
        .and_then(Value::as_i64)
        .map(|value| value.to_string())
        .unwrap_or_else(|| "unknown".to_string());
    let message = error
        .get("message")
        .and_then(Value::as_str)
        .unwrap_or("unknown error");
    format!("code={code} message={message}")
}

async fn post_jsonrpc(
    client: &reqwest::Client,
    base_url: &str,
    payload: Value,
) -> Result<(StatusCode, Option<Value>), String> {
    let response = client
        .post(endpoint_url(base_url, "/mcp"))
        .json(&payload)
        .send()
        .await
        .map_err(|error| format!("mcp post failed: {error}"))?;

    let status = response.status();
    if status == StatusCode::NO_CONTENT {
        return Ok((status, None));
    }

    let body = response
        .json::<Value>()
        .await
        .map_err(|error| format!("decode mcp response failed: {error}"))?;
    Ok((status, Some(body)))
}

async fn call_method(
    client: &reqwest::Client,
    base_url: &str,
    method: &str,
    params: Value,
) -> Result<Value, String> {
    let payload = json!({
        "jsonrpc": "2.0",
        "id": format!("live-{method}"),
        "method": method,
        "params": params
    });
    let (status, body) = post_jsonrpc(client, base_url, payload).await?;
    if status != StatusCode::OK {
        return Err(format!("expected 200 response for {method}, got {status}"));
    }
    body.ok_or_else(|| format!("missing JSON body for {method}"))
}

async fn discovered_tool_names(
    client: &reqwest::Client,
    base_url: &str,
) -> Result<HashSet<String>, String> {
    let tools_list = call_method(client, base_url, "tools/list", json!({})).await?;
    Ok(tools_list["result"]["tools"]
        .as_array()
        .ok_or_else(|| "tools/list should return array".to_string())?
        .iter()
        .filter_map(|tool| tool.get("name").and_then(Value::as_str))
        .map(str::to_string)
        .collect())
}

async fn call_tool(
    client: &reqwest::Client,
    base_url: &str,
    tool_name: &str,
    arguments: Value,
) -> Result<Value, String> {
    let response = call_method(
        client,
        base_url,
        "tools/call",
        json!({
            "name": tool_name,
            "arguments": arguments
        }),
    )
    .await?;

    if let Some(error) = response.get("error") {
        return Err(rpc_error_message(error));
    }

    let result = response
        .get("result")
        .ok_or_else(|| format!("missing result payload for tool {tool_name}"))?;
    if result
        .get("isError")
        .and_then(Value::as_bool)
        .unwrap_or(false)
    {
        return Err(format!("tool {tool_name} reported error payload"));
    }

    result
        .get("structuredContent")
        .cloned()
        .ok_or_else(|| format!("missing structuredContent for tool {tool_name}"))
}

async fn call_tool_expect_error(
    client: &reqwest::Client,
    base_url: &str,
    tool_name: &str,
    arguments: Value,
) -> Result<String, String> {
    let response = call_method(
        client,
        base_url,
        "tools/call",
        json!({
            "name": tool_name,
            "arguments": arguments
        }),
    )
    .await?;

    if let Some(error) = response.get("error") {
        return Ok(rpc_error_message(error));
    }

    let result = response
        .get("result")
        .ok_or_else(|| format!("expected error response for tool {tool_name}"))?;
    if !result
        .get("isError")
        .and_then(Value::as_bool)
        .unwrap_or(false)
    {
        return Err(format!("expected error response for tool {tool_name}"));
    }
    let structured = result
        .get("structuredContent")
        .ok_or_else(|| format!("missing structuredContent for errored tool {tool_name}"))?;
    Ok(structured
        .get("description")
        .and_then(Value::as_str)
        .or_else(|| structured.get("message").and_then(Value::as_str))
        .unwrap_or("unknown tool error")
        .to_string())
}

async fn wait_for_note_read(
    client: &reqwest::Client,
    base_url: &str,
    note_id: &str,
    timeout_after: Duration,
) -> Result<Value, String> {
    let deadline = Instant::now() + timeout_after;
    let mut last_error = String::new();

    while Instant::now() < deadline {
        match call_tool(client, base_url, "get_note", json!({ "id": note_id })).await {
            Ok(note) => return Ok(note),
            Err(error) => {
                last_error = error;
                sleep(Duration::from_secs(1)).await;
            }
        }
    }

    Err(format!(
        "timed out waiting for get_note success: {last_error}"
    ))
}

fn live_test_client(bearer_token: Option<&str>) -> reqwest::Client {
    let mut builder = reqwest::Client::builder().timeout(Duration::from_secs(20));
    if let Some(token) = bearer_token {
        let mut headers = reqwest::header::HeaderMap::new();
        let value = format!("Bearer {token}")
            .parse()
            .expect("build authorization header");
        headers.insert(reqwest::header::AUTHORIZATION, value);
        builder = builder.default_headers(headers);
    }
    builder.build().expect("build live mcp reqwest client")
}

#[tokio::test]
async fn live_mcp_initialize_and_sse_endpoint_are_available() {
    let Some(config) =
        live_mcp_config_or_skip("live_mcp_initialize_and_sse_endpoint_are_available")
    else {
        return;
    };

    let client = live_test_client(config.external_bearer_token.as_deref());

    let initialize = call_method(
        &client,
        &config.external_url,
        "initialize",
        json!({
            "protocolVersion": "2025-06-18",
            "capabilities": {},
            "clientInfo": {
                "name": "vault-bridge-live-mcp-tests",
                "version": "1.0"
            }
        }),
    )
    .await
    .expect("initialize should succeed");

    assert_eq!(initialize["jsonrpc"], "2.0");
    assert_eq!(
        initialize["result"]["protocolVersion"].as_str(),
        Some("2025-06-18")
    );
    assert_eq!(
        initialize["result"]["serverInfo"]["name"].as_str(),
        Some("vault-bridge-mcp")
    );

    let initialized_payload = json!({
        "jsonrpc": "2.0",
        "method": "notifications/initialized",
        "params": {}
    });
    let (status, body) = post_jsonrpc(&client, &config.external_url, initialized_payload)
        .await
        .expect("notifications/initialized request should succeed");
    assert_eq!(status, StatusCode::NO_CONTENT);
    assert!(body.is_none());

    let mut sse_response = client
        .get(endpoint_url(&config.external_url, "/sse"))
        .send()
        .await
        .expect("sse request should succeed");
    assert_eq!(sse_response.status(), StatusCode::OK);

    let content_type = sse_response
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default();
    assert!(
        content_type.contains("text/event-stream"),
        "unexpected content-type: {content_type}"
    );

    let first_chunk = timeout(Duration::from_secs(10), sse_response.chunk())
        .await
        .expect("sse first chunk timed out")
        .expect("sse stream closed before first chunk")
        .expect("failed reading sse first chunk");
    let first_chunk_text = String::from_utf8_lossy(&first_chunk);
    assert!(
        first_chunk_text.contains("event: endpoint"),
        "missing endpoint event in sse chunk: {first_chunk_text}"
    );
    assert!(
        first_chunk_text.contains("data: /mcp"),
        "missing /mcp endpoint payload in sse chunk: {first_chunk_text}"
    );
}

#[tokio::test]
async fn live_mcp_external_context_tool_flow_matches_prd_surface() {
    let Some(config) =
        live_mcp_config_or_skip("live_mcp_external_context_tool_flow_matches_prd_surface")
    else {
        return;
    };

    let client = live_test_client(config.external_bearer_token.as_deref());
    let tools = discovered_tool_names(&client, &config.external_url)
        .await
        .expect("tools/list should succeed");
    assert!(
        !tools.is_empty(),
        "external context should discover at least one MCP tool"
    );

    if tools.contains("search_notes") {
        let search = call_tool(
            &client,
            &config.external_url,
            "search_notes",
            json!({
                "query": config.query,
                "mode": "hybrid",
                "limit": 5
            }),
        )
        .await
        .expect("search_notes should succeed");
        assert!(
            search.get("results").is_some(),
            "search response should contain results"
        );
    }

    if tools.contains("recent_notes") {
        let recent = call_tool(
            &client,
            &config.external_url,
            "recent_notes",
            json!({
                "last_n_days": 14,
                "limit": 5
            }),
        )
        .await
        .expect("recent_notes should succeed");
        assert!(
            recent.get("notes").is_some(),
            "recent response should contain notes"
        );
    }

    if tools.contains("query_notes") {
        let queried = call_tool(
            &client,
            &config.external_url,
            "query_notes",
            json!({
                "text_query": config.query,
                "search_mode": "hybrid",
                "limit": 5
            }),
        )
        .await
        .expect("query_notes should succeed");
        assert!(
            queried.get("notes").is_some(),
            "query response should contain notes"
        );
    }

    if tools.contains("list_tags") {
        let tags = call_tool(&client, &config.external_url, "list_tags", json!({}))
            .await
            .expect("list_tags should succeed");
        assert!(tags.get("tags").is_some(), "list_tags should return tags");
    }

    if tools.contains("vault_status") {
        let status = call_tool(&client, &config.external_url, "vault_status", json!({}))
            .await
            .expect("vault_status should succeed");
        assert_eq!(status.get("status").and_then(Value::as_str), Some("ok"));
    }

    if tools.contains("get_note")
        && let Some(public_note_id) = config.public_note_id.as_deref()
    {
        let public_note = call_tool(
            &client,
            &config.external_url,
            "get_note",
            json!({ "id": public_note_id }),
        )
        .await
        .expect("configured public note id should be readable from external context");
        assert_eq!(public_note["id"].as_str(), Some(public_note_id));
    }

    if tools.contains("new_note") && tools.contains("get_note") {
        let suffix = format!(
            "{}-{}",
            Utc::now().timestamp(),
            Utc::now().timestamp_subsec_nanos()
        );
        let title = format!("Live MCP Validation {suffix}");
        let new_note = call_tool(
            &client,
            &config.external_url,
            "new_note",
            json!({
                "title": title,
                "content": format!("---\ntags: [mcp-validation, integration]\nsource: live-mcp-validation-tests\nconfidence: 1.0\n---\n\n# {title}\n\nCreated from live MCP validation test.")
            }),
        )
        .await
        .expect("new_note should succeed");
        let created_id = new_note["id"]
            .as_str()
            .expect("new_note should return id")
            .to_string();

        let indexed_note = wait_for_note_read(
            &client,
            &config.external_url,
            &created_id,
            Duration::from_secs(60),
        )
        .await
        .expect("created note should become readable through MCP");
        assert_eq!(indexed_note["id"].as_str(), Some(created_id.as_str()));
        assert!(
            indexed_note["content"]
                .as_str()
                .unwrap_or_default()
                .contains("Created from live MCP validation test."),
            "indexed note content mismatch"
        );

        if tools.contains("get_neighbors") {
            let neighbors = call_tool(
                &client,
                &config.external_url,
                "get_neighbors",
                json!({
                    "id": created_id.as_str(),
                    "depth": 1
                }),
            )
            .await
            .expect("get_neighbors should succeed");
            assert_eq!(neighbors["center"].as_str(), Some(created_id.as_str()));
        }

        if tools.contains("get_backlinks") {
            let backlinks = call_tool(
                &client,
                &config.external_url,
                "get_backlinks",
                json!({
                    "id": created_id.as_str()
                }),
            )
            .await
            .expect("get_backlinks should succeed");
            assert_eq!(backlinks["target"].as_str(), Some(created_id.as_str()));
        }

        if tools.contains("query_notes") {
            let tagged_notes = call_tool(
                &client,
                &config.external_url,
                "query_notes",
                json!({
                    "tags_all": ["mcp-validation"],
                    "sort_by": "updated_at",
                    "sort_order": "desc",
                    "limit": 10
                }),
            )
            .await
            .expect("query_notes tag filter should succeed");
            assert!(
                tagged_notes["notes"]
                    .as_array()
                    .expect("query_notes should return notes")
                    .iter()
                    .any(|note| note["id"].as_str() == Some(created_id.as_str())),
                "query_notes tag filter should return the created validation note"
            );
        }
    }
}

#[tokio::test]
async fn live_mcp_context_opacity_blocks_personal_notes_for_external_context() {
    let Some(config) = live_mcp_config_or_skip(
        "live_mcp_context_opacity_blocks_personal_notes_for_external_context",
    ) else {
        return;
    };

    let local_url = config.local_url.as_deref().unwrap_or_else(|| {
        panic!(
            "live_mcp_context_opacity_blocks_personal_notes_for_external_context requires {LIVE_MCP_LOCAL_URL} when {LIVE_MCP_TESTS_FLAG}=1"
        )
    });

    let personal_note_id = config.personal_note_id.as_deref().unwrap_or_else(|| {
        panic!(
            "live_mcp_context_opacity_blocks_personal_notes_for_external_context requires {LIVE_MCP_PERSONAL_NOTE_ID} when {LIVE_MCP_TESTS_FLAG}=1"
        )
    });

    let external_client = live_test_client(config.external_bearer_token.as_deref());
    let local_client = live_test_client(config.local_bearer_token.as_deref());
    let external_tools = discovered_tool_names(&external_client, &config.external_url)
        .await
        .expect("external tools/list should succeed");
    let local_tools = discovered_tool_names(&local_client, local_url)
        .await
        .expect("local tools/list should succeed");
    if !external_tools.contains("get_note") || !local_tools.contains("get_note") {
        return;
    }

    let external_error = call_tool_expect_error(
        &external_client,
        &config.external_url,
        "get_note",
        json!({ "id": personal_note_id }),
    )
    .await
    .expect("external context should fail for personal note");
    let external_error_lower = external_error.to_lowercase();
    assert!(
        external_error_lower.contains("404")
            || external_error_lower.contains("not found")
            || external_error_lower.contains("client error"),
        "unexpected external read error: {external_error}"
    );

    let local_note = call_tool(
        &local_client,
        local_url,
        "get_note",
        json!({ "id": personal_note_id }),
    )
    .await
    .expect("local context should read personal note");
    assert_eq!(local_note["id"].as_str(), Some(personal_note_id));
}

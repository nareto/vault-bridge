use std::collections::BTreeMap;

use axum::body::Body;
use axum::http::header::AUTHORIZATION;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use serde_json::{Value, json};
use tempfile::TempDir;
use tower::ServiceExt;

use vault_bridge::authorization::{AccessMatcher, AccessPolicy, AccessRule};
use vault_bridge::config::{AppConfig, McpTokenConfig};
use vault_bridge::mcp::{McpState, app_router, tool_definitions};
use vault_bridge::service::VaultBridgeService;
use vault_bridge::store::{MAX_GRAPH_TRAVERSAL_DEPTH, MAX_NOTE_LIST_LIMIT, VaultStore};

fn mcp_call(id: Value, method: &str, params: Value) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": id,
        "method": method,
        "params": params
    })
}

fn authorized_request(body: Value) -> Request<Body> {
    Request::builder()
        .method("POST")
        .uri("/mcp")
        .header("content-type", "application/json")
        .header(AUTHORIZATION, "Bearer mcp-test-token")
        .body(Body::from(body.to_string()))
        .expect("request")
}

fn unauthorized_request(body: Value) -> Request<Body> {
    Request::builder()
        .method("POST")
        .uri("/mcp")
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
        .expect("request")
}

async fn test_state(context: &str) -> (McpState, TempDir) {
    let mut config = AppConfig::default();
    let mut external = AccessPolicy::default_agent();
    external.read = vec![
        AccessRule::deny(AccessMatcher {
            path_prefix: Some("00Journal/".to_string()),
            ..Default::default()
        }),
        AccessRule::allow(AccessMatcher::allow_all()),
    ];
    let read_only = AccessPolicy {
        read: vec![AccessRule::allow(AccessMatcher::allow_all())],
        create: Vec::new(),
        edit: Vec::new(),
    };
    config.contexts.insert("external".to_string(), external);
    config.contexts.insert("read-only".to_string(), read_only);
    let store = VaultStore::new(20);
    store.seed_example_data().await;
    store.set_authorization_config(config.contexts).await;
    let service = VaultBridgeService::new(store, None);

    let dir = TempDir::new().expect("token dir");
    std::fs::write(dir.path().join("test-client.token"), "mcp-test-token").expect("write token");
    let mut tokens = BTreeMap::new();
    tokens.insert(
        "test-client".to_string(),
        McpTokenConfig {
            context: context.to_string(),
        },
    );

    (
        McpState::new(service, None, None, Some(dir.path().to_path_buf()), tokens)
            .expect("mcp state"),
        dir,
    )
}

fn tool_names(payload: &Value) -> Vec<String> {
    payload["result"]["tools"]
        .as_array()
        .expect("tools array")
        .iter()
        .filter_map(|entry| entry["name"].as_str().map(str::to_string))
        .collect()
}

async fn response_json(response: axum::response::Response) -> Value {
    let body = response
        .into_body()
        .collect()
        .await
        .expect("collect body")
        .to_bytes();
    serde_json::from_slice(&body).expect("json body")
}

#[tokio::test]
async fn tools_list_requires_configured_bearer_token() {
    let (state, _dir) = test_state("external").await;
    let app = app_router(state);

    let response = app
        .oneshot(unauthorized_request(mcp_call(
            Value::from(1),
            "tools/list",
            json!({}),
        )))
        .await
        .expect("response");

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn tools_list_returns_all_yaml_tool_definitions_for_full_capability_context() {
    let (state, _dir) = test_state("external").await;
    let app = app_router(state);

    let response = app
        .oneshot(authorized_request(mcp_call(
            Value::from(1),
            "tools/list",
            json!({}),
        )))
        .await
        .expect("response");

    assert_eq!(response.status(), StatusCode::OK);
    let payload = response_json(response).await;
    let tools = tool_names(&payload);
    let expected = tool_definitions()
        .into_iter()
        .map(|tool| tool.name)
        .collect::<Vec<_>>();
    assert_eq!(tools, expected);
    assert!(tools.iter().all(|tool| tool != "assemble_context"));
    assert!(tools.iter().all(|tool| tool != "notes_by_tag"));
}

#[tokio::test]
async fn assemble_context_is_not_exposed_as_mcp_tool() {
    let (state, _dir) = test_state("external").await;
    let app = app_router(state);

    let response = app
        .oneshot(authorized_request(mcp_call(
            Value::from(1),
            "tools/call",
            json!({
                "name": "assemble_context",
                "arguments": {
                    "seed_query": "vault bridge architecture",
                    "max_depth": 1
                }
            }),
        )))
        .await
        .expect("response");

    assert_eq!(response.status(), StatusCode::OK);
    let payload = response_json(response).await;
    assert_eq!(payload["error"]["message"], json!("unknown tool"));
    assert!(
        payload["error"]["data"]["description"]
            .as_str()
            .expect("error description")
            .contains("not exposed")
    );
    let available_tools = payload["error"]["data"]["details"]["availableTools"]
        .as_array()
        .expect("available tools");
    assert!(
        available_tools
            .iter()
            .all(|tool| tool.as_str() != Some("assemble_context"))
    );
}

#[tokio::test]
async fn tools_list_hides_mutation_tools_for_read_only_context() {
    let (state, _dir) = test_state("read-only").await;
    let app = app_router(state);

    let response = app
        .oneshot(authorized_request(mcp_call(
            Value::from(1),
            "tools/list",
            json!({}),
        )))
        .await
        .expect("response");

    assert_eq!(response.status(), StatusCode::OK);
    let payload = response_json(response).await;
    let tools = tool_names(&payload);
    assert!(tools.contains(&"get_note".to_string()));
    assert!(tools.contains(&"recent_notes".to_string()));
    assert!(!tools.contains(&"new_note".to_string()));
    assert!(!tools.contains(&"edit_note".to_string()));
}

#[tokio::test]
async fn tooling_catalog_reflects_read_only_context_capabilities() {
    let (state, _dir) = test_state("read-only").await;
    let app = app_router(state);

    let response = app
        .oneshot(authorized_request(mcp_call(
            Value::from(1),
            "resources/read",
            json!({ "uri": "vault-bridge://catalog/tooling.json" }),
        )))
        .await
        .expect("response");

    assert_eq!(response.status(), StatusCode::OK);
    let payload = response_json(response).await;
    let text = payload["result"]["contents"][0]["text"]
        .as_str()
        .expect("catalog text");
    let catalog: Value = serde_json::from_str(text).expect("catalog json");
    let tools = catalog["tools"]
        .as_array()
        .expect("catalog tools")
        .iter()
        .filter_map(|entry| entry["name"].as_str().map(str::to_string))
        .collect::<Vec<_>>();

    assert_eq!(catalog["boundaries"]["context"], json!("read-only"));
    assert_eq!(catalog["boundaries"]["capabilities"]["read"], json!(true));
    assert_eq!(
        catalog["boundaries"]["capabilities"]["create"],
        json!(false)
    );
    assert_eq!(catalog["boundaries"]["capabilities"]["edit"], json!(false));
    assert!(tools.contains(&"get_note".to_string()));
    assert!(!tools.contains(&"new_note".to_string()));
    assert!(
        catalog["boundaries"]["unavailableTools"]
            .as_array()
            .expect("unavailable tools")
            .iter()
            .any(|entry| entry["name"] == "new_note" && entry["requiredCapability"] == "create")
    );
}

#[tokio::test]
async fn tool_call_rejects_unavailable_tool_for_context_before_service_policy() {
    let (state, _dir) = test_state("read-only").await;
    let app = app_router(state);

    let response = app
        .oneshot(authorized_request(mcp_call(
            Value::from(1),
            "tools/call",
            json!({
                "name": "new_note",
                "arguments": {"title": "Should not be created"}
            }),
        )))
        .await
        .expect("response");

    assert_eq!(response.status(), StatusCode::OK);
    let payload = response_json(response).await;
    assert_eq!(payload["result"]["isError"], json!(true));
    assert_eq!(
        payload["result"]["structuredContent"]["errorCategory"],
        json!("permission")
    );
    assert_eq!(
        payload["result"]["structuredContent"]["message"],
        json!("tool unavailable for this token context")
    );
    assert_eq!(
        payload["result"]["structuredContent"]["details"]["requiredCapability"],
        json!("create")
    );
    let available = payload["result"]["structuredContent"]["details"]["availableTools"]
        .as_array()
        .expect("available tools");
    assert!(available.contains(&json!("get_note")));
    assert!(!available.contains(&json!("new_note")));
}

#[tokio::test]
async fn tool_call_uses_token_context_permissions() {
    let (state, _dir) = test_state("external").await;
    let app = app_router(state);

    let public_response = app
        .clone()
        .oneshot(authorized_request(mcp_call(
            Value::from(1),
            "tools/call",
            json!({
                "name": "get_note",
                "arguments": {"id": "03Concepts/rust-phantom-types.md"}
            }),
        )))
        .await
        .expect("public response");
    let public_payload = response_json(public_response).await;
    assert_eq!(public_payload["result"]["isError"], json!(false));
    assert_eq!(
        public_payload["result"]["structuredContent"]["id"],
        "03Concepts/rust-phantom-types.md"
    );

    let private_response = app
        .oneshot(authorized_request(mcp_call(
            Value::from(2),
            "tools/call",
            json!({
                "name": "get_note",
                "arguments": {"id": "00Journal/private.md"}
            }),
        )))
        .await
        .expect("private response");
    let private_payload = response_json(private_response).await;
    assert_eq!(private_payload["result"]["isError"], json!(true));
    assert_eq!(
        private_payload["result"]["structuredContent"]["httpStatus"],
        404
    );
}

#[tokio::test]
async fn tool_call_rejects_arguments_outside_advertised_caps() {
    let (state, _dir) = test_state("external").await;
    let app = app_router(state);
    let cases = [
        (
            "recent_notes",
            json!({"limit": MAX_NOTE_LIST_LIMIT + 1}),
            format!("limit must be between 0 and {MAX_NOTE_LIST_LIMIT}"),
        ),
        (
            "query_notes",
            json!({"limit": MAX_NOTE_LIST_LIMIT + 1}),
            format!("limit must be between 0 and {MAX_NOTE_LIST_LIMIT}"),
        ),
        (
            "query_base",
            json!({"base_query": "views:\n  - type: table\n", "limit": MAX_NOTE_LIST_LIMIT + 1}),
            format!("limit must be between 0 and {MAX_NOTE_LIST_LIMIT}"),
        ),
        (
            "get_neighbors",
            json!({
                "id": "03Concepts/rust-phantom-types.md",
                "depth": MAX_GRAPH_TRAVERSAL_DEPTH + 1
            }),
            format!("depth must be between 1 and {MAX_GRAPH_TRAVERSAL_DEPTH}"),
        ),
        (
            "get_neighbors",
            json!({
                "id": "03Concepts/rust-phantom-types.md",
                "depth": 0
            }),
            format!("depth must be between 1 and {MAX_GRAPH_TRAVERSAL_DEPTH}"),
        ),
    ];

    for (index, (tool_name, arguments, expected_message)) in cases.into_iter().enumerate() {
        let response = app
            .clone()
            .oneshot(authorized_request(mcp_call(
                json!(index + 1),
                "tools/call",
                json!({
                    "name": tool_name,
                    "arguments": arguments
                }),
            )))
            .await
            .expect("response");

        assert_eq!(response.status(), StatusCode::OK);
        let payload = response_json(response).await;
        assert_eq!(payload["result"]["isError"], json!(true));
        assert_eq!(
            payload["result"]["structuredContent"]["errorCategory"],
            json!("validation")
        );
        assert_eq!(
            payload["result"]["structuredContent"]["tool"],
            json!(tool_name)
        );
        assert!(
            payload["result"]["structuredContent"]["description"]
                .as_str()
                .expect("error description")
                .contains(&expected_message),
            "expected {tool_name} error to contain {expected_message}; payload: {payload}"
        );
    }
}

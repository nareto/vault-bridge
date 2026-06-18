use std::fs;
use std::path::Path;

use axum::body::Body;
use axum::http::header::AUTHORIZATION;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use serde_json::{Value, json};
use tempfile::TempDir;
use tower::ServiceExt;

use vault_bridge::api::{ApiTokenState, AppState, app_router};
use vault_bridge::config::AppConfig;
use vault_bridge::mcp::McpState;
use vault_bridge::runtime_config::{ConfigReloadTrigger, RuntimeConfigState};
use vault_bridge::service::VaultBridgeService;
use vault_bridge::store::VaultStore;

fn base_contexts() -> &'static str {
    r#"
contexts:
  external:
    read:
      - allow:
          default: true
    create: []
    edit: []
"#
}

fn api_token_config(name: &str, context: &str) -> String {
    format!(
        r#"
api_tokens:
  {name}:
    context: {context}
{}
"#,
        base_contexts()
    )
}

fn mcp_token_config(name: &str, context: &str) -> String {
    format!(
        r#"
mcp_tokens:
  {name}:
    context: {context}
{}
"#,
        base_contexts()
    )
}

fn mcp_token_config_with_write_access(name: &str, context: &str) -> String {
    format!(
        r#"
mcp_tokens:
  {name}:
    context: {context}
contexts:
  external:
    read:
      - allow:
          default: true
    create:
      - allow:
          default: true
        add_tags: ["ai-created"]
        set_owner: true
    edit:
      - allow:
          default: true
"#
    )
}

fn deny_all_context_config() -> String {
    r#"
api_tokens:
  client:
    context: external
  deny-client:
    context: deny_all
contexts:
  external:
    read:
      - allow:
          default: true
    create: []
    edit: []
  deny_all:
    read: []
    create: []
    edit: []
"#
    .to_string()
}

fn write_config(path: &Path, contents: &str) {
    fs::write(path, contents).expect("write config");
}

fn get(uri: &str, key: &str) -> Request<Body> {
    Request::builder()
        .method("GET")
        .uri(uri)
        .header("x-api-key", key)
        .body(Body::empty())
        .expect("request")
}

fn mcp_request(token: &str, body: Value) -> Request<Body> {
    Request::builder()
        .method("POST")
        .uri("/mcp")
        .header("content-type", "application/json")
        .header(AUTHORIZATION, format!("Bearer {token}"))
        .body(Body::from(body.to_string()))
        .expect("request")
}

fn mcp_tools_list() -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "tools/list",
        "params": {}
    })
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

fn mcp_tool_names(payload: &Value) -> Vec<String> {
    payload["result"]["tools"]
        .as_array()
        .expect("tools array")
        .iter()
        .filter_map(|entry| entry["name"].as_str().map(str::to_string))
        .collect()
}

async fn rest_app(config_path: &Path, api_token_dir: &Path) -> (axum::Router, RuntimeConfigState) {
    let config = AppConfig::load_from_path(config_path).expect("load config");
    let runtime_config = RuntimeConfigState::new(&config, Some(config_path.to_path_buf()));
    let store = VaultStore::new_with_auth_config(20, runtime_config.auth_config());
    store.seed_example_data().await;
    let service = VaultBridgeService::new(store, None);
    let app = app_router(AppState {
        service,
        api_tokens: ApiTokenState::new_with_auth_config(
            None,
            Some(api_token_dir.to_path_buf()),
            runtime_config.auth_config(),
        ),
        mcp: None,
        runtime_config: runtime_config.clone(),
    });
    (app, runtime_config)
}

async fn mcp_app(config_path: &Path, mcp_token_dir: &Path) -> (axum::Router, RuntimeConfigState) {
    let config = AppConfig::load_from_path(config_path).expect("load config");
    let runtime_config = RuntimeConfigState::new(&config, Some(config_path.to_path_buf()));
    let store = VaultStore::new_with_auth_config(20, runtime_config.auth_config());
    store.seed_example_data().await;
    let service = VaultBridgeService::new(store, None);
    let mcp = McpState::new_with_auth_config(
        service.clone(),
        None,
        None,
        Some(mcp_token_dir.to_path_buf()),
        runtime_config.auth_config(),
    )
    .expect("mcp state");
    let app = app_router(AppState {
        service,
        api_tokens: ApiTokenState::default(),
        mcp: Some(mcp),
        runtime_config: runtime_config.clone(),
    });
    (app, runtime_config)
}

#[tokio::test]
async fn rest_api_token_mapping_reloads_from_config() {
    let temp = TempDir::new().expect("temp dir");
    let config_path = temp.path().join("config.yaml");
    let api_dir = temp.path().join("api");
    fs::create_dir(&api_dir).expect("api token dir");
    fs::write(api_dir.join("new-client.token"), "new-token").expect("write token");
    write_config(&config_path, base_contexts());

    let (app, runtime_config) = rest_app(&config_path, &api_dir).await;

    let before = app
        .clone()
        .oneshot(get("/api/v1/status", "new-token"))
        .await
        .expect("before response");
    assert_eq!(before.status(), StatusCode::UNAUTHORIZED);

    write_config(&config_path, &api_token_config("new-client", "external"));
    runtime_config
        .reload_from_path(&config_path, ConfigReloadTrigger::Poll)
        .await
        .expect("reload");

    let after = app
        .oneshot(get("/api/v1/status", "new-token"))
        .await
        .expect("after response");
    assert_eq!(after.status(), StatusCode::OK);
}

#[tokio::test]
async fn mcp_token_mapping_reloads_from_config() {
    let temp = TempDir::new().expect("temp dir");
    let config_path = temp.path().join("config.yaml");
    let mcp_dir = temp.path().join("mcp");
    fs::create_dir(&mcp_dir).expect("mcp token dir");
    fs::write(mcp_dir.join("new-mcp.token"), "mcp-token").expect("write token");
    write_config(&config_path, base_contexts());

    let (app, runtime_config) = mcp_app(&config_path, &mcp_dir).await;

    let before = app
        .clone()
        .oneshot(mcp_request("mcp-token", mcp_tools_list()))
        .await
        .expect("before response");
    assert_eq!(before.status(), StatusCode::UNAUTHORIZED);

    write_config(&config_path, &mcp_token_config("new-mcp", "external"));
    runtime_config
        .reload_from_path(&config_path, ConfigReloadTrigger::Poll)
        .await
        .expect("reload");

    let after = app
        .oneshot(mcp_request("mcp-token", mcp_tools_list()))
        .await
        .expect("after response");
    assert_eq!(after.status(), StatusCode::OK);
}

#[tokio::test]
async fn mcp_tool_discovery_reloads_context_capabilities() {
    let temp = TempDir::new().expect("temp dir");
    let config_path = temp.path().join("config.yaml");
    let mcp_dir = temp.path().join("mcp");
    fs::create_dir(&mcp_dir).expect("mcp token dir");
    fs::write(mcp_dir.join("client.token"), "mcp-token").expect("write token");
    write_config(&config_path, &mcp_token_config("client", "external"));

    let (app, runtime_config) = mcp_app(&config_path, &mcp_dir).await;

    let before = app
        .clone()
        .oneshot(mcp_request("mcp-token", mcp_tools_list()))
        .await
        .expect("before response");
    assert_eq!(before.status(), StatusCode::OK);
    let before_payload = response_json(before).await;
    let before_tools = mcp_tool_names(&before_payload);
    assert!(before_tools.contains(&"get_note".to_string()));
    assert!(!before_tools.contains(&"new_note".to_string()));
    assert!(!before_tools.contains(&"edit_note".to_string()));

    write_config(
        &config_path,
        &mcp_token_config_with_write_access("client", "external"),
    );
    runtime_config
        .reload_from_path(&config_path, ConfigReloadTrigger::Poll)
        .await
        .expect("reload");

    let after = app
        .oneshot(mcp_request("mcp-token", mcp_tools_list()))
        .await
        .expect("after response");
    assert_eq!(after.status(), StatusCode::OK);
    let after_payload = response_json(after).await;
    let after_tools = mcp_tool_names(&after_payload);
    assert!(after_tools.contains(&"get_note".to_string()));
    assert!(after_tools.contains(&"new_note".to_string()));
    assert!(after_tools.contains(&"edit_note".to_string()));
}

#[tokio::test]
async fn invalid_reload_keeps_previous_good_config() {
    let temp = TempDir::new().expect("temp dir");
    let config_path = temp.path().join("config.yaml");
    let api_dir = temp.path().join("api");
    fs::create_dir(&api_dir).expect("api token dir");
    fs::write(api_dir.join("client.token"), "client-token").expect("write client token");
    fs::write(api_dir.join("leaky.token"), "leaky-token").expect("write leaky token");
    write_config(&config_path, &api_token_config("client", "external"));

    let (app, runtime_config) = rest_app(&config_path, &api_dir).await;
    let before = app
        .clone()
        .oneshot(get("/api/v1/status", "client-token"))
        .await
        .expect("before response");
    assert_eq!(before.status(), StatusCode::OK);

    write_config(
        &config_path,
        r#"
api_tokens:
  client:
    context: external
  leaky:
    context: missing
contexts:
  external:
    read:
      - allow:
          default: true
    create: []
    edit: []
"#,
    );
    runtime_config
        .reload_from_path(&config_path, ConfigReloadTrigger::Poll)
        .await
        .expect_err("invalid reload should fail");

    let still_authorized = app
        .clone()
        .oneshot(get("/api/v1/status", "client-token"))
        .await
        .expect("old token response");
    assert_eq!(still_authorized.status(), StatusCode::OK);

    let no_partial_leak = app
        .oneshot(get("/api/v1/status", "leaky-token"))
        .await
        .expect("new token response");
    assert_eq!(no_partial_leak.status(), StatusCode::UNAUTHORIZED);

    let reload_status = runtime_config.reload_status().await;
    assert_eq!(reload_status.failure_count, 1);
    assert!(reload_status.last_error.is_some());
}

#[tokio::test]
async fn new_deny_all_context_reloads_safely() {
    let temp = TempDir::new().expect("temp dir");
    let config_path = temp.path().join("config.yaml");
    let api_dir = temp.path().join("api");
    fs::create_dir(&api_dir).expect("api token dir");
    fs::write(api_dir.join("client.token"), "client-token").expect("write client token");
    fs::write(api_dir.join("deny-client.token"), "deny-token").expect("write deny token");
    write_config(&config_path, &api_token_config("client", "external"));

    let (app, runtime_config) = rest_app(&config_path, &api_dir).await;
    let before = app
        .clone()
        .oneshot(get("/api/v1/status", "deny-token"))
        .await
        .expect("before response");
    assert_eq!(before.status(), StatusCode::UNAUTHORIZED);

    write_config(&config_path, &deny_all_context_config());
    runtime_config
        .reload_from_path(&config_path, ConfigReloadTrigger::Poll)
        .await
        .expect("reload");

    let after = app
        .oneshot(get(
            "/api/v1/notes/03Concepts/rust-phantom-types.md",
            "deny-token",
        ))
        .await
        .expect("after response");
    assert_eq!(after.status(), StatusCode::NOT_FOUND);
}

use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use serde_json::{Value, json};
use tower::ServiceExt;

use vault_bridge::api::{ApiTokenState, AppState, app_router};
use vault_bridge::authorization::{AccessMatcher, AccessPolicy, AccessRule};
use vault_bridge::config::{ApiTokenConfig, AppConfig};
use vault_bridge::runtime_config::RuntimeConfigState;
use vault_bridge::service::VaultBridgeService;
use vault_bridge::store::VaultStore;

fn test_config() -> AppConfig {
    let mut config = AppConfig::default();
    let mut external = AccessPolicy::default_agent();
    external.read = vec![
        AccessRule::deny(AccessMatcher {
            path_prefix: Some("00Journal/".to_string()),
            ..Default::default()
        }),
        AccessRule::allow(AccessMatcher::allow_all()),
    ];
    config.contexts.insert("external".to_string(), external);
    config
        .contexts
        .insert("local".to_string(), AccessPolicy::default_agent());
    config
        .contexts
        .insert("admin".to_string(), AccessPolicy::admin());
    config
}

async fn test_app(config: AppConfig) -> axum::Router {
    let runtime_config = RuntimeConfigState::for_tests(&config);
    let store = VaultStore::new(20);
    store.seed_example_data().await;
    store.set_authorization_config(config.contexts).await;
    app_router(AppState {
        service: VaultBridgeService::new(store, None),
        api_tokens: ApiTokenState::for_tests([
            ("external", "external-dev-token", "external"),
            ("local", "local-dev-token", "local"),
            ("admin", "admin-dev-token", "admin"),
            ("research", "research-token", "research"),
        ]),
        mcp: None,
        runtime_config,
    })
}

async fn response_json(response: axum::response::Response) -> Value {
    let body = response
        .into_body()
        .collect()
        .await
        .expect("collect body")
        .to_bytes();
    serde_json::from_slice(&body).expect("json")
}

fn assert_structured_error(
    body: &Value,
    category: &str,
    is_retryable: bool,
    message: &str,
    http_status: u16,
) {
    assert!(
        body["error"].as_str().is_some(),
        "legacy error string should be present: {body}"
    );
    assert_eq!(body["errorCategory"], category);
    assert_eq!(body["isRetryable"], is_retryable);
    assert_eq!(body["message"], message);
    assert_eq!(body["httpStatus"], http_status);
    assert!(
        body["description"]
            .as_str()
            .is_some_and(|value| !value.is_empty()),
        "description should be present: {body}"
    );
}

fn get(uri: &str, key: &str) -> Request<Body> {
    Request::builder()
        .method("GET")
        .uri(uri)
        .header("x-api-key", key)
        .body(Body::empty())
        .expect("request")
}

fn post(uri: &str, key: &str, body: Value) -> Request<Body> {
    Request::builder()
        .method("POST")
        .uri(uri)
        .header("x-api-key", key)
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
        .expect("request")
}

#[tokio::test]
async fn rest_api_resolves_api_token_to_configured_context() {
    let app = test_app(test_config()).await;

    let external = app
        .clone()
        .oneshot(get(
            "/api/v1/notes/00Journal/private.md",
            "external-dev-token",
        ))
        .await
        .expect("external response");
    assert_eq!(external.status(), StatusCode::NOT_FOUND);
    let external_body = response_json(external).await;
    assert_eq!(external_body["error"], "not found");
    assert_structured_error(&external_body, "business", false, "resource not found", 404);

    let local = app
        .oneshot(get("/api/v1/notes/00Journal/private.md", "local-dev-token"))
        .await
        .expect("local response");
    assert_eq!(local.status(), StatusCode::OK);
    let body = response_json(local).await;
    assert_eq!(body["id"], "00Journal/private.md");
}

#[tokio::test]
async fn rest_validation_errors_are_structured() {
    let app = test_app(test_config()).await;

    let empty_query = app
        .clone()
        .oneshot(get("/api/v1/search?q=", "external-dev-token"))
        .await
        .expect("empty query response");
    assert_eq!(empty_query.status(), StatusCode::BAD_REQUEST);
    let body = response_json(empty_query).await;
    assert_eq!(body["error"], "q is required");
    assert_structured_error(&body, "validation", false, "bad request", 400);

    let malformed_query = app
        .oneshot(get(
            "/api/v1/search?q=rust&mode=not-a-mode",
            "external-dev-token",
        ))
        .await
        .expect("malformed query response");
    assert_eq!(malformed_query.status(), StatusCode::BAD_REQUEST);
    let body = response_json(malformed_query).await;
    assert_structured_error(&body, "validation", false, "bad request", 400);
    assert!(
        body["error"]
            .as_str()
            .unwrap_or_default()
            .contains("Failed to deserialize"),
        "extractor rejection should be returned as a structured REST error: {body}"
    );
}

#[tokio::test]
async fn rest_permission_errors_are_structured() {
    let mut config = AppConfig::default();
    config
        .contexts
        .insert("research".to_string(), AccessPolicy::default());

    let app = test_app(config).await;
    let response = app
        .oneshot(post(
            "/api/v1/notes",
            "research-token",
            json!({
                "title": "Denied Create",
                "content": "# Denied Create\n\nThis context has no create policy."
            }),
        ))
        .await
        .expect("permission response");

    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    let body = response_json(response).await;
    assert_structured_error(&body, "permission", false, "write denied", 403);
    assert!(
        body["error"]
            .as_str()
            .unwrap_or_default()
            .contains("denied"),
        "legacy error should remain human-readable: {body}"
    );
}

#[tokio::test]
async fn openapi_documents_structured_rest_errors() {
    let app = test_app(test_config()).await;

    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/api-doc/openapi.json")
                .body(Body::empty())
                .expect("openapi request"),
        )
        .await
        .expect("openapi response");

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_json(response).await;
    let schema = &body["components"]["schemas"]["ApiError"];
    for field in [
        "error",
        "errorCategory",
        "isRetryable",
        "message",
        "description",
        "httpStatus",
    ] {
        assert!(
            schema["required"]
                .as_array()
                .expect("required fields")
                .iter()
                .any(|value| value == field),
            "ApiError should require {field}: {schema}"
        );
    }
    assert_eq!(schema["properties"]["error"]["type"], "string");
    assert_eq!(
        body["components"]["schemas"]["ApiErrorCategory"]["enum"],
        json!(["transient", "validation", "business", "permission"])
    );
    assert_eq!(
        body["paths"]["/api/v1/notes"]["post"]["responses"]["503"]["content"]["application/json"]["schema"]
            ["$ref"],
        "#/components/schemas/ApiError"
    );
    assert!(body["paths"].get("/api/v1/tags/{tag}/notes").is_none());
    assert!(
        body["components"]["schemas"]
            .get("NotesByTagResponse")
            .is_none()
    );
    assert!(
        body["components"]["schemas"]
            .get("QueryBaseRequest")
            .is_some()
    );
    assert_eq!(
        body["paths"]["/api/v1/base/query"]["post"]["responses"]["200"]["content"]["application/json"]
            ["schema"]["$ref"],
        "#/components/schemas/QueryBaseResponse"
    );
}

#[tokio::test]
async fn query_base_rest_endpoint_returns_structured_rows() {
    let app = test_app(test_config()).await;

    let response = app
        .oneshot(post(
            "/api/v1/base/query",
            "external-dev-token",
            json!({
                "base_query": "filters:\n  and:\n    - file.hasTag(\"rust\")\nviews:\n  - type: table\n    name: Rust notes\n    order:\n      - file.name\n      - file.tags\n    limit: 10\n",
                "view": "Rust notes"
            }),
        ))
        .await
        .expect("base query response");

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_json(response).await;
    assert_eq!(body["view"], "Rust notes");
    assert_eq!(body["columns"][0]["id"], "file.name");
    assert!(body["rows"].as_array().expect("rows").len() >= 3);
    assert_eq!(body["truncated"], false);
}

#[tokio::test]
async fn removed_notes_by_tag_rest_route_is_not_available() {
    let app = test_app(test_config()).await;

    let response = app
        .oneshot(get("/api/v1/tags/shared/notes", "external-dev-token"))
        .await
        .expect("removed route response");

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn query_and_search_do_not_expose_hidden_counts() {
    let app = test_app(test_config()).await;

    let response = app
        .oneshot(post(
            "/api/v1/notes/query",
            "external-dev-token",
            json!({"text_query": "private", "limit": 10}),
        ))
        .await
        .expect("query response");

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_json(response).await;
    assert_eq!(body["total_filtered"], 0);
    assert_eq!(body["notes"].as_array().expect("notes").len(), 0);
}

#[tokio::test]
async fn custom_context_names_are_first_class() {
    let mut config = AppConfig::default();
    config.api_tokens.insert(
        "research".to_string(),
        ApiTokenConfig {
            context: "research".to_string(),
        },
    );
    let mut policy = AccessPolicy::default();
    policy.read = vec![AccessRule::allow(AccessMatcher {
        path_prefix: Some("03Concepts/".to_string()),
        ..Default::default()
    })];
    config.contexts.insert("research".to_string(), policy);

    let app = test_app(config).await;

    let allowed = app
        .clone()
        .oneshot(get(
            "/api/v1/notes/03Concepts/rust-phantom-types.md",
            "research-token",
        ))
        .await
        .expect("allowed response");
    assert_eq!(allowed.status(), StatusCode::OK);

    let denied = app
        .oneshot(get("/api/v1/notes/00Journal/private.md", "research-token"))
        .await
        .expect("denied response");
    assert_eq!(denied.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn create_note_with_template_id_rejects_missing_template_frontmatter() {
    let app = test_app(test_config()).await;

    let template = app
        .clone()
        .oneshot(post(
            "/api/v1/notes",
            "external-dev-token",
            json!({
                "title": "Template Source",
                "content": "---\nstatus: draft\n---\n\n# Template Source\n\nTemplate body."
            }),
        ))
        .await
        .expect("template create response");
    assert_eq!(template.status(), StatusCode::OK);
    let template = response_json(template).await;
    let template_id = template["id"].as_str().expect("template id");

    let create = app
        .oneshot(post(
            "/api/v1/notes",
            "external-dev-token",
            json!({
                "title": "Template Consumer",
                "template_id": template_id,
                "content": "# Template Consumer\n\nMissing required frontmatter."
            }),
        ))
        .await
        .expect("create response");

    assert_eq!(create.status(), StatusCode::BAD_REQUEST);
    let body = response_json(create).await;
    assert!(
        body["description"]
            .as_str()
            .unwrap_or_default()
            .contains("status"),
        "template validation should report the missing key: {body}"
    );
}

#[tokio::test]
async fn create_base_file_uses_base_extension_without_indexing_note() {
    let app = test_app(test_config()).await;
    let content = "views:\n  - type: table\n    name: Dashboard\n";

    let create = app
        .clone()
        .oneshot(post(
            "/api/v1/notes",
            "external-dev-token",
            json!({
                "title": "Project Dashboard",
                "file_type": "base",
                "content": content
            }),
        ))
        .await
        .expect("create response");

    assert_eq!(create.status(), StatusCode::OK);
    let created = response_json(create).await;
    let id = created["id"].as_str().expect("created id");
    assert!(id.ends_with("project-dashboard.base"));
    assert_eq!(created["file_type"], "base");
    assert_eq!(created["indexed_as_note"], false);

    // Notes API still 404s because base files are not indexed notes.
    let read_note = app
        .clone()
        .oneshot(get(&format!("/api/v1/notes/{id}"), "external-dev-token"))
        .await
        .expect("note read response");
    assert_eq!(read_note.status(), StatusCode::NOT_FOUND);

    // Vault-files API returns the raw content.
    let read = app
        .oneshot(get(
            &format!("/api/v1/vault-files/{id}"),
            "external-dev-token",
        ))
        .await
        .expect("vault file read response");
    assert_eq!(read.status(), StatusCode::OK);
    let file = response_json(read).await;
    assert_eq!(file["id"], json!(id));
    assert_eq!(file["file_type"], "base");
    assert_eq!(file["content"], content);
    assert!(file["size_bytes"].as_u64().unwrap_or(0) > 0);
}

#[tokio::test]
async fn create_note_applies_context_write_mutations() {
    let app = test_app(test_config()).await;

    let create = app
        .clone()
        .oneshot(post(
            "/api/v1/notes",
            "external-dev-token",
            json!({
                "title": "Context Created Note",
                "content": "---\ntags: [custom]\n---\n\n# Context Created Note\n\nCreated through the context policy."
            }),
        ))
        .await
        .expect("create response");

    assert_eq!(create.status(), StatusCode::OK);
    let created = response_json(create).await;
    let id = created["id"].as_str().expect("created id");

    let read = app
        .oneshot(get(&format!("/api/v1/notes/{id}"), "external-dev-token"))
        .await
        .expect("read response");
    assert_eq!(read.status(), StatusCode::OK);
    let body = response_json(read).await;
    let tags = body["tags"].as_array().expect("tags");
    assert!(tags.iter().any(|tag| tag == "ai-created"));
    assert_eq!(body["frontmatter"]["created_by"], "api_token:external");
    assert!(body["frontmatter"].get("vault_bridge").is_none());
}

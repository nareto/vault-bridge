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

async fn create_template(app: &axum::Router, title: &str, content: &str) -> String {
    let response = app
        .clone()
        .oneshot(post(
            "/api/v1/notes",
            "external-dev-token",
            json!({
                "title": title,
                "content": content
            }),
        ))
        .await
        .expect("template create response");
    assert_eq!(response.status(), StatusCode::OK);
    response_json(response).await["id"]
        .as_str()
        .expect("template id")
        .to_string()
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

fn put(uri: &str, key: &str, body: Value) -> Request<Body> {
    Request::builder()
        .method("PUT")
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
    let policy = AccessPolicy {
        read: vec![AccessRule::allow(AccessMatcher {
            path_prefix: Some("03Concepts/".to_string()),
            ..Default::default()
        })],
        ..Default::default()
    };
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
async fn create_note_with_template_id_accepts_populated_null_defaults() {
    let app = test_app(test_config()).await;
    let template_id = create_template(
        &app,
        "Nullable Template",
        "---\napplied_date: null\ncompany: null\nsource_ids: []\nstatus: prospect\n---\n\n# Nullable Template",
    )
    .await;

    let create = app
        .oneshot(post(
            "/api/v1/notes",
            "external-dev-token",
            json!({
                "title": "Populated Nullable Fields",
                "template_id": template_id,
                "content": "---\napplied_date: 2026-07-16\ncompany: Example Co\nsource_ids:\n  - listing_url:https://example.com/jobs/123\nstatus: applied\n---\n\n# Populated Nullable Fields"
            }),
        ))
        .await
        .expect("create response");

    assert_eq!(create.status(), StatusCode::OK);
}

#[tokio::test]
async fn create_note_with_template_id_accepts_null_for_null_defaults() {
    let app = test_app(test_config()).await;
    let template_id = create_template(
        &app,
        "Nullable Template",
        "---\napplied_date: null\ncompany: null\n---\n\n# Nullable Template",
    )
    .await;

    let create = app
        .oneshot(post(
            "/api/v1/notes",
            "external-dev-token",
            json!({
                "title": "Unpopulated Nullable Fields",
                "template_id": template_id,
                "content": "---\napplied_date: null\ncompany: null\n---\n\n# Unpopulated Nullable Fields"
            }),
        ))
        .await
        .expect("create response");

    assert_eq!(create.status(), StatusCode::OK);
}

#[tokio::test]
async fn create_note_with_template_id_requires_null_default_keys() {
    let app = test_app(test_config()).await;
    let template_id = create_template(
        &app,
        "Nullable Template",
        "---\ncompany: null\nstatus: prospect\n---\n\n# Nullable Template",
    )
    .await;

    let create = app
        .oneshot(post(
            "/api/v1/notes",
            "external-dev-token",
            json!({
                "title": "Missing Nullable Field",
                "template_id": template_id,
                "content": "---\nstatus: applied\n---\n\n# Missing Nullable Field"
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
            .contains("content is missing template frontmatter key 'company'"),
        "template validation should still require null-default keys: {body}"
    );
}

#[tokio::test]
async fn create_note_with_template_id_rejects_non_null_type_mismatch() {
    let app = test_app(test_config()).await;
    let template_id = create_template(
        &app,
        "Typed Template",
        "---\nsource_ids: []\n---\n\n# Typed Template",
    )
    .await;

    let create = app
        .oneshot(post(
            "/api/v1/notes",
            "external-dev-token",
            json!({
                "title": "Invalid Typed Field",
                "template_id": template_id,
                "content": "---\nsource_ids: listing_url:https://example.com/jobs/123\n---\n\n# Invalid Typed Field"
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
            .contains("type string, expected array from template"),
        "template validation should retain non-null type constraints: {body}"
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

#[tokio::test]
async fn raw_markdown_read_uses_indexed_tag_policy() {
    let mut config = test_config();
    let external = config
        .contexts
        .get_mut("external")
        .expect("external context");
    external.read = vec![
        AccessRule::deny(AccessMatcher {
            tags_any: vec!["personal".to_string()],
            ..Default::default()
        }),
        AccessRule::allow(AccessMatcher::allow_all()),
    ];
    let app = test_app(config).await;

    let create = app
        .clone()
        .oneshot(post(
            "/api/v1/vault-files",
            "admin-dev-token",
            json!({
                "title": "Tag Hidden Raw File",
                "content": "---\ntags: [personal]\n---\n\n# Tag Hidden Raw File\n\nPrivate body."
            }),
        ))
        .await
        .expect("create response");
    assert_eq!(create.status(), StatusCode::OK);
    let created = response_json(create).await;
    let id = created["id"].as_str().expect("created id");

    let note_read = app
        .clone()
        .oneshot(get(&format!("/api/v1/notes/{id}"), "external-dev-token"))
        .await
        .expect("note read response");
    assert_eq!(note_read.status(), StatusCode::NOT_FOUND);

    let raw_read = app
        .oneshot(get(
            &format!("/api/v1/vault-files/{id}"),
            "external-dev-token",
        ))
        .await
        .expect("raw read response");
    assert_eq!(raw_read.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn raw_markdown_edit_keeps_policy_metadata_and_index_in_sync() {
    let app = test_app(test_config()).await;
    let create = app
        .clone()
        .oneshot(post(
            "/api/v1/vault-files",
            "external-dev-token",
            json!({
                "title": "Raw Edit Contract",
                "content": "---\ntags: [custom]\nstatus: draft\n---\n\n# Raw Edit Contract\n\nOriginal body."
            }),
        ))
        .await
        .expect("create response");
    assert_eq!(create.status(), StatusCode::OK);
    let created = response_json(create).await;
    let id = created["id"].as_str().expect("created id").to_string();

    let before = app
        .clone()
        .oneshot(get(&format!("/api/v1/notes/{id}"), "external-dev-token"))
        .await
        .expect("initial note read");
    let before = response_json(before).await;
    let created_at = before["frontmatter"]["created"].clone();

    let edit = app
        .clone()
        .oneshot(put(
            &format!("/api/v1/vault-files/{id}"),
            "external-dev-token",
            json!({
                "content_patch": [{
                    "op": "replace",
                    "old": "Original body.",
                    "new": "Updated body."
                }],
                "tags": ["custom-updated"],
                "metadata": {
                    "status": "done",
                    "created": "forged",
                    "created_by": "forged"
                }
            }),
        ))
        .await
        .expect("edit response");
    assert_eq!(edit.status(), StatusCode::OK);

    let raw = app
        .clone()
        .oneshot(get(
            &format!("/api/v1/vault-files/{id}"),
            "external-dev-token",
        ))
        .await
        .expect("raw read response");
    assert_eq!(raw.status(), StatusCode::OK);
    let raw = response_json(raw).await;
    let raw_content = raw["content"].as_str().expect("raw content");
    assert!(raw_content.contains("Updated body."));
    assert!(raw_content.contains("custom-updated"));
    assert!(raw_content.contains("ai-created"));
    assert!(!raw_content.contains("created_by: forged"));

    let note = app
        .clone()
        .oneshot(get(&format!("/api/v1/notes/{id}"), "external-dev-token"))
        .await
        .expect("note read response");
    let note = response_json(note).await;
    assert_eq!(note["frontmatter"]["status"], "done");
    assert_eq!(note["frontmatter"]["created"], created_at);
    assert_eq!(note["frontmatter"]["created_by"], "api_token:external");
    assert!(note["tags"].as_array().is_some_and(|tags| {
        tags.iter().any(|tag| tag == "custom-updated") && tags.iter().any(|tag| tag == "ai-created")
    }));

    let invalid = app
        .clone()
        .oneshot(put(
            &format!("/api/v1/vault-files/{id}"),
            "external-dev-token",
            json!({
                "content": "replacement",
                "content_patch": [{"op": "append", "text": "also patch"}]
            }),
        ))
        .await
        .expect("invalid edit response");
    assert_eq!(invalid.status(), StatusCode::BAD_REQUEST);

    let ambiguous = app
        .oneshot(put(
            &format!("/api/v1/vault-files/{id}"),
            "external-dev-token",
            json!({
                "content_patch": [{
                    "op": "replace",
                    "old": "---",
                    "new": "Ambiguous"
                }]
            }),
        ))
        .await
        .expect("ambiguous edit response");
    assert_eq!(ambiguous.status(), StatusCode::BAD_REQUEST);
}

use std::collections::HashSet;
use std::fs;
use std::sync::Arc;

use axum::body::Body;
use axum::extract::{Path, State};
use axum::http::{Request, StatusCode};
use axum::routing::put;
use axum::{Json, Router};
use http_body_util::BodyExt;
use serde_json::{Value, json};
use tokio::sync::Mutex;
use tower::ServiceExt;

use vault_bridge::api::{ApiTokenState, AppState, app_router};
use vault_bridge::authorization::AccessPolicy;
use vault_bridge::config::{AppConfig, CouchDbConfig, EncryptionConfig, FeedMode};
use vault_bridge::couchdb::{CouchDbClient, build_livesync_note_documents};
use vault_bridge::encryption::Decryptor;
use vault_bridge::runtime_config::RuntimeConfigState;
use vault_bridge::service::VaultBridgeService;
use vault_bridge::store::VaultStore;

#[derive(Clone, Default)]
struct MockCouchState {
    upserted_docs: Arc<Mutex<Vec<(String, Value)>>>,
    existing_ids: Arc<Mutex<HashSet<String>>>,
}

async fn mock_put_document(
    State(state): State<MockCouchState>,
    Path((_db, doc_id)): Path<(String, String)>,
    Json(doc): Json<Value>,
) -> (StatusCode, Json<Value>) {
    let mut existing = state.existing_ids.lock().await;
    if !existing.insert(doc_id.clone()) {
        return (
            StatusCode::CONFLICT,
            Json(json!({
                "error": "conflict",
                "reason": "Document update conflict."
            })),
        );
    }

    let mut guard = state.upserted_docs.lock().await;
    guard.push((doc_id, doc));
    (
        StatusCode::OK,
        Json(json!({
            "ok": true
        })),
    )
}

async fn mock_put_document_unavailable() -> (StatusCode, Json<Value>) {
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(json!({
            "error": "server_error",
            "reason": "temporary upstream failure"
        })),
    )
}

fn spawn_mock_couchdb() -> (String, MockCouchState) {
    let state = MockCouchState::default();
    let app = Router::new()
        .route("/{db}/{doc_id}", put(mock_put_document))
        .with_state(state.clone());

    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind mock couchdb listener");
    listener
        .set_nonblocking(true)
        .expect("set non-blocking listener");
    let addr = listener.local_addr().expect("read mock couchdb addr");

    tokio::spawn(async move {
        let listener =
            tokio::net::TcpListener::from_std(listener).expect("tokio listener from std");
        axum::serve(listener, app)
            .await
            .expect("serve mock couchdb");
    });

    (format!("http://{addr}"), state)
}

fn spawn_unavailable_mock_couchdb() -> String {
    let app = Router::new().route("/{db}/{doc_id}", put(mock_put_document_unavailable));

    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind mock couchdb listener");
    listener
        .set_nonblocking(true)
        .expect("set non-blocking listener");
    let addr = listener.local_addr().expect("read mock couchdb addr");

    tokio::spawn(async move {
        let listener =
            tokio::net::TcpListener::from_std(listener).expect("tokio listener from std");
        axum::serve(listener, app)
            .await
            .expect("serve mock couchdb");
    });

    format!("http://{addr}")
}

async fn app_state_with_couchdb(couchdb: Arc<CouchDbClient>) -> AppState {
    let mut config = AppConfig::default();
    config
        .contexts
        .insert("external".to_string(), AccessPolicy::default_agent());
    let runtime_config = RuntimeConfigState::for_tests(&config);
    let store = VaultStore::new(20);
    store.set_authorization_config(config.contexts).await;
    AppState {
        service: VaultBridgeService::new(store, Some(couchdb)),
        api_tokens: ApiTokenState::for_tests([("external", "external-dev-token", "external")]),
        mcp: None,
        runtime_config,
    }
}

#[tokio::test]
async fn create_note_with_couchdb_configured_is_immediately_readable() {
    let (couchdb_url, mock_state) = spawn_mock_couchdb();
    let couchdb = Arc::new(
        CouchDbClient::new(&CouchDbConfig {
            url: couchdb_url,
            database: "mainvault".to_string(),
            username: "user".to_string(),
            password: "pass".to_string(),
            poll_interval_seconds: 5,
            feed_mode: FeedMode::Longpoll,
            encryption: EncryptionConfig::default(),
            ..Default::default()
        })
        .expect("build couchdb client"),
    );

    let state = app_state_with_couchdb(couchdb).await;
    let app = app_router(state);

    let payload = json!({
        "title": "Write Through Only",
        "content": "---\ntags: [integration]\n---\n\n# Write Through Only\n\nCreated via API in CouchDB mode."
    });

    let create = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/notes")
                .header("content-type", "application/json")
                .header("x-api-key", "external-dev-token")
                .body(Body::from(payload.to_string()))
                .expect("create request"),
        )
        .await
        .expect("create response");
    assert_eq!(create.status(), StatusCode::OK);
    let create_body = create.into_body().collect().await.expect("body").to_bytes();
    let create_json: Value = serde_json::from_slice(&create_body).expect("create json");
    let created_id = create_json["id"].as_str().expect("id string");

    let read = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!("/api/v1/notes/{created_id}"))
                .header("x-api-key", "external-dev-token")
                .body(Body::empty())
                .expect("read request"),
        )
        .await
        .expect("read response");
    assert_eq!(read.status(), StatusCode::OK);
    let read_body = read.into_body().collect().await.expect("body").to_bytes();
    let read_json: Value = serde_json::from_slice(&read_body).expect("read json");
    assert!(
        read_json["content"]
            .as_str()
            .unwrap_or_default()
            .contains("Created via API in CouchDB mode."),
        "the note should be readable from the API immediately after create"
    );

    let docs = mock_state.upserted_docs.lock().await.clone();
    assert_eq!(docs.len(), 2);

    let file_doc = docs
        .iter()
        .find(|(id, _)| id.ends_with(".md"))
        .map(|(_, doc)| doc)
        .expect("file document should be written");
    let leaf_doc = docs
        .iter()
        .find(|(id, _)| id.starts_with("h:"))
        .map(|(_, doc)| doc)
        .expect("leaf document should be written");

    assert_eq!(file_doc["type"], "plain");
    assert_eq!(file_doc["path"], created_id);
    assert_eq!(file_doc["children"].as_array().map(|v| v.len()), Some(1));
    assert_eq!(leaf_doc["type"], "leaf");
    assert_eq!(leaf_doc["e_"], true);

    let payload = leaf_doc["data"]
        .as_str()
        .expect("leaf data should be raw markdown");
    assert!(
        payload.contains("Write Through Only"),
        "leaf payload should include the supplied title heading"
    );
}

#[tokio::test]
async fn create_note_with_couchdb_duplicate_path_returns_conflict() {
    let (couchdb_url, _mock_state) = spawn_mock_couchdb();
    let couchdb = Arc::new(
        CouchDbClient::new(&CouchDbConfig {
            url: couchdb_url,
            database: "mainvault".to_string(),
            username: "user".to_string(),
            password: "pass".to_string(),
            poll_interval_seconds: 5,
            feed_mode: FeedMode::Longpoll,
            encryption: EncryptionConfig::default(),
            ..Default::default()
        })
        .expect("build couchdb client"),
    );

    let state = app_state_with_couchdb(couchdb).await;
    let app = app_router(state);

    let payload = json!({
        "title": "Duplicate Through CouchDB",
        "content": "---\ntags: [integration]\n---\n\n# Duplicate Through CouchDB\n\nCreated via API in CouchDB mode."
    });

    let first = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/notes")
                .header("content-type", "application/json")
                .header("x-api-key", "external-dev-token")
                .body(Body::from(payload.to_string()))
                .expect("first request"),
        )
        .await
        .expect("first response");
    assert_eq!(first.status(), StatusCode::OK);

    let second = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/notes")
                .header("content-type", "application/json")
                .header("x-api-key", "external-dev-token")
                .body(Body::from(payload.to_string()))
                .expect("second request"),
        )
        .await
        .expect("second response");
    assert_eq!(second.status(), StatusCode::CONFLICT);
    let body = second.into_body().collect().await.expect("body").to_bytes();
    let json: Value = serde_json::from_slice(&body).expect("json");
    assert!(
        json["error"]
            .as_str()
            .unwrap_or_default()
            .contains("duplicate-through-couchdb"),
        "conflict should expose the existing note path"
    );
    assert_eq!(json["errorCategory"], "business");
    assert_eq!(json["isRetryable"], false);
    assert_eq!(json["message"], "resource already exists");
    assert_eq!(json["httpStatus"], 409);
    assert!(
        json["description"]
            .as_str()
            .unwrap_or_default()
            .contains("duplicate-through-couchdb"),
        "structured description should expose the existing note path"
    );
}

#[tokio::test]
async fn create_note_with_couchdb_unavailable_returns_retryable_transient_error() {
    let couchdb_url = spawn_unavailable_mock_couchdb();
    let couchdb = Arc::new(
        CouchDbClient::new(&CouchDbConfig {
            url: couchdb_url,
            database: "mainvault".to_string(),
            username: "user".to_string(),
            password: "pass".to_string(),
            poll_interval_seconds: 5,
            feed_mode: FeedMode::Longpoll,
            encryption: EncryptionConfig::default(),
            ..Default::default()
        })
        .expect("build couchdb client"),
    );

    let state = app_state_with_couchdb(couchdb).await;
    let app = app_router(state);

    let payload = json!({
        "title": "Unavailable CouchDB",
        "content": "---\ntags: [integration]\n---\n\n# Unavailable CouchDB\n\nCreated via API in CouchDB mode."
    });

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/notes")
                .header("content-type", "application/json")
                .header("x-api-key", "external-dev-token")
                .body(Body::from(payload.to_string()))
                .expect("create request"),
        )
        .await
        .expect("create response");

    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    let body = response
        .into_body()
        .collect()
        .await
        .expect("body")
        .to_bytes();
    let json: Value = serde_json::from_slice(&body).expect("json");
    assert!(json["error"].as_str().is_some());
    assert_eq!(json["errorCategory"], "transient");
    assert_eq!(json["isRetryable"], true);
    assert_eq!(json["message"], "persistence failed");
    assert_eq!(json["httpStatus"], 503);
}

#[tokio::test]
async fn create_note_with_encrypted_couchdb_writes_hkdf_payloads() {
    let (couchdb_url, mock_state) = spawn_mock_couchdb();
    let crypto = Arc::new(Decryptor::new("test-passphrase", &[0x42u8; 32]));
    let couchdb = Arc::new(
        CouchDbClient::new(&CouchDbConfig {
            url: couchdb_url,
            database: "mainvault".to_string(),
            username: "user".to_string(),
            password: "pass".to_string(),
            poll_interval_seconds: 5,
            feed_mode: FeedMode::Longpoll,
            encryption: EncryptionConfig {
                passphrase: "test-passphrase".to_string(),
            },
            ..Default::default()
        })
        .expect("build couchdb client")
        .with_livesync_crypto(Some(crypto.clone())),
    );

    let state = app_state_with_couchdb(couchdb).await;
    let app = app_router(state);

    let payload = json!({
        "title": "Encrypted Write Through",
        "content": "---\ntags: [integration]\n---\n\n# Encrypted Write Through\n\nCreated via API in encrypted CouchDB mode."
    });

    let create = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/notes")
                .header("content-type", "application/json")
                .header("x-api-key", "external-dev-token")
                .body(Body::from(payload.to_string()))
                .expect("create request"),
        )
        .await
        .expect("create response");
    assert_eq!(create.status(), StatusCode::OK);

    let docs = mock_state.upserted_docs.lock().await.clone();
    assert_eq!(docs.len(), 2);

    let file_doc = docs
        .iter()
        .find(|(id, _)| id.starts_with("f:"))
        .map(|(_, doc)| doc)
        .expect("file document should be written");
    let leaf_doc = docs
        .iter()
        .find(|(id, _)| id.starts_with("h:+"))
        .map(|(_, doc)| doc)
        .expect("leaf document should be written");

    let encrypted_path = file_doc["path"].as_str().expect("encrypted path");
    assert!(encrypted_path.starts_with("/\\:%="));
    let meta = crypto
        .decrypt_meta_document(encrypted_path)
        .expect("decrypt metadata json");
    let note_path = meta["path"].as_str().expect("metadata path");
    assert!(note_path.starts_with("11New/"));
    assert_eq!(file_doc["children"], json!([]));
    assert_eq!(file_doc["ctime"], 0);
    assert_eq!(file_doc["mtime"], 0);
    assert_eq!(file_doc["size"], 0);
    assert_eq!(meta["children"][0], leaf_doc["_id"]);
    assert!(meta["mtime"].as_i64().unwrap_or_default() > 0);
    assert!(meta["size"].as_i64().unwrap_or_default() > 0);

    let encrypted_payload = leaf_doc["data"].as_str().expect("encrypted leaf payload");
    assert!(encrypted_payload.starts_with("%="));
    let payload = crypto
        .decrypt(encrypted_payload)
        .expect("decrypt leaf payload");
    assert!(
        payload.contains("Encrypted Write Through"),
        "encrypted leaf payload should preserve markdown content"
    );
}

#[test]
fn generated_livesync_docs_match_appendix_c_field_contract() {
    let fixture_path = "livesync_schema_probe.json";
    let fixture = fs::read_to_string(fixture_path)
        .unwrap_or_else(|error| panic!("failed to read {fixture_path}: {error}"));
    let fixture_json: Value = serde_json::from_str(&fixture).expect("fixture json");

    let expected_file_fields = fixture_json["5_size_variance_analysis"]["smallest_documents"]
        .as_array()
        .expect("smallest_documents array")
        .first()
        .and_then(|doc| doc["field_names"].as_array())
        .expect("file field_names")
        .iter()
        .filter_map(Value::as_str)
        .filter(|field| !field.starts_with('_') && *field != "deleted")
        .map(ToString::to_string)
        .collect::<HashSet<_>>();

    let expected_leaf_fields = fixture_json["4_changes_feed"]["events"]
        .as_array()
        .expect("changes events")
        .iter()
        .find(|event| {
            event["id"]
                .as_str()
                .is_some_and(|doc_id| doc_id.starts_with("h:"))
        })
        .and_then(|event| event["doc_field_names"].as_array())
        .expect("leaf doc_field_names")
        .iter()
        .filter_map(Value::as_str)
        .filter(|field| !field.starts_with('_'))
        .map(ToString::to_string)
        .collect::<HashSet<_>>();

    let generated = build_livesync_note_documents(
        "11New/2026-02-26-appendix-c-fixture-contract.md",
        "# Appendix C Fixture Contract\n\nGenerated for compatibility checks.",
    );
    let file_doc = generated.file_doc.as_object().expect("file doc object");
    let leaf_doc = generated.leaf_doc.as_object().expect("leaf doc object");

    for field in expected_file_fields {
        assert!(
            file_doc.contains_key(&field),
            "generated file doc missing Appendix C field `{field}`"
        );
    }
    for field in expected_leaf_fields {
        assert!(
            leaf_doc.contains_key(&field),
            "generated leaf doc missing Appendix C field `{field}`"
        );
    }

    let leaf_payload = generated
        .leaf_doc
        .get("data")
        .and_then(Value::as_str)
        .expect("leaf data payload string");
    assert!(
        leaf_payload.contains("Appendix C Fixture Contract"),
        "leaf payload content should preserve markdown body"
    );
}

#[test]
fn appendix_c_fixture_includes_deletion_samples_and_decoder_contract_annotations() {
    let fixture_path = "livesync_schema_probe.json";
    let fixture = fs::read_to_string(fixture_path)
        .unwrap_or_else(|error| panic!("failed to read {fixture_path}: {error}"));
    let fixture_json: Value = serde_json::from_str(&fixture).expect("fixture json");

    let deletion_markers = &fixture_json["6_deletion_markers"];
    let found = deletion_markers["found"].as_u64().unwrap_or(0);
    assert!(
        found >= 1,
        "Appendix C fixture must include at least one deletion marker sample"
    );

    let samples = deletion_markers["samples"]
        .as_array()
        .expect("deletion marker samples array");
    assert!(
        !samples.is_empty(),
        "deletion marker samples should not be empty"
    );

    let has_tombstone_shape = samples.iter().any(|sample| {
        sample["deleted"].as_bool().unwrap_or(false)
            || sample["doc"]["_deleted"].as_bool().unwrap_or(false)
            || sample["doc"]["deleted"].as_bool().unwrap_or(false)
    });
    assert!(
        has_tombstone_shape,
        "deletion marker samples must include either deleted=true or doc._deleted/doc.deleted"
    );

    let contract = &fixture_json["7_decoder_field_contract"];
    assert!(
        contract.is_object(),
        "Appendix C fixture should include 7_decoder_field_contract annotations"
    );

    let file_required = contract["file_document"]["required_for_decode"]
        .as_array()
        .expect("file_document.required_for_decode");
    let leaf_required = contract["leaf_document"]["required_for_decode"]
        .as_array()
        .expect("leaf_document.required_for_decode");
    let change_required = contract["change_event"]["required_for_decode"]
        .as_array()
        .expect("change_event.required_for_decode");

    for field in ["_id", "_rev", "children", "path", "type"] {
        assert!(
            file_required
                .iter()
                .any(|item| item.as_str() == Some(field)),
            "file contract missing required field `{field}`"
        );
    }
    for field in ["_id", "_rev", "data", "type"] {
        assert!(
            leaf_required
                .iter()
                .any(|item| item.as_str() == Some(field)),
            "leaf contract missing required field `{field}`"
        );
    }
    for field in ["id", "seq"] {
        assert!(
            change_required
                .iter()
                .any(|item| item.as_str() == Some(field)),
            "change event contract missing required field `{field}`"
        );
    }
}

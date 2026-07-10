use std::fs;
use std::sync::Arc;

use axum::extract::{Path, State};
use axum::routing::put;
use axum::{Json, Router};
use serde_json::{Value, json};
use tokio::sync::Mutex;

use vault_bridge::config::{CouchDbConfig, EncryptionConfig, FeedMode};
use vault_bridge::couchdb::CouchDbClient;
use vault_bridge::encryption::Decryptor;

const EXPECTED_SUBMODULE_PATH: &str = "third_party/obsidian-livesync";
const EXPECTED_SUBMODULE_URL: &str = "https://github.com/vrtmrz/obsidian-livesync";
const EXPECTED_OBFUSCATED_FILE_ID: &str =
    "f:f47eb7c286c9b0740f1897938de60d3c18359c49d5d5a9fea8bc30fc34648079";

#[derive(Clone, Default)]
struct MockCouchState {
    upserted_docs: Arc<Mutex<Vec<(String, Value)>>>,
}

async fn mock_put_document(
    State(state): State<MockCouchState>,
    Path((_db, doc_id)): Path<(String, String)>,
    Json(doc): Json<Value>,
) -> Json<Value> {
    let mut guard = state.upserted_docs.lock().await;
    guard.push((doc_id, doc));
    Json(json!({ "ok": true, "rev": "1-mock" }))
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

#[test]
fn pinned_livesync_submodule_is_registered() {
    let gitmodules = fs::read_to_string(".gitmodules").expect("read .gitmodules");
    assert!(
        gitmodules.contains(&format!("path = {EXPECTED_SUBMODULE_PATH}")),
        "expected LiveSync submodule path in .gitmodules"
    );
    assert!(
        gitmodules.contains(&format!("url = {EXPECTED_SUBMODULE_URL}")),
        "expected LiveSync submodule URL in .gitmodules"
    );
}

#[test]
fn pinned_livesync_path_service_contract_is_present() {
    let path_service = fs::read_to_string(
        "third_party/obsidian-livesync/src/lib/src/services/base/PathService.ts",
    )
    .expect("read PathService.ts");
    let path_impl =
        fs::read_to_string("third_party/obsidian-livesync/src/lib/src/string_and_binary/path.ts")
            .expect("read path.ts");

    assert!(
        path_service.contains("setting.usePathObfuscation ? setting.passphrase : \"\""),
        "expected LiveSync path service to derive obfuscated IDs from the vault passphrase"
    );
    assert!(
        path_impl.contains("if (caseInsensitive)")
            && path_impl.contains("filename = filename.toLowerCase()"),
        "expected LiveSync path2id_base to lowercase filenames in case-insensitive mode"
    );
    assert!(
        path_impl.contains("const newPrefix = obfuscatePassphrase ? PREFIX_OBFUSCATED : \"\""),
        "expected LiveSync path2id_base to use the f: obfuscation prefix"
    );
}

#[test]
fn pinned_livesync_hkdf_metadata_contract_is_present() {
    let encryption =
        fs::read_to_string("third_party/obsidian-livesync/src/lib/src/pouchdb/encryption.ts")
            .expect("read encryption.ts");

    assert!(
        encryption.contains("saveDoc.mtime = 0;")
            && encryption.contains("saveDoc.ctime = 0;")
            && encryption.contains("saveDoc.size = 0;"),
        "expected LiveSync HKDF metadata encryption to zero top-level timestamps and size"
    );
    assert!(
        encryption.contains("if (\"children\" in saveDoc) saveDoc.children = [];"),
        "expected LiveSync HKDF metadata encryption to clear top-level children"
    );
}

#[tokio::test]
async fn encrypted_write_matches_pinned_livesync_id_and_metadata_contract() {
    let (couchdb_url, mock_state) = spawn_mock_couchdb();
    let crypto = Arc::new(Decryptor::new("test-passphrase", &[0x42u8; 32]));
    let client = CouchDbClient::new(&CouchDbConfig {
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
    .with_livesync_crypto(Some(crypto.clone()));

    client
        .write_livesync_note("00New/2026-02-26-new-note.md", "# New Note\n\nBody")
        .await
        .expect("write livesync note");

    let docs = mock_state.upserted_docs.lock().await.clone();
    assert_eq!(docs.len(), 2);

    let (file_id, file_doc) = docs
        .iter()
        .find(|(id, _)| id.starts_with("f:"))
        .expect("file document should be written");
    let (leaf_id, leaf_doc) = docs
        .iter()
        .find(|(id, _)| id.starts_with("h:+"))
        .expect("encrypted leaf document should be written");

    assert_eq!(file_id, EXPECTED_OBFUSCATED_FILE_ID);
    assert_eq!(file_doc["_id"], *file_id);
    assert_eq!(file_doc["children"], json!([]));
    assert_eq!(file_doc["ctime"], 0);
    assert_eq!(file_doc["mtime"], 0);
    assert_eq!(file_doc["size"], 0);

    let meta = crypto
        .decrypt_meta_document(file_doc["path"].as_str().expect("encrypted metadata path"))
        .expect("decrypt metadata");
    assert_eq!(meta["path"], "00New/2026-02-26-new-note.md");
    assert_eq!(meta["children"][0], leaf_doc["_id"]);

    let leaf_payload = leaf_doc["data"].as_str().expect("encrypted leaf payload");
    assert!(leaf_payload.starts_with("%="));
    let payload = crypto.decrypt(leaf_payload).expect("decrypt leaf payload");
    assert_eq!(payload, "# New Note\n\nBody");

    assert_eq!(leaf_doc["type"], "leaf");
    assert_eq!(leaf_doc["e_"], true);
    assert_eq!(leaf_doc["_id"], *leaf_id);
}

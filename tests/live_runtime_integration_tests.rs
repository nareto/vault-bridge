use vault_bridge::config::AppConfig;
use vault_bridge::service::VaultBridgeService;
use vault_bridge::store::VaultStore;

#[tokio::test]
async fn live_runtime_smoke_uses_forward_context_config() {
    if std::env::var("VAULT_BRIDGE_LIVE_TESTS").ok().as_deref() != Some("1") {
        eprintln!("skipping live runtime smoke; set VAULT_BRIDGE_LIVE_TESTS=1 to run");
        return;
    }

    let config = AppConfig::default();
    let store = VaultStore::new(20);
    store.seed_example_data().await;
    store.set_authorization_config(config.contexts).await;
    let service = VaultBridgeService::new(store, None);

    let status = service.status().await;
    assert_eq!(status.status, "ok");
    assert!(status.index.total_notes >= 1);
}

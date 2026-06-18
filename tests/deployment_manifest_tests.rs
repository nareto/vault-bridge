use std::fs;

use serde_yaml::Value;

fn mapping_get<'a>(map: &'a serde_yaml::Mapping, key: &str) -> Option<&'a Value> {
    map.get(Value::String(key.to_string()))
}

#[test]
fn docker_compose_runs_single_app_process_with_embedded_mcp() {
    let raw = fs::read_to_string("docker-compose.yml").expect("read docker-compose.yml");
    let manifest: Value = serde_yaml::from_str(&raw).expect("parse docker-compose.yml");

    let root = manifest
        .as_mapping()
        .expect("docker-compose root should be a mapping");
    let services = mapping_get(root, "services")
        .and_then(Value::as_mapping)
        .expect("docker-compose should define services");

    assert!(mapping_get(services, "mcp-external").is_none());
    assert!(mapping_get(services, "mcp-local").is_none());
    assert!(mapping_get(services, "mcp-admin").is_none());

    let service = mapping_get(services, "vault-bridge")
        .and_then(Value::as_mapping)
        .expect("vault-bridge service");
    let ports = mapping_get(service, "ports")
        .and_then(Value::as_sequence)
        .expect("ports");
    assert!(
        ports
            .iter()
            .filter_map(Value::as_str)
            .any(|port| port == "8080:8080")
    );

    let environment = mapping_get(service, "environment")
        .and_then(Value::as_mapping)
        .expect("environment");
    assert_eq!(
        mapping_get(environment, "MCP_BEARER_TOKEN_DIR").and_then(Value::as_str),
        Some("/var/run/vault-bridge-mcp")
    );
    assert_eq!(
        mapping_get(environment, "API_TOKEN_DIR").and_then(Value::as_str),
        Some("/var/run/vault-bridge-api")
    );

    let volumes = mapping_get(service, "volumes")
        .and_then(Value::as_sequence)
        .expect("volumes");
    assert!(
        volumes
            .iter()
            .filter_map(Value::as_str)
            .any(|volume| { volume == "./.secrets/mcp:/var/run/vault-bridge-mcp:ro" })
    );
    assert!(
        volumes
            .iter()
            .filter_map(Value::as_str)
            .any(|volume| { volume == "./.secrets/api:/var/run/vault-bridge-api:ro" })
    );
}

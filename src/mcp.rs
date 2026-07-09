use std::collections::BTreeMap;
use std::path::PathBuf;
use std::time::Duration;

use axum::extract::State;
use axum::http::header::AUTHORIZATION;
use axum::http::{HeaderMap, StatusCode};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use core::convert::Infallible;
use once_cell::sync::Lazy;
use regex::Regex;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use thiserror::Error;
use tokio_stream::StreamExt;
use tokio_stream::wrappers::IntervalStream;
use tracing::{info, warn};

use crate::api_docs;
use crate::authorization::{AccessMatcher, AccessPolicy, AccessRule, AuthContext, ContextName};
use crate::base_query::QueryBaseRequest;
use crate::config::McpTokenConfig;
use crate::error_metadata::{ErrorCategory, ErrorMetadata, service_error_metadata};
use crate::model::NoteId;
use crate::new_note::{NewNoteRequest, UpdateNoteRequest, WriteError};
use crate::runtime_config::{AuthConfigSnapshot, RuntimeAuthConfig};
use crate::service::{ServiceError, VaultBridgeService};
use crate::store::{
    MAX_GRAPH_TRAVERSAL_DEPTH, MAX_NOTE_LIST_LIMIT, NeighborDirection, NoteTimeFilter,
    QueryNotesRequest,
};

const JSONRPC_VERSION: &str = "2.0";
const LATEST_PROTOCOL_VERSION: &str = "2025-11-25";
const SUPPORTED_PROTOCOL_VERSIONS: &[&str] = &[LATEST_PROTOCOL_VERSION, "2025-06-18"];
const SERVER_VERSION: &str = env!("CARGO_PKG_VERSION");
const TOOL_DEFINITIONS_YAML: &str = include_str!("../config/mcp_tools.yaml");
const EMBEDDED_IMAGE_BASE64_MIN_CHARS: usize = 512;
const TOOLING_CATALOG_URI: &str = "vault-bridge://catalog/tooling.json";
const OPENAPI_SCHEMA_URI: &str = "vault-bridge://schemas/openapi.json";
const VAULT_FILE_RESOURCE_PREFIX: &str = "vault-bridge://files/";
const READ_TOOL_NAMES: &[&str] = &[
    "get_vault_file",
    "query_notes",
    "query_base",
    "get_neighbors",
    "list_tags",
];
const CREATE_TOOL_NAMES: &[&str] = &["create_vault_file"];
const EDIT_TOOL_NAMES: &[&str] = &["edit_vault_file"];

static EMBEDDED_IMAGE_DATA_URI_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(&format!(
        r"(?i)data:image/(?P<mime>[a-z0-9.+-]+);base64,(?P<data>[A-Za-z0-9+/=]{{{},}})",
        EMBEDDED_IMAGE_BASE64_MIN_CHARS
    ))
    .expect("embedded image sanitizer regex should compile")
});

#[derive(Clone, Debug)]
pub struct McpState {
    service: VaultBridgeService,
    bearer_token: Option<String>,
    bearer_token_file: Option<PathBuf>,
    bearer_token_dir: Option<PathBuf>,
    auth_config: RuntimeAuthConfig,
}

impl McpState {
    pub fn new(
        service: VaultBridgeService,
        bearer_token: Option<String>,
        bearer_token_file: Option<PathBuf>,
        bearer_token_dir: Option<PathBuf>,
        tokens: BTreeMap<String, McpTokenConfig>,
    ) -> Result<Self, McpError> {
        let snapshot = AuthConfigSnapshot {
            mcp_tokens: tokens,
            ..AuthConfigSnapshot::default()
        };
        Ok(Self {
            service,
            bearer_token,
            bearer_token_file,
            bearer_token_dir,
            auth_config: RuntimeAuthConfig::new(snapshot),
        })
    }

    pub fn new_with_auth_config(
        service: VaultBridgeService,
        bearer_token: Option<String>,
        bearer_token_file: Option<PathBuf>,
        bearer_token_dir: Option<PathBuf>,
        auth_config: RuntimeAuthConfig,
    ) -> Result<Self, McpError> {
        Ok(Self {
            service,
            bearer_token,
            bearer_token_file,
            bearer_token_dir,
            auth_config,
        })
    }

    pub fn from_env(
        service: VaultBridgeService,
        tokens: BTreeMap<String, McpTokenConfig>,
    ) -> Result<Self, McpError> {
        let bearer_token = std::env::var("MCP_BEARER_TOKEN")
            .ok()
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty());
        let bearer_token_file = std::env::var("MCP_BEARER_TOKEN_FILE")
            .ok()
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty())
            .map(PathBuf::from);
        let bearer_token_dir = std::env::var("MCP_BEARER_TOKEN_DIR")
            .ok()
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty())
            .map(PathBuf::from);

        Self::new(
            service,
            bearer_token,
            bearer_token_file,
            bearer_token_dir,
            tokens,
        )
    }

    pub fn from_env_with_auth_config(
        service: VaultBridgeService,
        auth_config: RuntimeAuthConfig,
    ) -> Result<Self, McpError> {
        let bearer_token = std::env::var("MCP_BEARER_TOKEN")
            .ok()
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty());
        let bearer_token_file = std::env::var("MCP_BEARER_TOKEN_FILE")
            .ok()
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty())
            .map(PathBuf::from);
        let bearer_token_dir = std::env::var("MCP_BEARER_TOKEN_DIR")
            .ok()
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty())
            .map(PathBuf::from);

        Self::new_with_auth_config(
            service,
            bearer_token,
            bearer_token_file,
            bearer_token_dir,
            auth_config,
        )
    }

    fn read_token_file(path: &std::path::Path) -> Option<String> {
        let contents = std::fs::read_to_string(path).ok()?;
        let token = contents.trim().to_string();
        if token.is_empty() { None } else { Some(token) }
    }

    async fn expected_bearer_tokens(&self) -> Vec<BearerTokenIdentity> {
        let mut tokens: Vec<BearerTokenIdentity> = Vec::new();
        let snapshot = self.auth_config.snapshot().await;

        if let Some(dir) = self.bearer_token_dir.as_ref()
            && let Ok(entries) = std::fs::read_dir(dir)
        {
            let mut paths = entries
                .filter_map(|entry| entry.ok().map(|entry| entry.path()))
                .filter(|path| path.is_file())
                .collect::<Vec<_>>();
            paths.sort();
            for path in paths {
                if let Some(token) = Self::read_token_file(&path)
                    && !tokens.iter().any(|identity| identity.token == token)
                    && let Some(identity) =
                        self.identity_for_token_path(&snapshot.mcp_tokens, &path, token)
                {
                    tokens.push(identity);
                }
            }
        }

        if let Some(path) = self.bearer_token_file.as_ref()
            && let Some(token) = Self::read_token_file(path)
            && !tokens.iter().any(|identity| identity.token == token)
            && let Some(identity) = self.identity_for_token_path(&snapshot.mcp_tokens, path, token)
        {
            tokens.push(identity);
        }

        if let Some(token) = self.bearer_token.clone()
            && !tokens.iter().any(|identity| identity.token == token)
        {
            let name = std::env::var("MCP_BEARER_TOKEN_NAME")
                .ok()
                .map(|value| value.trim().to_string())
                .filter(|value| !value.is_empty())
                .unwrap_or_else(|| "env-token".to_string());
            if let Some(identity) = Self::identity_for_token(&snapshot.mcp_tokens, name, token) {
                tokens.push(identity);
            }
        }

        tokens
    }

    fn identity_for_token_path(
        &self,
        token_config: &BTreeMap<String, McpTokenConfig>,
        path: &std::path::Path,
        token: String,
    ) -> Option<BearerTokenIdentity> {
        let name = path.file_stem()?.to_str()?.to_string();
        Self::identity_for_token(token_config, name, token)
    }

    fn identity_for_token(
        token_config: &BTreeMap<String, McpTokenConfig>,
        name: String,
        token: String,
    ) -> Option<BearerTokenIdentity> {
        let config = token_config.get(&name)?;
        Some(BearerTokenIdentity {
            token,
            auth: AuthContext::new(
                ContextName::new(config.context.clone()),
                format!("mcp_token:{name}"),
            ),
        })
    }

    async fn authorize(&self, headers: &HeaderMap) -> Result<AuthContext, McpError> {
        let expected_tokens = self.expected_bearer_tokens().await;
        if expected_tokens.is_empty() {
            return Err(McpError::Unauthorized);
        }

        let value = headers
            .get(AUTHORIZATION)
            .and_then(|value| value.to_str().ok())
            .ok_or(McpError::Unauthorized)?;
        let (scheme, provided_token) = value.split_once(' ').ok_or(McpError::Unauthorized)?;
        if !scheme.eq_ignore_ascii_case("bearer") {
            return Err(McpError::Unauthorized);
        }
        let Some(identity) = expected_tokens
            .into_iter()
            .find(|identity| identity.token == provided_token.trim())
        else {
            return Err(McpError::Unauthorized);
        };

        Ok(identity.auth)
    }
}

#[derive(Debug, Clone)]
struct BearerTokenIdentity {
    token: String,
    auth: AuthContext,
}

pub fn app_router(state: McpState) -> Router {
    Router::new()
        .route("/sse", get(sse_endpoint))
        .route("/mcp", post(mcp_endpoint))
        .with_state(state)
}

async fn sse_endpoint(
    State(state): State<McpState>,
    headers: HeaderMap,
) -> Result<Sse<impl tokio_stream::Stream<Item = Result<Event, Infallible>>>, McpError> {
    if let Err(error) = state.authorize(&headers).await {
        log_request_outcome("/sse", "sse/connect", None, StatusCode::UNAUTHORIZED, false);
        return Err(error);
    }

    let endpoint = tokio_stream::once(Ok(Event::default().event("endpoint").data("/mcp")));
    let heartbeat = IntervalStream::new(tokio::time::interval(Duration::from_secs(30)))
        .map(|_| Ok(Event::default().event("ping").data("{}")));
    let stream = endpoint.chain(heartbeat);

    log_request_outcome("/sse", "sse/connect", None, StatusCode::OK, true);
    Ok(Sse::new(stream).keep_alive(
        KeepAlive::new()
            .interval(Duration::from_secs(15))
            .text("keepalive"),
    ))
}

async fn mcp_endpoint(
    State(state): State<McpState>,
    headers: HeaderMap,
    Json(request): Json<Value>,
) -> Response {
    let auth = match state.authorize(&headers).await {
        Ok(auth) => auth,
        Err(error) => {
            return respond_with_log("/mcp", "unknown", None, error.into_response(), false);
        }
    };

    let rpc: JsonRpcRequest = match serde_json::from_value(request) {
        Ok(rpc) => rpc,
        Err(_) => {
            return respond_with_log(
                "/mcp",
                "unknown",
                None,
                json_error_with_metadata(
                    None,
                    -32600,
                    &ErrorMetadata::new(
                        ErrorCategory::Validation,
                        false,
                        "invalid request",
                        "Request body is not a valid JSON-RPC object",
                    ),
                ),
                false,
            );
        }
    };
    let id = rpc.id.clone();

    if rpc.jsonrpc != JSONRPC_VERSION {
        return respond_with_log(
            "/mcp",
            rpc.method.as_str(),
            None,
            json_error_with_metadata(
                id,
                -32600,
                &ErrorMetadata::new(
                    ErrorCategory::Validation,
                    false,
                    "invalid jsonrpc version",
                    format!(
                        "jsonrpc must be '{}', but the request used '{}'",
                        JSONRPC_VERSION, rpc.jsonrpc
                    ),
                ),
            ),
            false,
        );
    }

    match rpc.method.as_str() {
        "initialize" => {
            let Some(requested_version) = rpc
                .params
                .get("protocolVersion")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|value| !value.is_empty())
            else {
                return respond_with_log(
                    "/mcp",
                    "initialize",
                    None,
                    json_error_with_metadata(
                        id,
                        -32602,
                        &ErrorMetadata::new(
                            ErrorCategory::Validation,
                            false,
                            "missing protocolVersion",
                            "initialize requests must include a non-empty protocolVersion",
                        ),
                    ),
                    false,
                );
            };

            let negotiated_version = if SUPPORTED_PROTOCOL_VERSIONS.contains(&requested_version) {
                requested_version
            } else {
                return respond_with_log(
                    "/mcp",
                    "initialize",
                    None,
                    json_error_with_metadata(
                        id,
                        -32602,
                        &ErrorMetadata::new(
                            ErrorCategory::Validation,
                            false,
                            "Unsupported protocol version",
                            format!(
                                "Unsupported protocol version '{}'. Supported versions: {}",
                                requested_version,
                                SUPPORTED_PROTOCOL_VERSIONS.join(", ")
                            ),
                        )
                        .with_details(json!({
                            "supported": SUPPORTED_PROTOCOL_VERSIONS,
                            "requested": requested_version
                        })),
                    ),
                    false,
                );
            };

            respond_with_log(
                "/mcp",
                "initialize",
                None,
                {
                    let server = server_definition();
                    let mut payload = json!({
                        "protocolVersion": negotiated_version,
                        "capabilities": {
                            "tools": {
                                "listChanged": false
                            },
                            "resources": {
                                "subscribe": false,
                                "listChanged": false
                            }
                        },
                        "serverInfo": {
                            "name": server.name,
                            "version": SERVER_VERSION
                        }
                    });
                    if !server.instructions.trim().is_empty() {
                        payload["instructions"] = Value::String(server.instructions);
                    }
                    json_result(id, payload)
                },
                true,
            )
        }
        "ping" => respond_with_log("/mcp", "ping", None, json_result(id, json!({})), true),
        "notifications/initialized" => respond_with_log(
            "/mcp",
            "notifications/initialized",
            None,
            StatusCode::ACCEPTED.into_response(),
            true,
        ),
        "tools/list" => {
            let discovery = context_discovery_for_auth(&state, &auth).await;
            respond_with_log(
                "/mcp",
                "tools/list",
                None,
                json_result(
                    id,
                    json!({ "tools": tool_definitions_for_capabilities(discovery.capabilities) }),
                ),
                true,
            )
        }
        "resources/list" => respond_with_log(
            "/mcp",
            "resources/list",
            None,
            json_result(id, json!({ "resources": resource_definitions() })),
            true,
        ),
        "resources/templates/list" => respond_with_log(
            "/mcp",
            "resources/templates/list",
            None,
            json_result(
                id,
                json!({ "resourceTemplates": resource_template_definitions() }),
            ),
            true,
        ),
        "resources/read" => {
            let params: ResourceReadParams = match serde_json::from_value(rpc.params) {
                Ok(params) => params,
                Err(_) => {
                    return respond_with_log(
                        "/mcp",
                        "resources/read",
                        None,
                        json_error_with_metadata(
                            id,
                            -32602,
                            &ErrorMetadata::new(
                                ErrorCategory::Validation,
                                false,
                                "invalid resources/read params",
                                "resources/read requires a params object with a non-empty uri string",
                            ),
                        ),
                        false,
                    );
                }
            };
            handle_resource_read(&state, &auth, id, params.uri.trim()).await
        }
        "tools/call" => {
            let params: ToolCallParams = match serde_json::from_value(rpc.params) {
                Ok(params) => params,
                Err(_) => {
                    return respond_with_log(
                        "/mcp",
                        "tools/call",
                        None,
                        json_error_with_metadata(
                            id,
                            -32602,
                            &ErrorMetadata::new(
                                ErrorCategory::Validation,
                                false,
                                "invalid tools/call params",
                                "tools/call requires a params object with a tool name and optional arguments object",
                            ),
                        ),
                        false,
                    );
                }
            };
            let discovery = context_discovery_for_auth(&state, &auth).await;
            let available_tools = tool_definitions_for_capabilities(discovery.capabilities);
            if !tool_is_enabled(&params.name) {
                return respond_with_log(
                    "/mcp",
                    "tools/call",
                    Some(params.name.as_str()),
                    json_error_with_metadata(
                        id,
                        -32602,
                        &unknown_tool_metadata(&params.name, &available_tools),
                    ),
                    false,
                );
            }
            if let Some(required_capability) = required_capability_for_tool(&params.name)
                && !discovery.capabilities.allows(required_capability)
            {
                return respond_with_log(
                    "/mcp",
                    "tools/call",
                    Some(params.name.as_str()),
                    tool_error_response(
                        id,
                        unavailable_tool_metadata(
                            &auth,
                            &params.name,
                            required_capability,
                            &available_tools,
                        ),
                    ),
                    true,
                );
            }

            let response =
                match execute_tool_call(&state, &auth, &params.name, &params.arguments).await {
                    Ok(response) => response,
                    Err(error) => {
                        return respond_with_log(
                            "/mcp",
                            "tools/call",
                            Some(params.name.as_str()),
                            tool_error_response(id, error.to_error_metadata(Some(&params.name))),
                            true,
                        );
                    }
                };

            let (response, sanitization_report) =
                match sanitize_tool_response(&params.name, &params.arguments, response) {
                    Ok(result) => result,
                    Err(error) => {
                        return respond_with_log(
                            "/mcp",
                            "tools/call",
                            Some(params.name.as_str()),
                            tool_error_response(
                                id,
                                error
                                    .with_tool(&params.name)
                                    .to_error_metadata(Some(&params.name)),
                            ),
                            true,
                        );
                    }
                };

            respond_with_log(
                "/mcp",
                "tools/call",
                Some(params.name.as_str()),
                tool_success_response(id, response, sanitization_report),
                true,
            )
        }
        method if method.starts_with("notifications/") => respond_with_log(
            "/mcp",
            method,
            None,
            StatusCode::ACCEPTED.into_response(),
            true,
        ),
        _ => respond_with_log(
            "/mcp",
            rpc.method.as_str(),
            None,
            json_error_with_metadata(
                id,
                -32601,
                &ErrorMetadata::new(
                    ErrorCategory::Validation,
                    false,
                    "method not found",
                    format!(
                        "Method '{}' is not implemented by this MCP server",
                        rpc.method
                    ),
                ),
            ),
            false,
        ),
    }
}

async fn handle_resource_read(
    state: &McpState,
    auth: &AuthContext,
    id: Option<Value>,
    uri: &str,
) -> Response {
    if uri.is_empty() {
        return respond_with_log(
            "/mcp",
            "resources/read",
            None,
            json_error_with_metadata(
                id,
                -32602,
                &ErrorMetadata::new(
                    ErrorCategory::Validation,
                    false,
                    "uri is required",
                    "resources/read requires a non-empty uri string",
                ),
            ),
            false,
        );
    }

    let contents = match read_resource_contents(state, auth, uri).await {
        Ok(contents) => contents,
        Err(error) => {
            return respond_with_log(
                "/mcp",
                "resources/read",
                None,
                json_error_with_metadata(id, -32000, &error),
                false,
            );
        }
    };

    respond_with_log(
        "/mcp",
        "resources/read",
        None,
        json_result(id, json!({ "contents": contents })),
        true,
    )
}

async fn read_resource_contents(
    state: &McpState,
    auth: &AuthContext,
    uri: &str,
) -> Result<Vec<Value>, ErrorMetadata> {
    match uri {
        TOOLING_CATALOG_URI => {
            let discovery = context_discovery_for_auth(state, auth).await;
            let tools = tool_definitions_for_capabilities(discovery.capabilities);
            let catalog = json!({
                "server": server_definition(),
                "tools": tools,
                "resources": resource_definitions(),
                "resourceTemplates": resource_template_definitions(),
                "boundaries": {
                    "context": auth.context.as_str(),
                    "capabilities": discovery.capabilities,
                    "availableTools": tool_names_for_capabilities(discovery.capabilities),
                    "unavailableTools": unavailable_tool_entries(discovery.capabilities),
                    "policy": discovery.policy
                }
            });
            Ok(vec![json!({
                "uri": uri,
                "mimeType": "application/json",
                "text": serde_json::to_string_pretty(&catalog).unwrap_or_else(|_| "{}".to_string())
            })])
        }
        OPENAPI_SCHEMA_URI => {
            let schema = api_docs::openapi_spec();
            Ok(vec![json!({
                "uri": uri,
                "mimeType": "application/json",
                "text": serde_json::to_string_pretty(&schema).unwrap_or_else(|_| "{}".to_string())
            })])
        }
        _ if uri.starts_with(VAULT_FILE_RESOURCE_PREFIX) => {
            let encoded_file_id = &uri[VAULT_FILE_RESOURCE_PREFIX.len()..];
            let decoded_file_id = urlencoding::decode(encoded_file_id).map_err(|_| {
                ErrorMetadata::new(
                    ErrorCategory::Validation,
                    false,
                    "invalid vault file resource uri",
                    format!(
                        "vault file resource uri '{}' contains invalid percent-encoding in the file id",
                        uri
                    ),
                )
            })?;
            let file_id = decoded_file_id.trim();
            if file_id.is_empty() {
                return Err(ErrorMetadata::new(
                    ErrorCategory::Validation,
                    false,
                    "file id is required",
                    "vault file resource URIs must include a non-empty canonical file id after vault-bridge://files/",
                ));
            }

            let response = state
                .service
                .get_vault_file(auth, &NoteId::new(file_id))
                .await
                .map_err(|error| service_error_metadata(&error, Some("resources/read")))?;
            let response = serde_json::to_value(response).map_err(|error| {
                ErrorMetadata::new(
                    ErrorCategory::Business,
                    false,
                    "serialization failed",
                    format!("Failed to serialize vault file resource response: {error}"),
                )
            })?;

            Ok(vec![json!({
                "uri": uri,
                "mimeType": "application/json",
                "text": serde_json::to_string_pretty(&response).unwrap_or_else(|_| "{}".to_string())
            })])
        }
        _ => Err(ErrorMetadata::new(
            ErrorCategory::Business,
            false,
            "unknown resource",
            format!("resource '{}' is not exposed by this MCP server", uri),
        )
        .with_details(json!({
            "availableResources": resource_definitions(),
            "availableTemplates": resource_template_definitions()
        }))),
    }
}

#[derive(Debug, Clone)]
struct ContextDiscovery {
    policy: Option<AccessPolicy>,
    capabilities: ContextToolCapabilities,
}

#[derive(Debug, Clone, Copy, Default, Serialize, PartialEq, Eq)]
struct ContextToolCapabilities {
    read: bool,
    create: bool,
    edit: bool,
}

impl ContextToolCapabilities {
    fn allows(self, capability: ToolCapability) -> bool {
        match capability {
            ToolCapability::Read => self.read,
            ToolCapability::Create => self.create,
            ToolCapability::Edit => self.edit,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ToolCapability {
    Read,
    Create,
    Edit,
}

impl ToolCapability {
    fn as_str(self) -> &'static str {
        match self {
            Self::Read => "read",
            Self::Create => "create",
            Self::Edit => "edit",
        }
    }
}

async fn context_discovery_for_auth(state: &McpState, auth: &AuthContext) -> ContextDiscovery {
    let snapshot = state.auth_config.snapshot().await;
    let mut contexts = snapshot.contexts;
    if contexts.is_empty() || !contexts.contains_key(auth.context.as_str()) {
        contexts = state.service.store.authorization_config().await;
    }

    let policy = contexts.get(auth.context.as_str()).cloned();
    let capabilities = policy
        .as_ref()
        .map(capabilities_for_policy)
        .unwrap_or_default();

    ContextDiscovery {
        policy,
        capabilities,
    }
}

fn capabilities_for_policy(policy: &AccessPolicy) -> ContextToolCapabilities {
    ContextToolCapabilities {
        read: operation_has_discoverable_allow(&policy.read),
        create: operation_has_discoverable_allow(&policy.create),
        edit: operation_has_discoverable_allow(&policy.edit),
    }
}

fn operation_has_discoverable_allow(rules: &[AccessRule]) -> bool {
    if rules.iter().any(|rule| {
        rule.is_deny()
            && rule
                .matcher()
                .map(matcher_blocks_everything)
                .unwrap_or(false)
    }) {
        return false;
    }

    rules.iter().any(AccessRule::is_allow)
}

fn matcher_blocks_everything(matcher: &AccessMatcher) -> bool {
    matcher.default == Some(true)
}

fn tool_definitions_for_capabilities(capabilities: ContextToolCapabilities) -> Vec<ToolDefinition> {
    tool_definitions()
        .into_iter()
        .filter(|tool| {
            required_capability_for_tool(&tool.name)
                .map(|required| capabilities.allows(required))
                .unwrap_or(false)
        })
        .collect()
}

fn tool_names_for_capabilities(capabilities: ContextToolCapabilities) -> Vec<String> {
    tool_definitions_for_capabilities(capabilities)
        .into_iter()
        .map(|tool| tool.name)
        .collect()
}

fn unavailable_tool_entries(capabilities: ContextToolCapabilities) -> Vec<Value> {
    tool_definitions()
        .into_iter()
        .filter_map(|tool| {
            let required_capability = required_capability_for_tool(&tool.name)?;
            if capabilities.allows(required_capability) {
                return None;
            }
            Some(json!({
                "name": tool.name,
                "requiredCapability": required_capability.as_str(),
                "reason": format!(
                    "context has no {} allow rule in its effective authorization policy",
                    required_capability.as_str()
                )
            }))
        })
        .collect()
}

fn available_tool_names(tools: &[ToolDefinition]) -> Vec<String> {
    tools.iter().map(|tool| tool.name.clone()).collect()
}

fn required_capability_for_tool(tool_name: &str) -> Option<ToolCapability> {
    if READ_TOOL_NAMES.contains(&tool_name) {
        Some(ToolCapability::Read)
    } else if CREATE_TOOL_NAMES.contains(&tool_name) {
        Some(ToolCapability::Create)
    } else if EDIT_TOOL_NAMES.contains(&tool_name) {
        Some(ToolCapability::Edit)
    } else {
        None
    }
}

fn unknown_tool_metadata(tool_name: &str, available_tools: &[ToolDefinition]) -> ErrorMetadata {
    ErrorMetadata::new(
        ErrorCategory::Validation,
        false,
        "unknown tool",
        format!("Tool '{tool_name}' is not exposed by this MCP server for this token context"),
    )
    .with_tool(tool_name)
    .with_details(json!({
        "availableTools": available_tool_names(available_tools)
    }))
}

fn unavailable_tool_metadata(
    auth: &AuthContext,
    tool_name: &str,
    required_capability: ToolCapability,
    available_tools: &[ToolDefinition],
) -> ErrorMetadata {
    ErrorMetadata::new(
        ErrorCategory::Permission,
        false,
        "tool unavailable for this token context",
        format!(
            "Tool '{}' requires {} capability, but token context '{}' does not advertise that capability",
            tool_name,
            required_capability.as_str(),
            auth.context.as_str()
        ),
    )
    .with_tool(tool_name)
    .with_details(json!({
        "context": auth.context.as_str(),
        "requiredCapability": required_capability.as_str(),
        "availableTools": available_tool_names(available_tools)
    }))
}

async fn execute_tool_call(
    state: &McpState,
    auth: &AuthContext,
    tool_name: &str,
    arguments: &Value,
) -> Result<Value, McpError> {
    match tool_name {
        "get_vault_file" => {
            let id = required_string(arguments, "id")?;
            let file = state
                .service
                .get_vault_file(auth, &NoteId::new(id))
                .await
                .map_err(McpError::Service)?;
            to_tool_value(file)
        }
        "query_notes" => {
            let request = deserialize_arguments::<QueryNotesRequest>(tool_name, arguments)?;
            validate_optional_usize_range(
                tool_name,
                "limit",
                request.limit,
                0,
                MAX_NOTE_LIST_LIMIT,
            )?;
            to_tool_value(state.service.query_notes(auth, request).await)
        }
        "query_base" => {
            let request = deserialize_arguments::<QueryBaseRequest>(tool_name, arguments)?;
            validate_optional_usize_range(
                tool_name,
                "limit",
                request.limit,
                0,
                MAX_NOTE_LIST_LIMIT,
            )?;
            let response = state
                .service
                .query_base(auth, request)
                .await
                .map_err(McpError::Service)?;
            to_tool_value(response)
        }
        "get_neighbors" => {
            let id = required_string(arguments, "id")?;
            let depth = optional_usize(arguments, "depth")?;
            validate_optional_usize_range(tool_name, "depth", depth, 1, MAX_GRAPH_TRAVERSAL_DEPTH)?;
            let depth = depth.unwrap_or(1);
            let direction = optional_string(arguments, "direction")?
                .map(|direction| {
                    serde_json::from_value::<NeighborDirection>(json!(direction)).map_err(|_| {
                        McpError::InvalidArguments {
                            tool: tool_name.to_string(),
                            message: "direction must be outgoing, incoming, or both".to_string(),
                        }
                    })
                })
                .transpose()?
                .unwrap_or_default();
            let response = state
                .service
                .neighbors(auth, &NoteId::new(id), depth, direction)
                .await
                .map_err(McpError::Service)?;
            to_tool_value(response)
        }
        "list_tags" => {
            let filter = deserialize_arguments::<NoteTimeFilter>(tool_name, arguments)?;
            to_tool_value(state.service.list_tags(auth, filter).await)
        }
        "create_vault_file" => {
            let request = deserialize_arguments::<NewNoteRequest>(tool_name, arguments)?;
            let response = state
                .service
                .create_vault_file(auth, request)
                .await
                .map_err(McpError::Service)?;
            to_tool_value(response)
        }
        "edit_vault_file" => {
            let note_id = required_string(arguments, "id")?;
            let mut update_body = arguments.clone();
            if let Some(map) = update_body.as_object_mut() {
                map.remove("id");
            }
            let request = deserialize_arguments::<UpdateNoteRequest>(tool_name, &update_body)?;
            let response = state
                .service
                .edit_vault_file(auth, &NoteId::new(note_id), request)
                .await
                .map_err(McpError::Service)?;
            to_tool_value(response)
        }
        _ => Err(McpError::UnknownTool(tool_name.to_string())),
    }
}

fn deserialize_arguments<T>(tool_name: &str, arguments: &Value) -> Result<T, McpError>
where
    T: DeserializeOwned,
{
    let value = if arguments.is_null() {
        json!({})
    } else if arguments.is_object() {
        arguments.clone()
    } else {
        return Err(McpError::InvalidArguments {
            tool: tool_name.to_string(),
            message: "arguments must be an object".to_string(),
        });
    };
    serde_json::from_value(value).map_err(|error| McpError::InvalidArguments {
        tool: tool_name.to_string(),
        message: error.to_string(),
    })
}

fn to_tool_value<T>(value: T) -> Result<Value, McpError>
where
    T: Serialize,
{
    serde_json::to_value(value).map_err(McpError::Serialization)
}

fn respond_with_log(
    endpoint: &'static str,
    rpc_method: &str,
    tool_name: Option<&str>,
    response: Response,
    success: bool,
) -> Response {
    log_request_outcome(endpoint, rpc_method, tool_name, response.status(), success);
    response
}

fn log_request_outcome(
    endpoint: &'static str,
    rpc_method: &str,
    tool_name: Option<&str>,
    status: StatusCode,
    success: bool,
) {
    match (success, tool_name) {
        (true, Some(tool_name)) => info!(
            endpoint,
            rpc_method,
            tool_name,
            status = status.as_u16(),
            success,
            "mcp request completed"
        ),
        (true, None) => info!(
            endpoint,
            rpc_method,
            status = status.as_u16(),
            success,
            "mcp request completed"
        ),
        (false, Some(tool_name)) => warn!(
            endpoint,
            rpc_method,
            tool_name,
            status = status.as_u16(),
            success,
            "mcp request completed"
        ),
        (false, None) => warn!(
            endpoint,
            rpc_method,
            status = status.as_u16(),
            success,
            "mcp request completed"
        ),
    }
}

fn json_result(id: Option<Value>, result: Value) -> Response {
    Json(json!({
        "jsonrpc": JSONRPC_VERSION,
        "id": id.unwrap_or(Value::Null),
        "result": result
    }))
    .into_response()
}

fn json_error_with_data(id: Option<Value>, code: i64, message: &str, data: Value) -> Response {
    Json(json!({
        "jsonrpc": JSONRPC_VERSION,
        "id": id.unwrap_or(Value::Null),
        "error": {
            "code": code,
            "message": message,
            "data": data
        }
    }))
    .into_response()
}

fn json_error_with_metadata(id: Option<Value>, code: i64, metadata: &ErrorMetadata) -> Response {
    let data = serde_json::to_value(metadata).unwrap_or_else(|_| Value::Null);
    json_error_with_data(id, code, metadata.message(), data)
}

fn tool_success_response(
    id: Option<Value>,
    response: Value,
    sanitization_report: Option<SanitizationReport>,
) -> Response {
    let pretty = serde_json::to_string_pretty(&response).unwrap_or_else(|_| "{}".to_string());

    let mut payload = json!({
        "content": [
            {
                "type": "text",
                "text": pretty
            }
        ],
        "structuredContent": response,
        "isError": false
    });
    if let Some(report) = sanitization_report {
        payload["meta"] = json!({
            "sanitization_report": report,
        });
    }

    json_result(id, payload)
}

fn tool_error_response(id: Option<Value>, metadata: ErrorMetadata) -> Response {
    let structured = serde_json::to_value(&metadata).unwrap_or_else(|_| Value::Null);
    json_result(
        id,
        json!({
            "content": [
                {
                    "type": "text",
                    "text": metadata.summary()
                }
            ],
            "structuredContent": structured,
            "isError": true
        }),
    )
}

#[derive(Debug, Clone, Deserialize)]
struct JsonRpcRequest {
    jsonrpc: String,
    #[serde(default)]
    id: Option<Value>,
    method: String,
    #[serde(default)]
    params: Value,
}

#[derive(Debug, Clone, Deserialize)]
struct ToolCallParams {
    name: String,
    #[serde(default)]
    arguments: Value,
}

#[derive(Debug, Clone, Deserialize)]
struct ResourceReadParams {
    uri: String,
}

#[derive(Debug, Clone, Default, Serialize, PartialEq, Eq)]
struct SanitizationReport {
    embedded_images_stripped: usize,
    text_values_sanitized: usize,
    approx_bytes_removed: usize,
    approx_chars_removed: usize,
}

impl SanitizationReport {
    fn has_changes(&self) -> bool {
        self.embedded_images_stripped > 0
    }
}

#[derive(Debug, Clone)]
struct TextSanitizationResult {
    text: String,
    embedded_images_stripped: usize,
    approx_bytes_removed: usize,
    approx_chars_removed: usize,
}

fn sanitize_tool_response(
    tool_name: &str,
    arguments: &Value,
    mut response: Value,
) -> Result<(Value, Option<SanitizationReport>), McpError> {
    if raw_response_requested(tool_name, arguments)? {
        return Ok((response, None));
    }

    let mut report = SanitizationReport::default();
    sanitize_json_value(&mut response, &mut report);

    if report.has_changes() {
        Ok((response, Some(report)))
    } else {
        Ok((response, None))
    }
}

fn raw_response_requested(tool_name: &str, arguments: &Value) -> Result<bool, McpError> {
    let Some(map) = arguments.as_object() else {
        return Ok(false);
    };

    match map.get("raw") {
        None | Some(Value::Null) => Ok(false),
        Some(Value::Bool(raw)) => Ok(*raw),
        _ => Err(McpError::InvalidArguments {
            tool: tool_name.to_string(),
            message: "raw must be a boolean".to_string(),
        }),
    }
}

fn sanitize_json_value(value: &mut Value, report: &mut SanitizationReport) {
    match value {
        Value::String(text) => {
            if let Some(sanitized) = sanitize_embedded_image_data_uris(text) {
                *text = sanitized.text;
                report.embedded_images_stripped += sanitized.embedded_images_stripped;
                report.text_values_sanitized += 1;
                report.approx_bytes_removed += sanitized.approx_bytes_removed;
                report.approx_chars_removed += sanitized.approx_chars_removed;
            }
        }
        Value::Array(items) => {
            for item in items {
                sanitize_json_value(item, report);
            }
        }
        Value::Object(map) => {
            for item in map.values_mut() {
                sanitize_json_value(item, report);
            }
        }
        _ => {}
    }
}

fn sanitize_embedded_image_data_uris(input: &str) -> Option<TextSanitizationResult> {
    let mut text = String::with_capacity(input.len());
    let mut cursor = 0usize;
    let mut embedded_images_stripped = 0usize;
    let mut approx_bytes_removed = 0usize;
    let mut approx_chars_removed = 0usize;

    for captures in EMBEDDED_IMAGE_DATA_URI_RE.captures_iter(input) {
        let Some(full_match) = captures.get(0) else {
            continue;
        };

        text.push_str(&input[cursor..full_match.start()]);

        let mime = captures
            .name("mime")
            .map(|capture| capture.as_str().to_ascii_lowercase())
            .unwrap_or_else(|| "unknown".to_string());
        let payload = captures
            .name("data")
            .map(|capture| capture.as_str())
            .unwrap_or_default();

        let approx_bytes = approximate_base64_decoded_bytes(payload);
        text.push_str(&format!(
            "<embedded image stripped: image/{mime}, {}>",
            format_approx_size(approx_bytes)
        ));

        embedded_images_stripped += 1;
        approx_bytes_removed += approx_bytes;
        approx_chars_removed += full_match.as_str().len();
        cursor = full_match.end();
    }

    if embedded_images_stripped == 0 {
        return None;
    }

    text.push_str(&input[cursor..]);

    Some(TextSanitizationResult {
        text,
        embedded_images_stripped,
        approx_bytes_removed,
        approx_chars_removed,
    })
}

fn approximate_base64_decoded_bytes(payload: &str) -> usize {
    if payload.is_empty() {
        return 0;
    }

    let length = payload.len();
    let padding = payload
        .as_bytes()
        .iter()
        .rev()
        .take_while(|&&byte| byte == b'=')
        .count()
        .min(2);

    let full_quads = length / 4;
    let remainder = length % 4;

    let mut decoded = full_quads * 3;
    if remainder > 0 {
        decoded += remainder.saturating_sub(1);
    }

    decoded.saturating_sub(padding)
}

fn format_approx_size(bytes: usize) -> String {
    const KB: f64 = 1024.0;
    const MB: f64 = KB * 1024.0;

    if bytes >= MB as usize {
        format!("~{:.1}MB", bytes as f64 / MB)
    } else if bytes >= KB as usize {
        format!("~{}KB", (bytes as f64 / KB).round() as usize)
    } else {
        format!("~{}B", bytes)
    }
}

fn required_string(arguments: &Value, key: &str) -> Result<String, McpError> {
    optional_string(arguments, key)?.ok_or_else(|| McpError::InvalidArguments {
        tool: "unknown".to_string(),
        message: format!("{key} is required"),
    })
}

fn optional_string(arguments: &Value, key: &str) -> Result<Option<String>, McpError> {
    let Some(map) = arguments.as_object() else {
        return if arguments.is_null() {
            Ok(None)
        } else {
            Err(McpError::InvalidArguments {
                tool: "unknown".to_string(),
                message: "arguments must be an object".to_string(),
            })
        };
    };

    match map.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(value)) => Ok(Some(value.clone())),
        _ => Err(McpError::InvalidArguments {
            tool: "unknown".to_string(),
            message: format!("{key} must be a string"),
        }),
    }
}

fn optional_usize(arguments: &Value, key: &str) -> Result<Option<usize>, McpError> {
    let Some(map) = arguments.as_object() else {
        return if arguments.is_null() {
            Ok(None)
        } else {
            Err(McpError::InvalidArguments {
                tool: "unknown".to_string(),
                message: "arguments must be an object".to_string(),
            })
        };
    };

    match map.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Number(number)) => {
            let value = number.as_u64().ok_or_else(|| McpError::InvalidArguments {
                tool: "unknown".to_string(),
                message: format!("{key} must be a positive integer"),
            })?;
            usize::try_from(value)
                .map(Some)
                .map_err(|_| McpError::InvalidArguments {
                    tool: "unknown".to_string(),
                    message: format!("{key} is too large"),
                })
        }
        _ => Err(McpError::InvalidArguments {
            tool: "unknown".to_string(),
            message: format!("{key} must be a number"),
        }),
    }
}

fn validate_optional_usize_range(
    tool: &str,
    key: &str,
    value: Option<usize>,
    min: usize,
    max: usize,
) -> Result<(), McpError> {
    if let Some(value) = value
        && !(min..=max).contains(&value)
    {
        return Err(McpError::InvalidArguments {
            tool: tool.to_string(),
            message: format!("{key} must be between {min} and {max}"),
        });
    }
    Ok(())
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
pub struct McpServerDefinition {
    pub id: String,
    pub name: String,
    #[serde(default)]
    pub instructions: String,
}

#[derive(Debug, Clone, Deserialize, PartialEq)]
struct McpDefinitionFile {
    server: McpServerDefinition,
    #[serde(default)]
    resources: Vec<ResourceDefinition>,
    #[serde(default, rename = "resourceTemplates")]
    resource_templates: Vec<ResourceTemplateDefinition>,
    tools: Vec<ToolDefinition>,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
pub struct ToolDefinition {
    #[serde(skip_serializing)]
    pub id: String,
    pub name: String,
    pub description: String,
    #[serde(rename = "inputSchema")]
    pub input_schema: Value,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
pub struct ResourceDefinition {
    #[serde(skip_serializing)]
    pub id: String,
    pub uri: String,
    pub name: String,
    pub description: String,
    #[serde(rename = "mimeType")]
    pub mime_type: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
pub struct ResourceTemplateDefinition {
    #[serde(skip_serializing)]
    pub id: String,
    #[serde(rename = "uriTemplate")]
    pub uri_template: String,
    pub name: String,
    pub description: String,
    #[serde(rename = "mimeType")]
    pub mime_type: String,
}

static MCP_DEFINITION_FILE: Lazy<McpDefinitionFile> = Lazy::new(load_mcp_definition_file);

pub fn server_definition() -> McpServerDefinition {
    MCP_DEFINITION_FILE.server.clone()
}

pub fn resource_definitions() -> Vec<ResourceDefinition> {
    MCP_DEFINITION_FILE.resources.clone()
}

pub fn resource_template_definitions() -> Vec<ResourceTemplateDefinition> {
    MCP_DEFINITION_FILE.resource_templates.clone()
}

pub fn tool_definitions() -> Vec<ToolDefinition> {
    MCP_DEFINITION_FILE.tools.clone()
}

fn load_mcp_definition_file() -> McpDefinitionFile {
    let file: McpDefinitionFile = serde_yaml::from_str(TOOL_DEFINITIONS_YAML)
        .expect("embedded MCP tool definitions YAML should parse");
    assert!(
        !file.server.id.trim().is_empty(),
        "missing MCP server id in config/mcp_tools.yaml"
    );
    assert!(
        !file.server.name.trim().is_empty(),
        "missing MCP server name in config/mcp_tools.yaml"
    );
    let mut seen = std::collections::HashSet::new();
    let mut seen_resource_ids = std::collections::HashSet::new();
    let mut seen_resource_uris = std::collections::HashSet::new();
    let mut seen_template_ids = std::collections::HashSet::new();
    let mut seen_template_uris = std::collections::HashSet::new();
    for tool in &file.tools {
        assert!(
            !tool.id.trim().is_empty(),
            "missing MCP tool id in config/mcp_tools.yaml: {}",
            tool.name
        );
        assert!(
            supported_tool_name(&tool.name),
            "unsupported MCP tool in config/mcp_tools.yaml: {}",
            tool.name
        );
        assert!(
            seen.insert(tool.id.clone()),
            "duplicate MCP tool id in config/mcp_tools.yaml: {}",
            tool.id
        );
        assert!(
            file.tools
                .iter()
                .filter(|other| other.name == tool.name)
                .count()
                == 1,
            "duplicate MCP tool in config/mcp_tools.yaml: {}",
            tool.name
        );
    }
    for resource in &file.resources {
        assert!(
            !resource.id.trim().is_empty(),
            "missing MCP resource id in config/mcp_tools.yaml: {}",
            resource.name
        );
        assert!(
            !resource.uri.trim().is_empty(),
            "missing MCP resource uri in config/mcp_tools.yaml: {}",
            resource.name
        );
        assert!(
            !resource.name.trim().is_empty(),
            "missing MCP resource name in config/mcp_tools.yaml: {}",
            resource.id
        );
        assert!(
            seen_resource_ids.insert(resource.id.clone()),
            "duplicate MCP resource id in config/mcp_tools.yaml: {}",
            resource.id
        );
        assert!(
            seen_resource_uris.insert(resource.uri.clone()),
            "duplicate MCP resource uri in config/mcp_tools.yaml: {}",
            resource.uri
        );
    }
    for template in &file.resource_templates {
        assert!(
            !template.id.trim().is_empty(),
            "missing MCP resource template id in config/mcp_tools.yaml: {}",
            template.name
        );
        assert!(
            !template.uri_template.trim().is_empty(),
            "missing MCP resource template uri in config/mcp_tools.yaml: {}",
            template.name
        );
        assert!(
            !template.name.trim().is_empty(),
            "missing MCP resource template name in config/mcp_tools.yaml: {}",
            template.id
        );
        assert!(
            seen_template_ids.insert(template.id.clone()),
            "duplicate MCP resource template id in config/mcp_tools.yaml: {}",
            template.id
        );
        assert!(
            seen_template_uris.insert(template.uri_template.clone()),
            "duplicate MCP resource template uri in config/mcp_tools.yaml: {}",
            template.uri_template
        );
    }
    file
}

fn supported_tool_name(tool_name: &str) -> bool {
    matches!(
        tool_name,
        "get_vault_file"
            | "query_notes"
            | "query_base"
            | "get_neighbors"
            | "list_tags"
            | "create_vault_file"
            | "edit_vault_file"
    )
}

fn tool_is_enabled(tool_name: &str) -> bool {
    MCP_DEFINITION_FILE
        .tools
        .iter()
        .any(|tool| tool.name == tool_name)
}

#[derive(Debug, Error)]
pub enum McpError {
    #[error("unauthorized")]
    Unauthorized,
    #[error("missing required environment variable: {0}")]
    MissingEnvironment(String),
    #[error("invalid environment variable: {0}")]
    InvalidEnvironment(String),
    #[error("invalid request: {0}")]
    InvalidRequest(serde_json::Error),
    #[error("invalid arguments: {tool}: {message}")]
    InvalidArguments { tool: String, message: String },
    #[error("unknown tool: {0}")]
    UnknownTool(String),
    #[error("service error: {0}")]
    Service(ServiceError),
    #[error("serialization error: {0}")]
    Serialization(serde_json::Error),
    #[error("failed to build HTTP client: {0}")]
    HttpClientBuild(reqwest::Error),
    #[error("api returned status {status}: {body}")]
    Api { status: u16, body: String },
    #[error("http error: {0}")]
    Http(#[from] reqwest::Error),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}

impl McpError {
    fn with_tool(self, tool_name: &str) -> Self {
        match self {
            Self::InvalidArguments { tool, message } if tool == "unknown" => {
                Self::InvalidArguments {
                    tool: tool_name.to_string(),
                    message,
                }
            }
            other => other,
        }
    }

    fn to_error_metadata(&self, tool_name: Option<&str>) -> ErrorMetadata {
        let mut metadata = match self {
            Self::Unauthorized => ErrorMetadata::new(
                ErrorCategory::Permission,
                false,
                "unauthorized",
                "Bearer token is missing or invalid for this MCP endpoint",
            ),
            Self::MissingEnvironment(name) => ErrorMetadata::new(
                ErrorCategory::Business,
                false,
                "server misconfigured",
                format!("Required server environment variable '{}' is missing", name),
            ),
            Self::InvalidEnvironment(name) => ErrorMetadata::new(
                ErrorCategory::Business,
                false,
                "server misconfigured",
                format!("Server environment variable '{}' is invalid", name),
            ),
            Self::InvalidRequest(error) => ErrorMetadata::new(
                ErrorCategory::Validation,
                false,
                "invalid request",
                format!("Request payload is not valid JSON-RPC: {}", error),
            ),
            Self::InvalidArguments { tool, message } => {
                let resolved_tool = if tool == "unknown" {
                    tool_name.unwrap_or("tool")
                } else {
                    tool.as_str()
                };
                ErrorMetadata::new(
                    ErrorCategory::Validation,
                    false,
                    "invalid arguments",
                    format!("Invalid arguments for {}: {}", resolved_tool, message),
                )
            }
            Self::UnknownTool(name) => ErrorMetadata::new(
                ErrorCategory::Validation,
                false,
                "unknown tool",
                format!("Tool '{}' is not exposed by this MCP server", name),
            ),
            Self::Service(error) => service_error_metadata(error, tool_name),
            Self::Serialization(error) => ErrorMetadata::new(
                ErrorCategory::Business,
                false,
                "serialization failed",
                format!("The MCP server could not serialize the tool response: {error}"),
            ),
            Self::HttpClientBuild(error) => ErrorMetadata::new(
                ErrorCategory::Business,
                false,
                "failed to build HTTP client",
                format!(
                    "The MCP server could not construct its upstream HTTP client: {}",
                    error
                ),
            ),
            Self::Api { status, body } => {
                let (error_category, is_retryable, message) = match *status {
                    400 => (
                        ErrorCategory::Validation,
                        false,
                        "upstream validation failed",
                    ),
                    401 | 403 => (
                        ErrorCategory::Permission,
                        false,
                        "upstream permission denied",
                    ),
                    404 => (
                        ErrorCategory::Business,
                        false,
                        "upstream resource not found",
                    ),
                    409 => (ErrorCategory::Business, false, "upstream conflict"),
                    429 => (ErrorCategory::Transient, true, "upstream rate limited"),
                    500..=599 => (
                        ErrorCategory::Transient,
                        true,
                        "upstream service unavailable",
                    ),
                    _ => (ErrorCategory::Business, false, "upstream request failed"),
                };
                let body_value = parse_api_error_body(body);
                let body_summary = body_value
                    .get("error")
                    .and_then(Value::as_str)
                    .map(str::trim)
                    .filter(|value| !value.is_empty())
                    .unwrap_or(body);
                ErrorMetadata::new(
                    error_category,
                    is_retryable,
                    message,
                    format!(
                        "Vault Bridge API returned status {}: {}",
                        status, body_summary
                    ),
                )
                .with_http_status(*status)
                .with_details(body_value)
            }
            Self::Http(error) => ErrorMetadata::new(
                ErrorCategory::Transient,
                true,
                "upstream request failed",
                format!(
                    "The MCP server could not reach the Vault Bridge API: {}",
                    error
                ),
            ),
            Self::Io(error) => ErrorMetadata::new(
                ErrorCategory::Business,
                false,
                "io error",
                format!("The MCP server hit an I/O error: {}", error),
            ),
        };

        if let Some(tool_name) = tool_name {
            metadata = metadata.with_tool(tool_name);
        } else if let Self::InvalidArguments { tool, .. } = self
            && tool != "unknown"
        {
            metadata = metadata.with_tool(tool);
        }

        metadata
    }
}

fn parse_api_error_body(body: &str) -> Value {
    serde_json::from_str(body).unwrap_or_else(|_| Value::String(body.to_string()))
}

impl IntoResponse for McpError {
    fn into_response(self) -> Response {
        let metadata = self.to_error_metadata(None);
        let status = match self {
            McpError::Unauthorized => StatusCode::UNAUTHORIZED,
            McpError::InvalidRequest(_)
            | McpError::InvalidArguments { .. }
            | McpError::UnknownTool(_) => StatusCode::BAD_REQUEST,
            McpError::Service(ServiceError::NotFound) => StatusCode::NOT_FOUND,
            McpError::Service(ServiceError::BadRequest(_)) => StatusCode::BAD_REQUEST,
            McpError::Service(ServiceError::Write(WriteError::PolicyDenied { .. })) => {
                StatusCode::FORBIDDEN
            }
            McpError::Service(ServiceError::Write(WriteError::AlreadyExists { .. })) => {
                StatusCode::CONFLICT
            }
            McpError::Service(ServiceError::Write(_)) => StatusCode::BAD_REQUEST,
            McpError::Service(ServiceError::CouchDbWrite(_) | ServiceError::CouchDbUpdate(_))
            | McpError::Serialization(_) => StatusCode::INTERNAL_SERVER_ERROR,
            McpError::MissingEnvironment(_)
            | McpError::InvalidEnvironment(_)
            | McpError::HttpClientBuild(_)
            | McpError::Api { .. }
            | McpError::Http(_)
            | McpError::Io(_) => StatusCode::INTERNAL_SERVER_ERROR,
        };
        (status, Json(json!({ "error": metadata }))).into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::{
        MAX_GRAPH_TRAVERSAL_DEPTH, MAX_NOTE_LIST_LIMIT, McpError, log_request_outcome,
        sanitize_tool_response, server_definition, tool_definitions,
    };
    use axum::http::StatusCode;
    use serde_json::json;
    use std::io::{self, Write};
    use std::sync::{Arc, Mutex};
    use tracing_subscriber::fmt::MakeWriter;

    #[derive(Clone, Default)]
    struct SharedLogBuffer {
        inner: Arc<Mutex<Vec<u8>>>,
    }

    impl SharedLogBuffer {
        fn output(&self) -> String {
            String::from_utf8(self.inner.lock().expect("log buffer").clone()).expect("utf-8 logs")
        }
    }

    struct SharedLogWriter {
        inner: Arc<Mutex<Vec<u8>>>,
    }

    impl Write for SharedLogWriter {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.inner
                .lock()
                .expect("log writer")
                .extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    impl<'a> MakeWriter<'a> for SharedLogBuffer {
        type Writer = SharedLogWriter;

        fn make_writer(&'a self) -> Self::Writer {
            SharedLogWriter {
                inner: Arc::clone(&self.inner),
            }
        }
    }

    fn tool_named(name: &str) -> super::ToolDefinition {
        tool_definitions()
            .into_iter()
            .find(|tool| tool.name == name)
            .unwrap_or_else(|| panic!("missing tool definition: {name}"))
    }

    #[test]
    fn tool_definitions_load_from_yaml_source_of_truth() {
        let tools = tool_definitions();
        assert!(!tools.is_empty());
        assert!(tools.iter().all(|tool| !tool.id.is_empty()));
        assert!(
            tools
                .iter()
                .all(|tool| tool.input_schema["type"] == "object")
        );
    }

    #[test]
    fn tool_descriptions_include_concise_examples() {
        for tool in tool_definitions() {
            assert!(
                tool.description.contains("Example:"),
                "{} should include a concise example",
                tool.name
            );
            assert!(
                tool.description.len() <= 700,
                "{} description is too long for tool discovery",
                tool.name
            );
        }
    }

    #[test]
    fn tool_descriptions_call_out_key_edge_cases() {
        let query_notes = tool_named("query_notes");
        assert!(query_notes.description.contains("semantic"));
        assert!(query_notes.description.contains("fulltext"));
        assert!(query_notes.description.contains("hybrid"));
        assert!(query_notes.description.contains("case-sensitive tag"));
        assert!(query_notes.description.contains("Empty `notes`"));
        assert!(query_notes.description.contains("caps at 500"));

        let query_base = tool_named("query_base");
        assert!(query_base.description.contains("Base-compatible"));
        assert!(query_base.description.contains("columns"));
        assert!(query_base.description.contains("rows"));
        assert!(query_base.description.contains("caps at 500"));

        let get_neighbors = tool_named("get_neighbors");
        assert!(get_neighbors.description.contains("1-5"));
        assert!(get_neighbors.description.contains("404"));
        assert!(get_neighbors.description.contains("filtered/private"));

        let list_tags = tool_named("list_tags");
        assert!(list_tags.description.contains("empty `tags`"));
        assert!(list_tags.description.contains("non-visible"));

        let create_vault_file = tool_named("create_vault_file");
        assert!(create_vault_file.description.contains("indexed_as_note"));
        assert!(create_vault_file.description.contains("file_type"));

        let get_vault_file = tool_named("get_vault_file");
        assert!(get_vault_file.description.contains("raw"));
        assert!(get_vault_file.description.contains("404"));

        let edit_vault_file = tool_named("edit_vault_file");
        assert!(edit_vault_file.description.contains("mutually exclusive"));
        assert!(edit_vault_file.description.contains("403"));
        assert!(edit_vault_file.description.contains("YAML"));
    }

    #[test]
    fn tool_schema_caps_match_runtime_clamps() {
        let query_notes = tool_named("query_notes");
        assert_eq!(
            query_notes.input_schema["properties"]["limit"]["maximum"],
            json!(MAX_NOTE_LIST_LIMIT)
        );

        let query_base = tool_named("query_base");
        assert_eq!(
            query_base.input_schema["properties"]["limit"]["maximum"],
            json!(MAX_NOTE_LIST_LIMIT)
        );

        let get_neighbors = tool_named("get_neighbors");
        assert_eq!(
            get_neighbors.input_schema["properties"]["depth"]["maximum"],
            json!(MAX_GRAPH_TRAVERSAL_DEPTH)
        );
    }

    #[test]
    fn server_definition_loads_instructions_from_yaml_source_of_truth() {
        let server = server_definition();
        assert!(!server.id.is_empty());
        assert!(!server.name.is_empty());
        assert!(!server.instructions.trim().is_empty());
    }

    #[test]
    fn assemble_context_is_not_exposed_as_mcp_tool() {
        assert!(
            tool_definitions()
                .into_iter()
                .all(|tool| tool.name != "assemble_context")
        );
    }

    #[test]
    fn tool_limit_schemas_match_runtime_caps() {
        let tools = tool_definitions();
        let find_tool = |name: &str| {
            tools
                .iter()
                .find(|tool| tool.name == name)
                .unwrap_or_else(|| panic!("missing {name} tool definition"))
        };

        let query_notes = find_tool("query_notes");
        assert_eq!(
            query_notes.input_schema["properties"]["limit"]["maximum"],
            json!(MAX_NOTE_LIST_LIMIT)
        );
        assert_eq!(
            query_notes.input_schema["properties"]["limit"]["minimum"],
            json!(0)
        );
        assert!(query_notes.description.contains("500"));

        let query_base = find_tool("query_base");
        assert_eq!(
            query_base.input_schema["properties"]["limit"]["maximum"],
            json!(MAX_NOTE_LIST_LIMIT)
        );
        assert_eq!(
            query_base.input_schema["properties"]["limit"]["minimum"],
            json!(0)
        );
        assert!(query_base.description.contains("500"));

        let get_neighbors = find_tool("get_neighbors");
        assert_eq!(
            get_neighbors.input_schema["properties"]["depth"]["maximum"],
            json!(MAX_GRAPH_TRAVERSAL_DEPTH)
        );
        assert_eq!(
            get_neighbors.input_schema["properties"]["depth"]["minimum"],
            json!(1)
        );
        assert!(get_neighbors.description.contains("depth 5"));
    }

    #[test]
    fn query_notes_schema_exposes_structured_filters() {
        let tool = tool_named("query_notes");

        assert_eq!(
            tool.input_schema["properties"]["sort_by"]["enum"],
            json!(["relevance", "updated_at", "created_at", "title"])
        );
        assert_eq!(
            tool.input_schema["properties"]["sort_order"]["enum"],
            json!(["asc", "desc"])
        );
        assert_eq!(
            tool.input_schema["properties"]["search_mode"]["default"],
            "hybrid"
        );
        assert_eq!(
            tool.input_schema["properties"]["title_exact"]["type"],
            "string"
        );
        assert!(
            tool.description
                .contains("Use result `id` with `get_vault_file`")
        );
        assert!(tool.description.contains("heading_title"));
    }

    #[test]
    fn query_base_schema_exposes_base_query_projection() {
        let tool = tool_named("query_base");

        assert_eq!(
            tool.input_schema["properties"]["base_query"]["type"],
            "string"
        );
        assert_eq!(
            tool.input_schema["properties"]["limit"]["maximum"],
            json!(MAX_NOTE_LIST_LIMIT)
        );
        assert!(tool.description.contains("views[].order"));
        assert!(tool.description.contains("formulas"));
    }

    #[test]
    fn get_vault_file_schema_exposes_raw_toggle() {
        let tool = tool_named("get_vault_file");

        assert_eq!(tool.input_schema["properties"]["raw"]["type"], "boolean");
        assert_eq!(tool.input_schema["properties"]["raw"]["default"], false);
    }

    #[test]
    fn sanitizer_replaces_large_embedded_data_uris() {
        let payload = "A".repeat(600);
        let content = format!("before ![img](data:image/png;base64,{payload}) after");

        let (sanitized, report) = sanitize_tool_response(
            "get_vault_file",
            &json!({}),
            json!({ "content": content.clone() }),
        )
        .expect("sanitized response");

        let sanitized_text = sanitized["content"].as_str().expect("content string");
        assert!(!sanitized_text.contains(&payload));
        assert!(sanitized_text.contains("<embedded image stripped: image/png"));

        let report = report.expect("sanitization report");
        assert_eq!(report.embedded_images_stripped, 1);
        assert_eq!(report.text_values_sanitized, 1);
        assert!(report.approx_bytes_removed > 0);
        assert!(report.approx_chars_removed >= payload.len());
    }

    #[test]
    fn sanitizer_preserves_raw_mode() {
        let payload = "A".repeat(600);
        let content = format!("![img](data:image/png;base64,{payload})");

        let (sanitized, report) = sanitize_tool_response(
            "get_vault_file",
            &json!({"raw": true}),
            json!({ "content": content.clone() }),
        )
        .expect("raw response");

        assert_eq!(sanitized["content"].as_str(), Some(content.as_str()));
        assert!(report.is_none());
    }

    #[test]
    fn sanitizer_rejects_non_boolean_raw_flag() {
        let error = sanitize_tool_response(
            "get_vault_file",
            &json!({"raw": "true"}),
            json!({ "content": "x" }),
        )
        .expect_err("raw type validation");

        assert!(matches!(
            error,
            McpError::InvalidArguments { ref tool, ref message }
                if tool == "get_vault_file" && message.contains("raw must be a boolean")
        ));
    }

    #[test]
    fn request_logs_include_tool_name_for_tool_calls() {
        let logs = SharedLogBuffer::default();
        let subscriber = tracing_subscriber::fmt()
            .with_ansi(false)
            .without_time()
            .with_target(false)
            .with_level(false)
            .with_writer(logs.clone())
            .finish();

        tracing::subscriber::with_default(subscriber, || {
            log_request_outcome(
                "/mcp",
                "tools/call",
                Some("get_vault_file"),
                StatusCode::NO_CONTENT,
                true,
            );
        });

        let output = logs.output();
        assert!(output.contains("endpoint=\"/mcp\""));
        assert!(output.contains("rpc_method=\"tools/call\""));
        assert!(output.contains("tool_name=\"get_vault_file\""));
        assert!(output.contains("status=204"));
    }
}

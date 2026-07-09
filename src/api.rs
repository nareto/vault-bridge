use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::path::PathBuf;

use axum::extract::rejection::{JsonRejection, QueryRejection};
use axum::extract::{Path, Query, State};
use axum::http::header::CONTENT_TYPE;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::json;
use thiserror::Error;
use tracing::info;

use crate::api_docs;
use crate::authorization::{AuthContext, ContextName};
use crate::base_query::QueryBaseRequest;
use crate::config::ApiTokenConfig;
use crate::context::AssembleContextRequest;
use crate::error_metadata::{
    ErrorCategory, ErrorMetadata, service_error_metadata, service_error_status,
};
use crate::model::NoteId;
use crate::new_note::{NewNoteRequest, UpdateNoteRequest};
use crate::runtime_config::{AuthConfigSnapshot, RuntimeAuthConfig, RuntimeConfigState};
use crate::search::SearchMode;
use crate::service::{ServiceError, VaultBridgeService};
use crate::store::{
    MAX_GRAPH_TRAVERSAL_DEPTH, MAX_NOTE_LIST_LIMIT, NeighborDirection, NoteTimeFilter,
    QueryNotesRequest, StatusResponse,
};

#[derive(Clone, Debug)]
pub struct AppState {
    pub service: VaultBridgeService,
    pub api_tokens: ApiTokenState,
    pub mcp: Option<crate::mcp::McpState>,
    pub runtime_config: RuntimeConfigState,
}

impl AppState {
    pub async fn with_seed_data() -> Self {
        let config = crate::config::AppConfig::default();
        let runtime_config = RuntimeConfigState::for_tests(&config);
        let store =
            crate::store::VaultStore::new_with_auth_config(20, runtime_config.auth_config());
        store.seed_example_data().await;
        Self {
            service: VaultBridgeService::new(store, None),
            api_tokens: ApiTokenState::default(),
            mcp: None,
            runtime_config,
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct ApiTokenState {
    token_file: Option<PathBuf>,
    token_dir: Option<PathBuf>,
    inline_tokens: Vec<(String, String)>,
    auth_config: RuntimeAuthConfig,
}

#[derive(Clone, Debug)]
struct ApiTokenIdentity {
    token: String,
    auth: AuthContext,
}

impl ApiTokenState {
    pub fn new(
        token_file: Option<PathBuf>,
        token_dir: Option<PathBuf>,
        tokens: BTreeMap<String, ApiTokenConfig>,
    ) -> Self {
        let snapshot = AuthConfigSnapshot {
            api_tokens: tokens,
            ..AuthConfigSnapshot::default()
        };
        Self {
            token_file,
            token_dir,
            inline_tokens: Vec::new(),
            auth_config: RuntimeAuthConfig::new(snapshot),
        }
    }

    pub fn new_with_auth_config(
        token_file: Option<PathBuf>,
        token_dir: Option<PathBuf>,
        auth_config: RuntimeAuthConfig,
    ) -> Self {
        Self {
            token_file,
            token_dir,
            inline_tokens: Vec::new(),
            auth_config,
        }
    }

    pub fn from_env(tokens: BTreeMap<String, ApiTokenConfig>) -> Self {
        let token_file = std::env::var("API_TOKEN_FILE")
            .ok()
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty())
            .map(PathBuf::from);
        let token_dir = std::env::var("API_TOKEN_DIR")
            .ok()
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty())
            .map(PathBuf::from);

        Self::new(token_file, token_dir, tokens)
    }

    pub fn from_env_with_auth_config(auth_config: RuntimeAuthConfig) -> Self {
        let token_file = std::env::var("API_TOKEN_FILE")
            .ok()
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty())
            .map(PathBuf::from);
        let token_dir = std::env::var("API_TOKEN_DIR")
            .ok()
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty())
            .map(PathBuf::from);

        Self::new_with_auth_config(token_file, token_dir, auth_config)
    }

    pub fn for_tests(
        entries: impl IntoIterator<Item = (&'static str, &'static str, &'static str)>,
    ) -> Self {
        let mut tokens = BTreeMap::new();
        let mut token_identities = Vec::new();
        for (name, token, context) in entries {
            tokens.insert(
                name.to_string(),
                ApiTokenConfig {
                    context: context.to_string(),
                },
            );
            token_identities.push((name.to_string(), token.to_string()));
        }
        Self {
            token_file: None,
            token_dir: None,
            inline_tokens: token_identities,
            auth_config: RuntimeAuthConfig::new(AuthConfigSnapshot {
                api_tokens: tokens,
                ..AuthConfigSnapshot::default()
            }),
        }
    }

    fn read_token_file(path: &std::path::Path) -> Option<String> {
        let contents = std::fs::read_to_string(path).ok()?;
        let token = contents.trim().to_string();
        if token.is_empty() { None } else { Some(token) }
    }

    async fn expected_tokens(&self) -> Vec<ApiTokenIdentity> {
        let mut tokens = Vec::new();
        let snapshot = self.auth_config.snapshot().await;

        if let Some(dir) = self.token_dir.as_ref()
            && let Ok(entries) = std::fs::read_dir(dir)
        {
            let mut paths = entries
                .filter_map(|entry| entry.ok().map(|entry| entry.path()))
                .filter(|path| path.is_file())
                .collect::<Vec<_>>();
            paths.sort();
            for path in paths {
                if let Some(token) = Self::read_token_file(&path)
                    && !tokens
                        .iter()
                        .any(|identity: &ApiTokenIdentity| identity.token == token)
                    && let Some(identity) =
                        self.identity_for_token_path(&snapshot.api_tokens, &path, token)
                {
                    tokens.push(identity);
                }
            }
        }

        if let Some(path) = self.token_file.as_ref()
            && let Some(token) = Self::read_token_file(path)
            && !tokens
                .iter()
                .any(|identity: &ApiTokenIdentity| identity.token == token)
            && let Some(identity) = self.identity_for_token_path(&snapshot.api_tokens, path, token)
        {
            tokens.push(identity);
        }

        for (name, token) in &self.inline_tokens {
            if !tokens
                .iter()
                .any(|identity: &ApiTokenIdentity| identity.token == *token)
                && let Some(identity) =
                    Self::identity_for_token(&snapshot.api_tokens, name.clone(), token.clone())
            {
                tokens.push(identity);
            }
        }

        tokens
    }

    fn identity_for_token_path(
        &self,
        token_config: &BTreeMap<String, ApiTokenConfig>,
        path: &std::path::Path,
        token: String,
    ) -> Option<ApiTokenIdentity> {
        let name = path.file_stem()?.to_str()?.to_string();
        Self::identity_for_token(token_config, name, token)
    }

    fn identity_for_token(
        token_config: &BTreeMap<String, ApiTokenConfig>,
        name: String,
        token: String,
    ) -> Option<ApiTokenIdentity> {
        let config = token_config.get(&name)?;
        Some(ApiTokenIdentity {
            token,
            auth: AuthContext::new(
                ContextName::new(config.context.clone()),
                format!("api_token:{name}"),
            ),
        })
    }

    async fn authorize(&self, token: &str) -> Option<AuthContext> {
        let token = token.trim();
        if token.is_empty() {
            return None;
        }
        self.expected_tokens()
            .await
            .into_iter()
            .find(|identity| identity.token == token)
            .map(|identity| identity.auth)
    }
}

#[derive(Debug, Error)]
pub enum ApiError {
    #[error("unauthorized")]
    Unauthorized,
    #[error("not found")]
    NotFound,
    #[error("bad request: {0}")]
    BadRequest(String),
    #[error("forbidden: {0}")]
    Forbidden(String),
    #[error("conflict: {0}")]
    Conflict(String),
    #[error("service unavailable: {0}")]
    Unavailable(String),
    #[error("internal error: {0}")]
    Internal(String),
    #[error(transparent)]
    Service(#[from] ServiceError),
}

#[derive(Debug, Serialize)]
struct ApiErrorResponse {
    error: String,
    #[serde(flatten)]
    metadata: ErrorMetadata,
}

impl ApiError {
    fn status(&self) -> StatusCode {
        match self {
            Self::Unauthorized => StatusCode::UNAUTHORIZED,
            Self::NotFound => StatusCode::NOT_FOUND,
            Self::BadRequest(_) => StatusCode::BAD_REQUEST,
            Self::Forbidden(_) => StatusCode::FORBIDDEN,
            Self::Conflict(_) => StatusCode::CONFLICT,
            Self::Unavailable(_) => StatusCode::SERVICE_UNAVAILABLE,
            Self::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Self::Service(error) => service_error_status(error),
        }
    }

    fn legacy_error(&self) -> String {
        match self {
            Self::Unauthorized => "unauthorized".to_string(),
            Self::NotFound => "not found".to_string(),
            Self::BadRequest(message)
            | Self::Forbidden(message)
            | Self::Conflict(message)
            | Self::Unavailable(message)
            | Self::Internal(message) => message.clone(),
            Self::Service(error) => service_error_legacy_error(error),
        }
    }

    fn metadata(&self) -> ErrorMetadata {
        match self {
            Self::Unauthorized => ErrorMetadata::new(
                ErrorCategory::Permission,
                false,
                "unauthorized",
                "API key is missing or invalid for this REST endpoint",
            )
            .with_http_status(401),
            Self::NotFound => ErrorMetadata::new(
                ErrorCategory::Business,
                false,
                "resource not found",
                "The requested note or graph resource is not visible to this context",
            )
            .with_http_status(404),
            Self::BadRequest(message) => {
                ErrorMetadata::new(ErrorCategory::Validation, false, "bad request", message)
                    .with_http_status(400)
            }
            Self::Forbidden(message) => ErrorMetadata::new(
                ErrorCategory::Permission,
                false,
                "permission denied",
                message,
            )
            .with_http_status(403),
            Self::Conflict(message) => ErrorMetadata::new(
                ErrorCategory::Business,
                false,
                "resource already exists",
                message,
            )
            .with_http_status(409),
            Self::Unavailable(message) => ErrorMetadata::new(
                ErrorCategory::Transient,
                true,
                "service unavailable",
                message,
            )
            .with_http_status(503),
            Self::Internal(message) => ErrorMetadata::new(
                ErrorCategory::Transient,
                true,
                "internal server error",
                message,
            )
            .with_http_status(500),
            Self::Service(error) => service_error_metadata(error, None),
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let status = self.status();
        let body = ApiErrorResponse {
            error: self.legacy_error(),
            metadata: self.metadata(),
        };

        (status, Json(body)).into_response()
    }
}

fn service_error_legacy_error(error: &ServiceError) -> String {
    match error {
        ServiceError::NotFound => "not found".to_string(),
        ServiceError::BadRequest(message) => message.clone(),
        ServiceError::Write(error) => match error {
            crate::new_note::WriteError::NotFound { .. } => "not found".to_string(),
            other => other.to_string(),
        },
        ServiceError::CouchDbWrite(error) | ServiceError::CouchDbUpdate(error) => error.to_string(),
    }
}

#[derive(Debug, Deserialize)]
struct SearchParams {
    q: String,
    #[serde(default)]
    mode: Option<SearchMode>,
    #[serde(default)]
    limit: Option<usize>,
}

#[derive(Debug, Deserialize)]
struct RecentParams {
    #[serde(default)]
    since: Option<String>,
    #[serde(default)]
    last_n_days: Option<i64>,
    #[serde(default)]
    limit: Option<usize>,
}

#[derive(Debug, Deserialize)]
struct TimeFilterParams {
    #[serde(default)]
    created_after: Option<String>,
    #[serde(default)]
    created_before: Option<String>,
    #[serde(default)]
    updated_after: Option<String>,
    #[serde(default)]
    updated_before: Option<String>,
}

#[derive(Debug, Deserialize)]
struct NeighborsParams {
    #[serde(default)]
    depth: Option<usize>,
    #[serde(default)]
    direction: Option<NeighborDirection>,
}

#[derive(Debug, Deserialize)]
struct PathParams {
    from: String,
    to: String,
}

#[derive(Debug, Deserialize)]
struct GetNoteLookupRequest {
    #[serde(default)]
    id: Option<String>,
    #[serde(default)]
    title: Option<String>,
}

pub async fn serve(state: AppState, addr: SocketAddr) -> anyhow::Result<()> {
    let listener = tokio::net::TcpListener::bind(addr).await?;
    info!("vault_bridge listening on {}", addr);
    axum::serve(listener, app_router(state)).await?;
    Ok(())
}

pub fn app_router(state: AppState) -> Router {
    let mcp_state = state.mcp.clone();
    let mut router = Router::new()
        .route("/api/v1/notes/recent", get(recent_notes))
        .route("/api/v1/notes/query", post(query_notes))
        .route("/api/v1/base/query", post(query_base))
        .route("/api/v1/notes/get", post(get_note_lookup))
        .route("/api/v1/notes", post(create_note))
        .route("/api/v1/notes/{*id}", get(get_note).put(update_note))
        .route("/api/v1/search", get(search_notes))
        .route("/api/v1/neighbors/{*id}", get(get_neighbors))
        .route("/api/v1/backlinks/{*id}", get(get_backlinks))
        .route("/api/v1/assemble-context", post(assemble_context_endpoint))
        .route("/api/v1/graph/path", get(find_path))
        .route("/api/v1/tags", get(list_tags))
        .route("/api/v1/status", get(status))
        .route("/api/v1/metrics", get(metrics))
        .merge(api_docs::router::<AppState>())
        .with_state(state);
    if let Some(mcp_state) = mcp_state {
        router = router.merge(crate::mcp::app_router(mcp_state));
    }
    router
}

async fn auth_from_headers(state: &AppState, headers: &HeaderMap) -> Result<AuthContext, ApiError> {
    let key = api_key_from_headers(headers).ok_or(ApiError::Unauthorized)?;
    state
        .api_tokens
        .authorize(key)
        .await
        .ok_or(ApiError::Unauthorized)
}

fn decode_json<T>(payload: Result<Json<T>, JsonRejection>) -> Result<T, ApiError> {
    let Json(payload) = payload.map_err(|rejection| ApiError::BadRequest(rejection.body_text()))?;
    Ok(payload)
}

fn decode_query<T>(query: Result<Query<T>, QueryRejection>) -> Result<T, ApiError> {
    let Query(query) = query.map_err(|rejection| ApiError::BadRequest(rejection.body_text()))?;
    Ok(query)
}

async fn get_note(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Result<Json<crate::model::Note>, ApiError> {
    let auth = auth_from_headers(&state, &headers).await?;
    let note_id = NoteId::new(id);
    let note = state
        .service
        .get_note(&auth, &note_id)
        .await
        .map_err(map_service_error)?;
    log_access(
        &state,
        &auth,
        "/api/v1/notes/{id}",
        &json!({"id": note_id}),
        &[note.id.clone()],
    )
    .await;
    Ok(Json(note))
}

async fn get_note_lookup(
    State(state): State<AppState>,
    headers: HeaderMap,
    payload: Result<Json<GetNoteLookupRequest>, JsonRejection>,
) -> Result<Json<crate::model::Note>, ApiError> {
    let auth = auth_from_headers(&state, &headers).await?;
    let request = decode_json(payload)?;
    let note = if let Some(id) = request
        .id
        .as_deref()
        .map(str::trim)
        .filter(|id| !id.is_empty())
    {
        state.service.get_note(&auth, &NoteId::new(id)).await
    } else if let Some(title) = request
        .title
        .as_deref()
        .map(str::trim)
        .filter(|title| !title.is_empty())
    {
        state.service.get_note_by_title(&auth, title).await
    } else {
        return Err(ApiError::BadRequest(
            "either id or title is required".to_string(),
        ));
    }
    .map_err(map_service_error)?;
    log_access(
        &state,
        &auth,
        "/api/v1/notes/get",
        &json!({"id": note.id}),
        &[note.id.clone()],
    )
    .await;
    Ok(Json(note))
}

async fn search_notes(
    State(state): State<AppState>,
    headers: HeaderMap,
    query: Result<Query<SearchParams>, QueryRejection>,
) -> Result<Json<crate::search::SearchResponse>, ApiError> {
    let auth = auth_from_headers(&state, &headers).await?;
    let params = decode_query(query)?;
    if params.q.trim().is_empty() {
        return Err(ApiError::BadRequest("q is required".to_string()));
    }
    let mode = params.mode.unwrap_or_default();
    let limit = params.limit.unwrap_or(20).min(50);
    let response = state.service.search(&auth, &params.q, mode, limit).await;
    let ids = response
        .results
        .iter()
        .map(|hit| hit.id.clone())
        .collect::<Vec<_>>();
    log_access(
        &state,
        &auth,
        "/api/v1/search",
        &json!({"q": params.q, "mode": mode, "limit": limit}),
        &ids,
    )
    .await;
    Ok(Json(response))
}

async fn recent_notes(
    State(state): State<AppState>,
    headers: HeaderMap,
    query: Result<Query<RecentParams>, QueryRejection>,
) -> Result<Json<crate::store::RecentNotesResponse>, ApiError> {
    let auth = auth_from_headers(&state, &headers).await?;
    let params = decode_query(query)?;
    let since = parse_datetime_option(params.since.as_deref())?;
    let limit = params.limit.unwrap_or(20).min(MAX_NOTE_LIST_LIMIT);
    let response = state
        .service
        .recent_notes(&auth, since, params.last_n_days, limit)
        .await
        .map_err(map_service_error)?;
    let ids = response
        .notes
        .iter()
        .map(|note| note.id.clone())
        .collect::<Vec<_>>();
    log_access(
        &state,
        &auth,
        "/api/v1/notes/recent",
        &json!({"since": since, "last_n_days": params.last_n_days, "limit": limit}),
        &ids,
    )
    .await;
    Ok(Json(response))
}

async fn query_notes(
    State(state): State<AppState>,
    headers: HeaderMap,
    payload: Result<Json<QueryNotesRequest>, JsonRejection>,
) -> Result<Json<crate::store::RecentNotesResponse>, ApiError> {
    let auth = auth_from_headers(&state, &headers).await?;
    let request = decode_json(payload)?;
    let request_log = json!(&request);
    let response = state.service.query_notes(&auth, request).await;
    let ids = response
        .notes
        .iter()
        .map(|note| note.id.clone())
        .collect::<Vec<_>>();
    log_access(&state, &auth, "/api/v1/notes/query", &request_log, &ids).await;
    Ok(Json(response))
}

async fn query_base(
    State(state): State<AppState>,
    headers: HeaderMap,
    payload: Result<Json<QueryBaseRequest>, JsonRejection>,
) -> Result<Json<crate::base_query::QueryBaseResponse>, ApiError> {
    let auth = auth_from_headers(&state, &headers).await?;
    let request = decode_json(payload)?;
    let request_log = json!(&request);
    let response = state
        .service
        .query_base(&auth, request)
        .await
        .map_err(map_service_error)?;
    let ids = response
        .rows
        .iter()
        .map(|row| row.note_id.clone())
        .collect::<Vec<_>>();
    log_access(&state, &auth, "/api/v1/base/query", &request_log, &ids).await;
    Ok(Json(response))
}

async fn get_neighbors(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
    query: Result<Query<NeighborsParams>, QueryRejection>,
) -> Result<Json<crate::store::NeighborsResponse>, ApiError> {
    let auth = auth_from_headers(&state, &headers).await?;
    let params = decode_query(query)?;
    let note_id = NoteId::new(id);
    let depth = params
        .depth
        .unwrap_or(1)
        .clamp(1, MAX_GRAPH_TRAVERSAL_DEPTH);
    let direction = params.direction.unwrap_or_default();
    let response = state
        .service
        .neighbors(&auth, &note_id, depth, direction)
        .await
        .map_err(map_service_error)?;
    let ids = response
        .nodes
        .iter()
        .map(|node| node.id.clone())
        .collect::<Vec<_>>();
    log_access(
        &state,
        &auth,
        "/api/v1/neighbors/{id}",
        &json!({"id": note_id, "depth": depth, "direction": direction}),
        &ids,
    )
    .await;
    Ok(Json(response))
}

async fn get_backlinks(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Result<Json<crate::store::BacklinksResponse>, ApiError> {
    let auth = auth_from_headers(&state, &headers).await?;
    let note_id = NoteId::new(id);
    let response = state
        .service
        .backlinks(&auth, &note_id)
        .await
        .map_err(map_service_error)?;
    let ids = response
        .backlinks
        .iter()
        .map(|entry| entry.id.clone())
        .collect::<Vec<_>>();
    log_access(
        &state,
        &auth,
        "/api/v1/backlinks/{id}",
        &json!({"id": note_id}),
        &ids,
    )
    .await;
    Ok(Json(response))
}

async fn assemble_context_endpoint(
    State(state): State<AppState>,
    headers: HeaderMap,
    payload: Result<Json<AssembleContextRequest>, JsonRejection>,
) -> Result<Json<crate::context::AssembleContextResponse>, ApiError> {
    let auth = auth_from_headers(&state, &headers).await?;
    let request = decode_json(payload)?;
    let request_log = serde_json::to_value(&request).unwrap_or_else(|_| json!({}));
    let response = state.service.assemble_context(&auth, request).await;
    let ids = response
        .notes
        .iter()
        .map(|note| note.id.clone())
        .collect::<Vec<_>>();
    log_access(
        &state,
        &auth,
        "/api/v1/assemble-context",
        &request_log,
        &ids,
    )
    .await;
    Ok(Json(response))
}

async fn find_path(
    State(state): State<AppState>,
    headers: HeaderMap,
    query: Result<Query<PathParams>, QueryRejection>,
) -> Result<Json<crate::store::PathResponse>, ApiError> {
    let auth = auth_from_headers(&state, &headers).await?;
    let params = decode_query(query)?;
    let from = NoteId::new(params.from);
    let to = NoteId::new(params.to);
    let response = state.service.shortest_path(&auth, &from, &to).await;
    let ids = response.path.clone().unwrap_or_default();
    log_access(
        &state,
        &auth,
        "/api/v1/graph/path",
        &json!({"from": from, "to": to}),
        &ids,
    )
    .await;
    Ok(Json(response))
}

async fn list_tags(
    State(state): State<AppState>,
    headers: HeaderMap,
    query: Result<Query<TimeFilterParams>, QueryRejection>,
) -> Result<Json<crate::store::TagsResponse>, ApiError> {
    let auth = auth_from_headers(&state, &headers).await?;
    let params = decode_query(query)?;
    let filter = parse_time_filter(params)?;
    let request_log = json!(&filter);
    let response = state.service.list_tags(&auth, filter).await;
    log_access(&state, &auth, "/api/v1/tags", &request_log, &[]).await;
    Ok(Json(response))
}

async fn create_note(
    State(state): State<AppState>,
    headers: HeaderMap,
    payload: Result<Json<NewNoteRequest>, JsonRejection>,
) -> Result<Json<crate::store::NewNoteResponse>, ApiError> {
    let auth = auth_from_headers(&state, &headers).await?;
    let request = decode_json(payload)?;
    let response = state
        .service
        .create_note(&auth, request)
        .await
        .map_err(map_service_error)?;
    log_access(
        &state,
        &auth,
        "/api/v1/notes",
        &json!({"id": response.id}),
        &[response.id.clone()],
    )
    .await;
    Ok(Json(response))
}

async fn update_note(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
    payload: Result<Json<UpdateNoteRequest>, JsonRejection>,
) -> Result<Json<crate::store::UpdateNoteResponse>, ApiError> {
    let auth = auth_from_headers(&state, &headers).await?;
    let request = decode_json(payload)?;
    let note_id = NoteId::new(id);
    let response = state
        .service
        .update_note(&auth, &note_id, request)
        .await
        .map_err(map_service_error)?;
    log_access(
        &state,
        &auth,
        "/api/v1/notes/{id}",
        &json!({"id": response.id, "action": "update"}),
        &[response.id.clone()],
    )
    .await;
    Ok(Json(response))
}

async fn status(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<StatusResponse>, ApiError> {
    let auth = auth_from_headers(&state, &headers).await?;
    let mut response = state.service.status().await;
    response.config_reload = state.runtime_config.reload_status().await;
    log_access(&state, &auth, "/api/v1/status", &json!({}), &[]).await;
    Ok(Json(response))
}

async fn metrics(State(state): State<AppState>, headers: HeaderMap) -> Result<Response, ApiError> {
    let auth = auth_from_headers(&state, &headers).await?;
    metrics_response(&state, auth.context.as_str()).await
}

async fn log_access(
    state: &AppState,
    auth: &AuthContext,
    endpoint: &str,
    query_params: &serde_json::Value,
    returned_ids: &[NoteId],
) {
    state
        .service
        .store
        .log_access(
            auth.context.as_str(),
            endpoint,
            query_params,
            returned_ids,
            0,
        )
        .await;
}

async fn metrics_response(state: &AppState, auth_context: &str) -> Result<Response, ApiError> {
    let mut status = state.service.status().await;
    status.config_reload = state.runtime_config.reload_status().await;
    let body = render_prometheus_metrics(&status);

    state
        .service
        .store
        .log_access(auth_context, "/api/v1/metrics", &json!({}), &[], 0)
        .await;

    Ok((
        [(CONTENT_TYPE, "text/plain; version=0.0.4; charset=utf-8")],
        body,
    )
        .into_response())
}

fn api_key_from_headers(headers: &HeaderMap) -> Option<&str> {
    headers
        .get("x-api-key")
        .or_else(|| headers.get("X-Api-Key"))
        .and_then(|value| value.to_str().ok())
}

fn map_service_error(error: ServiceError) -> ApiError {
    ApiError::Service(error)
}

fn parse_datetime_option(value: Option<&str>) -> Result<Option<DateTime<Utc>>, ApiError> {
    parse_named_datetime_option("since", value)
}

fn parse_named_datetime_option(
    field: &str,
    value: Option<&str>,
) -> Result<Option<DateTime<Utc>>, ApiError> {
    match value {
        None => Ok(None),
        Some(raw) => DateTime::parse_from_rfc3339(raw)
            .map(|dt| Some(dt.with_timezone(&Utc)))
            .map_err(|_| ApiError::BadRequest(format!("{field} must be RFC3339"))),
    }
}

fn parse_time_filter(params: TimeFilterParams) -> Result<NoteTimeFilter, ApiError> {
    Ok(NoteTimeFilter {
        created_after: parse_named_datetime_option(
            "created_after",
            params.created_after.as_deref(),
        )?,
        created_before: parse_named_datetime_option(
            "created_before",
            params.created_before.as_deref(),
        )?,
        updated_after: parse_named_datetime_option(
            "updated_after",
            params.updated_after.as_deref(),
        )?,
        updated_before: parse_named_datetime_option(
            "updated_before",
            params.updated_before.as_deref(),
        )?,
    })
}

fn render_prometheus_metrics(status: &StatusResponse) -> String {
    let image_tag = std::env::var("VAULT_BRIDGE_IMAGE_TAG").ok();
    let git_commit = std::env::var("VAULT_BRIDGE_GIT_COMMIT").ok();
    let mut lines = vec![
        "# HELP vault_bridge_build_info Build and deployment metadata for the Vault Bridge API process.".to_string(),
        "# TYPE vault_bridge_build_info gauge".to_string(),
        build_info_metric_line(
            env!("CARGO_PKG_VERSION"),
            image_tag.as_deref(),
            git_commit.as_deref(),
        ),
        "# HELP vault_bridge_sync_behind_by CouchDB change-sequence lag.".to_string(),
        "# TYPE vault_bridge_sync_behind_by gauge".to_string(),
        format!("vault_bridge_sync_behind_by {}", status.sync.behind_by),
        "# HELP vault_bridge_pending_embeddings Notes missing embeddings.".to_string(),
        "# TYPE vault_bridge_pending_embeddings gauge".to_string(),
        format!(
            "vault_bridge_pending_embeddings {}",
            status.index.pending_embeddings
        ),
        "# HELP vault_bridge_quarantined_embeddings Notes that exceeded max embedding failures."
            .to_string(),
        "# TYPE vault_bridge_quarantined_embeddings gauge".to_string(),
        format!(
            "vault_bridge_quarantined_embeddings {}",
            status.index.quarantined_embeddings
        ),
        "# HELP vault_bridge_pending_chunk_embeddings Semantic chunks missing embeddings."
            .to_string(),
        "# TYPE vault_bridge_pending_chunk_embeddings gauge".to_string(),
        format!(
            "vault_bridge_pending_chunk_embeddings {}",
            status.index.pending_chunk_embeddings
        ),
        "# HELP vault_bridge_quarantined_chunk_embeddings Semantic chunks that exceeded max embedding failures."
            .to_string(),
        "# TYPE vault_bridge_quarantined_chunk_embeddings gauge".to_string(),
        format!(
            "vault_bridge_quarantined_chunk_embeddings {}",
            status.index.quarantined_chunk_embeddings
        ),
        "# HELP vault_bridge_embedding_backend_degraded Embedding backend degraded state."
            .to_string(),
        "# TYPE vault_bridge_embedding_backend_degraded gauge".to_string(),
        format!(
            "vault_bridge_embedding_backend_degraded {}",
            usize::from(status.embedding.backend_state == "degraded")
        ),
        "# HELP vault_bridge_embedding_last_success_timestamp_seconds Last embedding provider success timestamp."
            .to_string(),
        "# TYPE vault_bridge_embedding_last_success_timestamp_seconds gauge".to_string(),
        format!(
            "vault_bridge_embedding_last_success_timestamp_seconds {}",
            status
                .embedding
                .last_success_at
                .map(|timestamp| timestamp.timestamp())
                .unwrap_or(0)
        ),
        "# HELP vault_bridge_embedding_last_error_timestamp_seconds Last embedding provider or payload error timestamp."
            .to_string(),
        "# TYPE vault_bridge_embedding_last_error_timestamp_seconds gauge".to_string(),
        format!(
            "vault_bridge_embedding_last_error_timestamp_seconds {}",
            status
                .embedding
                .last_error_at
                .map(|timestamp| timestamp.timestamp())
                .unwrap_or(0)
        ),
        "# HELP vault_bridge_pending_chunks Parents waiting for full chunk sets.".to_string(),
        "# TYPE vault_bridge_pending_chunks gauge".to_string(),
        format!(
            "vault_bridge_pending_chunks {}",
            status.index.pending_chunks
        ),
        "# HELP vault_bridge_orphan_leaf_staging Parents in chunk staging that are bare LiveSync leaf ids without a current file alias.".to_string(),
        "# TYPE vault_bridge_orphan_leaf_staging gauge".to_string(),
        format!(
            "vault_bridge_orphan_leaf_staging {}",
            status.index.orphan_leaf_staging_count
        ),
        "# HELP vault_bridge_stale_file_aliases LiveSync file aliases whose indexed note row is missing or stale.".to_string(),
        "# TYPE vault_bridge_stale_file_aliases gauge".to_string(),
        format!(
            "vault_bridge_stale_file_aliases {}",
            status.index.stale_file_aliases
        ),
        "# HELP vault_bridge_total_notes Total indexed notes.".to_string(),
        "# TYPE vault_bridge_total_notes gauge".to_string(),
        format!("vault_bridge_total_notes {}", status.index.total_notes),
        "# HELP vault_bridge_total_links Total indexed links.".to_string(),
        "# TYPE vault_bridge_total_links gauge".to_string(),
        format!("vault_bridge_total_links {}", status.index.total_links),
        "# HELP vault_bridge_total_tags Total unique tags.".to_string(),
        "# TYPE vault_bridge_total_tags gauge".to_string(),
        format!("vault_bridge_total_tags {}", status.index.total_tags),
        "# HELP vault_bridge_config_reload_enabled Config hot reload enabled state.".to_string(),
        "# TYPE vault_bridge_config_reload_enabled gauge".to_string(),
        format!(
            "vault_bridge_config_reload_enabled {}",
            usize::from(status.config_reload.enabled)
        ),
        "# HELP vault_bridge_config_reload_generation Active validated config generation.".to_string(),
        "# TYPE vault_bridge_config_reload_generation gauge".to_string(),
        format!(
            "vault_bridge_config_reload_generation {}",
            status.config_reload.generation
        ),
        "# HELP vault_bridge_config_reload_success_total Successful config reload attempts."
            .to_string(),
        "# TYPE vault_bridge_config_reload_success_total counter".to_string(),
        format!(
            "vault_bridge_config_reload_success_total {}",
            status.config_reload.success_count
        ),
        "# HELP vault_bridge_config_reload_failure_total Failed config reload attempts.".to_string(),
        "# TYPE vault_bridge_config_reload_failure_total counter".to_string(),
        format!(
            "vault_bridge_config_reload_failure_total {}",
            status.config_reload.failure_count
        ),
        "# HELP vault_bridge_config_reload_last_success_timestamp_seconds Last successful config load or reload timestamp.".to_string(),
        "# TYPE vault_bridge_config_reload_last_success_timestamp_seconds gauge".to_string(),
        format!(
            "vault_bridge_config_reload_last_success_timestamp_seconds {}",
            status
                .config_reload
                .last_success_at
                .map(|timestamp| timestamp.timestamp())
                .unwrap_or(0)
        ),
        "# HELP vault_bridge_config_reload_last_failure_timestamp_seconds Last failed config reload timestamp.".to_string(),
        "# TYPE vault_bridge_config_reload_last_failure_timestamp_seconds gauge".to_string(),
        format!(
            "vault_bridge_config_reload_last_failure_timestamp_seconds {}",
            status
                .config_reload
                .last_failure_at
                .map(|timestamp| timestamp.timestamp())
                .unwrap_or(0)
        ),
    ];

    let mut contexts = status.context_stats.iter().collect::<Vec<_>>();
    contexts.sort_by(|(a, _), (b, _)| a.cmp(b));
    for (context, stats) in contexts {
        lines.push(format!(
            "vault_bridge_accessible_notes{{context=\"{}\"}} {}",
            context, stats.accessible_notes
        ));
        lines.push(format!(
            "vault_bridge_filtered_notes{{context=\"{}\"}} {}",
            context, stats.filtered_notes
        ));
    }

    lines.push(String::new());
    lines.join("\n")
}

fn build_info_metric_line(
    version: &str,
    image_tag: Option<&str>,
    git_commit: Option<&str>,
) -> String {
    let image_tag = non_empty_label_value(image_tag).unwrap_or("unknown");
    let commit = non_empty_label_value(git_commit)
        .or_else(|| commit_from_image_tag(image_tag))
        .unwrap_or("unknown");

    format!(
        "vault_bridge_build_info{{version=\"{}\",image_tag=\"{}\",commit=\"{}\"}} 1",
        prometheus_label_escape(version),
        prometheus_label_escape(image_tag),
        prometheus_label_escape(commit)
    )
}

fn non_empty_label_value(value: Option<&str>) -> Option<&str> {
    value.map(str::trim).filter(|value| !value.is_empty())
}

fn commit_from_image_tag(image_tag: &str) -> Option<&str> {
    image_tag
        .strip_prefix("sha-")
        .filter(|value| !value.is_empty())
}

fn prometheus_label_escape(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len());
    for ch in value.chars() {
        match ch {
            '\\' => escaped.push_str("\\\\"),
            '"' => escaped.push_str("\\\""),
            '\n' => escaped.push_str("\\n"),
            _ => escaped.push(ch),
        }
    }
    escaped
}

#[cfg(test)]
mod tests {
    use axum::http::StatusCode;
    use axum::response::IntoResponse;
    use http_body_util::BodyExt;
    use serde_json::Value;

    use super::{ApiError, build_info_metric_line, commit_from_image_tag, prometheus_label_escape};

    async fn api_error_body(error: ApiError) -> (StatusCode, Value) {
        let response = error.into_response();
        let status = response.status();
        let body = response
            .into_body()
            .collect()
            .await
            .expect("collect body")
            .to_bytes();
        let body = serde_json::from_slice(&body).expect("json body");
        (status, body)
    }

    #[tokio::test]
    async fn internal_rest_errors_are_structured() {
        let (status, body) =
            api_error_body(ApiError::Internal("unexpected failure".to_string())).await;

        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(body["error"], "unexpected failure");
        assert_eq!(body["errorCategory"], "transient");
        assert_eq!(body["isRetryable"], true);
        assert_eq!(body["message"], "internal server error");
        assert_eq!(body["description"], "unexpected failure");
        assert_eq!(body["httpStatus"], 500);
    }

    #[test]
    fn build_info_metric_derives_commit_from_sha_image_tag() {
        assert_eq!(
            build_info_metric_line("0.1.0", Some("sha-1f0ad51"), None),
            r#"vault_bridge_build_info{version="0.1.0",image_tag="sha-1f0ad51",commit="1f0ad51"} 1"#
        );
    }

    #[test]
    fn build_info_metric_prefers_explicit_git_commit() {
        assert_eq!(
            build_info_metric_line(
                "0.1.0",
                Some("sha-1f0ad51"),
                Some("1f0ad51fdd49f437cb61a09ab14cbb2d4c508dde")
            ),
            r#"vault_bridge_build_info{version="0.1.0",image_tag="sha-1f0ad51",commit="1f0ad51fdd49f437cb61a09ab14cbb2d4c508dde"} 1"#
        );
    }

    #[test]
    fn build_info_metric_uses_unknown_for_unset_deploy_metadata() {
        assert_eq!(
            build_info_metric_line("0.1.0", None, None),
            r#"vault_bridge_build_info{version="0.1.0",image_tag="unknown",commit="unknown"} 1"#
        );
    }

    #[test]
    fn build_info_metric_escapes_prometheus_label_values() {
        assert_eq!(
            prometheus_label_escape("quote\"slash\\newline\n"),
            "quote\\\"slash\\\\newline\\n"
        );
    }

    #[test]
    fn commit_from_image_tag_requires_sha_prefix() {
        assert_eq!(commit_from_image_tag("sha-abc123"), Some("abc123"));
        assert_eq!(commit_from_image_tag("prod"), None);
        assert_eq!(commit_from_image_tag("sha-"), None);
    }
}

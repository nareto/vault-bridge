use axum::http::StatusCode;
use serde::Serialize;
use serde_json::Value;

use crate::couchdb::CouchDbError;
use crate::new_note::WriteError;
use crate::service::ServiceError;

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub(crate) enum ErrorCategory {
    Transient,
    Validation,
    Business,
    Permission,
}

impl ErrorCategory {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Transient => "transient",
            Self::Validation => "validation",
            Self::Business => "business",
            Self::Permission => "permission",
        }
    }
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub(crate) struct ErrorMetadata {
    #[serde(rename = "errorCategory")]
    error_category: ErrorCategory,
    #[serde(rename = "isRetryable")]
    is_retryable: bool,
    message: String,
    description: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    tool: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", rename = "httpStatus")]
    http_status: Option<u16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    attempted: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none", rename = "partialResult")]
    partial_result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    details: Option<Value>,
}

impl ErrorMetadata {
    pub(crate) fn new(
        error_category: ErrorCategory,
        is_retryable: bool,
        message: impl Into<String>,
        description: impl Into<String>,
    ) -> Self {
        Self {
            error_category,
            is_retryable,
            message: message.into(),
            description: description.into(),
            tool: None,
            http_status: None,
            attempted: None,
            partial_result: None,
            details: None,
        }
    }

    pub(crate) fn with_tool(mut self, tool_name: &str) -> Self {
        self.tool = Some(tool_name.to_string());
        self
    }

    pub(crate) fn with_http_status(mut self, status: u16) -> Self {
        self.http_status = Some(status);
        self
    }

    pub(crate) fn with_details(mut self, details: Value) -> Self {
        if !details.is_null() {
            self.details = Some(details);
        }
        self
    }

    pub(crate) fn http_status(&self) -> Option<u16> {
        self.http_status
    }

    pub(crate) fn message(&self) -> &str {
        &self.message
    }

    pub(crate) fn summary(&self) -> String {
        format!(
            "{} error: {}",
            self.error_category.as_str(),
            self.description
        )
    }
}

pub(crate) fn service_error_status(error: &ServiceError) -> StatusCode {
    metadata_status(&service_error_metadata(error, None))
}

pub(crate) fn metadata_status(metadata: &ErrorMetadata) -> StatusCode {
    metadata
        .http_status()
        .and_then(|status| StatusCode::from_u16(status).ok())
        .unwrap_or(StatusCode::INTERNAL_SERVER_ERROR)
}

pub(crate) fn service_error_metadata(
    error: &ServiceError,
    tool_name: Option<&str>,
) -> ErrorMetadata {
    let mut metadata = match error {
        ServiceError::NotFound => ErrorMetadata::new(
            ErrorCategory::Business,
            false,
            "resource not found",
            "The requested note or graph resource is not visible to this context",
        )
        .with_http_status(404),
        ServiceError::BadRequest(message) => {
            ErrorMetadata::new(ErrorCategory::Validation, false, "bad request", message)
                .with_http_status(400)
        }
        ServiceError::Write(error) => write_error_metadata(error),
        ServiceError::CouchDbWrite(error) => couchdb_error_metadata(error),
        ServiceError::CouchDbUpdate(error) => couchdb_error_metadata(error),
        ServiceError::VaultFileRepair(error) => couchdb_error_metadata(error),
        ServiceError::VaultFileTemporarilyUnavailable => ErrorMetadata::new(
            ErrorCategory::Transient,
            true,
            "raw vault file unavailable",
            "The policy-visible file could not be reconstructed from CouchDB; retry after source reconciliation",
        )
        .with_http_status(503),
    };
    if let Some(tool_name) = tool_name {
        metadata = metadata.with_tool(tool_name);
    }
    metadata
}

fn write_error_metadata(error: &WriteError) -> ErrorMetadata {
    match error {
        WriteError::PolicyDenied { .. } | WriteError::PathNotAllowed { .. } => ErrorMetadata::new(
            ErrorCategory::Permission,
            false,
            "write denied",
            error.to_string(),
        )
        .with_http_status(403),
        WriteError::AlreadyExists { .. } => ErrorMetadata::new(
            ErrorCategory::Business,
            false,
            "resource already exists",
            error.to_string(),
        )
        .with_http_status(409),
        WriteError::NotFound { .. } => ErrorMetadata::new(
            ErrorCategory::Business,
            false,
            "resource not found",
            "The requested note is not visible or not editable in this context",
        )
        .with_http_status(404),
        WriteError::TemplateNotFound { .. } => ErrorMetadata::new(
            ErrorCategory::Business,
            false,
            "resource not found",
            error.to_string(),
        )
        .with_http_status(404),
        WriteError::EmptyTitle
        | WriteError::InvalidCreate { .. }
        | WriteError::InvalidUpdate { .. } => ErrorMetadata::new(
            ErrorCategory::Validation,
            false,
            "write validation failed",
            error.to_string(),
        )
        .with_http_status(400),
        WriteError::Persistence => ErrorMetadata::new(
            ErrorCategory::Transient,
            true,
            "persistence failed",
            "The durable local content transaction failed; retry after source reconciliation",
        )
        .with_http_status(503),
    }
}

fn couchdb_error_metadata(error: &CouchDbError) -> ErrorMetadata {
    match error {
        CouchDbError::NoteAlreadyExists { .. } => ErrorMetadata::new(
            ErrorCategory::Business,
            false,
            "resource already exists",
            error.to_string(),
        )
        .with_http_status(409),
        CouchDbError::NoteNotFound { .. } => ErrorMetadata::new(
            ErrorCategory::Business,
            false,
            "resource not found",
            error.to_string(),
        )
        .with_http_status(404),
        _ => ErrorMetadata::new(
            ErrorCategory::Transient,
            true,
            "persistence failed",
            error.to_string(),
        )
        .with_http_status(503),
    }
}

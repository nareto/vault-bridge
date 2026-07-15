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
        ServiceError::VaultFileTemporarilyUnavailable {
            target_hash,
            source_state,
        } => ErrorMetadata::new(
            ErrorCategory::Transient,
            true,
            "raw vault file unavailable",
            "The authoritative CouchDB file is incomplete or unavailable; repair the reported source state before retrying",
        )
        .with_http_status(503)
        .with_details(serde_json::json!({
            "component": "couchdb",
            "phase": "source_reconciliation",
            "sourceState": source_state,
            "targetHash": target_hash,
            "action": "inspect the source diagnostic and restore missing LiveSync documents"
        })),
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
        WriteError::Persistence { kind } => {
            let (description, action) = match kind {
                crate::new_note::PersistenceFailureKind::InsufficientStorage => (
                    "PostgreSQL cannot persist the write because storage capacity is exhausted",
                    "restore database storage capacity and verify PostgreSQL recovery before retrying",
                ),
                crate::new_note::PersistenceFailureKind::DatabaseUnavailable => (
                    "PostgreSQL is unavailable; no authoritative source write was committed",
                    "restore PostgreSQL availability and retry with backoff",
                ),
                crate::new_note::PersistenceFailureKind::TransactionConflict => (
                    "The PostgreSQL transaction conflicted with concurrent work",
                    "retry the operation with backoff",
                ),
                crate::new_note::PersistenceFailureKind::Unknown => (
                    "The durable local content transaction failed before a source commit",
                    "inspect the correlated server log before retrying",
                ),
            };
            ErrorMetadata::new(
                ErrorCategory::Transient,
                kind.caller_retryable(),
                "persistence failed",
                description,
            )
            .with_http_status(503)
            .with_details(serde_json::json!({
                "component": "postgres",
                "phase": "local_persistence",
                "failureKind": kind.as_str(),
                "sourceCommitted": false,
                "action": action
            }))
        }
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
        CouchDbError::RevisionConflict { .. } | CouchDbError::Conflict { .. } => {
            ErrorMetadata::new(
                ErrorCategory::Business,
                false,
                "source revision conflict",
                "The authoritative CouchDB revision changed while the edit was being prepared; read the latest file and reapply the edit",
            )
            .with_http_status(409)
            .with_details(serde_json::json!({
                "component": "couchdb",
                "phase": "source_commit",
                "sourceState": "revision_conflict",
                "action": "read the latest source-backed file and reapply the exact edit"
            }))
        }
        _ => ErrorMetadata::new(
            ErrorCategory::Transient,
            true,
            "persistence failed",
            error.to_string(),
        )
        .with_http_status(503),
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::service_error_metadata;
    use crate::new_note::{PersistenceFailureKind, WriteError};
    use crate::service::ServiceError;

    #[test]
    fn insufficient_storage_is_actionable_and_not_blindly_retryable() {
        let metadata = service_error_metadata(
            &ServiceError::Write(WriteError::Persistence {
                kind: PersistenceFailureKind::InsufficientStorage,
            }),
            None,
        );
        let value = serde_json::to_value(metadata).expect("serialize metadata");

        assert_eq!(value["isRetryable"], false);
        assert_eq!(value["httpStatus"], 503);
        assert_eq!(value["details"]["failureKind"], "insufficient_storage");
        assert_eq!(value["details"]["sourceCommitted"], false);
        assert_ne!(value["details"]["action"], json!(null));
    }
}

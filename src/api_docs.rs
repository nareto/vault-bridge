use axum::response::Html;
use axum::routing::get;
use axum::{Json, Router};
use serde_json::{Value, json};

use crate::store::{MAX_GRAPH_TRAVERSAL_DEPTH, MAX_NOTE_LIST_LIMIT};

pub fn router<S>() -> Router<S>
where
    S: Clone + Send + Sync + 'static,
{
    Router::new()
        .route("/api-doc/openapi.json", get(openapi_json))
        .route("/swagger-ui", get(swagger_ui))
        .route("/swagger-ui/", get(swagger_ui))
}

async fn openapi_json() -> Json<Value> {
    Json(openapi_spec())
}

async fn swagger_ui() -> Html<&'static str> {
    Html(SWAGGER_UI_HTML)
}

pub(crate) fn openapi_spec() -> Value {
    json!({
        "openapi": "3.1.0",
        "info": {
            "title": "Vault Bridge REST API",
            "version": "0.1.0",
            "description": "Self-hosted REST API for context-authorized note access, search, graph traversal, and context assembly."
        },
        "tags": [
            {
                "name": "vault_bridge",
                "description": "Vault Bridge REST endpoints under /api/v1."
            }
        ],
        "components": {
            "securitySchemes": {
                "api_key": {
                    "type": "apiKey",
                    "in": "header",
                    "name": "X-Api-Key"
                }
            },
            "schemas": {
                "ApiErrorCategory": {
                    "type": "string",
                    "enum": ["transient", "validation", "business", "permission"]
                },
                "ApiError": {
                    "type": "object",
                    "required": ["error", "errorCategory", "isRetryable", "message", "description", "httpStatus"],
                    "properties": {
                        "error": {
                            "type": "string",
                            "description": "Backward-compatible human-readable error string."
                        },
                        "errorCategory": {"$ref": "#/components/schemas/ApiErrorCategory"},
                        "isRetryable": {
                            "type": "boolean",
                            "description": "Whether clients should retry after a short backoff."
                        },
                        "message": {
                            "type": "string",
                            "description": "Short machine-readable summary of the failure."
                        },
                        "description": {
                            "type": "string",
                            "description": "Human-readable explanation suitable for logs, clients, and agents."
                        },
                        "httpStatus": {
                            "type": "integer",
                            "minimum": 400,
                            "maximum": 599
                        },
                        "details": {
                            "description": "Optional structured context for this error."
                        },
                        "attempted": {
                            "type": "array",
                            "items": {"type": "string"}
                        },
                        "partialResult": {
                            "description": "Optional partial result when a request failed after producing usable data."
                        }
                    }
                },
                "Note": {
                    "type": "object",
                    "required": ["id", "path", "title", "content", "summary", "frontmatter", "links", "backlinks", "tags", "updated_at"],
                    "properties": {
                        "id": {
                            "type": "string",
                            "description": "Canonical vault-relative note ID. Pass this unchanged to `get_note(id=...)`."
                        },
                        "path": {"type": "string"},
                        "title": {
                            "type": "string",
                            "description": "Exact filename-derived title. This is the exact key accepted by `get_note(title=...)` and may differ from the first markdown H1."
                        },
                        "heading_title": {
                            "type": ["string", "null"],
                            "description": "First markdown H1 when present. Display-only metadata; do not use this as an exact `get_note(title=...)` lookup key."
                        },
                        "content": {"type": "string"},
                        "summary": {"type": "string"},
                        "frontmatter": {},
                        "links": {"type": "array", "items": {"type": "string"}},
                        "backlinks": {"type": "array", "items": {"type": "string"}},
                        "tags": {"type": "array", "items": {"type": "string"}},
                        "updated_at": {"type": "string", "format": "date-time"}
                    }
                },
                "SearchMode": {
                    "type": "string",
                    "enum": ["semantic", "fulltext", "hybrid"]
                },
                "MatchType": {
                    "type": "string",
                    "enum": ["semantic", "fulltext", "both"]
                },
                "SearchHit": {
                    "type": "object",
                    "required": ["id", "title", "snippet", "score", "match_type"],
                    "properties": {
                        "id": {"type": "string"},
                        "title": {"type": "string"},
                        "snippet": {"type": "string"},
                        "score": {"type": "number", "format": "float"},
                        "match_type": {"$ref": "#/components/schemas/MatchType"},
                        "matched_chunk_id": {"type": ["string", "null"]},
                        "matched_heading_path": {"type": ["string", "null"]},
                        "matched_snippet": {"type": ["string", "null"]}
                    }
                },
                "SearchResponse": {
                    "type": "object",
                    "required": ["results", "total_filtered"],
                    "properties": {
                        "results": {
                            "type": "array",
                            "items": {"$ref": "#/components/schemas/SearchHit"}
                        },
                        "total_filtered": {"type": "integer", "minimum": 0}
                    }
                },
                "RecentNoteSummary": {
                    "type": "object",
                    "required": ["id", "title", "summary", "tags", "updated_at", "link_count", "backlink_count"],
                    "properties": {
                        "id": {
                            "type": "string",
                            "description": "Canonical vault-relative note ID. Pass this unchanged to `get_note(id=...)`."
                        },
                        "title": {
                            "type": "string",
                            "description": "Exact filename-derived title. This is the exact key accepted by `get_note(title=...)` and may differ from the first markdown H1."
                        },
                        "heading_title": {
                            "type": ["string", "null"],
                            "description": "First markdown H1 when present. Display-only metadata; do not use this as an exact `get_note(title=...)` lookup key."
                        },
                        "summary": {"type": "string"},
                        "tags": {"type": "array", "items": {"type": "string"}},
                        "updated_at": {
                            "type": "string",
                            "format": "date-time",
                            "description": "Stored note update time: frontmatter `updated` (RFC3339) if present, otherwise Livesync file mtime, otherwise ingest time"
                        },
                        "link_count": {"type": "integer", "minimum": 0},
                        "backlink_count": {"type": "integer", "minimum": 0},
                        "search_score": {"type": ["number", "null"], "format": "float"},
                        "search_match_type": {
                            "anyOf": [
                                {"$ref": "#/components/schemas/MatchType"},
                                {"type": "null"}
                            ]
                        },
                        "search_snippet": {"type": ["string", "null"]},
                        "matched_chunk_id": {"type": ["string", "null"]},
                        "matched_heading_path": {"type": ["string", "null"]},
                        "matched_snippet": {"type": ["string", "null"]}
                    }
                },
                "RecentNotesResponse": {
                    "type": "object",
                    "required": ["notes", "total", "total_filtered"],
                    "properties": {
                        "notes": {
                            "type": "array",
                            "items": {"$ref": "#/components/schemas/RecentNoteSummary"}
                        },
                        "total": {"type": "integer", "minimum": 0},
                        "total_filtered": {"type": "integer", "minimum": 0}
                    }
                },
                "NoteSortField": {
                    "type": "string",
                    "enum": ["relevance", "updated_at", "created_at", "title"]
                },
                "SortOrder": {
                    "type": "string",
                    "enum": ["asc", "desc"]
                },
                "NoteTimeFilter": {
                    "type": "object",
                    "properties": {
                        "created_after": {"type": ["string", "null"], "format": "date-time"},
                        "created_before": {"type": ["string", "null"], "format": "date-time"},
                        "updated_after": {"type": ["string", "null"], "format": "date-time"},
                        "updated_before": {"type": ["string", "null"], "format": "date-time"}
                    }
                },
                "QueryNotesRequest": {
                    "type": "object",
                    "properties": {
                        "tags_all": {
                            "type": "array",
                            "items": {"type": "string"},
                            "default": [],
                            "description": "Require every listed exact case-sensitive tag; use this for tag browsing."
                        },
                        "tags_any": {
                            "type": "array",
                            "items": {"type": "string"},
                            "default": [],
                            "description": "Require at least one listed exact case-sensitive tag."
                        },
                        "tags_none": {"type": "array", "items": {"type": "string"}, "default": []},
                        "created_after": {"type": ["string", "null"], "format": "date-time"},
                        "created_before": {"type": ["string", "null"], "format": "date-time"},
                        "updated_after": {"type": ["string", "null"], "format": "date-time"},
                        "updated_before": {"type": ["string", "null"], "format": "date-time"},
                        "has_frontmatter": {"type": "array", "items": {"type": "string"}, "default": []},
                        "missing_frontmatter": {"type": "array", "items": {"type": "string"}, "default": []},
                        "path_prefix": {"type": ["string", "null"]},
                        "title_exact": {"type": ["string", "null"]},
                        "text_query": {"type": ["string", "null"]},
                        "search_mode": {
                            "anyOf": [
                                {"$ref": "#/components/schemas/SearchMode"},
                                {"type": "null"}
                            ]
                        },
                        "sort_by": {
                            "anyOf": [
                                {"$ref": "#/components/schemas/NoteSortField"},
                                {"type": "null"}
                            ]
                        },
                        "sort_order": {
                            "anyOf": [
                                {"$ref": "#/components/schemas/SortOrder"},
                                {"type": "null"}
                            ]
                        },
                        "limit": {"type": ["integer", "null"], "minimum": 0, "maximum": MAX_NOTE_LIST_LIMIT}
                    }
                },
                "NeighborDirection": {
                    "type": "string",
                    "enum": ["outgoing", "incoming", "both"]
                },
                "NeighborNode": {
                    "type": "object",
                    "required": ["id", "title", "depth", "link_context", "is_hub", "direction"],
                    "properties": {
                        "id": {"type": "string"},
                        "title": {"type": "string"},
                        "depth": {"type": "integer", "minimum": 0},
                        "link_context": {"type": "string"},
                        "is_hub": {"type": "boolean"},
                        "direction": {"$ref": "#/components/schemas/NeighborDirection"}
                    }
                },
                "NeighborEdge": {
                    "type": "object",
                    "required": ["from", "to"],
                    "properties": {
                        "from": {"type": "string"},
                        "to": {"type": "string"}
                    }
                },
                "NeighborsResponse": {
                    "type": "object",
                    "required": ["center", "direction", "nodes", "edges"],
                    "properties": {
                        "center": {"type": "string"},
                        "direction": {"$ref": "#/components/schemas/NeighborDirection"},
                        "nodes": {
                            "type": "array",
                            "items": {"$ref": "#/components/schemas/NeighborNode"}
                        },
                        "edges": {
                            "type": "array",
                            "items": {"$ref": "#/components/schemas/NeighborEdge"}
                        }
                    }
                },
                "GetNoteRequest": {
                    "type": "object",
                    "properties": {
                        "id": {"type": ["string", "null"]},
                        "title": {"type": ["string", "null"]}
                    }
                },
                "BacklinkEntry": {
                    "type": "object",
                    "required": ["id", "title", "context"],
                    "properties": {
                        "id": {"type": "string"},
                        "title": {"type": "string"},
                        "context": {"type": "string"}
                    }
                },
                "BacklinksResponse": {
                    "type": "object",
                    "required": ["target", "backlinks"],
                    "properties": {
                        "target": {"type": "string"},
                        "backlinks": {
                            "type": "array",
                            "items": {"$ref": "#/components/schemas/BacklinkEntry"}
                        }
                    }
                },
                "PathResponse": {
                    "type": "object",
                    "required": ["from", "to", "path", "length"],
                    "properties": {
                        "from": {"type": "string"},
                        "to": {"type": "string"},
                        "path": {
                            "type": ["array", "null"],
                            "items": {"type": "string"}
                        },
                        "length": {
                            "type": ["integer", "null"],
                            "minimum": 0
                        }
                    }
                },
                "TagCount": {
                    "type": "object",
                    "required": ["tag", "count"],
                    "properties": {
                        "tag": {"type": "string"},
                        "count": {"type": "integer", "minimum": 0}
                    }
                },
                "TagsResponse": {
                    "type": "object",
                    "required": ["tags"],
                    "properties": {
                        "tags": {
                            "type": "array",
                            "items": {"$ref": "#/components/schemas/TagCount"}
                        }
                    }
                },
                "NewNoteRequest": {
                    "type": "object",
                    "required": ["title"],
                    "properties": {
                        "title": {
                            "type": "string",
                            "description": "Human-readable note title. The server sanitizes this into the configured generated path; clients should not slugify it."
                        },
                        "content": {"type": "string"},
                        "tags": {"type": "array", "items": {"type": "string"}, "default": []},
                        "metadata": {"default": {}}
                    }
                },
                "NewNoteResponse": {
                    "type": "object",
                    "required": ["id", "status"],
                    "properties": {
                        "id": {"type": "string"},
                        "status": {"type": "string"}
                    }
                },
                "ContentPatchOperation": {
                    "oneOf": [
                        {
                            "type": "object",
                            "required": ["op", "old", "new"],
                            "properties": {
                                "op": {"type": "string", "const": "replace"},
                                "old": {
                                    "type": "string",
                                    "description": "Existing body text to replace; must occur exactly once."
                                },
                                "new": {"type": "string"}
                            }
                        },
                        {
                            "type": "object",
                            "required": ["op", "old"],
                            "properties": {
                                "op": {"type": "string", "const": "delete"},
                                "old": {
                                    "type": "string",
                                    "description": "Existing body text to delete; must occur exactly once."
                                }
                            }
                        },
                        {
                            "type": "object",
                            "required": ["op", "anchor", "text"],
                            "properties": {
                                "op": {"type": "string", "enum": ["insert_before", "insert_after"]},
                                "anchor": {
                                    "type": "string",
                                    "description": "Existing body text used as the insertion anchor; must occur exactly once."
                                },
                                "text": {"type": "string"}
                            }
                        },
                        {
                            "type": "object",
                            "required": ["op", "text"],
                            "properties": {
                                "op": {"type": "string", "enum": ["append", "prepend"]},
                                "text": {"type": "string"}
                            }
                        }
                    ]
                },
                "UpdateNoteRequest": {
                    "type": "object",
                    "properties": {
                        "content": {
                            "type": "string",
                            "description": "Replacement markdown body. Mutually exclusive with content_patch."
                        },
                        "content_patch": {
                            "type": "array",
                            "items": {"$ref": "#/components/schemas/ContentPatchOperation"},
                            "description": "Ordered exact-match body patch operations. Each operation is applied after the previous one; unmatched or ambiguous operations reject the whole update."
                        },
                        "tags": {"type": "array", "items": {"type": "string"}},
                        "metadata": {"type": "object"}
                    }
                },
                "UpdateNoteResponse": {
                    "type": "object",
                    "required": ["id", "status"],
                    "properties": {
                        "id": {"type": "string"},
                        "status": {"type": "string"}
                    }
                },
                "ContextFormat": {
                    "type": "string",
                    "enum": ["structured", "flat"]
                },
                "ContextRole": {
                    "type": "string",
                    "enum": ["seed", "context", "peripheral"]
                },
                "AssembleContextRequest": {
                    "type": "object",
                    "properties": {
                        "seeds": {"type": "array", "items": {"type": "string"}, "default": []},
                        "seed_query": {"type": ["string", "null"]},
                        "max_depth": {"type": ["integer", "null"], "minimum": 0},
                        "max_tokens": {"type": ["integer", "null"], "minimum": 0},
                        "include_graph_summary": {"type": ["boolean", "null"]},
                        "format": {"anyOf": [{"$ref": "#/components/schemas/ContextFormat"}, {"type": "null"}]}
                    }
                },
                "ContextNote": {
                    "type": "object",
                    "required": ["id", "title", "role"],
                    "properties": {
                        "id": {"type": "string"},
                        "title": {"type": "string"},
                        "content": {"type": ["string", "null"]},
                        "summary": {"type": ["string", "null"]},
                        "role": {"$ref": "#/components/schemas/ContextRole"},
                        "depth": {"type": ["integer", "null"], "minimum": 0},
                        "links_to": {
                            "type": ["array", "null"],
                            "items": {"type": "string"}
                        },
                        "linked_from": {
                            "type": ["array", "null"],
                            "items": {"type": "string"}
                        },
                        "is_hub": {"type": ["boolean", "null"]}
                    }
                },
                "AssembleContextResponse": {
                    "type": "object",
                    "required": ["notes", "token_estimate", "notes_included", "notes_summarized", "notes_excluded"],
                    "properties": {
                        "graph_summary": {"type": ["string", "null"]},
                        "notes": {
                            "type": "array",
                            "items": {"$ref": "#/components/schemas/ContextNote"}
                        },
                        "flat_context": {"type": ["string", "null"]},
                        "token_estimate": {"type": "integer", "minimum": 0},
                        "notes_included": {"type": "integer", "minimum": 0},
                        "notes_summarized": {"type": "integer", "minimum": 0},
                        "notes_excluded": {"type": "integer", "minimum": 0}
                    }
                },
                "IndexStats": {
                    "type": "object",
                    "required": ["total_notes", "total_links", "total_tags", "pending_embeddings", "quarantined_embeddings", "pending_chunk_embeddings", "quarantined_chunk_embeddings", "pending_chunks", "orphan_leaf_staging_count", "stale_file_aliases"],
                    "properties": {
                        "total_notes": {"type": "integer", "minimum": 0},
                        "total_links": {"type": "integer", "minimum": 0},
                        "total_tags": {"type": "integer", "minimum": 0},
                        "pending_embeddings": {"type": "integer", "minimum": 0},
                        "quarantined_embeddings": {"type": "integer", "minimum": 0},
                        "pending_chunk_embeddings": {"type": "integer", "minimum": 0},
                        "quarantined_chunk_embeddings": {"type": "integer", "minimum": 0},
                        "pending_chunks": {"type": "integer", "minimum": 0},
                        "orphan_leaf_staging_count": {"type": "integer", "minimum": 0},
                        "stale_file_aliases": {"type": "integer", "minimum": 0}
                    }
                },
                "EmbeddingStatus": {
                    "type": "object",
                    "required": ["mode", "model", "dimensions", "pending_notes", "quarantined_notes", "pending_chunks", "quarantined_chunks", "backend_state"],
                    "properties": {
                        "mode": {"type": "string"},
                        "model": {"type": "string"},
                        "dimensions": {"type": "integer", "minimum": 1},
                        "endpoint": {"type": ["string", "null"]},
                        "pending_notes": {"type": "integer", "minimum": 0},
                        "quarantined_notes": {"type": "integer", "minimum": 0},
                        "pending_chunks": {"type": "integer", "minimum": 0},
                        "quarantined_chunks": {"type": "integer", "minimum": 0},
                        "last_success_at": {"type": ["string", "null"], "format": "date-time"},
                        "last_error_at": {"type": ["string", "null"], "format": "date-time"},
                        "last_error": {"type": ["string", "null"]},
                        "backend_state": {"type": "string", "enum": ["disabled", "unknown", "available", "degraded"]}
                    }
                },
                "SyncStats": {
                    "type": "object",
                    "required": ["last_seq", "couchdb_current_seq", "behind_by", "last_sync_at"],
                    "properties": {
                        "last_seq": {"type": "string"},
                        "couchdb_current_seq": {"type": "string"},
                        "behind_by": {"type": "integer"},
                        "last_sync_at": {"type": "string", "format": "date-time"}
                    }
                },
                "ContextStats": {
                    "type": "object",
                    "required": ["accessible_notes", "filtered_notes"],
                    "properties": {
                        "accessible_notes": {"type": "integer", "minimum": 0},
                        "filtered_notes": {"type": "integer", "minimum": 0}
                    }
                },
                "ConfigReloadStatus": {
                    "type": "object",
                    "required": ["enabled", "generation", "sighup_enabled", "success_count", "failure_count"],
                    "properties": {
                        "enabled": {"type": "boolean"},
                        "path": {"type": ["string", "null"]},
                        "generation": {"type": "integer", "minimum": 0},
                        "poll_interval_seconds": {"type": ["integer", "null"], "minimum": 0},
                        "sighup_enabled": {"type": "boolean"},
                        "last_attempt_at": {"type": ["string", "null"], "format": "date-time"},
                        "last_success_at": {"type": ["string", "null"], "format": "date-time"},
                        "last_failure_at": {"type": ["string", "null"], "format": "date-time"},
                        "last_error": {"type": ["string", "null"]},
                        "success_count": {"type": "integer", "minimum": 0},
                        "failure_count": {"type": "integer", "minimum": 0}
                    }
                },
                "StatusResponse": {
                    "type": "object",
                    "required": ["status", "index", "embedding", "sync", "context_stats", "config_reload"],
                    "properties": {
                        "status": {"type": "string"},
                        "index": {"$ref": "#/components/schemas/IndexStats"},
                        "embedding": {"$ref": "#/components/schemas/EmbeddingStatus"},
                        "sync": {"$ref": "#/components/schemas/SyncStats"},
                        "context_stats": {
                            "type": "object",
                            "additionalProperties": {"$ref": "#/components/schemas/ContextStats"}
                        },
                        "config_reload": {"$ref": "#/components/schemas/ConfigReloadStatus"}
                    }
                }
            }
        },
        "paths": {
            "/api/v1/notes/{id}": {
                "get": {
                    "tags": ["vault_bridge"],
                    "summary": "Read a note by id or path",
                    "security": [{"api_key": []}],
                    "parameters": [
                        {
                            "name": "id",
                            "in": "path",
                            "required": true,
                            "schema": {"type": "string"},
                            "description": "Note identifier or path"
                        }
                    ],
                    "responses": {
                        "200": json_response("#/components/schemas/Note", "Note found"),
                        "401": json_response("#/components/schemas/ApiError", "Missing or invalid API key"),
                        "404": json_response("#/components/schemas/ApiError", "Note not found or not visible to this context")
                    }
                },
                "put": {
                    "tags": ["vault_bridge"],
                    "summary": "Update note content, body patch, tags, or metadata",
                    "security": [{"api_key": []}],
                    "parameters": [
                        {
                            "name": "id",
                            "in": "path",
                            "required": true,
                            "schema": {"type": "string"},
                            "description": "Note identifier or path"
                        }
                    ],
                    "requestBody": {
                        "required": true,
                        "content": {
                            "application/json": {
                                "schema": {"$ref": "#/components/schemas/UpdateNoteRequest"}
                            }
                        }
                    },
                    "responses": {
                        "200": json_response("#/components/schemas/UpdateNoteResponse", "Note updated"),
                        "400": json_response("#/components/schemas/ApiError", "Invalid update request or content patch"),
                        "401": json_response("#/components/schemas/ApiError", "Missing or invalid API key"),
                        "403": json_response("#/components/schemas/ApiError", "Edit denied by context policy"),
                        "404": json_response("#/components/schemas/ApiError", "Note not found or not editable in this context"),
                        "503": json_response("#/components/schemas/ApiError", "CouchDB write-through failed")
                    }
                }
            },
            "/api/v1/search": {
                "get": {
                    "tags": ["vault_bridge"],
                    "summary": "Search notes",
                    "security": [{"api_key": []}],
                    "parameters": [
                        query_param("q", true, json!({"type": "string"}), "Search query text"),
                        query_param("mode", false, json!({"$ref": "#/components/schemas/SearchMode"}), "Ranking mode"),
                        query_param("limit", false, json!({"type": "integer", "minimum": 1}), "Result limit; runtime clamps to 50")
                    ],
                    "responses": {
                        "200": json_response("#/components/schemas/SearchResponse", "Search results"),
                        "400": json_response("#/components/schemas/ApiError", "Missing or invalid query"),
                        "401": json_response("#/components/schemas/ApiError", "Missing or invalid API key")
                    }
                }
            },
            "/api/v1/notes/get": {
                "post": {
                    "tags": ["vault_bridge"],
                    "summary": "Resolve a note by ID or exact title",
                    "security": [{"api_key": []}],
                    "requestBody": {
                        "required": true,
                        "content": {
                            "application/json": {
                                "schema": {"$ref": "#/components/schemas/GetNoteRequest"}
                            }
                        }
                    },
                    "responses": {
                        "200": json_response("#/components/schemas/Note", "Resolved note"),
                        "400": json_response("#/components/schemas/ApiError", "Neither id nor title was provided"),
                        "401": json_response("#/components/schemas/ApiError", "Missing or invalid API key"),
                        "404": json_response("#/components/schemas/ApiError", "Matching note not found or not visible to this context")
                    }
                }
            },
            "/api/v1/notes/recent": {
                "get": {
                    "tags": ["vault_bridge"],
                    "summary": "List recent notes",
                    "security": [{"api_key": []}],
                    "parameters": [
                        query_param("since", false, json!({"type": "string", "format": "date-time"}), "RFC3339 lower bound for stored note update time (frontmatter `updated`, then Livesync mtime, then ingest time)"),
                        query_param("last_n_days", false, json!({"type": "integer"}), "Relative fallback window in days when since is absent; uses the same stored updated_at value"),
                        query_param("limit", false, json!({"type": "integer", "minimum": 1, "maximum": MAX_NOTE_LIST_LIMIT}), format!("Result limit; runtime clamps to {MAX_NOTE_LIST_LIMIT}"))
                    ],
                    "responses": {
                        "200": json_response("#/components/schemas/RecentNotesResponse", "Recent notes"),
                        "400": json_response("#/components/schemas/ApiError", "Invalid since or last_n_days"),
                        "401": json_response("#/components/schemas/ApiError", "Missing or invalid API key")
                    }
                }
            },
            "/api/v1/notes/query": {
                "post": {
                    "tags": ["vault_bridge"],
                    "summary": "Query notes by metadata, path, time, and optional ranked text search",
                    "security": [{"api_key": []}],
                    "requestBody": {
                        "required": true,
                        "content": {
                            "application/json": {
                                "schema": {"$ref": "#/components/schemas/QueryNotesRequest"}
                            }
                        }
                    },
                    "responses": {
                        "200": json_response("#/components/schemas/RecentNotesResponse", "Query results"),
                        "400": json_response("#/components/schemas/ApiError", "Invalid query payload"),
                        "401": json_response("#/components/schemas/ApiError", "Missing or invalid API key")
                    }
                }
            },
            "/api/v1/neighbors/{id}": {
                "get": {
                    "tags": ["vault_bridge"],
                    "summary": "Traverse note neighbors",
                    "security": [{"api_key": []}],
                    "parameters": [
                        {
                            "name": "id",
                            "in": "path",
                            "required": true,
                            "schema": {"type": "string"},
                            "description": "Center note identifier or path"
                        },
                        query_param("depth", false, json!({"type": "integer", "minimum": 1, "maximum": MAX_GRAPH_TRAVERSAL_DEPTH}), format!("Traversal depth; runtime clamps to 1..={MAX_GRAPH_TRAVERSAL_DEPTH}")),
                        query_param("direction", false, json!({"$ref": "#/components/schemas/NeighborDirection"}), "Traverse outgoing links, incoming links, or both; runtime defaults to both")
                    ],
                    "responses": {
                        "200": json_response("#/components/schemas/NeighborsResponse", "Neighbor graph"),
                        "400": json_response("#/components/schemas/ApiError", "Invalid traversal parameters"),
                        "401": json_response("#/components/schemas/ApiError", "Missing or invalid API key"),
                        "404": json_response("#/components/schemas/ApiError", "Center note not found or not visible to this context")
                    }
                }
            },
            "/api/v1/backlinks/{id}": {
                "get": {
                    "tags": ["vault_bridge"],
                    "summary": "List note backlinks",
                    "security": [{"api_key": []}],
                    "parameters": [
                        {
                            "name": "id",
                            "in": "path",
                            "required": true,
                            "schema": {"type": "string"},
                            "description": "Target note identifier or path"
                        }
                    ],
                    "responses": {
                        "200": json_response("#/components/schemas/BacklinksResponse", "Backlinks for the note"),
                        "401": json_response("#/components/schemas/ApiError", "Missing or invalid API key"),
                        "404": json_response("#/components/schemas/ApiError", "Target note not found or not visible to this context")
                    }
                }
            },
            "/api/v1/assemble-context": {
                "post": {
                    "tags": ["vault_bridge"],
                    "summary": "Assemble retrieval context",
                    "security": [{"api_key": []}],
                    "requestBody": {
                        "required": true,
                        "content": {
                            "application/json": {
                                "schema": {"$ref": "#/components/schemas/AssembleContextRequest"}
                            }
                        }
                    },
                    "responses": {
                        "200": json_response("#/components/schemas/AssembleContextResponse", "Assembled context bundle"),
                        "400": json_response("#/components/schemas/ApiError", "Invalid context assembly payload"),
                        "401": json_response("#/components/schemas/ApiError", "Missing or invalid API key")
                    }
                }
            },
            "/api/v1/graph/path": {
                "get": {
                    "tags": ["vault_bridge"],
                    "summary": "Find shortest graph path",
                    "security": [{"api_key": []}],
                    "parameters": [
                        query_param("from", true, json!({"type": "string"}), "Source note identifier or path"),
                        query_param("to", true, json!({"type": "string"}), "Target note identifier or path")
                    ],
                    "responses": {
                        "200": json_response("#/components/schemas/PathResponse", "Shortest path result"),
                        "400": json_response("#/components/schemas/ApiError", "Invalid graph path parameters"),
                        "401": json_response("#/components/schemas/ApiError", "Missing or invalid API key")
                    }
                }
            },
            "/api/v1/tags": {
                "get": {
                    "tags": ["vault_bridge"],
                    "summary": "List tags",
                    "security": [{"api_key": []}],
                    "parameters": [
                        query_param("created_after", false, json!({"type": "string", "format": "date-time"}), "Only include tags from notes created on or after this RFC3339 timestamp"),
                        query_param("created_before", false, json!({"type": "string", "format": "date-time"}), "Only include tags from notes created on or before this RFC3339 timestamp"),
                        query_param("updated_after", false, json!({"type": "string", "format": "date-time"}), "Only include tags from notes updated on or after this RFC3339 timestamp"),
                        query_param("updated_before", false, json!({"type": "string", "format": "date-time"}), "Only include tags from notes updated on or before this RFC3339 timestamp")
                    ],
                    "responses": {
                        "200": json_response("#/components/schemas/TagsResponse", "Tag counts"),
                        "400": json_response("#/components/schemas/ApiError", "Invalid time filter"),
                        "401": json_response("#/components/schemas/ApiError", "Missing or invalid API key")
                    }
                }
            },
            "/api/v1/notes": {
                "post": {
                    "tags": ["vault_bridge"],
                    "summary": "Create a new note",
                    "security": [{"api_key": []}],
                    "requestBody": {
                        "required": true,
                        "content": {
                            "application/json": {
                                "schema": {"$ref": "#/components/schemas/NewNoteRequest"}
                            }
                        }
                    },
                    "responses": {
                        "200": json_response("#/components/schemas/NewNoteResponse", "Note created"),
                        "400": json_response("#/components/schemas/ApiError", "Invalid note payload"),
                        "401": json_response("#/components/schemas/ApiError", "Missing or invalid API key"),
                        "403": json_response("#/components/schemas/ApiError", "Write path not allowed for context"),
                        "409": json_response("#/components/schemas/ApiError", "A note with the generated path already exists"),
                        "503": json_response("#/components/schemas/ApiError", "CouchDB write-through failed")
                    }
                }
            },
            "/api/v1/status": {
                "get": {
                    "tags": ["vault_bridge"],
                    "summary": "Read vault status",
                    "security": [{"api_key": []}],
                    "responses": {
                        "200": json_response("#/components/schemas/StatusResponse", "Vault sync and index status"),
                        "401": json_response("#/components/schemas/ApiError", "Missing or invalid API key")
                    }
                }
            },
            "/api/v1/metrics": {
                "get": {
                    "tags": ["vault_bridge"],
                    "summary": "Read Prometheus metrics",
                    "description": "Accepts any configured file-backed REST API token.",
                    "security": [{"api_key": []}],
                    "responses": {
                        "200": {
                            "description": "Prometheus metrics payload",
                            "content": {
                                "text/plain": {
                                    "schema": {"type": "string"}
                                }
                            }
                        },
                        "401": json_response("#/components/schemas/ApiError", "Missing or invalid API key")
                    }
                }
            }
        }
    })
}

fn json_response(schema_ref: &str, description: &str) -> Value {
    json!({
        "description": description,
        "content": {
            "application/json": {
                "schema": {
                    "$ref": schema_ref
                }
            }
        }
    })
}

fn query_param(name: &str, required: bool, schema: Value, description: impl Into<String>) -> Value {
    json!({
        "name": name,
        "in": "query",
        "required": required,
        "schema": schema,
        "description": description.into()
    })
}

const SWAGGER_UI_HTML: &str = r#"<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>Vault Bridge API Docs</title>
    <link rel="stylesheet" href="https://unpkg.com/swagger-ui-dist@5/swagger-ui.css">
    <style>
      body { margin: 0; background: #faf7f2; }
      .topbar { display: none; }
      .fallback {
        font-family: system-ui, sans-serif;
        max-width: 760px;
        margin: 32px auto;
        padding: 0 20px;
        color: #2f241f;
      }
      .fallback code {
        background: #efe6db;
        padding: 2px 6px;
        border-radius: 4px;
      }
    </style>
  </head>
  <body>
    <div id="swagger-ui"></div>
    <noscript>
      <div class="fallback">
        <h1>Vault Bridge API Docs</h1>
        <p>JavaScript is required to render Swagger UI.</p>
        <p>Raw OpenAPI document: <code>/api-doc/openapi.json</code></p>
      </div>
    </noscript>
    <script src="https://unpkg.com/swagger-ui-dist@5/swagger-ui-bundle.js" crossorigin></script>
    <script>
      window.onload = function() {
        if (!window.SwaggerUIBundle) {
          document.body.innerHTML = '<div class="fallback"><h1>Vault Bridge API Docs</h1><p>Swagger UI assets failed to load. Raw OpenAPI document: <code>/api-doc/openapi.json</code></p></div>';
          return;
        }
        window.ui = SwaggerUIBundle({
          url: '/api-doc/openapi.json',
          dom_id: '#swagger-ui',
          deepLinking: true,
          displayRequestDuration: true,
          persistAuthorization: true,
        });
      };
    </script>
  </body>
</html>
"#;

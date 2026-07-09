#![recursion_limit = "512"]

pub mod api;
mod api_docs;
pub mod authorization;
pub mod base_query;
pub mod config;
pub mod context;
pub mod couchdb;
pub mod encryption;
mod error_metadata;
pub mod graph;
pub mod livesync;
pub mod markdown;
pub mod mcp;
pub mod model;
pub mod new_note;
pub mod persistence;
pub mod runtime_config;
pub mod search;
pub mod service;
pub mod store;
pub mod summary;
pub mod workers;

pub use api::{AppState, app_router};
pub use model::{Note, NoteId, UnscopedNote};

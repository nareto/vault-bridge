use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, PartialOrd, Ord)]
#[serde(transparent)]
pub struct NoteId(String);

impl NoteId {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for NoteId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl From<&str> for NoteId {
    fn from(value: &str) -> Self {
        Self::new(value)
    }
}

impl From<String> for NoteId {
    fn from(value: String) -> Self {
        Self::new(value)
    }
}

pub struct UnscopedNote {
    pub id: NoteId,
    pub path: String,
    pub title: String,
    pub heading_title: Option<String>,
    pub content: String,
    pub summary: String,
    pub frontmatter: serde_json::Value,
    pub links: Vec<NoteId>,
    pub backlinks: Vec<NoteId>,
    pub tags: Vec<String>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize)]
pub struct Note {
    pub id: NoteId,
    pub path: String,
    pub title: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub heading_title: Option<String>,
    pub content: String,
    pub summary: String,
    pub frontmatter: serde_json::Value,
    pub links: Vec<NoteId>,
    pub backlinks: Vec<NoteId>,
    pub tags: Vec<String>,
    pub updated_at: DateTime<Utc>,
}

impl UnscopedNote {
    pub fn into_note(self) -> Note {
        Note {
            id: self.id.clone(),
            path: self.path,
            title: self.title,
            heading_title: self.heading_title,
            content: self.content,
            summary: self.summary,
            frontmatter: self.frontmatter,
            links: self.links,
            backlinks: self.backlinks,
            tags: self.tags,
            updated_at: self.updated_at,
        }
    }
}

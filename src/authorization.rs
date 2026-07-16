use std::collections::BTreeMap;

use chrono::{DateTime, Duration, Utc};
use regex::Regex;
use serde::{Deserialize, Serialize};
use serde_json::Value;

pub const OWNER_METADATA_KEY: &str = "created_by";
pub const LEGACY_OWNER_METADATA_KEY: &str = "vault_bridge";
pub const OWNER_METADATA_FIELD: &str = "created_by";

pub type AuthorizationConfig = BTreeMap<String, AccessPolicy>;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, PartialOrd, Ord)]
#[serde(transparent)]
pub struct ContextName(String);

impl ContextName {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for ContextName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl From<&str> for ContextName {
    fn from(value: &str) -> Self {
        Self::new(value)
    }
}

impl From<String> for ContextName {
    fn from(value: String) -> Self {
        Self::new(value)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct AccessPolicy {
    pub read: Vec<AccessRule>,
    pub create: Vec<AccessRule>,
    pub edit: Vec<AccessRule>,
}

impl AccessPolicy {
    pub fn default_agent() -> Self {
        Self {
            read: vec![AccessRule::allow(AccessMatcher::allow_all())],
            create: vec![AccessRule {
                allow: Some(AccessMatcher::allow_all()),
                deny: None,
                add_tags: vec!["ai-created".to_string()],
                set_owner: true,
                preserve_tags: Vec::new(),
            }],
            edit: vec![
                AccessRule {
                    allow: Some(AccessMatcher {
                        tags_any: vec!["ai-editable".to_string()],
                        ..AccessMatcher::default()
                    }),
                    deny: None,
                    preserve_tags: vec!["ai-editable".to_string()],
                    ..AccessRule::default()
                },
                AccessRule {
                    allow: Some(AccessMatcher {
                        tags_all: vec!["ai-created".to_string()],
                        owner: Some(true),
                        ..AccessMatcher::default()
                    }),
                    deny: None,
                    preserve_tags: vec!["ai-created".to_string()],
                    ..AccessRule::default()
                },
            ],
        }
    }

    pub fn admin() -> Self {
        Self {
            read: vec![AccessRule::allow(AccessMatcher::allow_all())],
            create: vec![AccessRule {
                allow: Some(AccessMatcher::allow_all()),
                deny: None,
                add_tags: vec!["ai-created".to_string()],
                set_owner: true,
                preserve_tags: Vec::new(),
            }],
            edit: vec![AccessRule::allow(AccessMatcher::allow_all())],
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct AccessRule {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub allow: Option<AccessMatcher>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deny: Option<AccessMatcher>,
    pub add_tags: Vec<String>,
    pub set_owner: bool,
    pub preserve_tags: Vec<String>,
}

impl AccessRule {
    pub fn allow(matcher: AccessMatcher) -> Self {
        Self {
            allow: Some(matcher),
            ..Self::default()
        }
    }

    pub fn deny(matcher: AccessMatcher) -> Self {
        Self {
            deny: Some(matcher),
            ..Self::default()
        }
    }

    pub fn matcher(&self) -> Option<&AccessMatcher> {
        self.allow.as_ref().or(self.deny.as_ref())
    }

    pub fn is_allow(&self) -> bool {
        self.allow.is_some() && self.deny.is_none()
    }

    pub fn is_deny(&self) -> bool {
        self.deny.is_some() && self.allow.is_none()
    }

    pub fn has_mutations(&self) -> bool {
        !self.add_tags.is_empty() || self.set_owner || !self.preserve_tags.is_empty()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct AccessMatcher {
    pub default: Option<bool>,
    pub path_prefix: Option<String>,
    pub path_prefixes: Vec<String>,
    pub tags_all: Vec<String>,
    pub tags_any: Vec<String>,
    pub tags_none: Vec<String>,
    pub created_after: Option<DateTime<Utc>>,
    pub created_before: Option<DateTime<Utc>>,
    pub updated_after: Option<DateTime<Utc>>,
    pub updated_before: Option<DateTime<Utc>>,
    pub created_within_seconds: Option<i64>,
    pub updated_within_seconds: Option<i64>,
    pub owner: Option<bool>,
    pub title_regex: Option<String>,
}

impl AccessMatcher {
    pub fn allow_all() -> Self {
        Self {
            default: Some(true),
            ..Self::default()
        }
    }

    pub fn matches(&self, note: &PolicyNote, principal: &str, now: DateTime<Utc>) -> bool {
        if self.default.unwrap_or(false) {
            return true;
        }

        if let Some(prefix) = self.path_prefix.as_deref()
            && !path_has_prefix(&note.path, prefix)
        {
            return false;
        }
        if !self.path_prefixes.is_empty()
            && !self
                .path_prefixes
                .iter()
                .any(|prefix| path_has_prefix(&note.path, prefix))
        {
            return false;
        }
        if !self.tags_all.is_empty()
            && !self.tags_all.iter().all(|tag| {
                note.tags
                    .iter()
                    .any(|candidate| tag_matches(candidate, tag))
            })
        {
            return false;
        }
        if !self.tags_any.is_empty()
            && !self.tags_any.iter().any(|tag| {
                note.tags
                    .iter()
                    .any(|candidate| tag_matches(candidate, tag))
            })
        {
            return false;
        }
        if self.tags_none.iter().any(|tag| {
            note.tags
                .iter()
                .any(|candidate| tag_matches(candidate, tag))
        }) {
            return false;
        }
        if let Some(created_after) = self.created_after
            && note
                .created_at
                .is_none_or(|created| created < created_after)
        {
            return false;
        }
        if let Some(created_before) = self.created_before
            && note
                .created_at
                .is_none_or(|created| created > created_before)
        {
            return false;
        }
        if let Some(updated_after) = self.updated_after
            && note.updated_at < updated_after
        {
            return false;
        }
        if let Some(updated_before) = self.updated_before
            && note.updated_at > updated_before
        {
            return false;
        }
        if let Some(seconds) = self.created_within_seconds {
            let Some(created_at) = note.created_at else {
                return false;
            };
            if now - created_at > Duration::seconds(seconds.max(0)) {
                return false;
            }
        }
        if let Some(seconds) = self.updated_within_seconds
            && now - note.updated_at > Duration::seconds(seconds.max(0))
        {
            return false;
        }
        if let Some(owner_required) = self.owner {
            let owned_by_principal = note.owner.as_deref() == Some(principal);
            if owned_by_principal != owner_required {
                return false;
            }
        }
        if let Some(pattern) = self.title_regex.as_deref() {
            let Ok(regex) = Regex::new(pattern) else {
                return false;
            };
            if !regex.is_match(&note.title) {
                return false;
            }
        }

        true
    }
}

#[derive(Debug, Clone)]
pub struct AuthContext {
    pub context: ContextName,
    pub principal: String,
}

impl AuthContext {
    pub fn new(context: ContextName, principal: String) -> Self {
        Self { context, principal }
    }
}

#[derive(Debug, Clone)]
pub struct PolicyNote {
    pub path: String,
    pub title: String,
    pub tags: Vec<String>,
    pub created_at: Option<DateTime<Utc>>,
    pub updated_at: DateTime<Utc>,
    pub owner: Option<String>,
}

pub fn owner_from_frontmatter(frontmatter: &Value) -> Option<&str> {
    owner_from_value(frontmatter.get(OWNER_METADATA_KEY)).or_else(|| {
        frontmatter
            .get(LEGACY_OWNER_METADATA_KEY)
            .and_then(Value::as_object)
            .and_then(|metadata| owner_from_value(metadata.get(OWNER_METADATA_FIELD)))
    })
}

pub fn set_owner_metadata(metadata: &mut Value, principal: &str) {
    let principal = principal.trim();
    if principal.is_empty() {
        return;
    }

    if !metadata.is_object() {
        *metadata = Value::Object(serde_json::Map::new());
    }
    let Some(map) = metadata.as_object_mut() else {
        return;
    };
    map.insert(
        OWNER_METADATA_KEY.to_string(),
        Value::String(principal.to_string()),
    );

    let remove_legacy_key = if let Some(legacy_value) = map.get_mut(LEGACY_OWNER_METADATA_KEY)
        && let Some(legacy_map) = legacy_value.as_object_mut()
    {
        legacy_map.remove(OWNER_METADATA_FIELD);
        legacy_map.is_empty()
    } else {
        false
    };
    if remove_legacy_key {
        map.remove(LEGACY_OWNER_METADATA_KEY);
    }
}

fn owner_from_value(value: Option<&Value>) -> Option<&str> {
    value
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
}

pub fn add_unique_tag(tags: &mut Vec<String>, tag: &str) {
    let tag = normalize_tag(tag);
    if tag.is_empty() {
        return;
    }
    if !tags.iter().any(|candidate| tag_matches(candidate, &tag)) {
        tags.push(tag);
    }
}

pub fn normalize_tag(tag: &str) -> String {
    tag.trim().trim_start_matches('#').trim().to_string()
}

fn tag_matches(candidate: &str, expected: &str) -> bool {
    normalize_tag(candidate).eq_ignore_ascii_case(&normalize_tag(expected))
}

fn path_has_prefix(path: &str, prefix: &str) -> bool {
    let path = path.trim_start_matches('/');
    let prefix = prefix.trim().trim_start_matches('/');
    if prefix.is_empty() {
        return true;
    }
    if prefix.ends_with('/') {
        path.starts_with(prefix)
    } else {
        path == prefix || path.starts_with(&format!("{prefix}/"))
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{LEGACY_OWNER_METADATA_KEY, OWNER_METADATA_KEY};
    use super::{owner_from_frontmatter, set_owner_metadata};

    #[test]
    fn owner_from_frontmatter_reads_top_level_created_by() {
        let frontmatter = json!({
            "created_by": " mcp_token:example_agent "
        });

        assert_eq!(
            owner_from_frontmatter(&frontmatter),
            Some("mcp_token:example_agent")
        );
    }

    #[test]
    fn owner_from_frontmatter_reads_legacy_nested_created_by() {
        let frontmatter = json!({
            "vault_bridge": {
                "created_by": "mcp_token:example_agent"
            }
        });

        assert_eq!(
            owner_from_frontmatter(&frontmatter),
            Some("mcp_token:example_agent")
        );
    }

    #[test]
    fn owner_from_frontmatter_prefers_top_level_created_by() {
        let frontmatter = json!({
            "created_by": "mcp_token:current",
            "vault_bridge": {
                "created_by": "mcp_token:legacy"
            }
        });

        assert_eq!(
            owner_from_frontmatter(&frontmatter),
            Some("mcp_token:current")
        );
    }

    #[test]
    fn set_owner_metadata_writes_top_level_created_by() {
        let mut metadata = json!({});

        set_owner_metadata(&mut metadata, "mcp_token:example_agent");

        assert_eq!(
            metadata[OWNER_METADATA_KEY],
            json!("mcp_token:example_agent")
        );
        assert!(metadata.get(LEGACY_OWNER_METADATA_KEY).is_none());
    }

    #[test]
    fn set_owner_metadata_removes_empty_legacy_owner_metadata() {
        let mut metadata = json!({
            "vault_bridge": {
                "created_by": "mcp_token:legacy"
            }
        });

        set_owner_metadata(&mut metadata, "mcp_token:example_agent");

        assert_eq!(
            metadata[OWNER_METADATA_KEY],
            json!("mcp_token:example_agent")
        );
        assert!(metadata.get(LEGACY_OWNER_METADATA_KEY).is_none());
    }

    #[test]
    fn set_owner_metadata_preserves_other_legacy_metadata() {
        let mut metadata = json!({
            "vault_bridge": {
                "created_by": "mcp_token:legacy",
                "other": "value"
            }
        });

        set_owner_metadata(&mut metadata, "mcp_token:example_agent");

        assert_eq!(
            metadata[OWNER_METADATA_KEY],
            json!("mcp_token:example_agent")
        );
        assert_eq!(
            metadata[LEGACY_OWNER_METADATA_KEY],
            json!({"other": "value"})
        );
    }
}

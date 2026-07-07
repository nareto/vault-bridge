use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use slug::slugify;
use thiserror::Error;

use crate::authorization::{add_unique_tag, set_owner_metadata};
use crate::config::NewNoteConfig;
use crate::markdown::{extract_frontmatter_tags, parse_frontmatter};
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NewNotePathSettings {
    pub base_path: String,
    pub path_template: String,
    pub date_format: String,
    pub max_title_slug_length: usize,
}

impl Default for NewNotePathSettings {
    fn default() -> Self {
        Self {
            base_path: "11New".to_string(),
            path_template: "{base}/{date}-{slug}.md".to_string(),
            date_format: "%Y-%m-%d".to_string(),
            max_title_slug_length: 60,
        }
    }
}

impl From<&NewNoteConfig> for NewNotePathSettings {
    fn from(value: &NewNoteConfig) -> Self {
        Self {
            base_path: value.base_path.clone(),
            path_template: value.path_template.clone(),
            date_format: value.date_format.clone(),
            max_title_slug_length: value.max_title_slug_length,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum NewNoteFileType {
    Md,
    Base,
}

impl Default for NewNoteFileType {
    fn default() -> Self {
        Self::Md
    }
}

impl NewNoteFileType {
    pub fn extension(self) -> &'static str {
        match self {
            Self::Md => "md",
            Self::Base => "base",
        }
    }

    pub fn is_markdown(self) -> bool {
        matches!(self, Self::Md)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NewNoteRequest {
    pub title: String,
    pub content: String,
    #[serde(default)]
    pub template_id: Option<String>,
    #[serde(default)]
    pub file_type: NewNoteFileType,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpdateNoteRequest {
    #[serde(default)]
    pub content: Option<String>,
    #[serde(default)]
    pub content_patch: Option<Vec<ContentPatchOperation>>,
    #[serde(default)]
    pub tags: Option<Vec<String>>,
    #[serde(default)]
    pub metadata: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum ContentPatchOperation {
    Replace { old: String, new: String },
    Delete { old: String },
    InsertBefore { anchor: String, text: String },
    InsertAfter { anchor: String, text: String },
    Append { text: String },
    Prepend { text: String },
}

#[derive(Debug, Error)]
pub enum WriteError {
    #[error("path '{path}' is not allowed under configured new note path")]
    PathNotAllowed { path: String },
    #[error("title cannot be empty")]
    EmptyTitle,
    #[error("note already exists: {path}")]
    AlreadyExists { path: String },
    #[error("note not found: {path}")]
    NotFound { path: String },
    #[error("invalid create: {reason}")]
    InvalidCreate { reason: String },
    #[error("template not found or not visible: {path}")]
    TemplateNotFound { path: String },
    #[error("invalid update: {reason}")]
    InvalidUpdate { reason: String },
    #[error("{operation} denied for '{path}': {reason}")]
    PolicyDenied {
        operation: &'static str,
        path: String,
        reason: String,
    },
}

impl NewNoteRequest {
    pub fn generate_path(&self) -> String {
        self.generate_path_at(Utc::now())
    }

    pub fn generate_path_at(&self, now: DateTime<Utc>) -> String {
        self.generate_path_with_settings_at(now, &NewNotePathSettings::default())
    }

    pub fn generate_path_with_settings(&self, settings: &NewNotePathSettings) -> String {
        self.generate_path_with_settings_at(Utc::now(), settings)
    }

    pub fn generate_path_with_settings_at(
        &self,
        now: DateTime<Utc>,
        settings: &NewNotePathSettings,
    ) -> String {
        let base = normalize_base_path(&settings.base_path);
        let template = normalized_template(&settings.path_template);
        let date_format = if settings.date_format.trim().is_empty() {
            "%Y-%m-%d"
        } else {
            settings.date_format.as_str()
        };
        let date = now.format(date_format).to_string();
        let slug = slug_with_limit(&self.title, settings.max_title_slug_length);
        let title = title_segment_with_limit(&self.title, settings.max_title_slug_length);

        let mut rendered = template
            .replace("{base}", &base)
            .replace("{date}", &date)
            .replace("{title}", &title)
            .replace("{slug}", &slug);
        rendered = rendered.replace('\\', "/");
        let mut rendered = rendered.trim().trim_start_matches('/').to_string();

        if rendered.is_empty() {
            rendered = format!("{base}/{date}-{slug}");
        }
        with_file_type_extension(&rendered, self.file_type)
    }

    pub fn validate_write(&self) -> Result<String, WriteError> {
        self.validate_write_at(Utc::now())
    }

    pub fn validate_write_at(&self, now: DateTime<Utc>) -> Result<String, WriteError> {
        self.validate_write_with_settings_at(now, &NewNotePathSettings::default())
    }

    pub fn validate_write_with_settings(
        &self,
        settings: &NewNotePathSettings,
    ) -> Result<String, WriteError> {
        self.validate_write_with_settings_at(Utc::now(), settings)
    }

    pub fn validate_write_with_settings_at(
        &self,
        now: DateTime<Utc>,
        settings: &NewNotePathSettings,
    ) -> Result<String, WriteError> {
        if self.title.trim().is_empty() {
            return Err(WriteError::EmptyTitle);
        }

        let path = self.generate_path_with_settings_at(now, settings);
        let configured_prefix = format!("{}/", normalize_base_path(&settings.base_path));
        let allowed =
            path.starts_with(&configured_prefix) && is_safe_new_file_path(&path, self.file_type);

        if !allowed {
            return Err(WriteError::PathNotAllowed { path });
        }

        Ok(path)
    }

    pub fn apply_markdown_create_metadata(
        &mut self,
        created_at: DateTime<Utc>,
        add_tags: &[String],
        owner_principal: Option<&str>,
    ) -> Result<(), WriteError> {
        if !self.file_type.is_markdown() {
            return Ok(());
        }

        let (frontmatter, body) = parse_frontmatter(&self.content);
        if !frontmatter.is_object() {
            return Err(WriteError::InvalidCreate {
                reason: "markdown frontmatter must be a YAML mapping".to_string(),
            });
        }

        let mut frontmatter = frontmatter;
        let Some(map) = frontmatter.as_object_mut() else {
            return Err(WriteError::InvalidCreate {
                reason: "markdown frontmatter must be a YAML mapping".to_string(),
            });
        };
        map.insert(
            "created".to_string(),
            Value::String(created_at.to_rfc3339()),
        );

        if !add_tags.is_empty() {
            add_tags_to_frontmatter(&mut frontmatter, add_tags);
        }
        if let Some(principal) = owner_principal {
            set_owner_metadata(&mut frontmatter, principal);
        }

        self.content = markdown_with_frontmatter(&frontmatter, &body)?;
        Ok(())
    }
}

impl UpdateNoteRequest {
    /// Rebuild the full markdown from the existing note's frontmatter and body,
    /// applying any overrides from this update request.
    pub fn rebuild_markdown(
        &self,
        existing_frontmatter: &Value,
        existing_body: &str,
        existing_tags: &[String],
        now: DateTime<Utc>,
    ) -> Result<String, WriteError> {
        if self.content.is_some() && self.content_patch.is_some() {
            return Err(WriteError::InvalidUpdate {
                reason: "content and content_patch are mutually exclusive".to_string(),
            });
        }

        let mut fm = existing_frontmatter
            .as_object()
            .cloned()
            .unwrap_or_default();

        // Update the `updated` timestamp in frontmatter.
        fm.insert("updated".to_string(), Value::String(now.to_rfc3339()));

        // Apply tag override if provided.
        if let Some(tags) = &self.tags {
            fm.insert(
                "tags".to_string(),
                Value::Array(tags.iter().map(|t| Value::String(t.clone())).collect()),
            );
        }

        // Apply metadata overrides if provided.
        if let Some(metadata) = &self.metadata {
            if let Some(metadata_obj) = metadata.as_object() {
                for (k, v) in metadata_obj {
                    if matches!(k.as_str(), "tags" | "updated") {
                        continue;
                    }
                    fm.insert(k.clone(), v.clone());
                }
            }
        }

        // Rebuild YAML frontmatter as serde_yaml::Mapping to preserve key order.
        let yaml_mapping: serde_yaml::Mapping = fm
            .iter()
            .filter_map(|(k, v)| {
                let yaml_val = serde_yaml::to_value(v).ok()?;
                Some((serde_yaml::Value::String(k.clone()), yaml_val))
            })
            .collect();

        let yaml = serde_yaml::to_string(&yaml_mapping).unwrap_or_default();
        let yaml = yaml.strip_prefix("---\n").unwrap_or(&yaml);

        let body = match (&self.content, &self.content_patch) {
            (Some(content), None) => content.clone(),
            (None, Some(patch)) => apply_content_patch(existing_body, patch)?,
            (None, None) => existing_body.to_string(),
            (Some(_), Some(_)) => unreachable!("mutual exclusion checked above"),
        };
        let body = body.trim();

        // If tags were not explicitly overridden, keep the existing tags in frontmatter
        // (they're already there from the existing frontmatter we started with).
        // If the existing frontmatter had no tags but the note had parsed tags,
        // preserve them.
        if self.tags.is_none() && !existing_tags.is_empty() && !fm.contains_key("tags") {
            // This case shouldn't happen since existing_frontmatter should have tags,
            // but as a safety net we leave the frontmatter as-is.
        }

        Ok(format!("---\n{}---\n\n{}\n", yaml, body))
    }
}

fn apply_content_patch(
    existing_body: &str,
    patch: &[ContentPatchOperation],
) -> Result<String, WriteError> {
    if patch.is_empty() {
        return Err(invalid_patch(
            "content_patch must contain at least one operation",
        ));
    }

    let mut body = existing_body.to_string();
    for (index, operation) in patch.iter().enumerate() {
        apply_content_patch_operation(&mut body, operation)
            .map_err(|reason| invalid_patch(format!("operation {index}: {reason}")))?;
    }

    Ok(body)
}

fn apply_content_patch_operation(
    body: &mut String,
    operation: &ContentPatchOperation,
) -> Result<(), String> {
    match operation {
        ContentPatchOperation::Replace { old, new } => {
            let range = unique_match_range(body, old, "old")?;
            body.replace_range(range, new);
        }
        ContentPatchOperation::Delete { old } => {
            let range = unique_match_range(body, old, "old")?;
            body.replace_range(range, "");
        }
        ContentPatchOperation::InsertBefore { anchor, text } => {
            let range = unique_match_range(body, anchor, "anchor")?;
            body.insert_str(range.start, text);
        }
        ContentPatchOperation::InsertAfter { anchor, text } => {
            let range = unique_match_range(body, anchor, "anchor")?;
            body.insert_str(range.end, text);
        }
        ContentPatchOperation::Append { text } => body.push_str(text),
        ContentPatchOperation::Prepend { text } => body.insert_str(0, text),
    }

    Ok(())
}

fn unique_match_range(
    body: &str,
    needle: &str,
    field_name: &str,
) -> Result<std::ops::Range<usize>, String> {
    if needle.is_empty() {
        return Err(format!("{field_name} must not be empty"));
    }

    let mut matches = body.match_indices(needle);
    let Some((start, _)) = matches.next() else {
        return Err(format!("{field_name} text was not found"));
    };
    if matches.next().is_some() {
        return Err(format!("{field_name} text matched more than once"));
    }

    Ok(start..start + needle.len())
}

fn invalid_patch(reason: impl Into<String>) -> WriteError {
    WriteError::InvalidUpdate {
        reason: format!("content_patch: {}", reason.into()),
    }
}

fn markdown_with_frontmatter(frontmatter: &Value, body: &str) -> Result<String, WriteError> {
    let Some(frontmatter_obj) = frontmatter.as_object() else {
        return Err(WriteError::InvalidCreate {
            reason: "markdown frontmatter must be a YAML mapping".to_string(),
        });
    };
    let yaml_mapping: serde_yaml::Mapping = frontmatter_obj
        .iter()
        .filter_map(|(k, v)| {
            let yaml_value = serde_yaml::to_value(v).ok()?;
            Some((serde_yaml::Value::String(k.clone()), yaml_value))
        })
        .collect();
    let yaml = serde_yaml::to_string(&yaml_mapping).unwrap_or_default();
    let yaml = yaml.strip_prefix("---\n").unwrap_or(&yaml);
    let mut markdown = format!("---\n{}---\n\n{}", yaml, body);
    if !markdown.ends_with('\n') {
        markdown.push('\n');
    }
    Ok(markdown)
}

fn add_tags_to_frontmatter(frontmatter: &mut Value, tags: &[String]) {
    let mut merged_tags = extract_frontmatter_tags(frontmatter);
    for tag in tags {
        add_unique_tag(&mut merged_tags, tag);
    }
    merged_tags.sort();
    merged_tags.dedup_by(|a, b| a.eq_ignore_ascii_case(b));

    if let Some(map) = frontmatter.as_object_mut() {
        map.insert(
            "tags".to_string(),
            Value::Array(merged_tags.into_iter().map(Value::String).collect()),
        );
    }
}

fn slug_with_limit(title: &str, max_len: usize) -> String {
    let mut slug = slugify(title);
    if slug.is_empty() {
        slug = "note".to_string();
    }

    let max_len = max_len.max(1);
    if slug.len() > max_len {
        slug.truncate(max_len);
        slug = slug.trim_end_matches('-').to_string();
        if slug.is_empty() {
            slug = "note".to_string();
        }
    }

    slug
}

fn title_segment_with_limit(title: &str, max_len: usize) -> String {
    let mut segment = String::new();
    let mut previous_was_space = false;
    for ch in title.trim().chars() {
        let ch = if is_unsafe_path_char(ch) || ch == '/' || ch == '\\' || ch.is_whitespace() {
            ' '
        } else {
            ch
        };

        if ch == ' ' {
            if !segment.is_empty() && !previous_was_space {
                segment.push(ch);
            }
            previous_was_space = true;
        } else {
            segment.push(ch);
            previous_was_space = false;
        }
    }

    segment = trim_filename_segment(segment);
    if segment.is_empty() || is_reserved_windows_filename(&segment) {
        segment = "Untitled".to_string();
    }

    let segment = truncate_filename_segment(&segment, max_len);
    if segment.is_empty() || is_reserved_windows_filename(&segment) {
        truncate_filename_segment("Untitled", max_len)
    } else {
        segment
    }
}

fn truncate_filename_segment(segment: &str, max_len: usize) -> String {
    let max_len = max_len.max(1);
    let mut truncated = String::new();
    for ch in segment.chars() {
        if truncated.len() + ch.len_utf8() > max_len {
            break;
        }
        truncated.push(ch);
    }
    let truncated = trim_filename_segment(truncated);
    if truncated.is_empty() {
        "Untitled".chars().take(max_len).collect()
    } else {
        truncated
    }
}

fn trim_filename_segment(segment: String) -> String {
    segment
        .trim_matches(|ch: char| ch == ' ' || ch == '.')
        .to_string()
}

fn with_file_type_extension(path: &str, file_type: NewNoteFileType) -> String {
    let extension = format!(".{}", file_type.extension());
    let lower = path.to_ascii_lowercase();
    for existing_extension in [".md", ".base"] {
        if lower.ends_with(existing_extension) {
            return format!(
                "{}{}",
                &path[..path.len() - existing_extension.len()],
                extension
            );
        }
    }
    format!("{path}{extension}")
}

fn is_safe_new_file_path(path: &str, file_type: NewNoteFileType) -> bool {
    if path.starts_with('/')
        || !path
            .to_ascii_lowercase()
            .ends_with(&format!(".{}", file_type.extension()))
    {
        return false;
    }

    path.split('/').all(is_safe_path_segment)
}

fn is_safe_path_segment(segment: &str) -> bool {
    !segment.is_empty()
        && segment != "."
        && segment != ".."
        && !segment.chars().any(is_unsafe_path_char)
        && !segment.ends_with(' ')
        && !segment.ends_with('.')
        && !is_reserved_windows_filename(segment)
}

fn is_unsafe_path_char(ch: char) -> bool {
    ch.is_control() || matches!(ch, ':' | '"' | '?' | '<' | '>' | '|' | '*' | '#')
}

fn is_reserved_windows_filename(segment: &str) -> bool {
    let stem = segment
        .split('.')
        .next()
        .unwrap_or(segment)
        .trim()
        .to_ascii_uppercase();
    matches!(stem.as_str(), "CON" | "PRN" | "AUX" | "NUL" | "CLOCK$")
        || stem
            .strip_prefix("COM")
            .is_some_and(|suffix| suffix.len() == 1 && suffix.chars().all(|ch| ch.is_ascii_digit()))
        || stem
            .strip_prefix("LPT")
            .is_some_and(|suffix| suffix.len() == 1 && suffix.chars().all(|ch| ch.is_ascii_digit()))
}

fn normalize_base_path(base_path: &str) -> String {
    let normalized = base_path
        .trim()
        .trim_matches('/')
        .replace('\\', "/")
        .trim()
        .to_string();
    if normalized.is_empty() {
        "11New".to_string()
    } else {
        normalized
    }
}

fn normalized_template(path_template: &str) -> String {
    let trimmed = path_template.trim();
    if trimmed.is_empty() {
        "{base}/{date}-{slug}.md".to_string()
    } else {
        trimmed.to_string()
    }
}

#[cfg(test)]
mod tests {
    use chrono::{TimeZone, Utc};

    use super::{NewNoteFileType, NewNotePathSettings, NewNoteRequest, WriteError};

    fn request(title: &str) -> NewNoteRequest {
        NewNoteRequest {
            title: title.to_string(),
            content: String::new(),
            template_id: None,
            file_type: NewNoteFileType::Md,
        }
    }

    fn fixed_now() -> chrono::DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 2, 26, 12, 0, 0)
            .single()
            .expect("valid timestamp")
    }

    #[test]
    fn slug_template_keeps_existing_lowercase_hyphenated_paths() {
        let settings = NewNotePathSettings {
            base_path: "00New".to_string(),
            path_template: "{base}/{date}-{slug}.md".to_string(),
            date_format: "%Y-%m-%d".to_string(),
            max_title_slug_length: 60,
        };

        let path = request("Conversational Practical Essay Style Guide")
            .validate_write_with_settings_at(fixed_now(), &settings)
            .expect("valid slug path");

        assert_eq!(
            path,
            "00New/2026-02-26-conversational-practical-essay-style-guide.md"
        );
    }

    #[test]
    fn title_template_preserves_spaces_and_case() {
        let settings = NewNotePathSettings {
            base_path: "00New".to_string(),
            path_template: "{base}/{title}.md".to_string(),
            date_format: "%Y-%m-%d".to_string(),
            max_title_slug_length: 60,
        };

        let path = request("Conversational Practical Essay Style Guide")
            .validate_write_with_settings_at(fixed_now(), &settings)
            .expect("valid display title path");

        assert_eq!(path, "00New/Conversational Practical Essay Style Guide.md");
    }

    #[test]
    fn title_template_replaces_path_unsafe_characters() {
        let settings = NewNotePathSettings {
            base_path: "00New".to_string(),
            path_template: "{base}/{title}.md".to_string(),
            date_format: "%Y-%m-%d".to_string(),
            max_title_slug_length: 60,
        };

        let path = request(r#"C# Notes: "Agent"/Draft? <v1>"#)
            .validate_write_with_settings_at(fixed_now(), &settings)
            .expect("unsafe title characters should be sanitized");

        assert_eq!(path, "00New/C Notes Agent Draft v1.md");
    }

    #[test]
    fn base_file_type_replaces_configured_markdown_extension() {
        let settings = NewNotePathSettings {
            base_path: "00New".to_string(),
            path_template: "{base}/{title}.md".to_string(),
            date_format: "%Y-%m-%d".to_string(),
            max_title_slug_length: 60,
        };

        let mut request = request("Project Dashboard");
        request.file_type = NewNoteFileType::Base;
        let path = request
            .validate_write_with_settings_at(fixed_now(), &settings)
            .expect("valid base path");

        assert_eq!(path, "00New/Project Dashboard.base");
    }

    #[test]
    fn new_note_validation_rejects_template_path_traversal() {
        let settings = NewNotePathSettings {
            base_path: "00New".to_string(),
            path_template: "{base}/../{title}.md".to_string(),
            date_format: "%Y-%m-%d".to_string(),
            max_title_slug_length: 60,
        };

        let error = request("Escaping Note")
            .validate_write_with_settings_at(fixed_now(), &settings)
            .expect_err("template traversal should be rejected");

        assert!(matches!(error, WriteError::PathNotAllowed { .. }));
    }
}

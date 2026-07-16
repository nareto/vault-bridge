use std::ops::Range;

use comrak::{
    Arena, Options,
    nodes::{NodeValue, Sourcepos},
    parse_document,
};
use once_cell::sync::Lazy;
use regex::Regex;
use serde_json::Value;

static WIKILINK_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"\[\[([^\]|]+)(?:\|[^\]]+)?\]\]").expect("valid wikilink regex"));

static TAG_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?m)(?:^|\s)#([A-Za-z0-9_\-/]+)").expect("valid tag regex"));
static FRONTMATTER_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?s)\A(?:\u{feff})?---\r?\n(.*?)\r?\n---(?:\r?\n|$)")
        .expect("valid frontmatter regex")
});

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExtractedLink {
    pub target: String,
    pub context: String,
    pub byte_offset: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Heading {
    pub level: u8,
    pub text: String,
}

#[derive(Debug, Clone)]
pub struct ParsedMarkdown {
    pub frontmatter: serde_json::Value,
    pub body: String,
    pub headings: Vec<Heading>,
    pub title: Option<String>,
    pub links: Vec<ExtractedLink>,
    pub tags: Vec<String>,
}

pub fn parse_markdown(content: &str, context_window: usize) -> ParsedMarkdown {
    let (frontmatter, body) = parse_frontmatter(content);
    let headings = extract_headings(&body);
    let title = headings
        .iter()
        .find(|h| h.level == 1)
        .map(|h| h.text.clone());
    let links = extract_wikilinks(&body, context_window);
    let mut tags = extract_tags(&body);
    tags.extend(extract_frontmatter_tags(&frontmatter));
    tags.sort();
    tags.dedup();

    ParsedMarkdown {
        frontmatter,
        body,
        headings,
        title,
        links,
        tags,
    }
}

pub fn first_h1_title(content: &str) -> Option<String> {
    extract_headings(content)
        .into_iter()
        .find(|heading| heading.level == 1)
        .map(|heading| heading.text)
}

/// Deterministic markdown-to-plain-text extraction for FTS/search indexing.
///
/// This keeps user-facing note content in markdown while stripping markdown
/// syntax for ranking/indexing paths.
pub fn markdown_plain_text(content: &str) -> String {
    let arena = Arena::new();
    let root = parse_document(&arena, content, &Options::default());
    let mut lines = Vec::new();

    for node in root.children() {
        let line = normalize_whitespace(&collect_text(node));
        if !line.is_empty() {
            lines.push(line);
        }
    }

    lines.join("\n")
}

pub fn parse_frontmatter(content: &str) -> (serde_json::Value, String) {
    let Some(captures) = FRONTMATTER_RE.captures(content) else {
        return (serde_json::json!({}), content.to_string());
    };

    let yaml = captures.get(1).map_or("", |m| m.as_str());
    let full_match_end = captures.get(0).map_or(0, |m| m.end());
    let body = content
        .get(full_match_end..)
        .unwrap_or_default()
        .trim_start_matches(['\r', '\n'])
        .to_string();
    let frontmatter = serde_yaml::from_str::<serde_yaml::Value>(yaml)
        .ok()
        .and_then(|value| serde_json::to_value(value).ok())
        .unwrap_or_else(|| serde_json::json!({}));

    (frontmatter, body)
}

pub fn extract_wikilinks(content: &str, context_window: usize) -> Vec<ExtractedLink> {
    let code_ranges = find_code_ranges(content);

    WIKILINK_RE
        .captures_iter(content)
        .filter_map(|cap| {
            let m = cap.get(0)?;
            if code_ranges.iter().any(|range| range.contains(&m.start())) {
                return None;
            }
            let target = cap.get(1)?.as_str().trim().to_string();
            let context = extract_context_window(content, m.start(), m.end(), context_window);

            Some(ExtractedLink {
                target,
                context,
                byte_offset: m.start(),
            })
        })
        .collect()
}

pub fn extract_context_window(content: &str, start: usize, end: usize, window: usize) -> String {
    let left_boundary = content[..start]
        .rfind(['.', '!', '?', '\n'])
        .map_or(start.saturating_sub(window), |i| i + 1);

    let right_boundary = content[end..]
        .find(['.', '!', '?', '\n'])
        .map_or((end + window).min(content.len()), |i| end + i + 1);

    let safe_left = floor_char_boundary(content, left_boundary);
    let safe_right = ceil_char_boundary(content, right_boundary);

    content[safe_left..safe_right].trim().replace("\n", " ")
}

pub fn extract_tags(content: &str) -> Vec<String> {
    let code_ranges = find_code_ranges(content);
    let mut tags = TAG_RE
        .captures_iter(content)
        .filter_map(|cap| {
            let m = cap.get(0)?;
            if code_ranges.iter().any(|range| range.contains(&m.start())) {
                return None;
            }
            cap.get(1).map(|x| x.as_str().to_string())
        })
        .collect::<Vec<_>>();

    tags.sort();
    tags.dedup();
    tags
}

pub fn extract_frontmatter_tags(frontmatter: &Value) -> Vec<String> {
    let mut tags = Vec::new();

    match frontmatter.get("tags") {
        Some(Value::Array(items)) => {
            for item in items {
                if let Some(tag) = item.as_str() {
                    push_tag(&mut tags, tag);
                }
            }
        }
        Some(Value::String(raw)) => {
            for tag in raw.split([',', ' ']) {
                push_tag(&mut tags, tag);
            }
        }
        _ => {}
    }

    tags.sort();
    tags.dedup();
    tags
}

pub fn extract_headings(content: &str) -> Vec<Heading> {
    let arena = Arena::new();
    let root = parse_document(&arena, content, &Options::default());

    root.descendants()
        .filter_map(|node| {
            let data = node.data.borrow();
            if let NodeValue::Heading(heading) = &data.value {
                let text = collect_text(node);
                Some(Heading {
                    level: heading.level,
                    text: text.trim().to_string(),
                })
            } else {
                None
            }
        })
        .collect()
}

fn collect_text<'a>(node: &'a comrak::nodes::AstNode<'a>) -> String {
    fn walk<'a>(node: &'a comrak::nodes::AstNode<'a>, out: &mut String) {
        let data = node.data.borrow();
        match &data.value {
            NodeValue::Text(text) => out.push_str(text),
            NodeValue::Code(code) => out.push_str(&code.literal),
            NodeValue::LineBreak | NodeValue::SoftBreak => out.push(' '),
            _ => {
                drop(data);
                for child in node.children() {
                    walk(child, out);
                }
            }
        }
    }

    let mut out = String::new();
    for child in node.children() {
        walk(child, &mut out);
    }
    out
}

fn push_tag(tags: &mut Vec<String>, raw: &str) {
    let normalized = raw.trim().trim_start_matches('#');
    if !normalized.is_empty() {
        tags.push(normalized.to_string());
    }
}

/// AST-driven code range detection for fenced/inline code segments.
fn find_code_ranges(content: &str) -> Vec<Range<usize>> {
    if content.is_empty() {
        return Vec::new();
    }

    let arena = Arena::new();
    let root = parse_document(&arena, content, &Options::default());
    let line_starts = line_start_offsets(content);

    let mut ranges = Vec::new();

    for node in root.descendants() {
        let data = node.data.borrow();
        let is_code = matches!(&data.value, NodeValue::CodeBlock(_) | NodeValue::Code(_));
        if !is_code {
            continue;
        }
        if let Some(range) = sourcepos_to_byte_range(content, &line_starts, data.sourcepos) {
            ranges.push(range);
        }
    }

    if ranges.len() <= 1 {
        return ranges;
    }

    ranges.sort_by(|a, b| a.start.cmp(&b.start).then_with(|| a.end.cmp(&b.end)));
    let mut merged: Vec<Range<usize>> = Vec::with_capacity(ranges.len());
    for range in ranges {
        if let Some(last) = merged.last_mut()
            && range.start <= last.end
        {
            last.end = last.end.max(range.end);
            continue;
        }
        merged.push(range);
    }

    merged
}

fn sourcepos_to_byte_range(
    content: &str,
    line_starts: &[usize],
    sourcepos: Sourcepos,
) -> Option<Range<usize>> {
    if sourcepos.start.line == 0 || sourcepos.end.line == 0 {
        return None;
    }

    let start = line_column_to_offset(
        content,
        line_starts,
        sourcepos.start.line,
        sourcepos.start.column,
    );
    let end_char_start = line_column_to_offset(
        content,
        line_starts,
        sourcepos.end.line,
        sourcepos.end.column,
    );
    let end = char_end_offset(content, end_char_start);

    if start >= end { None } else { Some(start..end) }
}

fn line_start_offsets(content: &str) -> Vec<usize> {
    let mut starts = vec![0usize];
    for (idx, ch) in content.char_indices() {
        if ch == '\n' {
            starts.push(idx + ch.len_utf8());
        }
    }
    starts
}

fn line_column_to_offset(
    content: &str,
    line_starts: &[usize],
    line: usize,
    column: usize,
) -> usize {
    if line == 0 {
        return 0;
    }
    let line_index = line.saturating_sub(1);
    if line_index >= line_starts.len() {
        return content.len();
    }

    let line_start = line_starts[line_index];
    let line_end = line_starts
        .get(line_index + 1)
        .copied()
        .unwrap_or(content.len());
    if column <= 1 {
        return line_start;
    }

    for (char_count, (byte_index, _)) in
        (1usize..).zip(content[line_start..line_end].char_indices())
    {
        if char_count == column {
            return line_start + byte_index;
        }
    }

    line_end
}

fn char_end_offset(content: &str, start: usize) -> usize {
    if start >= content.len() {
        return content.len();
    }

    let safe_start = floor_char_boundary(content, start);
    let Some(ch) = content[safe_start..].chars().next() else {
        return content.len();
    };
    safe_start + ch.len_utf8()
}

fn normalize_whitespace(content: &str) -> String {
    content.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn floor_char_boundary(text: &str, index: usize) -> usize {
    let mut idx = index.min(text.len());
    while idx > 0 && !text.is_char_boundary(idx) {
        idx -= 1;
    }
    idx
}

fn ceil_char_boundary(text: &str, index: usize) -> usize {
    let mut idx = index.min(text.len());
    while idx < text.len() && !text.is_char_boundary(idx) {
        idx += 1;
    }
    idx
}

// ---------------------------------------------------------------------------
// Heading-based block chunking
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MarkdownBlock {
    pub heading_path: Vec<Heading>,
    pub content: String,
    pub block_index: usize,
}

/// Split a markdown body (frontmatter already stripped) into heading-delimited
/// blocks.  Blocks shorter than `min_chars` are merged into the previous block.
/// Notes with no headings produce a single block covering the entire body.
pub fn split_into_blocks(body: &str, min_chars: usize) -> Vec<MarkdownBlock> {
    if body.is_empty() {
        return Vec::new();
    }

    let arena = Arena::new();
    let root = parse_document(&arena, body, &Options::default());

    // Collect (byte_start, heading_level, heading_text) for each heading node.
    let line_starts = line_start_offsets(body);
    let mut heading_positions: Vec<(usize, u8, String)> = Vec::new();

    for node in root.children() {
        let data = node.data.borrow();
        if let NodeValue::Heading(h) = &data.value {
            let text = collect_text(node).trim().to_string();
            let byte_start = sourcepos_to_byte_range(body, &line_starts, data.sourcepos)
                .map(|r| r.start)
                .unwrap_or(0);
            heading_positions.push((byte_start, h.level, text));
        }
    }

    // No headings → single block with the entire body.
    if heading_positions.is_empty() {
        return vec![MarkdownBlock {
            heading_path: Vec::new(),
            content: body.to_string(),
            block_index: 0,
        }];
    }

    // Build raw blocks by splitting body at heading boundaries.
    struct RawBlock {
        heading_path: Vec<Heading>,
        content: String,
    }

    let mut raw_blocks: Vec<RawBlock> = Vec::new();
    let mut heading_stack: Vec<Heading> = Vec::new();

    // Preamble: text before first heading.
    let first_heading_start = heading_positions[0].0;
    if first_heading_start > 0 {
        let preamble = body[..first_heading_start].trim();
        if !preamble.is_empty() {
            raw_blocks.push(RawBlock {
                heading_path: Vec::new(),
                content: preamble.to_string(),
            });
        }
    }

    for (i, (byte_start, level, text)) in heading_positions.iter().enumerate() {
        // Update heading stack: pop headings >= current level, push new one.
        heading_stack.retain(|h| h.level < *level);
        heading_stack.push(Heading {
            level: *level,
            text: text.clone(),
        });

        let block_end = heading_positions
            .get(i + 1)
            .map(|(start, _, _)| *start)
            .unwrap_or(body.len());

        let block_content = body[*byte_start..block_end].trim();
        if !block_content.is_empty() {
            raw_blocks.push(RawBlock {
                heading_path: heading_stack.clone(),
                content: block_content.to_string(),
            });
        }
    }

    // Merge short blocks into their predecessor.
    let mut merged: Vec<RawBlock> = Vec::new();
    for block in raw_blocks {
        if block.content.len() < min_chars
            && let Some(prev) = merged.last_mut()
        {
            prev.content.push('\n');
            prev.content.push_str(&block.content);
            continue;
        }
        merged.push(block);
    }

    merged
        .into_iter()
        .enumerate()
        .map(|(i, b)| MarkdownBlock {
            heading_path: b.heading_path,
            content: b.content,
            block_index: i,
        })
        .collect()
}

/// Split a markdown body into heading-aware semantic chunks.
///
/// Heading paths are preserved as metadata, but large heading sections are split
/// within the section so retrieval and embedding operate on bounded chunks.
pub fn split_into_semantic_blocks(
    body: &str,
    min_chars: usize,
    max_bytes: usize,
    overlap_sentences: usize,
) -> Vec<MarkdownBlock> {
    let max_bytes = max_bytes.max(32);
    let mut out = Vec::new();

    for block in split_into_blocks(body, min_chars) {
        if block.content.len() <= max_bytes {
            out.push(MarkdownBlock {
                block_index: out.len(),
                ..block
            });
            continue;
        }

        for chunk in split_semantic_content(&block.content, max_bytes, overlap_sentences) {
            if chunk.trim().is_empty() {
                continue;
            }
            out.push(MarkdownBlock {
                heading_path: block.heading_path.clone(),
                content: chunk,
                block_index: out.len(),
            });
        }
    }

    out
}

fn split_semantic_content(
    content: &str,
    max_bytes: usize,
    overlap_sentences: usize,
) -> Vec<String> {
    let units = paragraph_units(content)
        .into_iter()
        .flat_map(|paragraph| {
            if paragraph.len() <= max_bytes {
                vec![paragraph]
            } else {
                split_oversized_paragraph(&paragraph, max_bytes)
            }
        })
        .collect::<Vec<_>>();

    let mut chunks = Vec::new();
    let mut current = String::new();
    for unit in units {
        let candidate_len = if current.is_empty() {
            unit.len()
        } else {
            current.len() + 2 + unit.len()
        };
        if !current.is_empty() && candidate_len > max_bytes {
            chunks.push(current);
            current = unit;
        } else if current.is_empty() {
            current = unit;
        } else {
            current.push_str("\n\n");
            current.push_str(&unit);
        }
    }
    if !current.is_empty() {
        chunks.push(current);
    }

    apply_sentence_overlap(chunks, max_bytes, overlap_sentences)
}

fn paragraph_units(content: &str) -> Vec<String> {
    let mut units = Vec::new();
    let mut current = Vec::new();

    for line in content.lines() {
        if line.trim().is_empty() {
            if !current.is_empty() {
                units.push(current.join("\n").trim().to_string());
                current.clear();
            }
        } else {
            current.push(line);
        }
    }

    if !current.is_empty() {
        units.push(current.join("\n").trim().to_string());
    }

    if units.is_empty() && !content.trim().is_empty() {
        units.push(content.trim().to_string());
    }

    units
}

fn split_oversized_paragraph(paragraph: &str, max_bytes: usize) -> Vec<String> {
    let sentences = sentence_units(paragraph);
    if sentences.len() <= 1 && sentences.first().is_some_and(|s| s.len() > max_bytes) {
        return hard_split_text(paragraph, max_bytes);
    }

    let mut chunks = Vec::new();
    let mut current = String::new();
    for sentence in sentences {
        if sentence.len() > max_bytes {
            if !current.is_empty() {
                chunks.push(current);
                current = String::new();
            }
            chunks.extend(hard_split_text(&sentence, max_bytes));
            continue;
        }

        let candidate_len = if current.is_empty() {
            sentence.len()
        } else {
            current.len() + 1 + sentence.len()
        };
        if !current.is_empty() && candidate_len > max_bytes {
            chunks.push(current);
            current = sentence;
        } else if current.is_empty() {
            current = sentence;
        } else {
            current.push(' ');
            current.push_str(&sentence);
        }
    }

    if !current.is_empty() {
        chunks.push(current);
    }
    chunks
}

fn sentence_units(text: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut start = 0usize;
    let mut chars = text.char_indices().peekable();
    while let Some((idx, ch)) = chars.next() {
        if !matches!(ch, '.' | '!' | '?') {
            continue;
        }
        let end = idx + ch.len_utf8();
        let boundary = chars
            .peek()
            .map(|(_, next)| next.is_whitespace())
            .unwrap_or(true);
        if boundary {
            let sentence = text[start..end].trim();
            if !sentence.is_empty() {
                out.push(sentence.to_string());
            }
            start = end;
            while start < text.len()
                && text[start..]
                    .chars()
                    .next()
                    .is_some_and(|next| next.is_whitespace())
            {
                start = ceil_char_boundary(text, start + 1);
            }
        }
    }

    let tail = text[start..].trim();
    if !tail.is_empty() {
        out.push(tail.to_string());
    }
    out
}

fn hard_split_text(text: &str, max_bytes: usize) -> Vec<String> {
    let mut chunks = Vec::new();
    let mut remaining = text.trim();
    while !remaining.is_empty() {
        if remaining.len() <= max_bytes {
            chunks.push(remaining.to_string());
            break;
        }
        let mut end = floor_char_boundary(remaining, max_bytes);
        if end == 0 {
            end = ceil_char_boundary(remaining, 1);
        }
        let chunk = remaining[..end].trim();
        if !chunk.is_empty() {
            chunks.push(chunk.to_string());
        }
        remaining = remaining[end..].trim_start();
    }
    chunks
}

fn apply_sentence_overlap(
    chunks: Vec<String>,
    max_bytes: usize,
    overlap_sentences: usize,
) -> Vec<String> {
    if overlap_sentences == 0 || chunks.len() < 2 {
        return chunks;
    }

    let mut out: Vec<String> = Vec::with_capacity(chunks.len());
    for chunk in chunks {
        if let Some(previous) = out.last() {
            let overlap = trailing_sentences(previous, overlap_sentences);
            if !overlap.is_empty() && overlap.len() + 2 + chunk.len() <= max_bytes {
                out.push(format!("{overlap}\n\n{chunk}"));
                continue;
            }
        }
        out.push(chunk);
    }
    out
}

fn trailing_sentences(text: &str, count: usize) -> String {
    let sentences = sentence_units(text);
    let start = sentences.len().saturating_sub(count);
    sentences[start..].join(" ")
}

/// Build a breadcrumb prefix for embedding.
///
/// Format: `folder > note title > H2 text > H3 text`
pub fn breadcrumb_prefix(note_path: &str, note_title: &str, heading_path: &[Heading]) -> String {
    let mut parts: Vec<&str> = Vec::new();

    // Folder portion: strip .md, take directory part.
    let stripped = note_path.strip_suffix(".md").unwrap_or(note_path);
    if let Some(pos) = stripped.rfind('/') {
        let folder = &stripped[..pos];
        if !folder.is_empty() {
            parts.push(folder);
        }
    }

    if !note_title.is_empty() {
        parts.push(note_title);
    }

    let heading_texts: Vec<String> = heading_path.iter().map(|h| h.text.clone()).collect();
    for text in &heading_texts {
        parts.push(text);
    }

    parts.join(" > ")
}

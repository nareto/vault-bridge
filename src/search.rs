use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::model::NoteId;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum SearchMode {
    Semantic,
    Fulltext,
    #[default]
    Hybrid,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum MatchType {
    Semantic,
    Fulltext,
    Both,
}

#[derive(Debug, Clone)]
pub struct UnscopedSearchHit {
    pub id: NoteId,
    pub title: String,
    pub snippet: String,
    pub score: f32,
    pub match_type: MatchType,
    pub matched_chunk_id: Option<String>,
    pub matched_heading_path: Option<String>,
    pub matched_snippet: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct SearchHit {
    pub id: NoteId,
    pub title: String,
    pub snippet: String,
    pub score: f32,
    pub match_type: MatchType,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub matched_chunk_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub matched_heading_path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub matched_snippet: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct SearchResponse {
    pub results: Vec<SearchHit>,
    pub total_filtered: usize,
}

#[derive(Debug, Clone)]
pub struct CandidateSearchDoc {
    pub id: NoteId,
    pub title: String,
    pub content: String,
    pub embedding: Option<Vec<f32>>,
}

impl UnscopedSearchHit {
    pub fn into_hit(self) -> SearchHit {
        SearchHit {
            id: self.id,
            title: self.title,
            snippet: self.snippet,
            score: self.score,
            match_type: self.match_type,
            matched_chunk_id: self.matched_chunk_id,
            matched_heading_path: self.matched_heading_path,
            matched_snippet: self.matched_snippet,
        }
    }
}

impl SearchResponse {
    pub fn new(results: Vec<SearchHit>, total_filtered: usize) -> Self {
        Self {
            results,
            total_filtered,
        }
    }
}

const RRF_K: f32 = 60.0;

pub fn embed_text(text: &str, dimensions: usize) -> Vec<f32> {
    let mut out = vec![0.0f32; dimensions];
    if text.trim().is_empty() {
        return out;
    }

    for token in text
        .to_lowercase()
        .split_whitespace()
        .map(|x| x.trim_matches(|c: char| !c.is_alphanumeric()))
        .filter(|x| !x.is_empty())
    {
        let mut hash = 1469598103934665603u64;
        for b in token.as_bytes() {
            hash ^= *b as u64;
            hash = hash.wrapping_mul(1099511628211);
        }
        let idx = (hash as usize) % dimensions;
        out[idx] += 1.0;
    }

    normalize(&mut out);
    out
}

pub fn cosine_similarity(a: &[f32], b: &[f32]) -> f32 {
    if a.len() != b.len() || a.is_empty() {
        return 0.0;
    }

    let dot = a.iter().zip(b).map(|(x, y)| x * y).sum::<f32>();
    let norm_a = a.iter().map(|x| x * x).sum::<f32>().sqrt();
    let norm_b = b.iter().map(|x| x * x).sum::<f32>().sqrt();

    if norm_a == 0.0 || norm_b == 0.0 {
        return 0.0;
    }

    dot / (norm_a * norm_b)
}

fn normalize(vec: &mut [f32]) {
    let norm = vec.iter().map(|x| x * x).sum::<f32>().sqrt();
    if norm > 0.0 {
        for x in vec {
            *x /= norm;
        }
    }
}

pub fn fulltext_ranking(docs: &[CandidateSearchDoc], query: &str) -> Vec<(NoteId, f32)> {
    let q = query.to_lowercase();
    docs.iter()
        .filter_map(|doc| {
            let title = doc.title.to_lowercase();
            let body = doc.content.to_lowercase();
            let title_hits = title.matches(&q).count() as f32;
            let body_hits = body.matches(&q).count() as f32;
            let score = title_hits * 2.0 + body_hits;
            if score > 0.0 {
                Some((doc.id.clone(), score))
            } else {
                None
            }
        })
        .collect::<Vec<_>>()
        .into_iter()
        .sorted_desc_by_score()
}

pub fn semantic_ranking(
    docs: &[CandidateSearchDoc],
    query_embedding: &[f32],
) -> Vec<(NoteId, f32)> {
    docs.iter()
        .filter_map(|doc| {
            let emb = doc.embedding.as_ref()?;
            Some((doc.id.clone(), cosine_similarity(emb, query_embedding)))
        })
        .filter(|(_, score)| *score > 0.0)
        .collect::<Vec<_>>()
        .into_iter()
        .sorted_desc_by_score()
}

pub fn hybrid_ranking(
    fulltext: &[(NoteId, f32)],
    semantic: &[(NoteId, f32)],
) -> Vec<(NoteId, f32, MatchType)> {
    let mut out: HashMap<NoteId, (f32, bool, bool)> = HashMap::new();

    for (idx, (id, _)) in fulltext.iter().enumerate() {
        let score = 1.0 / (RRF_K + idx as f32 + 1.0);
        let entry = out.entry(id.clone()).or_insert((0.0, false, false));
        entry.0 += score;
        entry.1 = true;
    }

    for (idx, (id, _)) in semantic.iter().enumerate() {
        let score = 1.0 / (RRF_K + idx as f32 + 1.0);
        let entry = out.entry(id.clone()).or_insert((0.0, false, false));
        entry.0 += score;
        entry.2 = true;
    }

    let mut items = out
        .into_iter()
        .map(|(id, (score, has_fulltext, has_semantic))| {
            let match_type = match (has_fulltext, has_semantic) {
                (true, true) => MatchType::Both,
                (true, false) => MatchType::Fulltext,
                (false, true) => MatchType::Semantic,
                (false, false) => MatchType::Fulltext,
            };
            (id, score, match_type)
        })
        .collect::<Vec<_>>();

    items.sort_by(|a, b| {
        b.1.total_cmp(&a.1)
            .then_with(|| a.0.as_str().cmp(b.0.as_str()))
    });
    items
}

trait SortedByScore {
    fn sorted_desc_by_score(self) -> Vec<(NoteId, f32)>;
}

impl<I> SortedByScore for I
where
    I: Iterator<Item = (NoteId, f32)>,
{
    fn sorted_desc_by_score(self) -> Vec<(NoteId, f32)> {
        let mut items = self.collect::<Vec<_>>();
        items.sort_by(|a, b| {
            b.1.total_cmp(&a.1)
                .then_with(|| a.0.as_str().cmp(b.0.as_str()))
        });
        items
    }
}

#[cfg(test)]
mod tests {
    use static_assertions::assert_not_impl_any;

    use super::UnscopedSearchHit;

    #[test]
    fn unscoped_search_hit_is_not_serializable() {
        assert_not_impl_any!(UnscopedSearchHit: serde::Serialize);
    }
}

use std::collections::{HashMap, HashSet, VecDeque};

use serde::{Deserialize, Serialize};

use crate::model::NoteId;
use crate::search::cosine_similarity;

#[derive(Debug, Clone, Copy, Deserialize, Serialize, Default, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ContextFormat {
    #[default]
    Structured,
    Flat,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct AssembleContextRequest {
    #[serde(default)]
    pub seeds: Vec<NoteId>,
    #[serde(default)]
    pub seed_query: Option<String>,
    #[serde(default)]
    pub max_depth: Option<usize>,
    #[serde(default)]
    pub max_tokens: Option<usize>,
    #[serde(default)]
    pub include_graph_summary: Option<bool>,
    #[serde(default)]
    pub format: Option<ContextFormat>,
}

#[derive(Debug, Clone, Serialize)]
pub struct AssembleContextResponse {
    pub graph_summary: Option<String>,
    pub notes: Vec<ContextNote>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub flat_context: Option<String>,
    pub token_estimate: usize,
    pub notes_included: usize,
    pub notes_summarized: usize,
    pub notes_excluded: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct ContextNote {
    pub id: NoteId,
    pub title: String,
    pub content: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub summary: Option<String>,
    pub role: ContextRole,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub depth: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub links_to: Option<Vec<NoteId>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub linked_from: Option<Vec<NoteId>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub is_hub: Option<bool>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum ContextRole {
    Seed,
    Context,
    Peripheral,
}

#[derive(Debug, Clone)]
pub struct ContextCandidate {
    pub id: NoteId,
    pub title: String,
    pub content: String,
    pub summary: String,
    pub links_to: Vec<NoteId>,
    pub linked_from: Vec<NoteId>,
    pub embedding: Option<Vec<f32>>,
    pub depth: usize,
    pub is_seed: bool,
    pub is_hub: bool,
}

pub fn estimate_tokens(text: &str) -> usize {
    ((text.len() as f64) / 4.0).ceil() as usize
}

pub fn assemble_context(
    mut candidates: Vec<ContextCandidate>,
    max_tokens: usize,
    include_graph_summary: bool,
    query_embedding: Option<&[f32]>,
    format: ContextFormat,
) -> AssembleContextResponse {
    let centrality_scores = betweenness_centrality_scores(&candidates);

    candidates.sort_by(|a, b| {
        let score_a = candidate_score(
            a,
            query_embedding,
            centrality_scores.get(&a.id).copied().unwrap_or(0.0),
        );
        let score_b = candidate_score(
            b,
            query_embedding,
            centrality_scores.get(&b.id).copied().unwrap_or(0.0),
        );
        b.is_seed
            .cmp(&a.is_seed)
            .then_with(|| score_b.total_cmp(&score_a))
            .then_with(|| a.depth.cmp(&b.depth))
            .then_with(|| a.id.as_str().cmp(b.id.as_str()))
    });

    let mut seen = HashSet::new();
    candidates.retain(|c| seen.insert(c.id.clone()));

    let mut notes = Vec::new();
    let mut used_tokens = 0usize;
    let mut included = 0usize;
    let mut summarized = 0usize;

    for candidate in &candidates {
        let full_tokens = estimate_tokens(&candidate.content);
        let summary_tokens = estimate_tokens(&candidate.summary);
        let reserve_summary = candidate.is_hub || used_tokens + full_tokens > max_tokens;

        if reserve_summary {
            if used_tokens + summary_tokens > max_tokens {
                if candidate.is_seed && notes.is_empty() {
                    used_tokens += summary_tokens;
                    summarized += 1;
                    notes.push(ContextNote {
                        id: candidate.id.clone(),
                        title: candidate.title.clone(),
                        content: None,
                        summary: Some(candidate.summary.clone()),
                        role: ContextRole::Seed,
                        depth: Some(candidate.depth),
                        links_to: None,
                        linked_from: None,
                        is_hub: Some(candidate.is_hub),
                    });
                }
                continue;
            }
            used_tokens += summary_tokens;
            summarized += 1;
            notes.push(ContextNote {
                id: candidate.id.clone(),
                title: candidate.title.clone(),
                content: None,
                summary: Some(candidate.summary.clone()),
                role: if candidate.is_seed {
                    ContextRole::Seed
                } else {
                    ContextRole::Peripheral
                },
                depth: Some(candidate.depth),
                links_to: None,
                linked_from: None,
                is_hub: Some(candidate.is_hub),
            });
        } else {
            used_tokens += full_tokens;
            included += 1;
            notes.push(ContextNote {
                id: candidate.id.clone(),
                title: candidate.title.clone(),
                content: Some(candidate.content.clone()),
                summary: None,
                role: if candidate.is_seed {
                    ContextRole::Seed
                } else {
                    ContextRole::Context
                },
                depth: Some(candidate.depth),
                links_to: Some(candidate.links_to.clone()),
                linked_from: Some(candidate.linked_from.clone()),
                is_hub: Some(candidate.is_hub),
            });
        }
    }

    let excluded = candidates.len().saturating_sub(notes.len());
    let graph_summary = include_graph_summary.then(|| build_graph_summary(&notes));
    let flat_context = (format == ContextFormat::Flat)
        .then(|| build_flat_context(graph_summary.as_deref(), &notes));

    AssembleContextResponse {
        graph_summary,
        notes,
        flat_context,
        token_estimate: used_tokens,
        notes_included: included,
        notes_summarized: summarized,
        notes_excluded: excluded,
    }
}

/// Rank notes by structural and semantic relevance.
///
/// Betweenness centrality adds a topology signal so connector notes are ranked
/// higher as navigation context, while hub notes are still summarized later by
/// the token-budget phase.
fn candidate_score(
    candidate: &ContextCandidate,
    query_embedding: Option<&[f32]>,
    centrality_score: f32,
) -> f32 {
    let depth_score = 1.0 / (1.0 + candidate.depth as f32);
    let semantic_score = query_embedding
        .and_then(|q| {
            candidate
                .embedding
                .as_ref()
                .map(|e| cosine_similarity(e, q))
        })
        .unwrap_or(0.0);
    let hub_navigation_boost = if candidate.is_hub { 0.05 } else { 0.0 };

    depth_score * 0.5 + semantic_score * 0.3 + centrality_score * 0.2 + hub_navigation_boost
}

fn betweenness_centrality_scores(candidates: &[ContextCandidate]) -> HashMap<NoteId, f32> {
    let mut nodes = candidates
        .iter()
        .map(|candidate| candidate.id.clone())
        .collect::<Vec<_>>();
    nodes.sort_by(|a, b| a.as_str().cmp(b.as_str()));
    nodes.dedup();

    if nodes.is_empty() {
        return HashMap::new();
    }

    let node_set = nodes.iter().cloned().collect::<HashSet<_>>();
    let mut adjacency: HashMap<NoteId, Vec<NoteId>> = HashMap::new();

    for candidate in candidates {
        let mut neighbors = candidate
            .links_to
            .iter()
            .filter(|id| node_set.contains(*id))
            .cloned()
            .collect::<Vec<_>>();
        neighbors.sort_by(|a, b| a.as_str().cmp(b.as_str()));
        neighbors.dedup();
        adjacency.insert(candidate.id.clone(), neighbors);
    }

    let mut centrality = nodes
        .iter()
        .cloned()
        .map(|id| (id, 0.0f32))
        .collect::<HashMap<_, _>>();

    for source in &nodes {
        let mut stack = Vec::new();
        let mut predecessors = nodes
            .iter()
            .cloned()
            .map(|id| (id, Vec::<NoteId>::new()))
            .collect::<HashMap<_, _>>();
        let mut sigma = nodes
            .iter()
            .cloned()
            .map(|id| (id, 0.0f32))
            .collect::<HashMap<_, _>>();
        let mut distance = nodes
            .iter()
            .cloned()
            .map(|id| (id, -1i32))
            .collect::<HashMap<_, _>>();
        let mut queue = VecDeque::from([source.clone()]);

        sigma.insert(source.clone(), 1.0);
        distance.insert(source.clone(), 0);

        while let Some(node) = queue.pop_front() {
            stack.push(node.clone());
            let node_distance = *distance.get(&node).unwrap_or(&-1);
            let node_sigma = *sigma.get(&node).unwrap_or(&0.0);

            if let Some(neighbors) = adjacency.get(&node) {
                for neighbor in neighbors {
                    if *distance.get(neighbor).unwrap_or(&-1) < 0 {
                        distance.insert(neighbor.clone(), node_distance + 1);
                        queue.push_back(neighbor.clone());
                    }

                    if *distance.get(neighbor).unwrap_or(&-1) == node_distance + 1 {
                        predecessors
                            .entry(neighbor.clone())
                            .or_default()
                            .push(node.clone());
                        *sigma.entry(neighbor.clone()).or_default() += node_sigma;
                    }
                }
            }
        }

        let mut delta = nodes
            .iter()
            .cloned()
            .map(|id| (id, 0.0f32))
            .collect::<HashMap<_, _>>();

        while let Some(node) = stack.pop() {
            let sigma_node = *sigma.get(&node).unwrap_or(&0.0);
            let dependencies = predecessors.get(&node).cloned().unwrap_or_default();
            for dependency in dependencies {
                if sigma_node <= 0.0 {
                    continue;
                }
                let sigma_dependency = *sigma.get(&dependency).unwrap_or(&0.0);
                let delta_node = *delta.get(&node).unwrap_or(&0.0);
                let contribution = (sigma_dependency / sigma_node) * (1.0 + delta_node);
                *delta.entry(dependency).or_default() += contribution;
            }

            if &node != source {
                *centrality.entry(node.clone()).or_default() += *delta.get(&node).unwrap_or(&0.0);
            }
        }
    }

    let node_count = nodes.len() as f32;
    if node_count > 2.0 {
        let normalization = 1.0 / ((node_count - 1.0) * (node_count - 2.0));
        for value in centrality.values_mut() {
            *value *= normalization;
        }
    }

    let max_value = centrality.values().copied().fold(0.0f32, f32::max);
    if max_value > 0.0 {
        for value in centrality.values_mut() {
            *value /= max_value;
        }
    }

    centrality
}

fn build_graph_summary(notes: &[ContextNote]) -> String {
    if notes.is_empty() {
        return "No notes matched the requested context.".to_string();
    }

    let seeds = notes
        .iter()
        .filter(|n| matches!(n.role, ContextRole::Seed))
        .map(|n| n.title.clone())
        .collect::<Vec<_>>();
    let contexts = notes
        .iter()
        .filter(|n| matches!(n.role, ContextRole::Context))
        .map(|n| n.title.clone())
        .collect::<Vec<_>>();

    if seeds.is_empty() {
        format!(
            "Context includes {} notes connected across the authorized graph.",
            notes.len()
        )
    } else if contexts.is_empty() {
        format!("Context centers on seed notes: {}.", seeds.join(", "))
    } else {
        format!(
            "This context centers on {} and connects to {}.",
            seeds.join(", "),
            contexts
                .iter()
                .take(5)
                .cloned()
                .collect::<Vec<_>>()
                .join(", ")
        )
    }
}

pub fn edge_list(notes: &[ContextCandidate]) -> HashMap<NoteId, Vec<NoteId>> {
    notes
        .iter()
        .map(|c| (c.id.clone(), c.links_to.clone()))
        .collect()
}

fn build_flat_context(graph_summary: Option<&str>, notes: &[ContextNote]) -> String {
    let mut sections = Vec::new();

    if let Some(summary) = graph_summary
        && !summary.trim().is_empty()
    {
        sections.push(format!("Graph Summary:\n{}", summary.trim()));
    }

    for note in notes {
        let header = format!("## {} ({})", note.title, note.id.as_str());
        let body = note
            .content
            .as_deref()
            .or(note.summary.as_deref())
            .unwrap_or("")
            .trim();
        sections.push(format!("{header}\n{body}"));
    }

    sections.join("\n\n")
}

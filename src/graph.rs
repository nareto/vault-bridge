use std::collections::{HashMap, HashSet, VecDeque};

use crate::model::NoteId;

#[derive(Debug, Clone)]
pub struct FilteredGraph {
    accessible_notes: HashSet<NoteId>,
    forward_links: HashMap<NoteId, Vec<NoteId>>,
    backward_links: HashMap<NoteId, Vec<NoteId>>,
}

impl FilteredGraph {
    pub fn from_accessible_notes(
        accessible: HashSet<NoteId>,
        all_links: &[(NoteId, NoteId)],
    ) -> Self {
        let mut forward: HashMap<NoteId, Vec<NoteId>> = HashMap::new();
        let mut backward: HashMap<NoteId, Vec<NoteId>> = HashMap::new();
        let mut seen_edges: HashSet<(NoteId, NoteId)> = HashSet::new();

        for (source, target) in all_links {
            if accessible.contains(source) && accessible.contains(target) {
                if !seen_edges.insert((source.clone(), target.clone())) {
                    continue;
                }
                forward
                    .entry(source.clone())
                    .or_default()
                    .push(target.clone());
                backward
                    .entry(target.clone())
                    .or_default()
                    .push(source.clone());
            }
        }

        for neighbors in forward.values_mut() {
            neighbors.sort_by(|a, b| a.as_str().cmp(b.as_str()));
        }
        for backlinks in backward.values_mut() {
            backlinks.sort_by(|a, b| a.as_str().cmp(b.as_str()));
        }

        Self {
            accessible_notes: accessible,
            forward_links: forward,
            backward_links: backward,
        }
    }

    pub fn is_accessible(&self, id: &NoteId) -> bool {
        self.accessible_notes.contains(id)
    }

    pub fn accessible_count(&self) -> usize {
        self.accessible_notes.len()
    }

    pub fn get_neighbors(&self, id: &NoteId) -> Vec<NoteId> {
        self.forward_links.get(id).cloned().unwrap_or_default()
    }

    pub fn get_backlinks(&self, id: &NoteId) -> Vec<NoteId> {
        self.backward_links.get(id).cloned().unwrap_or_default()
    }

    pub fn bfs(&self, start: &NoteId, max_depth: usize) -> Vec<(NoteId, usize)> {
        if !self.accessible_notes.contains(start) {
            return Vec::new();
        }

        let mut queue: VecDeque<(NoteId, usize)> = VecDeque::from([(start.clone(), 0)]);
        let mut visited: HashSet<NoteId> = HashSet::from([start.clone()]);
        let mut out = vec![(start.clone(), 0)];

        while let Some((current, depth)) = queue.pop_front() {
            if depth == max_depth {
                continue;
            }

            let next_depth = depth + 1;
            for neighbor in self.get_neighbors(&current) {
                if visited.insert(neighbor.clone()) {
                    out.push((neighbor.clone(), next_depth));
                    queue.push_back((neighbor, next_depth));
                }
            }
        }

        out
    }

    pub fn shortest_path(&self, from: &NoteId, to: &NoteId) -> Option<Vec<NoteId>> {
        if !self.is_accessible(from) || !self.is_accessible(to) {
            return None;
        }
        if from == to {
            return Some(vec![from.clone()]);
        }

        let mut queue: VecDeque<NoteId> = VecDeque::from([from.clone()]);
        let mut visited: HashSet<NoteId> = HashSet::from([from.clone()]);
        let mut parent: HashMap<NoteId, NoteId> = HashMap::new();

        while let Some(current) = queue.pop_front() {
            for neighbor in self.get_neighbors(&current) {
                if visited.contains(&neighbor) {
                    continue;
                }

                parent.insert(neighbor.clone(), current.clone());
                if &neighbor == to {
                    let mut path = vec![to.clone()];
                    let mut cursor = to.clone();
                    while let Some(prev) = parent.get(&cursor) {
                        path.push(prev.clone());
                        cursor = prev.clone();
                        if &cursor == from {
                            break;
                        }
                    }
                    path.reverse();
                    return Some(path);
                }

                visited.insert(neighbor.clone());
                queue.push_back(neighbor);
            }
        }

        None
    }

    pub fn filtered_count(&self, total_notes: usize) -> usize {
        total_notes.saturating_sub(self.accessible_notes.len())
    }
}

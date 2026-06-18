/// Deterministic structural summary: keep only markdown headers.
pub fn structural_summary(content: &str) -> String {
    content
        .lines()
        .filter(|line| line.starts_with('#'))
        .collect::<Vec<_>>()
        .join("\n")
}

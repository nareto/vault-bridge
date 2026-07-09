-- Store raw vault file content for supported text file types (.md, .base).
-- Separate from the parsed note index; .md files appear in both tables.

CREATE TABLE IF NOT EXISTS vault_files (
    path TEXT PRIMARY KEY,
    content TEXT NOT NULL,
    couchdb_rev TEXT NOT NULL,
    created_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    indexed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

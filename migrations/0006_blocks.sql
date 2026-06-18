CREATE TABLE IF NOT EXISTS blocks (
    id TEXT PRIMARY KEY,               -- "{note_id}##{block_index}"
    note_id TEXT NOT NULL REFERENCES notes(id) ON DELETE CASCADE,
    block_index INT NOT NULL,
    heading_path TEXT NOT NULL,         -- "## Setup > ### Config" or "" for preamble
    breadcrumb TEXT NOT NULL,           -- full breadcrumb used as embedding prefix
    content TEXT NOT NULL,
    content_hash TEXT NOT NULL,         -- SHA-256, for skipping unchanged blocks
    embedding vector,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_blocks_note_id ON blocks(note_id);
-- HNSW index on blocks.embedding is created by ensure_embedding_schema() at runtime
-- (after the column is typed to the configured dimensions)

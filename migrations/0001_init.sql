CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE IF NOT EXISTS notes (
    id TEXT PRIMARY KEY,
    path TEXT NOT NULL,
    title TEXT NOT NULL,
    content TEXT NOT NULL,
    summary TEXT NOT NULL DEFAULT '',
    frontmatter JSONB NOT NULL DEFAULT '{}'::jsonb,
    sensitivity TEXT NOT NULL DEFAULT 'public',
    embedding vector,
    couchdb_rev TEXT NOT NULL,
    created_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    indexed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS links (
    source_id TEXT NOT NULL REFERENCES notes(id) ON DELETE CASCADE,
    target_id TEXT NOT NULL,
    context_text TEXT,
    position INT,
    PRIMARY KEY (source_id, target_id)
);

CREATE INDEX IF NOT EXISTS idx_links_target ON links(target_id);

CREATE TABLE IF NOT EXISTS tags (
    note_id TEXT NOT NULL REFERENCES notes(id) ON DELETE CASCADE,
    tag TEXT NOT NULL,
    PRIMARY KEY (note_id, tag)
);

CREATE INDEX IF NOT EXISTS idx_tags_tag ON tags(tag);

CREATE TABLE IF NOT EXISTS sync_state (
    id INT PRIMARY KEY DEFAULT 1 CHECK (id = 1),
    last_seq TEXT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS chunk_staging (
    parent_id TEXT NOT NULL,
    chunk_index INT NOT NULL,
    chunk_count INT NOT NULL,
    content TEXT NOT NULL,
    couchdb_rev TEXT NOT NULL,
    received_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (parent_id, chunk_index)
);

CREATE TABLE IF NOT EXISTS scopes (
    name TEXT PRIMARY KEY,
    allowed_sensitivities TEXT[] NOT NULL,
    allowed_write_paths TEXT[] NOT NULL DEFAULT '{}',
    description TEXT
);

INSERT INTO scopes (name, allowed_sensitivities, allowed_write_paths, description)
VALUES
    ('external', ARRAY['public'], ARRAY['11New/'], 'External AI providers (OpenAI, Anthropic)'),
    ('local', ARRAY['public', 'personal'], ARRAY['11New/'], 'Trusted local agents'),
    ('admin', ARRAY['public', 'personal'], ARRAY['11New/'], 'Admin and debugging')
ON CONFLICT (name) DO UPDATE
SET
    allowed_sensitivities = EXCLUDED.allowed_sensitivities,
    allowed_write_paths = EXCLUDED.allowed_write_paths,
    description = EXCLUDED.description;

CREATE TABLE IF NOT EXISTS api_keys (
    key_hash TEXT PRIMARY KEY,
    scope_name TEXT NOT NULL REFERENCES scopes(name),
    description TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_used_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS access_log (
    id BIGSERIAL PRIMARY KEY,
    timestamp TIMESTAMPTZ NOT NULL DEFAULT now(),
    scope TEXT NOT NULL,
    endpoint TEXT NOT NULL,
    query_params JSONB,
    notes_returned TEXT[],
    notes_filtered_count INT DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_notes_fts
    ON notes USING gin (to_tsvector('english', coalesce(title, '') || ' ' || coalesce(content, '')));

CREATE INDEX IF NOT EXISTS idx_notes_embedding
    ON notes USING hnsw (embedding vector_cosine_ops)
    WITH (m = 16, ef_construction = 64);
CREATE INDEX IF NOT EXISTS idx_notes_sensitivity ON notes(sensitivity);

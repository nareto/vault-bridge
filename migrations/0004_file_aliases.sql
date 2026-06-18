CREATE TABLE IF NOT EXISTS file_aliases (
    file_doc_id TEXT PRIMARY KEY,
    note_path TEXT NOT NULL,
    couchdb_rev TEXT NOT NULL,
    children TEXT[] NOT NULL DEFAULT '{}',
    ctime BIGINT NOT NULL DEFAULT 0,
    mtime BIGINT NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_file_aliases_note_path ON file_aliases(note_path);

ALTER TABLE blocks
    ADD COLUMN IF NOT EXISTS embedding_failures INT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS embedding_failed_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS last_embedding_error TEXT;

CREATE INDEX IF NOT EXISTS idx_blocks_embedding_pending
    ON blocks(embedding_failures, note_id, block_index)
    WHERE embedding IS NULL;

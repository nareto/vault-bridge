ALTER TABLE embedding_schema
    ADD COLUMN IF NOT EXISTS hnsw_m INT NOT NULL DEFAULT 16 CHECK (hnsw_m > 0),
    ADD COLUMN IF NOT EXISTS hnsw_ef_construction INT NOT NULL DEFAULT 64 CHECK (hnsw_ef_construction > 0);

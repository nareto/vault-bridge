CREATE TABLE IF NOT EXISTS embedding_runtime (
    id INT PRIMARY KEY DEFAULT 1 CHECK (id = 1),
    last_success_at TIMESTAMPTZ,
    last_error_at TIMESTAMPTZ,
    last_error TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS embedding_schema (
    id INT PRIMARY KEY DEFAULT 1 CHECK (id = 1),
    model TEXT NOT NULL,
    dimensions INT NOT NULL CHECK (dimensions > 0),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Monotonic generation used to invalidate caches across API and worker processes.

CREATE TABLE IF NOT EXISTS store_state (
    id SMALLINT PRIMARY KEY DEFAULT 1 CHECK (id = 1),
    generation BIGINT NOT NULL DEFAULT 0 CHECK (generation >= 0)
);

INSERT INTO store_state (id, generation)
VALUES (1, 0)
ON CONFLICT (id) DO NOTHING;

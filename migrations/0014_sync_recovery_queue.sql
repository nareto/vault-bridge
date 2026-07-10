CREATE TABLE IF NOT EXISTS sync_recovery_queue (
    recovery_kind TEXT NOT NULL,
    target_id TEXT NOT NULL,
    failure_count INT NOT NULL DEFAULT 0,
    next_retry_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    quarantined_at TIMESTAMPTZ,
    last_failure_kind TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (recovery_kind, target_id)
);

CREATE INDEX IF NOT EXISTS idx_sync_recovery_queue_due
    ON sync_recovery_queue (next_retry_at, recovery_kind, target_id)
    WHERE quarantined_at IS NULL;

ALTER TABLE sync_recovery_queue
    ADD COLUMN IF NOT EXISTS last_reactivated_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS reactivated_source_revision TEXT;

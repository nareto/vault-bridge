ALTER TABLE sync_recovery_queue
    ADD COLUMN IF NOT EXISTS expected_child_count INT,
    ADD COLUMN IF NOT EXISTS live_child_count INT,
    ADD COLUMN IF NOT EXISTS missing_child_count INT,
    ADD COLUMN IF NOT EXISTS tombstoned_child_count INT,
    ADD COLUMN IF NOT EXISTS last_diagnosed_at TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS idx_sync_recovery_queue_child_failures
    ON sync_recovery_queue (last_failure_kind)
    WHERE recovery_kind = 'file_alias'
      AND last_failure_kind IN (
          'missing_children',
          'tombstoned_children',
          'mixed_unavailable_children'
      );

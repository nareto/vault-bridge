ALTER TABLE sync_state
    ADD COLUMN IF NOT EXISTS couchdb_current_seq TEXT;

UPDATE sync_state
SET couchdb_current_seq = last_seq
WHERE couchdb_current_seq IS NULL;

ALTER TABLE sync_state
    ALTER COLUMN couchdb_current_seq SET NOT NULL;

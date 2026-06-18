DROP INDEX IF EXISTS idx_notes_sensitivity;

ALTER TABLE notes DROP COLUMN IF EXISTS sensitivity;

ALTER TABLE api_keys DROP CONSTRAINT IF EXISTS api_keys_scope_name_fkey;
ALTER TABLE api_keys DROP COLUMN IF EXISTS scope_name;
ALTER TABLE api_keys ADD COLUMN IF NOT EXISTS name TEXT NOT NULL DEFAULT 'legacy';
ALTER TABLE api_keys ADD COLUMN IF NOT EXISTS context TEXT NOT NULL DEFAULT 'external';
ALTER TABLE api_keys ALTER COLUMN name DROP DEFAULT;
ALTER TABLE api_keys ALTER COLUMN context DROP DEFAULT;

DROP TABLE IF EXISTS scopes;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_name = 'access_log'
          AND column_name = 'scope'
    ) AND NOT EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_name = 'access_log'
          AND column_name = 'context'
    ) THEN
        ALTER TABLE access_log RENAME COLUMN scope TO context;
    END IF;
END $$;

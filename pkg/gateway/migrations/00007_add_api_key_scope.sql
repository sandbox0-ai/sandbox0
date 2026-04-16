-- +goose Up
ALTER TABLE IF EXISTS api_keys
    ADD COLUMN IF NOT EXISTS scope TEXT NOT NULL DEFAULT 'team';

ALTER TABLE IF EXISTS api_keys
    DROP CONSTRAINT IF EXISTS api_keys_scope_check;

ALTER TABLE IF EXISTS api_keys
    ADD CONSTRAINT api_keys_scope_check CHECK (scope IN ('team', 'platform'));

-- +goose Down
ALTER TABLE IF EXISTS api_keys
    DROP CONSTRAINT IF EXISTS api_keys_scope_check;

ALTER TABLE IF EXISTS api_keys
    DROP COLUMN IF EXISTS scope;

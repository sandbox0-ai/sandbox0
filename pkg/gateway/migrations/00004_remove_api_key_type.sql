-- +goose Up
ALTER TABLE IF EXISTS api_keys
    DROP COLUMN IF EXISTS type;

-- +goose Down
ALTER TABLE IF EXISTS api_keys
    ADD COLUMN IF NOT EXISTS type TEXT NOT NULL DEFAULT 'service' CHECK (type IN ('user', 'service', 'internal'));

ALTER TABLE IF EXISTS api_keys
    ALTER COLUMN type DROP DEFAULT;

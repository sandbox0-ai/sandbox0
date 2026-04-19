-- +goose Up
ALTER TABLE sandbox_volumes
DROP COLUMN IF EXISTS backend_type;

-- +goose Down
ALTER TABLE sandbox_volumes
ADD COLUMN IF NOT EXISTS backend_type TEXT NOT NULL DEFAULT 's0fs';

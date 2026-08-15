-- +goose Up
ALTER TABLE sandbox_volumes
ADD COLUMN IF NOT EXISTS source_volume_id TEXT;

CREATE INDEX IF NOT EXISTS idx_sandbox_volumes_source_id ON sandbox_volumes(source_volume_id);

-- +goose Down
DROP INDEX IF EXISTS idx_sandbox_volumes_source_id;
ALTER TABLE sandbox_volumes DROP COLUMN IF EXISTS source_volume_id;

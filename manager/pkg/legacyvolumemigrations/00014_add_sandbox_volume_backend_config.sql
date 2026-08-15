-- +goose Up
ALTER TABLE sandbox_volumes
ADD COLUMN IF NOT EXISTS backend TEXT NOT NULL DEFAULT 's0fs',
ADD COLUMN IF NOT EXISTS backend_config JSONB NOT NULL DEFAULT '{}'::jsonb;

UPDATE sandbox_volumes
SET backend = 's0fs'
WHERE trim(coalesce(backend, '')) = '';

UPDATE sandbox_volumes
SET backend_config = '{}'::jsonb
WHERE backend_config IS NULL;

-- +goose Down
ALTER TABLE sandbox_volumes
DROP COLUMN IF EXISTS backend_config,
DROP COLUMN IF EXISTS backend;

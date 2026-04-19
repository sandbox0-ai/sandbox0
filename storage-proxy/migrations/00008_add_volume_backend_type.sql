-- +goose Up
ALTER TABLE sandbox_volumes
ADD COLUMN IF NOT EXISTS backend_type TEXT NOT NULL DEFAULT 's0fs';

UPDATE sandbox_volumes
SET backend_type = 's0fs'
WHERE trim(coalesce(backend_type, '')) = '';

-- +goose Down
ALTER TABLE sandbox_volumes DROP COLUMN IF EXISTS backend_type;

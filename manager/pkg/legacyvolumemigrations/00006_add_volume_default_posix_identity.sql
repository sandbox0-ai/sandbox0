-- +goose Up
ALTER TABLE sandbox_volumes
ADD COLUMN IF NOT EXISTS default_posix_uid BIGINT,
ADD COLUMN IF NOT EXISTS default_posix_gid BIGINT;

-- +goose Down
ALTER TABLE sandbox_volumes
DROP COLUMN IF EXISTS default_posix_gid,
DROP COLUMN IF EXISTS default_posix_uid;

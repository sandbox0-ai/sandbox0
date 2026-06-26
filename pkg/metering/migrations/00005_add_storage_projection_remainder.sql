-- +goose Up

ALTER TABLE storage_projection_state
ADD COLUMN IF NOT EXISTS unbilled_byte_nanoseconds BIGINT NOT NULL DEFAULT 0;

-- +goose Down

ALTER TABLE storage_projection_state
DROP COLUMN IF EXISTS unbilled_byte_nanoseconds;

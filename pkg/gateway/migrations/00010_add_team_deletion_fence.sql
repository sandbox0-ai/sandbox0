-- +goose Up
ALTER TABLE teams
    ADD COLUMN IF NOT EXISTS deletion_fenced_at TIMESTAMPTZ;

-- +goose Down
ALTER TABLE teams
    DROP COLUMN IF EXISTS deletion_fenced_at;

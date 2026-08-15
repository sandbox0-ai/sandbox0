-- +goose Up
ALTER TABLE sandbox_volume_sync_journal
    ADD COLUMN IF NOT EXISTS entry_kind TEXT,
    ADD COLUMN IF NOT EXISTS mode BIGINT,
    ADD COLUMN IF NOT EXISTS content_ref TEXT;

-- +goose Down
ALTER TABLE sandbox_volume_sync_journal
    DROP COLUMN IF EXISTS content_ref,
    DROP COLUMN IF EXISTS mode,
    DROP COLUMN IF EXISTS entry_kind;

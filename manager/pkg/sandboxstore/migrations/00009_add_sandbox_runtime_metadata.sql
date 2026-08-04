-- +goose Up

ALTER TABLE manager.sandboxes
    ADD COLUMN IF NOT EXISTS webhook_state_volume_id TEXT NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS owner_kind TEXT NOT NULL DEFAULT '';

-- +goose Down

ALTER TABLE manager.sandboxes
    DROP COLUMN IF EXISTS owner_kind,
    DROP COLUMN IF EXISTS webhook_state_volume_id;

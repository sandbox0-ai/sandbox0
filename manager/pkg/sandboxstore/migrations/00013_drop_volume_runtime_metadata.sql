-- +goose Up

ALTER TABLE manager.sandboxes
    DROP COLUMN IF EXISTS webhook_state_volume_id,
    DROP COLUMN IF EXISTS mounts;

-- +goose Down

-- Removed runtime metadata cannot be reconstructed after volume retirement.

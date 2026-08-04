-- +goose Up

ALTER TABLE manager.sandbox_lifecycle_txns
    ADD COLUMN IF NOT EXISTS source TEXT NOT NULL DEFAULT 'manual',
    ADD COLUMN IF NOT EXISTS cancelable BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS cancel_reason TEXT NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS cancel_requested_at TIMESTAMPTZ;

-- +goose Down

ALTER TABLE manager.sandbox_lifecycle_txns
    DROP COLUMN IF EXISTS cancel_requested_at,
    DROP COLUMN IF EXISTS cancel_reason,
    DROP COLUMN IF EXISTS cancelable,
    DROP COLUMN IF EXISTS source;

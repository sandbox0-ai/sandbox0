-- +goose Up

ALTER TABLE sandboxes
    ADD COLUMN IF NOT EXISTS rootfs_id TEXT NOT NULL DEFAULT '';

CREATE INDEX IF NOT EXISTS idx_sandboxes_rootfs_id
    ON sandboxes(rootfs_id)
    WHERE rootfs_id <> '';

-- +goose Down

DROP INDEX IF EXISTS idx_sandboxes_rootfs_id;

ALTER TABLE sandboxes
    DROP COLUMN IF EXISTS rootfs_id;

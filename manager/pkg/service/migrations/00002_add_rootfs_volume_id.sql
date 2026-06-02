-- +goose Up

ALTER TABLE sandboxes
    ADD COLUMN IF NOT EXISTS rootfs_volume_id TEXT NOT NULL DEFAULT '';

ALTER TABLE sandboxes
    ADD COLUMN IF NOT EXISTS current_node_name TEXT NOT NULL DEFAULT '';

-- +goose Down

ALTER TABLE sandboxes
    DROP COLUMN IF EXISTS current_node_name;

ALTER TABLE sandboxes
    DROP COLUMN IF EXISTS rootfs_volume_id;

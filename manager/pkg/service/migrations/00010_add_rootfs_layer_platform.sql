-- +goose Up

ALTER TABLE manager.rootfs_layers
    ADD COLUMN IF NOT EXISTS platform_os TEXT NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS platform_architecture TEXT NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS platform_variant TEXT NOT NULL DEFAULT '';

-- +goose Down

ALTER TABLE manager.rootfs_layers
    DROP COLUMN IF EXISTS platform_variant,
    DROP COLUMN IF EXISTS platform_architecture,
    DROP COLUMN IF EXISTS platform_os;

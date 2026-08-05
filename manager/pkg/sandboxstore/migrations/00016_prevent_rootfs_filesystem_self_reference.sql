-- +goose Up

UPDATE manager.rootfs_filesystems
SET source_filesystem_id = NULL,
    updated_at = NOW()
WHERE source_filesystem_id = filesystem_id;

ALTER TABLE manager.rootfs_filesystems
    ADD CONSTRAINT rootfs_filesystems_source_not_self
    CHECK (source_filesystem_id IS NULL OR source_filesystem_id <> filesystem_id);

-- +goose Down

ALTER TABLE manager.rootfs_filesystems
    DROP CONSTRAINT rootfs_filesystems_source_not_self;

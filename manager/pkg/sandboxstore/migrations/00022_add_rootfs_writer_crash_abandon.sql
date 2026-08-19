-- +goose Up

ALTER TABLE manager.rootfs_writer_grants
    DROP CONSTRAINT IF EXISTS rootfs_writer_grants_retire_kind_check;
ALTER TABLE manager.rootfs_writer_grants
    ADD CONSTRAINT rootfs_writer_grants_retire_kind_check CHECK (retire_kind IN (
        '', 'planned_publish', 'prelaunch_abort', 'crash_abandon'
    ));

-- +goose Down

ALTER TABLE manager.rootfs_writer_grants
    DROP CONSTRAINT IF EXISTS rootfs_writer_grants_retire_kind_check;
ALTER TABLE manager.rootfs_writer_grants
    ADD CONSTRAINT rootfs_writer_grants_retire_kind_check CHECK (retire_kind IN (
        '', 'planned_publish', 'prelaunch_abort'
    ));

-- +goose Up

CREATE INDEX idx_rootfs_snapshots_source_retention
    ON manager.rootfs_snapshots (source_sandbox_id, created_at DESC, snapshot_id DESC)
    WHERE source_sandbox_id <> '' AND snapshot_id NOT LIKE 'template-build-%';

-- +goose Down

DROP INDEX manager.idx_rootfs_snapshots_source_retention;

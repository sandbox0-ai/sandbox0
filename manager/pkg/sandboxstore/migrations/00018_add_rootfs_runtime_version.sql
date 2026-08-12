-- +goose Up

ALTER TABLE manager.sandboxes
    ADD COLUMN rootfs_runtime_version TEXT NOT NULL DEFAULT 'legacy-v1';

ALTER TABLE manager.sandboxes
    ADD CONSTRAINT sandboxes_rootfs_runtime_version_check
    CHECK (rootfs_runtime_version IN ('legacy-v1', 's0fs-v2'));

ALTER TABLE manager.sandbox_lifecycle_txns
    ADD COLUMN rootfs_runtime_version TEXT NOT NULL DEFAULT 'legacy-v1';

ALTER TABLE manager.sandbox_lifecycle_txns
    ADD CONSTRAINT sandbox_lifecycle_txns_rootfs_runtime_version_check
    CHECK (rootfs_runtime_version IN ('legacy-v1', 's0fs-v2'));

-- Existing COW v3 heads were experimental and never shipped as the durable
-- format on main. Rows created after this migration are explicitly routed by
-- the sandbox runtime version instead of inferring a writer from nullable head
-- columns.

-- +goose Down

ALTER TABLE manager.sandbox_lifecycle_txns
    DROP CONSTRAINT sandbox_lifecycle_txns_rootfs_runtime_version_check,
    DROP COLUMN rootfs_runtime_version;

ALTER TABLE manager.sandboxes
    DROP CONSTRAINT sandboxes_rootfs_runtime_version_check,
    DROP COLUMN rootfs_runtime_version;

-- +goose Up

ALTER TABLE manager.rootfs_snapshots
    ALTER COLUMN head_layer_id DROP NOT NULL,
    ADD COLUMN IF NOT EXISTS head_generation_id TEXT
        REFERENCES manager.rootfs_generations(generation_id) ON DELETE RESTRICT;

ALTER TABLE manager.rootfs_snapshots
    ADD CONSTRAINT rootfs_snapshots_head_shape_check CHECK (
        (head_layer_id IS NOT NULL AND head_generation_id IS NULL)
        OR
        (head_layer_id IS NULL AND head_generation_id IS NOT NULL)
    );

CREATE INDEX IF NOT EXISTS idx_rootfs_snapshots_generation_head
    ON manager.rootfs_snapshots(head_generation_id)
    WHERE head_generation_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS manager.rootfs_head_rollbacks (
    operation_id TEXT PRIMARY KEY,
    filesystem_id TEXT NOT NULL
        REFERENCES manager.rootfs_filesystems(filesystem_id) ON DELETE RESTRICT,
    sandbox_id TEXT NOT NULL,
    team_id TEXT NOT NULL,
    operation_kind TEXT NOT NULL CHECK (operation_kind IN ('restore', 'rebase')),
    old_generation_id TEXT NOT NULL
        REFERENCES manager.rootfs_generations(generation_id) ON DELETE RESTRICT,
    new_generation_id TEXT NOT NULL
        REFERENCES manager.rootfs_generations(generation_id) ON DELETE RESTRICT,
    health_check_digest BYTEA,
    state TEXT NOT NULL DEFAULT 'available' CHECK (state IN ('available', 'rolled_back', 'expired')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at TIMESTAMPTZ,
    rolled_back_at TIMESTAMPTZ,
    CHECK (old_generation_id <> new_generation_id),
    CHECK (health_check_digest IS NULL OR octet_length(health_check_digest) = 32)
);

CREATE INDEX IF NOT EXISTS idx_rootfs_head_rollbacks_filesystem_available
    ON manager.rootfs_head_rollbacks(filesystem_id, created_at DESC)
    WHERE state = 'available';

-- +goose Down

DROP INDEX IF EXISTS manager.idx_rootfs_head_rollbacks_filesystem_available;
DROP TABLE IF EXISTS manager.rootfs_head_rollbacks;
DROP INDEX IF EXISTS manager.idx_rootfs_snapshots_generation_head;
ALTER TABLE manager.rootfs_snapshots
    DROP CONSTRAINT IF EXISTS rootfs_snapshots_head_shape_check,
    DROP COLUMN IF EXISTS head_generation_id;
ALTER TABLE manager.rootfs_snapshots
    ALTER COLUMN head_layer_id SET NOT NULL;

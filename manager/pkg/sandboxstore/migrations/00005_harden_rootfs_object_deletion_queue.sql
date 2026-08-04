-- +goose Up

ALTER TABLE manager.rootfs_object_deletions
    ADD COLUMN IF NOT EXISTS last_attempt_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    ADD COLUMN IF NOT EXISTS claimed_by TEXT NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS claimed_until TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS dead_lettered_at TIMESTAMPTZ;

UPDATE manager.rootfs_object_deletions
SET next_attempt_at = COALESCE(next_attempt_at, updated_at, NOW())
WHERE next_attempt_at IS NULL;

CREATE INDEX IF NOT EXISTS idx_rootfs_object_deletions_due
    ON manager.rootfs_object_deletions(next_attempt_at ASC, updated_at ASC)
    WHERE dead_lettered_at IS NULL;

CREATE INDEX IF NOT EXISTS idx_rootfs_object_deletions_claim
    ON manager.rootfs_object_deletions(claimed_until ASC)
    WHERE claimed_until IS NOT NULL
      AND dead_lettered_at IS NULL;

CREATE INDEX IF NOT EXISTS idx_rootfs_object_deletions_dead_lettered
    ON manager.rootfs_object_deletions(dead_lettered_at ASC)
    WHERE dead_lettered_at IS NOT NULL;

-- +goose Down

DROP INDEX IF EXISTS manager.idx_rootfs_object_deletions_dead_lettered;
DROP INDEX IF EXISTS manager.idx_rootfs_object_deletions_claim;
DROP INDEX IF EXISTS manager.idx_rootfs_object_deletions_due;

ALTER TABLE manager.rootfs_object_deletions
    DROP COLUMN IF EXISTS dead_lettered_at,
    DROP COLUMN IF EXISTS claimed_until,
    DROP COLUMN IF EXISTS claimed_by,
    DROP COLUMN IF EXISTS next_attempt_at,
    DROP COLUMN IF EXISTS last_attempt_at;

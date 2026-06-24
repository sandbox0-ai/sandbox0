-- +goose Up

DROP TRIGGER IF EXISTS update_rootfs_objects_updated_at ON manager.rootfs_objects;
DROP INDEX IF EXISTS manager.idx_rootfs_objects_missing;
DROP INDEX IF EXISTS manager.idx_rootfs_objects_deleted;
DROP INDEX IF EXISTS manager.idx_rootfs_objects_team_updated;
DROP TABLE IF EXISTS manager.rootfs_objects;

DROP TRIGGER IF EXISTS update_rootfs_object_deletions_updated_at ON manager.rootfs_object_deletions;
DROP INDEX IF EXISTS manager.idx_rootfs_object_deletions_dead_lettered;
DROP INDEX IF EXISTS manager.idx_rootfs_object_deletions_claim;
DROP INDEX IF EXISTS manager.idx_rootfs_object_deletions_due;
DROP INDEX IF EXISTS manager.idx_rootfs_object_deletions_updated;
DROP TABLE IF EXISTS manager.rootfs_object_deletions;

DROP INDEX IF EXISTS manager.idx_sandbox_rootfs_heads_team_updated;
DROP TABLE IF EXISTS manager.sandbox_rootfs_heads;

DROP INDEX IF EXISTS manager.idx_sandbox_rootfs_states_team_updated;
DROP TABLE IF EXISTS manager.sandbox_rootfs_states;

ALTER TABLE manager.rootfs_layers
    DROP COLUMN IF EXISTS diff_object_key,
    DROP COLUMN IF EXISTS diff_size,
    DROP COLUMN IF EXISTS diff_media_type,
    DROP COLUMN IF EXISTS diff_id,
    DROP COLUMN IF EXISTS diff_digest;

-- +goose Down

ALTER TABLE manager.rootfs_layers
    ADD COLUMN IF NOT EXISTS diff_digest TEXT NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS diff_id TEXT NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS diff_media_type TEXT NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS diff_size BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS diff_object_key TEXT NOT NULL DEFAULT '';

CREATE TABLE IF NOT EXISTS manager.sandbox_rootfs_states (
    sandbox_id TEXT NOT NULL REFERENCES manager.sandboxes(sandbox_id) ON DELETE CASCADE,
    team_id TEXT NOT NULL,
    runtime_generation BIGINT NOT NULL,
    runtime TEXT NOT NULL DEFAULT '',
    runtime_handler TEXT NOT NULL DEFAULT '',
    base_image_ref TEXT NOT NULL DEFAULT '',
    base_image_digest TEXT NOT NULL DEFAULT '',
    snapshotter TEXT NOT NULL DEFAULT '',
    snapshot_parent TEXT NOT NULL DEFAULT '',
    snapshot_parent_chain JSONB NOT NULL DEFAULT '[]',
    diff_digest TEXT NOT NULL,
    diff_media_type TEXT NOT NULL DEFAULT '',
    diff_size BIGINT NOT NULL DEFAULT 0,
    diff_object_key TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (sandbox_id, runtime_generation)
);

CREATE INDEX IF NOT EXISTS idx_sandbox_rootfs_states_team_updated
    ON manager.sandbox_rootfs_states(team_id, updated_at DESC);

CREATE TABLE IF NOT EXISTS manager.sandbox_rootfs_heads (
    sandbox_id TEXT PRIMARY KEY REFERENCES manager.sandboxes(sandbox_id) ON DELETE CASCADE,
    team_id TEXT NOT NULL,
    head_layer_id TEXT NOT NULL REFERENCES manager.rootfs_layers(layer_id) ON DELETE RESTRICT,
    runtime_generation BIGINT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_sandbox_rootfs_heads_team_updated
    ON manager.sandbox_rootfs_heads(team_id, updated_at DESC);

CREATE TABLE IF NOT EXISTS manager.rootfs_object_deletions (
    object_key TEXT PRIMARY KEY,
    team_id TEXT NOT NULL DEFAULT '',
    attempts INTEGER NOT NULL DEFAULT 0,
    last_error TEXT NOT NULL DEFAULT '',
    last_attempt_at TIMESTAMPTZ,
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    claimed_by TEXT NOT NULL DEFAULT '',
    claimed_until TIMESTAMPTZ,
    dead_lettered_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_rootfs_object_deletions_updated
    ON manager.rootfs_object_deletions(updated_at ASC);

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

DROP TRIGGER IF EXISTS update_rootfs_object_deletions_updated_at ON manager.rootfs_object_deletions;
CREATE TRIGGER update_rootfs_object_deletions_updated_at
    BEFORE UPDATE ON manager.rootfs_object_deletions
    FOR EACH ROW
    EXECUTE FUNCTION manager.update_updated_at_column();

CREATE TABLE IF NOT EXISTS manager.rootfs_objects (
    object_key TEXT PRIMARY KEY,
    team_id TEXT NOT NULL DEFAULT '',
    diff_digest TEXT NOT NULL DEFAULT '',
    diff_media_type TEXT NOT NULL DEFAULT '',
    diff_size BIGINT NOT NULL DEFAULT 0,
    first_layer_id TEXT NOT NULL DEFAULT '',
    last_referenced_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    missing_at TIMESTAMPTZ,
    deleted_at TIMESTAMPTZ,
    last_error TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_rootfs_objects_team_updated
    ON manager.rootfs_objects(team_id, updated_at DESC);

CREATE INDEX IF NOT EXISTS idx_rootfs_objects_deleted
    ON manager.rootfs_objects(deleted_at)
    WHERE deleted_at IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_rootfs_objects_missing
    ON manager.rootfs_objects(missing_at)
    WHERE missing_at IS NOT NULL;

DROP TRIGGER IF EXISTS update_rootfs_objects_updated_at ON manager.rootfs_objects;
CREATE TRIGGER update_rootfs_objects_updated_at
    BEFORE UPDATE ON manager.rootfs_objects
    FOR EACH ROW
    EXECUTE FUNCTION manager.update_updated_at_column();

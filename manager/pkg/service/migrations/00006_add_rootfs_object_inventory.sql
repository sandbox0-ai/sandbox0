-- +goose Up

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

INSERT INTO manager.rootfs_objects (
    object_key, team_id, diff_digest, diff_media_type, diff_size,
    first_layer_id, last_referenced_at, created_at, updated_at
)
SELECT DISTINCT ON (diff_object_key)
    diff_object_key,
    team_id,
    diff_digest,
    diff_media_type,
    diff_size,
    layer_id,
    created_at,
    created_at,
    created_at
FROM manager.rootfs_layers
WHERE diff_object_key <> ''
ORDER BY diff_object_key, created_at ASC
ON CONFLICT (object_key) DO NOTHING;

-- +goose Down

DROP TRIGGER IF EXISTS update_rootfs_objects_updated_at ON manager.rootfs_objects;
DROP INDEX IF EXISTS manager.idx_rootfs_objects_missing;
DROP INDEX IF EXISTS manager.idx_rootfs_objects_deleted;
DROP INDEX IF EXISTS manager.idx_rootfs_objects_team_updated;
DROP TABLE IF EXISTS manager.rootfs_objects;

-- +goose Up

WITH latest_legacy AS (
    SELECT DISTINCT ON (sandbox_id)
        sandbox_id, team_id, runtime_generation, runtime, runtime_handler,
        base_image_ref, base_image_digest, snapshotter, snapshot_parent,
        snapshot_parent_chain, diff_digest, diff_media_type, diff_size,
        diff_object_key, created_at
    FROM manager.sandbox_rootfs_states
    ORDER BY sandbox_id, runtime_generation DESC, updated_at DESC
),
legacy_without_head AS (
    SELECT l.*
    FROM latest_legacy l
    LEFT JOIN manager.sandbox_rootfs_heads h ON h.sandbox_id = l.sandbox_id
    WHERE h.sandbox_id IS NULL
)
INSERT INTO manager.rootfs_layers (
    layer_id, parent_layer_id, source_sandbox_id, team_id, runtime_generation,
    runtime, runtime_handler, base_image_ref, base_image_digest, snapshotter,
    snapshot_parent, snapshot_parent_chain, diff_digest, diff_id, diff_media_type,
    diff_size, diff_object_key, created_at
)
SELECT
    'legacy:' || sandbox_id || ':' || runtime_generation,
    NULL,
    sandbox_id,
    team_id,
    runtime_generation,
    runtime,
    runtime_handler,
    base_image_ref,
    base_image_digest,
    snapshotter,
    snapshot_parent,
    snapshot_parent_chain,
    diff_digest,
    '',
    diff_media_type,
    diff_size,
    diff_object_key,
    created_at
FROM legacy_without_head
ON CONFLICT (layer_id) DO NOTHING;

WITH latest_legacy AS (
    SELECT DISTINCT ON (sandbox_id)
        sandbox_id, team_id, runtime_generation
    FROM manager.sandbox_rootfs_states
    ORDER BY sandbox_id, runtime_generation DESC, updated_at DESC
)
INSERT INTO manager.sandbox_rootfs_heads (
    sandbox_id, team_id, head_layer_id, runtime_generation, updated_at
)
SELECT
    l.sandbox_id,
    l.team_id,
    'legacy:' || l.sandbox_id || ':' || l.runtime_generation,
    l.runtime_generation,
    NOW()
FROM latest_legacy l
LEFT JOIN manager.sandbox_rootfs_heads h ON h.sandbox_id = l.sandbox_id
WHERE h.sandbox_id IS NULL
ON CONFLICT (sandbox_id) DO NOTHING;

CREATE TABLE IF NOT EXISTS manager.rootfs_filesystems (
    filesystem_id TEXT PRIMARY KEY,
    team_id TEXT NOT NULL,
    source_filesystem_id TEXT REFERENCES manager.rootfs_filesystems(filesystem_id) ON DELETE RESTRICT,
    head_layer_id TEXT REFERENCES manager.rootfs_layers(layer_id) ON DELETE RESTRICT,
    base_image_ref TEXT NOT NULL DEFAULT '',
    base_image_digest TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_rootfs_filesystems_team_updated
    ON manager.rootfs_filesystems(team_id, updated_at DESC);

CREATE INDEX IF NOT EXISTS idx_rootfs_filesystems_head
    ON manager.rootfs_filesystems(head_layer_id)
    WHERE head_layer_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_rootfs_filesystems_source
    ON manager.rootfs_filesystems(source_filesystem_id)
    WHERE source_filesystem_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS manager.sandbox_rootfs_bindings (
    sandbox_id TEXT PRIMARY KEY REFERENCES manager.sandboxes(sandbox_id) ON DELETE CASCADE,
    filesystem_id TEXT NOT NULL REFERENCES manager.rootfs_filesystems(filesystem_id) ON DELETE RESTRICT,
    team_id TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_sandbox_rootfs_bindings_filesystem
    ON manager.sandbox_rootfs_bindings(filesystem_id);

CREATE INDEX IF NOT EXISTS idx_sandbox_rootfs_bindings_team_updated
    ON manager.sandbox_rootfs_bindings(team_id, updated_at DESC);

-- +goose StatementBegin
CREATE OR REPLACE FUNCTION manager.update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
-- +goose StatementEnd

DROP TRIGGER IF EXISTS update_rootfs_filesystems_updated_at ON manager.rootfs_filesystems;
CREATE TRIGGER update_rootfs_filesystems_updated_at
    BEFORE UPDATE ON manager.rootfs_filesystems
    FOR EACH ROW
    EXECUTE FUNCTION manager.update_updated_at_column();

DROP TRIGGER IF EXISTS update_sandbox_rootfs_bindings_updated_at ON manager.sandbox_rootfs_bindings;
CREATE TRIGGER update_sandbox_rootfs_bindings_updated_at
    BEFORE UPDATE ON manager.sandbox_rootfs_bindings
    FOR EACH ROW
    EXECUTE FUNCTION manager.update_updated_at_column();

INSERT INTO manager.rootfs_filesystems (
    filesystem_id, team_id, head_layer_id, base_image_ref, base_image_digest,
    created_at, updated_at
)
SELECT
    h.sandbox_id,
    h.team_id,
    h.head_layer_id,
    COALESCE(l.base_image_ref, ''),
    COALESCE(l.base_image_digest, ''),
    COALESCE(l.created_at, NOW()),
    h.updated_at
FROM manager.sandbox_rootfs_heads h
LEFT JOIN manager.rootfs_layers l ON l.layer_id = h.head_layer_id
ON CONFLICT (filesystem_id) DO NOTHING;

INSERT INTO manager.sandbox_rootfs_bindings (
    sandbox_id, filesystem_id, team_id, created_at, updated_at
)
SELECT
    h.sandbox_id,
    h.sandbox_id,
    h.team_id,
    h.updated_at,
    h.updated_at
FROM manager.sandbox_rootfs_heads h
ON CONFLICT (sandbox_id) DO NOTHING;

ALTER TABLE manager.rootfs_snapshots
    ADD COLUMN IF NOT EXISTS filesystem_id TEXT REFERENCES manager.rootfs_filesystems(filesystem_id) ON DELETE RESTRICT;

UPDATE manager.rootfs_snapshots s
SET filesystem_id = b.filesystem_id
FROM manager.sandbox_rootfs_bindings b
WHERE s.filesystem_id IS NULL
  AND s.source_sandbox_id = b.sandbox_id;

CREATE INDEX IF NOT EXISTS idx_rootfs_snapshots_filesystem
    ON manager.rootfs_snapshots(filesystem_id)
    WHERE filesystem_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS manager.rootfs_object_deletions (
    object_key TEXT PRIMARY KEY,
    team_id TEXT NOT NULL DEFAULT '',
    attempts INTEGER NOT NULL DEFAULT 0,
    last_error TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_rootfs_object_deletions_updated
    ON manager.rootfs_object_deletions(updated_at ASC);

DROP TRIGGER IF EXISTS update_rootfs_object_deletions_updated_at ON manager.rootfs_object_deletions;
CREATE TRIGGER update_rootfs_object_deletions_updated_at
    BEFORE UPDATE ON manager.rootfs_object_deletions
    FOR EACH ROW
    EXECUTE FUNCTION manager.update_updated_at_column();

-- +goose Down

DROP TRIGGER IF EXISTS update_rootfs_object_deletions_updated_at ON manager.rootfs_object_deletions;
DROP INDEX IF EXISTS manager.idx_rootfs_object_deletions_updated;
DROP TABLE IF EXISTS manager.rootfs_object_deletions;
DROP INDEX IF EXISTS manager.idx_rootfs_snapshots_filesystem;
ALTER TABLE manager.rootfs_snapshots DROP COLUMN IF EXISTS filesystem_id;
DROP TRIGGER IF EXISTS update_sandbox_rootfs_bindings_updated_at ON manager.sandbox_rootfs_bindings;
DROP INDEX IF EXISTS manager.idx_sandbox_rootfs_bindings_team_updated;
DROP INDEX IF EXISTS manager.idx_sandbox_rootfs_bindings_filesystem;
DROP TABLE IF EXISTS manager.sandbox_rootfs_bindings;
DROP TRIGGER IF EXISTS update_rootfs_filesystems_updated_at ON manager.rootfs_filesystems;
DROP INDEX IF EXISTS manager.idx_rootfs_filesystems_source;
DROP INDEX IF EXISTS manager.idx_rootfs_filesystems_head;
DROP INDEX IF EXISTS manager.idx_rootfs_filesystems_team_updated;
DROP TABLE IF EXISTS manager.rootfs_filesystems;
DROP FUNCTION IF EXISTS manager.update_updated_at_column();

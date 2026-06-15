-- +goose Up

CREATE TABLE IF NOT EXISTS rootfs_layers (
    layer_id TEXT PRIMARY KEY,
    parent_layer_id TEXT REFERENCES rootfs_layers(layer_id) ON DELETE RESTRICT,
    source_sandbox_id TEXT NOT NULL DEFAULT '',
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
    diff_id TEXT NOT NULL DEFAULT '',
    diff_media_type TEXT NOT NULL DEFAULT '',
    diff_size BIGINT NOT NULL DEFAULT 0,
    diff_object_key TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_rootfs_layers_team_created
    ON rootfs_layers(team_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_rootfs_layers_parent
    ON rootfs_layers(parent_layer_id)
    WHERE parent_layer_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS sandbox_rootfs_heads (
    sandbox_id TEXT PRIMARY KEY REFERENCES sandboxes(sandbox_id) ON DELETE CASCADE,
    team_id TEXT NOT NULL,
    head_layer_id TEXT NOT NULL REFERENCES rootfs_layers(layer_id) ON DELETE RESTRICT,
    runtime_generation BIGINT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_sandbox_rootfs_heads_team_updated
    ON sandbox_rootfs_heads(team_id, updated_at DESC);

CREATE TABLE IF NOT EXISTS rootfs_snapshots (
    snapshot_id TEXT PRIMARY KEY,
    team_id TEXT NOT NULL,
    source_sandbox_id TEXT NOT NULL DEFAULT '',
    head_layer_id TEXT NOT NULL REFERENCES rootfs_layers(layer_id) ON DELETE RESTRICT,
    name TEXT NOT NULL DEFAULT '',
    description TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_rootfs_snapshots_team_created
    ON rootfs_snapshots(team_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_rootfs_snapshots_head
    ON rootfs_snapshots(head_layer_id);

-- +goose Down

DROP INDEX IF EXISTS idx_rootfs_snapshots_head;
DROP INDEX IF EXISTS idx_rootfs_snapshots_team_created;
DROP TABLE IF EXISTS rootfs_snapshots;
DROP INDEX IF EXISTS idx_sandbox_rootfs_heads_team_updated;
DROP TABLE IF EXISTS sandbox_rootfs_heads;
DROP INDEX IF EXISTS idx_rootfs_layers_parent;
DROP INDEX IF EXISTS idx_rootfs_layers_team_created;
DROP TABLE IF EXISTS rootfs_layers;

-- +goose Up

CREATE TABLE IF NOT EXISTS sandbox_rootfs_states (
    sandbox_id TEXT NOT NULL REFERENCES sandboxes(sandbox_id) ON DELETE CASCADE,
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
    ON sandbox_rootfs_states(team_id, updated_at DESC);

-- +goose Down

DROP INDEX IF EXISTS idx_sandbox_rootfs_states_team_updated;
DROP TABLE IF EXISTS sandbox_rootfs_states;

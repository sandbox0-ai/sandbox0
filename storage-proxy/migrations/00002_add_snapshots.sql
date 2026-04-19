-- +goose Up
-- Snapshot/Restore Support for SandboxVolume

-- Sandbox Volume Snapshots table
CREATE TABLE IF NOT EXISTS sandbox_volume_snapshots (
    id TEXT PRIMARY KEY,
    volume_id TEXT NOT NULL REFERENCES sandbox_volumes(id) ON DELETE CASCADE,
    team_id TEXT NOT NULL,
    user_id TEXT NOT NULL,

    -- Filesystem metadata
    root_inode BIGINT NOT NULL,           -- Snapshot root directory inode
    source_inode BIGINT NOT NULL,         -- Source volume root inode at snapshot time

    -- Metadata
    name TEXT NOT NULL,
    description TEXT,
    size_bytes BIGINT DEFAULT 0,          -- Logical size (not including shared data)

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at TIMESTAMPTZ                -- Optional: auto-expiration time
);

CREATE INDEX IF NOT EXISTS idx_snapshots_volume_id ON sandbox_volume_snapshots(volume_id);
CREATE INDEX IF NOT EXISTS idx_snapshots_team_id ON sandbox_volume_snapshots(team_id);
CREATE INDEX IF NOT EXISTS idx_snapshots_user_id ON sandbox_volume_snapshots(user_id);

-- Volume Mounts table (for cross-cluster coordination)
CREATE TABLE IF NOT EXISTS sandbox_volume_mounts (
    id TEXT PRIMARY KEY,
    volume_id TEXT NOT NULL,
    cluster_id TEXT NOT NULL,             -- Cluster identifier
    pod_id TEXT NOT NULL,                 -- Storage-proxy pod ID

    -- Heartbeat info
    last_heartbeat TIMESTAMPTZ NOT NULL,
    mounted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- Mount options
    mount_options JSONB,                  -- Mount options (cache config, etc.)

    UNIQUE (volume_id, cluster_id, pod_id)
);

CREATE INDEX IF NOT EXISTS idx_mounts_volume_id ON sandbox_volume_mounts(volume_id);
CREATE INDEX IF NOT EXISTS idx_mounts_last_heartbeat ON sandbox_volume_mounts(last_heartbeat);

-- Snapshot Coordinations table (for tracking snapshot creation state)
CREATE TABLE IF NOT EXISTS snapshot_coordinations (
    id TEXT PRIMARY KEY,
    volume_id TEXT NOT NULL,
    snapshot_id TEXT,                     -- Filled after successful creation

    -- Coordination state
    status TEXT NOT NULL,                 -- pending, flushing, completed, failed, timeout
    expected_nodes INT NOT NULL,          -- Expected nodes to flush
    completed_nodes INT DEFAULT 0,        -- Completed flush nodes

    -- Timestamps
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at TIMESTAMPTZ NOT NULL       -- Timeout (prevent deadlock)
);

CREATE INDEX IF NOT EXISTS idx_coords_volume_id ON snapshot_coordinations(volume_id);
CREATE INDEX IF NOT EXISTS idx_coords_status ON snapshot_coordinations(status);
CREATE INDEX IF NOT EXISTS idx_coords_expires_at ON snapshot_coordinations(expires_at);

-- Flush Responses table (for tracking node flush results)
CREATE TABLE IF NOT EXISTS snapshot_flush_responses (
    id TEXT PRIMARY KEY,
    coord_id TEXT NOT NULL REFERENCES snapshot_coordinations(id) ON DELETE CASCADE,
    cluster_id TEXT NOT NULL,
    pod_id TEXT NOT NULL,

    -- Flush result
    success BOOLEAN NOT NULL,
    flushed_at TIMESTAMPTZ,
    error_message TEXT,

    UNIQUE (coord_id, cluster_id, pod_id)
);

CREATE INDEX IF NOT EXISTS idx_flush_coord_id ON snapshot_flush_responses(coord_id);

-- Apply updated_at trigger for coordinations
DROP TRIGGER IF EXISTS update_snapshot_coordinations_updated_at ON snapshot_coordinations;
CREATE TRIGGER update_snapshot_coordinations_updated_at
    BEFORE UPDATE ON snapshot_coordinations
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- +goose Down
DROP TRIGGER IF EXISTS update_snapshot_coordinations_updated_at ON snapshot_coordinations;
DROP TABLE IF EXISTS snapshot_flush_responses;
DROP TABLE IF EXISTS snapshot_coordinations;
DROP TABLE IF EXISTS sandbox_volume_mounts;
DROP TABLE IF EXISTS sandbox_volume_snapshots;

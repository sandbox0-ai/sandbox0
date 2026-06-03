-- +goose Up

CREATE TABLE IF NOT EXISTS sandbox_filesystems (
    id TEXT PRIMARY KEY,
    team_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    source_filesystem_id TEXT REFERENCES sandbox_filesystems(id) ON DELETE SET NULL,
    template_id TEXT,
    base_image_digest TEXT NOT NULL,
    s0fs_head TEXT NOT NULL DEFAULT '',
    state TEXT NOT NULL DEFAULT 'available',
    deleted_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_sandbox_filesystems_team_id
    ON sandbox_filesystems(team_id)
    WHERE deleted_at IS NULL;

CREATE INDEX IF NOT EXISTS idx_sandbox_filesystems_source_id
    ON sandbox_filesystems(source_filesystem_id)
    WHERE source_filesystem_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_sandbox_filesystems_base_image
    ON sandbox_filesystems(base_image_digest)
    WHERE deleted_at IS NULL;

DROP TRIGGER IF EXISTS update_sandbox_filesystems_updated_at ON sandbox_filesystems;
CREATE TRIGGER update_sandbox_filesystems_updated_at
    BEFORE UPDATE ON sandbox_filesystems
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

CREATE TABLE IF NOT EXISTS sandbox_filesystem_s0fs_heads (
    filesystem_id TEXT PRIMARY KEY REFERENCES sandbox_filesystems(id) ON DELETE CASCADE,
    manifest_seq BIGINT NOT NULL,
    checkpoint_seq BIGINT NOT NULL,
    manifest_key TEXT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_sandbox_filesystem_s0fs_heads_updated_at
    ON sandbox_filesystem_s0fs_heads(updated_at DESC);

DROP TRIGGER IF EXISTS update_sandbox_filesystem_s0fs_heads_updated_at ON sandbox_filesystem_s0fs_heads;
CREATE TRIGGER update_sandbox_filesystem_s0fs_heads_updated_at
    BEFORE UPDATE ON sandbox_filesystem_s0fs_heads
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

CREATE TABLE IF NOT EXISTS sandbox_filesystem_mounts (
    id TEXT PRIMARY KEY,
    filesystem_id TEXT NOT NULL REFERENCES sandbox_filesystems(id) ON DELETE CASCADE,
    cluster_id TEXT NOT NULL,
    pod_id TEXT NOT NULL,
    last_heartbeat TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    mounted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    mount_options JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(filesystem_id, cluster_id, pod_id)
);

CREATE INDEX IF NOT EXISTS idx_sandbox_filesystem_mounts_filesystem_id
    ON sandbox_filesystem_mounts(filesystem_id);

CREATE INDEX IF NOT EXISTS idx_sandbox_filesystem_mounts_heartbeat
    ON sandbox_filesystem_mounts(last_heartbeat);

CREATE TABLE IF NOT EXISTS sandbox_filesystem_snapshots (
    id TEXT PRIMARY KEY,
    filesystem_id TEXT NOT NULL REFERENCES sandbox_filesystems(id) ON DELETE CASCADE,
    team_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    base_image_digest TEXT NOT NULL,
    s0fs_head TEXT NOT NULL DEFAULT '',
    name TEXT NOT NULL,
    description TEXT,
    size_bytes BIGINT NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_sandbox_filesystem_snapshots_filesystem_id
    ON sandbox_filesystem_snapshots(filesystem_id);

CREATE INDEX IF NOT EXISTS idx_sandbox_filesystem_snapshots_team_id
    ON sandbox_filesystem_snapshots(team_id);

-- +goose Down

DROP INDEX IF EXISTS idx_sandbox_filesystem_snapshots_team_id;
DROP INDEX IF EXISTS idx_sandbox_filesystem_snapshots_filesystem_id;
DROP TABLE IF EXISTS sandbox_filesystem_snapshots;

DROP INDEX IF EXISTS idx_sandbox_filesystem_mounts_heartbeat;
DROP INDEX IF EXISTS idx_sandbox_filesystem_mounts_filesystem_id;
DROP TABLE IF EXISTS sandbox_filesystem_mounts;

DROP TRIGGER IF EXISTS update_sandbox_filesystem_s0fs_heads_updated_at ON sandbox_filesystem_s0fs_heads;
DROP INDEX IF EXISTS idx_sandbox_filesystem_s0fs_heads_updated_at;
DROP TABLE IF EXISTS sandbox_filesystem_s0fs_heads;

DROP TRIGGER IF EXISTS update_sandbox_filesystems_updated_at ON sandbox_filesystems;
DROP INDEX IF EXISTS idx_sandbox_filesystems_base_image;
DROP INDEX IF EXISTS idx_sandbox_filesystems_source_id;
DROP INDEX IF EXISTS idx_sandbox_filesystems_team_id;
DROP TABLE IF EXISTS sandbox_filesystems;

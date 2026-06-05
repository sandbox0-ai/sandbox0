-- +goose Up

CREATE TABLE IF NOT EXISTS sandbox_filesystems (
    filesystem_id TEXT PRIMARY KEY,
    team_id TEXT NOT NULL,
    user_id TEXT NOT NULL DEFAULT '',
    base_image_ref TEXT NOT NULL DEFAULT '',
    base_image_digest TEXT NOT NULL DEFAULT '',
    upperdir_head JSONB NOT NULL DEFAULT '{}',
    status TEXT NOT NULL DEFAULT 'ready',
    owner_sandbox_id TEXT NOT NULL DEFAULT '',
    owner_runtime_generation BIGINT NOT NULL DEFAULT 0,
    owner_acquired_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_sandbox_filesystems_team_updated
    ON sandbox_filesystems(team_id, updated_at DESC);

CREATE INDEX IF NOT EXISTS idx_sandbox_filesystems_owner
    ON sandbox_filesystems(owner_sandbox_id, owner_runtime_generation)
    WHERE owner_sandbox_id <> '';

-- +goose Down

DROP INDEX IF EXISTS idx_sandbox_filesystems_owner;
DROP INDEX IF EXISTS idx_sandbox_filesystems_team_updated;
DROP TABLE IF EXISTS sandbox_filesystems;

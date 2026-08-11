-- +goose Up
CREATE TABLE IF NOT EXISTS sandbox_volume_s0fs_commit_intents (
    volume_id TEXT NOT NULL REFERENCES sandbox_volumes(id) ON DELETE CASCADE,
    commit_id TEXT NOT NULL,
    base_generation BIGINT NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (volume_id, commit_id)
);

CREATE INDEX IF NOT EXISTS idx_s0fs_commit_intents_expiry
    ON sandbox_volume_s0fs_commit_intents(expires_at);

CREATE TABLE IF NOT EXISTS sandbox_volume_s0fs_gc_leases (
    volume_id TEXT PRIMARY KEY REFERENCES sandbox_volumes(id) ON DELETE CASCADE,
    token TEXT NOT NULL,
    head_generation BIGINT NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_s0fs_gc_leases_expiry
    ON sandbox_volume_s0fs_gc_leases(expires_at);

CREATE TABLE IF NOT EXISTS sandbox_volume_s0fs_gc_tombstones (
    volume_id TEXT NOT NULL REFERENCES sandbox_volumes(id) ON DELETE CASCADE,
    object_key TEXT NOT NULL,
    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    delete_after TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (volume_id, object_key)
);

CREATE INDEX IF NOT EXISTS idx_s0fs_gc_tombstones_due
    ON sandbox_volume_s0fs_gc_tombstones(volume_id, delete_after);

-- +goose Down
DROP TABLE IF EXISTS sandbox_volume_s0fs_gc_tombstones;
DROP TABLE IF EXISTS sandbox_volume_s0fs_gc_leases;
DROP TABLE IF EXISTS sandbox_volume_s0fs_commit_intents;

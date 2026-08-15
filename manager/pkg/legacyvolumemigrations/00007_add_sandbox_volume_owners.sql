-- +goose Up
CREATE TABLE IF NOT EXISTS sandbox_volume_owners (
    volume_id TEXT PRIMARY KEY REFERENCES sandbox_volumes(id) ON DELETE CASCADE,
    owner_kind TEXT NOT NULL DEFAULT 'sandbox',
    owner_sandbox_id TEXT NOT NULL,
    owner_cluster_id TEXT NOT NULL,
    purpose TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    cleanup_requested_at TIMESTAMPTZ,
    cleanup_reason TEXT,
    last_cleanup_attempt_at TIMESTAMPTZ,
    last_cleanup_error TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_sandbox_volume_owners_live_owner_purpose
    ON sandbox_volume_owners(owner_cluster_id, owner_sandbox_id, purpose)
    WHERE cleanup_requested_at IS NULL;

CREATE INDEX IF NOT EXISTS idx_sandbox_volume_owners_cluster_sandbox
    ON sandbox_volume_owners(owner_cluster_id, owner_sandbox_id);

CREATE INDEX IF NOT EXISTS idx_sandbox_volume_owners_pending_cleanup
    ON sandbox_volume_owners(owner_cluster_id, cleanup_requested_at)
    WHERE cleanup_requested_at IS NOT NULL;

DROP TRIGGER IF EXISTS update_sandbox_volume_owners_updated_at ON sandbox_volume_owners;
CREATE TRIGGER update_sandbox_volume_owners_updated_at
    BEFORE UPDATE ON sandbox_volume_owners
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- +goose Down
DROP TRIGGER IF EXISTS update_sandbox_volume_owners_updated_at ON sandbox_volume_owners;
DROP INDEX IF EXISTS idx_sandbox_volume_owners_pending_cleanup;
DROP INDEX IF EXISTS idx_sandbox_volume_owners_cluster_sandbox;
DROP INDEX IF EXISTS idx_sandbox_volume_owners_live_owner_purpose;
DROP TABLE IF EXISTS sandbox_volume_owners;

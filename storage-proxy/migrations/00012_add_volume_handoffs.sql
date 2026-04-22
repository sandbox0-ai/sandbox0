-- +goose Up
CREATE TABLE IF NOT EXISTS sandbox_volume_handoffs (
    volume_id TEXT PRIMARY KEY REFERENCES sandbox_volumes(id) ON DELETE CASCADE,
    source_cluster_id TEXT NOT NULL,
    source_pod_id TEXT NOT NULL,
    target_cluster_id TEXT NOT NULL,
    target_pod_id TEXT NOT NULL,
    target_ctld_addr TEXT NOT NULL,
    status TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_sandbox_volume_handoffs_expires_at
    ON sandbox_volume_handoffs(expires_at);

DROP TRIGGER IF EXISTS update_sandbox_volume_handoffs_updated_at ON sandbox_volume_handoffs;
CREATE TRIGGER update_sandbox_volume_handoffs_updated_at
    BEFORE UPDATE ON sandbox_volume_handoffs
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- +goose Down
DROP TRIGGER IF EXISTS update_sandbox_volume_handoffs_updated_at ON sandbox_volume_handoffs;
DROP INDEX IF EXISTS idx_sandbox_volume_handoffs_expires_at;
DROP TABLE IF EXISTS sandbox_volume_handoffs;

-- +goose Up
CREATE TABLE IF NOT EXISTS sandbox_volume_s0fs_heads (
    volume_id TEXT PRIMARY KEY REFERENCES sandbox_volumes(id) ON DELETE CASCADE,
    manifest_seq BIGINT NOT NULL,
    checkpoint_seq BIGINT NOT NULL,
    manifest_key TEXT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_sandbox_volume_s0fs_heads_updated_at
    ON sandbox_volume_s0fs_heads(updated_at DESC);

DROP TRIGGER IF EXISTS update_sandbox_volume_s0fs_heads_updated_at ON sandbox_volume_s0fs_heads;
CREATE TRIGGER update_sandbox_volume_s0fs_heads_updated_at
    BEFORE UPDATE ON sandbox_volume_s0fs_heads
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- +goose Down
DROP TRIGGER IF EXISTS update_sandbox_volume_s0fs_heads_updated_at ON sandbox_volume_s0fs_heads;
DROP INDEX IF EXISTS idx_sandbox_volume_s0fs_heads_updated_at;
DROP TABLE IF EXISTS sandbox_volume_s0fs_heads;

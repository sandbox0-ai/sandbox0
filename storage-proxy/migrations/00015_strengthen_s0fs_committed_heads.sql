-- +goose Up
ALTER TABLE sandbox_volume_s0fs_heads
    ADD COLUMN IF NOT EXISTS manifest_digest TEXT NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS commit_id TEXT NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS generation BIGINT NOT NULL DEFAULT 1;

ALTER TABLE sandbox_volume_s0fs_heads
    ADD CONSTRAINT sandbox_volume_s0fs_heads_generation_positive CHECK (generation > 0);

-- +goose Down
ALTER TABLE sandbox_volume_s0fs_heads
    DROP CONSTRAINT IF EXISTS sandbox_volume_s0fs_heads_generation_positive;

ALTER TABLE sandbox_volume_s0fs_heads
    DROP COLUMN IF EXISTS generation,
    DROP COLUMN IF EXISTS commit_id,
    DROP COLUMN IF EXISTS manifest_digest;

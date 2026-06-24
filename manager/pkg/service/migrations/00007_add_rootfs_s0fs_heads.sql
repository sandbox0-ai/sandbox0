-- +goose Up

ALTER TABLE manager.rootfs_layers
    ADD COLUMN IF NOT EXISTS storage_engine TEXT NOT NULL DEFAULT 'oci-diff',
    ADD COLUMN IF NOT EXISTS s0fs_volume_id TEXT NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS s0fs_manifest_key TEXT NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS s0fs_manifest_seq BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS s0fs_checkpoint_seq BIGINT NOT NULL DEFAULT 0;

CREATE INDEX IF NOT EXISTS idx_rootfs_layers_s0fs_volume
    ON manager.rootfs_layers(s0fs_volume_id, s0fs_manifest_seq)
    WHERE storage_engine = 's0fs';

-- +goose Down

DROP INDEX IF EXISTS manager.idx_rootfs_layers_s0fs_volume;
ALTER TABLE manager.rootfs_layers
    DROP COLUMN IF EXISTS s0fs_checkpoint_seq,
    DROP COLUMN IF EXISTS s0fs_manifest_seq,
    DROP COLUMN IF EXISTS s0fs_manifest_key,
    DROP COLUMN IF EXISTS s0fs_volume_id,
    DROP COLUMN IF EXISTS storage_engine;

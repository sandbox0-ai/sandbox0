-- +goose Up

DROP TABLE IF EXISTS sandbox_volume_s0fs_gc_tombstones CASCADE;
DROP TABLE IF EXISTS sandbox_volume_s0fs_gc_leases CASCADE;
DROP TABLE IF EXISTS sandbox_volume_s0fs_commit_intents CASCADE;
DROP TABLE IF EXISTS sandbox_volume_handoffs CASCADE;
DROP TABLE IF EXISTS sandbox_volume_s0fs_heads CASCADE;
DROP TABLE IF EXISTS sandbox_volume_owners CASCADE;
DROP TABLE IF EXISTS snapshot_flush_responses CASCADE;
DROP TABLE IF EXISTS snapshot_coordinations CASCADE;
DROP TABLE IF EXISTS sandbox_volume_mounts CASCADE;
DROP TABLE IF EXISTS sandbox_volume_snapshots CASCADE;
DROP TABLE IF EXISTS sandbox_volumes CASCADE;
DROP FUNCTION IF EXISTS update_updated_at_column() CASCADE;

-- +goose Down

-- Volume data is intentionally not recreated after retirement.

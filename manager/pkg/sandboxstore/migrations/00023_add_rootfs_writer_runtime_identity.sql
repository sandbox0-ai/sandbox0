-- +goose Up

ALTER TABLE manager.rootfs_writer_grants
    ADD COLUMN IF NOT EXISTS runtime_pod_namespace TEXT NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS runtime_pod_name TEXT NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS runtime_pod_uid TEXT NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS runtime_node_name TEXT NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS runtime_gate_parent TEXT NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS runtime_generation TEXT NOT NULL DEFAULT '';

CREATE INDEX IF NOT EXISTS idx_rootfs_writer_grants_expired_recovery
    ON manager.rootfs_writer_grants(lease_expires_at)
    WHERE state IN ('consumed', 'retiring')
      AND runtime_pod_namespace <> ''
      AND runtime_pod_name <> ''
      AND runtime_pod_uid <> ''
      AND runtime_node_name <> ''
      AND runtime_gate_parent <> ''
      AND runtime_generation <> '';

-- +goose Down

DROP INDEX IF EXISTS manager.idx_rootfs_writer_grants_expired_recovery;
ALTER TABLE manager.rootfs_writer_grants
    DROP COLUMN IF EXISTS runtime_generation,
    DROP COLUMN IF EXISTS runtime_gate_parent,
    DROP COLUMN IF EXISTS runtime_node_name,
    DROP COLUMN IF EXISTS runtime_pod_uid,
    DROP COLUMN IF EXISTS runtime_pod_name,
    DROP COLUMN IF EXISTS runtime_pod_namespace;

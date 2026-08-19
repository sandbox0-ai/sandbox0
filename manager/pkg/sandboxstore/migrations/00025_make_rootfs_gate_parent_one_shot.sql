-- +goose Up

-- A synthetic parent authorizes exactly one runtime incarnation. Keeping the
-- uniqueness constraint after terminal grant cleanup prevents an old parent
-- from becoming a valid attachment capability again.
CREATE UNIQUE INDEX IF NOT EXISTS idx_rootfs_writer_grants_runtime_gate_parent
    ON manager.rootfs_writer_grants(runtime_gate_parent)
    WHERE runtime_gate_parent <> '';

-- +goose Down

DROP INDEX IF EXISTS manager.idx_rootfs_writer_grants_runtime_gate_parent;

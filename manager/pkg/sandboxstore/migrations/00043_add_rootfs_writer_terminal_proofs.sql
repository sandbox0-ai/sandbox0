-- +goose Up

CREATE TABLE manager.rootfs_writer_terminal_proofs (
    grant_id TEXT PRIMARY KEY,
    sandbox_id TEXT NOT NULL,
    writer_epoch BIGINT NOT NULL CHECK (writer_epoch > 0),
    binding_version INTEGER NOT NULL CHECK (binding_version > 0),
    binding_digest BYTEA NOT NULL CHECK (octet_length(binding_digest) = 32),
    node_uid TEXT NOT NULL CHECK (node_uid <> ''),
    state TEXT NOT NULL CHECK (state IN ('retired', 'canceled')),
    expires_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX manager_rootfs_writer_terminal_proofs_expiry
    ON manager.rootfs_writer_terminal_proofs (expires_at, grant_id);

-- +goose Down

DROP INDEX IF EXISTS manager.manager_rootfs_writer_terminal_proofs_expiry;
DROP TABLE IF EXISTS manager.rootfs_writer_terminal_proofs;

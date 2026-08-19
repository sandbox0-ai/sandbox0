-- +goose Up

CREATE TABLE IF NOT EXISTS manager.rootfs_running_forks (
    operation_id TEXT PRIMARY KEY,
    source_sandbox_id TEXT NOT NULL
        REFERENCES manager.sandboxes(sandbox_id) ON DELETE RESTRICT,
    source_filesystem_id TEXT NOT NULL
        REFERENCES manager.rootfs_filesystems(filesystem_id) ON DELETE RESTRICT,
    source_grant_id TEXT NOT NULL
        REFERENCES manager.rootfs_writer_grants(grant_id) ON DELETE RESTRICT,
    source_writer_epoch BIGINT NOT NULL CHECK (source_writer_epoch > 0),
    source_generation_id TEXT NOT NULL
        REFERENCES manager.rootfs_generations(generation_id) ON DELETE RESTRICT,
    target_sandbox_id TEXT NOT NULL UNIQUE
        REFERENCES manager.sandboxes(sandbox_id) ON DELETE RESTRICT,
    target_filesystem_id TEXT NOT NULL UNIQUE
        REFERENCES manager.rootfs_filesystems(filesystem_id) ON DELETE RESTRICT,
    checkpoint_generation_id TEXT NOT NULL UNIQUE
        REFERENCES manager.rootfs_generations(generation_id) ON DELETE RESTRICT,
    binding_version INTEGER NOT NULL CHECK (binding_version > 0),
    binding_digest BYTEA NOT NULL CHECK (octet_length(binding_digest) = 32),
    checkpoint_sequence BIGINT NOT NULL CHECK (checkpoint_sequence >= 0),
    checkpoint_descriptor_digest TEXT NOT NULL,
    checkpoint_proof_digest BYTEA NOT NULL CHECK (octet_length(checkpoint_proof_digest) = 32),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_rootfs_running_forks_source
    ON manager.rootfs_running_forks(source_filesystem_id, created_at DESC);

-- +goose Down

DROP INDEX IF EXISTS manager.idx_rootfs_running_forks_source;
DROP TABLE IF EXISTS manager.rootfs_running_forks;

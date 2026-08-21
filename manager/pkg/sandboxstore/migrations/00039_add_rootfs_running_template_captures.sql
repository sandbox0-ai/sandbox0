-- +goose Up

-- A pending row is the regional authorization for one exact live-writer
-- checkpoint. Published rows retain bounded audit identity only until the
-- template releases its snapshot.
CREATE TABLE IF NOT EXISTS manager.rootfs_running_template_captures (
    operation_id TEXT PRIMARY KEY,
    snapshot_id TEXT NOT NULL UNIQUE,
    team_id TEXT NOT NULL,
    source_sandbox_id TEXT NOT NULL,
    source_filesystem_id TEXT NOT NULL,
    source_grant_id TEXT NOT NULL,
    source_writer_epoch BIGINT NOT NULL CHECK (source_writer_epoch > 0),
    source_generation_id TEXT NOT NULL,
    target_filesystem_id TEXT NOT NULL UNIQUE,
    checkpoint_generation_id TEXT NOT NULL UNIQUE,
    request_digest BYTEA NOT NULL CHECK (octet_length(request_digest) = 32),
    binding_version INTEGER NOT NULL CHECK (binding_version > 0),
    binding_digest BYTEA NOT NULL CHECK (octet_length(binding_digest) = 32),
    state TEXT NOT NULL CHECK (state IN ('pending', 'published')),
    checkpoint_sequence BIGINT CHECK (checkpoint_sequence >= 0),
    checkpoint_descriptor_digest TEXT,
    checkpoint_proof_digest BYTEA CHECK (
        checkpoint_proof_digest IS NULL OR octet_length(checkpoint_proof_digest) = 32
    ),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    published_at TIMESTAMPTZ,
    cancel_reason TEXT NOT NULL DEFAULT '',
    CHECK (
        (state = 'pending'
            AND checkpoint_sequence IS NULL
            AND checkpoint_descriptor_digest IS NULL
            AND checkpoint_proof_digest IS NULL
            AND published_at IS NULL
            AND cancel_reason = '')
        OR
        (state = 'published'
            AND checkpoint_sequence IS NOT NULL
            AND checkpoint_descriptor_digest IS NOT NULL
            AND checkpoint_proof_digest IS NOT NULL
            AND published_at IS NOT NULL)
    )
);

CREATE INDEX IF NOT EXISTS idx_rootfs_running_template_captures_source_pending
    ON manager.rootfs_running_template_captures (source_sandbox_id, updated_at)
    WHERE state = 'pending';

DROP TRIGGER IF EXISTS update_rootfs_running_template_captures_updated_at
    ON manager.rootfs_running_template_captures;
CREATE TRIGGER update_rootfs_running_template_captures_updated_at
    BEFORE UPDATE ON manager.rootfs_running_template_captures
    FOR EACH ROW
    EXECUTE FUNCTION manager.update_updated_at_column();

-- +goose Down

DROP TRIGGER IF EXISTS update_rootfs_running_template_captures_updated_at
    ON manager.rootfs_running_template_captures;
DROP INDEX IF EXISTS manager.idx_rootfs_running_template_captures_source_pending;
DROP TABLE IF EXISTS manager.rootfs_running_template_captures;

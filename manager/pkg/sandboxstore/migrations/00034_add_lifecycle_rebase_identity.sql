-- +goose Up

ALTER TABLE manager.sandbox_lifecycle_txns
    ADD COLUMN source_base_artifact_digest TEXT NOT NULL DEFAULT '',
    ADD COLUMN target_base_artifact_digest TEXT NOT NULL DEFAULT '',
    ADD COLUMN rollback_expires_at TIMESTAMPTZ,
    ADD CONSTRAINT sandbox_lifecycle_txns_rebase_identity_check CHECK (
        kind <> 'rebase'
        OR (
            source = 'manual'
            AND cancelable = FALSE
            AND from_generation = to_generation
            AND from_pod_namespace = ''
            AND from_pod_name = ''
            AND to_pod_namespace = ''
            AND to_pod_name = ''
            AND target_sandbox_id = ''
            AND octet_length(target_record_digest) = 0
            AND target_generation_id <> ''
            AND source_base_artifact_digest <> ''
            AND target_base_artifact_digest <> ''
            AND source_base_artifact_digest <> target_base_artifact_digest
            AND expected_head_layer_id <> ''
            AND rollback_expires_at IS NOT NULL
        )
    );

-- +goose Down

ALTER TABLE manager.sandbox_lifecycle_txns
    DROP CONSTRAINT IF EXISTS sandbox_lifecycle_txns_rebase_identity_check,
    DROP COLUMN IF EXISTS rollback_expires_at,
    DROP COLUMN IF EXISTS target_base_artifact_digest,
    DROP COLUMN IF EXISTS source_base_artifact_digest;

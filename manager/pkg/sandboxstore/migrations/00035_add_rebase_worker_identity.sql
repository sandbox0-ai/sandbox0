-- +goose Up

ALTER TABLE manager.sandbox_lifecycle_txns
    DROP CONSTRAINT sandbox_lifecycle_txns_rebase_identity_check,
    ADD COLUMN worker_cluster_id TEXT NOT NULL DEFAULT '',
    ADD COLUMN worker_node_id TEXT NOT NULL DEFAULT '',
    ADD COLUMN worker_node_uid TEXT NOT NULL DEFAULT '',
    ADD COLUMN worker_proof_digest BYTEA NOT NULL DEFAULT '',
    ADD COLUMN worker_acknowledged_at TIMESTAMPTZ;

UPDATE manager.sandbox_lifecycle_txns
SET phase = 'aborted',
    error = 'superseded pre-worker-identity rebase operation',
    aborted_at = NOW(),
    updated_at = NOW()
WHERE kind = 'rebase'
  AND phase IN ('preparing', 'barriered', 'publishing', 'committing');

ALTER TABLE manager.sandbox_lifecycle_txns
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
            AND (
                (worker_cluster_id = '' AND worker_node_id = '' AND worker_node_uid = ''
                    AND phase IN ('committed', 'aborted'))
                OR (worker_cluster_id <> '' AND worker_node_id <> '' AND worker_node_uid <> '')
            )
            AND octet_length(worker_proof_digest) IN (0, 32)
            AND (worker_acknowledged_at IS NULL
                OR (phase IN ('committed', 'aborted') AND octet_length(worker_proof_digest) = 32))
        )
    );

-- +goose Down

ALTER TABLE manager.sandbox_lifecycle_txns
    DROP CONSTRAINT sandbox_lifecycle_txns_rebase_identity_check,
    DROP COLUMN worker_acknowledged_at,
    DROP COLUMN worker_proof_digest,
    DROP COLUMN worker_node_uid,
    DROP COLUMN worker_node_id,
    DROP COLUMN worker_cluster_id,
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

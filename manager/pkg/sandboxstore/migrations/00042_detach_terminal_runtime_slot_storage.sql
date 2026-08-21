-- +goose Up

ALTER TABLE manager.runtime_slots
    DROP CONSTRAINT runtime_slots_claim_binding;

-- A claimed terminal slot remains durable allocation and claim history after
-- sandbox deletion, but it no longer holds filesystem or writer authority.
ALTER TABLE manager.runtime_slots
    ADD CONSTRAINT runtime_slots_claim_binding CHECK (
        (
            state IN ('registered', 'fastpath_ready')
            AND claim_operation_id = '' AND claim_id = ''
            AND claim_cluster_filter = '' AND claim_ttl_milliseconds = 0
            AND claim_runtime_assignment_revision = ''
            AND claim_network_policy_digest = ''
            AND sandbox_id IS NULL AND filesystem_id IS NULL
            AND source_generation_id IS NULL AND writer_grant_id IS NULL
            AND claim_lease_expires_at IS NULL AND claimed_at IS NULL
        ) OR (
            state = 'terminal' AND claim_operation_id = '' AND claim_id = ''
            AND claim_cluster_filter = '' AND claim_ttl_milliseconds = 0
            AND claim_runtime_assignment_revision = ''
            AND claim_network_policy_digest = ''
            AND sandbox_id IS NULL AND filesystem_id IS NULL
            AND source_generation_id IS NULL AND writer_grant_id IS NULL
            AND claim_lease_expires_at IS NULL AND claimed_at IS NULL
        ) OR (
            claim_operation_id <> '' AND claim_id <> ''
            AND claim_ttl_milliseconds BETWEEN 1000 AND 60000
            AND (
                (claim_runtime_assignment_revision <> '' AND claim_network_policy_digest <> '')
                OR
                (claim_runtime_assignment_revision = '' AND claim_network_policy_digest = '')
            )
            AND sandbox_id IS NOT NULL AND filesystem_id IS NOT NULL
            AND source_generation_id IS NOT NULL
            AND claim_lease_expires_at IS NOT NULL AND claimed_at IS NOT NULL
        ) OR (
            state = 'terminal'
            AND claim_operation_id <> '' AND claim_id <> ''
            AND claim_ttl_milliseconds BETWEEN 1000 AND 60000
            AND (
                (claim_runtime_assignment_revision <> '' AND claim_network_policy_digest <> '')
                OR
                (claim_runtime_assignment_revision = '' AND claim_network_policy_digest = '')
            )
            AND sandbox_id IS NOT NULL AND filesystem_id IS NULL
            AND source_generation_id IS NULL AND writer_grant_id IS NULL
            AND claim_lease_expires_at IS NOT NULL AND claimed_at IS NOT NULL
        )
    );

-- +goose Down

ALTER TABLE manager.runtime_slots
    DROP CONSTRAINT runtime_slots_claim_binding;

ALTER TABLE manager.runtime_slots
    ADD CONSTRAINT runtime_slots_claim_binding CHECK (
        (
            state IN ('registered', 'fastpath_ready')
            AND claim_operation_id = '' AND claim_id = ''
            AND claim_cluster_filter = '' AND claim_ttl_milliseconds = 0
            AND claim_runtime_assignment_revision = ''
            AND claim_network_policy_digest = ''
            AND sandbox_id IS NULL AND filesystem_id IS NULL
            AND source_generation_id IS NULL AND writer_grant_id IS NULL
            AND claim_lease_expires_at IS NULL AND claimed_at IS NULL
        ) OR (
            state = 'terminal' AND claim_operation_id = '' AND claim_id = ''
            AND claim_cluster_filter = '' AND claim_ttl_milliseconds = 0
            AND claim_runtime_assignment_revision = ''
            AND claim_network_policy_digest = ''
            AND sandbox_id IS NULL AND filesystem_id IS NULL
            AND source_generation_id IS NULL AND writer_grant_id IS NULL
            AND claim_lease_expires_at IS NULL AND claimed_at IS NULL
        ) OR (
            claim_operation_id <> '' AND claim_id <> ''
            AND claim_ttl_milliseconds BETWEEN 1000 AND 60000
            AND (
                (claim_runtime_assignment_revision <> '' AND claim_network_policy_digest <> '')
                OR
                (claim_runtime_assignment_revision = '' AND claim_network_policy_digest = '')
            )
            AND sandbox_id IS NOT NULL AND filesystem_id IS NOT NULL
            AND source_generation_id IS NOT NULL
            AND claim_lease_expires_at IS NOT NULL AND claimed_at IS NOT NULL
        )
    );

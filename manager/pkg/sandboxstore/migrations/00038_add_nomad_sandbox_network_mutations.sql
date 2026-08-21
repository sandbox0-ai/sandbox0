-- +goose Up

CREATE TABLE manager.sandbox_network_mutations (
    sandbox_id TEXT PRIMARY KEY REFERENCES manager.sandboxes(sandbox_id) ON DELETE CASCADE,
    operation_id TEXT NOT NULL UNIQUE,
    slot_id TEXT NOT NULL REFERENCES manager.runtime_slots(slot_id),
    slot_revision BIGINT NOT NULL,
    team_id TEXT NOT NULL,
    cluster_id TEXT NOT NULL,
    allocation_id TEXT NOT NULL,
    allocation_namespace TEXT NOT NULL,
    node_id TEXT NOT NULL,
    node_uid TEXT NOT NULL,
    node_boot_id TEXT NOT NULL,
    netns_identity TEXT NOT NULL,
    claim_id TEXT NOT NULL,
    current_policy_digest TEXT NOT NULL,
    desired_policy TEXT NOT NULL,
    desired_policy_digest TEXT NOT NULL,
    request_policy JSONB NOT NULL,
    phase TEXT NOT NULL DEFAULT 'pending',
    applied_policy_token JSONB,
    applied_token_digest BYTEA NOT NULL DEFAULT ''::bytea,
    cancellation_reason TEXT NOT NULL DEFAULT '',
    applied_at TIMESTAMPTZ,
    canceled_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT sandbox_network_mutations_phase CHECK (
        phase IN ('pending', 'applied', 'canceled')
    ),
    CONSTRAINT sandbox_network_mutations_policy_digests CHECK (
        current_policy_digest ~ '^sha256:[0-9a-f]{64}$'
        AND desired_policy_digest ~ '^sha256:[0-9a-f]{64}$'
    ),
    CONSTRAINT sandbox_network_mutations_identity_bounds CHECK (
        slot_revision >= 0
        AND operation_id <> '' AND octet_length(operation_id) <= 512
        AND team_id <> '' AND octet_length(team_id) <= 512
        AND cluster_id <> '' AND octet_length(cluster_id) <= 512
        AND allocation_id <> '' AND octet_length(allocation_id) <= 512
        AND allocation_namespace <> '' AND octet_length(allocation_namespace) <= 512
        AND node_id <> '' AND octet_length(node_id) <= 512
        AND node_uid <> '' AND octet_length(node_uid) <= 512
        AND node_boot_id <> '' AND octet_length(node_boot_id) <= 512
        AND netns_identity <> '' AND octet_length(netns_identity) <= 512
        AND claim_id <> '' AND octet_length(claim_id) <= 512
    ),
    CONSTRAINT sandbox_network_mutations_payload_bounds CHECK (
        octet_length(desired_policy) BETWEEN 1 AND 65536
        AND octet_length(request_policy::text) BETWEEN 2 AND 65536
        AND jsonb_typeof(request_policy) = 'object'
        AND octet_length(cancellation_reason) <= 1024
    ),
    CONSTRAINT sandbox_network_mutations_terminal_state CHECK (
        (
            phase = 'pending'
            AND applied_policy_token IS NULL
            AND octet_length(applied_token_digest) = 0
            AND cancellation_reason = ''
            AND applied_at IS NULL
            AND canceled_at IS NULL
        ) OR (
            phase = 'applied'
            AND applied_policy_token IS NOT NULL
            AND jsonb_typeof(applied_policy_token) = 'object'
            AND octet_length(applied_token_digest) = 32
            AND cancellation_reason = ''
            AND applied_at IS NOT NULL
            AND canceled_at IS NULL
        ) OR (
            phase = 'canceled'
            AND applied_policy_token IS NULL
            AND octet_length(applied_token_digest) = 0
            AND cancellation_reason <> ''
            AND applied_at IS NULL
            AND canceled_at IS NOT NULL
        )
    )
);

CREATE INDEX idx_sandbox_network_mutations_pending
    ON manager.sandbox_network_mutations(updated_at, sandbox_id)
    WHERE phase = 'pending';

-- +goose Down

DROP TABLE IF EXISTS manager.sandbox_network_mutations;

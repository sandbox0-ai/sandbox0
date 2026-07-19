-- +goose Up

CREATE SEQUENCE policy_revision_seq;

CREATE TABLE region_default_policies (
    quota_key TEXT PRIMARY KEY,
    kind TEXT NOT NULL,
    revision BIGINT NOT NULL DEFAULT nextval('policy_revision_seq') CHECK (revision > 0),
    limit_value BIGINT,
    rate_tokens BIGINT,
    rate_interval_ms BIGINT,
    rate_burst BIGINT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CHECK (
        (kind = 'capacity'
            AND limit_value IS NOT NULL AND limit_value >= 0
            AND rate_tokens IS NULL AND rate_interval_ms IS NULL AND rate_burst IS NULL)
        OR
        (kind = 'concurrency'
            AND limit_value IS NOT NULL AND limit_value >= 0
            AND rate_tokens IS NULL AND rate_interval_ms IS NULL AND rate_burst IS NULL)
        OR
        (kind = 'rate'
            AND limit_value IS NULL
            AND rate_tokens IS NOT NULL
            AND rate_interval_ms IS NOT NULL
            AND rate_burst IS NOT NULL
            AND rate_tokens > 0 AND rate_interval_ms > 0
            AND rate_burst >= rate_tokens)
    )
);

CREATE TABLE team_policies (
    team_id TEXT NOT NULL,
    quota_key TEXT NOT NULL,
    kind TEXT NOT NULL,
    revision BIGINT NOT NULL DEFAULT nextval('policy_revision_seq') CHECK (revision > 0),
    limit_value BIGINT,
    rate_tokens BIGINT,
    rate_interval_ms BIGINT,
    rate_burst BIGINT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (team_id, quota_key),
    CHECK (
        (kind = 'capacity'
            AND limit_value IS NOT NULL AND limit_value >= 0
            AND rate_tokens IS NULL AND rate_interval_ms IS NULL AND rate_burst IS NULL)
        OR
        (kind = 'concurrency'
            AND limit_value IS NOT NULL AND limit_value >= 0
            AND rate_tokens IS NULL AND rate_interval_ms IS NULL AND rate_burst IS NULL)
        OR
        (kind = 'rate'
            AND limit_value IS NULL
            AND rate_tokens IS NOT NULL
            AND rate_interval_ms IS NOT NULL
            AND rate_burst IS NOT NULL
            AND rate_tokens > 0 AND rate_interval_ms > 0
            AND rate_burst >= rate_tokens)
    )
);

CREATE INDEX idx_team_policies_quota_key ON team_policies(quota_key);

-- The old dimension-based quota model has no compatible semantics for this
-- policy and allocation ledger. It is intentionally discarded.
DROP INDEX IF EXISTS idx_team_quota_limits_dimension;
DROP TABLE IF EXISTS team_quota_limits;
DROP FUNCTION IF EXISTS update_updated_at_column();

CREATE TABLE team_states (
    team_id TEXT PRIMARY KEY,
    revision BIGINT NOT NULL DEFAULT 0 CHECK (revision >= 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE team_usage (
    team_id TEXT NOT NULL REFERENCES team_states(team_id) ON DELETE CASCADE,
    quota_key TEXT NOT NULL,
    committed_value BIGINT NOT NULL DEFAULT 0 CHECK (committed_value >= 0),
    reserved_value BIGINT NOT NULL DEFAULT 0 CHECK (reserved_value >= 0),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (team_id, quota_key)
);

CREATE TABLE allocations (
    allocation_id TEXT PRIMARY KEY,
    team_id TEXT NOT NULL REFERENCES team_states(team_id) ON DELETE CASCADE,
    owner_kind TEXT NOT NULL,
    owner_id TEXT NOT NULL,
    cluster_id TEXT NOT NULL DEFAULT '',
    state TEXT NOT NULL CHECK (state IN ('reserved', 'active', 'paused', 'releasing', 'released')),
    operation_id TEXT,
    operation_kind TEXT NOT NULL DEFAULT '',
    operation_generation BIGINT NOT NULL DEFAULT 0 CHECK (operation_generation >= 0),
    operation_base_state TEXT NOT NULL DEFAULT '',
    last_operation_id TEXT NOT NULL DEFAULT '',
    last_operation_result TEXT NOT NULL DEFAULT '',
    pod_namespace TEXT NOT NULL DEFAULT '',
    pod_name TEXT NOT NULL DEFAULT '',
    pod_uid TEXT NOT NULL DEFAULT '',
    runtime_generation BIGINT NOT NULL DEFAULT 0 CHECK (runtime_generation >= 0),
    reconcile_after TIMESTAMPTZ,
    last_error TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (team_id, owner_kind, owner_id),
    CHECK (last_operation_result IN ('', 'committed', 'aborted'))
);

CREATE INDEX idx_allocations_reconcile
    ON allocations(reconcile_after)
    WHERE reconcile_after IS NOT NULL;

CREATE INDEX idx_allocations_cluster_state
    ON allocations(cluster_id, state);

CREATE TABLE allocation_items (
    allocation_id TEXT NOT NULL REFERENCES allocations(allocation_id) ON DELETE CASCADE,
    quota_key TEXT NOT NULL,
    committed_value BIGINT NOT NULL DEFAULT 0 CHECK (committed_value >= 0),
    pending_value BIGINT CHECK (pending_value >= 0),
    PRIMARY KEY (allocation_id, quota_key)
);

CREATE TABLE transfer_operations (
    team_id TEXT NOT NULL REFERENCES team_states(team_id) ON DELETE CASCADE,
    operation_id TEXT NOT NULL,
    operation_kind TEXT NOT NULL,
    operation_generation BIGINT NOT NULL DEFAULT 0 CHECK (operation_generation >= 0),
    request_fingerprint TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('prepared', 'committed', 'aborted')),
    source_allocation_id TEXT NOT NULL REFERENCES allocations(allocation_id),
    destination_allocation_id TEXT NOT NULL REFERENCES allocations(allocation_id),
    pod_namespace TEXT NOT NULL,
    pod_name TEXT NOT NULL,
    pod_uid TEXT NOT NULL,
    runtime_generation BIGINT NOT NULL DEFAULT 0 CHECK (runtime_generation >= 0),
    last_error TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ,
    PRIMARY KEY (team_id, operation_id)
);

CREATE INDEX idx_transfer_operations_source_prepared
    ON transfer_operations(source_allocation_id)
    WHERE state = 'prepared';

CREATE INDEX idx_transfer_operations_destination_prepared
    ON transfer_operations(destination_allocation_id)
    WHERE state = 'prepared';

CREATE TABLE transfer_items (
    team_id TEXT NOT NULL,
    operation_id TEXT NOT NULL,
    quota_key TEXT NOT NULL,
    source_decrease BIGINT NOT NULL DEFAULT 0 CHECK (source_decrease >= 0),
    destination_committed BIGINT NOT NULL DEFAULT 0 CHECK (destination_committed >= 0),
    destination_target BIGINT NOT NULL DEFAULT 0 CHECK (destination_target >= 0),
    reserved_value BIGINT NOT NULL DEFAULT 0 CHECK (reserved_value >= 0),
    PRIMARY KEY (team_id, operation_id, quota_key),
    FOREIGN KEY (team_id, operation_id)
        REFERENCES transfer_operations(team_id, operation_id)
        ON DELETE CASCADE
);

-- +goose Down

DROP TABLE IF EXISTS transfer_items;
DROP INDEX IF EXISTS idx_transfer_operations_destination_prepared;
DROP INDEX IF EXISTS idx_transfer_operations_source_prepared;
DROP TABLE IF EXISTS transfer_operations;
DROP TABLE IF EXISTS allocation_items;
DROP INDEX IF EXISTS idx_allocations_cluster_state;
DROP INDEX IF EXISTS idx_allocations_reconcile;
DROP TABLE IF EXISTS allocations;
DROP TABLE IF EXISTS team_usage;
DROP TABLE IF EXISTS team_states;
DROP INDEX IF EXISTS idx_team_policies_quota_key;
DROP TABLE IF EXISTS team_policies;
DROP TABLE IF EXISTS region_default_policies;
DROP SEQUENCE IF EXISTS policy_revision_seq;

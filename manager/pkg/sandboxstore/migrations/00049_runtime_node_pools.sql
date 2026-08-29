-- +goose Up

-- Runtime workers are disposable. PostgreSQL owns only admission, capacity
-- pressure, network leases, and lifecycle fencing; no sandbox data is stored
-- on an ECS instance record.
CREATE TABLE manager.runtime_node_pool_states (
    pool_id TEXT PRIMARY KEY CHECK (octet_length(pool_id) BETWEEN 1 AND 128),
    cluster_id TEXT NOT NULL CHECK (octet_length(cluster_id) BETWEEN 1 AND 512),
    desired_nodes INTEGER NOT NULL DEFAULT 0 CHECK (desired_nodes BETWEEN 0 AND 299),
    low_pressure_since TIMESTAMPTZ,
    last_scale_out_at TIMESTAMPTZ,
    last_scale_in_at TIMESTAMPTZ,
    revision BIGINT NOT NULL DEFAULT 1 CHECK (revision > 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE manager.runtime_node_pool_controller_leases (
    pool_id TEXT PRIMARY KEY
        REFERENCES manager.runtime_node_pool_states(pool_id) ON DELETE CASCADE,
    owner_id TEXT NOT NULL CHECK (octet_length(owner_id) BETWEEN 1 AND 256),
    lease_expires_at TIMESTAMPTZ NOT NULL,
    revision BIGINT NOT NULL DEFAULT 1 CHECK (revision > 0),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE manager.runtime_node_pool_demands (
    pool_id TEXT NOT NULL
        REFERENCES manager.runtime_node_pool_states(pool_id) ON DELETE CASCADE,
    operation_id TEXT NOT NULL CHECK (octet_length(operation_id) BETWEEN 1 AND 512),
    cluster_id TEXT NOT NULL CHECK (octet_length(cluster_id) BETWEEN 1 AND 512),
    cpu_millicores BIGINT NOT NULL CHECK (cpu_millicores > 0),
    memory_bytes BIGINT NOT NULL CHECK (memory_bytes > 0),
    slots INTEGER NOT NULL DEFAULT 1 CHECK (slots BETWEEN 1 AND 1024),
    expires_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (pool_id, operation_id)
);

CREATE INDEX idx_runtime_node_pool_demands_live
    ON manager.runtime_node_pool_demands(pool_id, expires_at, operation_id);

CREATE TABLE manager.runtime_node_instances (
    pool_id TEXT NOT NULL
        REFERENCES manager.runtime_node_pool_states(pool_id) ON DELETE RESTRICT,
    provider TEXT NOT NULL CHECK (provider IN ('aliyun')),
    provider_instance_id TEXT NOT NULL CHECK (octet_length(provider_instance_id) BETWEEN 1 AND 256),
    pool_kind TEXT NOT NULL CHECK (pool_kind IN ('fixed', 'elastic')),
    cluster_id TEXT NOT NULL CHECK (octet_length(cluster_id) BETWEEN 1 AND 512),
    node_name TEXT NOT NULL CHECK (octet_length(node_name) BETWEEN 1 AND 128),
    node_uid TEXT NOT NULL CHECK (octet_length(node_uid) BETWEEN 1 AND 512),
    private_ip INET NOT NULL,
    allocation_cidr CIDR NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('enrolling', 'active', 'draining', 'revoked')),
    nomad_node_id TEXT,
    authority_common_name TEXT,
    agent_uid TEXT,
    admitted_at TIMESTAMPTZ,
    provider_ready_at TIMESTAMPTZ,
    drain_started_at TIMESTAMPTZ,
    revoked_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (pool_id, provider_instance_id),
    CHECK (family(private_ip) = 4 AND family(allocation_cidr) = 4),
    CHECK (provider_ready_at IS NULL OR state IN ('active', 'draining', 'revoked')),
    CHECK (
        (state = 'enrolling'
            AND nomad_node_id IS NULL AND authority_common_name IS NULL
            AND agent_uid IS NULL AND admitted_at IS NULL
            AND drain_started_at IS NULL AND revoked_at IS NULL)
        OR (state = 'active'
            AND nomad_node_id IS NOT NULL AND authority_common_name IS NOT NULL
            AND agent_uid IS NOT NULL AND admitted_at IS NOT NULL
            AND drain_started_at IS NULL AND revoked_at IS NULL)
        OR (state = 'draining'
            AND nomad_node_id IS NOT NULL AND authority_common_name IS NOT NULL
            AND agent_uid IS NOT NULL AND admitted_at IS NOT NULL
            AND drain_started_at IS NOT NULL AND revoked_at IS NULL)
        OR (state = 'revoked'
            AND nomad_node_id IS NOT NULL AND authority_common_name IS NOT NULL
            AND agent_uid IS NOT NULL AND admitted_at IS NOT NULL
            AND drain_started_at IS NOT NULL AND revoked_at IS NOT NULL)
    )
);

CREATE UNIQUE INDEX idx_runtime_node_instances_node_uid
    ON manager.runtime_node_instances(node_uid);
CREATE UNIQUE INDEX idx_runtime_node_instances_live_cidr
    ON manager.runtime_node_instances(allocation_cidr)
    WHERE state <> 'revoked';
CREATE UNIQUE INDEX idx_runtime_node_instances_live_nomad_node
    ON manager.runtime_node_instances(cluster_id, nomad_node_id)
    WHERE state <> 'revoked' AND nomad_node_id IS NOT NULL;
CREATE UNIQUE INDEX idx_runtime_node_instances_live_certificate
    ON manager.runtime_node_instances(authority_common_name)
    WHERE state <> 'revoked' AND authority_common_name IS NOT NULL;
CREATE INDEX idx_runtime_node_instances_pool_state
    ON manager.runtime_node_instances(pool_id, state, updated_at, provider_instance_id);

-- A fence is separate from the cloud-instance row so the hot claim query can
-- reject a node without depending on provider metadata. Revoked fences remain
-- durable and prevent a late node heartbeat from reopening scheduling.
CREATE TABLE manager.runtime_node_fences (
    cluster_id TEXT NOT NULL,
    node_id TEXT NOT NULL,
    node_uid TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('warming', 'draining', 'revoked')),
    reason TEXT NOT NULL CHECK (octet_length(reason) BETWEEN 1 AND 1024),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (cluster_id, node_id, node_uid)
);

CREATE INDEX idx_runtime_node_fences_uid
    ON manager.runtime_node_fences(node_uid, state);

CREATE TABLE manager.runtime_node_enrollment_challenges (
    challenge_digest BYTEA PRIMARY KEY CHECK (octet_length(challenge_digest) = 32),
    pool_id TEXT NOT NULL
        REFERENCES manager.runtime_node_pool_states(pool_id) ON DELETE CASCADE,
    provider_instance_id TEXT NOT NULL CHECK (octet_length(provider_instance_id) BETWEEN 1 AND 256),
    remote_ip INET NOT NULL CHECK (family(remote_ip) = 4),
    expires_at TIMESTAMPTZ NOT NULL,
    consumed_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_runtime_node_enrollment_challenges_expiry
    ON manager.runtime_node_enrollment_challenges(expires_at, challenge_digest);

CREATE TABLE manager.runtime_node_lifecycle_actions (
    lifecycle_action_token TEXT PRIMARY KEY
        CHECK (octet_length(lifecycle_action_token) BETWEEN 1 AND 512),
    pool_id TEXT NOT NULL
        REFERENCES manager.runtime_node_pool_states(pool_id) ON DELETE CASCADE,
    lifecycle_hook_id TEXT NOT NULL CHECK (octet_length(lifecycle_hook_id) BETWEEN 1 AND 256),
    transition TEXT NOT NULL CHECK (transition IN ('scale_out', 'scale_in')),
    state TEXT NOT NULL CHECK (state IN ('pending', 'draining', 'completed', 'abandoned')),
    first_observed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CHECK ((state IN ('completed', 'abandoned')) = (completed_at IS NOT NULL))
);

-- One ESS lifecycle token can contain multiple ECS instances. Completion is
-- therefore action-wide and is allowed only after every child instance is
-- ready (scale-out) or safely revoked (scale-in).
CREATE TABLE manager.runtime_node_lifecycle_action_instances (
    lifecycle_action_token TEXT NOT NULL
        REFERENCES manager.runtime_node_lifecycle_actions(lifecycle_action_token) ON DELETE RESTRICT,
    provider_instance_id TEXT NOT NULL CHECK (octet_length(provider_instance_id) BETWEEN 1 AND 256),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (lifecycle_action_token, provider_instance_id)
);

CREATE INDEX idx_runtime_node_lifecycle_action_instances_provider
    ON manager.runtime_node_lifecycle_action_instances(provider_instance_id, lifecycle_action_token);

CREATE INDEX idx_runtime_node_lifecycle_actions_pending
    ON manager.runtime_node_lifecycle_actions(pool_id, state, first_observed_at)
    WHERE state IN ('pending', 'draining');

-- +goose Down

-- Node-pool lifecycle records participate in scale-in safety decisions and are
-- intentionally not discarded by an automated rollback.
-- +goose StatementBegin
DO $$
BEGIN
    RAISE EXCEPTION 'runtime node-pool migration cannot be rolled back'
        USING ERRCODE = '55000';
END;
$$;
-- +goose StatementEnd

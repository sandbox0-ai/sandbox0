-- +goose Up

-- Ctld reports allocatable resources for one exact dedicated Nomad node boot.
-- Sandbox0, not Nomad carrier allocations, schedules this node-local capacity.
CREATE TABLE manager.runtime_node_capacities (
    cluster_id TEXT NOT NULL,
    node_id TEXT NOT NULL,
    node_uid TEXT NOT NULL,
    node_boot_id TEXT NOT NULL,
    cpu_millicores BIGINT NOT NULL CHECK (cpu_millicores > 0),
    memory_bytes BIGINT NOT NULL CHECK (memory_bytes > 0),
    cpuset_cpus TEXT NOT NULL CHECK (octet_length(cpuset_cpus) BETWEEN 1 AND 4096),
    cpuset_mems TEXT NOT NULL CHECK (octet_length(cpuset_mems) BETWEEN 1 AND 4096),
    heartbeat_expires_at TIMESTAMPTZ NOT NULL,
    revision BIGINT NOT NULL DEFAULT 1 CHECK (revision > 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (cluster_id, node_id, node_uid, node_boot_id)
);

CREATE INDEX idx_runtime_node_capacities_live
    ON manager.runtime_node_capacities(cluster_id, heartbeat_expires_at, node_id, node_uid, node_boot_id);

-- One immutable row is both scheduling truth and metering truth. Capacity is
-- available only when this row is released after plugin-independent cleanup.
CREATE TABLE manager.runtime_resource_leases (
    lease_id TEXT PRIMARY KEY,
    slot_id TEXT NOT NULL UNIQUE
        REFERENCES manager.runtime_slots(slot_id) ON DELETE RESTRICT,
    operation_id TEXT NOT NULL UNIQUE,
    claim_id TEXT NOT NULL UNIQUE,
    cluster_id TEXT NOT NULL,
    node_id TEXT NOT NULL,
    node_uid TEXT NOT NULL,
    node_boot_id TEXT NOT NULL,
    cpu_millicores BIGINT NOT NULL CHECK (cpu_millicores > 0),
    cpu_period_micros BIGINT NOT NULL CHECK (cpu_period_micros > 0),
    cpu_quota_micros BIGINT NOT NULL CHECK (cpu_quota_micros > 0),
    cpu_shares BIGINT NOT NULL CHECK (cpu_shares BETWEEN 2 AND 262144),
    cpu_weight BIGINT NOT NULL CHECK (cpu_weight BETWEEN 1 AND 10000),
    cpuset_cpus TEXT NOT NULL CHECK (octet_length(cpuset_cpus) BETWEEN 1 AND 4096),
    cpuset_mems TEXT NOT NULL CHECK (octet_length(cpuset_mems) BETWEEN 1 AND 4096),
    memory_bytes BIGINT NOT NULL CHECK (memory_bytes > 0),
    pids_limit BIGINT NOT NULL CHECK (pids_limit > 0),
    cgroup_name TEXT NOT NULL UNIQUE
        CHECK (cgroup_name ~ '^s0-[0-9a-f]{64}$'),
    lease_digest BYTEA NOT NULL CHECK (octet_length(lease_digest) = 32),
    lease_state TEXT NOT NULL CHECK (lease_state IN ('active', 'released')),
    acquired_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    released_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    FOREIGN KEY (cluster_id, node_id, node_uid, node_boot_id)
        REFERENCES manager.runtime_node_capacities(cluster_id, node_id, node_uid, node_boot_id)
        ON DELETE RESTRICT,
    CHECK (
        (lease_state = 'active' AND released_at IS NULL)
        OR (lease_state = 'released' AND released_at IS NOT NULL)
    )
);

CREATE INDEX idx_runtime_resource_leases_capacity
    ON manager.runtime_resource_leases(
        cluster_id, node_id, node_uid, node_boot_id, lease_state
    );

ALTER TABLE manager.runtime_slots
    ADD COLUMN resource_lease_id TEXT UNIQUE
        REFERENCES manager.runtime_resource_leases(lease_id) ON DELETE RESTRICT;

ALTER TABLE manager.runtime_slots
    ADD CONSTRAINT runtime_slots_resource_lease_claim CHECK (
        resource_lease_id IS NULL OR claim_operation_id <> ''
    );

-- +goose Down

ALTER TABLE manager.runtime_slots
    DROP CONSTRAINT IF EXISTS runtime_slots_resource_lease_claim,
    DROP COLUMN IF EXISTS resource_lease_id;

DROP INDEX IF EXISTS manager.idx_runtime_resource_leases_capacity;
DROP TABLE IF EXISTS manager.runtime_resource_leases;
DROP INDEX IF EXISTS manager.idx_runtime_node_capacities_live;
DROP TABLE IF EXISTS manager.runtime_node_capacities;

-- +goose Up

CREATE TABLE IF NOT EXISTS manager.runtime_slots (
    slot_id TEXT PRIMARY KEY,
    cluster_id TEXT NOT NULL,
    allocation_id TEXT NOT NULL,
    allocation_namespace TEXT NOT NULL,
    node_id TEXT NOT NULL,
    node_uid TEXT NOT NULL,
    node_boot_id TEXT NOT NULL,
    netns_identity TEXT NOT NULL,
    control_endpoint TEXT NOT NULL,
    compatibility_digest TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN (
        'registered', 'fastpath_ready', 'claiming', 'starting', 'active',
        'quiescing', 'orphaned', 'terminal'
    )),
    revision BIGINT NOT NULL DEFAULT 1 CHECK (revision > 0),
    runtime_ready_digest BYTEA CHECK (
        runtime_ready_digest IS NULL OR octet_length(runtime_ready_digest) = 32
    ),
    network_ready_digest BYTEA CHECK (
        network_ready_digest IS NULL OR octet_length(network_ready_digest) = 32
    ),
    storage_ready_digest BYTEA CHECK (
        storage_ready_digest IS NULL OR octet_length(storage_ready_digest) = 32
    ),
    heartbeat_expires_at TIMESTAMPTZ NOT NULL,
    fastpath_ready_at TIMESTAMPTZ,
    claim_operation_id TEXT NOT NULL DEFAULT '',
    claim_id TEXT NOT NULL DEFAULT '',
    claim_cluster_filter TEXT NOT NULL DEFAULT '',
    claim_ttl_milliseconds BIGINT NOT NULL DEFAULT 0 CHECK (
        claim_ttl_milliseconds >= 0 AND claim_ttl_milliseconds <= 60000
    ),
    sandbox_id TEXT REFERENCES manager.sandboxes(sandbox_id) ON DELETE RESTRICT,
    filesystem_id TEXT REFERENCES manager.rootfs_filesystems(filesystem_id) ON DELETE RESTRICT,
    source_generation_id TEXT REFERENCES manager.rootfs_generations(generation_id) ON DELETE RESTRICT,
    writer_grant_id TEXT REFERENCES manager.rootfs_writer_grants(grant_id) ON DELETE RESTRICT,
    claim_lease_expires_at TIMESTAMPTZ,
    claimed_at TIMESTAMPTZ,
    launch_attempt TEXT NOT NULL DEFAULT '',
    runsc_container_id TEXT NOT NULL DEFAULT '',
    rootfs_binding_digest BYTEA CHECK (
        rootfs_binding_digest IS NULL OR octet_length(rootfs_binding_digest) = 32
    ),
    claim_network_digest BYTEA CHECK (
        claim_network_digest IS NULL OR octet_length(claim_network_digest) = 32
    ),
    starting_at TIMESTAMPTZ,
    procd_instance_id TEXT NOT NULL DEFAULT '',
    command_ready_digest BYTEA CHECK (
        command_ready_digest IS NULL OR octet_length(command_ready_digest) = 32
    ),
    command_ready_at TIMESTAMPTZ,
    quiescing_at TIMESTAMPTZ,
    orphan_observation_digest BYTEA CHECK (
        orphan_observation_digest IS NULL OR octet_length(orphan_observation_digest) = 32
    ),
    terminal_reason TEXT NOT NULL DEFAULT '',
    terminal_proof_digest BYTEA CHECK (
        terminal_proof_digest IS NULL OR octet_length(terminal_proof_digest) = 32
    ),
    terminal_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT runtime_slots_fastpath_proofs CHECK (
        state <> 'fastpath_ready' OR (
            runtime_ready_digest IS NOT NULL
            AND network_ready_digest IS NOT NULL
            AND storage_ready_digest IS NOT NULL
        )
    ),
    CONSTRAINT runtime_slots_claim_binding CHECK (
        (
            state IN ('registered', 'fastpath_ready')
            AND claim_operation_id = '' AND claim_id = ''
            AND claim_cluster_filter = '' AND claim_ttl_milliseconds = 0
            AND sandbox_id IS NULL AND filesystem_id IS NULL
            AND source_generation_id IS NULL AND writer_grant_id IS NULL
            AND claim_lease_expires_at IS NULL AND claimed_at IS NULL
        ) OR (
            state = 'terminal' AND claim_operation_id = '' AND claim_id = ''
            AND claim_cluster_filter = '' AND claim_ttl_milliseconds = 0
            AND sandbox_id IS NULL AND filesystem_id IS NULL
            AND source_generation_id IS NULL AND writer_grant_id IS NULL
            AND claim_lease_expires_at IS NULL AND claimed_at IS NULL
        ) OR (
            claim_operation_id <> '' AND claim_id <> ''
            AND claim_ttl_milliseconds BETWEEN 1000 AND 60000
            AND sandbox_id IS NOT NULL AND filesystem_id IS NOT NULL
            AND source_generation_id IS NOT NULL
            AND claim_lease_expires_at IS NOT NULL AND claimed_at IS NOT NULL
        )
    ),
    UNIQUE (cluster_id, allocation_id)
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_runtime_slots_claim_operation
    ON manager.runtime_slots(claim_operation_id)
    WHERE claim_operation_id <> '';

CREATE UNIQUE INDEX IF NOT EXISTS idx_runtime_slots_live_claim
    ON manager.runtime_slots(claim_id)
    WHERE claim_id <> '' AND state <> 'terminal';

CREATE UNIQUE INDEX IF NOT EXISTS idx_runtime_slots_writer_grant
    ON manager.runtime_slots(writer_grant_id)
    WHERE writer_grant_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_runtime_slots_fastpath_selection
    ON manager.runtime_slots(compatibility_digest, cluster_id, fastpath_ready_at, slot_id)
    WHERE state = 'fastpath_ready';

CREATE INDEX IF NOT EXISTS idx_runtime_slots_reconcile
    ON manager.runtime_slots(state, heartbeat_expires_at)
    WHERE state <> 'terminal';

CREATE INDEX IF NOT EXISTS idx_runtime_slots_claim_expiry
    ON manager.runtime_slots(claim_lease_expires_at, slot_id)
    WHERE state = 'claiming';

-- +goose Down

DROP INDEX IF EXISTS manager.idx_runtime_slots_claim_expiry;
DROP INDEX IF EXISTS manager.idx_runtime_slots_reconcile;
DROP INDEX IF EXISTS manager.idx_runtime_slots_fastpath_selection;
DROP INDEX IF EXISTS manager.idx_runtime_slots_writer_grant;
DROP INDEX IF EXISTS manager.idx_runtime_slots_live_claim;
DROP INDEX IF EXISTS manager.idx_runtime_slots_claim_operation;
DROP TABLE IF EXISTS manager.runtime_slots;

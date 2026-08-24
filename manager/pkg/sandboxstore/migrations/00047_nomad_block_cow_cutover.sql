-- +goose Up

-- A deployed mixed-runtime database can legitimately be at version 43. The
-- squashed fresh-install baseline already contains the durable importer and
-- resource-lease tables, so fold the former 44/45 forward migrations into
-- this irreversible cutover with idempotent DDL. This keeps upgrades from a
-- released schema possible without retaining a second runtime architecture.
CREATE TABLE IF NOT EXISTS manager.rootfs_import_operations (
    operation_id TEXT PRIMARY KEY
        CHECK (octet_length(operation_id) BETWEEN 1 AND 128),
    source_oci_ref TEXT NOT NULL
        CHECK (octet_length(source_oci_ref) BETWEEN 1 AND 2048),
    source_oci_digest TEXT NOT NULL
        CHECK (source_oci_digest ~ '^sha256:[0-9a-f]{64}$'),
    oci_os TEXT NOT NULL CHECK (oci_os = 'linux'),
    oci_architecture TEXT NOT NULL
        CHECK (octet_length(oci_architecture) BETWEEN 1 AND 64),
    oci_variant TEXT NOT NULL DEFAULT ''
        CHECK (octet_length(oci_variant) <= 64),
    format_generation INTEGER NOT NULL CHECK (format_generation > 0),
    procd_protocol TEXT NOT NULL
        CHECK (octet_length(procd_protocol) BETWEEN 1 AND 128),
    procd_digest TEXT NOT NULL
        CHECK (procd_digest ~ '^sha256:[0-9a-f]{64}$'),
    logical_size_bytes BIGINT NOT NULL
        CHECK (logical_size_bytes BETWEEN 314572800 AND 1099511627776
            AND logical_size_bytes % 4096 = 0),
    block_data_range_bytes INTEGER NOT NULL CHECK (block_data_range_bytes > 0),
    block_pack_bytes INTEGER NOT NULL CHECK (block_pack_bytes > 0),
    block_page_entries INTEGER NOT NULL CHECK (block_page_entries > 0),
    object_prefix TEXT NOT NULL
        CHECK (octet_length(object_prefix) BETWEEN 1 AND 512),
    state TEXT NOT NULL CHECK (state IN ('pending', 'building', 'ready', 'abandoned')),
    lease_owner TEXT,
    lease_token TEXT,
    lease_expires_at TIMESTAMPTZ,
    attempt_count INTEGER NOT NULL DEFAULT 0
        CHECK (attempt_count >= 0 AND attempt_count <= 1000000),
    result_artifact_digest TEXT
        REFERENCES manager.rootfs_base_artifacts(artifact_digest) ON DELETE RESTRICT,
    abandon_reason TEXT NOT NULL DEFAULT ''
        CHECK (octet_length(abandon_reason) <= 4096),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    ready_at TIMESTAMPTZ,
    abandoned_at TIMESTAMPTZ,
    CHECK (
        (state = 'pending'
            AND lease_owner IS NULL AND lease_token IS NULL AND lease_expires_at IS NULL
            AND result_artifact_digest IS NULL AND abandon_reason = ''
            AND ready_at IS NULL AND abandoned_at IS NULL)
        OR (state = 'building'
            AND lease_owner IS NOT NULL AND octet_length(lease_owner) BETWEEN 1 AND 256
            AND lease_token IS NOT NULL AND octet_length(lease_token) = 64
            AND lease_expires_at IS NOT NULL
            AND result_artifact_digest IS NULL AND abandon_reason = ''
            AND ready_at IS NULL AND abandoned_at IS NULL)
        OR (state = 'ready'
            AND lease_owner IS NULL AND lease_token IS NULL AND lease_expires_at IS NULL
            AND result_artifact_digest IS NOT NULL AND abandon_reason = ''
            AND ready_at IS NOT NULL AND abandoned_at IS NULL)
        OR (state = 'abandoned'
            AND lease_owner IS NULL AND lease_token IS NULL AND lease_expires_at IS NULL
            AND result_artifact_digest IS NULL AND abandon_reason <> ''
            AND ready_at IS NULL AND abandoned_at IS NOT NULL)
    )
);

CREATE INDEX IF NOT EXISTS idx_rootfs_import_operations_work
    ON manager.rootfs_import_operations(state, lease_expires_at, created_at, operation_id)
    WHERE state IN ('pending', 'building');
CREATE INDEX IF NOT EXISTS idx_rootfs_import_operations_terminal
    ON manager.rootfs_import_operations(updated_at, operation_id)
    WHERE state IN ('ready', 'abandoned');

CREATE TABLE IF NOT EXISTS manager.rootfs_import_operation_objects (
    operation_id TEXT NOT NULL
        REFERENCES manager.rootfs_import_operations(operation_id) ON DELETE CASCADE,
    object_key TEXT NOT NULL
        REFERENCES manager.rootfs_materialization_objects(object_key) ON DELETE RESTRICT,
    upload_state TEXT NOT NULL CHECK (upload_state IN ('prepared', 'published')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (operation_id, object_key)
);
CREATE INDEX IF NOT EXISTS idx_rootfs_import_operation_objects_state
    ON manager.rootfs_import_operation_objects(upload_state, updated_at, operation_id);

CREATE TABLE IF NOT EXISTS manager.rootfs_base_artifact_objects (
    artifact_digest TEXT NOT NULL
        REFERENCES manager.rootfs_base_artifacts(artifact_digest) ON DELETE CASCADE,
    object_key TEXT NOT NULL
        REFERENCES manager.rootfs_materialization_objects(object_key) ON DELETE RESTRICT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (artifact_digest, object_key)
);
CREATE INDEX IF NOT EXISTS idx_rootfs_base_artifact_objects_object
    ON manager.rootfs_base_artifact_objects(object_key, artifact_digest);

ALTER TABLE manager.rootfs_base_artifacts
    ADD COLUMN IF NOT EXISTS attestation BYTEA
        CHECK (octet_length(attestation) BETWEEN 1 AND 65536),
    ADD COLUMN IF NOT EXISTS manifest_digest TEXT,
    ADD COLUMN IF NOT EXISTS config_digest TEXT,
    ADD COLUMN IF NOT EXISTS procd_protocol TEXT,
    ADD COLUMN IF NOT EXISTS procd_digest TEXT,
    ADD COLUMN IF NOT EXISTS logical_size_bytes BIGINT,
    ADD COLUMN IF NOT EXISTS descriptor_digest TEXT;

CREATE TABLE IF NOT EXISTS manager.runtime_node_capacities (
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
CREATE INDEX IF NOT EXISTS idx_runtime_node_capacities_live
    ON manager.runtime_node_capacities(
        cluster_id, heartbeat_expires_at, node_id, node_uid, node_boot_id
    );

CREATE TABLE IF NOT EXISTS manager.runtime_resource_leases (
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
CREATE INDEX IF NOT EXISTS idx_runtime_resource_leases_capacity
    ON manager.runtime_resource_leases(
        cluster_id, node_id, node_uid, node_boot_id, lease_state
    );

ALTER TABLE manager.runtime_slots
    ADD COLUMN IF NOT EXISTS resource_lease_id TEXT UNIQUE
        REFERENCES manager.runtime_resource_leases(lease_id) ON DELETE RESTRICT;
-- +goose StatementBegin
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'manager.runtime_slots'::regclass
            AND conname = 'runtime_slots_resource_lease_claim'
    ) THEN
        ALTER TABLE manager.runtime_slots
            ADD CONSTRAINT runtime_slots_resource_lease_claim CHECK (
                resource_lease_id IS NULL OR claim_operation_id <> ''
            );
    END IF;
END;
$$;
-- +goose StatementEnd

-- Upgrade the last mixed-runtime schema in place. The cutover never guesses
-- how to translate a live Kubernetes sandbox or a diff-layer filesystem.
-- +goose StatementBegin
DO $$
DECLARE
    unsafe_runtime BOOLEAN := FALSE;
    unsafe_filesystem BOOLEAN := FALSE;
    unsafe_snapshot BOOLEAN := FALSE;
    unsafe_writer BOOLEAN := FALSE;
    unsafe_resources BOOLEAN := FALSE;
    unsafe_base_artifact BOOLEAN := FALSE;
BEGIN
	IF NOT EXISTS (
		SELECT 1 FROM information_schema.columns
		WHERE table_schema = 'manager' AND table_name = 'sandboxes'
			AND column_name = 'resource_millicpu'
	) OR NOT EXISTS (
		SELECT 1 FROM information_schema.columns
		WHERE table_schema = 'manager' AND table_name = 'sandboxes'
			AND column_name = 'resource_memory_mib'
	) THEN
		RAISE EXCEPTION 'Nomad-only cutover requires durable resource lease metering columns'
			USING ERRCODE = '55000';
	END IF;
	SELECT EXISTS (
		SELECT 1 FROM manager.sandboxes
		WHERE resource_millicpu <= 0 OR resource_memory_mib <= 0
	) INTO unsafe_resources;
	IF unsafe_resources THEN
		RAISE EXCEPTION 'Nomad-only cutover requires positive resource lease metering truth for every sandbox'
			USING ERRCODE = '55000';
	END IF;
	SELECT EXISTS (
		SELECT 1 FROM manager.rootfs_base_artifacts
		WHERE oci_os IS NULL OR oci_architecture IS NULL OR oci_variant IS NULL
			OR procd_protocol IS NULL OR procd_protocol = ''
			OR procd_digest IS NULL OR procd_digest !~ '^sha256:[0-9a-f]{64}$'
			OR logical_size_bytes IS NULL
			OR logical_size_bytes < 314572800
			OR logical_size_bytes > 1099511627776
			OR logical_size_bytes % 4096 <> 0
	) INTO unsafe_base_artifact;
	IF unsafe_base_artifact THEN
		RAISE EXCEPTION 'Nomad block-COW cutover requires exact platform, procd, and logical-size truth for every RootFS base artifact'
			USING ERRCODE = '55000';
	END IF;

    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'manager' AND table_name = 'sandboxes'
            AND column_name = 'runtime_backend'
    ) THEN
        EXECUTE $query$
            SELECT EXISTS (
                SELECT 1 FROM manager.sandboxes
                WHERE runtime_backend <> 'nomad' AND deleted_at IS NULL
            )
        $query$ INTO unsafe_runtime;
    END IF;
    IF unsafe_runtime THEN
        RAISE EXCEPTION 'Nomad-only cutover requires every live sandbox runtime_backend to be nomad'
            USING ERRCODE = '55000';
    END IF;

    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'manager' AND table_name = 'rootfs_filesystems'
            AND column_name = 'storage_format'
    ) THEN
        EXECUTE $query$
            SELECT EXISTS (
                SELECT 1 FROM manager.rootfs_filesystems
                WHERE storage_format <> 'block-cow-v1'
            )
        $query$ INTO unsafe_filesystem;
    END IF;
    IF unsafe_filesystem THEN
        RAISE EXCEPTION 'block-COW cutover requires every retained filesystem to use block-cow-v1'
            USING ERRCODE = '55000';
    END IF;

    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'manager' AND table_name = 'rootfs_snapshots'
            AND column_name = 'head_layer_id'
    ) THEN
        EXECUTE $query$
            SELECT EXISTS (
                SELECT 1 FROM manager.rootfs_snapshots
                WHERE head_generation_id IS NULL OR filesystem_id IS NULL
            )
        $query$ INTO unsafe_snapshot;
    END IF;
    IF unsafe_snapshot THEN
        RAISE EXCEPTION 'block-COW cutover requires every retained snapshot to reference a generation'
            USING ERRCODE = '55000';
    END IF;

    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'manager' AND table_name = 'rootfs_writer_grants'
            AND column_name = 'initial_head_layer_id'
    ) THEN
        EXECUTE $query$
            SELECT EXISTS (
                SELECT 1 FROM manager.rootfs_writer_grants
                WHERE initial_generation_id = ''
            )
        $query$ INTO unsafe_writer;
    END IF;
    IF unsafe_writer THEN
        RAISE EXCEPTION 'block-COW cutover requires every retained writer grant to reference an initial generation'
            USING ERRCODE = '55000';
    END IF;
END;
$$;
-- +goose StatementEnd

DROP TRIGGER IF EXISTS enqueue_nomad_sandbox_metering_from_sandbox
    ON manager.sandboxes;

-- +goose StatementBegin
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'manager' AND table_name = 'sandboxes'
            AND column_name = 'current_pod_namespace'
    ) THEN
        ALTER TABLE manager.sandboxes
            RENAME COLUMN current_pod_namespace TO runtime_namespace;
        ALTER TABLE manager.sandboxes
            RENAME COLUMN current_pod_name TO runtime_id;
        ALTER INDEX IF EXISTS manager.idx_sandboxes_current_pod
            RENAME TO idx_sandboxes_runtime;
    END IF;

    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'manager' AND table_name = 'sandbox_lifecycle_txns'
            AND column_name = 'from_pod_namespace'
    ) THEN
        ALTER TABLE manager.sandbox_lifecycle_txns
            RENAME COLUMN from_pod_namespace TO from_runtime_namespace;
        ALTER TABLE manager.sandbox_lifecycle_txns
            RENAME COLUMN from_pod_name TO from_runtime_id;
        ALTER TABLE manager.sandbox_lifecycle_txns
            RENAME COLUMN to_pod_namespace TO to_runtime_namespace;
        ALTER TABLE manager.sandbox_lifecycle_txns
            RENAME COLUMN to_pod_name TO to_runtime_id;
    END IF;

    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'manager' AND table_name = 'rootfs_writer_grants'
            AND column_name = 'runtime_pod_namespace'
    ) THEN
        ALTER TABLE manager.rootfs_writer_grants
            RENAME COLUMN runtime_pod_namespace TO runtime_namespace;
        ALTER TABLE manager.rootfs_writer_grants
            RENAME COLUMN runtime_pod_name TO runtime_id;
        ALTER TABLE manager.rootfs_writer_grants
            RENAME COLUMN runtime_pod_uid TO runtime_incarnation_id;
        ALTER TABLE manager.rootfs_writer_grants
            RENAME COLUMN consumer_ctld_pod_uid TO consumer_agent_uid;
    END IF;

    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'manager' AND table_name = 'sandbox_lifecycle_txns'
            AND column_name = 'expected_head_layer_id'
    ) THEN
        ALTER TABLE manager.sandbox_lifecycle_txns
            RENAME COLUMN expected_head_layer_id TO expected_generation_id;
        ALTER TABLE manager.sandbox_lifecycle_txns
            RENAME COLUMN prepared_head_layer_id TO prepared_generation_id;
    END IF;
END;
$$;
-- +goose StatementEnd

ALTER TABLE manager.sandboxes
    DROP CONSTRAINT IF EXISTS sandboxes_runtime_backend_check,
    DROP COLUMN IF EXISTS runtime_backend;

ALTER TABLE manager.sandboxes
    DROP CONSTRAINT IF EXISTS sandboxes_resource_millicpu_check,
    DROP CONSTRAINT IF EXISTS sandboxes_resource_memory_mib_check,
    ALTER COLUMN resource_millicpu DROP DEFAULT,
    ALTER COLUMN resource_memory_mib DROP DEFAULT;
ALTER TABLE manager.sandboxes
    ADD CONSTRAINT sandboxes_resource_millicpu_check CHECK (resource_millicpu > 0),
    ADD CONSTRAINT sandboxes_resource_memory_mib_check CHECK (resource_memory_mib > 0);

ALTER TABLE manager.rootfs_base_artifacts
	DROP CONSTRAINT IF EXISTS rootfs_base_artifacts_logical_size_bytes_check,
	DROP CONSTRAINT IF EXISTS rootfs_base_artifacts_procd_digest_check,
	DROP CONSTRAINT IF EXISTS rootfs_base_artifacts_procd_protocol_check,
	ALTER COLUMN oci_os SET NOT NULL,
	ALTER COLUMN oci_architecture SET NOT NULL,
	ALTER COLUMN oci_variant SET NOT NULL,
	ALTER COLUMN procd_protocol SET NOT NULL,
	ALTER COLUMN procd_digest SET NOT NULL,
	ALTER COLUMN logical_size_bytes SET NOT NULL;
ALTER TABLE manager.rootfs_base_artifacts
	ADD CONSTRAINT rootfs_base_artifacts_logical_size_bytes_check CHECK (
		logical_size_bytes BETWEEN 314572800 AND 1099511627776
		AND logical_size_bytes % 4096 = 0
	),
	ADD CONSTRAINT rootfs_base_artifacts_procd_digest_check CHECK (
		procd_digest ~ '^sha256:[0-9a-f]{64}$'
	),
	ADD CONSTRAINT rootfs_base_artifacts_procd_protocol_check CHECK (
		octet_length(procd_protocol) BETWEEN 1 AND 128
	);

-- Version 30 created this index before the importer attestation fields
-- existed. Rebuild it to match the squashed baseline and exact ready-artifact
-- lookup used by the durable importer.
DROP INDEX IF EXISTS manager.idx_rootfs_base_artifacts_source_platform_ready;
CREATE INDEX idx_rootfs_base_artifacts_source_platform_ready
	ON manager.rootfs_base_artifacts(
		source_oci_digest, oci_os, oci_architecture, oci_variant,
		logical_size_bytes, format_generation, procd_protocol, procd_digest,
		created_at DESC
	)
	WHERE state = 'ready'
		AND oci_os IS NOT NULL
		AND oci_architecture IS NOT NULL
		AND oci_variant IS NOT NULL
		AND logical_size_bytes IS NOT NULL
		AND procd_protocol IS NOT NULL
		AND procd_digest IS NOT NULL;

DROP INDEX IF EXISTS manager.idx_sandboxes_nomad_hard_expiry;
DROP INDEX IF EXISTS manager.idx_sandboxes_nomad_soft_expiry;
CREATE INDEX IF NOT EXISTS idx_sandboxes_hard_expiry
    ON manager.sandboxes(hard_expires_at, sandbox_id)
    WHERE deleted_at IS NULL
        AND desired_state IN ('active', 'paused')
        AND hard_expires_at IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_sandboxes_soft_expiry
    ON manager.sandboxes(expires_at, sandbox_id)
    WHERE deleted_at IS NULL
        AND desired_state = 'active'
        AND expires_at IS NOT NULL;

ALTER TABLE manager.sandbox_lifecycle_txns
    DROP CONSTRAINT IF EXISTS sandbox_lifecycle_txns_rebase_identity_check;
ALTER TABLE manager.sandbox_lifecycle_txns
    ADD CONSTRAINT sandbox_lifecycle_txns_rebase_identity_check CHECK (
        kind <> 'rebase' OR (
            source = 'manual' AND NOT cancelable AND from_generation = to_generation
            AND from_runtime_namespace = '' AND from_runtime_id = ''
            AND to_runtime_namespace = '' AND to_runtime_id = ''
            AND target_sandbox_id = '' AND octet_length(target_record_digest) = 0
            AND target_generation_id <> ''
            AND source_base_artifact_digest <> '' AND target_base_artifact_digest <> ''
            AND source_base_artifact_digest <> target_base_artifact_digest
            AND expected_generation_id <> '' AND rollback_expires_at IS NOT NULL
            AND (
                (worker_cluster_id = '' AND worker_node_id = '' AND worker_node_uid = ''
                    AND phase IN ('committed', 'aborted'))
                OR (worker_cluster_id <> '' AND worker_node_id <> '' AND worker_node_uid <> '')
            )
            AND octet_length(worker_proof_digest) IN (0, 32)
            AND (worker_acknowledged_at IS NULL OR (
                phase IN ('committed', 'aborted') AND octet_length(worker_proof_digest) = 32
            ))
        )
    );

ALTER TABLE manager.rootfs_filesystems
    DROP CONSTRAINT IF EXISTS rootfs_filesystems_format_shape_check,
    DROP CONSTRAINT IF EXISTS rootfs_filesystems_storage_format_check,
    DROP CONSTRAINT IF EXISTS rootfs_filesystems_format_generation_check;
DROP INDEX IF EXISTS manager.idx_rootfs_filesystems_head;
ALTER TABLE manager.rootfs_filesystems
    DROP COLUMN IF EXISTS head_layer_id,
    DROP COLUMN IF EXISTS base_image_ref,
    DROP COLUMN IF EXISTS base_image_digest,
    DROP COLUMN IF EXISTS storage_format,
    ALTER COLUMN base_artifact_digest SET NOT NULL,
    ALTER COLUMN format_generation SET NOT NULL;
ALTER TABLE manager.rootfs_filesystems
    ADD CONSTRAINT rootfs_filesystems_format_generation_check
        CHECK (format_generation > 0);

ALTER TABLE manager.rootfs_snapshots
    DROP CONSTRAINT IF EXISTS rootfs_snapshots_head_shape_check;
DROP INDEX IF EXISTS manager.idx_rootfs_snapshots_head;
ALTER TABLE manager.rootfs_snapshots
    DROP COLUMN IF EXISTS head_layer_id,
    ALTER COLUMN filesystem_id SET NOT NULL,
    ALTER COLUMN head_generation_id SET NOT NULL;

ALTER TABLE manager.rootfs_writer_grants
    DROP CONSTRAINT IF EXISTS rootfs_writer_grants_initial_generation_check,
    DROP COLUMN IF EXISTS initial_head_layer_id;
ALTER TABLE manager.rootfs_writer_grants
    ADD CONSTRAINT rootfs_writer_grants_initial_generation_check
        CHECK (initial_generation_id <> '');

ALTER TABLE manager.rootfs_materialization_objects
    ADD COLUMN IF NOT EXISTS missing_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS last_error TEXT NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS last_audited_at TIMESTAMPTZ;
CREATE INDEX IF NOT EXISTS idx_rootfs_materialization_objects_missing
    ON manager.rootfs_materialization_objects(missing_at)
    WHERE missing_at IS NOT NULL;

DROP TABLE IF EXISTS manager.sandbox_rootfs_states;
DROP TABLE IF EXISTS manager.sandbox_rootfs_heads;
DROP TABLE IF EXISTS manager.rootfs_objects;
DROP TABLE IF EXISTS manager.rootfs_layers;
DROP SCHEMA IF EXISTS storage_proxy CASCADE;

-- +goose StatementBegin
CREATE OR REPLACE FUNCTION manager.enqueue_nomad_sandbox_metering_projection(target_sandbox_id TEXT)
RETURNS VOID AS $$
DECLARE
    next_revision BIGINT;
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM manager.sandboxes WHERE sandbox_id = target_sandbox_id
    ) THEN
        RETURN;
    END IF;

    next_revision := nextval('manager.sandbox_metering_revision_seq');
    INSERT INTO manager.sandbox_metering_projection_queue (
        sandbox_id, revision, attempts, available_at, last_error, created_at, updated_at
    ) VALUES (
        target_sandbox_id, next_revision, 0, NOW(), '', NOW(), NOW()
    )
    ON CONFLICT (sandbox_id) DO UPDATE
    SET revision = EXCLUDED.revision,
        attempts = 0,
        available_at = NOW(),
        last_error = '',
        updated_at = NOW();

    PERFORM pg_notify('sandbox0_nomad_metering', target_sandbox_id);
END;
$$ LANGUAGE plpgsql;
-- +goose StatementEnd

CREATE TRIGGER enqueue_nomad_sandbox_metering_from_sandbox
    AFTER INSERT OR UPDATE OF
        team_id, user_id, template_id, cluster_id,
        desired_state, runtime_id, runtime_namespace,
        runtime_generation, owner_kind, claimed_at, deleted_at,
        resource_millicpu, resource_memory_mib
    ON manager.sandboxes
    FOR EACH ROW
    EXECUTE FUNCTION manager.enqueue_nomad_sandbox_metering_from_sandbox();

INSERT INTO manager.sandbox_metering_projection_queue (sandbox_id, revision)
SELECT sandbox_id, nextval('manager.sandbox_metering_revision_seq')
FROM manager.sandboxes
ON CONFLICT (sandbox_id) DO NOTHING;

-- +goose Down

-- This physical deletion cutover is intentionally irreversible.
-- +goose StatementBegin
DO $$
BEGIN
    RAISE EXCEPTION 'Nomad block-COW cutover cannot be rolled back'
        USING ERRCODE = '55000';
END;
$$;
-- +goose StatementEnd

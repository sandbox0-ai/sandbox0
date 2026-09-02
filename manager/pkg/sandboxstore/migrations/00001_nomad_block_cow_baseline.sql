-- +goose Up

-- Fresh Nomad/gVisor and block-COW manager schema. This baseline contains no
-- superseded runtime identity or diff-layer compatibility objects.

-- +goose StatementBegin
CREATE FUNCTION manager.enqueue_nomad_sandbox_metering_from_lifecycle() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    IF NEW.phase IN ('committed', 'aborted')
        AND (TG_OP = 'INSERT'
            OR OLD.phase IS DISTINCT FROM NEW.phase
            OR OLD.committed_at IS DISTINCT FROM NEW.committed_at
            OR OLD.aborted_at IS DISTINCT FROM NEW.aborted_at) THEN
        PERFORM manager.enqueue_nomad_sandbox_metering_projection(NEW.sandbox_id);
    END IF;
    RETURN NEW;
END;
$$;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE FUNCTION manager.enqueue_nomad_sandbox_metering_from_sandbox() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    PERFORM manager.enqueue_nomad_sandbox_metering_projection(NEW.sandbox_id);
    RETURN NEW;
END;
$$;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE FUNCTION manager.enqueue_nomad_sandbox_metering_projection(target_sandbox_id text) RETURNS void
    LANGUAGE plpgsql
    AS $$
DECLARE
    next_revision BIGINT;
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM manager.sandboxes
        WHERE sandbox_id = target_sandbox_id
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
$$;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE FUNCTION manager.prevent_live_rootfs_writer_delete() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM manager.rootfs_writer_grants
        WHERE filesystem_id = OLD.filesystem_id
          AND state IN ('issued', 'consumed', 'retiring')
    ) THEN
        RAISE EXCEPTION 'rootfs filesystem % has a live writer grant', OLD.filesystem_id
            USING ERRCODE = '55000';
    END IF;
    RETURN OLD;
END;
$$;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE FUNCTION manager.update_updated_at_column() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$;
-- +goose StatementEnd

SET default_tablespace = '';

SET default_table_access_method = heap;

CREATE TABLE manager.rootfs_base_artifact_objects (
    artifact_digest text NOT NULL,
    object_key text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);

CREATE TABLE manager.rootfs_base_artifacts (
    artifact_digest text NOT NULL,
    source_oci_ref text NOT NULL,
    source_oci_digest text NOT NULL,
    base_block_root text NOT NULL,
    format_generation integer NOT NULL,
    state text NOT NULL,
    descriptor bytea NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    oci_os text NOT NULL,
    oci_architecture text NOT NULL,
    oci_variant text NOT NULL,
    attestation bytea,
    manifest_digest text,
    config_digest text,
    procd_protocol text NOT NULL,
    procd_digest text NOT NULL,
    logical_size_bytes bigint NOT NULL,
    descriptor_digest text,
    CONSTRAINT rootfs_base_artifacts_attestation_check CHECK (((octet_length(attestation) >= 1) AND (octet_length(attestation) <= 65536))),
    CONSTRAINT rootfs_base_artifacts_descriptor_check CHECK ((octet_length(descriptor) <= 65536)),
    CONSTRAINT rootfs_base_artifacts_format_generation_check CHECK ((format_generation > 0)),
    CONSTRAINT rootfs_base_artifacts_logical_size_bytes_check CHECK ((((logical_size_bytes >= 314572800) AND (logical_size_bytes <= '1099511627776'::bigint)) AND ((logical_size_bytes % (4096)::bigint) = 0))),
    CONSTRAINT rootfs_base_artifacts_procd_digest_check CHECK ((procd_digest ~ '^sha256:[0-9a-f]{64}$'::text)),
    CONSTRAINT rootfs_base_artifacts_procd_protocol_check CHECK (((octet_length(procd_protocol) >= 1) AND (octet_length(procd_protocol) <= 128))),
    CONSTRAINT rootfs_base_artifacts_state_check CHECK ((state = ANY (ARRAY['building'::text, 'ready'::text, 'failed'::text, 'retired'::text])))
);

CREATE TABLE manager.rootfs_composite_backlog_policy (
    singleton boolean DEFAULT true NOT NULL,
    max_descriptor_bytes bigint NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT rootfs_composite_backlog_policy_max_descriptor_bytes_check CHECK ((max_descriptor_bytes > 0)),
    CONSTRAINT rootfs_composite_backlog_policy_singleton_check CHECK (singleton)
);

CREATE TABLE manager.rootfs_filesystems (
    filesystem_id text NOT NULL,
    team_id text NOT NULL,
    source_filesystem_id text,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    writer_epoch bigint DEFAULT 0 NOT NULL,
    base_artifact_digest text NOT NULL,
    format_generation integer NOT NULL,
    head_generation_id text,
    CONSTRAINT rootfs_filesystems_format_generation_check CHECK ((format_generation > 0)),
    CONSTRAINT rootfs_filesystems_writer_epoch_check CHECK ((writer_epoch >= 0))
);

CREATE TABLE manager.rootfs_generation_materialization_objects (
    generation_id text NOT NULL,
    locator_version bigint NOT NULL,
    object_key text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT rootfs_generation_materialization_objects_locator_version_check CHECK ((locator_version > 0))
);

CREATE TABLE manager.rootfs_generations (
    generation_id text NOT NULL,
    filesystem_id text NOT NULL,
    parent_generation_id text,
    source_oci_digest text NOT NULL,
    base_artifact_digest text NOT NULL,
    base_block_root text NOT NULL,
    current_block_head text NOT NULL,
    writer_epoch bigint NOT NULL,
    format_generation integer NOT NULL,
    durability_state text NOT NULL,
    locator_version bigint NOT NULL,
    descriptor bytea NOT NULL,
    reset_copied_session_state boolean DEFAULT false NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT rootfs_generations_check CHECK (((parent_generation_id IS NULL) OR (parent_generation_id <> generation_id))),
    CONSTRAINT rootfs_generations_descriptor_check CHECK ((octet_length(descriptor) <= 65536)),
    CONSTRAINT rootfs_generations_durability_state_check CHECK ((durability_state = ANY (ARRAY['local_sealed'::text, 'composite_durable'::text, 's3_materialized'::text]))),
    CONSTRAINT rootfs_generations_format_generation_check CHECK ((format_generation > 0)),
    CONSTRAINT rootfs_generations_locator_version_check CHECK ((locator_version > 0)),
    CONSTRAINT rootfs_generations_writer_epoch_check CHECK ((writer_epoch >= 0))
);

CREATE TABLE manager.rootfs_head_rollbacks (
    operation_id text NOT NULL,
    filesystem_id text NOT NULL,
    sandbox_id text NOT NULL,
    team_id text NOT NULL,
    operation_kind text NOT NULL,
    old_generation_id text NOT NULL,
    new_generation_id text NOT NULL,
    health_check_digest bytea,
    state text DEFAULT 'available'::text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    expires_at timestamp with time zone,
    rolled_back_at timestamp with time zone,
    CONSTRAINT rootfs_head_rollbacks_check CHECK ((old_generation_id <> new_generation_id)),
    CONSTRAINT rootfs_head_rollbacks_health_check_digest_check CHECK (((health_check_digest IS NULL) OR (octet_length(health_check_digest) = 32))),
    CONSTRAINT rootfs_head_rollbacks_operation_kind_check CHECK ((operation_kind = ANY (ARRAY['restore'::text, 'rebase'::text]))),
    CONSTRAINT rootfs_head_rollbacks_state_check CHECK ((state = ANY (ARRAY['available'::text, 'rolled_back'::text, 'expired'::text])))
);

CREATE TABLE manager.rootfs_import_operation_objects (
    operation_id text NOT NULL,
    object_key text NOT NULL,
    upload_state text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT rootfs_import_operation_objects_upload_state_check CHECK ((upload_state = ANY (ARRAY['prepared'::text, 'published'::text])))
);

CREATE TABLE manager.rootfs_import_operations (
    operation_id text NOT NULL,
    source_oci_ref text NOT NULL,
    source_oci_digest text NOT NULL,
    oci_os text NOT NULL,
    oci_architecture text NOT NULL,
    oci_variant text DEFAULT ''::text NOT NULL,
    format_generation integer NOT NULL,
    procd_protocol text NOT NULL,
    procd_digest text NOT NULL,
    logical_size_bytes bigint NOT NULL,
    block_data_range_bytes integer NOT NULL,
    block_pack_bytes integer NOT NULL,
    block_page_entries integer NOT NULL,
    object_prefix text NOT NULL,
    state text NOT NULL,
    lease_owner text,
    lease_token text,
    lease_expires_at timestamp with time zone,
    attempt_count integer DEFAULT 0 NOT NULL,
    result_artifact_digest text,
    abandon_reason text DEFAULT ''::text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    ready_at timestamp with time zone,
    abandoned_at timestamp with time zone,
    CONSTRAINT rootfs_import_operations_abandon_reason_check CHECK ((octet_length(abandon_reason) <= 4096)),
    CONSTRAINT rootfs_import_operations_attempt_count_check CHECK (((attempt_count >= 0) AND (attempt_count <= 1000000))),
    CONSTRAINT rootfs_import_operations_block_data_range_bytes_check CHECK ((block_data_range_bytes > 0)),
    CONSTRAINT rootfs_import_operations_block_pack_bytes_check CHECK ((block_pack_bytes > 0)),
    CONSTRAINT rootfs_import_operations_block_page_entries_check CHECK ((block_page_entries > 0)),
    CONSTRAINT rootfs_import_operations_check CHECK ((((state = 'pending'::text) AND (lease_owner IS NULL) AND (lease_token IS NULL) AND (lease_expires_at IS NULL) AND (result_artifact_digest IS NULL) AND (abandon_reason = ''::text) AND (ready_at IS NULL) AND (abandoned_at IS NULL)) OR ((state = 'building'::text) AND (lease_owner IS NOT NULL) AND ((octet_length(lease_owner) >= 1) AND (octet_length(lease_owner) <= 256)) AND (lease_token IS NOT NULL) AND (octet_length(lease_token) = 64) AND (lease_expires_at IS NOT NULL) AND (result_artifact_digest IS NULL) AND (abandon_reason = ''::text) AND (ready_at IS NULL) AND (abandoned_at IS NULL)) OR ((state = 'ready'::text) AND (lease_owner IS NULL) AND (lease_token IS NULL) AND (lease_expires_at IS NULL) AND (result_artifact_digest IS NOT NULL) AND (abandon_reason = ''::text) AND (ready_at IS NOT NULL) AND (abandoned_at IS NULL)) OR ((state = 'abandoned'::text) AND (lease_owner IS NULL) AND (lease_token IS NULL) AND (lease_expires_at IS NULL) AND (result_artifact_digest IS NULL) AND (abandon_reason <> ''::text) AND (ready_at IS NULL) AND (abandoned_at IS NOT NULL)))),
    CONSTRAINT rootfs_import_operations_format_generation_check CHECK ((format_generation > 0)),
    CONSTRAINT rootfs_import_operations_logical_size_bytes_check CHECK ((((logical_size_bytes >= 314572800) AND (logical_size_bytes <= '1099511627776'::bigint)) AND ((logical_size_bytes % (4096)::bigint) = 0))),
    CONSTRAINT rootfs_import_operations_object_prefix_check CHECK (((octet_length(object_prefix) >= 1) AND (octet_length(object_prefix) <= 512))),
    CONSTRAINT rootfs_import_operations_oci_architecture_check CHECK (((octet_length(oci_architecture) >= 1) AND (octet_length(oci_architecture) <= 64))),
    CONSTRAINT rootfs_import_operations_oci_os_check CHECK ((oci_os = 'linux'::text)),
    CONSTRAINT rootfs_import_operations_oci_variant_check CHECK ((octet_length(oci_variant) <= 64)),
    CONSTRAINT rootfs_import_operations_operation_id_check CHECK (((octet_length(operation_id) >= 1) AND (octet_length(operation_id) <= 128))),
    CONSTRAINT rootfs_import_operations_procd_digest_check CHECK ((procd_digest ~ '^sha256:[0-9a-f]{64}$'::text)),
    CONSTRAINT rootfs_import_operations_procd_protocol_check CHECK (((octet_length(procd_protocol) >= 1) AND (octet_length(procd_protocol) <= 128))),
    CONSTRAINT rootfs_import_operations_source_oci_digest_check CHECK ((source_oci_digest ~ '^sha256:[0-9a-f]{64}$'::text)),
    CONSTRAINT rootfs_import_operations_source_oci_ref_check CHECK (((octet_length(source_oci_ref) >= 1) AND (octet_length(source_oci_ref) <= 2048))),
    CONSTRAINT rootfs_import_operations_state_check CHECK ((state = ANY (ARRAY['pending'::text, 'building'::text, 'ready'::text, 'abandoned'::text])))
);

CREATE TABLE manager.rootfs_materialization_batch_objects (
    batch_id text NOT NULL,
    object_key text NOT NULL,
    upload_state text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT rootfs_materialization_batch_objects_upload_state_check CHECK ((upload_state = ANY (ARRAY['registered'::text, 'uploaded'::text])))
);

CREATE TABLE manager.rootfs_materialization_batches (
    batch_id text NOT NULL,
    pack_lane text NOT NULL,
    team_id text NOT NULL,
    format_generation integer NOT NULL,
    member_count integer NOT NULL,
    state text NOT NULL,
    abandon_reason text DEFAULT ''::text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    published_at timestamp with time zone,
    abandoned_at timestamp with time zone,
    CONSTRAINT rootfs_materialization_batches_check CHECK ((((state = 'uploading'::text) AND (published_at IS NULL) AND (abandoned_at IS NULL) AND (abandon_reason = ''::text)) OR ((state = 'published'::text) AND (published_at IS NOT NULL) AND (abandoned_at IS NULL) AND (abandon_reason = ''::text)) OR ((state = 'abandoned'::text) AND (published_at IS NULL) AND (abandoned_at IS NOT NULL) AND (abandon_reason <> ''::text)))),
    CONSTRAINT rootfs_materialization_batches_format_generation_check CHECK ((format_generation > 0)),
    CONSTRAINT rootfs_materialization_batches_member_count_check CHECK (((member_count > 0) AND (member_count <= 10000))),
    CONSTRAINT rootfs_materialization_batches_pack_lane_check CHECK (((pack_lane <> ''::text) AND (octet_length(pack_lane) <= 256))),
    CONSTRAINT rootfs_materialization_batches_state_check CHECK ((state = ANY (ARRAY['uploading'::text, 'published'::text, 'abandoned'::text]))),
    CONSTRAINT rootfs_materialization_batches_team_id_check CHECK ((team_id <> ''::text))
);

CREATE TABLE manager.rootfs_materialization_member_objects (
    batch_id text NOT NULL,
    ordinal integer NOT NULL,
    object_key text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);

CREATE TABLE manager.rootfs_materialization_members (
    batch_id text NOT NULL,
    ordinal integer NOT NULL,
    generation_id text NOT NULL,
    expected_locator_version bigint NOT NULL,
    expected_descriptor bytea NOT NULL,
    expected_descriptor_digest bytea NOT NULL,
    state text NOT NULL,
    materialized_descriptor bytea,
    published_locator_version bigint,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT rootfs_materialization_members_check CHECK ((((state = 'published'::text) AND (materialized_descriptor IS NOT NULL) AND (published_locator_version = (expected_locator_version + 1))) OR ((state = ANY (ARRAY['uploading'::text, 'abandoned'::text])) AND (materialized_descriptor IS NULL) AND (published_locator_version IS NULL)))),
    CONSTRAINT rootfs_materialization_members_expected_descriptor_check CHECK (((octet_length(expected_descriptor) >= 1) AND (octet_length(expected_descriptor) <= 65536))),
    CONSTRAINT rootfs_materialization_members_expected_descriptor_digest_check CHECK ((octet_length(expected_descriptor_digest) = 32)),
    CONSTRAINT rootfs_materialization_members_expected_locator_version_check CHECK ((expected_locator_version > 0)),
    CONSTRAINT rootfs_materialization_members_ordinal_check CHECK (((ordinal >= 0) AND (ordinal < 10000))),
    CONSTRAINT rootfs_materialization_members_state_check CHECK ((state = ANY (ARRAY['uploading'::text, 'published'::text, 'abandoned'::text])))
);

CREATE TABLE manager.rootfs_materialization_objects (
    object_key text NOT NULL,
    object_kind text NOT NULL,
    object_size bigint NOT NULL,
    checksum text NOT NULL,
    uploaded_at timestamp with time zone,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    missing_at timestamp with time zone,
    last_error text DEFAULT ''::text NOT NULL,
    last_audited_at timestamp with time zone,
    CONSTRAINT rootfs_materialization_objects_checksum_check CHECK ((checksum ~ '^sha256:[0-9a-f]{64}$'::text)),
    CONSTRAINT rootfs_materialization_objects_object_kind_check CHECK ((object_kind = ANY (ARRAY['data_pack'::text, 'mapping_page'::text]))),
    CONSTRAINT rootfs_materialization_objects_object_size_check CHECK ((object_size > 0))
);

CREATE TABLE manager.rootfs_object_deletions (
    object_key text NOT NULL,
    team_id text DEFAULT ''::text NOT NULL,
    attempts integer DEFAULT 0 NOT NULL,
    last_error text DEFAULT ''::text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    last_attempt_at timestamp with time zone,
    next_attempt_at timestamp with time zone DEFAULT now() NOT NULL,
    claimed_by text DEFAULT ''::text NOT NULL,
    claimed_until timestamp with time zone,
    dead_lettered_at timestamp with time zone
);

CREATE TABLE manager.rootfs_running_forks (
    operation_id text NOT NULL,
    source_sandbox_id text NOT NULL,
    source_filesystem_id text NOT NULL,
    source_grant_id text NOT NULL,
    source_writer_epoch bigint NOT NULL,
    source_generation_id text NOT NULL,
    target_sandbox_id text NOT NULL,
    target_filesystem_id text NOT NULL,
    checkpoint_generation_id text NOT NULL,
    binding_version integer NOT NULL,
    binding_digest bytea NOT NULL,
    checkpoint_sequence bigint NOT NULL,
    checkpoint_descriptor_digest text NOT NULL,
    checkpoint_proof_digest bytea NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT rootfs_running_forks_binding_digest_check CHECK ((octet_length(binding_digest) = 32)),
    CONSTRAINT rootfs_running_forks_binding_version_check CHECK ((binding_version > 0)),
    CONSTRAINT rootfs_running_forks_checkpoint_proof_digest_check CHECK ((octet_length(checkpoint_proof_digest) = 32)),
    CONSTRAINT rootfs_running_forks_checkpoint_sequence_check CHECK ((checkpoint_sequence >= 0)),
    CONSTRAINT rootfs_running_forks_source_writer_epoch_check CHECK ((source_writer_epoch > 0))
);

CREATE TABLE manager.rootfs_running_template_captures (
    operation_id text NOT NULL,
    snapshot_id text NOT NULL,
    team_id text NOT NULL,
    source_sandbox_id text NOT NULL,
    source_filesystem_id text NOT NULL,
    source_grant_id text NOT NULL,
    source_writer_epoch bigint NOT NULL,
    source_generation_id text NOT NULL,
    target_filesystem_id text NOT NULL,
    checkpoint_generation_id text NOT NULL,
    request_digest bytea NOT NULL,
    binding_version integer NOT NULL,
    binding_digest bytea NOT NULL,
    state text NOT NULL,
    checkpoint_sequence bigint,
    checkpoint_descriptor_digest text,
    checkpoint_proof_digest bytea,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    published_at timestamp with time zone,
    cancel_reason text DEFAULT ''::text NOT NULL,
    CONSTRAINT rootfs_running_template_captures_binding_digest_check CHECK ((octet_length(binding_digest) = 32)),
    CONSTRAINT rootfs_running_template_captures_binding_version_check CHECK ((binding_version > 0)),
    CONSTRAINT rootfs_running_template_captures_check CHECK ((((state = 'pending'::text) AND (checkpoint_sequence IS NULL) AND (checkpoint_descriptor_digest IS NULL) AND (checkpoint_proof_digest IS NULL) AND (published_at IS NULL) AND (cancel_reason = ''::text)) OR ((state = 'published'::text) AND (checkpoint_sequence IS NOT NULL) AND (checkpoint_descriptor_digest IS NOT NULL) AND (checkpoint_proof_digest IS NOT NULL) AND (published_at IS NOT NULL)))),
    CONSTRAINT rootfs_running_template_captures_checkpoint_proof_digest_check CHECK (((checkpoint_proof_digest IS NULL) OR (octet_length(checkpoint_proof_digest) = 32))),
    CONSTRAINT rootfs_running_template_captures_checkpoint_sequence_check CHECK ((checkpoint_sequence >= 0)),
    CONSTRAINT rootfs_running_template_captures_request_digest_check CHECK ((octet_length(request_digest) = 32)),
    CONSTRAINT rootfs_running_template_captures_source_writer_epoch_check CHECK ((source_writer_epoch > 0)),
    CONSTRAINT rootfs_running_template_captures_state_check CHECK ((state = ANY (ARRAY['pending'::text, 'published'::text])))
);

CREATE TABLE manager.rootfs_snapshots (
    snapshot_id text NOT NULL,
    team_id text NOT NULL,
    source_sandbox_id text DEFAULT ''::text NOT NULL,
    name text DEFAULT ''::text NOT NULL,
    description text DEFAULT ''::text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    expires_at timestamp with time zone,
    filesystem_id text NOT NULL,
    head_generation_id text NOT NULL
);

CREATE TABLE manager.rootfs_writer_grants (
    grant_id text NOT NULL,
    filesystem_id text NOT NULL,
    sandbox_id text NOT NULL,
    claim_id text NOT NULL,
    slot_id text NOT NULL,
    issue_operation_id text NOT NULL,
    writer_epoch bigint NOT NULL,
    state text NOT NULL,
    binding_version integer NOT NULL,
    binding_digest bytea NOT NULL,
    token_digest bytea NOT NULL,
    node_uid text NOT NULL,
    node_boot_id text NOT NULL,
    consumer_node_uid text DEFAULT ''::text NOT NULL,
    consumer_agent_uid text DEFAULT ''::text NOT NULL,
    consume_expires_at timestamp with time zone NOT NULL,
    consumed_at timestamp with time zone,
    lease_expires_at timestamp with time zone,
    retire_operation_id text DEFAULT ''::text NOT NULL,
    retire_kind text DEFAULT ''::text NOT NULL,
    retire_proof_digest bytea,
    retire_started_at timestamp with time zone,
    retired_at timestamp with time zone,
    canceled_at timestamp with time zone,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    initial_generation_id text DEFAULT ''::text NOT NULL,
    runtime_namespace text DEFAULT ''::text NOT NULL,
    runtime_id text DEFAULT ''::text NOT NULL,
    runtime_incarnation_id text DEFAULT ''::text NOT NULL,
    runtime_node_name text DEFAULT ''::text NOT NULL,
    runtime_gate_parent text DEFAULT ''::text NOT NULL,
    runtime_generation text DEFAULT ''::text NOT NULL,
    CONSTRAINT rootfs_writer_grants_binding_digest_check CHECK ((octet_length(binding_digest) = 32)),
    CONSTRAINT rootfs_writer_grants_binding_version_check CHECK ((binding_version > 0)),
    CONSTRAINT rootfs_writer_grants_initial_generation_check CHECK ((initial_generation_id <> ''::text)),
    CONSTRAINT rootfs_writer_grants_retire_kind_check CHECK ((retire_kind = ANY (ARRAY[''::text, 'planned_publish'::text, 'prelaunch_abort'::text, 'crash_abandon'::text]))),
    CONSTRAINT rootfs_writer_grants_state_check CHECK ((state = ANY (ARRAY['issued'::text, 'consumed'::text, 'retiring'::text, 'retired'::text, 'canceled'::text]))),
    CONSTRAINT rootfs_writer_grants_token_digest_check CHECK ((octet_length(token_digest) = 32)),
    CONSTRAINT rootfs_writer_grants_writer_epoch_check CHECK ((writer_epoch > 0))
);

CREATE TABLE manager.rootfs_writer_terminal_proofs (
    grant_id text NOT NULL,
    sandbox_id text NOT NULL,
    writer_epoch bigint NOT NULL,
    binding_version integer NOT NULL,
    binding_digest bytea NOT NULL,
    node_uid text NOT NULL,
    state text NOT NULL,
    expires_at timestamp with time zone NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT rootfs_writer_terminal_proofs_binding_digest_check CHECK ((octet_length(binding_digest) = 32)),
    CONSTRAINT rootfs_writer_terminal_proofs_binding_version_check CHECK ((binding_version > 0)),
    CONSTRAINT rootfs_writer_terminal_proofs_node_uid_check CHECK ((node_uid <> ''::text)),
    CONSTRAINT rootfs_writer_terminal_proofs_state_check CHECK ((state = ANY (ARRAY['retired'::text, 'canceled'::text]))),
    CONSTRAINT rootfs_writer_terminal_proofs_writer_epoch_check CHECK ((writer_epoch > 0))
);

CREATE TABLE manager.runtime_node_capacities (
    cluster_id text NOT NULL,
    node_id text NOT NULL,
    node_uid text NOT NULL,
    node_boot_id text NOT NULL,
    cpu_millicores bigint NOT NULL,
    memory_bytes bigint NOT NULL,
    cpuset_cpus text NOT NULL,
    cpuset_mems text NOT NULL,
    heartbeat_expires_at timestamp with time zone NOT NULL,
    revision bigint DEFAULT 1 NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT runtime_node_capacities_cpu_millicores_check CHECK ((cpu_millicores > 0)),
    CONSTRAINT runtime_node_capacities_cpuset_cpus_check CHECK (((octet_length(cpuset_cpus) >= 1) AND (octet_length(cpuset_cpus) <= 4096))),
    CONSTRAINT runtime_node_capacities_cpuset_mems_check CHECK (((octet_length(cpuset_mems) >= 1) AND (octet_length(cpuset_mems) <= 4096))),
    CONSTRAINT runtime_node_capacities_memory_bytes_check CHECK ((memory_bytes > 0)),
    CONSTRAINT runtime_node_capacities_revision_check CHECK ((revision > 0))
);

CREATE TABLE manager.runtime_resource_leases (
    lease_id text NOT NULL,
    slot_id text NOT NULL,
    operation_id text NOT NULL,
    claim_id text NOT NULL,
    cluster_id text NOT NULL,
    node_id text NOT NULL,
    node_uid text NOT NULL,
    node_boot_id text NOT NULL,
    cpu_millicores bigint NOT NULL,
    cpu_period_micros bigint NOT NULL,
    cpu_quota_micros bigint NOT NULL,
    cpu_shares bigint NOT NULL,
    cpu_weight bigint NOT NULL,
    cpuset_cpus text NOT NULL,
    cpuset_mems text NOT NULL,
    memory_bytes bigint NOT NULL,
    pids_limit bigint NOT NULL,
    cgroup_name text NOT NULL,
    lease_digest bytea NOT NULL,
    lease_state text NOT NULL,
    acquired_at timestamp with time zone DEFAULT now() NOT NULL,
    released_at timestamp with time zone,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT runtime_resource_leases_cgroup_name_check CHECK ((cgroup_name ~ '^s0-[0-9a-f]{64}$'::text)),
    CONSTRAINT runtime_resource_leases_check CHECK ((((lease_state = 'active'::text) AND (released_at IS NULL)) OR ((lease_state = 'released'::text) AND (released_at IS NOT NULL)))),
    CONSTRAINT runtime_resource_leases_cpu_millicores_check CHECK ((cpu_millicores > 0)),
    CONSTRAINT runtime_resource_leases_cpu_period_micros_check CHECK ((cpu_period_micros > 0)),
    CONSTRAINT runtime_resource_leases_cpu_quota_micros_check CHECK ((cpu_quota_micros > 0)),
    CONSTRAINT runtime_resource_leases_cpu_shares_check CHECK (((cpu_shares >= 2) AND (cpu_shares <= 262144))),
    CONSTRAINT runtime_resource_leases_cpu_weight_check CHECK (((cpu_weight >= 1) AND (cpu_weight <= 10000))),
    CONSTRAINT runtime_resource_leases_cpuset_cpus_check CHECK (((octet_length(cpuset_cpus) >= 1) AND (octet_length(cpuset_cpus) <= 4096))),
    CONSTRAINT runtime_resource_leases_cpuset_mems_check CHECK (((octet_length(cpuset_mems) >= 1) AND (octet_length(cpuset_mems) <= 4096))),
    CONSTRAINT runtime_resource_leases_lease_digest_check CHECK ((octet_length(lease_digest) = 32)),
    CONSTRAINT runtime_resource_leases_lease_state_check CHECK ((lease_state = ANY (ARRAY['active'::text, 'released'::text]))),
    CONSTRAINT runtime_resource_leases_memory_bytes_check CHECK ((memory_bytes > 0)),
    CONSTRAINT runtime_resource_leases_pids_limit_check CHECK ((pids_limit > 0))
);

CREATE TABLE manager.runtime_slots (
    slot_id text NOT NULL,
    cluster_id text NOT NULL,
    allocation_id text NOT NULL,
    allocation_namespace text NOT NULL,
    node_id text NOT NULL,
    node_uid text NOT NULL,
    node_boot_id text NOT NULL,
    netns_identity text NOT NULL,
    control_endpoint text NOT NULL,
    compatibility_digest text NOT NULL,
    state text NOT NULL,
    revision bigint DEFAULT 1 NOT NULL,
    runtime_ready_digest bytea,
    network_ready_digest bytea,
    storage_ready_digest bytea,
    heartbeat_expires_at timestamp with time zone NOT NULL,
    fastpath_ready_at timestamp with time zone,
    claim_operation_id text DEFAULT ''::text NOT NULL,
    claim_id text DEFAULT ''::text NOT NULL,
    claim_cluster_filter text DEFAULT ''::text NOT NULL,
    claim_ttl_milliseconds bigint DEFAULT 0 NOT NULL,
    sandbox_id text,
    filesystem_id text,
    source_generation_id text,
    writer_grant_id text,
    claim_lease_expires_at timestamp with time zone,
    claimed_at timestamp with time zone,
    launch_attempt text DEFAULT ''::text NOT NULL,
    runsc_container_id text DEFAULT ''::text NOT NULL,
    rootfs_binding_digest bytea,
    claim_network_digest bytea,
    starting_at timestamp with time zone,
    procd_instance_id text DEFAULT ''::text NOT NULL,
    command_ready_digest bytea,
    command_ready_at timestamp with time zone,
    quiescing_at timestamp with time zone,
    orphan_observation_digest bytea,
    terminal_reason text DEFAULT ''::text NOT NULL,
    terminal_proof_digest bytea,
    terminal_at timestamp with time zone,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    claim_runtime_assignment_revision text DEFAULT ''::text NOT NULL,
    claim_network_policy_digest text DEFAULT ''::text NOT NULL,
    procd_address text DEFAULT ''::text NOT NULL,
    resource_lease_id text,
    CONSTRAINT runtime_slots_claim_binding CHECK ((((state = ANY (ARRAY['registered'::text, 'fastpath_ready'::text])) AND (claim_operation_id = ''::text) AND (claim_id = ''::text) AND (claim_cluster_filter = ''::text) AND (claim_ttl_milliseconds = 0) AND (claim_runtime_assignment_revision = ''::text) AND (claim_network_policy_digest = ''::text) AND (sandbox_id IS NULL) AND (filesystem_id IS NULL) AND (source_generation_id IS NULL) AND (writer_grant_id IS NULL) AND (claim_lease_expires_at IS NULL) AND (claimed_at IS NULL)) OR ((state = 'terminal'::text) AND (claim_operation_id = ''::text) AND (claim_id = ''::text) AND (claim_cluster_filter = ''::text) AND (claim_ttl_milliseconds = 0) AND (claim_runtime_assignment_revision = ''::text) AND (claim_network_policy_digest = ''::text) AND (sandbox_id IS NULL) AND (filesystem_id IS NULL) AND (source_generation_id IS NULL) AND (writer_grant_id IS NULL) AND (claim_lease_expires_at IS NULL) AND (claimed_at IS NULL)) OR ((claim_operation_id <> ''::text) AND (claim_id <> ''::text) AND ((claim_ttl_milliseconds >= 1000) AND (claim_ttl_milliseconds <= 60000)) AND (((claim_runtime_assignment_revision <> ''::text) AND (claim_network_policy_digest <> ''::text)) OR ((claim_runtime_assignment_revision = ''::text) AND (claim_network_policy_digest = ''::text))) AND (sandbox_id IS NOT NULL) AND (filesystem_id IS NOT NULL) AND (source_generation_id IS NOT NULL) AND (claim_lease_expires_at IS NOT NULL) AND (claimed_at IS NOT NULL)) OR ((state = 'terminal'::text) AND (claim_operation_id <> ''::text) AND (claim_id <> ''::text) AND ((claim_ttl_milliseconds >= 1000) AND (claim_ttl_milliseconds <= 60000)) AND (((claim_runtime_assignment_revision <> ''::text) AND (claim_network_policy_digest <> ''::text)) OR ((claim_runtime_assignment_revision = ''::text) AND (claim_network_policy_digest = ''::text))) AND (sandbox_id IS NOT NULL) AND (filesystem_id IS NULL) AND (source_generation_id IS NULL) AND (writer_grant_id IS NULL) AND (claim_lease_expires_at IS NOT NULL) AND (claimed_at IS NOT NULL)))),
    CONSTRAINT runtime_slots_claim_network_digest_check CHECK (((claim_network_digest IS NULL) OR (octet_length(claim_network_digest) = 32))),
    CONSTRAINT runtime_slots_claim_network_policy_digest CHECK (((claim_network_policy_digest = ''::text) OR (claim_network_policy_digest ~ '^sha256:[0-9a-f]{64}$'::text))),
    CONSTRAINT runtime_slots_claim_runtime_revision CHECK (((claim_runtime_assignment_revision = ''::text) OR (claim_runtime_assignment_revision ~ '^[0-9a-f]{64}$'::text))),
    CONSTRAINT runtime_slots_claim_ttl_milliseconds_check CHECK (((claim_ttl_milliseconds >= 0) AND (claim_ttl_milliseconds <= 60000))),
    CONSTRAINT runtime_slots_command_ready_digest_check CHECK (((command_ready_digest IS NULL) OR (octet_length(command_ready_digest) = 32))),
    CONSTRAINT runtime_slots_fastpath_proofs CHECK (((state <> 'fastpath_ready'::text) OR ((runtime_ready_digest IS NOT NULL) AND (network_ready_digest IS NOT NULL) AND (storage_ready_digest IS NOT NULL)))),
    CONSTRAINT runtime_slots_network_ready_digest_check CHECK (((network_ready_digest IS NULL) OR (octet_length(network_ready_digest) = 32))),
    CONSTRAINT runtime_slots_orphan_observation_digest_check CHECK (((orphan_observation_digest IS NULL) OR (octet_length(orphan_observation_digest) = 32))),
    CONSTRAINT runtime_slots_resource_lease_claim CHECK (((resource_lease_id IS NULL) OR (claim_operation_id <> ''::text))),
    CONSTRAINT runtime_slots_revision_check CHECK ((revision > 0)),
    CONSTRAINT runtime_slots_rootfs_binding_digest_check CHECK (((rootfs_binding_digest IS NULL) OR (octet_length(rootfs_binding_digest) = 32))),
    CONSTRAINT runtime_slots_runtime_ready_digest_check CHECK (((runtime_ready_digest IS NULL) OR (octet_length(runtime_ready_digest) = 32))),
    CONSTRAINT runtime_slots_state_check CHECK ((state = ANY (ARRAY['registered'::text, 'fastpath_ready'::text, 'claiming'::text, 'starting'::text, 'active'::text, 'quiescing'::text, 'orphaned'::text, 'terminal'::text]))),
    CONSTRAINT runtime_slots_storage_ready_digest_check CHECK (((storage_ready_digest IS NULL) OR (octet_length(storage_ready_digest) = 32))),
    CONSTRAINT runtime_slots_terminal_proof_digest_check CHECK (((terminal_proof_digest IS NULL) OR (octet_length(terminal_proof_digest) = 32)))
);

CREATE TABLE manager.sandbox_deletion_webhook_outbox (
    event_id text NOT NULL,
    sandbox_id text NOT NULL,
    team_id text NOT NULL,
    target_url text NOT NULL,
    payload bytea NOT NULL,
    signature text DEFAULT ''::text NOT NULL,
    attempts integer DEFAULT 0 NOT NULL,
    next_attempt_at timestamp with time zone DEFAULT now() NOT NULL,
    expires_at timestamp with time zone NOT NULL,
    claimed_by text DEFAULT ''::text NOT NULL,
    claimed_until timestamp with time zone,
    delivered_at timestamp with time zone,
    terminal_at timestamp with time zone,
    terminal_reason text DEFAULT ''::text NOT NULL,
    last_status integer,
    last_error text DEFAULT ''::text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT sandbox_deletion_webhook_outbox_attempts_check CHECK ((attempts >= 0)),
    CONSTRAINT sandbox_deletion_webhook_outbox_check CHECK (((delivered_at IS NULL) OR (terminal_at IS NULL)))
);

CREATE TABLE manager.sandbox_lifecycle_txns (
    txn_id text NOT NULL,
    sandbox_id text NOT NULL,
    kind text NOT NULL,
    phase text NOT NULL,
    source text DEFAULT 'manual'::text NOT NULL,
    cancelable boolean DEFAULT false NOT NULL,
    epoch bigint NOT NULL,
    from_generation bigint DEFAULT 0 NOT NULL,
    to_generation bigint DEFAULT 0 NOT NULL,
    from_runtime_namespace text DEFAULT ''::text NOT NULL,
    from_runtime_id text DEFAULT ''::text NOT NULL,
    to_runtime_namespace text DEFAULT ''::text NOT NULL,
    to_runtime_id text DEFAULT ''::text NOT NULL,
    expected_generation_id text DEFAULT ''::text NOT NULL,
    prepared_generation_id text DEFAULT ''::text NOT NULL,
    error text DEFAULT ''::text NOT NULL,
    cancel_reason text DEFAULT ''::text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    cancel_requested_at timestamp with time zone,
    committed_at timestamp with time zone,
    aborted_at timestamp with time zone,
    target_sandbox_id text DEFAULT ''::text NOT NULL,
    target_generation_id text DEFAULT ''::text NOT NULL,
    target_record_digest bytea DEFAULT '\x'::bytea NOT NULL,
    source_base_artifact_digest text DEFAULT ''::text NOT NULL,
    target_base_artifact_digest text DEFAULT ''::text NOT NULL,
    rollback_expires_at timestamp with time zone,
    worker_cluster_id text DEFAULT ''::text NOT NULL,
    worker_node_id text DEFAULT ''::text NOT NULL,
    worker_node_uid text DEFAULT ''::text NOT NULL,
    worker_proof_digest bytea DEFAULT '\x'::bytea NOT NULL,
    worker_acknowledged_at timestamp with time zone,
    recovery_attempts integer DEFAULT 0 NOT NULL,
    recovery_next_attempt_at timestamp with time zone DEFAULT now() NOT NULL,
    recovery_claimed_by text DEFAULT ''::text NOT NULL,
    recovery_claim_token text DEFAULT ''::text NOT NULL,
    recovery_claimed_until timestamp with time zone,
    recovery_last_error text DEFAULT ''::text NOT NULL,
    CONSTRAINT sandbox_lifecycle_txns_recovery_attempts_check CHECK (((recovery_attempts >= 0) AND (recovery_attempts <= 1000000))),
    CONSTRAINT sandbox_lifecycle_txns_recovery_claim_check CHECK ((((recovery_claimed_by = ''::text) AND (recovery_claim_token = ''::text) AND (recovery_claimed_until IS NULL)) OR ((octet_length(recovery_claimed_by) >= 1) AND (octet_length(recovery_claimed_by) <= 256) AND (octet_length(recovery_claim_token) = 36) AND (recovery_claimed_until IS NOT NULL)))),
    CONSTRAINT sandbox_lifecycle_txns_recovery_last_error_check CHECK ((octet_length(recovery_last_error) <= 4096)),
    CONSTRAINT sandbox_lifecycle_txns_rebase_identity_check CHECK (((kind <> 'rebase'::text) OR ((source = 'manual'::text) AND (NOT cancelable) AND (from_generation = to_generation) AND (from_runtime_namespace = ''::text) AND (from_runtime_id = ''::text) AND (to_runtime_namespace = ''::text) AND (to_runtime_id = ''::text) AND (target_sandbox_id = ''::text) AND (octet_length(target_record_digest) = 0) AND (target_generation_id <> ''::text) AND (source_base_artifact_digest <> ''::text) AND (target_base_artifact_digest <> ''::text) AND (source_base_artifact_digest <> target_base_artifact_digest) AND (expected_generation_id <> ''::text) AND (rollback_expires_at IS NOT NULL) AND (((worker_cluster_id = ''::text) AND (worker_node_id = ''::text) AND (worker_node_uid = ''::text) AND (phase = ANY (ARRAY['committed'::text, 'aborted'::text]))) OR ((worker_cluster_id <> ''::text) AND (worker_node_id <> ''::text) AND (worker_node_uid <> ''::text))) AND (octet_length(worker_proof_digest) = ANY (ARRAY[0, 32])) AND ((worker_acknowledged_at IS NULL) OR ((phase = ANY (ARRAY['committed'::text, 'aborted'::text])) AND (octet_length(worker_proof_digest) = 32))))))
);

CREATE TABLE manager.sandbox_metering_projection_queue (
    sandbox_id text NOT NULL,
    revision bigint NOT NULL,
    attempts integer DEFAULT 0 NOT NULL,
    available_at timestamp with time zone DEFAULT now() NOT NULL,
    last_error text DEFAULT ''::text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT sandbox_metering_projection_queue_attempts_check CHECK ((attempts >= 0))
);

CREATE SEQUENCE manager.sandbox_metering_revision_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

CREATE TABLE manager.sandbox_network_mutation_bindings (
    operation_id text NOT NULL,
    sandbox_id text NOT NULL,
    team_id text NOT NULL,
    ref text NOT NULL,
    source_ref text NOT NULL,
    source_id bigint NOT NULL,
    projection jsonb NOT NULL,
    cache_policy jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT sandbox_network_mutation_bindings_identity_bounds CHECK (((operation_id <> ''::text) AND (octet_length(operation_id) <= 512) AND (sandbox_id <> ''::text) AND (octet_length(sandbox_id) <= 512) AND (team_id <> ''::text) AND (octet_length(team_id) <= 512) AND (ref <> ''::text) AND (octet_length(ref) <= 512) AND (source_ref <> ''::text) AND (octet_length(source_ref) <= 512))),
    CONSTRAINT sandbox_network_mutation_bindings_payload_bounds CHECK (((jsonb_typeof(projection) = 'object'::text) AND (jsonb_typeof(cache_policy) = ANY (ARRAY['object'::text, 'null'::text])) AND (octet_length((projection)::text) <= 65536) AND (octet_length((cache_policy)::text) <= 4096)))
);

CREATE TABLE manager.sandbox_network_mutations (
    sandbox_id text NOT NULL,
    operation_id text NOT NULL,
    slot_id text NOT NULL,
    slot_revision bigint NOT NULL,
    team_id text NOT NULL,
    cluster_id text NOT NULL,
    allocation_id text NOT NULL,
    allocation_namespace text NOT NULL,
    node_id text NOT NULL,
    node_uid text NOT NULL,
    node_boot_id text NOT NULL,
    netns_identity text NOT NULL,
    claim_id text NOT NULL,
    current_policy_digest text NOT NULL,
    desired_policy text NOT NULL,
    desired_policy_digest text NOT NULL,
    request_policy jsonb NOT NULL,
    phase text DEFAULT 'pending'::text NOT NULL,
    applied_policy_token jsonb,
    applied_token_digest bytea DEFAULT '\x'::bytea NOT NULL,
    cancellation_reason text DEFAULT ''::text NOT NULL,
    applied_at timestamp with time zone,
    canceled_at timestamp with time zone,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    credential_binding_digest text DEFAULT 'sha256:4f53cda18c2baa0c0354bb5f9a3ecbe5ed12ab4d8e11ba873c2f11161202b945'::text NOT NULL,
    CONSTRAINT sandbox_network_mutations_credential_binding_digest_check CHECK ((credential_binding_digest ~ '^sha256:[0-9a-f]{64}$'::text)),
    CONSTRAINT sandbox_network_mutations_identity_bounds CHECK (((slot_revision >= 0) AND (operation_id <> ''::text) AND (octet_length(operation_id) <= 512) AND (team_id <> ''::text) AND (octet_length(team_id) <= 512) AND (cluster_id <> ''::text) AND (octet_length(cluster_id) <= 512) AND (allocation_id <> ''::text) AND (octet_length(allocation_id) <= 512) AND (allocation_namespace <> ''::text) AND (octet_length(allocation_namespace) <= 512) AND (node_id <> ''::text) AND (octet_length(node_id) <= 512) AND (node_uid <> ''::text) AND (octet_length(node_uid) <= 512) AND (node_boot_id <> ''::text) AND (octet_length(node_boot_id) <= 512) AND (netns_identity <> ''::text) AND (octet_length(netns_identity) <= 512) AND (claim_id <> ''::text) AND (octet_length(claim_id) <= 512))),
    CONSTRAINT sandbox_network_mutations_payload_bounds CHECK ((((octet_length(desired_policy) >= 1) AND (octet_length(desired_policy) <= 65536)) AND ((octet_length((request_policy)::text) >= 2) AND (octet_length((request_policy)::text) <= 65536)) AND (jsonb_typeof(request_policy) = 'object'::text) AND (octet_length(cancellation_reason) <= 1024))),
    CONSTRAINT sandbox_network_mutations_phase CHECK ((phase = ANY (ARRAY['pending'::text, 'applied'::text, 'canceled'::text]))),
    CONSTRAINT sandbox_network_mutations_policy_digests CHECK (((current_policy_digest ~ '^sha256:[0-9a-f]{64}$'::text) AND (desired_policy_digest ~ '^sha256:[0-9a-f]{64}$'::text))),
    CONSTRAINT sandbox_network_mutations_terminal_state CHECK ((((phase = 'pending'::text) AND (applied_policy_token IS NULL) AND (octet_length(applied_token_digest) = 0) AND (cancellation_reason = ''::text) AND (applied_at IS NULL) AND (canceled_at IS NULL)) OR ((phase = 'applied'::text) AND (applied_policy_token IS NOT NULL) AND (jsonb_typeof(applied_policy_token) = 'object'::text) AND (octet_length(applied_token_digest) = 32) AND (cancellation_reason = ''::text) AND (applied_at IS NOT NULL) AND (canceled_at IS NULL)) OR ((phase = 'canceled'::text) AND (applied_policy_token IS NULL) AND (octet_length(applied_token_digest) = 0) AND (cancellation_reason <> ''::text) AND (applied_at IS NULL) AND (canceled_at IS NOT NULL))))
);

CREATE TABLE manager.sandbox_rootfs_bindings (
    sandbox_id text NOT NULL,
    filesystem_id text NOT NULL,
    team_id text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);

CREATE TABLE manager.sandbox_runtime_claims (
    sandbox_id text NOT NULL,
    operation_id text NOT NULL,
    phase text NOT NULL,
    lease_expires_at timestamp with time zone,
    last_error text DEFAULT ''::text NOT NULL,
    completed_at timestamp with time zone,
    cleanup_started_at timestamp with time zone,
    cleaned_at timestamp with time zone,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    credential_binding_digest text DEFAULT 'sha256:4f53cda18c2baa0c0354bb5f9a3ecbe5ed12ab4d8e11ba873c2f11161202b945'::text NOT NULL,
    CONSTRAINT sandbox_runtime_claims_credential_binding_digest_check CHECK ((credential_binding_digest ~ '^sha256:[0-9a-f]{64}$'::text)),
    CONSTRAINT sandbox_runtime_claims_lease_check CHECK ((((phase = 'claiming'::text) AND (lease_expires_at IS NOT NULL)) OR ((phase <> 'claiming'::text) AND (lease_expires_at IS NULL)))),
    CONSTRAINT sandbox_runtime_claims_operation_id_check CHECK (((operation_id <> ''::text) AND (char_length(operation_id) <= 512))),
    CONSTRAINT sandbox_runtime_claims_phase_check CHECK ((phase = ANY (ARRAY['claiming'::text, 'ready'::text, 'cleanup_pending'::text, 'cleaned'::text])))
);

CREATE TABLE manager.sandboxes (
    sandbox_id text NOT NULL,
    team_id text NOT NULL,
    user_id text DEFAULT ''::text NOT NULL,
    template_id text NOT NULL,
    template_name text NOT NULL,
    template_namespace text NOT NULL,
    cluster_id text DEFAULT ''::text NOT NULL,
    desired_state text NOT NULL,
    config jsonb DEFAULT '{}'::jsonb NOT NULL,
    template_spec jsonb DEFAULT '{}'::jsonb NOT NULL,
    runtime_id text DEFAULT ''::text NOT NULL,
    runtime_namespace text DEFAULT ''::text NOT NULL,
    runtime_generation bigint DEFAULT 0 NOT NULL,
    claimed_at timestamp with time zone,
    expires_at timestamp with time zone,
    hard_expires_at timestamp with time zone,
    deleted_at timestamp with time zone,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    lifecycle_epoch bigint DEFAULT 0 NOT NULL,
    owner_kind text DEFAULT ''::text NOT NULL,
    hot_claim_completed_at timestamp with time zone,
    resource_millicpu bigint NOT NULL,
    resource_memory_mib bigint NOT NULL,
    CONSTRAINT sandboxes_desired_state_check CHECK ((desired_state = ANY (ARRAY['active'::text, 'paused'::text, 'terminating'::text, 'deleted'::text]))),
    CONSTRAINT sandboxes_resource_memory_mib_check CHECK ((resource_memory_mib > 0)),
    CONSTRAINT sandboxes_resource_millicpu_check CHECK ((resource_millicpu > 0))
);

ALTER TABLE ONLY manager.rootfs_base_artifact_objects
    ADD CONSTRAINT rootfs_base_artifact_objects_pkey PRIMARY KEY (artifact_digest, object_key);

ALTER TABLE ONLY manager.rootfs_base_artifacts
    ADD CONSTRAINT rootfs_base_artifacts_pkey PRIMARY KEY (artifact_digest);

ALTER TABLE ONLY manager.rootfs_base_artifacts
    ADD CONSTRAINT rootfs_base_artifacts_source_oci_digest_artifact_digest_for_key UNIQUE (source_oci_digest, artifact_digest, format_generation);

ALTER TABLE ONLY manager.rootfs_composite_backlog_policy
    ADD CONSTRAINT rootfs_composite_backlog_policy_pkey PRIMARY KEY (singleton);

ALTER TABLE ONLY manager.rootfs_filesystems
    ADD CONSTRAINT rootfs_filesystems_pkey PRIMARY KEY (filesystem_id);

ALTER TABLE ONLY manager.rootfs_generation_materialization_objects
    ADD CONSTRAINT rootfs_generation_materialization_objects_pkey PRIMARY KEY (generation_id, locator_version, object_key);

ALTER TABLE ONLY manager.rootfs_generations
    ADD CONSTRAINT rootfs_generations_filesystem_id_writer_epoch_key UNIQUE (filesystem_id, writer_epoch);

ALTER TABLE ONLY manager.rootfs_generations
    ADD CONSTRAINT rootfs_generations_pkey PRIMARY KEY (generation_id);

ALTER TABLE ONLY manager.rootfs_head_rollbacks
    ADD CONSTRAINT rootfs_head_rollbacks_pkey PRIMARY KEY (operation_id);

ALTER TABLE ONLY manager.rootfs_import_operation_objects
    ADD CONSTRAINT rootfs_import_operation_objects_pkey PRIMARY KEY (operation_id, object_key);

ALTER TABLE ONLY manager.rootfs_import_operations
    ADD CONSTRAINT rootfs_import_operations_pkey PRIMARY KEY (operation_id);

ALTER TABLE ONLY manager.rootfs_materialization_batch_objects
    ADD CONSTRAINT rootfs_materialization_batch_objects_pkey PRIMARY KEY (batch_id, object_key);

ALTER TABLE ONLY manager.rootfs_materialization_batches
    ADD CONSTRAINT rootfs_materialization_batches_pkey PRIMARY KEY (batch_id);

ALTER TABLE ONLY manager.rootfs_materialization_member_objects
    ADD CONSTRAINT rootfs_materialization_member_objects_pkey PRIMARY KEY (batch_id, ordinal, object_key);

ALTER TABLE ONLY manager.rootfs_materialization_members
    ADD CONSTRAINT rootfs_materialization_members_batch_id_generation_id_key UNIQUE (batch_id, generation_id);

ALTER TABLE ONLY manager.rootfs_materialization_members
    ADD CONSTRAINT rootfs_materialization_members_pkey PRIMARY KEY (batch_id, ordinal);

ALTER TABLE ONLY manager.rootfs_materialization_objects
    ADD CONSTRAINT rootfs_materialization_objects_pkey PRIMARY KEY (object_key);

ALTER TABLE ONLY manager.rootfs_object_deletions
    ADD CONSTRAINT rootfs_object_deletions_pkey PRIMARY KEY (object_key);

ALTER TABLE ONLY manager.rootfs_running_forks
    ADD CONSTRAINT rootfs_running_forks_checkpoint_generation_id_key UNIQUE (checkpoint_generation_id);

ALTER TABLE ONLY manager.rootfs_running_forks
    ADD CONSTRAINT rootfs_running_forks_pkey PRIMARY KEY (operation_id);

ALTER TABLE ONLY manager.rootfs_running_forks
    ADD CONSTRAINT rootfs_running_forks_target_filesystem_id_key UNIQUE (target_filesystem_id);

ALTER TABLE ONLY manager.rootfs_running_forks
    ADD CONSTRAINT rootfs_running_forks_target_sandbox_id_key UNIQUE (target_sandbox_id);

ALTER TABLE ONLY manager.rootfs_running_template_captures
    ADD CONSTRAINT rootfs_running_template_captures_checkpoint_generation_id_key UNIQUE (checkpoint_generation_id);

ALTER TABLE ONLY manager.rootfs_running_template_captures
    ADD CONSTRAINT rootfs_running_template_captures_pkey PRIMARY KEY (operation_id);

ALTER TABLE ONLY manager.rootfs_running_template_captures
    ADD CONSTRAINT rootfs_running_template_captures_snapshot_id_key UNIQUE (snapshot_id);

ALTER TABLE ONLY manager.rootfs_running_template_captures
    ADD CONSTRAINT rootfs_running_template_captures_target_filesystem_id_key UNIQUE (target_filesystem_id);

ALTER TABLE ONLY manager.rootfs_snapshots
    ADD CONSTRAINT rootfs_snapshots_pkey PRIMARY KEY (snapshot_id);

ALTER TABLE ONLY manager.rootfs_writer_grants
    ADD CONSTRAINT rootfs_writer_grants_filesystem_id_writer_epoch_key UNIQUE (filesystem_id, writer_epoch);

ALTER TABLE ONLY manager.rootfs_writer_grants
    ADD CONSTRAINT rootfs_writer_grants_issue_operation_id_key UNIQUE (issue_operation_id);

ALTER TABLE ONLY manager.rootfs_writer_grants
    ADD CONSTRAINT rootfs_writer_grants_pkey PRIMARY KEY (grant_id);

ALTER TABLE ONLY manager.rootfs_writer_terminal_proofs
    ADD CONSTRAINT rootfs_writer_terminal_proofs_pkey PRIMARY KEY (grant_id);

ALTER TABLE ONLY manager.runtime_node_capacities
    ADD CONSTRAINT runtime_node_capacities_pkey PRIMARY KEY (cluster_id, node_id, node_uid, node_boot_id);

ALTER TABLE ONLY manager.runtime_resource_leases
    ADD CONSTRAINT runtime_resource_leases_cgroup_name_key UNIQUE (cgroup_name);

ALTER TABLE ONLY manager.runtime_resource_leases
    ADD CONSTRAINT runtime_resource_leases_claim_id_key UNIQUE (claim_id);

ALTER TABLE ONLY manager.runtime_resource_leases
    ADD CONSTRAINT runtime_resource_leases_operation_id_key UNIQUE (operation_id);

ALTER TABLE ONLY manager.runtime_resource_leases
    ADD CONSTRAINT runtime_resource_leases_pkey PRIMARY KEY (lease_id);

ALTER TABLE ONLY manager.runtime_resource_leases
    ADD CONSTRAINT runtime_resource_leases_slot_id_key UNIQUE (slot_id);

ALTER TABLE ONLY manager.runtime_slots
    ADD CONSTRAINT runtime_slots_cluster_id_allocation_id_key UNIQUE (cluster_id, allocation_id);

ALTER TABLE ONLY manager.runtime_slots
    ADD CONSTRAINT runtime_slots_pkey PRIMARY KEY (slot_id);

ALTER TABLE ONLY manager.runtime_slots
    ADD CONSTRAINT runtime_slots_resource_lease_id_key UNIQUE (resource_lease_id);

ALTER TABLE ONLY manager.sandbox_deletion_webhook_outbox
    ADD CONSTRAINT sandbox_deletion_webhook_outbox_pkey PRIMARY KEY (event_id);

ALTER TABLE ONLY manager.sandbox_lifecycle_txns
    ADD CONSTRAINT sandbox_lifecycle_txns_pkey PRIMARY KEY (txn_id);

ALTER TABLE ONLY manager.sandbox_metering_projection_queue
    ADD CONSTRAINT sandbox_metering_projection_queue_pkey PRIMARY KEY (sandbox_id);

ALTER TABLE ONLY manager.sandbox_network_mutation_bindings
    ADD CONSTRAINT sandbox_network_mutation_bindings_pkey PRIMARY KEY (operation_id, ref);

ALTER TABLE ONLY manager.sandbox_network_mutation_bindings
    ADD CONSTRAINT sandbox_network_mutation_bindings_sandbox_id_ref_key UNIQUE (sandbox_id, ref);

ALTER TABLE ONLY manager.sandbox_network_mutations
    ADD CONSTRAINT sandbox_network_mutations_operation_id_key UNIQUE (operation_id);

ALTER TABLE ONLY manager.sandbox_network_mutations
    ADD CONSTRAINT sandbox_network_mutations_pkey PRIMARY KEY (sandbox_id);

ALTER TABLE ONLY manager.sandbox_rootfs_bindings
    ADD CONSTRAINT sandbox_rootfs_bindings_pkey PRIMARY KEY (sandbox_id);

ALTER TABLE ONLY manager.sandbox_runtime_claims
    ADD CONSTRAINT sandbox_runtime_claims_operation_id_key UNIQUE (operation_id);

ALTER TABLE ONLY manager.sandbox_runtime_claims
    ADD CONSTRAINT sandbox_runtime_claims_pkey PRIMARY KEY (sandbox_id);

ALTER TABLE ONLY manager.sandboxes
    ADD CONSTRAINT sandboxes_pkey PRIMARY KEY (sandbox_id);

CREATE INDEX idx_rootfs_base_artifact_objects_object ON manager.rootfs_base_artifact_objects USING btree (object_key, artifact_digest);

CREATE INDEX idx_rootfs_base_artifacts_source_platform_ready ON manager.rootfs_base_artifacts USING btree (source_oci_digest, oci_os, oci_architecture, oci_variant, logical_size_bytes, format_generation, procd_protocol, procd_digest, created_at DESC) WHERE ((state = 'ready'::text) AND (oci_os IS NOT NULL) AND (oci_architecture IS NOT NULL) AND (oci_variant IS NOT NULL) AND (logical_size_bytes IS NOT NULL) AND (procd_protocol IS NOT NULL) AND (procd_digest IS NOT NULL));

CREATE INDEX idx_rootfs_base_artifacts_source_ready ON manager.rootfs_base_artifacts USING btree (source_oci_digest, format_generation DESC) WHERE (state = 'ready'::text);

CREATE INDEX idx_rootfs_filesystems_generation_head ON manager.rootfs_filesystems USING btree (head_generation_id) WHERE (head_generation_id IS NOT NULL);

CREATE INDEX idx_rootfs_filesystems_source ON manager.rootfs_filesystems USING btree (source_filesystem_id) WHERE (source_filesystem_id IS NOT NULL);

CREATE INDEX idx_rootfs_filesystems_team_updated ON manager.rootfs_filesystems USING btree (team_id, updated_at DESC);

CREATE INDEX idx_rootfs_generation_materialization_objects_object ON manager.rootfs_generation_materialization_objects USING btree (object_key, generation_id, locator_version);

CREATE INDEX idx_rootfs_generations_filesystem_created ON manager.rootfs_generations USING btree (filesystem_id, created_at DESC);

CREATE INDEX idx_rootfs_generations_parent ON manager.rootfs_generations USING btree (parent_generation_id) WHERE (parent_generation_id IS NOT NULL);

CREATE INDEX idx_rootfs_head_rollbacks_filesystem_available ON manager.rootfs_head_rollbacks USING btree (filesystem_id, created_at DESC) WHERE (state = 'available'::text);

CREATE INDEX idx_rootfs_import_operation_objects_state ON manager.rootfs_import_operation_objects USING btree (upload_state, updated_at, operation_id);

CREATE INDEX idx_rootfs_import_operations_terminal ON manager.rootfs_import_operations USING btree (updated_at, operation_id) WHERE (state = ANY (ARRAY['ready'::text, 'abandoned'::text]));

CREATE INDEX idx_rootfs_import_operations_work ON manager.rootfs_import_operations USING btree (state, lease_expires_at, created_at, operation_id) WHERE (state = ANY (ARRAY['pending'::text, 'building'::text]));

CREATE INDEX idx_rootfs_materialization_batch_objects_state ON manager.rootfs_materialization_batch_objects USING btree (upload_state, updated_at, batch_id);

CREATE INDEX idx_rootfs_materialization_batches_state ON manager.rootfs_materialization_batches USING btree (state, updated_at, batch_id);

CREATE UNIQUE INDEX idx_rootfs_materialization_members_uploading_generation ON manager.rootfs_materialization_members USING btree (generation_id) WHERE (state = 'uploading'::text);

CREATE INDEX idx_rootfs_materialization_objects_missing ON manager.rootfs_materialization_objects USING btree (missing_at) WHERE (missing_at IS NOT NULL);

CREATE INDEX idx_rootfs_object_deletions_claim ON manager.rootfs_object_deletions USING btree (claimed_until) WHERE ((claimed_until IS NOT NULL) AND (dead_lettered_at IS NULL));

CREATE INDEX idx_rootfs_object_deletions_dead_lettered ON manager.rootfs_object_deletions USING btree (dead_lettered_at) WHERE (dead_lettered_at IS NOT NULL);

CREATE INDEX idx_rootfs_object_deletions_due ON manager.rootfs_object_deletions USING btree (next_attempt_at, updated_at) WHERE (dead_lettered_at IS NULL);

CREATE INDEX idx_rootfs_object_deletions_updated ON manager.rootfs_object_deletions USING btree (updated_at);

CREATE INDEX idx_rootfs_running_forks_source ON manager.rootfs_running_forks USING btree (source_filesystem_id, created_at DESC);

CREATE INDEX idx_rootfs_running_template_captures_source_pending ON manager.rootfs_running_template_captures USING btree (source_sandbox_id, updated_at) WHERE (state = 'pending'::text);

CREATE INDEX idx_rootfs_snapshots_filesystem ON manager.rootfs_snapshots USING btree (filesystem_id) WHERE (filesystem_id IS NOT NULL);

CREATE INDEX idx_rootfs_snapshots_generation_head ON manager.rootfs_snapshots USING btree (head_generation_id) WHERE (head_generation_id IS NOT NULL);

CREATE INDEX idx_rootfs_snapshots_team_created ON manager.rootfs_snapshots USING btree (team_id, created_at DESC);

CREATE INDEX idx_rootfs_writer_grants_expired_recovery ON manager.rootfs_writer_grants USING btree (lease_expires_at) WHERE ((state = ANY (ARRAY['consumed'::text, 'retiring'::text])) AND (runtime_namespace <> ''::text) AND (runtime_id <> ''::text) AND (runtime_incarnation_id <> ''::text) AND (runtime_node_name <> ''::text) AND (runtime_gate_parent <> ''::text) AND (runtime_generation <> ''::text));

CREATE INDEX idx_rootfs_writer_grants_lease_expiry ON manager.rootfs_writer_grants USING btree (state, lease_expires_at) WHERE (state = ANY (ARRAY['consumed'::text, 'retiring'::text]));

CREATE UNIQUE INDEX idx_rootfs_writer_grants_live_claim ON manager.rootfs_writer_grants USING btree (claim_id) WHERE (state = ANY (ARRAY['issued'::text, 'consumed'::text, 'retiring'::text]));

CREATE UNIQUE INDEX idx_rootfs_writer_grants_live_filesystem ON manager.rootfs_writer_grants USING btree (filesystem_id) WHERE (state = ANY (ARRAY['issued'::text, 'consumed'::text, 'retiring'::text]));

CREATE UNIQUE INDEX idx_rootfs_writer_grants_live_slot ON manager.rootfs_writer_grants USING btree (node_uid, node_boot_id, slot_id) WHERE (state = ANY (ARRAY['issued'::text, 'consumed'::text, 'retiring'::text]));

CREATE UNIQUE INDEX idx_rootfs_writer_grants_retire_operation ON manager.rootfs_writer_grants USING btree (retire_operation_id) WHERE (retire_operation_id <> ''::text);

CREATE UNIQUE INDEX idx_rootfs_writer_grants_runtime_gate_parent ON manager.rootfs_writer_grants USING btree (runtime_gate_parent) WHERE (runtime_gate_parent <> ''::text);

CREATE INDEX idx_runtime_node_capacities_live ON manager.runtime_node_capacities USING btree (cluster_id, heartbeat_expires_at, node_id, node_uid, node_boot_id);

CREATE INDEX idx_runtime_resource_leases_capacity ON manager.runtime_resource_leases USING btree (cluster_id, node_id, node_uid, node_boot_id, lease_state);

CREATE INDEX idx_runtime_slots_claim_expiry ON manager.runtime_slots USING btree (claim_lease_expires_at, slot_id) WHERE (state = 'claiming'::text);

CREATE UNIQUE INDEX idx_runtime_slots_claim_operation ON manager.runtime_slots USING btree (claim_operation_id) WHERE (claim_operation_id <> ''::text);

CREATE INDEX idx_runtime_slots_fastpath_selection ON manager.runtime_slots USING btree (compatibility_digest, cluster_id, fastpath_ready_at, slot_id) WHERE (state = 'fastpath_ready'::text);

CREATE UNIQUE INDEX idx_runtime_slots_live_claim ON manager.runtime_slots USING btree (claim_id) WHERE ((claim_id <> ''::text) AND (state <> 'terminal'::text));

CREATE UNIQUE INDEX idx_runtime_slots_live_sandbox ON manager.runtime_slots USING btree (sandbox_id) WHERE ((sandbox_id IS NOT NULL) AND (state <> 'terminal'::text));

CREATE INDEX idx_runtime_slots_reconcile ON manager.runtime_slots USING btree (state, heartbeat_expires_at) WHERE (state <> 'terminal'::text);

CREATE UNIQUE INDEX idx_runtime_slots_writer_grant ON manager.runtime_slots USING btree (writer_grant_id) WHERE (writer_grant_id IS NOT NULL);

CREATE INDEX idx_sandbox_deletion_webhook_outbox_claim ON manager.sandbox_deletion_webhook_outbox USING btree (claimed_until) WHERE ((claimed_until IS NOT NULL) AND (delivered_at IS NULL) AND (terminal_at IS NULL));

CREATE INDEX idx_sandbox_deletion_webhook_outbox_delivered ON manager.sandbox_deletion_webhook_outbox USING btree (delivered_at) WHERE (delivered_at IS NOT NULL);

CREATE INDEX idx_sandbox_deletion_webhook_outbox_due ON manager.sandbox_deletion_webhook_outbox USING btree (next_attempt_at, created_at) WHERE ((delivered_at IS NULL) AND (terminal_at IS NULL));

CREATE INDEX idx_sandbox_deletion_webhook_outbox_terminal ON manager.sandbox_deletion_webhook_outbox USING btree (terminal_at) WHERE (terminal_at IS NOT NULL);

CREATE UNIQUE INDEX idx_sandbox_lifecycle_txns_active ON manager.sandbox_lifecycle_txns USING btree (sandbox_id) WHERE (phase = ANY (ARRAY['preparing'::text, 'barriered'::text, 'publishing'::text, 'committing'::text]));

CREATE INDEX idx_sandbox_lifecycle_txns_active_target ON manager.sandbox_lifecycle_txns USING btree (target_sandbox_id) WHERE ((target_sandbox_id <> ''::text) AND (phase = ANY (ARRAY['preparing'::text, 'barriered'::text, 'publishing'::text, 'committing'::text])));

CREATE INDEX idx_sandbox_lifecycle_txns_kind_phase_updated ON manager.sandbox_lifecycle_txns USING btree (kind, phase, updated_at);

CREATE INDEX idx_sandbox_lifecycle_txns_recovery_due ON manager.sandbox_lifecycle_txns USING btree (recovery_next_attempt_at, recovery_claimed_until, sandbox_id, epoch) WHERE ((kind = 'pause'::text) AND (source = ANY (ARRAY['crash'::text, 'health'::text, 'lost'::text])));

CREATE INDEX idx_sandbox_metering_projection_queue_due ON manager.sandbox_metering_projection_queue USING btree (available_at, revision, sandbox_id);

CREATE INDEX idx_sandbox_network_mutation_bindings_source ON manager.sandbox_network_mutation_bindings USING btree (source_id);

CREATE INDEX idx_sandbox_network_mutations_pending ON manager.sandbox_network_mutations USING btree (updated_at, sandbox_id) WHERE (phase = 'pending'::text);

CREATE INDEX idx_sandbox_rootfs_bindings_filesystem ON manager.sandbox_rootfs_bindings USING btree (filesystem_id);

CREATE INDEX idx_sandbox_rootfs_bindings_team_updated ON manager.sandbox_rootfs_bindings USING btree (team_id, updated_at DESC);

CREATE INDEX idx_sandbox_runtime_claims_cleanup ON manager.sandbox_runtime_claims USING btree (phase, lease_expires_at, updated_at, sandbox_id) WHERE (phase = ANY (ARRAY['claiming'::text, 'cleanup_pending'::text]));

CREATE INDEX idx_sandboxes_hard_expiry ON manager.sandboxes USING btree (hard_expires_at, sandbox_id) WHERE ((deleted_at IS NULL) AND (desired_state = ANY (ARRAY['active'::text, 'paused'::text])) AND (hard_expires_at IS NOT NULL));

CREATE INDEX idx_sandboxes_runtime ON manager.sandboxes USING btree (runtime_namespace, runtime_id) WHERE (runtime_id <> ''::text);

CREATE INDEX idx_sandboxes_soft_expiry ON manager.sandboxes USING btree (expires_at, sandbox_id) WHERE ((deleted_at IS NULL) AND (desired_state = 'active'::text) AND (expires_at IS NOT NULL));

CREATE INDEX idx_sandboxes_team_desired_state ON manager.sandboxes USING btree (team_id, desired_state);

CREATE INDEX idx_sandboxes_team_updated ON manager.sandboxes USING btree (team_id, updated_at DESC);

CREATE INDEX manager_rootfs_writer_terminal_proofs_expiry ON manager.rootfs_writer_terminal_proofs USING btree (expires_at, grant_id);

CREATE TRIGGER enqueue_nomad_sandbox_metering_from_lifecycle AFTER INSERT OR UPDATE OF phase, committed_at, aborted_at ON manager.sandbox_lifecycle_txns FOR EACH ROW EXECUTE FUNCTION manager.enqueue_nomad_sandbox_metering_from_lifecycle();

CREATE TRIGGER enqueue_nomad_sandbox_metering_from_sandbox AFTER INSERT OR UPDATE OF team_id, user_id, template_id, cluster_id, desired_state, runtime_id, runtime_namespace, runtime_generation, owner_kind, claimed_at, deleted_at, resource_millicpu, resource_memory_mib ON manager.sandboxes FOR EACH ROW EXECUTE FUNCTION manager.enqueue_nomad_sandbox_metering_from_sandbox();

CREATE TRIGGER prevent_live_rootfs_writer_delete BEFORE DELETE ON manager.rootfs_filesystems FOR EACH ROW EXECUTE FUNCTION manager.prevent_live_rootfs_writer_delete();

CREATE TRIGGER update_rootfs_filesystems_updated_at BEFORE UPDATE ON manager.rootfs_filesystems FOR EACH ROW EXECUTE FUNCTION manager.update_updated_at_column();

CREATE TRIGGER update_rootfs_object_deletions_updated_at BEFORE UPDATE ON manager.rootfs_object_deletions FOR EACH ROW EXECUTE FUNCTION manager.update_updated_at_column();

CREATE TRIGGER update_rootfs_running_template_captures_updated_at BEFORE UPDATE ON manager.rootfs_running_template_captures FOR EACH ROW EXECUTE FUNCTION manager.update_updated_at_column();

CREATE TRIGGER update_sandbox_lifecycle_txns_updated_at BEFORE UPDATE ON manager.sandbox_lifecycle_txns FOR EACH ROW EXECUTE FUNCTION manager.update_updated_at_column();

CREATE TRIGGER update_sandbox_rootfs_bindings_updated_at BEFORE UPDATE ON manager.sandbox_rootfs_bindings FOR EACH ROW EXECUTE FUNCTION manager.update_updated_at_column();

CREATE TRIGGER update_sandbox_runtime_claims_updated_at BEFORE UPDATE ON manager.sandbox_runtime_claims FOR EACH ROW EXECUTE FUNCTION manager.update_updated_at_column();

ALTER TABLE ONLY manager.rootfs_base_artifact_objects
    ADD CONSTRAINT rootfs_base_artifact_objects_artifact_digest_fkey FOREIGN KEY (artifact_digest) REFERENCES manager.rootfs_base_artifacts(artifact_digest) ON DELETE CASCADE;

ALTER TABLE ONLY manager.rootfs_base_artifact_objects
    ADD CONSTRAINT rootfs_base_artifact_objects_object_key_fkey FOREIGN KEY (object_key) REFERENCES manager.rootfs_materialization_objects(object_key) ON DELETE RESTRICT;

ALTER TABLE ONLY manager.rootfs_filesystems
    ADD CONSTRAINT rootfs_filesystems_base_artifact_fk FOREIGN KEY (base_artifact_digest) REFERENCES manager.rootfs_base_artifacts(artifact_digest) ON DELETE RESTRICT;

ALTER TABLE ONLY manager.rootfs_filesystems
    ADD CONSTRAINT rootfs_filesystems_head_generation_fk FOREIGN KEY (head_generation_id) REFERENCES manager.rootfs_generations(generation_id) ON DELETE RESTRICT;

ALTER TABLE ONLY manager.rootfs_filesystems
    ADD CONSTRAINT rootfs_filesystems_source_filesystem_id_fkey FOREIGN KEY (source_filesystem_id) REFERENCES manager.rootfs_filesystems(filesystem_id) ON DELETE RESTRICT;

ALTER TABLE ONLY manager.rootfs_generation_materialization_objects
    ADD CONSTRAINT rootfs_generation_materialization_objects_generation_id_fkey FOREIGN KEY (generation_id) REFERENCES manager.rootfs_generations(generation_id) ON DELETE CASCADE;

ALTER TABLE ONLY manager.rootfs_generation_materialization_objects
    ADD CONSTRAINT rootfs_generation_materialization_objects_object_key_fkey FOREIGN KEY (object_key) REFERENCES manager.rootfs_materialization_objects(object_key) ON DELETE RESTRICT;

ALTER TABLE ONLY manager.rootfs_generations
    ADD CONSTRAINT rootfs_generations_base_artifact_digest_fkey FOREIGN KEY (base_artifact_digest) REFERENCES manager.rootfs_base_artifacts(artifact_digest) ON DELETE RESTRICT;

ALTER TABLE ONLY manager.rootfs_generations
    ADD CONSTRAINT rootfs_generations_filesystem_id_fkey FOREIGN KEY (filesystem_id) REFERENCES manager.rootfs_filesystems(filesystem_id) ON DELETE RESTRICT;

ALTER TABLE ONLY manager.rootfs_generations
    ADD CONSTRAINT rootfs_generations_parent_generation_id_fkey FOREIGN KEY (parent_generation_id) REFERENCES manager.rootfs_generations(generation_id) ON DELETE RESTRICT;

ALTER TABLE ONLY manager.rootfs_head_rollbacks
    ADD CONSTRAINT rootfs_head_rollbacks_filesystem_id_fkey FOREIGN KEY (filesystem_id) REFERENCES manager.rootfs_filesystems(filesystem_id) ON DELETE RESTRICT;

ALTER TABLE ONLY manager.rootfs_head_rollbacks
    ADD CONSTRAINT rootfs_head_rollbacks_new_generation_id_fkey FOREIGN KEY (new_generation_id) REFERENCES manager.rootfs_generations(generation_id) ON DELETE RESTRICT;

ALTER TABLE ONLY manager.rootfs_head_rollbacks
    ADD CONSTRAINT rootfs_head_rollbacks_old_generation_id_fkey FOREIGN KEY (old_generation_id) REFERENCES manager.rootfs_generations(generation_id) ON DELETE RESTRICT;

ALTER TABLE ONLY manager.rootfs_import_operation_objects
    ADD CONSTRAINT rootfs_import_operation_objects_object_key_fkey FOREIGN KEY (object_key) REFERENCES manager.rootfs_materialization_objects(object_key) ON DELETE RESTRICT;

ALTER TABLE ONLY manager.rootfs_import_operation_objects
    ADD CONSTRAINT rootfs_import_operation_objects_operation_id_fkey FOREIGN KEY (operation_id) REFERENCES manager.rootfs_import_operations(operation_id) ON DELETE CASCADE;

ALTER TABLE ONLY manager.rootfs_import_operations
    ADD CONSTRAINT rootfs_import_operations_result_artifact_digest_fkey FOREIGN KEY (result_artifact_digest) REFERENCES manager.rootfs_base_artifacts(artifact_digest) ON DELETE RESTRICT;

ALTER TABLE ONLY manager.rootfs_materialization_batch_objects
    ADD CONSTRAINT rootfs_materialization_batch_objects_batch_id_fkey FOREIGN KEY (batch_id) REFERENCES manager.rootfs_materialization_batches(batch_id) ON DELETE CASCADE;

ALTER TABLE ONLY manager.rootfs_materialization_batch_objects
    ADD CONSTRAINT rootfs_materialization_batch_objects_object_key_fkey FOREIGN KEY (object_key) REFERENCES manager.rootfs_materialization_objects(object_key) ON DELETE RESTRICT;

ALTER TABLE ONLY manager.rootfs_materialization_member_objects
    ADD CONSTRAINT rootfs_materialization_member_objects_batch_id_ordinal_fkey FOREIGN KEY (batch_id, ordinal) REFERENCES manager.rootfs_materialization_members(batch_id, ordinal) ON DELETE CASCADE;

ALTER TABLE ONLY manager.rootfs_materialization_member_objects
    ADD CONSTRAINT rootfs_materialization_member_objects_object_key_fkey FOREIGN KEY (object_key) REFERENCES manager.rootfs_materialization_objects(object_key) ON DELETE RESTRICT;

ALTER TABLE ONLY manager.rootfs_materialization_members
    ADD CONSTRAINT rootfs_materialization_members_batch_id_fkey FOREIGN KEY (batch_id) REFERENCES manager.rootfs_materialization_batches(batch_id) ON DELETE CASCADE;

ALTER TABLE ONLY manager.rootfs_running_forks
    ADD CONSTRAINT rootfs_running_forks_checkpoint_generation_id_fkey FOREIGN KEY (checkpoint_generation_id) REFERENCES manager.rootfs_generations(generation_id) ON DELETE RESTRICT;

ALTER TABLE ONLY manager.rootfs_running_forks
    ADD CONSTRAINT rootfs_running_forks_source_filesystem_id_fkey FOREIGN KEY (source_filesystem_id) REFERENCES manager.rootfs_filesystems(filesystem_id) ON DELETE RESTRICT;

ALTER TABLE ONLY manager.rootfs_running_forks
    ADD CONSTRAINT rootfs_running_forks_source_generation_id_fkey FOREIGN KEY (source_generation_id) REFERENCES manager.rootfs_generations(generation_id) ON DELETE RESTRICT;

ALTER TABLE ONLY manager.rootfs_running_forks
    ADD CONSTRAINT rootfs_running_forks_source_grant_id_fkey FOREIGN KEY (source_grant_id) REFERENCES manager.rootfs_writer_grants(grant_id) ON DELETE RESTRICT;

ALTER TABLE ONLY manager.rootfs_running_forks
    ADD CONSTRAINT rootfs_running_forks_source_sandbox_id_fkey FOREIGN KEY (source_sandbox_id) REFERENCES manager.sandboxes(sandbox_id) ON DELETE RESTRICT;

ALTER TABLE ONLY manager.rootfs_running_forks
    ADD CONSTRAINT rootfs_running_forks_target_filesystem_id_fkey FOREIGN KEY (target_filesystem_id) REFERENCES manager.rootfs_filesystems(filesystem_id) ON DELETE RESTRICT;

ALTER TABLE ONLY manager.rootfs_running_forks
    ADD CONSTRAINT rootfs_running_forks_target_sandbox_id_fkey FOREIGN KEY (target_sandbox_id) REFERENCES manager.sandboxes(sandbox_id) ON DELETE RESTRICT;

ALTER TABLE ONLY manager.rootfs_snapshots
    ADD CONSTRAINT rootfs_snapshots_filesystem_id_fkey FOREIGN KEY (filesystem_id) REFERENCES manager.rootfs_filesystems(filesystem_id) ON DELETE RESTRICT;

ALTER TABLE ONLY manager.rootfs_snapshots
    ADD CONSTRAINT rootfs_snapshots_head_generation_id_fkey FOREIGN KEY (head_generation_id) REFERENCES manager.rootfs_generations(generation_id) ON DELETE RESTRICT;

ALTER TABLE ONLY manager.rootfs_writer_grants
    ADD CONSTRAINT rootfs_writer_grants_filesystem_id_fkey FOREIGN KEY (filesystem_id) REFERENCES manager.rootfs_filesystems(filesystem_id) ON DELETE CASCADE;

ALTER TABLE ONLY manager.rootfs_writer_grants
    ADD CONSTRAINT rootfs_writer_grants_sandbox_id_fkey FOREIGN KEY (sandbox_id) REFERENCES manager.sandboxes(sandbox_id) ON DELETE RESTRICT;

ALTER TABLE ONLY manager.runtime_resource_leases
    ADD CONSTRAINT runtime_resource_leases_cluster_id_node_id_node_uid_node_b_fkey FOREIGN KEY (cluster_id, node_id, node_uid, node_boot_id) REFERENCES manager.runtime_node_capacities(cluster_id, node_id, node_uid, node_boot_id) ON DELETE RESTRICT;

ALTER TABLE ONLY manager.runtime_resource_leases
    ADD CONSTRAINT runtime_resource_leases_slot_id_fkey FOREIGN KEY (slot_id) REFERENCES manager.runtime_slots(slot_id) ON DELETE RESTRICT;

ALTER TABLE ONLY manager.runtime_slots
    ADD CONSTRAINT runtime_slots_filesystem_id_fkey FOREIGN KEY (filesystem_id) REFERENCES manager.rootfs_filesystems(filesystem_id) ON DELETE RESTRICT;

ALTER TABLE ONLY manager.runtime_slots
    ADD CONSTRAINT runtime_slots_resource_lease_id_fkey FOREIGN KEY (resource_lease_id) REFERENCES manager.runtime_resource_leases(lease_id) ON DELETE RESTRICT;

ALTER TABLE ONLY manager.runtime_slots
    ADD CONSTRAINT runtime_slots_sandbox_id_fkey FOREIGN KEY (sandbox_id) REFERENCES manager.sandboxes(sandbox_id) ON DELETE RESTRICT;

ALTER TABLE ONLY manager.runtime_slots
    ADD CONSTRAINT runtime_slots_source_generation_id_fkey FOREIGN KEY (source_generation_id) REFERENCES manager.rootfs_generations(generation_id) ON DELETE RESTRICT;

ALTER TABLE ONLY manager.runtime_slots
    ADD CONSTRAINT runtime_slots_writer_grant_id_fkey FOREIGN KEY (writer_grant_id) REFERENCES manager.rootfs_writer_grants(grant_id) ON DELETE RESTRICT;

ALTER TABLE ONLY manager.sandbox_lifecycle_txns
    ADD CONSTRAINT sandbox_lifecycle_txns_sandbox_id_fkey FOREIGN KEY (sandbox_id) REFERENCES manager.sandboxes(sandbox_id) ON DELETE CASCADE;

ALTER TABLE ONLY manager.sandbox_metering_projection_queue
    ADD CONSTRAINT sandbox_metering_projection_queue_sandbox_id_fkey FOREIGN KEY (sandbox_id) REFERENCES manager.sandboxes(sandbox_id) ON DELETE CASCADE;

ALTER TABLE ONLY manager.sandbox_network_mutation_bindings
    ADD CONSTRAINT sandbox_network_mutation_bindings_operation_id_fkey FOREIGN KEY (operation_id) REFERENCES manager.sandbox_network_mutations(operation_id) ON DELETE CASCADE;

ALTER TABLE ONLY manager.sandbox_network_mutation_bindings
    ADD CONSTRAINT sandbox_network_mutation_bindings_sandbox_id_fkey FOREIGN KEY (sandbox_id) REFERENCES manager.sandboxes(sandbox_id) ON DELETE CASCADE;

ALTER TABLE ONLY manager.sandbox_network_mutation_bindings
    ADD CONSTRAINT sandbox_network_mutation_bindings_source_id_fkey FOREIGN KEY (source_id) REFERENCES scheduler.credential_sources(id);

ALTER TABLE ONLY manager.sandbox_network_mutations
    ADD CONSTRAINT sandbox_network_mutations_sandbox_id_fkey FOREIGN KEY (sandbox_id) REFERENCES manager.sandboxes(sandbox_id) ON DELETE CASCADE;

ALTER TABLE ONLY manager.sandbox_network_mutations
    ADD CONSTRAINT sandbox_network_mutations_slot_id_fkey FOREIGN KEY (slot_id) REFERENCES manager.runtime_slots(slot_id);

ALTER TABLE ONLY manager.sandbox_rootfs_bindings
    ADD CONSTRAINT sandbox_rootfs_bindings_filesystem_id_fkey FOREIGN KEY (filesystem_id) REFERENCES manager.rootfs_filesystems(filesystem_id) ON DELETE RESTRICT;

ALTER TABLE ONLY manager.sandbox_rootfs_bindings
    ADD CONSTRAINT sandbox_rootfs_bindings_sandbox_id_fkey FOREIGN KEY (sandbox_id) REFERENCES manager.sandboxes(sandbox_id) ON DELETE CASCADE;

ALTER TABLE ONLY manager.sandbox_runtime_claims
    ADD CONSTRAINT sandbox_runtime_claims_sandbox_id_fkey FOREIGN KEY (sandbox_id) REFERENCES manager.sandboxes(sandbox_id) ON DELETE CASCADE;

INSERT INTO manager.rootfs_composite_backlog_policy (singleton, max_descriptor_bytes)
VALUES (TRUE, 1073741824)
ON CONFLICT (singleton) DO NOTHING;

-- +goose Down

-- The physical architecture cutover is intentionally irreversible.
-- +goose StatementBegin
DO $$
BEGIN
    RAISE EXCEPTION 'Nomad block-COW baseline cannot be rolled back'
        USING ERRCODE = '55000';
END;
$$;
-- +goose StatementEnd

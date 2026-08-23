-- +goose Up

-- Import operations are the durable boundary before OCI download, local XFS
-- construction, or object PUT. A lease fences workers; terminal publication
-- installs the ready artifact and its complete object reachability atomically.
CREATE TABLE manager.rootfs_import_operations (
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

CREATE INDEX idx_rootfs_import_operations_work
    ON manager.rootfs_import_operations(state, lease_expires_at, created_at, operation_id)
    WHERE state IN ('pending', 'building');

CREATE INDEX idx_rootfs_import_operations_terminal
    ON manager.rootfs_import_operations(updated_at, operation_id)
    WHERE state IN ('ready', 'abandoned');

-- Reuse the existing block-object catalog. This table records operation
-- ownership and upload ordering without creating a second metadata truth.
CREATE TABLE manager.rootfs_import_operation_objects (
    operation_id TEXT NOT NULL
        REFERENCES manager.rootfs_import_operations(operation_id) ON DELETE CASCADE,
    object_key TEXT NOT NULL
        REFERENCES manager.rootfs_materialization_objects(object_key) ON DELETE RESTRICT,
    upload_state TEXT NOT NULL CHECK (upload_state IN ('prepared', 'published')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (operation_id, object_key)
);

CREATE INDEX idx_rootfs_import_operation_objects_state
    ON manager.rootfs_import_operation_objects(upload_state, updated_at, operation_id);

-- Ready artifacts, not terminal operation rows, retain live base objects.
CREATE TABLE manager.rootfs_base_artifact_objects (
    artifact_digest TEXT NOT NULL
        REFERENCES manager.rootfs_base_artifacts(artifact_digest) ON DELETE CASCADE,
    object_key TEXT NOT NULL
        REFERENCES manager.rootfs_materialization_objects(object_key) ON DELETE RESTRICT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (artifact_digest, object_key)
);

CREATE INDEX idx_rootfs_base_artifact_objects_object
    ON manager.rootfs_base_artifact_objects(object_key, artifact_digest);

-- Nullable during the non-destructive migration window. The terminal Nomad
-- cutover removes legacy rows and makes this attestation shape mandatory.
ALTER TABLE manager.rootfs_base_artifacts
    ADD COLUMN IF NOT EXISTS attestation BYTEA
        CHECK (octet_length(attestation) BETWEEN 1 AND 65536),
    ADD COLUMN IF NOT EXISTS manifest_digest TEXT,
    ADD COLUMN IF NOT EXISTS config_digest TEXT,
    ADD COLUMN IF NOT EXISTS procd_protocol TEXT,
    ADD COLUMN IF NOT EXISTS procd_digest TEXT,
    ADD COLUMN IF NOT EXISTS logical_size_bytes BIGINT,
    ADD COLUMN IF NOT EXISTS descriptor_digest TEXT;

-- +goose Down

ALTER TABLE manager.rootfs_base_artifacts
    DROP COLUMN IF EXISTS descriptor_digest,
    DROP COLUMN IF EXISTS logical_size_bytes,
    DROP COLUMN IF EXISTS procd_digest,
    DROP COLUMN IF EXISTS procd_protocol,
    DROP COLUMN IF EXISTS config_digest,
    DROP COLUMN IF EXISTS manifest_digest,
    DROP COLUMN IF EXISTS attestation;

DROP INDEX IF EXISTS manager.idx_rootfs_base_artifact_objects_object;
DROP TABLE IF EXISTS manager.rootfs_base_artifact_objects;
DROP INDEX IF EXISTS manager.idx_rootfs_import_operation_objects_state;
DROP TABLE IF EXISTS manager.rootfs_import_operation_objects;
DROP INDEX IF EXISTS manager.idx_rootfs_import_operations_terminal;
DROP INDEX IF EXISTS manager.idx_rootfs_import_operations_work;
DROP TABLE IF EXISTS manager.rootfs_import_operations;

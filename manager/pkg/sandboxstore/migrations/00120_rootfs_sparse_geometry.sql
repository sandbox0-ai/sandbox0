-- +goose Up

-- Zero retains the original fixed-size artifact and canonical identity.
ALTER TABLE manager.rootfs_import_operations
    ADD COLUMN initial_filesystem_size_bytes bigint NOT NULL DEFAULT 0;
ALTER TABLE manager.rootfs_base_artifacts
    ADD COLUMN initial_filesystem_size_bytes bigint NOT NULL DEFAULT 0;

ALTER TABLE manager.rootfs_import_operations
    DROP CONSTRAINT rootfs_import_operations_logical_size_bytes_check,
    ADD CONSTRAINT rootfs_import_operations_logical_size_bytes_check CHECK (
        logical_size_bytes BETWEEN 314572800 AND 70368744177664 AND logical_size_bytes % 4096 = 0),
    ADD CONSTRAINT rootfs_import_sparse_geometry CHECK (
        (initial_filesystem_size_bytes = 0 AND logical_size_bytes <= 1099511627776)
        OR (initial_filesystem_size_bytes BETWEEN 314572800 AND 1099511627776
            AND initial_filesystem_size_bytes % 4096 = 0
            AND initial_filesystem_size_bytes <= logical_size_bytes AND format_generation = 2));
ALTER TABLE manager.rootfs_base_artifacts
    DROP CONSTRAINT rootfs_base_artifacts_logical_size_bytes_check,
    ADD CONSTRAINT rootfs_base_artifacts_logical_size_bytes_check CHECK (
        logical_size_bytes BETWEEN 314572800 AND 70368744177664 AND logical_size_bytes % 4096 = 0),
    ADD CONSTRAINT rootfs_artifact_sparse_geometry CHECK (
        (initial_filesystem_size_bytes = 0 AND logical_size_bytes <= 1099511627776)
        OR (initial_filesystem_size_bytes BETWEEN 314572800 AND 1099511627776
            AND initial_filesystem_size_bytes % 4096 = 0
            AND initial_filesystem_size_bytes <= logical_size_bytes AND format_generation = 2));

CREATE INDEX idx_rootfs_base_artifacts_sparse_lookup ON manager.rootfs_base_artifacts
    (source_oci_digest, initial_filesystem_size_bytes, logical_size_bytes, format_generation, created_at DESC)
    WHERE state = 'ready';

-- +goose StatementBegin
CREATE OR REPLACE FUNCTION manager.preserve_rootfs_artifact_layout()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    proof jsonb;
BEGIN
    IF TG_OP = 'UPDATE' AND NEW.data_layout_policy IS DISTINCT FROM OLD.data_layout_policy THEN
        RAISE EXCEPTION 'RootFS artifact layout policy is immutable' USING ERRCODE = '23514';
    END IF;
    IF TG_OP = 'UPDATE' AND NEW.data_layout_policy <> '' AND NEW.attestation IS DISTINCT FROM OLD.attestation THEN
        RAISE EXCEPTION 'RootFS layout attestation is immutable' USING ERRCODE = '23514';
    END IF;
    IF NEW.attestation IS NULL OR octet_length(NEW.attestation) = 0 THEN
        IF NEW.data_layout_policy <> '' THEN
            RAISE EXCEPTION 'RootFS layout policy requires an attestation' USING ERRCODE = '23514';
        END IF;
        RETURN NEW;
    END IF;
    IF octet_length(NEW.attestation) > 65536 THEN
        RAISE EXCEPTION 'RootFS layout attestation exceeds limit' USING ERRCODE = '23514';
    END IF;
    proof := convert_from(NEW.attestation, 'UTF8')::jsonb;
    IF COALESCE(proof->>'data_layout_policy', '') <> NEW.data_layout_policy THEN
        RAISE EXCEPTION 'RootFS layout policy differs from attestation' USING ERRCODE = '23514';
    END IF;
    IF NEW.data_layout_policy = '' THEN
        IF COALESCE(proof->>'data_layout_fallback', '') <> ''
            OR COALESCE(proof->>'data_layout_range_bytes', '0') <> '0'
            OR proof->>'version' = '2' THEN
            RAISE EXCEPTION 'Legacy RootFS cannot carry new layout evidence' USING ERRCODE = '23514';
        END IF;
    ELSIF COALESCE(proof->>'version', '') NOT IN ('2', '3', '4')
        OR proof->>'format_generation' IS DISTINCT FROM '2'
        OR proof->>'data_layout_range_bytes' IS DISTINCT FROM '65536'
        OR COALESCE(proof->>'data_layout_fallback', '') NOT IN ('', 'scan-budget-exceeded') THEN
        RAISE EXCEPTION 'RootFS layout attestation is incomplete' USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE OR REPLACE FUNCTION manager.preserve_rootfs_artifact_mapping()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    proof jsonb;
BEGIN
    IF TG_OP = 'UPDATE' AND NEW.mapping_group_policy IS DISTINCT FROM OLD.mapping_group_policy THEN
        RAISE EXCEPTION 'RootFS artifact mapping policy is immutable' USING ERRCODE = '23514';
    END IF;
    IF TG_OP = 'UPDATE' AND NEW.mapping_group_policy <> '' AND NEW.attestation IS DISTINCT FROM OLD.attestation THEN
        RAISE EXCEPTION 'RootFS mapping attestation is immutable' USING ERRCODE = '23514';
    END IF;
    IF NEW.attestation IS NULL OR octet_length(NEW.attestation) = 0 THEN
        IF NEW.mapping_group_policy <> '' THEN
            RAISE EXCEPTION 'RootFS mapping policy requires an attestation' USING ERRCODE = '23514';
        END IF;
        RETURN NEW;
    END IF;
    IF octet_length(NEW.attestation) > 65536 THEN
        RAISE EXCEPTION 'RootFS mapping attestation exceeds limit' USING ERRCODE = '23514';
    END IF;
    proof := convert_from(NEW.attestation, 'UTF8')::jsonb;
    IF COALESCE(proof->>'mapping_group_policy', '') <> NEW.mapping_group_policy THEN
        RAISE EXCEPTION 'RootFS mapping policy differs from attestation' USING ERRCODE = '23514';
    END IF;
    IF NEW.mapping_group_policy = '' THEN
        IF proof->>'version' = '3' THEN
            RAISE EXCEPTION 'Legacy mapping cannot carry version 3 evidence' USING ERRCODE = '23514';
        END IF;
    ELSIF COALESCE(proof->>'version', '') NOT IN ('3', '4') OR proof->>'format_generation' IS DISTINCT FROM '2' THEN
        RAISE EXCEPTION 'RootFS mapping attestation is incomplete' USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$;
-- +goose StatementEnd

-- The new attestation binds both capacities, independently of optional data or
-- mapping policies. Old publishers cannot label a version-4 artifact as legacy.
-- +goose StatementBegin
CREATE FUNCTION manager.preserve_rootfs_artifact_sparse_geometry()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    proof jsonb;
BEGIN
    IF TG_OP = 'UPDATE' AND ROW(NEW.initial_filesystem_size_bytes, NEW.logical_size_bytes)
        IS DISTINCT FROM ROW(OLD.initial_filesystem_size_bytes, OLD.logical_size_bytes) THEN
        RAISE EXCEPTION 'RootFS artifact geometry is immutable' USING ERRCODE = '23514';
    END IF;
    IF TG_OP = 'UPDATE' AND NEW.initial_filesystem_size_bytes <> 0
        AND NEW.attestation IS DISTINCT FROM OLD.attestation THEN
        RAISE EXCEPTION 'RootFS sparse attestation is immutable' USING ERRCODE = '23514';
    END IF;
    IF NEW.attestation IS NULL OR octet_length(NEW.attestation) = 0 THEN
        IF NEW.initial_filesystem_size_bytes <> 0 THEN
            RAISE EXCEPTION 'RootFS sparse geometry requires an attestation' USING ERRCODE = '23514';
        END IF;
        RETURN NEW;
    END IF;
    IF octet_length(NEW.attestation) > 65536 THEN
        RAISE EXCEPTION 'RootFS sparse attestation exceeds limit' USING ERRCODE = '23514';
    END IF;
    proof := convert_from(NEW.attestation, 'UTF8')::jsonb;
    IF COALESCE(proof->>'initial_filesystem_size_bytes', '0') <> NEW.initial_filesystem_size_bytes::text THEN
        RAISE EXCEPTION 'RootFS initial size differs from attestation' USING ERRCODE = '23514';
    END IF;
    IF NEW.initial_filesystem_size_bytes = 0 THEN
        IF proof->>'version' = '4' THEN
            RAISE EXCEPTION 'Fixed RootFS cannot carry sparse attestation' USING ERRCODE = '23514';
        END IF;
    ELSIF proof->>'version' IS DISTINCT FROM '4'
        OR proof->>'logical_size_bytes' IS DISTINCT FROM NEW.logical_size_bytes::text
        OR proof->>'format_generation' IS DISTINCT FROM '2' THEN
        RAISE EXCEPTION 'RootFS sparse attestation is incomplete' USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$;
-- +goose StatementEnd
CREATE TRIGGER preserve_rootfs_artifact_sparse_geometry
    BEFORE INSERT OR UPDATE OF initial_filesystem_size_bytes, logical_size_bytes, attestation
    ON manager.rootfs_base_artifacts
    FOR EACH ROW EXECUTE FUNCTION manager.preserve_rootfs_artifact_sparse_geometry();

-- Fence old workers at the same transaction that publishes Ready.
-- +goose StatementBegin
CREATE FUNCTION manager.preserve_rootfs_operation_sparse_geometry()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'UPDATE' AND ROW(NEW.initial_filesystem_size_bytes, NEW.logical_size_bytes)
        IS DISTINCT FROM ROW(OLD.initial_filesystem_size_bytes, OLD.logical_size_bytes) THEN
        RAISE EXCEPTION 'RootFS import geometry is immutable' USING ERRCODE = '23514';
    END IF;
    IF NEW.state = 'ready' AND (NEW.initial_filesystem_size_bytes <> 0 OR EXISTS (
        SELECT 1 FROM manager.rootfs_base_artifacts artifact
        WHERE artifact.artifact_digest = NEW.result_artifact_digest AND artifact.initial_filesystem_size_bytes <> 0
    )) AND NOT EXISTS (
        SELECT 1 FROM manager.rootfs_base_artifacts artifact
        WHERE artifact.artifact_digest = NEW.result_artifact_digest AND artifact.state = 'ready'
            AND artifact.initial_filesystem_size_bytes = NEW.initial_filesystem_size_bytes
            AND artifact.logical_size_bytes = NEW.logical_size_bytes
    ) THEN
        RAISE EXCEPTION 'RootFS ready artifact does not match import geometry' USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$;
-- +goose StatementEnd
CREATE TRIGGER preserve_rootfs_operation_sparse_geometry
    BEFORE INSERT OR UPDATE OF initial_filesystem_size_bytes, logical_size_bytes, state, result_artifact_digest
    ON manager.rootfs_import_operations
    FOR EACH ROW EXECUTE FUNCTION manager.preserve_rootfs_operation_sparse_geometry();

-- +goose Down

LOCK TABLE manager.rootfs_import_operations, manager.rootfs_base_artifacts IN ACCESS EXCLUSIVE MODE;
-- +goose StatementBegin
DO $$ BEGIN
    IF EXISTS (SELECT 1 FROM manager.rootfs_import_operations WHERE initial_filesystem_size_bytes <> 0)
        OR EXISTS (SELECT 1 FROM manager.rootfs_base_artifacts WHERE initial_filesystem_size_bytes <> 0) THEN
        RAISE EXCEPTION 'Cannot remove committed RootFS sparse geometry';
    END IF;
END $$;
-- +goose StatementEnd
DROP TRIGGER preserve_rootfs_operation_sparse_geometry ON manager.rootfs_import_operations;
DROP FUNCTION manager.preserve_rootfs_operation_sparse_geometry();
DROP TRIGGER preserve_rootfs_artifact_sparse_geometry ON manager.rootfs_base_artifacts;
DROP FUNCTION manager.preserve_rootfs_artifact_sparse_geometry();
DROP INDEX manager.idx_rootfs_base_artifacts_sparse_lookup;
ALTER TABLE manager.rootfs_import_operations
    DROP CONSTRAINT rootfs_import_sparse_geometry,
    DROP CONSTRAINT rootfs_import_operations_logical_size_bytes_check,
    ADD CONSTRAINT rootfs_import_operations_logical_size_bytes_check CHECK (
        logical_size_bytes BETWEEN 314572800 AND 1099511627776 AND logical_size_bytes % 4096 = 0),
    DROP COLUMN initial_filesystem_size_bytes;
ALTER TABLE manager.rootfs_base_artifacts
    DROP CONSTRAINT rootfs_artifact_sparse_geometry,
    DROP CONSTRAINT rootfs_base_artifacts_logical_size_bytes_check,
    ADD CONSTRAINT rootfs_base_artifacts_logical_size_bytes_check CHECK (
        logical_size_bytes BETWEEN 314572800 AND 1099511627776 AND logical_size_bytes % 4096 = 0),
    DROP COLUMN initial_filesystem_size_bytes;

-- +goose StatementBegin
CREATE OR REPLACE FUNCTION manager.preserve_rootfs_artifact_layout()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    proof jsonb;
BEGIN
    IF TG_OP = 'UPDATE' AND NEW.data_layout_policy IS DISTINCT FROM OLD.data_layout_policy THEN
        RAISE EXCEPTION 'RootFS artifact layout policy is immutable' USING ERRCODE = '23514';
    END IF;
    IF TG_OP = 'UPDATE' AND NEW.data_layout_policy <> '' AND NEW.attestation IS DISTINCT FROM OLD.attestation THEN
        RAISE EXCEPTION 'RootFS layout attestation is immutable' USING ERRCODE = '23514';
    END IF;
    IF NEW.attestation IS NULL OR octet_length(NEW.attestation) = 0 THEN
        IF NEW.data_layout_policy <> '' THEN
            RAISE EXCEPTION 'RootFS layout policy requires an attestation' USING ERRCODE = '23514';
        END IF;
        RETURN NEW;
    END IF;
    IF octet_length(NEW.attestation) > 65536 THEN
        RAISE EXCEPTION 'RootFS layout attestation exceeds limit' USING ERRCODE = '23514';
    END IF;
    proof := convert_from(NEW.attestation, 'UTF8')::jsonb;
    IF COALESCE(proof->>'data_layout_policy', '') <> NEW.data_layout_policy THEN
        RAISE EXCEPTION 'RootFS layout policy differs from attestation' USING ERRCODE = '23514';
    END IF;
    IF NEW.data_layout_policy = '' THEN
        IF COALESCE(proof->>'data_layout_fallback', '') <> ''
            OR COALESCE(proof->>'data_layout_range_bytes', '0') <> '0'
            OR proof->>'version' = '2' THEN
            RAISE EXCEPTION 'Legacy RootFS cannot carry new layout evidence' USING ERRCODE = '23514';
        END IF;
    ELSIF COALESCE(proof->>'version', '') NOT IN ('2', '3')
        OR proof->>'format_generation' IS DISTINCT FROM '2'
        OR proof->>'data_layout_range_bytes' IS DISTINCT FROM '65536'
        OR COALESCE(proof->>'data_layout_fallback', '') NOT IN ('', 'scan-budget-exceeded') THEN
        RAISE EXCEPTION 'RootFS layout attestation is incomplete' USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE OR REPLACE FUNCTION manager.preserve_rootfs_artifact_mapping()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    proof jsonb;
BEGIN
    IF TG_OP = 'UPDATE' AND NEW.mapping_group_policy IS DISTINCT FROM OLD.mapping_group_policy THEN
        RAISE EXCEPTION 'RootFS artifact mapping policy is immutable' USING ERRCODE = '23514';
    END IF;
    IF TG_OP = 'UPDATE' AND NEW.mapping_group_policy <> '' AND NEW.attestation IS DISTINCT FROM OLD.attestation THEN
        RAISE EXCEPTION 'RootFS mapping attestation is immutable' USING ERRCODE = '23514';
    END IF;
    IF NEW.attestation IS NULL OR octet_length(NEW.attestation) = 0 THEN
        IF NEW.mapping_group_policy <> '' THEN
            RAISE EXCEPTION 'RootFS mapping policy requires an attestation' USING ERRCODE = '23514';
        END IF;
        RETURN NEW;
    END IF;
    IF octet_length(NEW.attestation) > 65536 THEN
        RAISE EXCEPTION 'RootFS mapping attestation exceeds limit' USING ERRCODE = '23514';
    END IF;
    proof := convert_from(NEW.attestation, 'UTF8')::jsonb;
    IF COALESCE(proof->>'mapping_group_policy', '') <> NEW.mapping_group_policy THEN
        RAISE EXCEPTION 'RootFS mapping policy differs from attestation' USING ERRCODE = '23514';
    END IF;
    IF NEW.mapping_group_policy = '' THEN
        IF proof->>'version' = '3' THEN
            RAISE EXCEPTION 'Legacy mapping cannot carry version 3 evidence' USING ERRCODE = '23514';
        END IF;
    ELSIF proof->>'version' IS DISTINCT FROM '3' OR proof->>'format_generation' IS DISTINCT FROM '2' THEN
        RAISE EXCEPTION 'RootFS mapping attestation is incomplete' USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$;
-- +goose StatementEnd

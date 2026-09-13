-- +goose Up

-- Metadata delivery is an immutable import policy, not a runtime format.
ALTER TABLE manager.rootfs_import_operations
    ADD COLUMN mapping_group_policy text NOT NULL DEFAULT ''
        CHECK (mapping_group_policy IN ('', 'contiguous-mapping-v1')),
    ADD CONSTRAINT rootfs_import_mapping_format CHECK (
        mapping_group_policy = '' OR format_generation = 2);

ALTER TABLE manager.rootfs_base_artifacts
    ADD COLUMN mapping_group_policy text NOT NULL DEFAULT ''
        CHECK (mapping_group_policy IN ('', 'contiguous-mapping-v1')),
    ADD CONSTRAINT rootfs_artifact_mapping_format CHECK (
        mapping_group_policy = '' OR format_generation = 2);

DROP INDEX manager.idx_rootfs_base_artifacts_layout_lookup;
CREATE INDEX idx_rootfs_base_artifacts_mapping_lookup
    ON manager.rootfs_base_artifacts(source_oci_digest, data_layout_policy, mapping_group_policy, format_generation, created_at DESC)
    WHERE state = 'ready';

-- Version 3 may combine mapping grouping with either legacy or file-relative
-- data layout. The independent mapping fence below binds that version exactly.
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
CREATE FUNCTION manager.preserve_rootfs_artifact_mapping()
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

CREATE TRIGGER preserve_rootfs_artifact_mapping
    BEFORE INSERT OR UPDATE OF mapping_group_policy, attestation ON manager.rootfs_base_artifacts
    FOR EACH ROW EXECUTE FUNCTION manager.preserve_rootfs_artifact_mapping();

-- This terminal fence also applies to old workers which do not scan the new
-- column. They cannot publish an empty-policy artifact for a grouped operation.
-- +goose StatementBegin
CREATE FUNCTION manager.preserve_rootfs_operation_mapping()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'UPDATE' AND NEW.mapping_group_policy IS DISTINCT FROM OLD.mapping_group_policy THEN
        RAISE EXCEPTION 'RootFS operation mapping policy is immutable' USING ERRCODE = '23514';
    END IF;
    IF NEW.state = 'ready' AND (NEW.mapping_group_policy <> '' OR EXISTS (
        SELECT 1 FROM manager.rootfs_base_artifacts artifact
        WHERE artifact.artifact_digest = NEW.result_artifact_digest AND artifact.mapping_group_policy <> ''
    )) AND NOT EXISTS (
        SELECT 1 FROM manager.rootfs_base_artifacts artifact
        WHERE artifact.artifact_digest = NEW.result_artifact_digest
            AND artifact.state = 'ready'
            AND artifact.mapping_group_policy = NEW.mapping_group_policy
    ) THEN
        RAISE EXCEPTION 'RootFS ready artifact does not match operation mapping policy' USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$;
-- +goose StatementEnd

CREATE TRIGGER preserve_rootfs_operation_mapping
    BEFORE INSERT OR UPDATE OF mapping_group_policy, state, result_artifact_digest ON manager.rootfs_import_operations
    FOR EACH ROW EXECUTE FUNCTION manager.preserve_rootfs_operation_mapping();

-- +goose Down

-- Do not race the guard with another publisher or erase committed provenance.
LOCK TABLE manager.rootfs_import_operations, manager.rootfs_base_artifacts IN ACCESS EXCLUSIVE MODE;
-- +goose StatementBegin
DO $$ BEGIN
    IF EXISTS (SELECT 1 FROM manager.rootfs_import_operations WHERE mapping_group_policy <> '')
        OR EXISTS (SELECT 1 FROM manager.rootfs_base_artifacts WHERE mapping_group_policy <> '') THEN
        RAISE EXCEPTION 'Cannot remove RootFS mapping provenance with policy-bearing rows';
    END IF;
END $$;
-- +goose StatementEnd

DROP TRIGGER preserve_rootfs_operation_mapping ON manager.rootfs_import_operations;
DROP FUNCTION manager.preserve_rootfs_operation_mapping();
DROP TRIGGER preserve_rootfs_artifact_mapping ON manager.rootfs_base_artifacts;
DROP FUNCTION manager.preserve_rootfs_artifact_mapping();
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
    ELSIF proof->>'version' IS DISTINCT FROM '2'
        OR proof->>'format_generation' IS DISTINCT FROM '2'
        OR proof->>'data_layout_range_bytes' IS DISTINCT FROM '65536'
        OR COALESCE(proof->>'data_layout_fallback', '') NOT IN ('', 'scan-budget-exceeded') THEN
        RAISE EXCEPTION 'RootFS layout attestation is incomplete' USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$;
-- +goose StatementEnd
DROP INDEX manager.idx_rootfs_base_artifacts_mapping_lookup;
CREATE INDEX idx_rootfs_base_artifacts_layout_lookup
    ON manager.rootfs_base_artifacts(source_oci_digest, data_layout_policy, format_generation, created_at DESC)
    WHERE state = 'ready';
ALTER TABLE manager.rootfs_base_artifacts DROP CONSTRAINT rootfs_artifact_mapping_format, DROP COLUMN mapping_group_policy;
ALTER TABLE manager.rootfs_import_operations DROP CONSTRAINT rootfs_import_mapping_format, DROP COLUMN mapping_group_policy;

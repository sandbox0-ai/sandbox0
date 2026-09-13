-- +goose Up

-- Empty is the pre-policy import contract, not a guess from physical geometry.
-- New layout attestations use version 2; legacy attestation bytes stay unchanged.
ALTER TABLE manager.rootfs_import_operations
    ADD COLUMN data_layout_policy text NOT NULL DEFAULT ''
        CHECK (data_layout_policy IN ('', 'xfs-file-ranges-v1')),
    ADD CONSTRAINT rootfs_import_layout_geometry CHECK (
        data_layout_policy = '' OR (format_generation = 2 AND block_data_range_bytes = 65536));

ALTER TABLE manager.rootfs_base_artifacts
    ADD COLUMN data_layout_policy text NOT NULL DEFAULT ''
        CHECK (data_layout_policy IN ('', 'xfs-file-ranges-v1')),
    ADD CONSTRAINT rootfs_artifact_layout_geometry CHECK (
        data_layout_policy = '' OR (format_generation = 2 AND import_data_range_bytes IS NOT NULL AND import_data_range_bytes = 65536));

CREATE INDEX idx_rootfs_base_artifacts_layout_lookup
    ON manager.rootfs_base_artifacts(source_oci_digest, data_layout_policy, format_generation, created_at DESC)
    WHERE state = 'ready';

-- Keep the indexed metadata tied to the attestation. Ordinary legacy inserts,
-- including old replicas, retain the empty policy. They cannot label a version-2
-- attestation as legacy, or change a committed artifact's layout identity.
-- +goose StatementBegin
CREATE FUNCTION manager.preserve_rootfs_artifact_layout()
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

CREATE TRIGGER preserve_rootfs_artifact_layout
    BEFORE INSERT OR UPDATE OF data_layout_policy, attestation ON manager.rootfs_base_artifacts
    FOR EACH ROW EXECUTE FUNCTION manager.preserve_rootfs_artifact_layout();

-- Old workers can lease a new-policy row during a rolling upgrade. Even if they
-- ignore its new column, their legacy artifact must never complete that operation.
-- This fence is in the same transaction as Ready and cannot be bypassed by a
-- process-local default or a worker restart. Upgrade all managers before enablement.
-- +goose StatementBegin
CREATE FUNCTION manager.preserve_rootfs_operation_layout()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'UPDATE' AND NEW.data_layout_policy IS DISTINCT FROM OLD.data_layout_policy THEN
        RAISE EXCEPTION 'RootFS operation layout policy is immutable' USING ERRCODE = '23514';
    END IF;
    IF NEW.state = 'ready' AND (NEW.data_layout_policy <> '' OR EXISTS (
        SELECT 1 FROM manager.rootfs_base_artifacts artifact
        WHERE artifact.artifact_digest = NEW.result_artifact_digest AND artifact.data_layout_policy <> ''
    )) AND NOT EXISTS (
        SELECT 1 FROM manager.rootfs_base_artifacts artifact
        WHERE artifact.artifact_digest = NEW.result_artifact_digest
            AND artifact.state = 'ready'
            AND artifact.data_layout_policy = NEW.data_layout_policy
    ) THEN
        RAISE EXCEPTION 'RootFS ready artifact does not match operation layout policy' USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$;
-- +goose StatementEnd

CREATE TRIGGER preserve_rootfs_operation_layout
    BEFORE INSERT OR UPDATE OF data_layout_policy, state, result_artifact_digest ON manager.rootfs_import_operations
    FOR EACH ROW EXECUTE FUNCTION manager.preserve_rootfs_operation_layout();

-- +goose Down

-- Dropping this fence with live policy-bearing rows would make old workers
-- reinterpret them. Fail closed; do not rewrite or delete those identities.
-- +goose StatementBegin
DO $$ BEGIN
    IF EXISTS (SELECT 1 FROM manager.rootfs_import_operations WHERE data_layout_policy <> '')
        OR EXISTS (SELECT 1 FROM manager.rootfs_base_artifacts WHERE data_layout_policy <> '') THEN
        RAISE EXCEPTION 'Cannot remove RootFS layout provenance with policy-bearing rows';
    END IF;
END $$;
-- +goose StatementEnd

DROP TRIGGER preserve_rootfs_operation_layout ON manager.rootfs_import_operations;
DROP FUNCTION manager.preserve_rootfs_operation_layout();
DROP TRIGGER preserve_rootfs_artifact_layout ON manager.rootfs_base_artifacts;
DROP FUNCTION manager.preserve_rootfs_artifact_layout();
DROP INDEX manager.idx_rootfs_base_artifacts_layout_lookup;
ALTER TABLE manager.rootfs_base_artifacts DROP CONSTRAINT rootfs_artifact_layout_geometry, DROP COLUMN data_layout_policy;
ALTER TABLE manager.rootfs_import_operations DROP CONSTRAINT rootfs_import_layout_geometry, DROP COLUMN data_layout_policy;

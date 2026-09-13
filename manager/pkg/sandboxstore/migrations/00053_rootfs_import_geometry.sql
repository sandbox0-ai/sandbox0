-- +goose Up

-- Import geometry is selection provenance on the existing immutable artifact,
-- not part of the descriptor, attestation, or runtime compatibility format.
ALTER TABLE manager.rootfs_base_artifacts
    ADD COLUMN import_data_range_bytes integer
        CHECK (import_data_range_bytes > 0
            AND import_data_range_bytes <= 8388608
            AND import_data_range_bytes % 4096 = 0);

CREATE INDEX idx_rootfs_import_operations_ready_artifact
    ON manager.rootfs_import_operations(result_artifact_digest)
    WHERE state = 'ready';

-- The same evidence rule serves migration, rolling-upgrade repair, and GC.
-- Missing, conflicting, or malformed history remains NULL; the old default
-- is not evidence that an individual artifact used 8 MiB ranges.
-- +goose StatementBegin
CREATE FUNCTION manager.rootfs_import_geometry_provenance(target_artifact_digest text)
RETURNS integer LANGUAGE sql STABLE AS $$
    SELECT MIN(operation.block_data_range_bytes)
    FROM manager.rootfs_base_artifacts artifact
    JOIN manager.rootfs_import_operations operation
        ON operation.result_artifact_digest = artifact.artifact_digest
        AND operation.state = 'ready'
    WHERE artifact.state = 'ready' AND artifact.artifact_digest = target_artifact_digest
    GROUP BY artifact.artifact_digest
    HAVING COUNT(DISTINCT operation.block_data_range_bytes) = 1
        AND BOOL_AND(COALESCE(
            operation.source_oci_ref = artifact.source_oci_ref
            AND operation.source_oci_digest = artifact.source_oci_digest
            AND operation.oci_os = artifact.oci_os
            AND operation.oci_architecture = artifact.oci_architecture
            AND operation.oci_variant = artifact.oci_variant
            AND operation.format_generation = artifact.format_generation
            AND operation.procd_protocol = artifact.procd_protocol
            AND operation.procd_digest = artifact.procd_digest
            AND operation.logical_size_bytes = artifact.logical_size_bytes
            AND operation.block_data_range_bytes > 0
            AND operation.block_data_range_bytes <= 8388608
            AND operation.block_data_range_bytes % 4096 = 0
            AND operation.block_pack_bytes >= operation.block_data_range_bytes
            AND operation.block_pack_bytes <= 67108864
            AND operation.block_pack_bytes % NULLIF(operation.block_data_range_bytes, 0) = 0
            AND operation.block_page_entries BETWEEN 2 AND 65536,
            FALSE));
$$;
-- +goose StatementEnd

UPDATE manager.rootfs_base_artifacts artifact
SET import_data_range_bytes = manager.rootfs_import_geometry_provenance(artifact.artifact_digest);

-- An old publisher can still insert NULL during a rolling upgrade. Permit
-- only a proven monotonic NULL -> known repair; known values cannot change or
-- be cleared. This does not infer geometry from artifact age or current defaults.
-- +goose StatementBegin
CREATE FUNCTION manager.preserve_rootfs_import_geometry()
RETURNS trigger AS $$
BEGIN
    IF NEW.import_data_range_bytes IS DISTINCT FROM OLD.import_data_range_bytes THEN
        IF OLD.import_data_range_bytes IS NOT NULL OR NEW.import_data_range_bytes IS NULL THEN
            RAISE EXCEPTION 'RootFS artifact import geometry is immutable'
                USING ERRCODE = '23514';
        END IF;
        IF ROW(NEW.artifact_digest, NEW.source_oci_ref, NEW.source_oci_digest,
            NEW.manifest_digest, NEW.config_digest, NEW.base_block_root,
            NEW.format_generation, NEW.oci_os, NEW.oci_architecture, NEW.oci_variant,
            NEW.procd_protocol, NEW.procd_digest, NEW.logical_size_bytes,
            NEW.descriptor_digest, NEW.descriptor, NEW.attestation, NEW.state)
            IS DISTINCT FROM ROW(OLD.artifact_digest, OLD.source_oci_ref, OLD.source_oci_digest,
            OLD.manifest_digest, OLD.config_digest, OLD.base_block_root,
            OLD.format_generation, OLD.oci_os, OLD.oci_architecture, OLD.oci_variant,
            OLD.procd_protocol, OLD.procd_digest, OLD.logical_size_bytes,
            OLD.descriptor_digest, OLD.descriptor, OLD.attestation, OLD.state)
            OR NEW.import_data_range_bytes IS DISTINCT FROM
                manager.rootfs_import_geometry_provenance(OLD.artifact_digest) THEN
            RAISE EXCEPTION 'RootFS artifact import geometry lacks unanimous ready-operation provenance'
                USING ERRCODE = '23514';
        END IF;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
-- +goose StatementEnd

CREATE TRIGGER preserve_rootfs_import_geometry
    BEFORE UPDATE OF import_data_range_bytes ON manager.rootfs_base_artifacts
    FOR EACH ROW EXECUTE FUNCTION manager.preserve_rootfs_import_geometry();

-- +goose Down

DROP TRIGGER preserve_rootfs_import_geometry ON manager.rootfs_base_artifacts;
DROP FUNCTION manager.preserve_rootfs_import_geometry();
DROP FUNCTION manager.rootfs_import_geometry_provenance(text);
DROP INDEX manager.idx_rootfs_import_operations_ready_artifact;
ALTER TABLE manager.rootfs_base_artifacts DROP COLUMN import_data_range_bytes;

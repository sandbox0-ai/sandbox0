-- +goose Up

ALTER TABLE manager.rootfs_base_artifacts
    ADD COLUMN IF NOT EXISTS oci_os TEXT,
    ADD COLUMN IF NOT EXISTS oci_architecture TEXT,
    ADD COLUMN IF NOT EXISTS oci_variant TEXT;

CREATE INDEX IF NOT EXISTS idx_rootfs_base_artifacts_source_platform_ready
    ON manager.rootfs_base_artifacts(
        source_oci_digest, oci_os, oci_architecture, oci_variant,
        format_generation DESC, created_at DESC
    )
    WHERE state = 'ready'
        AND oci_os IS NOT NULL
        AND oci_architecture IS NOT NULL
        AND oci_variant IS NOT NULL;

-- +goose Down

DROP INDEX IF EXISTS manager.idx_rootfs_base_artifacts_source_platform_ready;

ALTER TABLE manager.rootfs_base_artifacts
    DROP COLUMN IF EXISTS oci_variant,
    DROP COLUMN IF EXISTS oci_architecture,
    DROP COLUMN IF EXISTS oci_os;

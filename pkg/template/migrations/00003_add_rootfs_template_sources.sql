-- +goose Up

ALTER TABLE scheduler_templates
    ADD COLUMN rootfs_storage_format TEXT,
    ADD COLUMN rootfs_snapshot_id TEXT,
    ADD COLUMN rootfs_generation_id TEXT,
    ADD COLUMN rootfs_source_oci_digest TEXT,
    ADD COLUMN rootfs_base_artifact_digest TEXT,
    ADD COLUMN rootfs_format_generation INTEGER,
    ADD COLUMN rootfs_platform_os TEXT,
    ADD COLUMN rootfs_platform_architecture TEXT,
    ADD COLUMN rootfs_platform_variant TEXT,
    ADD CONSTRAINT scheduler_templates_rootfs_source_shape_check CHECK (
        (
            rootfs_storage_format IS NULL
            AND rootfs_snapshot_id IS NULL
            AND rootfs_generation_id IS NULL
            AND rootfs_source_oci_digest IS NULL
            AND rootfs_base_artifact_digest IS NULL
            AND rootfs_format_generation IS NULL
            AND rootfs_platform_os IS NULL
            AND rootfs_platform_architecture IS NULL
            AND rootfs_platform_variant IS NULL
        )
        OR
        (
            rootfs_storage_format = 'block-cow-v1'
            AND rootfs_snapshot_id IS NOT NULL AND rootfs_snapshot_id <> ''
            AND rootfs_generation_id IS NOT NULL AND rootfs_generation_id <> ''
            AND rootfs_source_oci_digest IS NOT NULL AND rootfs_source_oci_digest <> ''
            AND rootfs_base_artifact_digest IS NOT NULL AND rootfs_base_artifact_digest <> ''
            AND rootfs_format_generation > 0
            AND rootfs_platform_os IS NOT NULL AND rootfs_platform_os <> ''
            AND rootfs_platform_architecture IS NOT NULL AND rootfs_platform_architecture <> ''
            AND rootfs_platform_variant IS NOT NULL
        )
    );

CREATE UNIQUE INDEX scheduler_templates_rootfs_snapshot
    ON scheduler_templates (rootfs_snapshot_id)
    WHERE rootfs_snapshot_id IS NOT NULL;

-- Cleanup rows deliberately outlive the visible template. A manager worker
-- removes the regional snapshot before deleting the tombstone.
CREATE TABLE scheduler_template_rootfs_deletions (
    snapshot_id TEXT PRIMARY KEY,
    team_id TEXT NOT NULL,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    next_attempt_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    lease_owner TEXT,
    lease_expires_at TIMESTAMP WITH TIME ZONE,
    last_error TEXT,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

CREATE INDEX scheduler_template_rootfs_deletions_due
    ON scheduler_template_rootfs_deletions (next_attempt_at, created_at);

CREATE TRIGGER update_scheduler_template_rootfs_deletions_updated_at
    BEFORE UPDATE ON scheduler_template_rootfs_deletions
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- +goose Down

DROP TRIGGER IF EXISTS update_scheduler_template_rootfs_deletions_updated_at
    ON scheduler_template_rootfs_deletions;
DROP TABLE IF EXISTS scheduler_template_rootfs_deletions;
DROP INDEX IF EXISTS scheduler_templates_rootfs_snapshot;
ALTER TABLE scheduler_templates
    DROP CONSTRAINT IF EXISTS scheduler_templates_rootfs_source_shape_check,
    DROP COLUMN IF EXISTS rootfs_platform_variant,
    DROP COLUMN IF EXISTS rootfs_platform_architecture,
    DROP COLUMN IF EXISTS rootfs_platform_os,
    DROP COLUMN IF EXISTS rootfs_format_generation,
    DROP COLUMN IF EXISTS rootfs_base_artifact_digest,
    DROP COLUMN IF EXISTS rootfs_source_oci_digest,
    DROP COLUMN IF EXISTS rootfs_generation_id,
    DROP COLUMN IF EXISTS rootfs_snapshot_id,
    DROP COLUMN IF EXISTS rootfs_storage_format;

-- +goose Up

ALTER TABLE scheduler_template_builds
    ADD COLUMN image_manifest_digest TEXT,
    ADD COLUMN image_logical_size_bytes BIGINT,
    ADD COLUMN image_quota_reserved_at TIMESTAMP WITH TIME ZONE,
    ADD COLUMN image_push_started_at TIMESTAMP WITH TIME ZONE,
    ADD CONSTRAINT scheduler_template_builds_image_plan_check CHECK (
        (image_manifest_digest IS NULL
            AND image_logical_size_bytes IS NULL
            AND image_quota_reserved_at IS NULL
            AND image_push_started_at IS NULL)
        OR
        (NULLIF(image_manifest_digest, '') IS NOT NULL
            AND image_logical_size_bytes >= 0
            AND (image_push_started_at IS NULL OR image_quota_reserved_at IS NOT NULL))
    );

ALTER TABLE scheduler_templates
    ADD COLUMN creation_image_logical_size_bytes BIGINT,
    ADD CONSTRAINT scheduler_templates_creation_image_logical_size_bytes_check
        CHECK (creation_image_logical_size_bytes IS NULL OR creation_image_logical_size_bytes >= 0);

ALTER TABLE scheduler_template_image_cleanups
    ADD COLUMN image_logical_size_bytes BIGINT NOT NULL DEFAULT 0,
    ADD CONSTRAINT scheduler_template_image_cleanups_image_logical_size_bytes_check
        CHECK (image_logical_size_bytes >= 0);

-- +goose Down

ALTER TABLE scheduler_template_image_cleanups
    DROP CONSTRAINT IF EXISTS scheduler_template_image_cleanups_image_logical_size_bytes_check,
    DROP COLUMN IF EXISTS image_logical_size_bytes;

ALTER TABLE scheduler_templates
    DROP CONSTRAINT IF EXISTS scheduler_templates_creation_image_logical_size_bytes_check,
    DROP COLUMN IF EXISTS creation_image_logical_size_bytes;

ALTER TABLE scheduler_template_builds
    DROP CONSTRAINT IF EXISTS scheduler_template_builds_image_plan_check,
    DROP COLUMN IF EXISTS image_push_started_at,
    DROP COLUMN IF EXISTS image_quota_reserved_at,
    DROP COLUMN IF EXISTS image_logical_size_bytes,
    DROP COLUMN IF EXISTS image_manifest_digest;

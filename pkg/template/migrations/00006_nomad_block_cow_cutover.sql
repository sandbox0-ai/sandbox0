-- +goose Up

-- Upgrade the last OCI-publication-capable schema without guessing how to
-- translate a build that still owns unpublished mutable state.
-- +goose StatementBegin
DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM scheduler_template_builds
        WHERE stage = 'reconciling'
           OR (
                capture_metadata IS NOT NULL
                AND capture_metadata->>'version' IS DISTINCT FROM '2'
           )
    ) THEN
        RAISE EXCEPTION 'Nomad block-COW cutover requires every template build to have durable version-2 capture state'
            USING ERRCODE = '55000';
    END IF;
    IF EXISTS (
        SELECT 1
        FROM scheduler_templates
        WHERE (spec #>> '{mainContainer,image}') IS NULL
           OR (spec #>> '{mainContainer,image}') !~ '^[^[:space:]@]+@sha256:[0-9a-f]{64}$'
    ) THEN
        RAISE EXCEPTION 'Nomad block-COW cutover requires every template image to be digest-pinned SHA-256 OCI input'
            USING ERRCODE = '55000';
    END IF;
END;
$$;
-- +goose StatementEnd

-- A completed historical publication may retain the old terminal stage even
-- though it no longer owns a build row. Its durable block snapshot remains a
-- valid template source.
UPDATE scheduler_templates
SET creation_stage = 'publishing'
WHERE creation_stage = 'reconciling';

DROP TABLE IF EXISTS scheduler_template_allocations;

ALTER TABLE scheduler_templates
    DROP CONSTRAINT IF EXISTS scheduler_templates_creation_stage_check,
	DROP CONSTRAINT IF EXISTS scheduler_templates_image_digest_check,
    DROP COLUMN IF EXISTS creation_output_image;
ALTER TABLE scheduler_templates
    ADD CONSTRAINT scheduler_templates_creation_stage_check
        CHECK (creation_stage IS NULL OR creation_stage IN ('capturing', 'publishing')),
    ADD CONSTRAINT scheduler_templates_image_digest_check CHECK (
        (spec #>> '{mainContainer,image}') ~ '^[^[:space:]@]+@sha256:[0-9a-f]{64}$'
    );

DROP INDEX IF EXISTS scheduler_template_builds_takeover_claim;
ALTER TABLE scheduler_template_builds
    DROP CONSTRAINT IF EXISTS scheduler_template_builds_stage_check,
    DROP CONSTRAINT IF EXISTS scheduler_template_builds_capture_version_check,
    DROP COLUMN IF EXISTS output_image;
ALTER TABLE scheduler_template_builds
    ADD CONSTRAINT scheduler_template_builds_stage_check
        CHECK (stage IN ('capturing', 'publishing')),
    ADD CONSTRAINT scheduler_template_builds_capture_version_check
        CHECK (capture_metadata IS NULL OR capture_metadata->>'version' = '2');

CREATE INDEX scheduler_template_builds_takeover_claim
    ON scheduler_template_builds(next_attempt_at, created_at)
    WHERE status IN ('queued', 'running')
      AND stage = 'publishing'
      AND cancel_requested_at IS NULL;

-- +goose Down

-- Physical deletion of OCI publication state is intentionally irreversible.
-- +goose StatementBegin
DO $$
BEGIN
    RAISE EXCEPTION 'Nomad block-COW template cutover cannot be rolled back'
        USING ERRCODE = '55000';
END;
$$;
-- +goose StatementEnd

-- +goose Up

ALTER TABLE scheduler_templates
    ADD COLUMN creation_image_cluster_id VARCHAR(255);

CREATE TABLE scheduler_template_image_cleanups (
    cleanup_id UUID PRIMARY KEY,
    template_id VARCHAR(255) NOT NULL,
    scope VARCHAR(32) NOT NULL,
    team_id VARCHAR(255) NOT NULL,
    target_cluster_id VARCHAR(255) NOT NULL,
    output_image TEXT NOT NULL,
    status VARCHAR(32) NOT NULL DEFAULT 'queued',
    attempt_count INTEGER NOT NULL DEFAULT 0,
    next_attempt_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    lease_owner VARCHAR(255),
    lease_expires_at TIMESTAMP WITH TIME ZONE,
    last_error TEXT,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    CONSTRAINT scheduler_template_image_cleanups_scope_check
        CHECK (scope IN ('public', 'team')),
    CONSTRAINT scheduler_template_image_cleanups_status_check
        CHECK (status IN ('queued', 'running')),
    CONSTRAINT scheduler_template_image_cleanups_template_unique
        UNIQUE (scope, team_id, template_id)
);

CREATE INDEX scheduler_template_image_cleanups_claim
    ON scheduler_template_image_cleanups (target_cluster_id, next_attempt_at, created_at)
    WHERE status IN ('queued', 'running');

CREATE TRIGGER update_scheduler_template_image_cleanups_updated_at
    BEFORE UPDATE ON scheduler_template_image_cleanups
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- +goose Down

DROP TRIGGER IF EXISTS update_scheduler_template_image_cleanups_updated_at
    ON scheduler_template_image_cleanups;
DROP TABLE IF EXISTS scheduler_template_image_cleanups;
ALTER TABLE scheduler_templates
    DROP COLUMN IF EXISTS creation_image_cluster_id;

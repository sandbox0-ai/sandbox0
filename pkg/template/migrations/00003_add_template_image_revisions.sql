-- +goose Up

CREATE TABLE scheduler_template_image_revisions (
    revision_id VARCHAR(96) PRIMARY KEY,
    template_id VARCHAR(255) NOT NULL,
    scope VARCHAR(32) NOT NULL,
    team_id VARCHAR(255) NOT NULL DEFAULT '',
    source_image TEXT NOT NULL,
    spec_hash VARCHAR(64) NOT NULL,
    resolved_digest TEXT NOT NULL DEFAULT '',
    platform_os VARCHAR(64) NOT NULL DEFAULT '',
    platform_architecture VARCHAR(64) NOT NULL DEFAULT '',
    platform_variant VARCHAR(64) NOT NULL DEFAULT '',
    image_fs_head_id TEXT,
    oci_config JSONB,
    state VARCHAR(32) NOT NULL DEFAULT 'resolving',
    attempt_count INTEGER NOT NULL DEFAULT 0,
    next_attempt_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    lease_owner VARCHAR(255),
    lease_expires_at TIMESTAMP WITH TIME ZONE,
    reason VARCHAR(128) NOT NULL DEFAULT '',
    message TEXT NOT NULL DEFAULT '',
    started_at TIMESTAMP WITH TIME ZONE,
    completed_at TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    CONSTRAINT scheduler_template_image_revisions_scope_check
        CHECK (scope IN ('public', 'team')),
    CONSTRAINT scheduler_template_image_revisions_state_check
        CHECK (state IN ('resolving', 'importing', 'ready', 'failed')),
    CONSTRAINT scheduler_template_image_revisions_template_fk
        FOREIGN KEY (scope, team_id, template_id)
        REFERENCES scheduler_templates(scope, team_id, template_id)
        ON DELETE CASCADE,
	 UNIQUE (scope, team_id, template_id, spec_hash)
);

ALTER TABLE scheduler_templates
    ADD COLUMN current_image_revision_id VARCHAR(96)
        REFERENCES scheduler_template_image_revisions(revision_id) ON DELETE SET NULL;

CREATE INDEX scheduler_template_image_revisions_claim
    ON scheduler_template_image_revisions(next_attempt_at, created_at)
    WHERE state IN ('resolving', 'importing');

CREATE INDEX scheduler_template_image_revisions_template
    ON scheduler_template_image_revisions(scope, team_id, template_id, created_at DESC);

CREATE TRIGGER update_scheduler_template_image_revisions_updated_at
    BEFORE UPDATE ON scheduler_template_image_revisions
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- +goose Down

DROP TRIGGER IF EXISTS update_scheduler_template_image_revisions_updated_at ON scheduler_template_image_revisions;
DROP INDEX IF EXISTS scheduler_template_image_revisions_template;
DROP INDEX IF EXISTS scheduler_template_image_revisions_claim;
ALTER TABLE scheduler_templates DROP COLUMN IF EXISTS current_image_revision_id;
DROP TABLE scheduler_template_image_revisions;

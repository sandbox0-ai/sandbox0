-- +goose Up

ALTER TABLE scheduler_templates
    ADD COLUMN creation_build_id UUID,
    ADD COLUMN creation_idempotency_key VARCHAR(255),
    ADD COLUMN creation_request_hash VARCHAR(64),
    ADD COLUMN creation_state VARCHAR(32) NOT NULL DEFAULT 'ready',
    ADD COLUMN creation_stage VARCHAR(32),
    ADD COLUMN creation_started_at TIMESTAMP WITH TIME ZONE,
    ADD COLUMN creation_captured_at TIMESTAMP WITH TIME ZONE,
    ADD COLUMN creation_completed_at TIMESTAMP WITH TIME ZONE,
    ADD COLUMN creation_output_image TEXT,
    ADD COLUMN creation_reason VARCHAR(128),
    ADD COLUMN creation_message TEXT,
    ADD CONSTRAINT scheduler_templates_creation_state_check
        CHECK (creation_state IN ('creating', 'ready', 'failed')),
    ADD CONSTRAINT scheduler_templates_creation_stage_check
        CHECK (creation_stage IS NULL OR creation_stage IN ('capturing', 'publishing', 'reconciling'));

CREATE UNIQUE INDEX scheduler_templates_creation_idempotency_key
    ON scheduler_templates (scope, team_id, creation_idempotency_key)
    WHERE creation_idempotency_key IS NOT NULL;

-- Build rows deliberately do not reference scheduler_templates. A DELETE can
-- remove the user-visible template immediately while retaining a cancellation
-- tombstone for a worker that is already publishing.
CREATE TABLE scheduler_template_builds (
    build_id UUID PRIMARY KEY,
    template_id VARCHAR(255) NOT NULL,
    scope VARCHAR(32) NOT NULL,
    team_id VARCHAR(255) NOT NULL,
    user_id VARCHAR(255) NOT NULL DEFAULT '',
    source_sandbox_id VARCHAR(255) NOT NULL,
    target_cluster_id VARCHAR(255) NOT NULL,
    request_hash VARCHAR(64) NOT NULL,
    idempotency_key VARCHAR(255),
    status VARCHAR(32) NOT NULL DEFAULT 'queued',
    stage VARCHAR(32) NOT NULL DEFAULT 'capturing',
    snapshot_id VARCHAR(255),
    capture_metadata JSONB,
    output_image TEXT,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    next_attempt_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    lease_owner VARCHAR(255),
    lease_expires_at TIMESTAMP WITH TIME ZONE,
    cancel_requested_at TIMESTAMP WITH TIME ZONE,
    last_error TEXT,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    CONSTRAINT scheduler_template_builds_scope_check CHECK (scope IN ('public', 'team')),
    CONSTRAINT scheduler_template_builds_status_check CHECK (status IN ('queued', 'running', 'cancelled')),
    CONSTRAINT scheduler_template_builds_stage_check CHECK (stage IN ('capturing', 'publishing', 'reconciling'))
);

CREATE INDEX scheduler_template_builds_capture_claim
    ON scheduler_template_builds (target_cluster_id, next_attempt_at, created_at)
    WHERE status IN ('queued', 'running')
      AND stage = 'capturing'
      AND cancel_requested_at IS NULL;
CREATE INDEX scheduler_template_builds_takeover_claim
    ON scheduler_template_builds (next_attempt_at, created_at)
    WHERE status IN ('queued', 'running')
      AND stage IN ('publishing', 'reconciling')
      AND cancel_requested_at IS NULL;
CREATE INDEX scheduler_template_builds_cleanup
    ON scheduler_template_builds (next_attempt_at, created_at)
    WHERE cancel_requested_at IS NOT NULL;
CREATE INDEX scheduler_template_builds_template
    ON scheduler_template_builds (scope, team_id, template_id);

CREATE TRIGGER update_scheduler_template_builds_updated_at
    BEFORE UPDATE ON scheduler_template_builds
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- +goose Down

DROP TRIGGER IF EXISTS update_scheduler_template_builds_updated_at ON scheduler_template_builds;
DROP TABLE IF EXISTS scheduler_template_builds;
DROP INDEX IF EXISTS scheduler_templates_creation_idempotency_key;
ALTER TABLE scheduler_templates
    DROP CONSTRAINT IF EXISTS scheduler_templates_creation_stage_check,
    DROP CONSTRAINT IF EXISTS scheduler_templates_creation_state_check,
    DROP COLUMN IF EXISTS creation_message,
    DROP COLUMN IF EXISTS creation_reason,
    DROP COLUMN IF EXISTS creation_output_image,
    DROP COLUMN IF EXISTS creation_completed_at,
    DROP COLUMN IF EXISTS creation_captured_at,
    DROP COLUMN IF EXISTS creation_started_at,
    DROP COLUMN IF EXISTS creation_stage,
    DROP COLUMN IF EXISTS creation_state,
    DROP COLUMN IF EXISTS creation_request_hash,
    DROP COLUMN IF EXISTS creation_idempotency_key,
    DROP COLUMN IF EXISTS creation_build_id;

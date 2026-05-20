-- +goose Up
ALTER TABLE function_runtime_instances
    ADD COLUMN IF NOT EXISTS readiness_state TEXT NOT NULL DEFAULT 'unknown',
    ADD COLUMN IF NOT EXISTS startup_duration_ms INTEGER,
    ADD COLUMN IF NOT EXISTS last_error_at TIMESTAMPTZ;

ALTER TABLE function_runtime_instances
    DROP CONSTRAINT IF EXISTS function_runtime_instances_readiness_state_check;
ALTER TABLE function_runtime_instances
    ADD CONSTRAINT function_runtime_instances_readiness_state_check
        CHECK (readiness_state IN ('unknown', 'checking', 'ready', 'failed'));

ALTER TABLE function_runtime_instances
    DROP CONSTRAINT IF EXISTS function_runtime_instances_startup_duration_check;
ALTER TABLE function_runtime_instances
    ADD CONSTRAINT function_runtime_instances_startup_duration_check
        CHECK (startup_duration_ms IS NULL OR startup_duration_ms >= 0);

UPDATE function_runtime_instances
SET readiness_state = CASE
    WHEN state = 'ready' THEN 'ready'
    WHEN state = 'failed' THEN 'failed'
    WHEN state = 'starting' THEN 'checking'
    ELSE readiness_state
END,
last_error_at = CASE
    WHEN state = 'failed' THEN COALESCE(last_error_at, failed_at, updated_at)
    ELSE last_error_at
END
WHERE readiness_state = 'unknown' OR (state = 'failed' AND last_error_at IS NULL);

CREATE TABLE IF NOT EXISTS function_runtime_events (
    id UUID PRIMARY KEY,
    team_id UUID NOT NULL,
    function_id UUID NOT NULL REFERENCES functions(id) ON DELETE CASCADE,
    revision_id UUID NOT NULL REFERENCES functions_revisions(id) ON DELETE CASCADE,
    runtime_instance_id UUID REFERENCES function_runtime_instances(id) ON DELETE SET NULL,
    runtime_sandbox_id TEXT,
    runtime_context_id TEXT,
    phase TEXT NOT NULL,
    readiness_state TEXT NOT NULL DEFAULT 'unknown',
    reason TEXT NOT NULL DEFAULT '',
    message TEXT NOT NULL DEFAULT '',
    startup_duration_ms INTEGER,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT function_runtime_events_phase_check
        CHECK (phase IN ('disabled', 'idle', 'provisioning', 'starting', 'ready', 'draining', 'failed')),
    CONSTRAINT function_runtime_events_readiness_state_check
        CHECK (readiness_state IN ('unknown', 'checking', 'ready', 'failed')),
    CONSTRAINT function_runtime_events_startup_duration_check
        CHECK (startup_duration_ms IS NULL OR startup_duration_ms >= 0)
);

CREATE INDEX IF NOT EXISTS idx_function_runtime_events_revision
    ON function_runtime_events(revision_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_function_runtime_events_function
    ON function_runtime_events(function_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_function_runtime_events_instance
    ON function_runtime_events(runtime_instance_id, created_at DESC)
    WHERE runtime_instance_id IS NOT NULL;

-- +goose Down
DROP INDEX IF EXISTS idx_function_runtime_events_instance;
DROP INDEX IF EXISTS idx_function_runtime_events_function;
DROP INDEX IF EXISTS idx_function_runtime_events_revision;
DROP TABLE IF EXISTS function_runtime_events;
ALTER TABLE function_runtime_instances
    DROP CONSTRAINT IF EXISTS function_runtime_instances_startup_duration_check;
ALTER TABLE function_runtime_instances
    DROP CONSTRAINT IF EXISTS function_runtime_instances_readiness_state_check;
ALTER TABLE function_runtime_instances
    DROP COLUMN IF EXISTS last_error_at;
ALTER TABLE function_runtime_instances
    DROP COLUMN IF EXISTS startup_duration_ms;
ALTER TABLE function_runtime_instances
    DROP COLUMN IF EXISTS readiness_state;

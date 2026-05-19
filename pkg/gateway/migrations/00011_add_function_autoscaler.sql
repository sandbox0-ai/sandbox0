-- +goose Up
ALTER TABLE functions
    ADD COLUMN IF NOT EXISTS autoscaling JSONB NOT NULL DEFAULT '{
        "min_warm": 0,
        "max_active": 20,
        "target_concurrency": 80,
        "scale_down_after_seconds": 300
    }'::jsonb;

CREATE TABLE IF NOT EXISTS function_runtime_instances (
    id UUID PRIMARY KEY,
    team_id UUID NOT NULL,
    function_id UUID NOT NULL REFERENCES functions(id) ON DELETE CASCADE,
    revision_id UUID NOT NULL REFERENCES functions_revisions(id) ON DELETE CASCADE,
    sandbox_id TEXT NOT NULL,
    context_id TEXT,
    state TEXT NOT NULL,
    last_error TEXT,
    ready_at TIMESTAMPTZ,
    last_used_at TIMESTAMPTZ,
    draining_at TIMESTAMPTZ,
    failed_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(revision_id, sandbox_id),
    CONSTRAINT function_runtime_instances_state_check
        CHECK (state IN ('starting', 'ready', 'draining', 'failed'))
);

CREATE INDEX IF NOT EXISTS idx_function_runtime_instances_revision
    ON function_runtime_instances(revision_id, state, updated_at);
CREATE INDEX IF NOT EXISTS idx_function_runtime_instances_function
    ON function_runtime_instances(function_id, state, updated_at);
CREATE INDEX IF NOT EXISTS idx_function_runtime_instances_team
    ON function_runtime_instances(team_id, state, updated_at);
CREATE INDEX IF NOT EXISTS idx_function_runtime_instances_idle
    ON function_runtime_instances(revision_id, last_used_at)
    WHERE state = 'ready';

DROP TRIGGER IF EXISTS update_function_runtime_instances_updated_at ON function_runtime_instances;
CREATE TRIGGER update_function_runtime_instances_updated_at
    BEFORE UPDATE ON function_runtime_instances
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

INSERT INTO function_runtime_instances (
    id, team_id, function_id, revision_id, sandbox_id, context_id, state, ready_at, last_used_at, created_at, updated_at
)
SELECT gen_random_uuid(), team_id, function_id, id, runtime_sandbox_id, runtime_context_id, 'ready',
    COALESCE(runtime_updated_at, NOW()), runtime_updated_at, COALESCE(runtime_updated_at, NOW()), COALESCE(runtime_updated_at, NOW())
FROM functions_revisions
WHERE runtime_sandbox_id IS NOT NULL AND btrim(runtime_sandbox_id) <> ''
ON CONFLICT (revision_id, sandbox_id) DO NOTHING;

-- +goose Down
DROP TRIGGER IF EXISTS update_function_runtime_instances_updated_at ON function_runtime_instances;
DROP INDEX IF EXISTS idx_function_runtime_instances_idle;
DROP INDEX IF EXISTS idx_function_runtime_instances_team;
DROP INDEX IF EXISTS idx_function_runtime_instances_function;
DROP INDEX IF EXISTS idx_function_runtime_instances_revision;
DROP TABLE IF EXISTS function_runtime_instances;
ALTER TABLE functions
    DROP COLUMN IF EXISTS autoscaling;

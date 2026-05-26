-- +goose Up
CREATE TABLE IF NOT EXISTS functions (
    id UUID PRIMARY KEY,
    team_id UUID NOT NULL,
    created_by UUID,
    name TEXT NOT NULL,
    slug TEXT NOT NULL,
    domain_label TEXT NOT NULL UNIQUE,
    active_revision_id UUID,
    enabled BOOLEAN NOT NULL DEFAULT true,
    scale_policy JSONB NOT NULL DEFAULT '{}',
    deleted_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_functions_team_slug_live
    ON functions(team_id, slug)
    WHERE deleted_at IS NULL;

CREATE INDEX IF NOT EXISTS idx_functions_team_created_at
    ON functions(team_id, created_at DESC)
    WHERE deleted_at IS NULL;

CREATE TABLE IF NOT EXISTS function_revisions (
    id UUID PRIMARY KEY,
    function_id UUID NOT NULL REFERENCES functions(id) ON DELETE CASCADE,
    team_id UUID NOT NULL,
    revision_number INTEGER NOT NULL,
    source JSONB NOT NULL DEFAULT '{}',
    spec JSONB NOT NULL,
    status TEXT NOT NULL DEFAULT 'created',
    runtime_sandbox_id TEXT,
    runtime_cluster_id TEXT,
    runtime_context_id TEXT,
    activated_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(function_id, revision_number)
);

CREATE INDEX IF NOT EXISTS idx_function_revisions_function_created_at
    ON function_revisions(function_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_function_revisions_runtime_sandbox_id
    ON function_revisions(runtime_sandbox_id)
    WHERE runtime_sandbox_id IS NOT NULL;

ALTER TABLE functions
    DROP CONSTRAINT IF EXISTS functions_active_revision_id_fkey;

ALTER TABLE functions
    ADD CONSTRAINT functions_active_revision_id_fkey
    FOREIGN KEY (active_revision_id)
    REFERENCES function_revisions(id)
    ON DELETE SET NULL;

DROP TRIGGER IF EXISTS update_functions_updated_at ON functions;
CREATE TRIGGER update_functions_updated_at
    BEFORE UPDATE ON functions
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

DROP TRIGGER IF EXISTS update_function_revisions_updated_at ON function_revisions;
CREATE TRIGGER update_function_revisions_updated_at
    BEFORE UPDATE ON function_revisions
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- +goose Down
DROP TRIGGER IF EXISTS update_function_revisions_updated_at ON function_revisions;
DROP TRIGGER IF EXISTS update_functions_updated_at ON functions;
ALTER TABLE functions DROP CONSTRAINT IF EXISTS functions_active_revision_id_fkey;
DROP INDEX IF EXISTS idx_function_revisions_runtime_sandbox_id;
DROP INDEX IF EXISTS idx_function_revisions_function_created_at;
DROP INDEX IF EXISTS idx_functions_team_created_at;
DROP INDEX IF EXISTS idx_functions_team_slug_live;
DROP TABLE IF EXISTS function_revisions;
DROP TABLE IF EXISTS functions;

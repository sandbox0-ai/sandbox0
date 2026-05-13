-- +goose Up
-- Region-local function registry. Function runtime execution is handled by
-- function-gateway; this registry owns function names, immutable revisions, and
-- mutable aliases such as production.

CREATE TABLE IF NOT EXISTS functions (
    id UUID PRIMARY KEY,
    team_id UUID NOT NULL,
    name TEXT NOT NULL,
    slug TEXT NOT NULL,
    domain_label TEXT NOT NULL,
    active_revision_id UUID,
    created_by UUID,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(team_id, slug),
    UNIQUE(domain_label)
);

CREATE INDEX IF NOT EXISTS idx_functions_team_id ON functions(team_id);
CREATE INDEX IF NOT EXISTS idx_functions_active_revision_id ON functions(active_revision_id);

CREATE TABLE IF NOT EXISTS functions_revisions (
    id UUID PRIMARY KEY,
    function_id UUID NOT NULL REFERENCES functions(id) ON DELETE CASCADE,
    team_id UUID NOT NULL,
    revision_number INTEGER NOT NULL,
    source_sandbox_id TEXT NOT NULL,
    source_service_id TEXT NOT NULL,
    source_template_id TEXT NOT NULL,
    service_snapshot JSONB NOT NULL,
    created_by UUID,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(function_id, revision_number)
);

CREATE INDEX IF NOT EXISTS idx_functions_revisions_function_id ON functions_revisions(function_id);
CREATE INDEX IF NOT EXISTS idx_functions_revisions_team_id ON functions_revisions(team_id);

CREATE TABLE IF NOT EXISTS functions_aliases (
    function_id UUID NOT NULL REFERENCES functions(id) ON DELETE CASCADE,
    alias TEXT NOT NULL,
    revision_id UUID NOT NULL REFERENCES functions_revisions(id) ON DELETE CASCADE,
    updated_by UUID,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY(function_id, alias)
);

ALTER TABLE functions
    ADD CONSTRAINT functions_active_revision_id_fkey
    FOREIGN KEY (active_revision_id) REFERENCES functions_revisions(id) ON DELETE SET NULL;

DROP TRIGGER IF EXISTS update_functions_updated_at ON functions;
CREATE TRIGGER update_functions_updated_at
    BEFORE UPDATE ON functions
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- +goose Down
DROP TRIGGER IF EXISTS update_functions_updated_at ON functions;
ALTER TABLE functions DROP CONSTRAINT IF EXISTS functions_active_revision_id_fkey;
DROP TABLE IF EXISTS functions_aliases;
DROP TABLE IF EXISTS functions_revisions;
DROP INDEX IF EXISTS idx_functions_active_revision_id;
DROP INDEX IF EXISTS idx_functions_team_id;
DROP TABLE IF EXISTS functions;

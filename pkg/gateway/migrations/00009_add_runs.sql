-- +goose Up
CREATE TABLE IF NOT EXISTS runs (
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

CREATE UNIQUE INDEX IF NOT EXISTS idx_runs_team_slug_live
    ON runs(team_id, slug)
    WHERE deleted_at IS NULL;

CREATE INDEX IF NOT EXISTS idx_runs_team_created_at
    ON runs(team_id, created_at DESC)
    WHERE deleted_at IS NULL;

CREATE TABLE IF NOT EXISTS run_revisions (
    id UUID PRIMARY KEY,
    run_id UUID NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
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
    UNIQUE(run_id, revision_number)
);

CREATE INDEX IF NOT EXISTS idx_run_revisions_run_created_at
    ON run_revisions(run_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_run_revisions_runtime_sandbox_id
    ON run_revisions(runtime_sandbox_id)
    WHERE runtime_sandbox_id IS NOT NULL;

ALTER TABLE runs
    DROP CONSTRAINT IF EXISTS runs_active_revision_id_fkey;

ALTER TABLE runs
    ADD CONSTRAINT runs_active_revision_id_fkey
    FOREIGN KEY (active_revision_id)
    REFERENCES run_revisions(id)
    ON DELETE SET NULL;

DROP TRIGGER IF EXISTS update_runs_updated_at ON runs;
CREATE TRIGGER update_runs_updated_at
    BEFORE UPDATE ON runs
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

DROP TRIGGER IF EXISTS update_run_revisions_updated_at ON run_revisions;
CREATE TRIGGER update_run_revisions_updated_at
    BEFORE UPDATE ON run_revisions
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- +goose Down
DROP TRIGGER IF EXISTS update_run_revisions_updated_at ON run_revisions;
DROP TRIGGER IF EXISTS update_runs_updated_at ON runs;
ALTER TABLE runs DROP CONSTRAINT IF EXISTS runs_active_revision_id_fkey;
DROP INDEX IF EXISTS idx_run_revisions_runtime_sandbox_id;
DROP INDEX IF EXISTS idx_run_revisions_run_created_at;
DROP INDEX IF EXISTS idx_runs_team_created_at;
DROP INDEX IF EXISTS idx_runs_team_slug_live;
DROP TABLE IF EXISTS run_revisions;
DROP TABLE IF EXISTS runs;

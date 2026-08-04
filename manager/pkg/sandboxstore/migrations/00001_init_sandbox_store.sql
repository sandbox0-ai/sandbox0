-- +goose Up

CREATE TABLE IF NOT EXISTS sandboxes (
    sandbox_id TEXT PRIMARY KEY,
    team_id TEXT NOT NULL,
    user_id TEXT NOT NULL DEFAULT '',
    template_id TEXT NOT NULL,
    template_name TEXT NOT NULL,
    template_namespace TEXT NOT NULL,
    cluster_id TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL,
    config JSONB NOT NULL DEFAULT '{}',
    mounts JSONB NOT NULL DEFAULT '[]',
    template_spec JSONB NOT NULL DEFAULT '{}',
    current_pod_name TEXT NOT NULL DEFAULT '',
    current_pod_namespace TEXT NOT NULL DEFAULT '',
    runtime_generation BIGINT NOT NULL DEFAULT 0,
    claimed_at TIMESTAMPTZ,
    expires_at TIMESTAMPTZ,
    hard_expires_at TIMESTAMPTZ,
    deleted_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_sandboxes_team_updated
    ON sandboxes(team_id, updated_at DESC);

CREATE INDEX IF NOT EXISTS idx_sandboxes_team_status
    ON sandboxes(team_id, status);

CREATE INDEX IF NOT EXISTS idx_sandboxes_current_pod
    ON sandboxes(current_pod_namespace, current_pod_name)
    WHERE current_pod_name <> '';

-- +goose Down

DROP INDEX IF EXISTS idx_sandboxes_current_pod;
DROP INDEX IF EXISTS idx_sandboxes_team_status;
DROP INDEX IF EXISTS idx_sandboxes_team_updated;
DROP TABLE IF EXISTS sandboxes;

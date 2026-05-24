-- +goose Up
CREATE TABLE IF NOT EXISTS artifacts (
    id TEXT PRIMARY KEY,
    team_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    name TEXT NOT NULL,
    kind TEXT NOT NULL DEFAULT 'generic',
    media_type TEXT NOT NULL DEFAULT 'application/octet-stream',
    digest TEXT,
    source_volume_id TEXT NOT NULL REFERENCES sandbox_volumes(id) ON DELETE RESTRICT,
    snapshot_id TEXT NOT NULL REFERENCES sandbox_volume_snapshots(id) ON DELETE RESTRICT,
    size_bytes BIGINT NOT NULL DEFAULT 0,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_artifacts_team_id ON artifacts(team_id);
CREATE INDEX IF NOT EXISTS idx_artifacts_user_id ON artifacts(user_id);
CREATE INDEX IF NOT EXISTS idx_artifacts_snapshot_id ON artifacts(snapshot_id);

DROP TRIGGER IF EXISTS update_artifacts_updated_at ON artifacts;
CREATE TRIGGER update_artifacts_updated_at
    BEFORE UPDATE ON artifacts
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- +goose Down
DROP TRIGGER IF EXISTS update_artifacts_updated_at ON artifacts;
DROP INDEX IF EXISTS idx_artifacts_snapshot_id;
DROP INDEX IF EXISTS idx_artifacts_user_id;
DROP INDEX IF EXISTS idx_artifacts_team_id;
DROP TABLE IF EXISTS artifacts;

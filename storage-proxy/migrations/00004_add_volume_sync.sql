-- +goose Up
CREATE TABLE IF NOT EXISTS sandbox_volume_sync_replicas (
    volume_id TEXT NOT NULL REFERENCES sandbox_volumes(id) ON DELETE CASCADE,
    id TEXT NOT NULL,
    team_id TEXT NOT NULL,
    display_name TEXT NOT NULL DEFAULT '',
    platform TEXT NOT NULL DEFAULT '',
    root_path TEXT NOT NULL DEFAULT '',
    capabilities JSONB NOT NULL DEFAULT '{}'::jsonb,
    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_applied_seq BIGINT NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (volume_id, id)
);

CREATE INDEX IF NOT EXISTS idx_sync_replicas_team_id ON sandbox_volume_sync_replicas(team_id);

DROP TRIGGER IF EXISTS update_sync_replicas_updated_at ON sandbox_volume_sync_replicas;
CREATE TRIGGER update_sync_replicas_updated_at
    BEFORE UPDATE ON sandbox_volume_sync_replicas
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

CREATE TABLE IF NOT EXISTS sandbox_volume_sync_journal (
    seq BIGSERIAL PRIMARY KEY,
    volume_id TEXT NOT NULL REFERENCES sandbox_volumes(id) ON DELETE CASCADE,
    team_id TEXT NOT NULL,
    source TEXT NOT NULL,
    replica_id TEXT,
    event_type TEXT NOT NULL,
    path TEXT NOT NULL DEFAULT '',
    normalized_path TEXT NOT NULL DEFAULT '',
    old_path TEXT,
    normalized_old_path TEXT,
    tombstone BOOLEAN NOT NULL DEFAULT false,
    content_sha256 TEXT,
    size_bytes BIGINT,
    metadata JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_sync_journal_volume_seq ON sandbox_volume_sync_journal(volume_id, seq);
CREATE INDEX IF NOT EXISTS idx_sync_journal_volume_norm_path_seq ON sandbox_volume_sync_journal(volume_id, normalized_path, seq DESC);
CREATE INDEX IF NOT EXISTS idx_sync_journal_volume_old_norm_path_seq ON sandbox_volume_sync_journal(volume_id, normalized_old_path, seq DESC);

CREATE TABLE IF NOT EXISTS sandbox_volume_sync_conflicts (
    id TEXT PRIMARY KEY,
    volume_id TEXT NOT NULL REFERENCES sandbox_volumes(id) ON DELETE CASCADE,
    team_id TEXT NOT NULL,
    replica_id TEXT,
    path TEXT NOT NULL,
    normalized_path TEXT NOT NULL,
    artifact_path TEXT NOT NULL DEFAULT '',
    incoming_path TEXT,
    incoming_old_path TEXT,
    existing_seq BIGINT,
    reason TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'open',
    metadata JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_sync_conflicts_volume_id ON sandbox_volume_sync_conflicts(volume_id);
CREATE INDEX IF NOT EXISTS idx_sync_conflicts_replica_id ON sandbox_volume_sync_conflicts(replica_id);

DROP TRIGGER IF EXISTS update_sync_conflicts_updated_at ON sandbox_volume_sync_conflicts;
CREATE TRIGGER update_sync_conflicts_updated_at
    BEFORE UPDATE ON sandbox_volume_sync_conflicts
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

CREATE TABLE IF NOT EXISTS sandbox_volume_sync_requests (
    volume_id TEXT NOT NULL REFERENCES sandbox_volumes(id) ON DELETE CASCADE,
    replica_id TEXT NOT NULL,
    request_id TEXT NOT NULL,
    request_fingerprint TEXT NOT NULL,
    response_payload JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (volume_id, replica_id, request_id)
);

CREATE INDEX IF NOT EXISTS idx_sync_requests_volume_replica_created_at
    ON sandbox_volume_sync_requests(volume_id, replica_id, created_at DESC);

CREATE TABLE IF NOT EXISTS sandbox_volume_sync_retention (
    volume_id TEXT PRIMARY KEY REFERENCES sandbox_volumes(id) ON DELETE CASCADE,
    team_id TEXT NOT NULL,
    compacted_through_seq BIGINT NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_sync_retention_team_id ON sandbox_volume_sync_retention(team_id);

DROP TRIGGER IF EXISTS update_sync_retention_updated_at ON sandbox_volume_sync_retention;
CREATE TRIGGER update_sync_retention_updated_at
    BEFORE UPDATE ON sandbox_volume_sync_retention
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

CREATE TABLE IF NOT EXISTS sandbox_volume_sync_namespace_policy (
    volume_id TEXT PRIMARY KEY REFERENCES sandbox_volumes(id) ON DELETE CASCADE,
    team_id TEXT NOT NULL,
    capabilities JSONB NOT NULL DEFAULT '{}'::jsonb,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_sync_namespace_policy_team_id ON sandbox_volume_sync_namespace_policy(team_id);

DROP TRIGGER IF EXISTS update_sync_namespace_policy_updated_at ON sandbox_volume_sync_namespace_policy;
CREATE TRIGGER update_sync_namespace_policy_updated_at
    BEFORE UPDATE ON sandbox_volume_sync_namespace_policy
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- +goose Down
DROP TRIGGER IF EXISTS update_sync_retention_updated_at ON sandbox_volume_sync_retention;
DROP INDEX IF EXISTS idx_sync_retention_team_id;
DROP TABLE IF EXISTS sandbox_volume_sync_retention;
DROP TRIGGER IF EXISTS update_sync_namespace_policy_updated_at ON sandbox_volume_sync_namespace_policy;
DROP INDEX IF EXISTS idx_sync_namespace_policy_team_id;
DROP TABLE IF EXISTS sandbox_volume_sync_namespace_policy;
DROP INDEX IF EXISTS idx_sync_requests_volume_replica_created_at;
DROP TABLE IF EXISTS sandbox_volume_sync_requests;
DROP TRIGGER IF EXISTS update_sync_conflicts_updated_at ON sandbox_volume_sync_conflicts;
DROP TRIGGER IF EXISTS update_sync_replicas_updated_at ON sandbox_volume_sync_replicas;
DROP INDEX IF EXISTS idx_sync_conflicts_replica_id;
DROP INDEX IF EXISTS idx_sync_conflicts_volume_id;
DROP TABLE IF EXISTS sandbox_volume_sync_conflicts;
DROP INDEX IF EXISTS idx_sync_journal_volume_old_norm_path_seq;
DROP INDEX IF EXISTS idx_sync_journal_volume_norm_path_seq;
DROP INDEX IF EXISTS idx_sync_journal_volume_seq;
DROP TABLE IF EXISTS sandbox_volume_sync_journal;
DROP INDEX IF EXISTS idx_sync_replicas_team_id;
DROP TABLE IF EXISTS sandbox_volume_sync_replicas;

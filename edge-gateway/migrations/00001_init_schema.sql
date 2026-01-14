-- +goose Up
-- Internal Gateway Database Schema
-- This schema is used by internal-gateway for authentication and authorization

-- API Keys table
CREATE TABLE IF NOT EXISTS api_keys (
    id TEXT PRIMARY KEY,
    key_value TEXT NOT NULL UNIQUE,
    team_id TEXT NOT NULL REFERENCES teams(id) ON DELETE CASCADE,
    created_by TEXT NOT NULL REFERENCES users(id) ON DELETE SET NULL,
    name TEXT NOT NULL,
    type TEXT NOT NULL CHECK (type IN ('user', 'service', 'internal')),
    roles JSONB NOT NULL DEFAULT '[]',
    is_active BOOLEAN NOT NULL DEFAULT true,
    expires_at TIMESTAMPTZ NOT NULL,
    last_used_at TIMESTAMPTZ,
    usage_count BIGINT DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_api_keys_team_id ON api_keys(team_id);
CREATE INDEX IF NOT EXISTS idx_api_keys_key_value ON api_keys(key_value);

-- Updated_at trigger
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS update_api_keys_updated_at ON api_keys;
CREATE TRIGGER update_api_keys_updated_at
    BEFORE UPDATE ON api_keys
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- +goose Down
-- ignore

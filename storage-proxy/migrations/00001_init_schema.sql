-- +goose Up
-- Storage Proxy Database Schema

-- Sandbox Volumes table
CREATE TABLE IF NOT EXISTS sandbox_volumes (
    id TEXT PRIMARY KEY,
    team_id TEXT NOT NULL,
    user_id TEXT NOT NULL,

    access_mode TEXT NOT NULL DEFAULT 'RWO',
    
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_sandbox_volumes_team_id ON sandbox_volumes(team_id);
CREATE INDEX IF NOT EXISTS idx_sandbox_volumes_user_id ON sandbox_volumes(user_id);

-- Updated_at trigger
-- +goose StatementBegin
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
-- +goose StatementEnd

-- Apply updated_at trigger
DROP TRIGGER IF EXISTS update_sandbox_volumes_updated_at ON sandbox_volumes;
CREATE TRIGGER update_sandbox_volumes_updated_at
    BEFORE UPDATE ON sandbox_volumes
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- +goose Down
DROP TRIGGER IF EXISTS update_sandbox_volumes_updated_at ON sandbox_volumes;
DROP INDEX IF EXISTS idx_sandbox_volumes_user_id;
DROP INDEX IF EXISTS idx_sandbox_volumes_team_id;
DROP TABLE IF EXISTS sandbox_volumes;
DROP FUNCTION IF EXISTS update_updated_at_column();

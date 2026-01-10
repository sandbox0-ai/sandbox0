-- +goose Up
-- Storage Proxy Database Schema

-- Sandbox Volumes table
CREATE TABLE IF NOT EXISTS sandbox_volumes (
    id TEXT PRIMARY KEY,
    team_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    cluster_id TEXT NOT NULL DEFAULT 'default',
    
    -- Volume Configuration
    cache_size TEXT NOT NULL DEFAULT '1G',
    prefetch INTEGER NOT NULL DEFAULT 0,
    buffer_size TEXT NOT NULL DEFAULT '32M',
    writeback BOOLEAN NOT NULL DEFAULT false,
    read_only BOOLEAN NOT NULL DEFAULT false,
    
    is_active BOOLEAN NOT NULL DEFAULT true,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_sandbox_volumes_team_id ON sandbox_volumes(team_id);
CREATE INDEX IF NOT EXISTS idx_sandbox_volumes_user_id ON sandbox_volumes(user_id);

-- Updated_at trigger
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Apply updated_at trigger
DROP TRIGGER IF EXISTS update_sandbox_volumes_updated_at ON sandbox_volumes;
CREATE TRIGGER update_sandbox_volumes_updated_at
    BEFORE UPDATE ON sandbox_volumes
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- +goose Down
-- ignore

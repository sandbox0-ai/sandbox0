-- +goose Up
CREATE TABLE IF NOT EXISTS device_auth_sessions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    provider VARCHAR(100) NOT NULL,
    device_code TEXT NOT NULL,
    user_code VARCHAR(100) NOT NULL,
    verification_uri TEXT NOT NULL,
    verification_uri_complete TEXT,
    interval_seconds INTEGER NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL,
    consumed_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_device_auth_sessions_expires_at ON device_auth_sessions(expires_at);
CREATE INDEX IF NOT EXISTS idx_device_auth_sessions_consumed_at ON device_auth_sessions(consumed_at);

DROP TRIGGER IF EXISTS update_device_auth_sessions_updated_at ON device_auth_sessions;
CREATE TRIGGER update_device_auth_sessions_updated_at
    BEFORE UPDATE ON device_auth_sessions
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- +goose Down
DROP TRIGGER IF EXISTS update_device_auth_sessions_updated_at ON device_auth_sessions;
DROP INDEX IF EXISTS idx_device_auth_sessions_consumed_at;
DROP INDEX IF EXISTS idx_device_auth_sessions_expires_at;
DROP TABLE IF EXISTS device_auth_sessions;

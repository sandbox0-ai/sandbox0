-- +goose Up
CREATE TABLE IF NOT EXISTS user_ssh_public_keys (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    name TEXT NOT NULL,
    public_key TEXT NOT NULL,
    key_type TEXT NOT NULL,
    fingerprint_sha256 TEXT NOT NULL,
    comment TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(fingerprint_sha256)
);

CREATE INDEX IF NOT EXISTS idx_user_ssh_public_keys_user_id ON user_ssh_public_keys(user_id);

DROP TRIGGER IF EXISTS update_user_ssh_public_keys_updated_at ON user_ssh_public_keys;
CREATE TRIGGER update_user_ssh_public_keys_updated_at
    BEFORE UPDATE ON user_ssh_public_keys
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- +goose Down
DROP TRIGGER IF EXISTS update_user_ssh_public_keys_updated_at ON user_ssh_public_keys;
DROP INDEX IF EXISTS idx_user_ssh_public_keys_user_id;
DROP TABLE IF EXISTS user_ssh_public_keys;

-- +goose Up
CREATE TABLE IF NOT EXISTS web_login_codes (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    code_hash TEXT NOT NULL UNIQUE,
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    return_url TEXT NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL,
    consumed_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_web_login_codes_user_id ON web_login_codes(user_id);
CREATE INDEX IF NOT EXISTS idx_web_login_codes_expires_at ON web_login_codes(expires_at);
CREATE INDEX IF NOT EXISTS idx_web_login_codes_consumed_at ON web_login_codes(consumed_at);

-- +goose Down
DROP INDEX IF EXISTS idx_web_login_codes_consumed_at;
DROP INDEX IF EXISTS idx_web_login_codes_expires_at;
DROP INDEX IF EXISTS idx_web_login_codes_user_id;
DROP TABLE IF EXISTS web_login_codes;

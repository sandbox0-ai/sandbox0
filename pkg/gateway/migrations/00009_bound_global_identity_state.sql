-- +goose Up
CREATE INDEX IF NOT EXISTS idx_teams_owner_created
    ON teams(owner_id, created_at, id);

CREATE INDEX IF NOT EXISTS idx_team_members_user_joined
    ON team_members(user_id, joined_at, id);

CREATE INDEX IF NOT EXISTS idx_team_members_team_joined
    ON team_members(team_id, joined_at, id);

CREATE INDEX IF NOT EXISTS idx_user_identities_user_created
    ON user_identities(user_id, created_at, id);

CREATE INDEX IF NOT EXISTS idx_refresh_tokens_user_lifecycle
    ON refresh_tokens(user_id, revoked, expires_at, created_at, id);

CREATE INDEX IF NOT EXISTS idx_web_login_codes_user_lifecycle
    ON web_login_codes(user_id, consumed_at, expires_at, created_at, id);

CREATE INDEX IF NOT EXISTS idx_device_auth_sessions_lifecycle
    ON device_auth_sessions(consumed_at, expires_at, created_at, id);

CREATE TABLE IF NOT EXISTS oidc_pending_states (
    state_hash TEXT PRIMARY KEY,
    provider TEXT NOT NULL,
    code_verifier TEXT NOT NULL,
    return_url TEXT NOT NULL,
    web_login_handoff BOOLEAN NOT NULL DEFAULT false,
    expires_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT oidc_pending_states_state_hash_size_guard
        CHECK (octet_length(state_hash) BETWEEN 1 AND 128),
    CONSTRAINT oidc_pending_states_provider_size_guard
        CHECK (octet_length(provider) BETWEEN 1 AND 256),
    CONSTRAINT oidc_pending_states_code_verifier_size_guard
        CHECK (octet_length(code_verifier) BETWEEN 1 AND 1024),
    CONSTRAINT oidc_pending_states_return_url_size_guard
        CHECK (octet_length(return_url) <= 8192),
    CONSTRAINT oidc_pending_states_expiry_guard
        CHECK (expires_at > created_at)
);

CREATE INDEX IF NOT EXISTS idx_oidc_pending_states_lifecycle
    ON oidc_pending_states(expires_at, created_at, state_hash);

ALTER TABLE users
    ADD CONSTRAINT users_avatar_url_size_guard
    CHECK (avatar_url IS NULL OR octet_length(avatar_url) <= 8192)
    NOT VALID;

ALTER TABLE user_identities
    ADD CONSTRAINT user_identities_raw_claims_size_guard
    CHECK (raw_claims IS NULL OR octet_length(raw_claims::text) <= 262144)
    NOT VALID;

ALTER TABLE web_login_codes
    ADD CONSTRAINT web_login_codes_return_url_size_guard
    CHECK (octet_length(return_url) <= 8192)
    NOT VALID;

ALTER TABLE device_auth_sessions
    ADD CONSTRAINT device_auth_sessions_device_code_size_guard
    CHECK (octet_length(device_code) <= 16384)
    NOT VALID;

ALTER TABLE device_auth_sessions
    ADD CONSTRAINT device_auth_sessions_verification_uri_size_guard
    CHECK (octet_length(verification_uri) <= 8192)
    NOT VALID;

ALTER TABLE device_auth_sessions
    ADD CONSTRAINT device_auth_sessions_verification_uri_complete_size_guard
    CHECK (
        verification_uri_complete IS NULL
        OR octet_length(verification_uri_complete) <= 8192
    )
    NOT VALID;

-- +goose Down
ALTER TABLE device_auth_sessions
    DROP CONSTRAINT IF EXISTS device_auth_sessions_verification_uri_complete_size_guard;
ALTER TABLE device_auth_sessions
    DROP CONSTRAINT IF EXISTS device_auth_sessions_verification_uri_size_guard;
ALTER TABLE device_auth_sessions
    DROP CONSTRAINT IF EXISTS device_auth_sessions_device_code_size_guard;
ALTER TABLE web_login_codes
    DROP CONSTRAINT IF EXISTS web_login_codes_return_url_size_guard;
ALTER TABLE user_identities
    DROP CONSTRAINT IF EXISTS user_identities_raw_claims_size_guard;
ALTER TABLE users
    DROP CONSTRAINT IF EXISTS users_avatar_url_size_guard;

DROP INDEX IF EXISTS idx_oidc_pending_states_lifecycle;
DROP TABLE IF EXISTS oidc_pending_states;

DROP INDEX IF EXISTS idx_device_auth_sessions_lifecycle;
DROP INDEX IF EXISTS idx_web_login_codes_user_lifecycle;
DROP INDEX IF EXISTS idx_refresh_tokens_user_lifecycle;
DROP INDEX IF EXISTS idx_user_identities_user_created;
DROP INDEX IF EXISTS idx_team_members_team_joined;
DROP INDEX IF EXISTS idx_team_members_user_joined;
DROP INDEX IF EXISTS idx_teams_owner_created;

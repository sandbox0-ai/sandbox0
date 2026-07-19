-- +goose Up

CREATE TABLE region_state_identity_claims (
    singleton BOOLEAN PRIMARY KEY DEFAULT TRUE,
    region_id TEXT NOT NULL,
    state_id UUID NOT NULL,
    endpoint TEXT NOT NULL,
    redis_db INTEGER NOT NULL,
    tls_enabled BOOLEAN NOT NULL,
    key_prefix TEXT NOT NULL,
    fingerprint TEXT NOT NULL,
    claimed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CHECK (singleton),
    CHECK (BTRIM(region_id) <> ''),
    CHECK (state_id <> '00000000-0000-0000-0000-000000000000'::UUID),
    CHECK (redis_db >= 0),
    CHECK (BTRIM(endpoint) <> ''),
    CHECK (BTRIM(key_prefix) <> ''),
    CHECK (fingerprint ~ '^[0-9a-f]{64}$')
);

-- +goose Down

DROP TABLE IF EXISTS region_state_identity_claims;

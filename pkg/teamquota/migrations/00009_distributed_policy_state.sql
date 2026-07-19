-- +goose Up

CREATE TABLE policy_state (
    singleton BOOLEAN PRIMARY KEY DEFAULT TRUE,
    enforcement_epoch BIGINT NOT NULL DEFAULT 1,
    redis_generation BIGINT NOT NULL DEFAULT 0,
    redis_initialized BOOLEAN NOT NULL DEFAULT FALSE,
    redis_run_id TEXT,
    redis_evicted_keys BIGINT,
    redis_reset_at TIMESTAMPTZ,
    rate_refill_from TIMESTAMPTZ,
    defaults_owner_epoch TIMESTAMPTZ,
    defaults_generation BIGINT,
    defaults_sha256 TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CHECK (singleton),
    CHECK (enforcement_epoch > 0),
    CHECK (redis_generation >= 0),
    CHECK (
        (
            redis_initialized
            AND redis_generation > 0
            AND redis_run_id IS NOT NULL
            AND BTRIM(redis_run_id) <> ''
            AND redis_evicted_keys IS NOT NULL
            AND redis_evicted_keys >= 0
        )
        OR
        (
            NOT redis_initialized
            AND redis_generation = 0
            AND redis_run_id IS NULL
            AND redis_evicted_keys IS NULL
        )
    ),
    CONSTRAINT policy_state_rate_refill_requires_reset CHECK (
        rate_refill_from IS NULL OR redis_reset_at IS NOT NULL
    ),
    CHECK (
        (defaults_owner_epoch IS NULL AND defaults_generation IS NULL AND defaults_sha256 IS NULL)
        OR
        (
            defaults_owner_epoch IS NOT NULL
            AND defaults_generation IS NOT NULL
            AND defaults_generation > 0
            AND defaults_sha256 ~ '^[0-9a-f]{64}$'
        )
    )
);

INSERT INTO policy_state (singleton)
VALUES (TRUE);

-- +goose Down

DROP TABLE IF EXISTS policy_state;

-- +goose Up

CREATE TABLE IF NOT EXISTS storage_projection_state (
    subject_type TEXT NOT NULL,
    subject_id TEXT NOT NULL,
    product TEXT NOT NULL DEFAULT 'sandbox',
    owner_kind TEXT NOT NULL DEFAULT '',
    team_id TEXT NOT NULL DEFAULT '',
    user_id TEXT NOT NULL DEFAULT '',
    sandbox_id TEXT,
    volume_id TEXT,
    snapshot_id TEXT,
    cluster_id TEXT,
    region_id TEXT NOT NULL DEFAULT '',
    size_bytes BIGINT NOT NULL DEFAULT 0,
    observed_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (subject_type, subject_id)
);

CREATE INDEX IF NOT EXISTS idx_storage_projection_state_observed_at
    ON storage_projection_state(observed_at);

CREATE INDEX IF NOT EXISTS idx_storage_projection_state_product
    ON storage_projection_state(product);

CREATE INDEX IF NOT EXISTS idx_storage_projection_state_team_id
    ON storage_projection_state(team_id);

-- +goose Down

DROP INDEX IF EXISTS idx_storage_projection_state_team_id;
DROP INDEX IF EXISTS idx_storage_projection_state_product;
DROP INDEX IF EXISTS idx_storage_projection_state_observed_at;
DROP TABLE IF EXISTS storage_projection_state;

-- +goose Up

CREATE TABLE IF NOT EXISTS manager_sandbox_projection_state (
    sandbox_id TEXT PRIMARY KEY,
    namespace TEXT NOT NULL,
    team_id TEXT NOT NULL DEFAULT '',
    user_id TEXT NOT NULL DEFAULT '',
    template_id TEXT NOT NULL DEFAULT '',
    cluster_id TEXT NOT NULL DEFAULT '',
    owner_kind TEXT NOT NULL DEFAULT '',
    resource_millicpu BIGINT NOT NULL DEFAULT 0,
    resource_memory_mib BIGINT NOT NULL DEFAULT 0,
    claimed_at TIMESTAMPTZ,
    active_since TIMESTAMPTZ,
    paused BOOLEAN NOT NULL DEFAULT FALSE,
    paused_at TIMESTAMPTZ,
    terminated_at TIMESTAMPTZ,
    last_observed_at TIMESTAMPTZ NOT NULL,
    last_resource_version TEXT NOT NULL DEFAULT ''
);

ALTER TABLE manager_sandbox_projection_state
    ADD COLUMN IF NOT EXISTS owner_kind TEXT NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS resource_millicpu BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS resource_memory_mib BIGINT NOT NULL DEFAULT 0;

CREATE INDEX IF NOT EXISTS idx_manager_sandbox_projection_state_observed_at
    ON manager_sandbox_projection_state(last_observed_at);

CREATE INDEX IF NOT EXISTS idx_manager_sandbox_projection_state_owner_kind
    ON manager_sandbox_projection_state(owner_kind);

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
    unbilled_byte_nanoseconds BIGINT NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (subject_type, subject_id)
);

ALTER TABLE storage_projection_state
    ADD COLUMN IF NOT EXISTS unbilled_byte_nanoseconds BIGINT NOT NULL DEFAULT 0;

CREATE INDEX IF NOT EXISTS idx_storage_projection_state_observed_at
    ON storage_projection_state(observed_at);

CREATE INDEX IF NOT EXISTS idx_storage_projection_state_team_id
    ON storage_projection_state(team_id);

CREATE TABLE IF NOT EXISTS projection_bootstrap (
    source TEXT PRIMARY KEY,
    completed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS projection_outbox (
    sequence BIGSERIAL PRIMARY KEY,
    batch_id BIGINT NOT NULL DEFAULT txid_current(),
    operation_type TEXT NOT NULL,
    dedupe_key TEXT NOT NULL,
    payload JSONB NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0,
    available_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    claimed_by TEXT NOT NULL DEFAULT '',
    claim_expires_at TIMESTAMPTZ,
    last_error TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    delivered_at TIMESTAMPTZ,
    UNIQUE (operation_type, dedupe_key)
);

CREATE INDEX IF NOT EXISTS idx_projection_outbox_pending
    ON projection_outbox(sequence)
    WHERE delivered_at IS NULL;

CREATE INDEX IF NOT EXISTS idx_projection_outbox_delivered
    ON projection_outbox(delivered_at)
    WHERE delivered_at IS NOT NULL;

-- +goose Down

DROP INDEX IF EXISTS idx_projection_outbox_delivered;
DROP INDEX IF EXISTS idx_projection_outbox_pending;
DROP TABLE IF EXISTS projection_outbox;
DROP TABLE IF EXISTS projection_bootstrap;

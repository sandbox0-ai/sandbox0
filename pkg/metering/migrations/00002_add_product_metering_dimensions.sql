-- +goose Up

ALTER TABLE manager_sandbox_projection_state
    ADD COLUMN IF NOT EXISTS owner_kind TEXT NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS resource_millicpu BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS resource_memory_mib BIGINT NOT NULL DEFAULT 0;

CREATE INDEX IF NOT EXISTS idx_manager_sandbox_projection_state_owner_kind
    ON manager_sandbox_projection_state(owner_kind);

-- +goose Down

DROP INDEX IF EXISTS idx_manager_sandbox_projection_state_owner_kind;

ALTER TABLE manager_sandbox_projection_state
    DROP COLUMN IF EXISTS resource_memory_mib,
    DROP COLUMN IF EXISTS resource_millicpu,
    DROP COLUMN IF EXISTS owner_kind;

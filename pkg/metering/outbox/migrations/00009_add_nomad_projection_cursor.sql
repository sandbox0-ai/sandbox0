-- +goose Up

ALTER TABLE manager_sandbox_projection_state
    ADD COLUMN source_revision BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN source_lifecycle_epoch BIGINT NOT NULL DEFAULT 0;

-- +goose Down

ALTER TABLE manager_sandbox_projection_state
    DROP COLUMN IF EXISTS source_lifecycle_epoch,
    DROP COLUMN IF EXISTS source_revision;

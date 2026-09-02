-- +goose Up

-- A generation cloned by cross-sandbox restore still contains the source
-- sandbox's procd session identity. Keep that immutable ownership transition
-- with the RootFS head so resume retries and manager failover make the same
-- activation decision. Generations published by a live target runtime retain
-- the default false value and therefore consume the reset requirement.
ALTER TABLE manager.rootfs_generations
    ADD COLUMN IF NOT EXISTS reset_copied_session_state boolean NOT NULL DEFAULT false;

-- +goose Down

ALTER TABLE manager.rootfs_generations
    DROP COLUMN IF EXISTS reset_copied_session_state;

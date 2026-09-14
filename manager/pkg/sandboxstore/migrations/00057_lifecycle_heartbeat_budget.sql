-- +goose Up
ALTER TABLE manager.runtime_node_lifecycle_actions
    ADD COLUMN heartbeat_attempts INTEGER NOT NULL DEFAULT 0
        CHECK (heartbeat_attempts BETWEEN 0 AND 20),
    ADD COLUMN heartbeat_not_before TIMESTAMPTZ;

-- +goose Down
-- Retain consumed provider attempts across rollback and later upgrades.
-- +goose StatementBegin
DO $$
BEGIN
    RAISE EXCEPTION 'Lifecycle heartbeat budgets cannot be rolled back'
        USING ERRCODE = '55000';
END;
$$;
-- +goose StatementEnd

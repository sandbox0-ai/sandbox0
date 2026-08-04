-- +goose Up

ALTER TABLE manager.sandboxes
    ADD COLUMN hot_claim_completed_at TIMESTAMPTZ;

UPDATE manager.sandboxes
SET deleted_at = COALESCE(deleted_at, updated_at)
WHERE status = 'deleted';

UPDATE manager.sandboxes
SET status = CASE
    WHEN deleted_at IS NOT NULL OR status = 'deleted' THEN 'deleted'
    WHEN status = 'paused' THEN 'paused'
    WHEN status = 'terminating' THEN 'terminating'
    ELSE 'active'
END;

DROP INDEX IF EXISTS manager.idx_sandboxes_team_status;

ALTER TABLE manager.sandboxes
    RENAME COLUMN status TO desired_state;

ALTER TABLE manager.sandboxes
    ADD CONSTRAINT sandboxes_desired_state_check
    CHECK (desired_state IN ('active', 'paused', 'terminating', 'deleted'));

CREATE INDEX idx_sandboxes_team_desired_state
    ON manager.sandboxes(team_id, desired_state);

-- +goose Down

DROP INDEX IF EXISTS manager.idx_sandboxes_team_desired_state;

ALTER TABLE manager.sandboxes
    DROP CONSTRAINT IF EXISTS sandboxes_desired_state_check;

ALTER TABLE manager.sandboxes
    RENAME COLUMN desired_state TO status;

UPDATE manager.sandboxes
SET status = CASE
    WHEN status = 'active' THEN 'running'
    ELSE status
END;

CREATE INDEX idx_sandboxes_team_status
    ON manager.sandboxes(team_id, status);

ALTER TABLE manager.sandboxes
    DROP COLUMN hot_claim_completed_at;

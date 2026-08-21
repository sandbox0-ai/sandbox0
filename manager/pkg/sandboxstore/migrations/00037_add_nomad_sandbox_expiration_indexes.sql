-- +goose Up

CREATE INDEX IF NOT EXISTS idx_sandboxes_nomad_hard_expiry
    ON manager.sandboxes(hard_expires_at, sandbox_id)
    WHERE deleted_at IS NULL
        AND runtime_backend = 'nomad'
        AND desired_state IN ('active', 'paused')
        AND hard_expires_at IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_sandboxes_nomad_soft_expiry
    ON manager.sandboxes(expires_at, sandbox_id)
    WHERE deleted_at IS NULL
        AND runtime_backend = 'nomad'
        AND desired_state = 'active'
        AND expires_at IS NOT NULL;

-- +goose Down

DROP INDEX IF EXISTS manager.idx_sandboxes_nomad_soft_expiry;
DROP INDEX IF EXISTS manager.idx_sandboxes_nomad_hard_expiry;

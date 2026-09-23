-- +goose Up
-- Completed source histories must not dominate the active pause worker scan.
CREATE INDEX idx_runtime_checkpoint_pause_work
    ON manager.sandbox_runtime_checkpoints(operation_id COLLATE "C")
    WHERE NOT (evidence ? 'fenced');

-- +goose Down
DROP INDEX manager.idx_runtime_checkpoint_pause_work;

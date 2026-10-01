-- +goose Up
-- Scheduling intent only. Memory authority stays immutable; a cold replacement
-- is admitted only after the source lifecycle and all physical writers end.
CREATE TABLE manager.sandbox_runtime_resume_fallbacks (
    operation_id TEXT PRIMARY KEY REFERENCES manager.sandbox_runtime_checkpoint_restores(operation_id) ON DELETE RESTRICT,
    resume_operation_id TEXT UNIQUE REFERENCES manager.sandbox_lifecycle_txns(txn_id) ON DELETE RESTRICT,
    reason TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
INSERT INTO manager.sandbox_runtime_resume_fallbacks(operation_id)
SELECT r.operation_id FROM manager.sandbox_runtime_checkpoint_restores r
JOIN manager.sandbox_lifecycle_txns l ON l.txn_id=r.operation_id
WHERE l.phase IN ('preparing','barriered','publishing','committing');

-- +goose Down
DROP TABLE manager.sandbox_runtime_resume_fallbacks;

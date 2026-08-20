-- +goose Up

ALTER TABLE manager.sandbox_lifecycle_txns
    ADD COLUMN target_sandbox_id TEXT NOT NULL DEFAULT '',
    ADD COLUMN target_generation_id TEXT NOT NULL DEFAULT '',
    ADD COLUMN target_record_digest BYTEA NOT NULL DEFAULT ''::bytea;

CREATE INDEX idx_sandbox_lifecycle_txns_active_target
    ON manager.sandbox_lifecycle_txns(target_sandbox_id)
    WHERE target_sandbox_id <> ''
        AND phase IN ('preparing', 'barriered', 'publishing', 'committing');

-- +goose Down

DROP INDEX IF EXISTS manager.idx_sandbox_lifecycle_txns_active_target;

ALTER TABLE manager.sandbox_lifecycle_txns
    DROP COLUMN IF EXISTS target_record_digest,
    DROP COLUMN IF EXISTS target_generation_id,
    DROP COLUMN IF EXISTS target_sandbox_id;

-- +goose NO TRANSACTION
-- +goose Up

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_sandbox_lifecycle_txns_sandbox_kind_epoch
    ON manager.sandbox_lifecycle_txns(sandbox_id, kind, epoch DESC);

-- +goose Down

DROP INDEX CONCURRENTLY IF EXISTS manager.idx_sandbox_lifecycle_txns_sandbox_kind_epoch;

-- +goose NO TRANSACTION
-- +goose Up

-- Metering reads completed history by sandbox and epoch. Active-only indexes
-- cannot serve it; a burst otherwise repeatedly scans all cached history.
-- Build without blocking lifecycle writes on the shared regional database.
CREATE INDEX CONCURRENTLY idx_sandbox_lifecycle_txns_history
    ON manager.sandbox_lifecycle_txns (sandbox_id, epoch, txn_id);

-- +goose Down

DROP INDEX CONCURRENTLY manager.idx_sandbox_lifecycle_txns_history;

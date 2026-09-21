-- +goose Up
-- Pending transfer work remains in the existing immutable migration journal.
CREATE INDEX idx_runtime_migration_pending_transfer
    ON manager.sandbox_runtime_migrations (operation_id COLLATE "C")
    WHERE publication_request IS NOT NULL AND source_fence_proof IS NULL;

-- +goose Down
DROP INDEX IF EXISTS manager.idx_runtime_migration_pending_transfer;

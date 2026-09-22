-- +goose Up
-- Recover completed source checkpoints from the existing command journal.
CREATE INDEX idx_runtime_migration_pending_source_recovery
    ON manager.sandbox_runtime_migrations (operation_id COLLATE "C")
    WHERE capture_request IS NOT NULL AND publication_request IS NULL;

-- +goose Down
DROP INDEX IF EXISTS manager.idx_runtime_migration_pending_source_recovery;

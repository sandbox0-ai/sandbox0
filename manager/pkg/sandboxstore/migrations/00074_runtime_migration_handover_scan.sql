-- +goose Up
CREATE INDEX idx_runtime_migration_pending_handover
    ON manager.sandbox_runtime_migrations (operation_id COLLATE "C")
    WHERE procd_handover_request IS NOT NULL AND generation_committed_at IS NULL;

-- +goose Down
DROP INDEX manager.idx_runtime_migration_pending_handover;

-- +goose Up

ALTER TABLE manager.sandboxes
    ADD COLUMN cleanup_completed_at TIMESTAMPTZ;

CREATE INDEX idx_sandboxes_team_deleted_retention
    ON manager.sandboxes(
        team_id,
        cleanup_completed_at DESC,
        sandbox_id
    )
    WHERE deleted_at IS NOT NULL AND cleanup_completed_at IS NOT NULL;

CREATE INDEX idx_sandbox_lifecycle_txns_terminal_retention
    ON manager.sandbox_lifecycle_txns(
        sandbox_id,
        (COALESCE(committed_at, aborted_at, updated_at)) DESC,
        txn_id
    )
    WHERE phase IN ('committed', 'aborted');

CREATE INDEX idx_rootfs_layers_source_sandbox
    ON manager.rootfs_layers(source_sandbox_id)
    WHERE source_sandbox_id <> '';

-- +goose Down

DROP INDEX IF EXISTS manager.idx_rootfs_layers_source_sandbox;
DROP INDEX IF EXISTS manager.idx_sandbox_lifecycle_txns_terminal_retention;
DROP INDEX IF EXISTS manager.idx_sandboxes_team_deleted_retention;

ALTER TABLE manager.sandboxes
    DROP COLUMN IF EXISTS cleanup_completed_at;

-- +goose Up

ALTER TABLE manager.sandboxes
    ADD COLUMN IF NOT EXISTS lifecycle_epoch BIGINT NOT NULL DEFAULT 0;

CREATE TABLE IF NOT EXISTS manager.sandbox_lifecycle_txns (
    txn_id TEXT PRIMARY KEY,
    sandbox_id TEXT NOT NULL REFERENCES manager.sandboxes(sandbox_id) ON DELETE CASCADE,
    kind TEXT NOT NULL,
    phase TEXT NOT NULL,
    source TEXT NOT NULL DEFAULT 'manual',
    cancelable BOOLEAN NOT NULL DEFAULT FALSE,
    epoch BIGINT NOT NULL,
    from_generation BIGINT NOT NULL DEFAULT 0,
    to_generation BIGINT NOT NULL DEFAULT 0,
    from_pod_namespace TEXT NOT NULL DEFAULT '',
    from_pod_name TEXT NOT NULL DEFAULT '',
    to_pod_namespace TEXT NOT NULL DEFAULT '',
    to_pod_name TEXT NOT NULL DEFAULT '',
    expected_head_layer_id TEXT NOT NULL DEFAULT '',
    prepared_head_layer_id TEXT NOT NULL DEFAULT '',
    error TEXT NOT NULL DEFAULT '',
    cancel_reason TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    cancel_requested_at TIMESTAMPTZ,
    committed_at TIMESTAMPTZ,
    aborted_at TIMESTAMPTZ
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_sandbox_lifecycle_txns_active
    ON manager.sandbox_lifecycle_txns(sandbox_id)
    WHERE phase IN ('preparing', 'barriered', 'publishing', 'committing');

CREATE INDEX IF NOT EXISTS idx_sandbox_lifecycle_txns_kind_phase_updated
    ON manager.sandbox_lifecycle_txns(kind, phase, updated_at ASC);

DROP TRIGGER IF EXISTS update_sandbox_lifecycle_txns_updated_at ON manager.sandbox_lifecycle_txns;
CREATE TRIGGER update_sandbox_lifecycle_txns_updated_at
    BEFORE UPDATE ON manager.sandbox_lifecycle_txns
    FOR EACH ROW
    EXECUTE FUNCTION manager.update_updated_at_column();

-- +goose Down

DROP TRIGGER IF EXISTS update_sandbox_lifecycle_txns_updated_at ON manager.sandbox_lifecycle_txns;
DROP INDEX IF EXISTS manager.idx_sandbox_lifecycle_txns_kind_phase_updated;
DROP INDEX IF EXISTS manager.idx_sandbox_lifecycle_txns_active;
DROP TABLE IF EXISTS manager.sandbox_lifecycle_txns;

ALTER TABLE manager.sandboxes
    DROP COLUMN IF EXISTS lifecycle_epoch;

-- +goose Up

ALTER TABLE manager.sandboxes
    ADD COLUMN IF NOT EXISTS runtime_backend TEXT NOT NULL DEFAULT 'kubernetes';

ALTER TABLE manager.sandboxes
    ADD CONSTRAINT sandboxes_runtime_backend_check
    CHECK (runtime_backend IN ('kubernetes', 'nomad'));

CREATE TABLE manager.sandbox_runtime_claims (
    sandbox_id TEXT PRIMARY KEY REFERENCES manager.sandboxes(sandbox_id) ON DELETE CASCADE,
    operation_id TEXT NOT NULL UNIQUE,
    phase TEXT NOT NULL,
    lease_expires_at TIMESTAMPTZ,
    last_error TEXT NOT NULL DEFAULT '',
    completed_at TIMESTAMPTZ,
    cleanup_started_at TIMESTAMPTZ,
    cleaned_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT sandbox_runtime_claims_operation_id_check
        CHECK (operation_id <> '' AND char_length(operation_id) <= 512),
    CONSTRAINT sandbox_runtime_claims_phase_check
        CHECK (phase IN ('claiming', 'ready', 'cleanup_pending', 'cleaned')),
    CONSTRAINT sandbox_runtime_claims_lease_check
        CHECK ((phase = 'claiming' AND lease_expires_at IS NOT NULL)
            OR (phase <> 'claiming' AND lease_expires_at IS NULL))
);

CREATE INDEX idx_sandbox_runtime_claims_cleanup
    ON manager.sandbox_runtime_claims(phase, lease_expires_at, updated_at, sandbox_id)
    WHERE phase IN ('claiming', 'cleanup_pending');

DROP TRIGGER IF EXISTS update_sandbox_runtime_claims_updated_at ON manager.sandbox_runtime_claims;
CREATE TRIGGER update_sandbox_runtime_claims_updated_at
    BEFORE UPDATE ON manager.sandbox_runtime_claims
    FOR EACH ROW
    EXECUTE FUNCTION manager.update_updated_at_column();

-- +goose Down

DROP TRIGGER IF EXISTS update_sandbox_runtime_claims_updated_at ON manager.sandbox_runtime_claims;
DROP INDEX IF EXISTS manager.idx_sandbox_runtime_claims_cleanup;
DROP TABLE IF EXISTS manager.sandbox_runtime_claims;

ALTER TABLE manager.sandboxes
    DROP CONSTRAINT IF EXISTS sandboxes_runtime_backend_check;

ALTER TABLE manager.sandboxes
    DROP COLUMN IF EXISTS runtime_backend;

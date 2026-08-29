-- +goose Up

-- Runtime recovery is a region-wide controller responsibility. Keep its
-- scheduling lease on the lifecycle transaction that created the recovery
-- obligation so multiple manager replicas cannot independently retry it.
ALTER TABLE manager.sandbox_lifecycle_txns
    ADD COLUMN IF NOT EXISTS recovery_attempts INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS recovery_next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    ADD COLUMN IF NOT EXISTS recovery_claimed_by TEXT NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS recovery_claim_token TEXT NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS recovery_claimed_until TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS recovery_last_error TEXT NOT NULL DEFAULT '';

-- +goose StatementBegin
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'manager.sandbox_lifecycle_txns'::regclass
            AND conname = 'sandbox_lifecycle_txns_recovery_attempts_check'
    ) THEN
        ALTER TABLE manager.sandbox_lifecycle_txns
            ADD CONSTRAINT sandbox_lifecycle_txns_recovery_attempts_check
            CHECK (recovery_attempts BETWEEN 0 AND 1000000);
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'manager.sandbox_lifecycle_txns'::regclass
            AND conname = 'sandbox_lifecycle_txns_recovery_claim_check'
    ) THEN
        ALTER TABLE manager.sandbox_lifecycle_txns
            ADD CONSTRAINT sandbox_lifecycle_txns_recovery_claim_check CHECK (
                (recovery_claimed_by = '' AND recovery_claim_token = ''
                    AND recovery_claimed_until IS NULL)
                OR (octet_length(recovery_claimed_by) BETWEEN 1 AND 256
                    AND octet_length(recovery_claim_token) = 36
                    AND recovery_claimed_until IS NOT NULL)
            );
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'manager.sandbox_lifecycle_txns'::regclass
            AND conname = 'sandbox_lifecycle_txns_recovery_last_error_check'
    ) THEN
        ALTER TABLE manager.sandbox_lifecycle_txns
            ADD CONSTRAINT sandbox_lifecycle_txns_recovery_last_error_check
            CHECK (octet_length(recovery_last_error) <= 4096);
    END IF;
END;
$$;
-- +goose StatementEnd

CREATE INDEX IF NOT EXISTS idx_sandbox_lifecycle_txns_recovery_due
    ON manager.sandbox_lifecycle_txns (
        recovery_next_attempt_at, recovery_claimed_until, sandbox_id, epoch
    )
    WHERE kind = 'pause' AND source IN ('crash', 'health', 'lost');

-- +goose Down
DROP INDEX IF EXISTS manager.idx_sandbox_lifecycle_txns_recovery_due;
ALTER TABLE manager.sandbox_lifecycle_txns
    DROP CONSTRAINT IF EXISTS sandbox_lifecycle_txns_recovery_last_error_check,
    DROP CONSTRAINT IF EXISTS sandbox_lifecycle_txns_recovery_claim_check,
    DROP CONSTRAINT IF EXISTS sandbox_lifecycle_txns_recovery_attempts_check,
    DROP COLUMN IF EXISTS recovery_last_error,
    DROP COLUMN IF EXISTS recovery_claimed_until,
    DROP COLUMN IF EXISTS recovery_claim_token,
    DROP COLUMN IF EXISTS recovery_claimed_by,
    DROP COLUMN IF EXISTS recovery_next_attempt_at,
    DROP COLUMN IF EXISTS recovery_attempts;

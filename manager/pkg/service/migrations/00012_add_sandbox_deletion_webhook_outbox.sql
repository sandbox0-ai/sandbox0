-- +goose Up

CREATE TABLE IF NOT EXISTS manager.sandbox_deletion_webhook_outbox (
    event_id TEXT PRIMARY KEY,
    sandbox_id TEXT NOT NULL,
    team_id TEXT NOT NULL,
    target_url TEXT NOT NULL,
    payload BYTEA NOT NULL,
    signature TEXT NOT NULL DEFAULT '',
    attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at TIMESTAMPTZ NOT NULL,
    claimed_by TEXT NOT NULL DEFAULT '',
    claimed_until TIMESTAMPTZ,
    delivered_at TIMESTAMPTZ,
    terminal_at TIMESTAMPTZ,
    terminal_reason TEXT NOT NULL DEFAULT '',
    last_status INTEGER,
    last_error TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CHECK (delivered_at IS NULL OR terminal_at IS NULL)
);

CREATE INDEX IF NOT EXISTS idx_sandbox_deletion_webhook_outbox_due
    ON manager.sandbox_deletion_webhook_outbox(next_attempt_at ASC, created_at ASC)
    WHERE delivered_at IS NULL AND terminal_at IS NULL;

CREATE INDEX IF NOT EXISTS idx_sandbox_deletion_webhook_outbox_claim
    ON manager.sandbox_deletion_webhook_outbox(claimed_until ASC)
    WHERE claimed_until IS NOT NULL AND delivered_at IS NULL AND terminal_at IS NULL;

CREATE INDEX IF NOT EXISTS idx_sandbox_deletion_webhook_outbox_terminal
    ON manager.sandbox_deletion_webhook_outbox(terminal_at ASC)
    WHERE terminal_at IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_sandbox_deletion_webhook_outbox_delivered
    ON manager.sandbox_deletion_webhook_outbox(delivered_at ASC)
    WHERE delivered_at IS NOT NULL;

-- +goose Down

DROP INDEX IF EXISTS manager.idx_sandbox_deletion_webhook_outbox_delivered;
DROP INDEX IF EXISTS manager.idx_sandbox_deletion_webhook_outbox_terminal;
DROP INDEX IF EXISTS manager.idx_sandbox_deletion_webhook_outbox_claim;
DROP INDEX IF EXISTS manager.idx_sandbox_deletion_webhook_outbox_due;
DROP TABLE IF EXISTS manager.sandbox_deletion_webhook_outbox;

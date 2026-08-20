-- +goose Up

ALTER TABLE manager.runtime_slots
    ADD COLUMN procd_address TEXT NOT NULL DEFAULT '';

CREATE UNIQUE INDEX IF NOT EXISTS idx_runtime_slots_live_sandbox
    ON manager.runtime_slots(sandbox_id)
    WHERE sandbox_id IS NOT NULL AND state <> 'terminal';

-- +goose Down

DROP INDEX IF EXISTS manager.idx_runtime_slots_live_sandbox;

ALTER TABLE manager.runtime_slots
    DROP COLUMN IF EXISTS procd_address;

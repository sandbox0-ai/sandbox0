-- +goose Up
ALTER TABLE manager.sandbox_runtime_migrations
    ADD COLUMN restore_receipt JSONB,
    ADD COLUMN procd_handover_request JSONB,
    ADD COLUMN procd_handover_digest TEXT,
    ADD COLUMN procd_handover_receipt JSONB,
    ADD COLUMN generation_committed_at TIMESTAMPTZ,
    ADD CONSTRAINT migration_handover_pair CHECK (
        (restore_receipt IS NULL AND procd_handover_request IS NULL AND procd_handover_digest IS NULL) OR
        (restore_receipt IS NOT NULL AND restore_request IS NOT NULL AND procd_handover_request IS NOT NULL
            AND procd_handover_digest IS NOT NULL AND procd_handover_digest ~ '^[0-9a-f]{64}$'
            AND octet_length(restore_receipt::text)<=1048576 AND octet_length(procd_handover_request::text)<=1048576)),
    ADD CONSTRAINT migration_handover_receipt_requires_intent CHECK (
        procd_handover_receipt IS NULL OR (procd_handover_request IS NOT NULL AND octet_length(procd_handover_receipt::text)<=16384)),
    ADD CONSTRAINT migration_generation_requires_handover CHECK (
        generation_committed_at IS NULL OR procd_handover_receipt IS NOT NULL);
-- +goose StatementBegin
CREATE FUNCTION manager.guard_runtime_migration_handover() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF (OLD.procd_handover_request IS NOT NULL AND
        ROW(NEW.restore_receipt,NEW.procd_handover_request,NEW.procd_handover_digest)
        IS DISTINCT FROM ROW(OLD.restore_receipt,OLD.procd_handover_request,OLD.procd_handover_digest)) OR
        (OLD.procd_handover_receipt IS NOT NULL AND NEW.procd_handover_receipt IS DISTINCT FROM OLD.procd_handover_receipt) OR
        (OLD.generation_committed_at IS NOT NULL AND NEW.generation_committed_at IS DISTINCT FROM OLD.generation_committed_at) THEN
        RAISE EXCEPTION 'Migration handover evidence is immutable' USING ERRCODE = '23514';
    END IF;
    IF OLD.procd_handover_request IS NULL AND NEW.procd_handover_request IS NOT NULL AND NOT EXISTS (
        SELECT 1 FROM manager.sandbox_lifecycle_txns WHERE txn_id=NEW.operation_id AND phase='committing'
    ) THEN
        RAISE EXCEPTION 'Migration handover requires source fencing' USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$;
-- +goose StatementEnd
CREATE TRIGGER runtime_migration_handover_guard BEFORE UPDATE ON manager.sandbox_runtime_migrations
    FOR EACH ROW EXECUTE FUNCTION manager.guard_runtime_migration_handover();
-- +goose Down
-- +goose StatementBegin
DO $$ BEGIN
    RAISE EXCEPTION 'Migration handover evidence cannot be discarded' USING ERRCODE = '55000';
END $$;
-- +goose StatementEnd

-- +goose Up
ALTER TABLE manager.sandbox_runtime_migrations
    ADD COLUMN failure_request JSONB,
    ADD COLUMN failure_digest TEXT,
    ADD COLUMN failure_stop_receipt JSONB,
    ADD CONSTRAINT migration_failure_stop_shape CHECK (failure_stop_receipt IS NULL OR
        (failure_request IS NOT NULL AND jsonb_typeof(failure_stop_receipt)='object' AND octet_length(failure_stop_receipt::text)<=4096)),
    ADD CONSTRAINT migration_failure_intent_shape CHECK (
        (failure_request IS NULL AND failure_digest IS NULL) OR
        (failure_request IS NOT NULL AND failure_digest IS NOT NULL AND failure_digest ~ '^[0-9a-f]{64}$'
            AND jsonb_typeof(failure_request)='object' AND octet_length(failure_request::text)<=4194304
            AND restore_request IS NOT NULL AND source_fence_proof IS NOT NULL
            AND generation_committed_at IS NULL AND adoption_request IS NULL));

-- +goose StatementBegin
CREATE FUNCTION manager.guard_runtime_migration_failure_intent() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF OLD.failure_stop_receipt IS NOT NULL AND NEW.failure_stop_receipt IS DISTINCT FROM OLD.failure_stop_receipt THEN
        RAISE EXCEPTION 'Migration failure stop receipt is immutable' USING ERRCODE='23514';
    END IF;
    IF OLD.failure_request IS NOT NULL AND ROW(NEW.failure_request,NEW.failure_digest)
        IS DISTINCT FROM ROW(OLD.failure_request,OLD.failure_digest) THEN
        RAISE EXCEPTION 'Migration failure authority is immutable' USING ERRCODE='23514';
    END IF;
    IF NEW.failure_request IS NOT NULL THEN
        IF NEW.failure_request->'restore' IS DISTINCT FROM NEW.restore_request
            OR COALESCE(NEW.failure_request->>'reason','') NOT IN ('termination','destination_unavailable') THEN
            RAISE EXCEPTION 'Migration failure must retain the exact restore' USING ERRCODE='23514';
        END IF;
        IF OLD.failure_request IS NULL AND NOT EXISTS (
            SELECT 1 FROM manager.sandbox_lifecycle_txns l
            WHERE l.txn_id=NEW.operation_id AND l.kind='migrate' AND l.source='auto'
                AND NOT l.cancelable AND l.phase='committing'
        ) THEN
            RAISE EXCEPTION 'Migration failure requires outstanding system-owned handover' USING ERRCODE='23514';
        END IF;
        IF ROW(NEW.restore_receipt,NEW.procd_handover_request,NEW.procd_handover_digest,
                NEW.procd_handover_receipt,NEW.generation_committed_at,NEW.adoption_request,NEW.adoption_receipt)
            IS DISTINCT FROM ROW(OLD.restore_receipt,OLD.procd_handover_request,OLD.procd_handover_digest,
                OLD.procd_handover_receipt,OLD.generation_committed_at,OLD.adoption_request,OLD.adoption_receipt) THEN
            RAISE EXCEPTION 'Failed migration cannot advance execution or handover' USING ERRCODE='23514';
        END IF;
    END IF;
    RETURN NEW;
END;
$$;
-- +goose StatementEnd
CREATE TRIGGER runtime_migration_failure_intent_guard BEFORE UPDATE ON manager.sandbox_runtime_migrations
    FOR EACH ROW EXECUTE FUNCTION manager.guard_runtime_migration_failure_intent();

-- +goose StatementBegin
CREATE FUNCTION manager.guard_runtime_migration_failure_slot() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.state IN ('claiming','starting','active') AND EXISTS (
        SELECT 1 FROM manager.sandbox_runtime_migrations m WHERE m.target_slot_id=NEW.slot_id AND m.failure_request IS NOT NULL
    ) THEN
        RAISE EXCEPTION 'Failed migration destination cannot reacquire execution' USING ERRCODE='23514';
    END IF;
    RETURN NEW;
END;
$$;
-- +goose StatementEnd
CREATE TRIGGER runtime_migration_failure_slot_guard BEFORE UPDATE OF state ON manager.runtime_slots
    FOR EACH ROW EXECUTE FUNCTION manager.guard_runtime_migration_failure_slot();

-- +goose StatementBegin
CREATE FUNCTION manager.check_runtime_migration_failure_slot() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM manager.runtime_slots WHERE slot_id=NEW.target_slot_id AND state IN ('quiescing','orphaned','terminal')) THEN
        RAISE EXCEPTION 'Migration failure intent must atomically close target claim authority' USING ERRCODE='23514';
    END IF;
    RETURN NULL;
END;
$$;
-- +goose StatementEnd
CREATE CONSTRAINT TRIGGER runtime_migration_failure_slot_check AFTER UPDATE ON manager.sandbox_runtime_migrations
    DEFERRABLE INITIALLY DEFERRED FOR EACH ROW WHEN (NEW.failure_request IS NOT NULL)
    EXECUTE FUNCTION manager.check_runtime_migration_failure_slot();

CREATE INDEX idx_runtime_migration_failure_candidates ON manager.sandbox_runtime_migrations(operation_id COLLATE "C")
    WHERE restore_request IS NOT NULL AND failure_request IS NULL AND generation_committed_at IS NULL;

-- +goose Down
-- +goose StatementBegin
DO $$ BEGIN
    RAISE EXCEPTION 'Migration failure authority cannot be discarded' USING ERRCODE='55000';
END $$;
-- +goose StatementEnd

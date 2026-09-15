-- +goose Up
-- Aborts own only incarnations that never acquired a regional runtime slot.
-- Retain their fences after physical cleanup, including across binary rollback.
CREATE TABLE manager.runtime_slot_registration_aborts (
    slot_id TEXT PRIMARY KEY,
    cleanup_request JSONB NOT NULL,
    cleanup_proof JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ,
    CHECK (cleanup_request->>'slot_id' = slot_id),
    CHECK ((cleanup_proof IS NULL) = (completed_at IS NULL))
);

-- The database enforces exclusion even while older manager binaries still
-- register slots during a rolling update. An application-only lock is unsafe.
-- +goose StatementBegin
CREATE FUNCTION manager.fence_runtime_slot_registration() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    PERFORM pg_advisory_xact_lock(hashtextextended('runtime-slot-registration:' || NEW.slot_id, 0));
    IF EXISTS (SELECT 1 FROM manager.runtime_slot_registration_aborts WHERE slot_id = NEW.slot_id) THEN
        RAISE EXCEPTION 'Runtime slot registration is durably aborted' USING ERRCODE = '23505';
    END IF;
    RETURN NEW;
END;
$$;
-- +goose StatementEnd
CREATE TRIGGER runtime_slot_registration_fence BEFORE INSERT ON manager.runtime_slots
    FOR EACH ROW EXECUTE FUNCTION manager.fence_runtime_slot_registration();

-- +goose StatementBegin
CREATE FUNCTION manager.exclude_registered_slot_abort() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    PERFORM pg_advisory_xact_lock(hashtextextended('runtime-slot-registration:' || NEW.slot_id, 0));
    IF EXISTS (SELECT 1 FROM manager.runtime_slots WHERE slot_id = NEW.slot_id) THEN
        RAISE EXCEPTION 'Registered runtime slots retain their existing lifecycle authority' USING ERRCODE = '23505';
    END IF;
    RETURN NEW;
END;
$$;
-- +goose StatementEnd
CREATE TRIGGER runtime_slot_abort_exclusion BEFORE INSERT ON manager.runtime_slot_registration_aborts
    FOR EACH ROW EXECUTE FUNCTION manager.exclude_registered_slot_abort();

-- +goose Down
-- +goose StatementBegin
DO $$ BEGIN
    RAISE EXCEPTION 'Runtime slot registration fences cannot be rolled back' USING ERRCODE = '55000';
END $$;
-- +goose StatementEnd

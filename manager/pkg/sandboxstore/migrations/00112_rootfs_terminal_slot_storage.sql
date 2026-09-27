-- +goose Up
-- Keep exact launch identity after physical termination without perpetually
-- retaining the disk generation used to start each obsolete incarnation.
ALTER TABLE manager.runtime_slots ADD COLUMN source_generation_ref TEXT
    REFERENCES manager.rootfs_generations(generation_id) ON DELETE RESTRICT;
UPDATE manager.runtime_slots SET source_generation_ref=source_generation_id;
ALTER TABLE manager.runtime_slots DROP CONSTRAINT runtime_slots_source_generation_id_fkey;
DROP INDEX manager.runtime_slots_source_generation_idx;
CREATE INDEX runtime_slots_source_generation_ref_idx ON manager.runtime_slots(source_generation_ref);
CREATE INDEX runtime_slots_terminal_storage_order_idx ON manager.runtime_slots(terminal_at,slot_id)
    WHERE state='terminal' AND source_generation_ref IS NOT NULL;

-- +goose StatementBegin
CREATE FUNCTION manager.guard_runtime_slot_generation_custody() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP='INSERT' THEN
        NEW.source_generation_ref := NEW.source_generation_id;
        RETURN NEW;
    END IF;
    IF OLD.source_generation_id IS NULL AND NEW.source_generation_id IS NOT NULL THEN
        IF OLD.state NOT IN ('registered','fastpath_ready') OR NEW.state<>'claiming' THEN
            RAISE EXCEPTION 'Only a new warm-slot claim may acquire disk custody' USING ERRCODE='23514';
        END IF;
        NEW.source_generation_ref := NEW.source_generation_id;
        RETURN NEW;
    END IF;
    IF NEW.source_generation_id IS DISTINCT FROM OLD.source_generation_id
        AND NOT (NEW.source_generation_id IS NULL AND NEW.state='terminal') THEN
        RAISE EXCEPTION 'Runtime launch generation identity cannot change' USING ERRCODE='23514';
    END IF;
    IF NEW.source_generation_id IS NULL THEN NEW.source_generation_ref := NULL; END IF;
    IF NEW.source_generation_ref IS DISTINCT FROM OLD.source_generation_ref THEN
        IF NEW.source_generation_ref IS NOT NULL OR NEW.state<>'terminal' OR octet_length(NEW.terminal_proof_digest) IS DISTINCT FROM 32
            OR (NEW.resource_lease_id IS NOT NULL AND NOT EXISTS (
                SELECT 1 FROM manager.runtime_resource_leases lease WHERE lease.lease_id=NEW.resource_lease_id
                    AND lease.lease_state='released' AND lease.released_at IS NOT NULL))
            OR (COALESCE(NEW.writer_grant_id,OLD.writer_grant_id) IS NOT NULL AND NOT EXISTS (
                SELECT 1 FROM manager.rootfs_writer_grants writer WHERE writer.grant_id=COALESCE(NEW.writer_grant_id,OLD.writer_grant_id) AND writer.state IN ('retired','canceled'))) THEN
            RAISE EXCEPTION 'Runtime disk release requires physical terminal proof' USING ERRCODE='23514';
        END IF;
    END IF;
    RETURN NEW;
END $$;
-- +goose StatementEnd
CREATE TRIGGER runtime_slot_generation_custody_guard BEFORE INSERT OR UPDATE ON manager.runtime_slots
    FOR EACH ROW EXECUTE FUNCTION manager.guard_runtime_slot_generation_custody();

-- +goose Down
-- +goose StatementBegin
DO $$ BEGIN RAISE EXCEPTION 'Terminal launch provenance cannot reacquire collected generations' USING ERRCODE='55000'; END $$;
-- +goose StatementEnd

-- +goose Up
-- The exact reservation input allows system workers to recover before procd
-- preparation. Old digest-only reservations cannot invent a payload on retry.
ALTER TABLE manager.sandbox_runtime_migrations
    ADD COLUMN assignment_request JSONB,
    ADD CONSTRAINT migration_assignment_shape CHECK (
        assignment_request IS NULL OR
        (jsonb_typeof(assignment_request) = 'object'
            AND octet_length(assignment_request::text) <= 1048576));

-- +goose StatementBegin
CREATE FUNCTION manager.guard_runtime_migration_assignment() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'UPDATE' THEN
        IF NEW.assignment_request IS DISTINCT FROM OLD.assignment_request THEN
            RAISE EXCEPTION 'Migration reservation assignment is immutable' USING ERRCODE = '23514';
        END IF;
    ELSE
        IF NEW.assignment_request IS NULL OR NOT EXISTS (
            SELECT 1 FROM manager.sandbox_lifecycle_txns l
            JOIN manager.sandboxes s ON s.sandbox_id=l.sandbox_id
            JOIN manager.runtime_slots source ON source.slot_id=NEW.source_slot_id
            WHERE l.txn_id=NEW.operation_id
                AND NEW.assignment_request->>'operation_id'=l.txn_id
                AND NEW.assignment_request->>'source_generation'=l.from_generation::text
                AND NEW.assignment_request->'target'->>'runtime_generation'=l.to_generation::text
                AND NEW.assignment_request->'target'->>'sandbox_id'=l.sandbox_id
                AND NEW.assignment_request->'target'->>'team_id'=s.team_id
                AND NEW.assignment_request->>'source_revision'=source.claim_runtime_assignment_revision
        ) THEN
            RAISE EXCEPTION 'Migration assignment does not match reservation' USING ERRCODE = '23514';
        END IF;
    END IF;
    RETURN NEW;
END;
$$;
-- +goose StatementEnd
CREATE TRIGGER runtime_migration_assignment_guard BEFORE INSERT OR UPDATE ON manager.sandbox_runtime_migrations
    FOR EACH ROW EXECUTE FUNCTION manager.guard_runtime_migration_assignment();

CREATE INDEX idx_runtime_migration_pending_cpu
    ON manager.sandbox_runtime_migrations (operation_id COLLATE "C")
    WHERE assignment_request IS NOT NULL AND preparation_request IS NULL AND cpu_preflight_destination IS NULL;

-- +goose Down
-- +goose StatementBegin
DO $$ BEGIN
    RAISE EXCEPTION 'Migration reservation assignments must retain recovery evidence' USING ERRCODE = '55000';
END $$;
-- +goose StatementEnd

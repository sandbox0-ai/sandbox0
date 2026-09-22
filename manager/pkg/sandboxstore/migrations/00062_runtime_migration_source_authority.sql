-- +goose Up
-- Exact commands are recovery evidence within the existing lifecycle. They
-- cannot be regenerated from mutable slot observations after dispatch.
ALTER TABLE manager.sandbox_runtime_migrations
    ADD COLUMN preparation_request JSONB,
    ADD COLUMN preparation_digest TEXT,
    ADD COLUMN capture_request JSONB,
    ADD COLUMN capture_digest TEXT,
    ADD CONSTRAINT migration_preparation_pair CHECK (
        (preparation_request IS NULL AND preparation_digest IS NULL) OR
        (preparation_request IS NOT NULL AND preparation_digest IS NOT NULL
            AND jsonb_typeof(preparation_request) = 'object'
            AND octet_length(preparation_request::text) <= 1048576
            AND preparation_digest ~ '^[0-9a-f]{64}$')),
    ADD CONSTRAINT migration_capture_pair CHECK (
        (capture_request IS NULL AND capture_digest IS NULL) OR
        (capture_request IS NOT NULL AND capture_digest IS NOT NULL
            AND preparation_request IS NOT NULL
            AND jsonb_typeof(capture_request) = 'object'
            AND octet_length(capture_request::text) <= 65536
            AND capture_digest ~ '^[0-9a-f]{64}$'));

-- +goose StatementBegin
CREATE FUNCTION manager.guard_runtime_migration_source_authority() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE current_phase TEXT;
BEGIN
    IF ROW(NEW.operation_id, NEW.assignment_digest, NEW.source_slot_id,
        NEW.target_slot_id, NEW.target_resource_lease_id, NEW.source_writer_grant_id,
        NEW.source_binding_digest, NEW.source_procd_instance_id, NEW.created_at)
        IS DISTINCT FROM ROW(OLD.operation_id, OLD.assignment_digest, OLD.source_slot_id,
        OLD.target_slot_id, OLD.target_resource_lease_id, OLD.source_writer_grant_id,
        OLD.source_binding_digest, OLD.source_procd_instance_id, OLD.created_at)
        OR (OLD.preparation_request IS NOT NULL AND
            ROW(NEW.preparation_request, NEW.preparation_digest)
            IS DISTINCT FROM ROW(OLD.preparation_request, OLD.preparation_digest))
        OR (OLD.capture_request IS NOT NULL AND
            ROW(NEW.capture_request, NEW.capture_digest)
            IS DISTINCT FROM ROW(OLD.capture_request, OLD.capture_digest)) THEN
        RAISE EXCEPTION 'Migration source authority is immutable' USING ERRCODE = '23514';
    END IF;
    SELECT phase INTO current_phase FROM manager.sandbox_lifecycle_txns WHERE txn_id=NEW.operation_id;
    IF (OLD.preparation_request IS NULL AND NEW.preparation_request IS NOT NULL AND current_phase <> 'barriered')
        OR (OLD.capture_request IS NULL AND NEW.capture_request IS NOT NULL AND current_phase <> 'publishing') THEN
        RAISE EXCEPTION 'Migration command requires its committed lifecycle phase' USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$;
-- +goose StatementEnd
CREATE TRIGGER runtime_migration_source_authority_guard BEFORE UPDATE ON manager.sandbox_runtime_migrations
    FOR EACH ROW EXECUTE FUNCTION manager.guard_runtime_migration_source_authority();

-- +goose Down
-- +goose StatementBegin
DO $$ BEGIN
    RAISE EXCEPTION 'Migration source authorization must retain recovery evidence' USING ERRCODE = '55000';
END $$;
-- +goose StatementEnd

-- +goose Up
-- CPU observations belong to the existing migration transaction. A fixed
-- database deadline starts before dispatch and cannot be renewed by retries.
ALTER TABLE manager.sandbox_runtime_migrations
    ADD COLUMN cpu_preflight_request JSONB,
    ADD COLUMN cpu_preflight_requested_at TIMESTAMPTZ,
    ADD COLUMN cpu_preflight_source JSONB,
    ADD COLUMN cpu_preflight_destination JSONB,
    ADD CONSTRAINT migration_cpu_preflight_shape CHECK (
        (cpu_preflight_request IS NULL AND cpu_preflight_requested_at IS NULL
            AND cpu_preflight_source IS NULL AND cpu_preflight_destination IS NULL) OR
        (cpu_preflight_request IS NOT NULL AND cpu_preflight_requested_at IS NOT NULL
            AND jsonb_typeof(cpu_preflight_request) = 'object'
            AND octet_length(cpu_preflight_request::text) <= 1048576
            AND (cpu_preflight_source IS NULL OR
                (jsonb_typeof(cpu_preflight_source) = 'object' AND octet_length(cpu_preflight_source::text) <= 1048576))
            AND (cpu_preflight_destination IS NULL OR
                (cpu_preflight_source IS NOT NULL AND jsonb_typeof(cpu_preflight_destination) = 'object'
                    AND octet_length(cpu_preflight_destination::text) <= 1048576))));

-- +goose StatementBegin
CREATE FUNCTION manager.guard_runtime_migration_cpu_preflight() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE current_phase TEXT;
BEGIN
    IF (OLD.cpu_preflight_request IS NOT NULL AND
        ROW(NEW.cpu_preflight_request, NEW.cpu_preflight_requested_at)
        IS DISTINCT FROM ROW(OLD.cpu_preflight_request, OLD.cpu_preflight_requested_at))
        OR (OLD.cpu_preflight_source IS NOT NULL AND NEW.cpu_preflight_source IS DISTINCT FROM OLD.cpu_preflight_source)
        OR (OLD.cpu_preflight_destination IS NOT NULL AND NEW.cpu_preflight_destination IS DISTINCT FROM OLD.cpu_preflight_destination) THEN
        RAISE EXCEPTION 'Migration CPU evidence is immutable' USING ERRCODE = '23514';
    END IF;
    SELECT phase INTO current_phase FROM manager.sandbox_lifecycle_txns WHERE txn_id=NEW.operation_id;
    IF ((OLD.cpu_preflight_request IS NULL AND NEW.cpu_preflight_request IS NOT NULL)
        OR (OLD.cpu_preflight_source IS NULL AND NEW.cpu_preflight_source IS NOT NULL)
        OR (OLD.cpu_preflight_destination IS NULL AND NEW.cpu_preflight_destination IS NOT NULL))
        AND current_phase <> 'preparing' THEN
        RAISE EXCEPTION 'Migration CPU preflight must precede preparation' USING ERRCODE = '23514';
    END IF;
    IF (OLD.cpu_preflight_source IS NULL AND NEW.cpu_preflight_source IS NOT NULL)
        OR (OLD.cpu_preflight_destination IS NULL AND NEW.cpu_preflight_destination IS NOT NULL)
        OR (OLD.preparation_request IS NULL AND NEW.preparation_request IS NOT NULL)
        OR (OLD.capture_request IS NULL AND NEW.capture_request IS NOT NULL) THEN
        IF NEW.cpu_preflight_requested_at IS NULL
            OR NEW.cpu_preflight_requested_at > clock_timestamp()
            OR NEW.cpu_preflight_requested_at + INTERVAL '2 minutes' <= clock_timestamp() THEN
            RAISE EXCEPTION 'New migration execution authority requires fresh CPU preflight' USING ERRCODE = '23514';
        END IF;
    END IF;
    IF ((OLD.preparation_request IS NULL AND NEW.preparation_request IS NOT NULL)
        OR (OLD.capture_request IS NULL AND NEW.capture_request IS NOT NULL))
        AND NEW.cpu_preflight_destination IS NULL THEN
        RAISE EXCEPTION 'Migration requires both CPU receipts' USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$;
-- +goose StatementEnd
CREATE TRIGGER runtime_migration_cpu_preflight_guard BEFORE UPDATE ON manager.sandbox_runtime_migrations
    FOR EACH ROW EXECUTE FUNCTION manager.guard_runtime_migration_cpu_preflight();

-- +goose Down
-- +goose StatementBegin
DO $$ BEGIN
    RAISE EXCEPTION 'Migration CPU evidence must survive recovery' USING ERRCODE = '55000';
END $$;
-- +goose StatementEnd

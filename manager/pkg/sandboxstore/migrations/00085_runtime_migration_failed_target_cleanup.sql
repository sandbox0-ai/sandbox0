-- +goose Up
ALTER TABLE manager.sandbox_runtime_migrations
    ADD COLUMN failure_cleanup_request JSONB,
    ADD COLUMN failure_cleanup_digest TEXT,
    ADD COLUMN failure_cleanup_receipt JSONB,
    ADD CONSTRAINT migration_failure_cleanup_shape CHECK (
        (failure_cleanup_request IS NULL AND failure_cleanup_digest IS NULL AND failure_cleanup_receipt IS NULL) OR
        (failure_cleanup_request IS NOT NULL AND failure_cleanup_digest IS NOT NULL
            AND failure_cleanup_digest ~ '^[0-9a-f]{64}$' AND failure_stop_receipt IS NOT NULL
            AND octet_length(failure_cleanup_request::text)<=4194304
            AND (failure_cleanup_receipt IS NULL OR octet_length(failure_cleanup_receipt::text)<=32768)));
-- +goose StatementBegin
CREATE FUNCTION manager.guard_runtime_migration_failure_cleanup() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF (OLD.failure_cleanup_request IS NOT NULL AND ROW(NEW.failure_cleanup_request,NEW.failure_cleanup_digest)
        IS DISTINCT FROM ROW(OLD.failure_cleanup_request,OLD.failure_cleanup_digest)) OR
        (OLD.failure_cleanup_receipt IS NOT NULL AND NEW.failure_cleanup_receipt IS DISTINCT FROM OLD.failure_cleanup_receipt) THEN
        RAISE EXCEPTION 'Failed target cleanup authority and evidence are immutable' USING ERRCODE='23514';
    END IF;
    IF NEW.failure_cleanup_request IS NOT NULL AND
        (NEW.failure_cleanup_request->'failure'->'request' IS DISTINCT FROM NEW.failure_request OR
         NEW.failure_cleanup_request->'failure'->'proof' IS DISTINCT FROM NEW.failure_stop_receipt) THEN
        RAISE EXCEPTION 'Failed target cleanup changed the irreversible stop' USING ERRCODE='23514';
    END IF;
    RETURN NEW;
END;
$$;
-- +goose StatementEnd
CREATE TRIGGER runtime_migration_failure_cleanup_guard BEFORE UPDATE ON manager.sandbox_runtime_migrations
    FOR EACH ROW EXECUTE FUNCTION manager.guard_runtime_migration_failure_cleanup();
-- +goose Down
-- +goose StatementBegin
DO $$ BEGIN
    RAISE EXCEPTION 'Failed target cleanup authority cannot be discarded' USING ERRCODE='55000';
END $$;
-- +goose StatementEnd

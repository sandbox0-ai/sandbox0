-- +goose Up
ALTER TABLE manager.sandbox_runtime_migrations
    ADD COLUMN adoption_request JSONB,
    ADD COLUMN adoption_digest TEXT,
    ADD COLUMN adoption_receipt JSONB,
    ADD CONSTRAINT migration_adoption_pair CHECK (
        (adoption_request IS NULL AND adoption_digest IS NULL) OR
        (adoption_request IS NOT NULL AND adoption_digest IS NOT NULL AND adoption_digest ~ '^[0-9a-f]{64}$'
            AND generation_committed_at IS NOT NULL AND octet_length(adoption_request::text)<=16384)),
    ADD CONSTRAINT migration_adoption_receipt_requires_intent CHECK (
        adoption_receipt IS NULL OR (adoption_request IS NOT NULL AND octet_length(adoption_receipt::text)<=4096));
-- +goose StatementBegin
CREATE FUNCTION manager.guard_runtime_migration_adoption() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF (OLD.adoption_request IS NOT NULL AND ROW(NEW.adoption_request,NEW.adoption_digest)
        IS DISTINCT FROM ROW(OLD.adoption_request,OLD.adoption_digest)) OR
        (OLD.adoption_receipt IS NOT NULL AND NEW.adoption_receipt IS DISTINCT FROM OLD.adoption_receipt) THEN
        RAISE EXCEPTION 'Migration adoption evidence is immutable' USING ERRCODE = '23514';
    END IF;
    IF OLD.adoption_request IS NULL AND NEW.adoption_request IS NOT NULL AND NOT EXISTS (
        SELECT 1 FROM manager.sandbox_lifecycle_txns WHERE txn_id=NEW.operation_id AND phase='committing'
    ) THEN
        RAISE EXCEPTION 'Migration adoption requires committed handover' USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$;
-- +goose StatementEnd
CREATE TRIGGER runtime_migration_adoption_guard BEFORE UPDATE ON manager.sandbox_runtime_migrations
    FOR EACH ROW EXECUTE FUNCTION manager.guard_runtime_migration_adoption();
-- +goose Down
-- +goose StatementBegin
DO $$ BEGIN
    RAISE EXCEPTION 'Migration adoption evidence cannot be discarded' USING ERRCODE = '55000';
END $$;
-- +goose StatementEnd

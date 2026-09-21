-- +goose Up
ALTER TABLE manager.sandbox_runtime_migrations
    ADD COLUMN source_finalization_request JSONB,
    ADD COLUMN source_finalization_digest TEXT,
    ADD COLUMN source_finalization_receipt JSONB,
    ADD CONSTRAINT migration_source_finalization_pair CHECK (
        (source_finalization_request IS NULL AND source_finalization_digest IS NULL) OR
        (source_finalization_request IS NOT NULL AND source_finalization_digest IS NOT NULL
            AND source_finalization_digest ~ '^[0-9a-f]{64}$'
            AND adoption_receipt IS NOT NULL AND source_fence_proof IS NOT NULL
            AND generation_committed_at IS NOT NULL
            AND octet_length(source_finalization_request::text)<=2097152)),
    ADD CONSTRAINT migration_source_finalization_receipt_requires_intent CHECK (
        source_finalization_receipt IS NULL OR (source_finalization_request IS NOT NULL
            AND octet_length(source_finalization_receipt::text)<=32768));
-- +goose StatementBegin
CREATE FUNCTION manager.guard_runtime_migration_source_finalization() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF (OLD.source_finalization_request IS NOT NULL AND
        ROW(NEW.source_finalization_request,NEW.source_finalization_digest)
        IS DISTINCT FROM ROW(OLD.source_finalization_request,OLD.source_finalization_digest)) OR
        (OLD.source_finalization_receipt IS NOT NULL AND NEW.source_finalization_receipt IS DISTINCT FROM OLD.source_finalization_receipt) THEN
        RAISE EXCEPTION 'Migration source finalization evidence is immutable' USING ERRCODE = '23514';
    END IF;
    IF OLD.source_finalization_request IS NULL AND NEW.source_finalization_request IS NOT NULL AND NOT EXISTS (
        SELECT 1 FROM manager.sandbox_lifecycle_txns WHERE txn_id=NEW.operation_id
            AND kind='migrate' AND source='auto' AND phase='committing'
    ) THEN
        RAISE EXCEPTION 'Migration source finalization requires committed handover' USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$;
-- +goose StatementEnd
CREATE TRIGGER runtime_migration_source_finalization_guard BEFORE UPDATE ON manager.sandbox_runtime_migrations
    FOR EACH ROW EXECUTE FUNCTION manager.guard_runtime_migration_source_finalization();
-- +goose Down
-- +goose StatementBegin
DO $$ BEGIN
    RAISE EXCEPTION 'Migration source finalization evidence cannot be discarded' USING ERRCODE = '55000';
END $$;
-- +goose StatementEnd

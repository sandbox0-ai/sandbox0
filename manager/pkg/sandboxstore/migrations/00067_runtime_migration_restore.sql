-- +goose Up
ALTER TABLE manager.sandbox_runtime_migrations
    ADD COLUMN restore_request JSONB,
    ADD COLUMN restore_digest TEXT,
    ADD CONSTRAINT migration_restore_pair CHECK (
        (restore_request IS NULL AND restore_digest IS NULL) OR
        (restore_request IS NOT NULL AND restore_digest IS NOT NULL
            AND source_fence_proof IS NOT NULL AND target_image_receipt IS NOT NULL
            AND jsonb_typeof(restore_request)='object' AND octet_length(restore_request::text)<=1048576
            AND restore_digest ~ '^[0-9a-f]{64}$'));
-- +goose StatementBegin
CREATE FUNCTION manager.guard_runtime_migration_restore() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE current_phase TEXT;
BEGIN
    IF OLD.restore_request IS NOT NULL AND ROW(NEW.restore_request,NEW.restore_digest)
        IS DISTINCT FROM ROW(OLD.restore_request,OLD.restore_digest) THEN
        RAISE EXCEPTION 'Migration restore authorization is immutable' USING ERRCODE = '23514';
    END IF;
    SELECT phase INTO current_phase FROM manager.sandbox_lifecycle_txns WHERE txn_id=NEW.operation_id;
    IF OLD.restore_request IS NULL AND NEW.restore_request IS NOT NULL AND current_phase <> 'committing' THEN
        RAISE EXCEPTION 'Migration restore requires committed source handoff' USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$;
-- +goose StatementEnd
CREATE TRIGGER runtime_migration_restore_guard BEFORE UPDATE ON manager.sandbox_runtime_migrations
    FOR EACH ROW EXECUTE FUNCTION manager.guard_runtime_migration_restore();
-- +goose Down
-- +goose StatementBegin
DO $$ BEGIN
    RAISE EXCEPTION 'Migration restore authorization cannot be discarded' USING ERRCODE = '55000';
END $$;
-- +goose StatementEnd

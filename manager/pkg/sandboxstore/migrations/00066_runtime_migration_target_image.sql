-- +goose Up
ALTER TABLE manager.sandbox_runtime_migrations
    ADD COLUMN target_image_request JSONB,
    ADD COLUMN target_image_digest TEXT,
    ADD COLUMN target_image_receipt JSONB,
    ADD CONSTRAINT migration_target_image_pair CHECK (
        (target_image_request IS NULL AND target_image_digest IS NULL) OR
        (target_image_request IS NOT NULL AND target_image_digest IS NOT NULL
            AND publication_receipt IS NOT NULL AND jsonb_typeof(target_image_request)='object'
            AND octet_length(target_image_request::text)<=1048576 AND target_image_digest ~ '^[0-9a-f]{64}$')),
    ADD CONSTRAINT migration_target_image_receipt CHECK (
        target_image_receipt IS NULL OR (target_image_request IS NOT NULL
            AND jsonb_typeof(target_image_receipt)='object' AND octet_length(target_image_receipt::text)<=65536));
-- +goose StatementBegin
CREATE FUNCTION manager.guard_runtime_migration_target_image() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE current_phase TEXT;
BEGIN
    IF (OLD.target_image_request IS NOT NULL AND ROW(NEW.target_image_request,NEW.target_image_digest)
        IS DISTINCT FROM ROW(OLD.target_image_request,OLD.target_image_digest))
        OR (OLD.target_image_receipt IS NOT NULL AND NEW.target_image_receipt IS DISTINCT FROM OLD.target_image_receipt) THEN
        RAISE EXCEPTION 'Migration target image evidence is immutable' USING ERRCODE = '23514';
    END IF;
    SELECT phase INTO current_phase FROM manager.sandbox_lifecycle_txns WHERE txn_id=NEW.operation_id;
    IF ((OLD.target_image_request IS NULL AND NEW.target_image_request IS NOT NULL)
        OR (OLD.target_image_receipt IS NULL AND NEW.target_image_receipt IS NOT NULL))
        AND current_phase NOT IN ('publishing','committing') THEN
        RAISE EXCEPTION 'Migration target image requires publication authority' USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$;
-- +goose StatementEnd
CREATE TRIGGER runtime_migration_target_image_guard BEFORE UPDATE ON manager.sandbox_runtime_migrations
    FOR EACH ROW EXECUTE FUNCTION manager.guard_runtime_migration_target_image();
-- +goose Down
-- +goose StatementBegin
DO $$ BEGIN
    RAISE EXCEPTION 'Migration target image evidence cannot be discarded' USING ERRCODE = '55000';
END $$;
-- +goose StatementEnd

-- +goose Up
ALTER TABLE manager.sandbox_runtime_migrations
    ADD COLUMN target_image_prefetch_request JSONB,
    ADD COLUMN target_image_prefetch_digest TEXT,
    ADD CONSTRAINT migration_target_image_prefetch_pair CHECK (
        (target_image_prefetch_request IS NULL AND target_image_prefetch_digest IS NULL) OR
        (target_image_prefetch_request IS NOT NULL AND target_image_prefetch_digest IS NOT NULL
            AND publication_request IS NOT NULL AND staging_request IS NOT NULL
            AND staging_source_receipt IS NOT NULL AND staging_destination_receipt IS NOT NULL
            AND jsonb_typeof(target_image_prefetch_request)='object'
            AND octet_length(target_image_prefetch_request::text)<=1048576
            AND target_image_prefetch_digest ~ '^[0-9a-f]{64}$'));
-- +goose StatementBegin
CREATE FUNCTION manager.guard_runtime_migration_prefetch() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE current_phase TEXT;
BEGIN
    IF OLD.target_image_prefetch_request IS NOT NULL AND
        ROW(NEW.target_image_prefetch_request,NEW.target_image_prefetch_digest)
        IS DISTINCT FROM ROW(OLD.target_image_prefetch_request,OLD.target_image_prefetch_digest) THEN
        RAISE EXCEPTION 'Migration prefetch authority is immutable' USING ERRCODE = '23514';
    END IF;
    IF OLD.target_image_prefetch_request IS NULL AND NEW.target_image_prefetch_request IS NOT NULL THEN
        SELECT phase INTO current_phase FROM manager.sandbox_lifecycle_txns WHERE txn_id=NEW.operation_id;
        IF current_phase IS DISTINCT FROM 'publishing' OR NEW.staging_source_release_requested
            OR NEW.staging_destination_release_requested OR NEW.target_image_request IS NOT NULL
            OR NEW.source_fence_request IS NOT NULL THEN
            RAISE EXCEPTION 'Migration prefetch requires reserved unpublished destination custody' USING ERRCODE = '23514';
        END IF;
    END IF;
    RETURN NEW;
END;
$$;
-- +goose StatementEnd
CREATE TRIGGER runtime_migration_prefetch_guard BEFORE UPDATE ON manager.sandbox_runtime_migrations
    FOR EACH ROW EXECUTE FUNCTION manager.guard_runtime_migration_prefetch();
-- +goose Down
-- +goose StatementBegin
DO $$ BEGIN
    RAISE EXCEPTION 'Migration prefetch authority cannot be discarded' USING ERRCODE = '55000';
END $$;
-- +goose StatementEnd

-- +goose Up
ALTER TABLE manager.rootfs_writer_grants DROP CONSTRAINT rootfs_writer_grants_retire_kind_check;
ALTER TABLE manager.rootfs_writer_grants ADD CONSTRAINT rootfs_writer_grants_retire_kind_check
    CHECK (retire_kind IN ('','planned_publish','prelaunch_abort','crash_abandon','migration'));
ALTER TABLE manager.sandbox_runtime_migrations
    ADD COLUMN source_fence_request JSONB,
    ADD COLUMN source_fence_digest TEXT,
    ADD COLUMN source_fence_proof JSONB,
    ADD CONSTRAINT migration_source_fence_pair CHECK (
        (source_fence_request IS NULL AND source_fence_digest IS NULL) OR
        (source_fence_request IS NOT NULL AND source_fence_digest IS NOT NULL
            AND publication_receipt IS NOT NULL AND jsonb_typeof(source_fence_request) = 'object'
            AND octet_length(source_fence_request::text) <= 1048576 AND source_fence_digest ~ '^[0-9a-f]{64}$')),
    ADD CONSTRAINT migration_source_fence_proof CHECK (
        source_fence_proof IS NULL OR (source_fence_request IS NOT NULL
            AND jsonb_typeof(source_fence_proof) = 'object' AND octet_length(source_fence_proof::text) <= 65536));
-- +goose StatementBegin
CREATE FUNCTION manager.guard_runtime_migration_source_fence() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE current_phase TEXT;
BEGIN
    IF (OLD.source_fence_request IS NOT NULL AND ROW(NEW.source_fence_request,NEW.source_fence_digest)
        IS DISTINCT FROM ROW(OLD.source_fence_request,OLD.source_fence_digest))
        OR (OLD.source_fence_proof IS NOT NULL AND NEW.source_fence_proof IS DISTINCT FROM OLD.source_fence_proof) THEN
        RAISE EXCEPTION 'Migration source fence evidence is immutable' USING ERRCODE = '23514';
    END IF;
    SELECT phase INTO current_phase FROM manager.sandbox_lifecycle_txns WHERE txn_id=NEW.operation_id;
    IF (OLD.source_fence_request IS NULL AND NEW.source_fence_request IS NOT NULL AND current_phase <> 'publishing')
        OR (OLD.source_fence_proof IS NULL AND NEW.source_fence_proof IS NOT NULL AND current_phase <> 'committing') THEN
        RAISE EXCEPTION 'Migration source fence requires its committed lifecycle phase' USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$;
-- +goose StatementEnd
CREATE TRIGGER runtime_migration_source_fence_guard BEFORE UPDATE ON manager.sandbox_runtime_migrations
    FOR EACH ROW EXECUTE FUNCTION manager.guard_runtime_migration_source_fence();
-- +goose Down
-- +goose StatementBegin
DO $$ BEGIN
    RAISE EXCEPTION 'Migration physical fence evidence cannot be discarded' USING ERRCODE = '55000';
END $$;
-- +goose StatementEnd

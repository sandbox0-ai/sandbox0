-- +goose Up
-- Writer identity is immutable audit evidence; its live FK is a separate
-- retention pin. Terminal migration history must not retain deleted RootFSs.
ALTER TABLE manager.sandbox_runtime_migrations
    ADD COLUMN source_writer_grant_ref TEXT REFERENCES manager.rootfs_writer_grants(grant_id) ON DELETE RESTRICT,
    ADD COLUMN storage_released_at TIMESTAMPTZ;
UPDATE manager.sandbox_runtime_migrations SET source_writer_grant_ref=source_writer_grant_id;
ALTER TABLE manager.sandbox_runtime_migrations
    DROP CONSTRAINT sandbox_runtime_migrations_source_writer_grant_id_fkey,
    ADD CONSTRAINT migration_storage_retention_shape CHECK (
        (source_writer_grant_ref IS NOT NULL AND source_writer_grant_ref=source_writer_grant_id AND storage_released_at IS NULL)
        OR (source_writer_grant_ref IS NULL AND storage_released_at IS NOT NULL));

-- +goose StatementBegin
CREATE FUNCTION manager.runtime_migration_storage_releasable(id TEXT) RETURNS BOOLEAN
LANGUAGE sql STABLE AS $$
    SELECT EXISTS (
        SELECT 1 FROM manager.sandbox_runtime_migrations m
        JOIN manager.sandbox_lifecycle_txns l ON l.txn_id=m.operation_id
        JOIN manager.runtime_slots source ON source.slot_id=m.source_slot_id
        JOIN manager.runtime_slots target ON target.slot_id=m.target_slot_id
        JOIN manager.runtime_resource_leases sl ON sl.lease_id=source.resource_lease_id
        JOIN manager.runtime_resource_leases tl ON tl.lease_id=m.target_resource_lease_id
        JOIN manager.rootfs_writer_grants writer ON writer.grant_id=m.source_writer_grant_id
        WHERE m.operation_id=id
            AND source.state='terminal' AND octet_length(source.terminal_proof_digest)=32
            AND target.state='terminal' AND octet_length(target.terminal_proof_digest)=32
            AND sl.lease_state='released' AND sl.released_at IS NOT NULL
            AND tl.lease_state='released' AND tl.released_at IS NOT NULL
            AND writer.state IN ('retired','canceled')
            AND ((l.phase='aborted' AND m.capture_request IS NULL
                    AND (m.preparation_request IS NULL OR m.preparation_cancel_receipt IS NOT NULL))
                OR (l.phase='committed' AND m.generation_committed_at IS NOT NULL
                    AND m.source_finalization_receipt IS NOT NULL))
            AND (m.staging_request IS NULL OR
                (m.staging_source_release_receipt IS NOT NULL AND m.staging_destination_release_receipt IS NOT NULL))
    );
$$;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE FUNCTION manager.guard_runtime_migration_storage_retention() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP='INSERT' THEN
        IF NEW.storage_released_at IS NOT NULL OR
            (NEW.source_writer_grant_ref IS NOT NULL AND NEW.source_writer_grant_ref<>NEW.source_writer_grant_id) THEN
            RAISE EXCEPTION 'Migration must retain its source writer' USING ERRCODE='23514';
        END IF;
        NEW.source_writer_grant_ref := NEW.source_writer_grant_id;
    ELSIF ROW(NEW.source_writer_grant_ref,NEW.storage_released_at)
        IS DISTINCT FROM ROW(OLD.source_writer_grant_ref,OLD.storage_released_at) THEN
        IF OLD.storage_released_at IS NOT NULL OR NEW.source_writer_grant_ref IS NOT NULL
            OR NEW.storage_released_at IS NULL OR NOT manager.runtime_migration_storage_releasable(OLD.operation_id) THEN
            RAISE EXCEPTION 'Migration storage release requires terminal custody and staging cleanup' USING ERRCODE='23514';
        END IF;
    END IF;
    RETURN NEW;
END;
$$;
-- +goose StatementEnd
CREATE TRIGGER runtime_migration_storage_retention_guard BEFORE INSERT OR UPDATE ON manager.sandbox_runtime_migrations
    FOR EACH ROW EXECUTE FUNCTION manager.guard_runtime_migration_storage_retention();

-- +goose Down
-- +goose StatementBegin
DO $$ BEGIN
    RAISE EXCEPTION 'Released migration storage cannot reacquire a deleted writer' USING ERRCODE='55000';
END $$;
-- +goose StatementEnd

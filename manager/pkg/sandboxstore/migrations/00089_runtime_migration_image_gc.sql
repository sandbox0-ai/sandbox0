-- +goose Up
ALTER TABLE manager.sandbox_runtime_migrations
    ADD COLUMN image_gc_binding_digest TEXT,
    ADD COLUMN image_gc_completed_at TIMESTAMPTZ;

-- +goose StatementBegin
CREATE FUNCTION manager.runtime_migration_image_releasable(id TEXT) RETURNS BOOLEAN
LANGUAGE sql STABLE AS $$
    SELECT EXISTS (
        SELECT 1 FROM manager.sandbox_runtime_migrations m
        JOIN manager.sandbox_lifecycle_txns l ON l.txn_id=m.operation_id
        JOIN manager.runtime_slots src ON src.slot_id=m.source_slot_id
        JOIN manager.runtime_resource_leases sl ON sl.lease_id=src.resource_lease_id
        WHERE m.operation_id=id AND l.kind='migrate' AND l.source='auto' AND NOT l.cancelable
            AND m.publication_receipt IS NOT NULL
            AND m.source_finalization_receipt IS NOT NULL
            AND src.state='terminal' AND octet_length(src.terminal_proof_digest)=32
            AND sl.lease_state='released' AND sl.released_at IS NOT NULL
            AND m.staging_source_release_receipt IS NOT NULL
            AND m.staging_destination_release_receipt IS NOT NULL
            AND ((l.phase='committed' AND m.generation_committed_at IS NOT NULL AND m.adoption_receipt IS NOT NULL)
                OR (l.phase='aborted' AND m.failure_completed_at IS NOT NULL AND m.failure_finalization_receipt IS NOT NULL))
    );
$$;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE FUNCTION manager.guard_runtime_migration_image_gc() RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    IF OLD.image_gc_binding_digest IS NOT NULL AND NEW.image_gc_binding_digest IS DISTINCT FROM OLD.image_gc_binding_digest
        OR OLD.image_gc_completed_at IS NOT NULL AND NEW.image_gc_completed_at IS DISTINCT FROM OLD.image_gc_completed_at THEN
        RAISE EXCEPTION 'Migration image reclamation is immutable' USING ERRCODE='23514';
    END IF;
    IF NEW.image_gc_binding_digest IS NOT NULL AND (
        NEW.image_gc_binding_digest IS DISTINCT FROM NEW.publication_receipt->'reference'->>'binding_digest'
        OR NEW.image_gc_binding_digest !~ '^sha256:[0-9a-f]{64}$'
        OR NOT manager.runtime_migration_image_releasable(NEW.operation_id)) THEN
        RAISE EXCEPTION 'Migration image reclamation lacks terminal custody' USING ERRCODE='23514';
    END IF;
    IF NEW.image_gc_completed_at IS NOT NULL AND NEW.image_gc_binding_digest IS NULL THEN
        RAISE EXCEPTION 'Migration image reclamation lacks binding' USING ERRCODE='23514';
    END IF;
    RETURN NEW;
END;
$$;
-- +goose StatementEnd
CREATE TRIGGER runtime_migration_image_gc_guard BEFORE UPDATE OF image_gc_binding_digest,image_gc_completed_at
    ON manager.sandbox_runtime_migrations FOR EACH ROW EXECUTE FUNCTION manager.guard_runtime_migration_image_gc();

-- +goose Down
-- +goose StatementBegin
DO $$ BEGIN
    RAISE EXCEPTION 'Migration image reclamation authority cannot be discarded' USING ERRCODE='55000';
END $$;
-- +goose StatementEnd

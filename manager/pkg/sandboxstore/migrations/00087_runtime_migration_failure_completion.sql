-- +goose Up
ALTER TABLE manager.sandbox_runtime_migrations
    ADD COLUMN failure_allocation_gc_request JSONB,
    ADD COLUMN failure_allocation_gc_receipt JSONB,
    ADD COLUMN failure_completed_at TIMESTAMPTZ,
    ADD CONSTRAINT migration_failure_completion_shape CHECK (
        (failure_completed_at IS NULL AND failure_allocation_gc_request IS NULL AND failure_allocation_gc_receipt IS NULL) OR
        (failure_completed_at IS NOT NULL AND failure_allocation_gc_request IS NOT NULL AND failure_allocation_gc_receipt IS NOT NULL
            AND failure_finalization_receipt IS NOT NULL AND source_finalization_receipt IS NOT NULL
            AND octet_length(failure_allocation_gc_request::text)<=8192 AND octet_length(failure_allocation_gc_receipt::text)<=4096));

-- +goose StatementBegin
CREATE FUNCTION manager.guard_runtime_migration_failure_completion() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF OLD.failure_completed_at IS NOT NULL AND ROW(NEW.failure_completed_at,NEW.failure_allocation_gc_request,NEW.failure_allocation_gc_receipt)
        IS DISTINCT FROM ROW(OLD.failure_completed_at,OLD.failure_allocation_gc_request,OLD.failure_allocation_gc_receipt) THEN
        RAISE EXCEPTION 'Failed migration completion is immutable' USING ERRCODE='23514';
    END IF;
    RETURN NEW;
END;
$$;
-- +goose StatementEnd
CREATE TRIGGER runtime_migration_failure_completion_guard BEFORE UPDATE ON manager.sandbox_runtime_migrations
    FOR EACH ROW EXECUTE FUNCTION manager.guard_runtime_migration_failure_completion();

-- +goose StatementBegin
CREATE FUNCTION manager.check_runtime_migration_failed_terminal() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE op TEXT;
BEGIN
    IF TG_TABLE_NAME='sandbox_runtime_migrations' THEN
        op := NEW.operation_id;
    ELSIF TG_TABLE_NAME='sandbox_lifecycle_txns' THEN
        op := NEW.txn_id;
    ELSE
        SELECT operation_id INTO op FROM manager.sandbox_runtime_migrations WHERE target_slot_id=NEW.slot_id;
    END IF;
    IF EXISTS (SELECT 1 FROM manager.sandbox_runtime_migrations m JOIN manager.sandbox_lifecycle_txns l ON l.txn_id=m.operation_id
        JOIN manager.runtime_slots t ON t.slot_id=m.target_slot_id
        WHERE m.operation_id=op AND m.failure_request IS NOT NULL
            AND (m.failure_completed_at IS NOT NULL OR l.phase='aborted' OR t.state='terminal')) AND NOT EXISTS (
        SELECT 1 FROM manager.sandbox_runtime_migrations m JOIN manager.sandbox_lifecycle_txns l ON l.txn_id=m.operation_id
        JOIN manager.runtime_slots s ON s.slot_id=m.source_slot_id
        JOIN manager.runtime_slots t ON t.slot_id=m.target_slot_id
        JOIN manager.runtime_resource_leases sl ON sl.lease_id=s.resource_lease_id
        JOIN manager.runtime_resource_leases tl ON tl.lease_id=t.resource_lease_id
        WHERE m.operation_id=op AND m.failure_completed_at IS NOT NULL
            AND m.failure_finalization_receipt IS NOT NULL AND m.source_finalization_receipt IS NOT NULL
            AND m.failure_allocation_gc_request IS NOT NULL AND m.failure_allocation_gc_receipt IS NOT NULL
            AND l.phase='aborted' AND l.aborted_at IS NOT NULL
            AND s.state='terminal' AND s.terminal_reason='migration_source'
            AND t.state='terminal' AND t.terminal_reason='migration_failed'
            AND octet_length(s.orphan_observation_digest)=32 AND octet_length(t.orphan_observation_digest)=32
            AND octet_length(s.terminal_proof_digest)=32 AND octet_length(t.terminal_proof_digest)=32
            AND sl.lease_state='released' AND sl.released_at IS NOT NULL
            AND tl.lease_state='released' AND tl.released_at IS NOT NULL
    ) THEN
        RAISE EXCEPTION 'Failed migration terminal state requires both physical releases and allocation acknowledgement' USING ERRCODE='23514';
    END IF;
    RETURN NULL;
END;
$$;
-- +goose StatementEnd
CREATE CONSTRAINT TRIGGER runtime_migration_failed_terminal_check AFTER UPDATE ON manager.sandbox_runtime_migrations
    DEFERRABLE INITIALLY DEFERRED FOR EACH ROW WHEN (NEW.failure_request IS NOT NULL)
    EXECUTE FUNCTION manager.check_runtime_migration_failed_terminal();
CREATE CONSTRAINT TRIGGER runtime_migration_failed_lifecycle_check AFTER UPDATE OF phase ON manager.sandbox_lifecycle_txns
    DEFERRABLE INITIALLY DEFERRED FOR EACH ROW WHEN (NEW.kind='migrate') EXECUTE FUNCTION manager.check_runtime_migration_failed_terminal();
CREATE CONSTRAINT TRIGGER runtime_migration_failed_slot_check AFTER UPDATE OF state ON manager.runtime_slots
    DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION manager.check_runtime_migration_failed_terminal();

-- +goose StatementBegin
CREATE OR REPLACE FUNCTION manager.runtime_migration_storage_releasable(id TEXT) RETURNS BOOLEAN
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
                OR (l.phase='aborted' AND m.failure_completed_at IS NOT NULL AND m.failure_finalization_receipt IS NOT NULL)
                OR (l.phase='committed' AND m.generation_committed_at IS NOT NULL
                    AND m.source_finalization_receipt IS NOT NULL))
            AND (m.staging_request IS NULL OR
                (m.staging_source_release_receipt IS NOT NULL AND m.staging_destination_release_receipt IS NOT NULL))
    );
$$;
-- +goose StatementEnd
-- +goose Down
-- +goose StatementBegin
DO $$ BEGIN
    RAISE EXCEPTION 'Failed migration terminal evidence cannot be discarded' USING ERRCODE='55000';
END $$;
-- +goose StatementEnd

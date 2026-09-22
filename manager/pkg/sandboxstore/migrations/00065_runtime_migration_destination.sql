-- +goose Up
-- A fenced predecessor retains physical custody and its resource lease. It is
-- not terminal, but no longer prevents the exact migration successor claim.
ALTER TABLE manager.runtime_slots ADD COLUMN migration_source_operation_id TEXT
    REFERENCES manager.sandbox_runtime_migrations(operation_id) ON DELETE RESTRICT;
DROP INDEX manager.idx_runtime_slots_live_sandbox;
CREATE UNIQUE INDEX idx_runtime_slots_live_sandbox ON manager.runtime_slots(sandbox_id)
    WHERE sandbox_id IS NOT NULL AND state <> 'terminal' AND migration_source_operation_id IS NULL;

-- +goose StatementBegin
CREATE FUNCTION manager.guard_runtime_migration_predecessor() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF OLD.migration_source_operation_id IS NOT NULL
        AND NEW.migration_source_operation_id IS DISTINCT FROM OLD.migration_source_operation_id THEN
        RAISE EXCEPTION 'Migration predecessor custody is immutable' USING ERRCODE = '23514';
    END IF;
    IF NEW.migration_source_operation_id IS NOT NULL AND NEW.state NOT IN ('quiescing','orphaned','terminal') THEN
        RAISE EXCEPTION 'Migration predecessor cannot execute again' USING ERRCODE = '23514';
    END IF;
    IF OLD.migration_source_operation_id IS NULL AND NEW.migration_source_operation_id IS NOT NULL
        AND NOT EXISTS (
            SELECT 1 FROM manager.sandbox_runtime_migrations migration
            JOIN manager.sandbox_lifecycle_txns lifecycle ON lifecycle.txn_id=migration.operation_id
            JOIN manager.rootfs_writer_grants writer ON writer.grant_id=migration.source_writer_grant_id
            WHERE migration.operation_id=NEW.migration_source_operation_id
                AND migration.source_slot_id=NEW.slot_id AND migration.source_fence_proof IS NOT NULL
                AND lifecycle.kind='migrate' AND lifecycle.source='auto' AND lifecycle.phase='committing'
                AND lifecycle.sandbox_id=NEW.sandbox_id
                AND lifecycle.from_runtime_id=NEW.allocation_id AND lifecycle.from_runtime_namespace=NEW.allocation_namespace
                AND writer.grant_id=NEW.writer_grant_id AND writer.state='retired'
                AND writer.retire_kind='migration' AND writer.retire_operation_id=lifecycle.txn_id
                AND writer.retire_proof_digest=decode(migration.source_fence_proof->>'digest','hex')
        ) THEN
        RAISE EXCEPTION 'Migration predecessor requires committed physical fencing' USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$;
-- +goose StatementEnd
CREATE TRIGGER runtime_migration_predecessor_guard BEFORE INSERT OR UPDATE ON manager.runtime_slots
    FOR EACH ROW EXECUTE FUNCTION manager.guard_runtime_migration_predecessor();

-- +goose Down
-- +goose StatementBegin
DO $$ BEGIN
    RAISE EXCEPTION 'Migration predecessor custody cannot be discarded' USING ERRCODE = '55000';
END $$;
-- +goose StatementEnd

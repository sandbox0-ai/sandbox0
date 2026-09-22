-- +goose Up
-- Prevent a generic lifecycle phase update from completing migration before
-- the predecessor's physical cleanup and shared resource-ledger transition.
-- +goose StatementBegin
CREATE FUNCTION manager.guard_runtime_migration_completion() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.kind='migrate' AND NEW.phase='committed' AND NOT EXISTS (
        SELECT 1 FROM manager.sandbox_runtime_migrations migration
        JOIN manager.runtime_slots source ON source.slot_id=migration.source_slot_id
        JOIN manager.runtime_resource_leases lease ON lease.lease_id=source.resource_lease_id
        WHERE migration.operation_id=NEW.txn_id
            AND migration.source_finalization_receipt IS NOT NULL
            AND migration.generation_committed_at IS NOT NULL
            AND source.migration_source_operation_id=NEW.txn_id
            AND source.state='terminal' AND source.terminal_reason='migration_source'
            AND octet_length(source.terminal_proof_digest)=32
            AND octet_length(source.orphan_observation_digest)=32
            AND lease.lease_state='released' AND lease.released_at IS NOT NULL
    ) THEN
        RAISE EXCEPTION 'Migration completion requires terminal source cleanup and lease release' USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$;
-- +goose StatementEnd
CREATE TRIGGER runtime_migration_completion_guard BEFORE UPDATE OF phase ON manager.sandbox_lifecycle_txns
    FOR EACH ROW EXECUTE FUNCTION manager.guard_runtime_migration_completion();
-- +goose Down
DROP TRIGGER runtime_migration_completion_guard ON manager.sandbox_lifecycle_txns;
DROP FUNCTION manager.guard_runtime_migration_completion();

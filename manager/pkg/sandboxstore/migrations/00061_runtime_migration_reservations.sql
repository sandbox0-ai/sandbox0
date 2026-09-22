-- +goose Up
-- Migration is an extension of the existing lifecycle transaction, not a
-- second lifecycle or capacity authority. A destination reservation is an
-- ordinary resource lease that has not yet been attached to a physical claim.
CREATE TABLE manager.sandbox_runtime_migrations (
    operation_id TEXT PRIMARY KEY REFERENCES manager.sandbox_lifecycle_txns(txn_id) ON DELETE RESTRICT,
    assignment_digest TEXT NOT NULL CHECK (assignment_digest ~ '^[0-9a-f]{64}$'),
    source_slot_id TEXT NOT NULL REFERENCES manager.runtime_slots(slot_id) ON DELETE RESTRICT,
    target_slot_id TEXT NOT NULL UNIQUE REFERENCES manager.runtime_slots(slot_id) ON DELETE RESTRICT,
    target_resource_lease_id TEXT NOT NULL UNIQUE REFERENCES manager.runtime_resource_leases(lease_id) ON DELETE RESTRICT,
    source_writer_grant_id TEXT NOT NULL REFERENCES manager.rootfs_writer_grants(grant_id) ON DELETE RESTRICT,
    source_binding_digest BYTEA NOT NULL CHECK (octet_length(source_binding_digest) = 32),
    source_procd_instance_id TEXT NOT NULL CHECK (octet_length(source_procd_instance_id) BETWEEN 1 AND 256),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CHECK (source_slot_id <> target_slot_id)
);

CREATE INDEX idx_sandbox_migration_reservation_expiry
    ON manager.sandbox_lifecycle_txns (created_at, txn_id)
    WHERE kind = 'migrate' AND source = 'auto' AND phase = 'preparing';

-- +goose StatementBegin
CREATE FUNCTION manager.guard_runtime_migration_reservation() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM manager.sandbox_lifecycle_txns lifecycle
        JOIN manager.runtime_slots source ON source.slot_id = NEW.source_slot_id
        JOIN manager.runtime_slots target ON target.slot_id = NEW.target_slot_id
        JOIN manager.runtime_resource_leases lease ON lease.lease_id = NEW.target_resource_lease_id
        WHERE lifecycle.txn_id = NEW.operation_id AND lifecycle.kind = 'migrate'
            AND lifecycle.source = 'auto' AND NOT lifecycle.cancelable
            AND lifecycle.phase = 'preparing'
            AND lifecycle.from_generation > 0
            AND lifecycle.to_generation > lifecycle.from_generation
            AND lifecycle.to_generation - lifecycle.from_generation = 1
            AND source.sandbox_id = lifecycle.sandbox_id AND source.state = 'active'
            AND source.allocation_id = lifecycle.from_runtime_id
            AND source.allocation_namespace = lifecycle.from_runtime_namespace
            AND source.writer_grant_id = NEW.source_writer_grant_id
            AND source.procd_instance_id = NEW.source_procd_instance_id
            AND source.rootfs_binding_digest = NEW.source_binding_digest
            AND target.cluster_id = source.cluster_id
            AND target.node_id <> source.node_id AND target.node_uid <> source.node_uid
            AND target.compatibility_digest = source.compatibility_digest
            AND target.state = 'fastpath_ready' AND NOT target.carrier_retired
            AND target.sandbox_id IS NULL AND target.resource_lease_id IS NULL
            AND target.allocation_id = lifecycle.to_runtime_id
            AND target.allocation_namespace = lifecycle.to_runtime_namespace
            AND lease.slot_id = target.slot_id AND lease.operation_id = lifecycle.txn_id
            AND lease.lease_state = 'active'
    ) THEN
        RAISE EXCEPTION 'Migration reservation does not match lifecycle and resource authority' USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$;
-- +goose StatementEnd
CREATE TRIGGER runtime_migration_reservation_guard BEFORE INSERT ON manager.sandbox_runtime_migrations
    FOR EACH ROW EXECUTE FUNCTION manager.guard_runtime_migration_reservation();

-- +goose Down
-- +goose StatementBegin
DO $$ BEGIN
    RAISE EXCEPTION 'Migration reservations must retain their lifecycle and capacity evidence' USING ERRCODE = '55000';
END $$;
-- +goose StatementEnd

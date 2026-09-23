-- +goose Up
ALTER TABLE manager.sandbox_runtime_checkpoints
    ADD COLUMN image_gc_binding_digest TEXT,
    ADD COLUMN image_gc_completed_at TIMESTAMPTZ;
CREATE INDEX idx_runtime_checkpoint_image_gc_pending ON manager.sandbox_runtime_checkpoints(operation_id)
    WHERE image_gc_completed_at IS NULL AND evidence ? 'published';

-- +goose StatementBegin
CREATE FUNCTION manager.runtime_checkpoint_image_releasable(id TEXT) RETURNS BOOLEAN
LANGUAGE sql STABLE AS $$
    SELECT EXISTS (
        SELECT 1 FROM manager.sandbox_runtime_checkpoints c
        JOIN manager.sandbox_lifecycle_txns l ON l.txn_id=c.operation_id
        JOIN manager.runtime_slots src ON src.slot_id=c.source_slot_id
        JOIN manager.runtime_resource_leases lease ON lease.lease_id=src.resource_lease_id
        WHERE c.operation_id=id AND c.evidence ?& ARRAY['publication','published','retained']
            AND src.state='terminal' AND octet_length(src.terminal_proof_digest)=32
            AND lease.lease_state='released' AND lease.released_at IS NOT NULL
            AND ((l.phase='committed' AND c.evidence ? 'finalized')
                OR (l.phase='aborted' AND c.evidence ? 'capture_failure_gc_ack'))
            AND NOT EXISTS (SELECT 1 FROM manager.sandbox_runtime_checkpoint_refs ref
                WHERE ref.checkpoint_id=c.operation_id)
            AND NOT EXISTS (SELECT 1 FROM manager.sandbox_runtime_checkpoint_restores r
                JOIN manager.sandbox_lifecycle_txns restore_l ON restore_l.txn_id=r.operation_id
                WHERE r.checkpoint_id=c.operation_id AND restore_l.phase IN ('preparing','barriered','publishing','committing'))
            AND NOT EXISTS (SELECT 1 FROM manager.sandbox_runtime_checkpoint_forks fork
                JOIN manager.sandbox_lifecycle_txns fork_l ON fork_l.txn_id=fork.operation_id
                WHERE fork.checkpoint_id=c.operation_id AND fork_l.phase IN ('preparing','barriered','publishing','committing'))
    );
$$;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE FUNCTION manager.guard_runtime_checkpoint_image_gc() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF OLD.image_gc_binding_digest IS NOT NULL AND NEW.image_gc_binding_digest IS DISTINCT FROM OLD.image_gc_binding_digest
        OR OLD.image_gc_completed_at IS NOT NULL AND NEW.image_gc_completed_at IS DISTINCT FROM OLD.image_gc_completed_at THEN
        RAISE EXCEPTION 'Checkpoint image reclamation is immutable' USING ERRCODE='23514';
    END IF;
    IF NEW.image_gc_binding_digest IS NOT NULL AND (
        NEW.image_gc_binding_digest IS DISTINCT FROM NEW.evidence->'published'->'reference'->>'binding_digest'
        OR NEW.image_gc_binding_digest !~ '^sha256:[0-9a-f]{64}$'
        OR NOT manager.runtime_checkpoint_image_releasable(NEW.operation_id)) THEN
        RAISE EXCEPTION 'Checkpoint image reclamation lacks terminal custody' USING ERRCODE='23514';
    END IF;
    IF NEW.image_gc_completed_at IS NOT NULL AND NEW.image_gc_binding_digest IS NULL THEN
        RAISE EXCEPTION 'Checkpoint image reclamation lacks binding' USING ERRCODE='23514';
    END IF;
    RETURN NEW;
END;
$$;
-- +goose StatementEnd
CREATE TRIGGER runtime_checkpoint_image_gc_guard BEFORE UPDATE OF image_gc_binding_digest,image_gc_completed_at
    ON manager.sandbox_runtime_checkpoints FOR EACH ROW EXECUTE FUNCTION manager.guard_runtime_checkpoint_image_gc();

-- +goose Down
-- +goose StatementBegin
DO $$ BEGIN RAISE EXCEPTION 'Checkpoint image reclamation authority cannot be discarded' USING ERRCODE='55000'; END $$;
-- +goose StatementEnd

-- +goose Up
-- Source-only captures reuse the migration physical failure protocol. Their
-- evidence remains in the owning checkpoint transaction, never a second owner.
CREATE UNIQUE INDEX idx_runtime_checkpoint_failed_source ON manager.sandbox_runtime_checkpoints(source_slot_id)
    WHERE evidence ? 'capture_failure';
-- Paused status polling must not scan lifecycle history as it grows.
CREATE INDEX idx_runtime_checkpoint_failed_projection ON manager.sandbox_lifecycle_txns(sandbox_id,epoch,from_generation)
    WHERE kind='pause' AND source='manual' AND phase='aborted';

-- +goose StatementBegin
CREATE FUNCTION manager.guard_runtime_checkpoint_capture_failure() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE phase TEXT;
BEGIN
    IF NEW.evidence ? 'capture_failure' THEN
        SELECT l.phase INTO phase FROM manager.sandbox_lifecycle_txns l WHERE l.txn_id=NEW.operation_id;
        IF TG_OP='INSERT' OR phase NOT IN ('publishing','aborted')
            OR NOT NEW.evidence ? 'capture_authorized' OR NEW.evidence ?| ARRAY['cancel_authorized','publication']
            OR NEW.evidence->'capture_failure'->'capture'->>'state' IS DISTINCT FROM 'uncertain'
            OR NEW.evidence->'capture_failure'->'capture' ? 'rootfs'
            OR NEW.evidence->'capture_failure'->'capture'->'request' IS DISTINCT FROM NEW.evidence->'preflight'->'source'
            OR NEW.evidence->'capture_failure'->'cleanup'->>'writer_grant_id' IS DISTINCT FROM NEW.source_writer_grant_id THEN
            RAISE EXCEPTION 'Checkpoint failure changed uncaptured publication custody' USING ERRCODE='23514';
        END IF;
        IF NOT OLD.evidence ? 'capture_failure' AND NOT EXISTS (
            SELECT 1 FROM manager.rootfs_writer_grants w JOIN manager.runtime_slots s ON s.slot_id=NEW.source_slot_id
            WHERE w.grant_id=NEW.source_writer_grant_id AND w.state='retiring' AND w.retire_kind='crash_abandon'
                AND w.retire_operation_id=NEW.evidence->'capture_failure'->'cleanup'->>'writer_operation_id'
                AND s.state IN ('quiescing','orphaned') AND s.carrier_retired) THEN
            RAISE EXCEPTION 'Checkpoint failure requires fenced writer and source allocation' USING ERRCODE='23514';
        END IF;
    END IF;
    IF NEW.evidence ? 'capture_failure_cleanup' AND NOT NEW.evidence ? 'capture_failure'
        OR NEW.evidence ? 'capture_failure_finalized' AND NOT NEW.evidence ? 'capture_failure_cleanup'
        OR NEW.evidence ? 'capture_failure_staging_released' AND NOT NEW.evidence ? 'capture_failure_finalized'
        OR (NEW.evidence ? 'capture_failure_gc') IS DISTINCT FROM (NEW.evidence ? 'capture_failure_gc_ack')
        OR NEW.evidence ? 'capture_failure_gc_ack' AND NOT NEW.evidence ? 'capture_failure_staging_released' THEN
        RAISE EXCEPTION 'Checkpoint failure lacks prerequisite physical receipts' USING ERRCODE='23514';
    END IF;
    RETURN NEW;
END;
$$;
-- +goose StatementEnd
CREATE TRIGGER runtime_checkpoint_capture_failure_guard BEFORE INSERT OR UPDATE ON manager.sandbox_runtime_checkpoints
    FOR EACH ROW EXECUTE FUNCTION manager.guard_runtime_checkpoint_capture_failure();

-- +goose StatementBegin
CREATE OR REPLACE FUNCTION manager.guard_runtime_checkpoint_completion() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE c manager.sandbox_runtime_checkpoints%ROWTYPE;
BEGIN
    SELECT * INTO c FROM manager.sandbox_runtime_checkpoints WHERE operation_id=NEW.txn_id;
    IF NOT FOUND THEN RETURN NEW; END IF;
    IF NEW.phase='committed' AND OLD.phase<>'committed' AND (
        NOT c.evidence ? 'finalized' OR NOT EXISTS (
            SELECT 1 FROM manager.runtime_slots slot
            JOIN manager.runtime_resource_leases lease ON lease.lease_id=slot.resource_lease_id
            JOIN manager.sandboxes owner ON owner.sandbox_id=NEW.sandbox_id
            WHERE slot.slot_id=c.source_slot_id AND slot.state='terminal' AND slot.terminal_reason='memory_pause'
                AND octet_length(slot.orphan_observation_digest)=32 AND octet_length(slot.terminal_proof_digest)=32
                AND lease.lease_state='released' AND lease.released_at IS NOT NULL
                AND (owner.desired_state IN ('terminating','deleted') OR
                    (owner.desired_state='paused' AND owner.runtime_id='' AND owner.runtime_namespace=''
                        AND owner.runtime_generation=NEW.from_generation))
                AND (owner.desired_state='deleted' OR EXISTS (SELECT 1 FROM manager.sandbox_runtime_checkpoint_refs r
                    WHERE r.sandbox_id=NEW.sandbox_id AND r.checkpoint_id=NEW.txn_id AND r.generation_id=NEW.prepared_generation_id))
        )) THEN
        RAISE EXCEPTION 'Memory pause completion requires retained image, source cleanup and released capacity' USING ERRCODE='23514';
    END IF;
    IF NEW.phase='aborted' AND OLD.phase<>'aborted' AND c.evidence ? 'capture_authorized' AND NOT c.evidence ? 'capture_failure_gc_ack' THEN
        RAISE EXCEPTION 'Authorized memory capture requires physical failure resolution' USING ERRCODE='23514';
    END IF;
    RETURN NEW;
END;
$$;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE FUNCTION manager.check_runtime_checkpoint_capture_failed_terminal() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE op TEXT;
BEGIN
    IF TG_TABLE_NAME='sandbox_runtime_checkpoints' THEN op := NEW.operation_id;
    ELSIF TG_TABLE_NAME='sandbox_lifecycle_txns' THEN op := NEW.txn_id;
    ELSE SELECT operation_id INTO op FROM manager.sandbox_runtime_checkpoints
        WHERE source_slot_id=NEW.slot_id AND evidence ? 'capture_failure';
    END IF;
    IF EXISTS (SELECT 1 FROM manager.sandbox_runtime_checkpoints c
        JOIN manager.sandbox_lifecycle_txns l ON l.txn_id=c.operation_id
        JOIN manager.runtime_slots s ON s.slot_id=c.source_slot_id
        WHERE c.operation_id=op AND c.evidence ? 'capture_failure'
            AND (c.evidence ? 'capture_failure_gc_ack' OR l.phase='aborted' OR s.state='terminal')) AND NOT EXISTS (
        SELECT 1 FROM manager.sandbox_runtime_checkpoints c
        JOIN manager.sandbox_lifecycle_txns l ON l.txn_id=c.operation_id
        JOIN manager.runtime_slots s ON s.slot_id=c.source_slot_id
        JOIN manager.runtime_resource_leases lease ON lease.lease_id=s.resource_lease_id
        JOIN manager.rootfs_writer_grants w ON w.grant_id=c.source_writer_grant_id
        JOIN manager.rootfs_filesystems fs ON fs.filesystem_id=w.filesystem_id
        JOIN manager.sandboxes owner ON owner.sandbox_id=l.sandbox_id
        WHERE c.operation_id=op AND c.evidence ?& ARRAY['capture_failure_finalized','capture_failure_staging_released','capture_failure_gc','capture_failure_gc_ack']
            AND l.phase='aborted' AND l.aborted_at IS NOT NULL
            AND s.state='terminal' AND s.terminal_reason='migration_capture_failed'
            AND octet_length(s.orphan_observation_digest)=32 AND octet_length(s.terminal_proof_digest)=32
            AND encode(s.orphan_observation_digest,'hex')=c.evidence->'capture_failure_gc'->>'allocation_absence_digest'
            AND lease.lease_state='released' AND lease.released_at IS NOT NULL
            AND w.state='retired' AND w.retire_kind='crash_abandon'
            AND encode(w.retire_proof_digest,'hex')=c.evidence->'capture_failure_cleanup'->'cleanup'->>'proof_digest'
            AND fs.head_generation_id=l.expected_generation_id
            AND owner.runtime_id='' AND owner.runtime_namespace='' AND owner.runtime_generation=l.from_generation
            AND owner.desired_state IN ('paused','terminating','deleted')
    ) THEN
        RAISE EXCEPTION 'Checkpoint failure completion requires physical cleanup and released source custody' USING ERRCODE='23514';
    END IF;
    RETURN NULL;
END;
$$;
-- +goose StatementEnd
CREATE CONSTRAINT TRIGGER runtime_checkpoint_capture_failed_terminal_check AFTER UPDATE ON manager.sandbox_runtime_checkpoints
    DEFERRABLE INITIALLY DEFERRED FOR EACH ROW WHEN (NEW.evidence ? 'capture_failure')
    EXECUTE FUNCTION manager.check_runtime_checkpoint_capture_failed_terminal();
CREATE CONSTRAINT TRIGGER runtime_checkpoint_capture_failed_lifecycle_check AFTER UPDATE OF phase ON manager.sandbox_lifecycle_txns
    DEFERRABLE INITIALLY DEFERRED FOR EACH ROW WHEN (NEW.kind='pause' AND NEW.source='manual')
    EXECUTE FUNCTION manager.check_runtime_checkpoint_capture_failed_terminal();
CREATE CONSTRAINT TRIGGER runtime_checkpoint_capture_failed_slot_check AFTER UPDATE OF state ON manager.runtime_slots
    DEFERRABLE INITIALLY DEFERRED FOR EACH ROW WHEN (NEW.state='terminal')
    EXECUTE FUNCTION manager.check_runtime_checkpoint_capture_failed_terminal();

-- +goose Down
-- +goose StatementBegin
DO $$ BEGIN
    RAISE EXCEPTION 'Checkpoint capture failure authority must be retained' USING ERRCODE='55000';
END $$;
-- +goose StatementEnd

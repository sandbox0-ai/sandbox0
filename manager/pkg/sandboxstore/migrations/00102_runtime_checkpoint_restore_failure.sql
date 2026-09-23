-- +goose Up
-- Independent restores share migration's physical failure protocol, retaining
-- their immutable checkpoint while permanently consuming the attempted generation.
CREATE INDEX idx_lifecycle_failed_manual_resume_projection
    ON manager.sandbox_lifecycle_txns (sandbox_id,to_generation,epoch)
    WHERE kind='resume' AND source='manual' AND phase='aborted';

-- +goose StatementBegin
CREATE FUNCTION manager.guard_runtime_checkpoint_restore_failure() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE l manager.sandbox_lifecycle_txns%ROWTYPE;
BEGIN
    IF TG_OP='INSERT' THEN
        SELECT * INTO l FROM manager.sandbox_lifecycle_txns WHERE txn_id=NEW.operation_id;
        IF COALESCE(NULLIF((NEW.authority->'assignment'->>'from_generation')::bigint,0),
            CASE WHEN NEW.authority->'assignment'->>'kind'='resume'
                THEN (NEW.authority->'assignment'->'capture'->>'runtime_generation')::bigint ELSE 0 END)
            IS DISTINCT FROM l.from_generation THEN
            RAISE EXCEPTION 'Memory restore must bind its admitted predecessor' USING ERRCODE='23514';
        END IF;
        RETURN NEW;
    END IF;
    IF NEW.evidence ? 'failure' THEN
        SELECT * INTO l FROM manager.sandbox_lifecycle_txns WHERE txn_id=NEW.operation_id;
        IF NOT NEW.evidence ? 'restore' OR NEW.evidence ?| ARRAY['adoption','adopted']
            OR NEW.evidence->'failure'->'restore' IS DISTINCT FROM NEW.evidence->'restore'
            OR l.phase NOT IN ('preparing','aborted') THEN
            RAISE EXCEPTION 'Memory restore failure changed execution custody' USING ERRCODE='23514';
        END IF;
        IF NOT OLD.evidence ? 'failure' AND NOT EXISTS (
            SELECT 1 FROM manager.runtime_slots t WHERE t.slot_id=NEW.evidence->'image'->'target'->>'slot_id'
                AND t.claim_operation_id=NEW.operation_id AND t.sandbox_id=l.sandbox_id
                AND t.state IN ('quiescing','orphaned') AND t.carrier_retired
                AND t.writer_grant_id=NEW.evidence->'restore'->'stage'->'identity'->>'writer_grant_id'
                AND t.resource_lease_id=NEW.evidence->'image'->'resources'->>'lease_id'
        ) THEN
            RAISE EXCEPTION 'Memory restore failure requires the fenced exact destination' USING ERRCODE='23514';
        END IF;
    END IF;
    IF OLD.evidence ? 'failure' AND
        (NEW.evidence-ARRAY['failure_stopped','failure_cleanup','failure_cleaned','failure_finalized','failure_gc','failure_gc_ack'])
        IS DISTINCT FROM (OLD.evidence-ARRAY['failure_stopped','failure_cleanup','failure_cleaned','failure_finalized','failure_gc','failure_gc_ack']) THEN
        RAISE EXCEPTION 'Failed memory restore cannot gain execution or adoption authority' USING ERRCODE='23514';
    END IF;
    IF NEW.evidence ? 'failure_stopped' AND NOT NEW.evidence ? 'failure'
        OR NEW.evidence ? 'failure_cleanup' AND NOT NEW.evidence ? 'failure_stopped'
        OR NEW.evidence ? 'failure_cleaned' AND NOT NEW.evidence ? 'failure_cleanup'
        OR NEW.evidence ? 'failure_finalized' AND NOT NEW.evidence ? 'failure_cleaned'
        OR (NEW.evidence ? 'failure_gc') IS DISTINCT FROM (NEW.evidence ? 'failure_gc_ack')
        OR NEW.evidence ? 'failure_gc_ack' AND NOT NEW.evidence ? 'failure_finalized' THEN
        RAISE EXCEPTION 'Memory restore failure lacks prerequisite physical evidence' USING ERRCODE='23514';
    END IF;
    RETURN NEW;
END;
$$;
-- +goose StatementEnd
CREATE TRIGGER runtime_checkpoint_restore_failure_guard BEFORE INSERT OR UPDATE ON manager.sandbox_runtime_checkpoint_restores
    FOR EACH ROW EXECUTE FUNCTION manager.guard_runtime_checkpoint_restore_failure();

-- +goose StatementBegin
CREATE OR REPLACE FUNCTION manager.guard_runtime_checkpoint_restore_completion() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE r manager.sandbox_runtime_checkpoint_restores%ROWTYPE;
BEGIN
    SELECT * INTO r FROM manager.sandbox_runtime_checkpoint_restores WHERE operation_id=NEW.txn_id;
    IF NOT FOUND OR NEW.phase=OLD.phase THEN RETURN NEW; END IF;
    IF NEW.phase='aborted' AND r.evidence ? 'restore' AND NOT r.evidence ? 'failure_gc_ack' THEN
        RAISE EXCEPTION 'Authorized memory restore requires physical failure resolution' USING ERRCODE='23514';
    END IF;
    IF NEW.phase='committed' AND (NOT r.evidence ? 'handed_over' OR NOT EXISTS (
        SELECT 1 FROM manager.runtime_slots slot JOIN manager.sandboxes owner ON owner.sandbox_id=slot.sandbox_id
        WHERE slot.slot_id=r.evidence->'image'->'target'->>'slot_id' AND slot.state='active'
            AND slot.claim_operation_id=NEW.txn_id AND slot.sandbox_id=NEW.sandbox_id
            AND slot.procd_instance_id=r.evidence->'handed_over'->>'instance_id'
            AND owner.runtime_id=slot.allocation_id AND owner.runtime_namespace=slot.allocation_namespace
            AND owner.runtime_generation=NEW.to_generation AND owner.desired_state='active'
    )) THEN
        RAISE EXCEPTION 'Memory resume completion requires the exact restored command-ready runtime' USING ERRCODE='23514';
    END IF;
    RETURN NEW;
END;
$$;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE OR REPLACE FUNCTION manager.guard_runtime_checkpoint_ref() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP='DELETE' THEN
        IF NOT EXISTS (SELECT 1 FROM manager.sandboxes s WHERE s.sandbox_id=OLD.sandbox_id
            AND (s.desired_state='deleted' OR
                (s.desired_state='active' AND s.runtime_id<>'' AND s.runtime_generation>OLD.runtime_generation
                    AND NOT EXISTS (SELECT 1 FROM manager.sandbox_lifecycle_txns l WHERE l.sandbox_id=s.sandbox_id
                        AND l.phase IN ('preparing','barriered','publishing','committing'))))) THEN
            RAISE EXCEPTION 'Checkpoint ownership requires completed restore or owner deletion' USING ERRCODE='23514';
        END IF;
        RETURN OLD;
    END IF;
    IF TG_OP='UPDATE' AND ROW(NEW.sandbox_id,NEW.checkpoint_id,NEW.generation_id,NEW.created_at)
        IS NOT DISTINCT FROM ROW(OLD.sandbox_id,OLD.checkpoint_id,OLD.generation_id,OLD.created_at)
        AND NEW.runtime_generation=OLD.runtime_generation+1 AND EXISTS (
            SELECT 1 FROM manager.sandbox_runtime_checkpoint_restores r
            JOIN manager.sandbox_lifecycle_txns l ON l.txn_id=r.operation_id
            JOIN manager.sandboxes s ON s.sandbox_id=l.sandbox_id
            JOIN manager.runtime_slots t ON t.slot_id=r.evidence->'image'->'target'->>'slot_id'
            JOIN manager.runtime_resource_leases lease ON lease.lease_id=t.resource_lease_id
            WHERE r.checkpoint_id=NEW.checkpoint_id AND l.sandbox_id=NEW.sandbox_id
                AND l.phase='preparing' AND l.from_generation=OLD.runtime_generation AND l.to_generation=NEW.runtime_generation
                AND l.expected_generation_id=NEW.generation_id AND r.evidence ? 'failure_gc_ack'
                AND s.runtime_generation=NEW.runtime_generation AND s.runtime_id='' AND s.runtime_namespace=''
                AND s.desired_state IN ('paused','terminating') AND s.lifecycle_epoch=l.epoch
                AND t.state='terminal' AND t.terminal_reason='migration_failed' AND lease.lease_state='released'
        ) THEN RETURN NEW; END IF;
    IF TG_OP='UPDATE' THEN
        RAISE EXCEPTION 'Checkpoint ownership is immutable' USING ERRCODE='23514';
    END IF;
    IF EXISTS (
        SELECT 1 FROM manager.sandbox_runtime_checkpoints c
        JOIN manager.sandbox_lifecycle_txns l ON l.txn_id=c.operation_id
        JOIN manager.sandboxes s ON s.sandbox_id=l.sandbox_id
        WHERE c.operation_id=NEW.checkpoint_id AND l.sandbox_id=NEW.sandbox_id
            AND l.phase='publishing' AND c.evidence ? 'publication'
            AND l.prepared_generation_id=NEW.generation_id
            AND c.evidence->'publication'->'capture'->'rootfs'->'generation'->>'generation_id'=NEW.generation_id
            AND s.desired_state='active' AND s.runtime_generation=l.from_generation
            AND NEW.runtime_generation=s.runtime_generation AND s.lifecycle_epoch=l.epoch
    ) THEN RETURN NEW; END IF;
    IF EXISTS (
        SELECT 1 FROM manager.sandbox_runtime_checkpoint_forks fork
        JOIN manager.sandbox_lifecycle_txns l ON l.txn_id=fork.operation_id
        JOIN manager.sandbox_runtime_checkpoint_refs parent ON parent.sandbox_id=l.sandbox_id
        WHERE fork.target_sandbox_id=NEW.sandbox_id AND fork.checkpoint_id=NEW.checkpoint_id
            AND fork.generation_id=NEW.generation_id AND NEW.runtime_generation=0
            AND l.phase='publishing' AND l.kind='fork' AND l.target_sandbox_id=NEW.sandbox_id
            AND parent.checkpoint_id=NEW.checkpoint_id AND parent.generation_id=NEW.generation_id
            AND parent.runtime_generation=l.from_generation
    ) THEN RETURN NEW; END IF;
    RAISE EXCEPTION 'Checkpoint reference requires exact capture or fork custody' USING ERRCODE='23514';
END;
$$;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE FUNCTION manager.check_runtime_checkpoint_restore_failed_terminal() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE op TEXT;
BEGIN
    IF TG_TABLE_NAME='sandbox_runtime_checkpoint_restores' THEN op:=NEW.operation_id;
    ELSIF TG_TABLE_NAME='sandbox_lifecycle_txns' THEN op:=NEW.txn_id;
    ELSE SELECT operation_id INTO op FROM manager.sandbox_runtime_checkpoint_restores
        WHERE evidence->'image'->'target'->>'slot_id'=NEW.slot_id;
    END IF;
    IF EXISTS (SELECT 1 FROM manager.sandbox_runtime_checkpoint_restores r
        JOIN manager.sandbox_lifecycle_txns l ON l.txn_id=r.operation_id
        JOIN manager.runtime_slots t ON t.slot_id=r.evidence->'image'->'target'->>'slot_id'
        WHERE r.operation_id=op AND r.evidence ? 'restore' AND l.phase<>'committed'
            AND (r.evidence ? 'failure_gc_ack' OR l.phase='aborted' OR t.state='terminal')) AND NOT EXISTS (
        SELECT 1 FROM manager.sandbox_runtime_checkpoint_restores r
        JOIN manager.sandbox_lifecycle_txns l ON l.txn_id=r.operation_id
        JOIN manager.runtime_slots t ON t.slot_id=r.evidence->'image'->'target'->>'slot_id'
        JOIN manager.runtime_resource_leases lease ON lease.lease_id=t.resource_lease_id
        JOIN manager.rootfs_writer_grants w ON w.grant_id=t.writer_grant_id
        JOIN manager.rootfs_filesystems fs ON fs.filesystem_id=w.filesystem_id
        JOIN manager.sandboxes s ON s.sandbox_id=l.sandbox_id
        JOIN manager.sandbox_runtime_checkpoint_refs ref ON ref.sandbox_id=s.sandbox_id
        WHERE r.operation_id=op AND r.evidence ?& ARRAY['failure_finalized','failure_gc','failure_gc_ack']
            AND l.phase='aborted' AND l.aborted_at IS NOT NULL
            AND t.state='terminal' AND t.terminal_reason='migration_failed'
            AND octet_length(t.orphan_observation_digest)=32 AND octet_length(t.terminal_proof_digest)=32
            AND encode(t.orphan_observation_digest,'hex')=r.evidence->'failure_gc'->>'allocation_absence_digest'
            AND lease.lease_state='released' AND lease.released_at IS NOT NULL
            AND (w.state='canceled' AND r.evidence->'failure_cleanup'->'cleanup'->>'writer_retire_kind'='canceled'
                OR w.state='retired' AND w.retire_kind='crash_abandon'
                    AND encode(w.retire_proof_digest,'hex')=r.evidence->'failure_cleaned'->'cleanup'->>'proof_digest')
            AND fs.head_generation_id=l.expected_generation_id
            AND s.runtime_id='' AND s.runtime_namespace='' AND s.runtime_generation=l.to_generation
            AND s.desired_state IN ('paused','terminating','deleted')
            AND ref.checkpoint_id=r.checkpoint_id AND ref.generation_id=l.expected_generation_id AND ref.runtime_generation=l.to_generation
    ) THEN
        RAISE EXCEPTION 'Memory restore failure requires physical cleanup, consumed generation and retained checkpoint' USING ERRCODE='23514';
    END IF;
    RETURN NULL;
END;
$$;
-- +goose StatementEnd
CREATE CONSTRAINT TRIGGER runtime_checkpoint_restore_failed_evidence_check AFTER UPDATE ON manager.sandbox_runtime_checkpoint_restores
    DEFERRABLE INITIALLY DEFERRED FOR EACH ROW WHEN (NEW.evidence ? 'failure') EXECUTE FUNCTION manager.check_runtime_checkpoint_restore_failed_terminal();
CREATE CONSTRAINT TRIGGER runtime_checkpoint_restore_failed_lifecycle_check AFTER UPDATE OF phase ON manager.sandbox_lifecycle_txns
    DEFERRABLE INITIALLY DEFERRED FOR EACH ROW WHEN (NEW.kind='resume' AND NEW.phase='aborted') EXECUTE FUNCTION manager.check_runtime_checkpoint_restore_failed_terminal();
CREATE CONSTRAINT TRIGGER runtime_checkpoint_restore_failed_slot_check AFTER UPDATE OF state ON manager.runtime_slots
    DEFERRABLE INITIALLY DEFERRED FOR EACH ROW WHEN (NEW.state='terminal') EXECUTE FUNCTION manager.check_runtime_checkpoint_restore_failed_terminal();

-- +goose Down
-- +goose StatementBegin
DO $$ BEGIN RAISE EXCEPTION 'Memory restore failure custody must be retained' USING ERRCODE='55000'; END $$;
-- +goose StatementEnd

-- +goose Up
-- Image authorization is durable before delivery. A cancellation therefore
-- needs a node tombstone and absence proof before generic carrier cleanup.
-- +goose StatementBegin
CREATE FUNCTION manager.guard_runtime_checkpoint_restore_cancel_evidence() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.evidence ? 'cancel_request' THEN
        IF NOT NEW.evidence ? 'image' OR NEW.evidence ? 'restore'
            OR NEW.evidence->'cancel_request'->'image' IS DISTINCT FROM NEW.evidence->'image'
            OR (OLD.evidence ? 'cancel_request' AND
                (NEW.evidence-ARRAY['cancel_proof']) IS DISTINCT FROM (OLD.evidence-ARRAY['cancel_proof']))
            OR (NOT OLD.evidence ? 'cancel_request' AND
                (NEW.evidence-ARRAY['cancel_request']) IS DISTINCT FROM OLD.evidence) THEN
            RAISE EXCEPTION 'Checkpoint image cancellation changed restore authority' USING ERRCODE='23514';
        END IF;
    END IF;
    IF NEW.evidence ? 'cancel_proof' AND (NOT NEW.evidence ? 'cancel_request'
        OR NEW.evidence->'cancel_proof'->>'image_absent' IS DISTINCT FROM 'true'
        OR NEW.evidence->'cancel_proof'->>'request_digest' !~ '^[0-9a-f]{64}$') THEN
        RAISE EXCEPTION 'Checkpoint image cancellation lacks node absence proof' USING ERRCODE='23514';
    END IF;
    RETURN NEW;
END;
$$;
-- +goose StatementEnd
CREATE TRIGGER runtime_checkpoint_restore_cancel_evidence_guard BEFORE UPDATE ON manager.sandbox_runtime_checkpoint_restores
    FOR EACH ROW EXECUTE FUNCTION manager.guard_runtime_checkpoint_restore_cancel_evidence();

-- +goose StatementBegin
CREATE FUNCTION manager.guard_runtime_checkpoint_restore_cancel_slot() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE r manager.sandbox_runtime_checkpoint_restores%ROWTYPE;
BEGIN
    SELECT * INTO r FROM manager.sandbox_runtime_checkpoint_restores
        WHERE operation_id=NEW.claim_operation_id AND evidence->'image'->'target'->>'slot_id'=NEW.slot_id;
    IF NOT FOUND OR r.evidence ? 'restore' THEN RETURN NEW; END IF;
    IF r.evidence ? 'cancel_request' AND
        (NEW.writer_grant_id IS DISTINCT FROM OLD.writer_grant_id OR
            NEW.state IN ('starting','active') AND NEW.state IS DISTINCT FROM OLD.state) THEN
        RAISE EXCEPTION 'Canceled memory restore cannot gain a writer or launch' USING ERRCODE='23514';
    END IF;
    IF NEW.state='terminal' AND OLD.state<>'terminal' AND NOT r.evidence ? 'cancel_proof' THEN
        RAISE EXCEPTION 'Memory restore target requires node image absence before terminal cleanup' USING ERRCODE='23514';
    END IF;
    RETURN NEW;
END;
$$;
-- +goose StatementEnd
CREATE TRIGGER runtime_checkpoint_restore_cancel_slot_guard BEFORE UPDATE ON manager.runtime_slots
    FOR EACH ROW EXECUTE FUNCTION manager.guard_runtime_checkpoint_restore_cancel_slot();

-- +goose StatementBegin
CREATE FUNCTION manager.guard_runtime_checkpoint_restore_cancel_lifecycle() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE r manager.sandbox_runtime_checkpoint_restores%ROWTYPE;
BEGIN
    IF NEW.phase<>'aborted' OR OLD.phase='aborted' THEN RETURN NEW; END IF;
    SELECT * INTO r FROM manager.sandbox_runtime_checkpoint_restores WHERE operation_id=NEW.txn_id;
    IF FOUND AND r.evidence ? 'image' AND NOT r.evidence ? 'restore' AND
        (NOT r.evidence ? 'cancel_proof' OR NOT EXISTS (
            SELECT 1 FROM manager.runtime_slots t
            JOIN manager.runtime_resource_leases lease ON lease.lease_id=t.resource_lease_id
            WHERE t.slot_id=r.evidence->'image'->'target'->>'slot_id'
                AND t.claim_operation_id=NEW.txn_id AND t.state='terminal' AND lease.lease_state='released')) THEN
        RAISE EXCEPTION 'Memory resume abort requires canceled image and physical target cleanup' USING ERRCODE='23514';
    END IF;
    RETURN NEW;
END;
$$;
-- +goose StatementEnd
CREATE TRIGGER runtime_checkpoint_restore_cancel_lifecycle_guard BEFORE UPDATE ON manager.sandbox_lifecycle_txns
    FOR EACH ROW EXECUTE FUNCTION manager.guard_runtime_checkpoint_restore_cancel_lifecycle();

-- +goose Down
-- +goose StatementBegin
DO $$ BEGIN RAISE EXCEPTION 'Checkpoint image cancellation custody must be retained' USING ERRCODE='55000'; END $$;
-- +goose StatementEnd

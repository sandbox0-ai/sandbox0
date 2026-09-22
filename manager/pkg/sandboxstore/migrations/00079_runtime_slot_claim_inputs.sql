-- +goose Up
-- Keep the actual immutable launch input. A later template/config read cannot
-- reconstruct the environment, webhook or tmpfs layout of a running process.
ALTER TABLE manager.runtime_slots
    ADD COLUMN claim_runtime_assignment TEXT,
    ADD COLUMN claim_network_policy TEXT;

-- +goose StatementBegin
CREATE FUNCTION manager.guard_runtime_slot_claim_inputs() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP='UPDATE' AND NEW.claim_runtime_assignment IS DISTINCT FROM OLD.claim_runtime_assignment
        AND (OLD.claim_runtime_assignment IS NOT NULL OR OLD.claim_operation_id <> '') THEN
        RAISE EXCEPTION 'Runtime launch inputs cannot be replaced or backfilled after claim' USING ERRCODE='23514';
    END IF;
    IF NEW.claim_runtime_assignment IS NOT NULL AND (
        octet_length(NEW.claim_runtime_assignment) NOT BETWEEN 1 AND 65536
        OR jsonb_typeof(NEW.claim_runtime_assignment::jsonb)<>'object'
        OR encode(sha256(convert_to(NEW.claim_runtime_assignment,'UTF8')),'hex')<>NEW.claim_runtime_assignment_revision
    ) THEN
        RAISE EXCEPTION 'Invalid exact runtime launch payload' USING ERRCODE='23514';
    END IF;
    IF NEW.claim_network_policy IS NOT NULL AND (
        octet_length(NEW.claim_network_policy) NOT BETWEEN 1 AND 65536
        OR jsonb_typeof(NEW.claim_network_policy::jsonb)<>'object'
        OR 'sha256:' || encode(sha256(convert_to(NEW.claim_network_policy,'UTF8')),'hex')<>NEW.claim_network_policy_digest
    ) THEN
        RAISE EXCEPTION 'Invalid exact runtime policy payload' USING ERRCODE='23514';
    END IF;
    IF NEW.claim_runtime_assignment IS NOT NULL AND NOT EXISTS (
        SELECT 1 FROM manager.sandboxes s WHERE s.sandbox_id=NEW.sandbox_id
            AND NEW.claim_runtime_assignment::jsonb->>'sandbox_id'=s.sandbox_id
            AND NEW.claim_runtime_assignment::jsonb->>'team_id'=s.team_id
    ) THEN
        RAISE EXCEPTION 'Runtime launch inputs changed sandbox identity' USING ERRCODE='23514';
    END IF;
    IF TG_OP='UPDATE' AND OLD.claim_network_policy IS NOT NULL AND NEW.claim_network_policy IS NULL THEN
        RAISE EXCEPTION 'Runtime applied policy custody cannot be erased' USING ERRCODE='23514';
    END IF;
    RETURN NEW;
END;
$$;
-- +goose StatementEnd
-- Do not parse/hash launch payloads during ordinary heartbeats or inventory.
CREATE TRIGGER runtime_slot_claim_inputs_guard BEFORE UPDATE OF claim_runtime_assignment,
    claim_runtime_assignment_revision,claim_network_policy,claim_network_policy_digest,sandbox_id ON manager.runtime_slots
    FOR EACH ROW EXECUTE FUNCTION manager.guard_runtime_slot_claim_inputs();

CREATE TRIGGER runtime_slot_claim_inputs_insert_guard BEFORE INSERT ON manager.runtime_slots
    FOR EACH ROW EXECUTE FUNCTION manager.guard_runtime_slot_claim_inputs();

-- The acknowledgement updates the slot before updating the mutation row. Check
-- at transaction commit so policy bytes can never publish without the same ack.
-- +goose StatementBegin
CREATE FUNCTION manager.guard_runtime_slot_applied_policy() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.claim_network_policy IS DISTINCT FROM OLD.claim_network_policy AND OLD.claim_operation_id <> ''
        AND NOT EXISTS (
            SELECT 1 FROM manager.sandbox_network_mutations m
            WHERE m.slot_id=NEW.slot_id AND m.sandbox_id=NEW.sandbox_id
                AND m.allocation_id=NEW.allocation_id AND m.node_uid=NEW.node_uid AND m.node_boot_id=NEW.node_boot_id
                AND m.phase='applied' AND m.applied_policy_token IS NOT NULL
                AND m.desired_policy=NEW.claim_network_policy AND m.desired_policy_digest=NEW.claim_network_policy_digest
        ) THEN
        RAISE EXCEPTION 'Runtime policy bytes require a committed exact node acknowledgement' USING ERRCODE='23514';
    END IF;
    RETURN NEW;
END;
$$;
-- +goose StatementEnd
CREATE CONSTRAINT TRIGGER runtime_slot_applied_policy_guard AFTER UPDATE ON manager.runtime_slots
    DEFERRABLE INITIALLY DEFERRED FOR EACH ROW
    WHEN (OLD.claim_operation_id <> '' AND NEW.claim_network_policy IS DISTINCT FROM OLD.claim_network_policy)
    EXECUTE FUNCTION manager.guard_runtime_slot_applied_policy();

-- +goose Down
-- +goose StatementBegin
DO $$ BEGIN
    RAISE EXCEPTION 'Runtime launch inputs retain migration recovery evidence' USING ERRCODE='55000';
END $$;
-- +goose StatementEnd

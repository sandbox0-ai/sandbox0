-- +goose Up
-- Restoring a retained image is an ordinary resume lifecycle with immutable
-- image authority. It does not reserve or revive the former carrier.
CREATE TABLE manager.sandbox_runtime_checkpoint_restores (
    operation_id TEXT PRIMARY KEY REFERENCES manager.sandbox_lifecycle_txns(txn_id) ON DELETE RESTRICT,
    checkpoint_id TEXT NOT NULL REFERENCES manager.sandbox_runtime_checkpoints(operation_id) ON DELETE RESTRICT,
    compatibility_digest TEXT NOT NULL CHECK (compatibility_digest ~ '^sha256:[0-9a-f]{64}$'),
    authority JSONB NOT NULL CHECK (jsonb_typeof(authority)='object' AND octet_length(authority::text)<=1048576),
    evidence JSONB NOT NULL CHECK (jsonb_typeof(evidence)='object' AND octet_length(evidence::text)<=2097152),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX idx_runtime_checkpoint_restore_image ON manager.sandbox_runtime_checkpoint_restores(checkpoint_id);
CREATE UNIQUE INDEX idx_runtime_checkpoint_restore_target ON manager.sandbox_runtime_checkpoint_restores
    ((evidence->'image'->'target'->>'slot_id')) WHERE evidence ? 'image';

-- +goose StatementBegin
CREATE FUNCTION manager.guard_runtime_checkpoint_restore() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE l manager.sandbox_lifecycle_txns%ROWTYPE; c manager.sandbox_runtime_checkpoints%ROWTYPE;
    source_phase TEXT; field TEXT; assignment JSONB;
BEGIN
    IF TG_OP='DELETE' THEN
        RAISE EXCEPTION 'Checkpoint restore history requires explicit custody retirement' USING ERRCODE='23514';
    END IF;
    SELECT * INTO l FROM manager.sandbox_lifecycle_txns WHERE txn_id=NEW.operation_id;
    SELECT * INTO c FROM manager.sandbox_runtime_checkpoints WHERE operation_id=NEW.checkpoint_id;
    SELECT phase INTO source_phase FROM manager.sandbox_lifecycle_txns WHERE txn_id=NEW.checkpoint_id;
    assignment := NEW.authority->'assignment';
    IF l.kind IS DISTINCT FROM 'resume' OR l.source IS DISTINCT FROM 'manual' OR l.cancelable
        OR NEW.operation_id=NEW.checkpoint_id OR source_phase IS DISTINCT FROM 'committed'
        OR NOT c.evidence ? 'finalized' OR NEW.compatibility_digest IS DISTINCT FROM c.compatibility_digest
        OR NEW.authority->'retained' IS DISTINCT FROM c.evidence->'retained'
        OR NEW.authority->>'lifecycle_epoch' IS DISTINCT FROM l.epoch::text
        OR assignment->>'operation_id' IS DISTINCT FROM NEW.operation_id
        OR assignment->'target'->>'sandbox_id' IS DISTINCT FROM l.sandbox_id
        OR assignment->'target'->>'runtime_generation' IS DISTINCT FROM l.to_generation::text
        OR assignment->'target'->>'team_id' IS DISTINCT FROM c.evidence->'assignment'->>'team_id'
        OR assignment->'capture'->>'operation_id' IS DISTINCT FROM NEW.checkpoint_id
        OR assignment->'capture'->>'sandbox_id' IS DISTINCT FROM c.evidence->'assignment'->>'sandbox_id'
        OR assignment->'capture'->>'team_id' IS DISTINCT FROM c.evidence->'assignment'->>'team_id'
        OR assignment->'capture'->>'runtime_generation' IS DISTINCT FROM c.evidence->'assignment'->>'runtime_generation'
        OR assignment->'capture'->>'revision' IS DISTINCT FROM c.evidence->'preflight'->'source'->>'assignment_revision' THEN
        RAISE EXCEPTION 'Checkpoint restore changed lifecycle or retained image authority' USING ERRCODE='23514';
    END IF;
    IF TG_OP='INSERT' THEN
        IF l.phase<>'preparing' OR NEW.evidence<>'{}'::jsonb OR NOT EXISTS (
            SELECT 1 FROM manager.sandbox_runtime_checkpoint_refs r
            JOIN manager.sandboxes s ON s.sandbox_id=r.sandbox_id
            WHERE r.sandbox_id=l.sandbox_id AND r.checkpoint_id=NEW.checkpoint_id
                AND r.generation_id=l.expected_generation_id AND r.runtime_generation=l.from_generation
                AND s.team_id=assignment->'target'->>'team_id' AND s.desired_state='paused'
                AND s.runtime_id='' AND s.runtime_namespace='' AND s.runtime_generation=l.from_generation
                AND s.lifecycle_epoch=l.epoch AND (s.hard_expires_at IS NULL OR s.hard_expires_at>clock_timestamp())
        ) THEN
            RAISE EXCEPTION 'Memory resume requires an exact live paused owner reference' USING ERRCODE='23514';
        END IF;
    ELSE
        IF ROW(NEW.operation_id,NEW.checkpoint_id,NEW.compatibility_digest,NEW.authority,NEW.created_at)
            IS DISTINCT FROM ROW(OLD.operation_id,OLD.checkpoint_id,OLD.compatibility_digest,OLD.authority,OLD.created_at) THEN
            RAISE EXCEPTION 'Checkpoint restore authority is immutable' USING ERRCODE='23514';
        END IF;
        FOR field IN SELECT jsonb_object_keys(OLD.evidence) LOOP
            IF NEW.evidence->field IS DISTINCT FROM OLD.evidence->field THEN
                RAISE EXCEPTION 'Checkpoint restore evidence is immutable: %', field USING ERRCODE='23514';
            END IF;
        END LOOP;
        IF NEW.evidence IS DISTINCT FROM OLD.evidence AND l.phase NOT IN ('preparing','barriered','publishing','committing')
            AND NOT (l.phase='committed' AND (NEW.evidence-ARRAY['adoption','adopted'])=(OLD.evidence-ARRAY['adoption','adopted'])) THEN
            RAISE EXCEPTION 'Checkpoint restore evidence requires an active lifecycle' USING ERRCODE='23514';
        END IF;
    END IF;
    IF NEW.evidence ? 'cpu_request' AND NOT NEW.evidence ? 'cpu_requested_at'
        OR NEW.evidence ? 'cpu_requested_at' AND NOT NEW.evidence ? 'cpu_request'
        OR NEW.evidence ? 'cpu' AND NOT NEW.evidence ? 'cpu_request'
        OR NEW.evidence ? 'image' AND NOT NEW.evidence ? 'cpu'
        OR NEW.evidence ? 'prepared' AND NOT NEW.evidence ? 'image'
        OR NEW.evidence ? 'restore' AND NOT NEW.evidence ? 'prepared'
        OR NEW.evidence ? 'restored' AND NOT NEW.evidence ? 'restore'
        OR NEW.evidence ? 'handover' AND NOT NEW.evidence ? 'restored'
        OR NEW.evidence ? 'handed_over' AND NOT NEW.evidence ? 'handover'
        OR NEW.evidence ? 'adoption' AND NOT NEW.evidence ? 'handed_over'
        OR NEW.evidence ? 'adopted' AND NOT NEW.evidence ? 'adoption' THEN
        RAISE EXCEPTION 'Checkpoint restore evidence is missing its prerequisite' USING ERRCODE='23514';
    END IF;
    IF NEW.evidence ? 'adoption' AND NOT OLD.evidence ? 'adoption' AND (l.phase<>'committed' OR NOT EXISTS (
        SELECT 1 FROM manager.runtime_slots slot WHERE slot.slot_id=NEW.evidence->'adoption'->'target'->>'slot_id'
            AND slot.state='active' AND slot.claim_operation_id=NEW.operation_id AND slot.sandbox_id=l.sandbox_id
            AND slot.allocation_id=l.to_runtime_id AND slot.allocation_namespace=l.to_runtime_namespace
            AND slot.procd_instance_id=NEW.evidence->'adoption'->>'procd_instance_id'
            AND encode(slot.command_ready_digest,'hex')=NEW.evidence->'adoption'->>'command_ready_digest'
            AND NEW.evidence->'adoption'->>'runtime_generation'=l.to_generation::text
            AND NEW.evidence->'adoption'->>'restore_digest'=NEW.evidence->'restored'->>'request_digest'
    )) THEN
        RAISE EXCEPTION 'Checkpoint adoption requires committed restored routing' USING ERRCODE='23514';
    END IF;
    IF NEW.evidence ? 'restore' AND (
        NEW.evidence->'restore'->'image' IS DISTINCT FROM NEW.evidence->'image'
        OR NEW.evidence->'restore'->'prepared' IS DISTINCT FROM NEW.evidence->'prepared'
        OR NEW.evidence->'restore'->'fence' IS DISTINCT FROM c.evidence->'source_fence'
        OR NEW.evidence->'restore'->'proof' IS DISTINCT FROM c.evidence->'fenced'
        OR COALESCE(NEW.evidence->'restore'->'stage'->'identity'->>'writer_grant_token','')<>'') THEN
        RAISE EXCEPTION 'Checkpoint restore requires exact image and source fencing' USING ERRCODE='23514';
    END IF;
    IF NEW.evidence ? 'restored' AND (
        NEW.evidence->'restored'->'request' IS DISTINCT FROM NEW.evidence->'restore'
        OR NEW.evidence->'restored'->>'state' IS DISTINCT FROM 'restored') THEN
        RAISE EXCEPTION 'Checkpoint handover requires completed exact restore' USING ERRCODE='23514';
    END IF;
    IF NEW.evidence ? 'handover' AND (
        NEW.evidence->'handover'->'restore' IS DISTINCT FROM assignment
        OR NEW.evidence->'handover'->>'instance_id' IS DISTINCT FROM c.evidence->'preparation'->>'instance_id'
        OR NEW.evidence->'handover'->>'capture_epoch' IS DISTINCT FROM c.evidence->'preparation'->>'capture_epoch'
        OR NEW.evidence->'handover'->>'lifecycle_epoch' IS DISTINCT FROM l.epoch::text) THEN
        RAISE EXCEPTION 'Checkpoint handover changed captured process or target authority' USING ERRCODE='23514';
    END IF;
    IF NEW.evidence ? 'cpu_request' AND (
        NEW.evidence->'cpu_request'->'checkpoint' IS DISTINCT FROM assignment
        OR NEW.evidence->'cpu_request'->'source' IS DISTINCT FROM c.evidence->'preflight'->'source'
        OR NEW.evidence->'cpu_request'->'source_resources' IS DISTINCT FROM c.evidence->'preflight'->'source_resources'
        OR NEW.evidence->'cpu_request'->'launch' IS DISTINCT FROM c.evidence->'cpu'->'launch') THEN
        RAISE EXCEPTION 'Checkpoint restore CPU history must match the retained capture' USING ERRCODE='23514';
    END IF;
    IF NEW.evidence ? 'image' AND (
        NEW.evidence->'image'->'checkpoint' IS DISTINCT FROM NEW.authority
        OR NEW.evidence->'image'->'publication' IS DISTINCT FROM c.evidence->'publication'
        OR NEW.evidence->'image'->'receipt' IS DISTINCT FROM c.evidence->'published'
        OR NEW.evidence->'image'->'target' IS DISTINCT FROM NEW.evidence->'cpu_request'->'target'
        OR NEW.evidence->'image'->'resources' IS DISTINCT FROM NEW.evidence->'cpu_request'->'destination_resources') THEN
        RAISE EXCEPTION 'Checkpoint restore image changed capture or target placement' USING ERRCODE='23514';
    END IF;
    RETURN NEW;
END;
$$;
-- +goose StatementEnd
CREATE TRIGGER runtime_checkpoint_restore_guard BEFORE INSERT OR UPDATE OR DELETE ON manager.sandbox_runtime_checkpoint_restores
    FOR EACH ROW EXECUTE FUNCTION manager.guard_runtime_checkpoint_restore();

-- Once memory mode is admitted, ordinary claim code must not issue a writer
-- before verified image preparation. This also fences accidental cold fallback.
-- +goose StatementBegin
CREATE FUNCTION manager.guard_runtime_checkpoint_restore_writer() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE r manager.sandbox_runtime_checkpoint_restores%ROWTYPE;
BEGIN
    IF NEW.writer_grant_id='' OR NEW.writer_grant_id IS NOT DISTINCT FROM OLD.writer_grant_id THEN RETURN NEW; END IF;
    SELECT * INTO r FROM manager.sandbox_runtime_checkpoint_restores WHERE operation_id=NEW.claim_operation_id;
    IF NOT FOUND THEN RETURN NEW; END IF;
    IF NOT r.evidence ? 'prepared'
        OR r.evidence->'image'->'target'->>'slot_id' IS DISTINCT FROM NEW.slot_id
        OR r.evidence->'image'->'target'->>'allocation_id' IS DISTINCT FROM NEW.allocation_id
        OR r.evidence->'image'->'resources'->>'lease_id' IS DISTINCT FROM NEW.resource_lease_id THEN
        RAISE EXCEPTION 'Memory restore writer requires the verified target image' USING ERRCODE='23514';
    END IF;
    RETURN NEW;
END;
$$;
-- +goose StatementEnd
CREATE TRIGGER runtime_checkpoint_restore_writer_guard BEFORE UPDATE ON manager.runtime_slots
    FOR EACH ROW EXECUTE FUNCTION manager.guard_runtime_checkpoint_restore_writer();

-- +goose StatementBegin
CREATE FUNCTION manager.guard_runtime_checkpoint_restore_execution() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE r manager.sandbox_runtime_checkpoint_restores%ROWTYPE;
BEGIN
    IF NEW.state NOT IN ('starting','active') OR NEW.state=OLD.state THEN RETURN NEW; END IF;
    SELECT * INTO r FROM manager.sandbox_runtime_checkpoint_restores WHERE operation_id=NEW.claim_operation_id;
    IF NOT FOUND THEN RETURN NEW; END IF;
    IF NOT r.evidence ? 'restore'
        OR r.evidence->'image'->'target'->>'slot_id' IS DISTINCT FROM NEW.slot_id
        OR r.evidence->'restore'->'stage'->'identity'->>'launch_attempt' IS DISTINCT FROM NEW.launch_attempt
        OR r.evidence->'restore'->'stage'->'identity'->>'writer_grant_id' IS DISTINCT FROM NEW.writer_grant_id THEN
        RAISE EXCEPTION 'Memory launch requires exact durable restore authority' USING ERRCODE='23514';
    END IF;
    IF NEW.state='active' AND (NOT r.evidence ? 'handed_over'
        OR r.evidence->'handed_over'->>'instance_id' IS DISTINCT FROM NEW.procd_instance_id
        OR r.evidence->'handed_over'->>'runtime_generation' IS DISTINCT FROM r.authority->'assignment'->'target'->>'runtime_generation') THEN
        RAISE EXCEPTION 'Memory readiness requires acknowledged procd identity handover' USING ERRCODE='23514';
    END IF;
    RETURN NEW;
END;
$$;
-- +goose StatementEnd
CREATE TRIGGER runtime_checkpoint_restore_execution_guard BEFORE UPDATE ON manager.runtime_slots
    FOR EACH ROW EXECUTE FUNCTION manager.guard_runtime_checkpoint_restore_execution();

-- +goose StatementBegin
CREATE FUNCTION manager.guard_runtime_checkpoint_restore_completion() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE r manager.sandbox_runtime_checkpoint_restores%ROWTYPE;
BEGIN
    SELECT * INTO r FROM manager.sandbox_runtime_checkpoint_restores WHERE operation_id=NEW.txn_id;
    IF NOT FOUND OR NEW.phase=OLD.phase THEN RETURN NEW; END IF;
    IF NEW.phase='aborted' AND r.evidence ? 'restore' THEN
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
CREATE TRIGGER runtime_checkpoint_restore_completion_guard BEFORE UPDATE ON manager.sandbox_lifecycle_txns
    FOR EACH ROW EXECUTE FUNCTION manager.guard_runtime_checkpoint_restore_completion();

-- +goose Down
-- +goose StatementBegin
DO $$ BEGIN
    RAISE EXCEPTION 'Checkpoint restore authority cannot be discarded' USING ERRCODE='55000';
END $$;
-- +goose StatementEnd

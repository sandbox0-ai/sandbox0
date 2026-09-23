-- +goose Up
-- Memory pause extends the existing sandbox lifecycle. It owns no destination
-- slot or resource lease; those are selected by a later resume/fork operation.
CREATE TABLE manager.sandbox_runtime_checkpoints (
    operation_id TEXT PRIMARY KEY REFERENCES manager.sandbox_lifecycle_txns(txn_id) ON DELETE RESTRICT,
    source_slot_id TEXT NOT NULL REFERENCES manager.runtime_slots(slot_id) ON DELETE RESTRICT,
    source_writer_grant_id TEXT NOT NULL REFERENCES manager.rootfs_writer_grants(grant_id) ON DELETE RESTRICT,
    compatibility_digest TEXT NOT NULL CHECK (compatibility_digest ~ '^sha256:[0-9a-f]{64}$'),
    evidence JSONB NOT NULL CHECK (jsonb_typeof(evidence)='object' AND octet_length(evidence::text)<=2097152),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE INDEX idx_runtime_checkpoint_source ON manager.sandbox_runtime_checkpoints(source_slot_id);

-- A reference pins the matching immutable RootFS generation along with the
-- memory image. Forks will add references rather than duplicate image bytes.
CREATE TABLE manager.sandbox_runtime_checkpoint_refs (
    sandbox_id TEXT PRIMARY KEY REFERENCES manager.sandboxes(sandbox_id) ON DELETE RESTRICT,
    checkpoint_id TEXT NOT NULL REFERENCES manager.sandbox_runtime_checkpoints(operation_id) ON DELETE RESTRICT,
    generation_id TEXT NOT NULL REFERENCES manager.rootfs_generations(generation_id) ON DELETE RESTRICT,
    runtime_generation BIGINT NOT NULL CHECK (runtime_generation>=0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX idx_runtime_checkpoint_refs ON manager.sandbox_runtime_checkpoint_refs(checkpoint_id);

-- +goose StatementBegin
CREATE FUNCTION manager.guard_runtime_checkpoint() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE l manager.sandbox_lifecycle_txns%ROWTYPE; field TEXT; source JSONB;
BEGIN
    SELECT * INTO l FROM manager.sandbox_lifecycle_txns WHERE txn_id=NEW.operation_id;
    source := NEW.evidence->'preflight'->'source';
    IF l.kind IS DISTINCT FROM 'pause' OR l.source IS DISTINCT FROM 'manual' OR l.cancelable
        OR l.to_generation<>0 OR l.to_runtime_id<>'' OR l.to_runtime_namespace<>''
        OR NEW.evidence->'preflight'->>'capture_only' IS DISTINCT FROM 'true'
        OR source->>'operation_id' IS DISTINCT FROM NEW.operation_id
        OR source->>'sandbox_id' IS DISTINCT FROM l.sandbox_id
        OR source->>'source_generation' IS DISTINCT FROM l.from_generation::text
        OR source->>'lifecycle_epoch' IS DISTINCT FROM l.epoch::text
        OR source->'target'->>'slot_id' IS DISTINCT FROM NEW.source_slot_id
        OR source->'target'->>'allocation_id' IS DISTINCT FROM l.from_runtime_id
        OR NEW.evidence->'assignment'->>'sandbox_id' IS DISTINCT FROM l.sandbox_id
        OR NEW.evidence->'assignment'->>'runtime_generation' IS DISTINCT FROM l.from_generation::text THEN
        RAISE EXCEPTION 'Checkpoint evidence changed lifecycle or source' USING ERRCODE='23514';
    END IF;
    IF TG_OP='INSERT' THEN
        IF l.phase<>'preparing' OR NEW.evidence ?| ARRAY['cpu','staging','staged','preparation','prepared','capture_authorized','publication','published','retained','source_fence','fenced','finalization','finalized']
            OR NOT EXISTS (
                SELECT 1 FROM manager.runtime_slots s JOIN manager.rootfs_writer_grants w ON w.grant_id=s.writer_grant_id
                WHERE s.slot_id=NEW.source_slot_id AND s.state='active'
                    AND s.sandbox_id=l.sandbox_id AND s.allocation_id=l.from_runtime_id
                    AND s.allocation_namespace=l.from_runtime_namespace
                    AND s.writer_grant_id=NEW.source_writer_grant_id
                    AND s.compatibility_digest=NEW.compatibility_digest
                    AND s.claim_runtime_assignment_revision=source->>'assignment_revision'
                    AND s.procd_instance_id=source->>'procd_instance_id'
                    AND encode(s.rootfs_binding_digest,'hex')=source->>'binding_digest'
                    AND w.state='consumed') THEN
            RAISE EXCEPTION 'Checkpoint reservation requires an exact live source' USING ERRCODE='23514';
        END IF;
    ELSE
        IF ROW(NEW.operation_id,NEW.source_slot_id,NEW.source_writer_grant_id,NEW.compatibility_digest,NEW.created_at)
            IS DISTINCT FROM ROW(OLD.operation_id,OLD.source_slot_id,OLD.source_writer_grant_id,OLD.compatibility_digest,OLD.created_at) THEN
            RAISE EXCEPTION 'Checkpoint source authority is immutable' USING ERRCODE='23514';
        END IF;
        -- Every dispatched command and acknowledged receipt is append-only.
        -- Retrying cannot silently replace an uncertain operation.
        FOR field IN SELECT jsonb_object_keys(OLD.evidence) LOOP
            IF NEW.evidence->field IS DISTINCT FROM OLD.evidence->field THEN
                RAISE EXCEPTION 'Checkpoint evidence is immutable: %', field USING ERRCODE='23514';
            END IF;
        END LOOP;
        IF (NEW.evidence ? 'cpu' AND NOT OLD.evidence ? 'cpu') AND l.phase<>'preparing'
            OR (NEW.evidence ? 'staging' AND NOT OLD.evidence ? 'staging') AND l.phase<>'preparing'
            OR (NEW.evidence ? 'staged' AND NOT OLD.evidence ? 'staged') AND l.phase<>'preparing'
            OR (NEW.evidence ? 'preparation' AND NOT OLD.evidence ? 'preparation') AND l.phase<>'barriered'
            OR (NEW.evidence ? 'capture_authorized' AND NOT OLD.evidence ? 'capture_authorized') AND l.phase<>'publishing'
            OR (NEW.evidence ? 'publication' AND NOT OLD.evidence ? 'publication') AND l.phase<>'publishing'
            OR (NEW.evidence ? 'published' AND NOT OLD.evidence ? 'published') AND l.phase<>'publishing'
            OR (NEW.evidence ? 'retained' AND NOT OLD.evidence ? 'retained') AND l.phase<>'publishing'
            OR (NEW.evidence ? 'source_fence' AND NOT OLD.evidence ? 'source_fence') AND l.phase<>'publishing'
            OR (NEW.evidence ? 'fenced' AND NOT OLD.evidence ? 'fenced') AND l.phase<>'committing'
            OR (NEW.evidence ? 'finalization' AND NOT OLD.evidence ? 'finalization') AND l.phase<>'committing'
            OR (NEW.evidence ? 'finalized' AND NOT OLD.evidence ? 'finalized') AND l.phase<>'committing' THEN
            RAISE EXCEPTION 'Checkpoint command requires its committed lifecycle phase' USING ERRCODE='23514';
        END IF;
    END IF;
    IF NEW.evidence ? 'cpu' AND NOT NEW.evidence ? 'preflight'
        OR NEW.evidence ? 'staging' AND NOT NEW.evidence ? 'cpu'
        OR NEW.evidence ? 'staged' AND NOT NEW.evidence ? 'staging'
        OR NEW.evidence ? 'preparation' AND NOT NEW.evidence ? 'staged'
        OR NEW.evidence ? 'capture_authorized' AND
            (NEW.evidence->>'capture_authorized'<>'true' OR NOT NEW.evidence ? 'prepared' OR NOT NEW.evidence ? 'preparation')
        OR NEW.evidence ? 'publication' AND NOT NEW.evidence ? 'capture_authorized'
        OR NEW.evidence ? 'published' AND NOT NEW.evidence ? 'publication'
        OR NEW.evidence ? 'source_fence' AND NOT NEW.evidence ? 'retained'
        OR NEW.evidence ? 'fenced' AND NOT NEW.evidence ? 'source_fence'
        OR NEW.evidence ? 'finalization' AND NOT NEW.evidence ? 'fenced'
        OR NEW.evidence ? 'finalized' AND NOT NEW.evidence ? 'finalization' THEN
        RAISE EXCEPTION 'Checkpoint evidence is missing its prerequisite' USING ERRCODE='23514';
    END IF;
    IF NEW.evidence ? 'retained' AND NOT OLD.evidence ? 'retained' AND (
        NOT NEW.evidence ? 'published'
        OR NEW.evidence->'retained'->>'checkpoint_id' IS DISTINCT FROM NEW.operation_id
        OR NEW.evidence->'retained'->>'publication_request_digest' IS DISTINCT FROM NEW.evidence->'published'->>'request_digest'
        OR NEW.evidence->'retained'->'reference' IS DISTINCT FROM NEW.evidence->'published'->'reference'
        OR NOT EXISTS (SELECT 1 FROM manager.sandbox_runtime_checkpoint_refs r
            WHERE r.checkpoint_id=NEW.operation_id AND r.sandbox_id=l.sandbox_id AND r.generation_id=l.prepared_generation_id)) THEN
        RAISE EXCEPTION 'Checkpoint retention requires committed memory and RootFS ownership' USING ERRCODE='23514';
    END IF;
    IF TG_OP='UPDATE' AND NEW.evidence ? 'source_fence' AND NOT OLD.evidence ? 'source_fence' AND (
        NEW.evidence->'source_fence'->'publication_request' IS DISTINCT FROM NEW.evidence->'publication'
        OR NEW.evidence->'source_fence'->'publication' IS DISTINCT FROM NEW.evidence->'published'
        OR NOT EXISTS (SELECT 1 FROM manager.rootfs_writer_grants w WHERE w.grant_id=NEW.source_writer_grant_id
            AND w.state='retiring' AND w.retire_kind='migration' AND w.retire_operation_id=NEW.operation_id)) THEN
        RAISE EXCEPTION 'Checkpoint fence requires retained image and revoked source renewal' USING ERRCODE='23514';
    END IF;
    IF TG_OP='UPDATE' AND NEW.evidence ? 'fenced' AND NOT OLD.evidence ? 'fenced' AND NOT EXISTS (
        SELECT 1 FROM manager.rootfs_writer_grants w JOIN manager.rootfs_filesystems f ON f.filesystem_id=w.filesystem_id
        WHERE w.grant_id=NEW.source_writer_grant_id AND w.state='retired' AND w.retire_kind='migration'
            AND w.retire_operation_id=NEW.operation_id AND encode(w.retire_proof_digest,'hex')=NEW.evidence->'fenced'->>'digest'
            AND f.head_generation_id=l.prepared_generation_id AND f.writer_epoch=w.writer_epoch) THEN
        RAISE EXCEPTION 'Checkpoint fence proof requires exact physical retirement and disk head' USING ERRCODE='23514';
    END IF;
    RETURN NEW;
END;
$$;
-- +goose StatementEnd
CREATE TRIGGER runtime_checkpoint_guard BEFORE INSERT OR UPDATE ON manager.sandbox_runtime_checkpoints
    FOR EACH ROW EXECUTE FUNCTION manager.guard_runtime_checkpoint();

-- +goose StatementBegin
CREATE FUNCTION manager.guard_runtime_checkpoint_ref() RETURNS trigger LANGUAGE plpgsql AS $$
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
    IF TG_OP='UPDATE' OR NOT EXISTS (
        SELECT 1 FROM manager.sandbox_runtime_checkpoints c
        JOIN manager.sandbox_lifecycle_txns l ON l.txn_id=c.operation_id
        JOIN manager.sandboxes s ON s.sandbox_id=l.sandbox_id
        WHERE c.operation_id=NEW.checkpoint_id AND l.sandbox_id=NEW.sandbox_id
            AND l.phase='publishing' AND c.evidence ? 'publication'
            AND l.prepared_generation_id=NEW.generation_id
            AND c.evidence->'publication'->'capture'->'rootfs'->'generation'->>'generation_id'=NEW.generation_id
            AND s.desired_state='active' AND s.runtime_generation=l.from_generation
            AND NEW.runtime_generation=s.runtime_generation
            AND s.lifecycle_epoch=l.epoch
    ) THEN
        RAISE EXCEPTION 'Checkpoint reference requires exact lifecycle and prepared RootFS' USING ERRCODE='23514';
    END IF;
    RETURN NEW;
END;
$$;
-- +goose StatementEnd
CREATE TRIGGER runtime_checkpoint_ref_guard BEFORE INSERT OR UPDATE OR DELETE ON manager.sandbox_runtime_checkpoint_refs
    FOR EACH ROW EXECUTE FUNCTION manager.guard_runtime_checkpoint_ref();

-- One carrier can have multiple abandoned preparation attempts but only one
-- finalized source incarnation.
CREATE UNIQUE INDEX idx_runtime_checkpoint_finalized_source ON manager.sandbox_runtime_checkpoints(source_slot_id)
    WHERE evidence ? 'finalized';

-- +goose StatementBegin
CREATE FUNCTION manager.guard_runtime_checkpoint_completion() RETURNS trigger LANGUAGE plpgsql AS $$
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
    IF NEW.phase='aborted' AND OLD.phase<>'aborted' AND c.evidence ? 'capture_authorized' THEN
        RAISE EXCEPTION 'Authorized memory capture requires physical failure resolution' USING ERRCODE='23514';
    END IF;
    RETURN NEW;
END;
$$;
-- +goose StatementEnd
CREATE TRIGGER runtime_checkpoint_completion_guard BEFORE UPDATE ON manager.sandbox_lifecycle_txns
    FOR EACH ROW EXECUTE FUNCTION manager.guard_runtime_checkpoint_completion();

-- +goose Down
-- +goose StatementBegin
DO $$ BEGIN
    RAISE EXCEPTION 'Checkpoint custody and lifecycle evidence must be retained' USING ERRCODE='55000';
END $$;
-- +goose StatementEnd

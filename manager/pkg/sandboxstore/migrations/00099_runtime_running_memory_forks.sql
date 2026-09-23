-- +goose Up
-- Correlate an explicit fork with existing pause/fork/resume transactions.
-- There is no second phase column: those lifecycle transactions remain truth.
CREATE TABLE manager.sandbox_runtime_running_memory_forks (
    operation_id TEXT PRIMARY KEY CHECK (length(operation_id) BETWEEN 1 AND 512),
    source_sandbox_id TEXT NOT NULL REFERENCES manager.sandboxes(sandbox_id) ON DELETE RESTRICT,
    capture_operation_id TEXT NOT NULL UNIQUE REFERENCES manager.sandbox_runtime_checkpoints(operation_id) ON DELETE RESTRICT,
    target_sandbox_id TEXT NOT NULL UNIQUE,
    target_record JSONB NOT NULL CHECK (jsonb_typeof(target_record)='object' AND octet_length(target_record::text)<=2097152),
    target_record_digest BYTEA NOT NULL CHECK (octet_length(target_record_digest)=32),
    source_expires_at TIMESTAMPTZ,
    parent_resume_operation_id TEXT UNIQUE REFERENCES manager.sandbox_lifecycle_txns(txn_id) ON DELETE RESTRICT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CHECK (source_sandbox_id<>target_sandbox_id)
);

-- +goose StatementBegin
CREATE FUNCTION manager.guard_running_memory_fork() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP='DELETE' THEN
        RAISE EXCEPTION 'Running memory fork custody cannot be discarded' USING ERRCODE='23514';
    END IF;
    IF TG_OP='INSERT' THEN
        IF (NEW.target_record->'record'->>'ID') IS DISTINCT FROM NEW.target_sandbox_id
            OR NEW.parent_resume_operation_id IS NOT NULL OR NOT EXISTS (
            SELECT 1 FROM manager.sandbox_runtime_checkpoints c
            JOIN manager.sandbox_lifecycle_txns l ON l.txn_id=c.operation_id
            JOIN manager.sandboxes s ON s.sandbox_id=l.sandbox_id
            WHERE c.operation_id=NEW.capture_operation_id AND l.sandbox_id=NEW.source_sandbox_id
                AND l.kind='pause' AND l.source='manual' AND l.phase='preparing' AND NOT l.cancelable
                AND s.desired_state='active' AND s.runtime_generation=l.from_generation AND s.lifecycle_epoch=l.epoch
                AND s.deleted_at IS NULL AND NOT (c.evidence ? 'preparation')
        ) THEN
            RAISE EXCEPTION 'Running memory fork requires fresh source capture authority' USING ERRCODE='23514';
        END IF;
        RETURN NEW;
    END IF;
    IF ROW(NEW.operation_id,NEW.source_sandbox_id,NEW.capture_operation_id,NEW.target_sandbox_id,NEW.target_record,NEW.target_record_digest,NEW.created_at,NEW.source_expires_at)
        IS DISTINCT FROM ROW(OLD.operation_id,OLD.source_sandbox_id,OLD.capture_operation_id,OLD.target_sandbox_id,OLD.target_record,OLD.target_record_digest,OLD.created_at,OLD.source_expires_at)
        OR (OLD.parent_resume_operation_id IS NOT NULL AND NEW.parent_resume_operation_id IS DISTINCT FROM OLD.parent_resume_operation_id) THEN
        RAISE EXCEPTION 'Running memory fork identity is immutable' USING ERRCODE='23514';
    END IF;
    IF NEW.parent_resume_operation_id IS NOT NULL AND NOT EXISTS (
        SELECT 1 FROM manager.sandbox_lifecycle_txns fork
        JOIN manager.sandbox_runtime_checkpoint_restores r ON r.operation_id=NEW.parent_resume_operation_id
        JOIN manager.sandbox_lifecycle_txns resume ON resume.txn_id=r.operation_id
        WHERE fork.txn_id=NEW.operation_id AND fork.sandbox_id=NEW.source_sandbox_id AND fork.kind='fork'
            AND fork.target_sandbox_id=NEW.target_sandbox_id AND fork.target_record_digest=NEW.target_record_digest
            AND r.checkpoint_id=NEW.capture_operation_id AND resume.sandbox_id=NEW.source_sandbox_id
            AND resume.kind='resume' AND resume.phase IN ('preparing','barriered','publishing','committing','committed')
            AND (
                (fork.phase='committed' AND EXISTS (SELECT 1 FROM manager.sandbox_runtime_checkpoint_forks f
                    WHERE f.operation_id=fork.txn_id AND f.checkpoint_id=NEW.capture_operation_id AND f.target_sandbox_id=NEW.target_sandbox_id))
                OR (fork.phase='aborted' AND fork.error='memory fork child expired before capture handoff'
                    AND (NEW.target_record->'record'->>'HardExpiresAt')::timestamptz>='1970-01-01'::timestamptz
                    AND (NEW.target_record->'record'->>'HardExpiresAt')::timestamptz<=clock_timestamp()
                    AND NOT EXISTS (SELECT 1 FROM manager.sandboxes child WHERE child.sandbox_id=NEW.target_sandbox_id))
            )
    ) THEN
        RAISE EXCEPTION 'Running fork must atomically retain child and admit parent restore' USING ERRCODE='23514';
    END IF;
    RETURN NEW;
END;
$$;
-- +goose StatementEnd
CREATE TRIGGER running_memory_fork_guard BEFORE INSERT OR UPDATE OR DELETE
    ON manager.sandbox_runtime_running_memory_forks FOR EACH ROW EXECUTE FUNCTION manager.guard_running_memory_fork();

-- +goose StatementBegin
CREATE FUNCTION manager.guard_running_memory_fork_handoff() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE f manager.sandbox_runtime_running_memory_forks%ROWTYPE;
BEGIN
    IF NEW.kind<>'pause' OR NEW.phase<>'committed' THEN RETURN NEW; END IF;
    SELECT * INTO f FROM manager.sandbox_runtime_running_memory_forks WHERE capture_operation_id=NEW.txn_id;
    IF NOT FOUND THEN RETURN NEW; END IF;
    IF f.parent_resume_operation_id IS NULL AND EXISTS (
        SELECT 1 FROM manager.sandboxes s WHERE s.sandbox_id=f.source_sandbox_id
            AND s.desired_state NOT IN ('terminating','deleted') AND s.deleted_at IS NULL
            AND (s.hard_expires_at IS NULL OR s.hard_expires_at>clock_timestamp())
    ) THEN
        RAISE EXCEPTION 'Running memory fork cannot expose an unowned paused parent' USING ERRCODE='23514';
    END IF;
    RETURN NEW;
END;
$$;
-- +goose StatementEnd
CREATE CONSTRAINT TRIGGER running_memory_fork_handoff_guard AFTER UPDATE ON manager.sandbox_lifecycle_txns
    DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION manager.guard_running_memory_fork_handoff();

-- +goose Down
DROP TRIGGER running_memory_fork_handoff_guard ON manager.sandbox_lifecycle_txns;
DROP FUNCTION manager.guard_running_memory_fork_handoff();
DROP TABLE manager.sandbox_runtime_running_memory_forks;
DROP FUNCTION manager.guard_running_memory_fork();

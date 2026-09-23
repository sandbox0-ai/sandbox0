-- +goose Up
-- Memory mode belongs to the existing fork lifecycle. This immutable custody
-- link makes retries independent of the parent's later paused/running state.
CREATE TABLE manager.sandbox_runtime_checkpoint_forks (
    operation_id TEXT PRIMARY KEY REFERENCES manager.sandbox_lifecycle_txns(txn_id) ON DELETE RESTRICT,
    checkpoint_id TEXT NOT NULL REFERENCES manager.sandbox_runtime_checkpoints(operation_id) ON DELETE RESTRICT,
    target_sandbox_id TEXT NOT NULL UNIQUE REFERENCES manager.sandboxes(sandbox_id) ON DELETE RESTRICT,
    generation_id TEXT NOT NULL REFERENCES manager.rootfs_generations(generation_id) ON DELETE RESTRICT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX idx_runtime_checkpoint_fork_image ON manager.sandbox_runtime_checkpoint_forks(checkpoint_id);

-- +goose StatementBegin
CREATE FUNCTION manager.guard_runtime_checkpoint_fork() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP<>'INSERT' THEN
        RAISE EXCEPTION 'Memory fork custody is immutable' USING ERRCODE='23514';
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM manager.sandbox_lifecycle_txns l
        JOIN manager.sandboxes source ON source.sandbox_id=l.sandbox_id
        JOIN manager.sandboxes target ON target.sandbox_id=l.target_sandbox_id
        JOIN manager.sandbox_runtime_checkpoint_refs r ON r.sandbox_id=source.sandbox_id
        JOIN manager.sandbox_runtime_checkpoints c ON c.operation_id=r.checkpoint_id
        JOIN manager.sandbox_lifecycle_txns capture ON capture.txn_id=c.operation_id
        JOIN manager.sandbox_rootfs_bindings b ON b.sandbox_id=target.sandbox_id
        JOIN manager.rootfs_filesystems f ON f.filesystem_id=b.filesystem_id
        WHERE l.txn_id=NEW.operation_id AND l.kind='fork' AND l.source='manual' AND NOT l.cancelable
            AND l.phase='publishing' AND l.from_runtime_id='' AND l.to_runtime_id=''
            AND l.from_runtime_namespace='' AND l.to_runtime_namespace=''
            AND l.target_sandbox_id=NEW.target_sandbox_id AND l.expected_generation_id=NEW.generation_id
            AND l.target_generation_id=NEW.generation_id
            AND source.desired_state='paused' AND source.runtime_id='' AND source.runtime_namespace=''
            AND source.runtime_generation=l.from_generation AND source.lifecycle_epoch=l.epoch
            AND (source.hard_expires_at IS NULL OR source.hard_expires_at>clock_timestamp())
            AND target.desired_state='paused' AND target.runtime_generation=0 AND target.runtime_id='' AND target.runtime_namespace=''
            AND target.team_id=source.team_id AND target.team_id=c.evidence->'assignment'->>'team_id'
            AND (target.hard_expires_at IS NULL OR target.hard_expires_at>clock_timestamp())
            AND r.checkpoint_id=NEW.checkpoint_id AND r.generation_id=NEW.generation_id AND r.runtime_generation=source.runtime_generation
            AND capture.phase='committed' AND c.evidence ? 'finalized'
            AND c.evidence->'publication'->'capture'->'rootfs'->'generation'->>'generation_id'=NEW.generation_id
            AND f.filesystem_id=target.sandbox_id AND f.head_generation_id=NEW.generation_id
    ) THEN
        RAISE EXCEPTION 'Memory fork requires exact paused source custody and atomic child RootFS' USING ERRCODE='23514';
    END IF;
    RETURN NEW;
END;
$$;
-- +goose StatementEnd
CREATE TRIGGER runtime_checkpoint_fork_guard BEFORE INSERT OR UPDATE OR DELETE ON manager.sandbox_runtime_checkpoint_forks
    FOR EACH ROW EXECUTE FUNCTION manager.guard_runtime_checkpoint_fork();

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
CREATE FUNCTION manager.guard_runtime_checkpoint_fork_completion() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE f manager.sandbox_runtime_checkpoint_forks%ROWTYPE;
BEGIN
    SELECT * INTO f FROM manager.sandbox_runtime_checkpoint_forks WHERE operation_id=NEW.txn_id;
    IF NOT FOUND OR NEW.phase=OLD.phase THEN RETURN NEW; END IF;
    IF NEW.phase='aborted' OR NEW.phase='committed' AND NOT EXISTS (
        SELECT 1 FROM manager.sandbox_runtime_checkpoint_refs r JOIN manager.sandboxes child ON child.sandbox_id=r.sandbox_id
        WHERE r.sandbox_id=f.target_sandbox_id AND r.checkpoint_id=f.checkpoint_id AND r.generation_id=f.generation_id
            AND r.runtime_generation=0 AND child.runtime_generation=0 AND child.desired_state='paused'
            AND NEW.prepared_generation_id=f.generation_id
    ) THEN
        RAISE EXCEPTION 'Memory fork must atomically commit child memory and filesystem ownership' USING ERRCODE='23514';
    END IF;
    RETURN NEW;
END;
$$;
-- +goose StatementEnd
CREATE TRIGGER runtime_checkpoint_fork_completion_guard BEFORE UPDATE ON manager.sandbox_lifecycle_txns
    FOR EACH ROW EXECUTE FUNCTION manager.guard_runtime_checkpoint_fork_completion();

-- +goose Down
-- +goose StatementBegin
DO $$ BEGIN
    RAISE EXCEPTION 'Memory fork custody cannot be discarded' USING ERRCODE='55000';
END $$;
-- +goose StatementEnd

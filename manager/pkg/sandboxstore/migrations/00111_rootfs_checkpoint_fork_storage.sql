-- +goose Up
-- Immutable fork identity proves retry mode, but is not permanent disk custody
-- after its child has completed restore or deletion and released the image.
-- The existing insertion guard still enforces exact atomic fork ownership.
DROP TRIGGER runtime_checkpoint_fork_guard ON manager.sandbox_runtime_checkpoint_forks;
CREATE TRIGGER runtime_checkpoint_fork_guard BEFORE INSERT ON manager.sandbox_runtime_checkpoint_forks
    FOR EACH ROW EXECUTE FUNCTION manager.guard_runtime_checkpoint_fork();
ALTER TABLE manager.sandbox_runtime_checkpoint_forks
    ADD COLUMN generation_ref TEXT REFERENCES manager.rootfs_generations(generation_id) ON DELETE RESTRICT,
    ADD COLUMN storage_released_at TIMESTAMPTZ;
UPDATE manager.sandbox_runtime_checkpoint_forks SET generation_ref=generation_id;
ALTER TABLE manager.sandbox_runtime_checkpoint_forks
    DROP CONSTRAINT sandbox_runtime_checkpoint_forks_generation_id_fkey,
    ADD CONSTRAINT runtime_checkpoint_fork_storage_shape CHECK (
        (generation_ref=generation_id AND storage_released_at IS NULL)
        OR (generation_ref IS NULL AND storage_released_at IS NOT NULL));
DROP INDEX manager.runtime_checkpoint_forks_generation_idx;
CREATE INDEX runtime_checkpoint_forks_generation_ref_idx ON manager.sandbox_runtime_checkpoint_forks(generation_ref);

-- +goose StatementBegin
CREATE FUNCTION manager.guard_runtime_checkpoint_fork_storage() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP='INSERT' THEN
        IF NEW.storage_released_at IS NOT NULL OR (NEW.generation_ref IS NOT NULL AND NEW.generation_ref<>NEW.generation_id) THEN
            RAISE EXCEPTION 'Memory fork must retain initial disk custody' USING ERRCODE='23514';
        END IF;
        NEW.generation_ref := NEW.generation_id;
        RETURN NEW;
    END IF;
    IF TG_OP='DELETE' THEN
        RAISE EXCEPTION 'Memory fork custody identity is immutable' USING ERRCODE='23514';
    END IF;
    IF ROW(NEW.operation_id,NEW.checkpoint_id,NEW.target_sandbox_id,NEW.generation_id,NEW.created_at)
        IS DISTINCT FROM ROW(OLD.operation_id,OLD.checkpoint_id,OLD.target_sandbox_id,OLD.generation_id,OLD.created_at)
        OR OLD.storage_released_at IS NOT NULL OR NEW.generation_ref IS NOT NULL OR NEW.storage_released_at IS NULL
        OR NOT EXISTS (SELECT 1 FROM manager.sandbox_lifecycle_txns l WHERE l.txn_id=OLD.operation_id AND l.phase='committed')
        OR EXISTS (SELECT 1 FROM manager.sandbox_runtime_checkpoint_refs r
            WHERE r.sandbox_id=OLD.target_sandbox_id AND r.checkpoint_id=OLD.checkpoint_id) THEN
        RAISE EXCEPTION 'Memory fork disk release requires terminal child custody' USING ERRCODE='23514';
    END IF;
    RETURN NEW;
END $$;
-- +goose StatementEnd
CREATE TRIGGER runtime_checkpoint_fork_storage_guard BEFORE INSERT OR UPDATE OR DELETE
    ON manager.sandbox_runtime_checkpoint_forks FOR EACH ROW EXECUTE FUNCTION manager.guard_runtime_checkpoint_fork_storage();
CREATE INDEX rootfs_filesystems_unbound_head_order_idx ON manager.rootfs_filesystems(updated_at,filesystem_id)
    WHERE head_generation_id IS NOT NULL;

-- +goose Down
-- +goose StatementBegin
DO $$ BEGIN RAISE EXCEPTION 'Released fork storage cannot reacquire collected generations' USING ERRCODE='55000'; END $$;
-- +goose StatementEnd

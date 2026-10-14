-- +goose Up
-- A writer can now publish several immutable live cuts, plus one terminal
-- lifecycle generation at sequence zero. Retain the old single-terminal rule.
ALTER TABLE manager.rootfs_generations ADD COLUMN live_commit_sequence BIGINT NOT NULL DEFAULT 0 CHECK (live_commit_sequence>=0);
ALTER TABLE manager.rootfs_generations DROP CONSTRAINT rootfs_generations_filesystem_id_writer_epoch_key;
ALTER TABLE manager.rootfs_generations ADD CONSTRAINT rootfs_generations_filesystem_epoch_cut_key UNIQUE(filesystem_id,writer_epoch,live_commit_sequence);
-- +goose StatementBegin
CREATE FUNCTION manager.guard_rootfs_live_cut_sequence() RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.live_commit_sequence<>OLD.live_commit_sequence THEN RAISE EXCEPTION 'generation live cut sequence is immutable'; END IF;
    RETURN NEW;
END $$;
-- +goose StatementEnd
CREATE TRIGGER guard_rootfs_live_cut_sequence BEFORE UPDATE OF live_commit_sequence ON manager.rootfs_generations
FOR EACH ROW EXECUTE FUNCTION manager.guard_rootfs_live_cut_sequence();
-- The launch binding and filesystem lifecycle head remain immutable during a
-- writer's lifetime. Live commits have a separate durable progress cursor.
-- Keep both the remote checkpoint and the locally installed read base pinned:
-- a lost reply or a failed local WAL switch cannot release either one.
CREATE TABLE manager.rootfs_writer_live_progress (
    grant_id TEXT PRIMARY KEY REFERENCES manager.rootfs_writer_grants(grant_id) ON DELETE CASCADE,
    committed_generation_id TEXT NOT NULL,
    committed_generation_ref TEXT REFERENCES manager.rootfs_generations(generation_id),
    committed_sequence BIGINT NOT NULL CHECK (committed_sequence > 0),
    installed_generation_id TEXT NOT NULL,
    installed_generation_ref TEXT REFERENCES manager.rootfs_generations(generation_id),
    installed_sequence BIGINT NOT NULL CHECK (installed_sequence >= 0 AND installed_sequence <= committed_sequence),
    operation_id TEXT NOT NULL,
    request_digest BYTEA NOT NULL CHECK (octet_length(request_digest)=32),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CHECK (committed_generation_ref IS NULL OR committed_generation_ref=committed_generation_id),
    CHECK (installed_generation_ref IS NULL OR installed_generation_ref=installed_generation_id)
);
CREATE INDEX rootfs_writer_live_committed_idx ON manager.rootfs_writer_live_progress(committed_generation_ref);
CREATE INDEX rootfs_writer_live_installed_idx ON manager.rootfs_writer_live_progress(installed_generation_ref);

-- +goose StatementBegin
CREATE FUNCTION manager.guard_rootfs_live_progress() RETURNS TRIGGER LANGUAGE plpgsql AS $$
DECLARE writer manager.rootfs_writer_grants%ROWTYPE;
DECLARE committed manager.rootfs_generations%ROWTYPE;
BEGIN
    IF TG_OP='DELETE' THEN
        SELECT * INTO writer FROM manager.rootfs_writer_grants WHERE grant_id=OLD.grant_id FOR KEY SHARE;
        IF FOUND AND writer.state IN ('issued','consumed','retiring') THEN
            RAISE EXCEPTION 'live progress cannot be deleted under an active writer';
        END IF;
        RETURN OLD;
    END IF;
    SELECT * INTO writer FROM manager.rootfs_writer_grants WHERE grant_id=NEW.grant_id FOR KEY SHARE;
    IF writer.grant_id IS NULL THEN RAISE EXCEPTION 'live progress requires its writer'; END IF;
    IF TG_OP='UPDATE' THEN
        IF NEW.grant_id<>OLD.grant_id OR NEW.committed_sequence<OLD.committed_sequence OR NEW.installed_sequence<OLD.installed_sequence THEN
            RAISE EXCEPTION 'live progress identity and sequence cannot regress';
        END IF;
        IF NEW.committed_sequence=OLD.committed_sequence AND
            (NEW.committed_generation_id<>OLD.committed_generation_id OR NEW.operation_id<>OLD.operation_id OR NEW.request_digest<>OLD.request_digest) THEN
            RAISE EXCEPTION 'live cut receipt is immutable at its sequence';
        END IF;
    END IF;
    IF writer.state IN ('retired','canceled') THEN
        IF TG_OP<>'UPDATE' OR NEW.committed_generation_ref IS NOT NULL OR NEW.installed_generation_ref IS NOT NULL OR
            NEW.committed_sequence<>OLD.committed_sequence OR NEW.installed_sequence<>OLD.installed_sequence OR NEW.installed_generation_id<>OLD.installed_generation_id THEN
            RAISE EXCEPTION 'terminal live progress can only release its generation custody';
        END IF;
        RETURN NEW;
    END IF;
    IF writer.state NOT IN ('consumed','retiring') OR NEW.committed_generation_ref IS NULL OR NEW.installed_generation_ref IS NULL THEN
        RAISE EXCEPTION 'active live progress requires both generation pins';
    END IF;
    IF TG_OP='INSERT' OR NEW.committed_sequence<>OLD.committed_sequence THEN
        IF writer.state<>'consumed' OR writer.lease_expires_at IS NULL OR writer.lease_expires_at<=clock_timestamp() THEN
            RAISE EXCEPTION 'new live cuts require an unexpired consumed writer';
        END IF;
    END IF;
    SELECT * INTO committed FROM manager.rootfs_generations WHERE generation_id=NEW.committed_generation_ref;
    IF committed.filesystem_id IS DISTINCT FROM writer.filesystem_id OR committed.writer_epoch<>writer.writer_epoch OR committed.durability_state<>'s3_materialized'
        OR committed.live_commit_sequence<>NEW.committed_sequence THEN
        RAISE EXCEPTION 'live cut does not match the writer filesystem and epoch';
    END IF;
    IF committed.storage_inventory_required OR NOT EXISTS (SELECT 1 FROM manager.rootfs_node_uploads receipt
        WHERE receipt.grant_id=writer.grant_id AND receipt.operation_id=NEW.operation_id
        AND receipt.state='published' AND receipt.generation_id=NEW.committed_generation_id) THEN
        RAISE EXCEPTION 'live cut lacks published object custody';
    END IF;
    IF NOT EXISTS (SELECT 1 FROM manager.rootfs_filesystems filesystem
        WHERE filesystem.filesystem_id=writer.filesystem_id AND filesystem.writer_epoch=writer.writer_epoch
        AND filesystem.head_generation_id=writer.initial_generation_id
        AND filesystem.base_artifact_digest=committed.base_artifact_digest AND filesystem.format_generation=committed.format_generation) THEN
        RAISE EXCEPTION 'live writer lifecycle head or format changed';
    END IF;
    IF TG_OP='INSERT' THEN
        IF NEW.installed_sequence<>0 OR NEW.installed_generation_id<>writer.initial_generation_id THEN
            RAISE EXCEPTION 'first live cut must retain the launch read base';
        END IF;
        IF committed.parent_generation_id IS DISTINCT FROM writer.initial_generation_id THEN
            RAISE EXCEPTION 'first live cut must descend from the launch generation';
        END IF;
    ELSIF NEW.committed_sequence<>OLD.committed_sequence THEN
        IF committed.parent_generation_id IS DISTINCT FROM OLD.committed_generation_id OR NEW.operation_id=OLD.operation_id THEN
            RAISE EXCEPTION 'live cut must advance the previous committed generation and operation';
        END IF;
    END IF;
    IF TG_OP='UPDATE' THEN
        IF NEW.installed_sequence<>OLD.installed_sequence OR NEW.installed_generation_id<>OLD.installed_generation_id THEN
            IF NEW.installed_sequence<>NEW.committed_sequence OR NEW.installed_generation_id<>NEW.committed_generation_id THEN
                RAISE EXCEPTION 'installation must acknowledge the latest committed cut';
            END IF;
        END IF;
    END IF;
    RETURN NEW;
END $$;
-- +goose StatementEnd
CREATE TRIGGER guard_rootfs_live_progress BEFORE INSERT OR UPDATE OR DELETE ON manager.rootfs_writer_live_progress
FOR EACH ROW EXECUTE FUNCTION manager.guard_rootfs_live_progress();

-- A lifecycle cut retains its launch parent for proof compatibility, but its
-- descriptor can also inherit objects from a rotated live read base. Protect
-- that second dependency until checksum-verified inventory finishes.
CREATE TABLE manager.rootfs_generation_live_dependencies (
    generation_id TEXT NOT NULL REFERENCES manager.rootfs_generations(generation_id) ON DELETE CASCADE,
    base_generation_id TEXT NOT NULL REFERENCES manager.rootfs_generations(generation_id),
    PRIMARY KEY (generation_id,base_generation_id),
    CHECK (generation_id<>base_generation_id)
);
CREATE INDEX rootfs_generation_live_base_idx ON manager.rootfs_generation_live_dependencies(base_generation_id);
-- +goose StatementBegin
CREATE FUNCTION manager.release_rootfs_live_dependencies() RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.inventory_complete AND NOT OLD.inventory_complete THEN
        DELETE FROM manager.rootfs_generation_live_dependencies WHERE generation_id=NEW.generation_id;
    END IF;
    RETURN NULL;
END $$;
-- +goose StatementEnd
CREATE TRIGGER release_rootfs_live_dependencies AFTER UPDATE OF inventory_complete ON manager.rootfs_generations
FOR EACH ROW EXECUTE FUNCTION manager.release_rootfs_live_dependencies();

-- +goose StatementBegin
CREATE FUNCTION manager.close_rootfs_live_progress() RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.state IN ('retired','canceled') AND OLD.state NOT IN ('retired','canceled') THEN
        UPDATE manager.rootfs_writer_live_progress SET committed_generation_ref=NULL,
            installed_generation_ref=NULL,updated_at=clock_timestamp() WHERE grant_id=NEW.grant_id;
    END IF;
    RETURN NULL;
END $$;
-- +goose StatementEnd
CREATE TRIGGER close_rootfs_live_progress AFTER UPDATE OF state ON manager.rootfs_writer_grants
FOR EACH ROW EXECUTE FUNCTION manager.close_rootfs_live_progress();

-- +goose Down
-- +goose StatementBegin
DO $$ BEGIN
    LOCK TABLE manager.rootfs_writer_live_progress,manager.rootfs_generation_live_dependencies IN ACCESS EXCLUSIVE MODE;
    IF EXISTS (SELECT 1 FROM manager.rootfs_writer_live_progress WHERE committed_generation_ref IS NOT NULL OR installed_generation_ref IS NOT NULL)
        OR EXISTS (SELECT 1 FROM manager.rootfs_generation_live_dependencies)
        OR EXISTS (SELECT 1 FROM manager.rootfs_generations WHERE live_commit_sequence>0) THEN
        RAISE EXCEPTION 'live commit rollback requires retired writers and completed generation inventories';
    END IF;
END $$;
-- +goose StatementEnd
DROP TRIGGER close_rootfs_live_progress ON manager.rootfs_writer_grants;
DROP FUNCTION manager.close_rootfs_live_progress();
DROP TRIGGER guard_rootfs_live_progress ON manager.rootfs_writer_live_progress;
DROP FUNCTION manager.guard_rootfs_live_progress();
DROP TRIGGER release_rootfs_live_dependencies ON manager.rootfs_generations;
DROP FUNCTION manager.release_rootfs_live_dependencies();
DROP TABLE manager.rootfs_generation_live_dependencies;
DROP TABLE manager.rootfs_writer_live_progress;
DROP TRIGGER guard_rootfs_live_cut_sequence ON manager.rootfs_generations;
DROP FUNCTION manager.guard_rootfs_live_cut_sequence();
ALTER TABLE manager.rootfs_generations DROP CONSTRAINT rootfs_generations_filesystem_epoch_cut_key;
ALTER TABLE manager.rootfs_generations DROP COLUMN live_commit_sequence;
ALTER TABLE manager.rootfs_generations ADD CONSTRAINT rootfs_generations_filesystem_id_writer_epoch_key UNIQUE(filesystem_id,writer_epoch);

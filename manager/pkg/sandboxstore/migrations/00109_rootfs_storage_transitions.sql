-- +goose Up
-- Byte-time authority survives removal of the last filesystem. A transition is
-- committed with its object references, rather than inferred by a later scan.
ALTER TABLE manager.rootfs_generation_materialization_objects ADD COLUMN team_id TEXT;
UPDATE manager.rootfs_generation_materialization_objects locator
SET team_id = filesystem.team_id
FROM manager.rootfs_generations generation
JOIN manager.rootfs_filesystems filesystem USING (filesystem_id)
WHERE generation.generation_id = locator.generation_id;
ALTER TABLE manager.rootfs_generation_materialization_objects ALTER COLUMN team_id SET NOT NULL;

CREATE TABLE manager.rootfs_storage_team_objects (
    team_id TEXT NOT NULL,
    object_key TEXT NOT NULL REFERENCES manager.rootfs_materialization_objects(object_key) ON DELETE RESTRICT,
    reference_count BIGINT NOT NULL CHECK (reference_count > 0),
    accounted_bytes BIGINT NOT NULL CHECK (accounted_bytes >= 0),
    PRIMARY KEY (team_id, object_key)
);
CREATE INDEX rootfs_storage_team_objects_key_idx ON manager.rootfs_storage_team_objects(object_key);

CREATE TABLE manager.rootfs_storage_usage (
    team_id TEXT PRIMARY KEY,
    storage_bytes BIGINT NOT NULL DEFAULT 0 CHECK (storage_bytes >= 0),
    object_count BIGINT NOT NULL DEFAULT 0 CHECK (object_count >= 0),
    revision BIGINT NOT NULL DEFAULT 0,
    last_metered_at TIMESTAMPTZ,
    changed_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX rootfs_storage_usage_metering_order_idx ON manager.rootfs_storage_usage(last_metered_at NULLS FIRST,team_id);
CREATE TABLE manager.rootfs_storage_transitions (
    team_id TEXT NOT NULL REFERENCES manager.rootfs_storage_usage(team_id),
    revision BIGINT NOT NULL,
    storage_bytes BIGINT NOT NULL CHECK (storage_bytes >= 0),
    changed_at TIMESTAMPTZ NOT NULL,
    reconciliation BOOLEAN NOT NULL DEFAULT FALSE,
    PRIMARY KEY (team_id, revision)
);
CREATE TABLE manager.rootfs_storage_reconciliation_audit (
    team_id TEXT NOT NULL,
    reconciled_at TIMESTAMPTZ NOT NULL,
    previous_state JSONB NOT NULL,
    reason TEXT NOT NULL,
    PRIMARY KEY (team_id, reconciled_at)
);

INSERT INTO manager.rootfs_storage_team_objects
SELECT locator.team_id, locator.object_key, COUNT(*),
    CASE WHEN object.uploaded_at IS NOT NULL THEN object.object_size ELSE 0 END
FROM manager.rootfs_generation_materialization_objects locator
JOIN manager.rootfs_materialization_objects object USING (object_key)
GROUP BY locator.team_id, locator.object_key, object.uploaded_at, object.object_size;
INSERT INTO manager.rootfs_storage_usage (team_id, storage_bytes, object_count)
SELECT teams.team_id, COALESCE(SUM(objects.accounted_bytes), 0),
    COUNT(objects.object_key) FILTER (WHERE object.uploaded_at IS NOT NULL)
FROM (SELECT DISTINCT team_id FROM manager.rootfs_filesystems) teams
LEFT JOIN manager.rootfs_storage_team_objects objects USING (team_id)
LEFT JOIN manager.rootfs_materialization_objects object USING (object_key)
GROUP BY teams.team_id;

-- Existing installations may already have a nonzero projection for a team
-- whose entire storage has gone. Its deletion time is unknown, not NOW().
-- +goose StatementBegin
DO $$ BEGIN
    IF to_regclass('metering.storage_projection_state') IS NOT NULL THEN
        INSERT INTO manager.rootfs_storage_usage (team_id)
        SELECT subject_id FROM metering.storage_projection_state
        WHERE subject_type = 'rootfs' AND subject_id = team_id
        ON CONFLICT (team_id) DO NOTHING;
    END IF;
END $$;
-- +goose StatementEnd
INSERT INTO manager.rootfs_storage_transitions (team_id, revision, storage_bytes, changed_at, reconciliation)
SELECT team_id, revision, storage_bytes, changed_at, TRUE FROM manager.rootfs_storage_usage;

-- +goose StatementBegin
CREATE FUNCTION manager.adjust_rootfs_storage_usage(owner TEXT, byte_delta BIGINT, object_delta BIGINT)
RETURNS VOID LANGUAGE plpgsql AS $$
DECLARE current_usage manager.rootfs_storage_usage; BEGIN
    INSERT INTO manager.rootfs_storage_usage (team_id) VALUES (owner) ON CONFLICT DO NOTHING;
    SELECT * INTO current_usage FROM manager.rootfs_storage_usage WHERE team_id = owner FOR UPDATE;
    IF byte_delta = 0 AND object_delta = 0 THEN RETURN; END IF;
    UPDATE manager.rootfs_storage_usage
    SET storage_bytes = storage_bytes + byte_delta, object_count = object_count + object_delta,
        revision = revision + 1,
        changed_at = GREATEST(clock_timestamp(), changed_at + INTERVAL '1 microsecond')
    WHERE team_id = owner RETURNING * INTO current_usage;
    INSERT INTO manager.rootfs_storage_transitions (team_id, revision, storage_bytes, changed_at)
    VALUES (owner, current_usage.revision, current_usage.storage_bytes, current_usage.changed_at);
END $$;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE FUNCTION manager.bind_rootfs_storage_owner() RETURNS TRIGGER LANGUAGE plpgsql AS $$
DECLARE owner TEXT; BEGIN
    SELECT filesystem.team_id INTO STRICT owner FROM manager.rootfs_generations generation
    JOIN manager.rootfs_filesystems filesystem USING (filesystem_id)
    WHERE generation.generation_id = NEW.generation_id;
    IF TG_OP = 'UPDATE' THEN
        IF NEW.generation_id <> OLD.generation_id OR NEW.object_key <> OLD.object_key
            OR NEW.team_id <> OLD.team_id THEN
            RAISE EXCEPTION 'immutable rootfs object reference cannot change owner or identity';
        END IF;
    END IF;
    NEW.team_id := owner;
    RETURN NEW;
END $$;
-- +goose StatementEnd
CREATE TRIGGER bind_rootfs_storage_owner BEFORE INSERT OR UPDATE
ON manager.rootfs_generation_materialization_objects FOR EACH ROW EXECUTE FUNCTION manager.bind_rootfs_storage_owner();

-- +goose StatementBegin
CREATE FUNCTION manager.track_rootfs_storage_reference() RETURNS TRIGGER LANGUAGE plpgsql AS $$
DECLARE owner TEXT; key TEXT; bytes BIGINT; uploaded BOOLEAN;
    previous manager.rootfs_storage_team_objects; BEGIN
    IF TG_OP = 'INSERT' THEN owner := NEW.team_id; key := NEW.object_key;
    ELSE owner := OLD.team_id; key := OLD.object_key; END IF;
    -- Serialize registration with upload completion before locking the team.
    SELECT object_size, uploaded_at IS NOT NULL INTO STRICT bytes, uploaded
    FROM manager.rootfs_materialization_objects WHERE object_key = key FOR SHARE;
    PERFORM manager.adjust_rootfs_storage_usage(owner, 0, 0);
    SELECT * INTO previous FROM manager.rootfs_storage_team_objects
    WHERE team_id = owner AND object_key = key;
    IF TG_OP = 'INSERT' THEN
        IF FOUND THEN
            UPDATE manager.rootfs_storage_team_objects SET reference_count = reference_count + 1
            WHERE team_id = owner AND object_key = key;
        ELSE
            INSERT INTO manager.rootfs_storage_team_objects VALUES (owner, key, 1, CASE WHEN uploaded THEN bytes ELSE 0 END);
            PERFORM manager.adjust_rootfs_storage_usage(owner, CASE WHEN uploaded THEN bytes ELSE 0 END, CASE WHEN uploaded THEN 1 ELSE 0 END);
        END IF;
    ELSE
        IF previous.reference_count > 1 THEN
            UPDATE manager.rootfs_storage_team_objects SET reference_count = reference_count - 1
            WHERE team_id = owner AND object_key = key;
        ELSE
            DELETE FROM manager.rootfs_storage_team_objects WHERE team_id = owner AND object_key = key;
            PERFORM manager.adjust_rootfs_storage_usage(owner, -previous.accounted_bytes, CASE WHEN uploaded THEN -1 ELSE 0 END);
        END IF;
    END IF;
    RETURN NULL;
END $$;
-- +goose StatementEnd
CREATE TRIGGER track_rootfs_storage_reference AFTER INSERT OR DELETE
ON manager.rootfs_generation_materialization_objects FOR EACH ROW EXECUTE FUNCTION manager.track_rootfs_storage_reference();

-- +goose StatementBegin
CREATE FUNCTION manager.track_rootfs_storage_upload() RETURNS TRIGGER LANGUAGE plpgsql AS $$
DECLARE owner TEXT; previous_bytes BIGINT; bytes BIGINT; count_delta BIGINT; BEGIN
    bytes := CASE WHEN NEW.uploaded_at IS NOT NULL THEN NEW.object_size ELSE 0 END;
    count_delta := (CASE WHEN NEW.uploaded_at IS NOT NULL THEN 1 ELSE 0 END)
        - (CASE WHEN OLD.uploaded_at IS NOT NULL THEN 1 ELSE 0 END);
    FOR owner IN SELECT team_id FROM manager.rootfs_storage_team_objects
        WHERE object_key = NEW.object_key ORDER BY team_id LOOP
        PERFORM manager.adjust_rootfs_storage_usage(owner, 0, 0);
        SELECT accounted_bytes INTO previous_bytes FROM manager.rootfs_storage_team_objects
        WHERE team_id = owner AND object_key = NEW.object_key;
        UPDATE manager.rootfs_storage_team_objects SET accounted_bytes = bytes
        WHERE team_id = owner AND object_key = NEW.object_key;
        PERFORM manager.adjust_rootfs_storage_usage(owner, bytes - previous_bytes, count_delta);
    END LOOP;
    RETURN NULL;
END $$;
-- +goose StatementEnd
CREATE TRIGGER track_rootfs_storage_upload AFTER UPDATE OF uploaded_at, object_size
ON manager.rootfs_materialization_objects FOR EACH ROW
WHEN (OLD.uploaded_at IS DISTINCT FROM NEW.uploaded_at OR OLD.object_size IS DISTINCT FROM NEW.object_size)
EXECUTE FUNCTION manager.track_rootfs_storage_upload();

-- +goose Down
-- Capacity transitions and reconciliation receipts are financial authority.
-- A binary rollback must preserve them and use a compatible processor.
-- +goose StatementBegin
DO $$ BEGIN RAISE EXCEPTION 'RootFS capacity history cannot be discarded' USING ERRCODE='55000'; END $$;
-- +goose StatementEnd
DROP TRIGGER track_rootfs_storage_upload ON manager.rootfs_materialization_objects;
DROP TRIGGER track_rootfs_storage_reference ON manager.rootfs_generation_materialization_objects;
DROP TRIGGER bind_rootfs_storage_owner ON manager.rootfs_generation_materialization_objects;
DROP FUNCTION manager.track_rootfs_storage_upload();
DROP FUNCTION manager.track_rootfs_storage_reference();
DROP FUNCTION manager.bind_rootfs_storage_owner();
DROP FUNCTION manager.adjust_rootfs_storage_usage(TEXT, BIGINT, BIGINT);
DROP TABLE manager.rootfs_storage_reconciliation_audit;
DROP TABLE manager.rootfs_storage_transitions;
DROP TABLE manager.rootfs_storage_usage;
DROP TABLE manager.rootfs_storage_team_objects;
ALTER TABLE manager.rootfs_generation_materialization_objects DROP COLUMN team_id;

-- +goose Up

-- The original exact-writer capture intent was introduced for template
-- builds. Public named snapshots use the same durable checkpoint authority,
-- so retain their immutable metadata on the existing intent instead of
-- creating a second running-capture state machine.
ALTER TABLE manager.rootfs_running_template_captures
    ADD COLUMN capture_kind text NOT NULL DEFAULT 'template',
    ADD COLUMN snapshot_name text NOT NULL DEFAULT 'Template RootFS capture',
    ADD COLUMN snapshot_description text NOT NULL DEFAULT 'Internal immutable live-writer checkpoint retained by a template.',
    ADD COLUMN snapshot_expires_at timestamp with time zone,
    ADD CONSTRAINT rootfs_running_template_captures_kind_check
        CHECK (capture_kind IN ('template', 'snapshot'));

-- During a rolling manager upgrade, an older RootFS authority replica can
-- receive the node callback and still supply the legacy template metadata.
-- Keep the durable capture intent authoritative regardless of callback
-- replica version.
-- +goose StatementBegin
CREATE FUNCTION manager.apply_running_rootfs_capture_snapshot_metadata()
RETURNS trigger AS $$
DECLARE
    capture_name text;
    capture_description text;
    capture_expires_at timestamp with time zone;
BEGIN
    SELECT snapshot_name, snapshot_description, snapshot_expires_at
    INTO capture_name, capture_description, capture_expires_at
    FROM manager.rootfs_running_template_captures
    WHERE snapshot_id = NEW.snapshot_id
      AND team_id = NEW.team_id
      AND source_sandbox_id = NEW.source_sandbox_id
      AND target_filesystem_id = NEW.filesystem_id
      AND checkpoint_generation_id = NEW.head_generation_id;

    IF FOUND THEN
        NEW.name := capture_name;
        NEW.description := capture_description;
        NEW.expires_at := capture_expires_at;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
-- +goose StatementEnd

CREATE TRIGGER apply_running_rootfs_capture_snapshot_metadata
    BEFORE INSERT ON manager.rootfs_snapshots
    FOR EACH ROW
    EXECUTE FUNCTION manager.apply_running_rootfs_capture_snapshot_metadata();

-- +goose Down

DROP TRIGGER apply_running_rootfs_capture_snapshot_metadata
    ON manager.rootfs_snapshots;
DROP FUNCTION manager.apply_running_rootfs_capture_snapshot_metadata();

ALTER TABLE manager.rootfs_running_template_captures
    DROP CONSTRAINT rootfs_running_template_captures_kind_check,
    DROP COLUMN snapshot_expires_at,
    DROP COLUMN snapshot_description,
    DROP COLUMN snapshot_name,
    DROP COLUMN capture_kind;

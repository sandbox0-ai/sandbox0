-- +goose Up
-- Fail and retry rather than queue an unbounded runtime catalog lock.
SET LOCAL lock_timeout = '5s';
-- The retired ACK importer kept an external FK after its catalog commit.
-- Preserve its audit rows, but let the current generation/base inventories own
-- object liveness after a fully committed cutover. Incomplete imports keep
-- their original restrictive FK and are handled conservatively by GC.
-- +goose StatementBegin
DO $$
BEGIN
    IF to_regclass('legacy_ack_migration.build_objects') IS NULL
       OR to_regclass('legacy_ack_migration.builds') IS NULL
       OR to_regclass('legacy_ack_migration.sessions') IS NULL THEN
        RETURN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint
        WHERE conrelid='legacy_ack_migration.build_objects'::regclass
          AND confrelid='manager.rootfs_materialization_objects'::regclass
          AND conname='build_objects_object_key_fkey' AND contype='f') THEN
        RETURN;
    END IF;

    -- Serialize the proof with any remaining legacy import activity. Only
    -- obsolete metadata is locked; runtime generation publication is untouched.
    LOCK TABLE legacy_ack_migration.sessions, legacy_ack_migration.builds,
        legacy_ack_migration.build_objects IN SHARE ROW EXCLUSIVE MODE;
    IF EXISTS (SELECT 1 FROM legacy_ack_migration.sessions
        WHERE state IS DISTINCT FROM 'committed' OR commit_digest IS NULL OR committed_at IS NULL)
       OR EXISTS (SELECT 1 FROM legacy_ack_migration.builds
        WHERE lease_owner IS NOT NULL OR lease_token IS NOT NULL OR lease_expires_at IS NOT NULL)
       OR EXISTS (SELECT 1 FROM legacy_ack_migration.build_objects object_reference
        LEFT JOIN legacy_ack_migration.builds build USING(build_id)
        LEFT JOIN legacy_ack_migration.sessions session USING(session_id)
        WHERE build.state IS DISTINCT FROM 'ready'
           OR session.state IS DISTINCT FROM 'committed'
           OR session.commit_digest IS NULL OR session.committed_at IS NULL
           OR object_reference.upload_state IS DISTINCT FROM 'published'
           OR NOT object_reference.result_object) THEN
        RAISE NOTICE 'Incomplete legacy import custody preserved; audit before retiring it';
        RETURN;
    END IF;
    ALTER TABLE legacy_ack_migration.build_objects
        DROP CONSTRAINT build_objects_object_key_fkey;
END $$;
-- +goose StatementEnd

-- +goose Down
-- Archived keys may now refer to objects legitimately reclaimed by runtime GC.
-- +goose StatementBegin
DO $$ BEGIN
    RAISE EXCEPTION USING ERRCODE='55000', MESSAGE='legacy import custody retirement requires compatible forward recovery';
END $$;
-- +goose StatementEnd

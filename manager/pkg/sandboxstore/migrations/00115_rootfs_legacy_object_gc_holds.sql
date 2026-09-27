-- +goose Up
-- Unknown legacy references can share objects with fully registered uploads.
-- Retiring a registered owner must preserve the object until all legacy graphs
-- are authenticated. Keep durable revisit custody rather than orphaning the row.
CREATE TABLE manager.rootfs_legacy_object_gc_holds (
    object_key TEXT PRIMARY KEY REFERENCES manager.rootfs_materialization_objects(object_key) ON DELETE RESTRICT,
    team_id TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX rootfs_legacy_object_gc_holds_order_idx ON manager.rootfs_legacy_object_gc_holds(created_at,object_key);
CREATE INDEX rootfs_generations_legacy_inventory_required_idx ON manager.rootfs_generations(generation_id)
WHERE storage_inventory_required;
-- Serialize the appearance of an unknown graph with physical deletion. Normal
-- publishers already reserve objects before PUT and transfer custody in this
-- transaction; old publishers require the documented maintenance cutover hold.
-- +goose StatementBegin
CREATE FUNCTION manager.guard_rootfs_legacy_inventory() RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.storage_inventory_required THEN
        PERFORM pg_advisory_xact_lock(hashtextextended('rootfs-legacy-inventory-gc',0));
    END IF;
    RETURN NEW;
END $$;
-- +goose StatementEnd
CREATE TRIGGER z_guard_rootfs_legacy_inventory BEFORE INSERT OR UPDATE OF storage_inventory_required
ON manager.rootfs_generations FOR EACH ROW EXECUTE FUNCTION manager.guard_rootfs_legacy_inventory();

-- +goose Down
-- Removing pending custody would make undiscovered shared objects collectible.
-- +goose StatementBegin
DO $$ BEGIN
    RAISE EXCEPTION USING ERRCODE='55000', MESSAGE='legacy object GC custody requires compatible forward recovery';
END $$;
-- +goose StatementEnd

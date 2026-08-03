-- +goose Up

ALTER TABLE manager.rootfs_layers
	ADD COLUMN IF NOT EXISTS head_object_digest TEXT NOT NULL DEFAULT '',
	ADD COLUMN IF NOT EXISTS head_object_media_type TEXT NOT NULL DEFAULT '',
	ADD COLUMN IF NOT EXISTS head_object_size BIGINT NOT NULL DEFAULT 0,
	ADD COLUMN IF NOT EXISTS head_object_key TEXT NOT NULL DEFAULT '',
	ADD COLUMN IF NOT EXISTS head_image_ref TEXT NOT NULL DEFAULT '',
	ADD COLUMN IF NOT EXISTS head_image_digest TEXT NOT NULL DEFAULT '',
	ADD COLUMN IF NOT EXISTS object_inventory_complete BOOLEAN NOT NULL DEFAULT FALSE,
	ADD COLUMN IF NOT EXISTS object_inventory_completed_at TIMESTAMPTZ;

ALTER TABLE manager.rootfs_layers
	DROP CONSTRAINT IF EXISTS rootfs_layers_parent_layer_id_fkey;

ALTER TABLE manager.rootfs_layers
	ADD CONSTRAINT rootfs_layers_parent_layer_id_fkey
	FOREIGN KEY (parent_layer_id)
	REFERENCES manager.rootfs_layers(layer_id)
	ON DELETE SET NULL;

CREATE INDEX IF NOT EXISTS idx_rootfs_layers_head_object
	ON manager.rootfs_layers(head_object_key)
	WHERE head_object_key <> '';

CREATE INDEX IF NOT EXISTS idx_rootfs_layers_inventory_pending
	ON manager.rootfs_layers(created_at)
	WHERE head_object_key <> '' AND object_inventory_complete = FALSE;

CREATE TABLE IF NOT EXISTS manager.rootfs_layer_objects (
	layer_id TEXT NOT NULL REFERENCES manager.rootfs_layers(layer_id) ON DELETE CASCADE,
	object_key TEXT NOT NULL REFERENCES manager.rootfs_objects(object_key) ON DELETE RESTRICT,
	created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
	PRIMARY KEY (layer_id, object_key)
);

CREATE INDEX IF NOT EXISTS idx_rootfs_layer_objects_object
	ON manager.rootfs_layer_objects(object_key);

INSERT INTO manager.rootfs_layer_objects (layer_id, object_key, created_at)
SELECT first_layer_id, object_key, created_at
FROM manager.rootfs_objects
WHERE first_layer_id <> ''
	AND EXISTS (
		SELECT 1
		FROM manager.rootfs_layers layer
		WHERE layer.layer_id = manager.rootfs_objects.first_layer_id
	)
ON CONFLICT (layer_id, object_key) DO NOTHING;

-- +goose Down

DROP INDEX IF EXISTS manager.idx_rootfs_layers_head_object;
DROP INDEX IF EXISTS manager.idx_rootfs_layers_inventory_pending;
DROP INDEX IF EXISTS manager.idx_rootfs_layer_objects_object;
DROP TABLE IF EXISTS manager.rootfs_layer_objects;

ALTER TABLE manager.rootfs_layers
	DROP CONSTRAINT IF EXISTS rootfs_layers_parent_layer_id_fkey;

ALTER TABLE manager.rootfs_layers
	ADD CONSTRAINT rootfs_layers_parent_layer_id_fkey
	FOREIGN KEY (parent_layer_id)
	REFERENCES manager.rootfs_layers(layer_id)
	ON DELETE RESTRICT;

ALTER TABLE manager.rootfs_layers
	DROP COLUMN IF EXISTS object_inventory_completed_at,
	DROP COLUMN IF EXISTS object_inventory_complete,
	DROP COLUMN IF EXISTS head_image_digest,
	DROP COLUMN IF EXISTS head_image_ref,
	DROP COLUMN IF EXISTS head_object_key,
	DROP COLUMN IF EXISTS head_object_size,
	DROP COLUMN IF EXISTS head_object_media_type,
	DROP COLUMN IF EXISTS head_object_digest;

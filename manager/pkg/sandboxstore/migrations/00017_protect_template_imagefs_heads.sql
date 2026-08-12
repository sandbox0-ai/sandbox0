-- +goose Up

-- The manager process runs scheduler migrations first. Keep the guard so the
-- sandbox-store migration set can still be exercised independently.
-- +goose StatementBegin
DO $$
BEGIN
    IF to_regclass('scheduler.scheduler_template_image_revisions') IS NOT NULL THEN
        EXECUTE '
            ALTER TABLE scheduler.scheduler_template_image_revisions
            ADD CONSTRAINT scheduler_template_image_revisions_imagefs_head_fk
            FOREIGN KEY (image_fs_head_id)
            REFERENCES manager.rootfs_heads_v3(head_id)
            ON DELETE RESTRICT';
    END IF;
END
$$;
-- +goose StatementEnd

-- +goose Down

-- +goose StatementBegin
DO $$
BEGIN
    IF to_regclass('scheduler.scheduler_template_image_revisions') IS NOT NULL THEN
        EXECUTE '
            ALTER TABLE scheduler.scheduler_template_image_revisions
            DROP CONSTRAINT IF EXISTS scheduler_template_image_revisions_imagefs_head_fk';
    END IF;
END
$$;
-- +goose StatementEnd

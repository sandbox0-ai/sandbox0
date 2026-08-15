-- +goose Up

-- A deterministic revision ID identifies template content, while an
-- incarnation identifies one durable queue row. Deleting and recreating the
-- same template may resolve a moving tag again, so it must publish a distinct
-- immutable ImageFS Head.
ALTER TABLE scheduler_template_image_revisions
    ADD COLUMN incarnation_id UUID NOT NULL DEFAULT gen_random_uuid();

-- +goose Down

ALTER TABLE scheduler_template_image_revisions
    DROP COLUMN IF EXISTS incarnation_id;

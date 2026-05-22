-- +goose Up
ALTER TABLE functions_revisions
    ADD COLUMN IF NOT EXISTS source_type TEXT NOT NULL DEFAULT 'sandbox_service',
    ADD COLUMN IF NOT EXISTS revision_spec JSONB,
    ADD COLUMN IF NOT EXISTS provenance JSONB NOT NULL DEFAULT '{}'::jsonb;

ALTER TABLE functions_revisions
    DROP CONSTRAINT IF EXISTS functions_revisions_source_type_check;
ALTER TABLE functions_revisions
    ADD CONSTRAINT functions_revisions_source_type_check
        CHECK (source_type IN ('sandbox_service', 'revision_spec', 'artifact'));

UPDATE functions_revisions
SET revision_spec = jsonb_build_object(
        'template_id', source_template_id,
        'runtime_service', service_snapshot,
        'mounts', COALESCE((
            SELECT jsonb_agg(jsonb_build_object(
                'mount_point', mount->>'mount_point',
                'mode', 'read_write',
                'source', jsonb_strip_nulls(jsonb_build_object(
                    'type', 'sandbox_volume',
                    'sandboxvolume_id', mount->>'sandboxvolume_id',
                    'source_sandboxvolume_id', NULLIF(mount->>'source_sandboxvolume_id', ''),
                    'snapshot_id', NULLIF(mount->>'snapshot_id', '')
                ))
            ))
            FROM jsonb_array_elements(restore_mounts) AS mount
        ), '[]'::jsonb)
    ),
    provenance = jsonb_build_object(
        'type', 'sandbox_service',
        'sandbox_service', jsonb_build_object(
            'sandbox_id', source_sandbox_id,
            'service_id', source_service_id,
            'template_id', source_template_id
        )
    )
WHERE revision_spec IS NULL;

ALTER TABLE functions_revisions
    ALTER COLUMN revision_spec SET NOT NULL;

-- +goose Down
ALTER TABLE functions_revisions
    DROP CONSTRAINT IF EXISTS functions_revisions_source_type_check;

ALTER TABLE functions_revisions
    DROP COLUMN IF EXISTS provenance,
    DROP COLUMN IF EXISTS revision_spec,
    DROP COLUMN IF EXISTS source_type;

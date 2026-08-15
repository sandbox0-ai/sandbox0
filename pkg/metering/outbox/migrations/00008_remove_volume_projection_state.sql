-- +goose Up

DELETE FROM projection_outbox
WHERE operation_type = 'storage_state'
  AND payload->>'subject_type' IN ('volume', 'snapshot');

DELETE FROM projection_outbox
WHERE operation_type = 'storage_state_delete'
  AND payload->'state'->>'subject_type' IN ('volume', 'snapshot')
  AND delivered_at IS NOT NULL;

-- Publish tombstones through the existing transactional outbox so ClickHouse
-- cannot retain a mutable Volume projection after PostgreSQL retires it.
INSERT INTO projection_outbox (operation_type, dedupe_key, payload)
SELECT
    'storage_state_delete',
    'retire-volume/' || subject_type || '/' || subject_id,
    jsonb_build_object(
        'state',
        jsonb_strip_nulls(jsonb_build_object(
            'subject_type', subject_type,
            'subject_id', subject_id,
            'product', product,
            'owner_kind', owner_kind,
            'team_id', team_id,
            'user_id', user_id,
            'sandbox_id', sandbox_id,
            'volume_id', volume_id,
            'snapshot_id', snapshot_id,
            'cluster_id', cluster_id,
            'region_id', region_id,
            'size_bytes', size_bytes,
            'observed_at', to_char(observed_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
            'unbilled_byte_nanoseconds', unbilled_byte_nanoseconds
        )),
        'deleted_at', to_char(NOW() AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"')
    )
FROM storage_projection_state
WHERE subject_type IN ('volume', 'snapshot')
ON CONFLICT (operation_type, dedupe_key) DO NOTHING;

DELETE FROM storage_projection_state
WHERE subject_type IN ('volume', 'snapshot');

-- +goose Down

-- Retired projection rows and already-removed outbox operations cannot be reconstructed.

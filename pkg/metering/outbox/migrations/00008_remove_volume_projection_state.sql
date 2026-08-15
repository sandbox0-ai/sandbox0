-- +goose Up

DELETE FROM projection_outbox
WHERE operation_type IN ('storage_state', 'storage_state_delete')
  AND payload->>'subject_type' IN ('volume', 'snapshot');

DELETE FROM projection_outbox
WHERE operation_type = 'storage_state_delete'
  AND payload->'state'->>'subject_type' IN ('volume', 'snapshot');

DELETE FROM storage_projection_state
WHERE subject_type IN ('volume', 'snapshot');

-- +goose Down

-- Retired projection rows and already-removed outbox operations cannot be reconstructed.

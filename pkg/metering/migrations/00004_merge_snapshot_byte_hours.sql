-- +goose Up

UPDATE usage_windows
SET window_type = 'sandbox.volume_byte_hours'
WHERE window_type = 'sandbox.snapshot_byte_hours';

-- +goose Down

UPDATE usage_windows
SET window_type = 'sandbox.snapshot_byte_hours'
WHERE window_type = 'sandbox.volume_byte_hours'
  AND subject_type = 'snapshot';

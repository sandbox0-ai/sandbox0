-- +goose Up

CREATE INDEX idx_allocation_operations_terminal_retention
    ON allocation_operations(allocation_id, completed_at DESC, operation_id)
    WHERE state IN ('committed', 'aborted');

CREATE INDEX idx_transfer_operations_team_terminal_retention
    ON transfer_operations(team_id, completed_at DESC, operation_id)
    WHERE state IN ('committed', 'aborted');

CREATE INDEX idx_transfer_operations_source_all
    ON transfer_operations(source_allocation_id);

CREATE INDEX idx_transfer_operations_destination_all
    ON transfer_operations(destination_allocation_id);

CREATE INDEX idx_allocations_team_released_retention
    ON allocations(team_id, updated_at DESC, allocation_id)
    WHERE state = 'released' AND operation_id IS NULL;

-- +goose Down

DROP INDEX IF EXISTS idx_allocations_team_released_retention;
DROP INDEX IF EXISTS idx_transfer_operations_destination_all;
DROP INDEX IF EXISTS idx_transfer_operations_source_all;
DROP INDEX IF EXISTS idx_transfer_operations_team_terminal_retention;
DROP INDEX IF EXISTS idx_allocation_operations_terminal_retention;

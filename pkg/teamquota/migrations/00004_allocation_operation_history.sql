-- +goose Up

ALTER TABLE allocations
    ADD COLUMN last_operation_generation BIGINT NOT NULL DEFAULT 0
        CHECK (last_operation_generation >= 0),
    ADD COLUMN operation_fence_generation BIGINT NOT NULL DEFAULT 0
        CHECK (operation_fence_generation >= 0);

CREATE TABLE allocation_operations (
    allocation_id TEXT NOT NULL REFERENCES allocations(allocation_id) ON DELETE CASCADE,
    operation_id TEXT NOT NULL,
    operation_kind TEXT NOT NULL,
    operation_generation BIGINT NOT NULL CHECK (operation_generation >= 0),
    request_fingerprint TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('prepared', 'committed', 'aborted')),
    last_error TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ,
    PRIMARY KEY (allocation_id, operation_id)
);

CREATE INDEX idx_allocation_operations_state
    ON allocation_operations(state, created_at);

-- +goose Down

DROP INDEX IF EXISTS idx_allocation_operations_state;
DROP TABLE IF EXISTS allocation_operations;

ALTER TABLE allocations
    DROP COLUMN IF EXISTS operation_fence_generation,
    DROP COLUMN IF EXISTS last_operation_generation;

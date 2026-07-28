-- +goose NO TRANSACTION
-- +goose Up

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_projection_outbox_pending_batch
    ON metering.projection_outbox(batch_id)
    WHERE delivered_at IS NULL;

-- +goose Down

DROP INDEX CONCURRENTLY IF EXISTS metering.idx_projection_outbox_pending_batch;

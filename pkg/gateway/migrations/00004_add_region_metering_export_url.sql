-- +goose Up
ALTER TABLE regions
    ADD COLUMN IF NOT EXISTS metering_export_url TEXT;

-- +goose Down
ALTER TABLE regions
    DROP COLUMN IF EXISTS metering_export_url;

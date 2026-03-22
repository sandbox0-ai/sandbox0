-- +goose Up
CREATE TABLE IF NOT EXISTS regions (
    id TEXT PRIMARY KEY,
    display_name TEXT,
    regional_gateway_url TEXT NOT NULL,
    enabled BOOLEAN NOT NULL DEFAULT true,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_regions_enabled ON regions(enabled);

DROP TRIGGER IF EXISTS update_regions_updated_at ON regions;
CREATE TRIGGER update_regions_updated_at
    BEFORE UPDATE ON regions
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- +goose Down
DROP TRIGGER IF EXISTS update_regions_updated_at ON regions;
DROP INDEX IF EXISTS idx_regions_enabled;
DROP TABLE IF EXISTS regions;

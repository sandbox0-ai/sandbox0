-- +goose Up

CREATE TABLE IF NOT EXISTS team_quota_limits (
    team_id TEXT NOT NULL,
    dimension TEXT NOT NULL,
    limit_value BIGINT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (team_id, dimension),
    CHECK (limit_value >= 0)
);

CREATE INDEX IF NOT EXISTS idx_team_quota_limits_dimension
    ON team_quota_limits(dimension);

-- +goose StatementBegin
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
-- +goose StatementEnd

DROP TRIGGER IF EXISTS update_team_quota_limits_updated_at ON team_quota_limits;
CREATE TRIGGER update_team_quota_limits_updated_at
    BEFORE UPDATE ON team_quota_limits
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- +goose Down

DROP TRIGGER IF EXISTS update_team_quota_limits_updated_at ON team_quota_limits;
DROP INDEX IF EXISTS idx_team_quota_limits_dimension;
DROP TABLE IF EXISTS team_quota_limits;

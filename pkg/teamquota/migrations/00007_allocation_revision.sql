-- +goose Up

ALTER TABLE quota.allocations
    ADD COLUMN revision BIGINT NOT NULL DEFAULT 1
        CHECK (revision > 0);

-- +goose StatementBegin
CREATE OR REPLACE FUNCTION quota.bump_allocation_revision()
RETURNS TRIGGER AS $$
BEGIN
    NEW.revision = OLD.revision + 1;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
-- +goose StatementEnd

DROP TRIGGER IF EXISTS bump_team_quota_allocation_revision
    ON quota.allocations;
CREATE TRIGGER bump_team_quota_allocation_revision
    BEFORE UPDATE ON quota.allocations
    FOR EACH ROW
    EXECUTE FUNCTION quota.bump_allocation_revision();

-- +goose Down

DROP TRIGGER IF EXISTS bump_team_quota_allocation_revision
    ON quota.allocations;
DROP FUNCTION IF EXISTS quota.bump_allocation_revision();
ALTER TABLE quota.allocations
    DROP COLUMN IF EXISTS revision;

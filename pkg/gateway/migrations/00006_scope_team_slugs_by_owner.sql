-- +goose Up
ALTER TABLE IF EXISTS teams
    DROP CONSTRAINT IF EXISTS teams_slug_key;

CREATE UNIQUE INDEX IF NOT EXISTS idx_teams_owner_slug
    ON teams(owner_id, slug)
    WHERE slug IS NOT NULL;

-- +goose Down
DROP INDEX IF EXISTS idx_teams_owner_slug;

-- +goose StatementBegin
DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM teams
        WHERE slug IS NOT NULL
        GROUP BY slug
        HAVING COUNT(*) > 1
    ) THEN
        RAISE EXCEPTION 'cannot restore globally unique team slugs while duplicate slugs exist';
    END IF;
END;
$$;
-- +goose StatementEnd

ALTER TABLE IF EXISTS teams
    ADD CONSTRAINT teams_slug_key UNIQUE (slug);

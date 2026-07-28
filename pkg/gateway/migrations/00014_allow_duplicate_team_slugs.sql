-- +goose Up
-- Version 14 intentionally follows migration versions recorded by existing deployments.
ALTER TABLE IF EXISTS teams
    DROP CONSTRAINT IF EXISTS teams_slug_key;

DROP INDEX IF EXISTS teams_slug_key;
DROP INDEX IF EXISTS idx_teams_owner_slug;

-- +goose Down
-- +goose StatementBegin
DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM teams
        WHERE owner_id IS NOT NULL
          AND slug IS NOT NULL
        GROUP BY owner_id, slug
        HAVING COUNT(*) > 1
    ) THEN
        RAISE EXCEPTION 'cannot restore owner-scoped unique team slugs while duplicate slugs exist';
    END IF;
END;
$$;
-- +goose StatementEnd

CREATE UNIQUE INDEX IF NOT EXISTS idx_teams_owner_slug
    ON teams(owner_id, slug)
    WHERE slug IS NOT NULL;

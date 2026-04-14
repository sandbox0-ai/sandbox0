-- Scope human-readable team slugs to their owning user instead of the global
-- gateway database. Team IDs remain the authorization boundary; slugs are only
-- a user-facing handle.
ALTER TABLE teams DROP CONSTRAINT IF EXISTS teams_slug_key;

CREATE UNIQUE INDEX IF NOT EXISTS idx_teams_owner_slug ON teams(owner_id, slug);

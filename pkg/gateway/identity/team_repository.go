package identity

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

// CreateTeam creates a new team.
func (r *Repository) CreateTeam(ctx context.Context, team *Team) error {
	if team.Slug == "" {
		team.Slug = generateSlug(team.Name)
	}

	err := r.pool.QueryRow(ctx, `
		INSERT INTO teams (name, slug, owner_id, home_region_id)
		VALUES ($1, $2, $3, $4)
		RETURNING id, created_at, updated_at
	`, team.Name, team.Slug, team.OwnerID, team.HomeRegionID).Scan(&team.ID, &team.CreatedAt, &team.UpdatedAt)
	if err != nil {
		if isDuplicateKeyError(err) {
			return ErrTeamAlreadyExists
		}
		return fmt.Errorf("insert team: %w", err)
	}
	return nil
}

// GetTeamByID retrieves a team by ID.
func (r *Repository) GetTeamByID(ctx context.Context, id string) (*Team, error) {
	var team Team
	err := r.pool.QueryRow(ctx, `
		SELECT id, name, slug, owner_id, home_region_id, created_at, updated_at
		FROM teams
		WHERE id = $1
	`, id).Scan(&team.ID, &team.Name, &team.Slug, &team.OwnerID, &team.HomeRegionID, &team.CreatedAt, &team.UpdatedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrTeamNotFound
		}
		return nil, fmt.Errorf("query team: %w", err)
	}
	return &team, nil
}

// GetTeamBySlug retrieves a team by slug.
func (r *Repository) GetTeamBySlug(ctx context.Context, slug string) (*Team, error) {
	var team Team
	err := r.pool.QueryRow(ctx, `
		SELECT id, name, slug, owner_id, home_region_id, created_at, updated_at
		FROM teams
		WHERE slug = $1
	`, slug).Scan(&team.ID, &team.Name, &team.Slug, &team.OwnerID, &team.HomeRegionID, &team.CreatedAt, &team.UpdatedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrTeamNotFound
		}
		return nil, fmt.Errorf("query team: %w", err)
	}
	return &team, nil
}

// UpdateTeam updates a team.
func (r *Repository) UpdateTeam(ctx context.Context, team *Team) error {
	err := r.pool.QueryRow(ctx, `
		UPDATE teams
		SET name = $2, slug = $3, owner_id = $4, home_region_id = $5
		WHERE id = $1
		RETURNING updated_at
	`, team.ID, team.Name, team.Slug, team.OwnerID, team.HomeRegionID).Scan(&team.UpdatedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrTeamNotFound
		}
		if isDuplicateKeyError(err) {
			return ErrTeamAlreadyExists
		}
		return fmt.Errorf("update team: %w", err)
	}
	return nil
}

// DeleteTeam deletes a team.
func (r *Repository) DeleteTeam(ctx context.Context, id string) error {
	result, err := r.pool.Exec(ctx, `DELETE FROM teams WHERE id = $1`, id)
	if err != nil {
		return fmt.Errorf("delete team: %w", err)
	}
	if result.RowsAffected() == 0 {
		return ErrTeamNotFound
	}
	return nil
}

// GetTeamsByUserID retrieves all teams a user belongs to.
func (r *Repository) GetTeamsByUserID(ctx context.Context, userID string) ([]*Team, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT t.id, t.name, t.slug, t.owner_id, t.home_region_id, t.created_at, t.updated_at
		FROM teams t
		INNER JOIN team_members tm ON t.id = tm.team_id
		WHERE tm.user_id = $1
		ORDER BY t.name
	`, userID)
	if err != nil {
		return nil, fmt.Errorf("query teams: %w", err)
	}
	defer rows.Close()

	var teams []*Team
	for rows.Next() {
		var team Team
		if err := rows.Scan(
			&team.ID,
			&team.Name,
			&team.Slug,
			&team.OwnerID,
			&team.HomeRegionID,
			&team.CreatedAt,
			&team.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan team: %w", err)
		}
		teams = append(teams, &team)
	}
	return teams, nil
}

// ListTeamGrantsByUserID retrieves all team grants for a user in one query.
func (r *Repository) ListTeamGrantsByUserID(ctx context.Context, userID string) ([]TeamGrantRecord, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT t.id, tm.role, t.home_region_id
		FROM team_members tm
		INNER JOIN teams t ON t.id = tm.team_id
		WHERE tm.user_id = $1
		ORDER BY t.id
	`, userID)
	if err != nil {
		return nil, fmt.Errorf("query team grants: %w", err)
	}
	defer rows.Close()

	grants := make([]TeamGrantRecord, 0)
	for rows.Next() {
		var grant TeamGrantRecord
		if err := rows.Scan(&grant.TeamID, &grant.TeamRole, &grant.HomeRegionID); err != nil {
			return nil, fmt.Errorf("scan team grant: %w", err)
		}
		grants = append(grants, grant)
	}
	return grants, nil
}

// AddTeamMember adds a user to a team.
func (r *Repository) AddTeamMember(ctx context.Context, member *TeamMember) error {
	err := r.pool.QueryRow(ctx, `
		INSERT INTO team_members (team_id, user_id, role)
		VALUES ($1, $2, $3)
		RETURNING id, joined_at
	`, member.TeamID, member.UserID, member.Role).Scan(&member.ID, &member.JoinedAt)
	if err != nil {
		if isDuplicateKeyError(err) {
			return ErrAlreadyMember
		}
		return fmt.Errorf("insert team member: %w", err)
	}
	return nil
}

// UpdateTeamMemberRole updates a team member's role.
func (r *Repository) UpdateTeamMemberRole(ctx context.Context, teamID, userID, role string) error {
	result, err := r.pool.Exec(ctx, `
		UPDATE team_members SET role = $3 WHERE team_id = $1 AND user_id = $2
	`, teamID, userID, role)
	if err != nil {
		return fmt.Errorf("update member role: %w", err)
	}
	if result.RowsAffected() == 0 {
		return ErrMemberNotFound
	}
	return nil
}

// RemoveTeamMember removes a user from a team.
func (r *Repository) RemoveTeamMember(ctx context.Context, teamID, userID string) error {
	result, err := r.pool.Exec(ctx, `
		DELETE FROM team_members WHERE team_id = $1 AND user_id = $2
	`, teamID, userID)
	if err != nil {
		return fmt.Errorf("remove member: %w", err)
	}
	if result.RowsAffected() == 0 {
		return ErrMemberNotFound
	}
	return nil
}

// GetTeamMembers retrieves all members of a team.
func (r *Repository) GetTeamMembers(ctx context.Context, teamID string) ([]*TeamMemberWithUser, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT tm.id, tm.team_id, tm.user_id, tm.role, tm.joined_at,
		       u.id, u.email, u.name, u.avatar_url, u.email_verified, u.is_admin, u.created_at, u.updated_at
		FROM team_members tm
		INNER JOIN users u ON u.id = tm.user_id
		WHERE tm.team_id = $1
		ORDER BY tm.joined_at
	`, teamID)
	if err != nil {
		return nil, fmt.Errorf("query team members: %w", err)
	}
	defer rows.Close()

	var members []*TeamMemberWithUser
	for rows.Next() {
		var m TeamMemberWithUser
		if err := rows.Scan(
			&m.ID, &m.TeamID, &m.UserID, &m.Role, &m.JoinedAt,
			&m.UserID2, &m.Email, &m.Name, &m.AvatarURL, &m.EmailVerified, &m.IsAdmin, &m.CreatedAt, &m.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan member: %w", err)
		}
		members = append(members, &m)
	}
	return members, nil
}

// GetTeamMember retrieves a specific team member.
func (r *Repository) GetTeamMember(ctx context.Context, teamID, userID string) (*TeamMember, error) {
	var member TeamMember
	err := r.pool.QueryRow(ctx, `
		SELECT id, team_id, user_id, role, joined_at
		FROM team_members
		WHERE team_id = $1 AND user_id = $2
	`, teamID, userID).Scan(&member.ID, &member.TeamID, &member.UserID, &member.Role, &member.JoinedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrMemberNotFound
		}
		return nil, fmt.Errorf("query member: %w", err)
	}
	return &member, nil
}

func generateSlug(name string) string {
	slug := strings.ToLower(strings.TrimSpace(name))
	slug = strings.ReplaceAll(slug, " ", "-")
	slug = strings.Map(func(r rune) rune {
		switch {
		case r >= 'a' && r <= 'z':
			return r
		case r >= '0' && r <= '9':
			return r
		case r == '-':
			return r
		default:
			return -1
		}
	}, slug)
	for strings.Contains(slug, "--") {
		slug = strings.ReplaceAll(slug, "--", "-")
	}
	slug = strings.Trim(slug, "-")
	if slug == "" {
		slug = fmt.Sprintf("team-%d", time.Now().Unix())
	}
	return slug
}

func isDuplicateKeyError(err error) bool {
	return strings.Contains(err.Error(), "duplicate key")
}

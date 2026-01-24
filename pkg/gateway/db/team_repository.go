package db

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

// TeamMemberWithUser is a result type that combines TeamMember with User details
type TeamMemberWithUser struct {
	ID            string    `json:"id"`
	TeamID        string    `json:"team_id"`
	UserID        string    `json:"user_id"`
	Role          string    `json:"role"`
	JoinedAt      time.Time `json:"joined_at"`
	UserID2       string    `json:"user_id2"`
	Email         string    `json:"email"`
	Name          string    `json:"name"`
	AvatarURL     string    `json:"avatar_url"`
	EmailVerified bool      `json:"email_verified"`
	IsAdmin       bool      `json:"is_admin"`
	CreatedAt     time.Time `json:"created_at"`
	UpdatedAt     time.Time `json:"updated_at"`
}

var (
	ErrTeamNotFound      = errors.New("team not found")
	ErrTeamAlreadyExists = errors.New("team already exists")
	ErrMemberNotFound    = errors.New("team member not found")
	ErrAlreadyMember     = errors.New("user is already a team member")
)

// CreateTeam creates a new team
func (r *Repository) CreateTeam(ctx context.Context, team *Team) error {
	// Generate slug from name if not provided
	if team.Slug == "" {
		team.Slug = generateSlug(team.Name)
	}

	err := r.pool.QueryRow(ctx, `
		INSERT INTO teams (name, slug, owner_id)
		VALUES ($1, $2, $3)
		RETURNING id, created_at, updated_at
	`, team.Name, team.Slug, team.OwnerID).Scan(&team.ID, &team.CreatedAt, &team.UpdatedAt)

	if err != nil {
		if isDuplicateKeyError(err) {
			return ErrTeamAlreadyExists
		}
		return fmt.Errorf("insert team: %w", err)
	}

	return nil
}

// GetTeamByID retrieves a team by ID
func (r *Repository) GetTeamByID(ctx context.Context, id string) (*Team, error) {
	var team Team
	err := r.pool.QueryRow(ctx, `
		SELECT id, name, slug, owner_id, created_at, updated_at
		FROM teams
		WHERE id = $1
	`, id).Scan(&team.ID, &team.Name, &team.Slug, &team.OwnerID, &team.CreatedAt, &team.UpdatedAt)

	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrTeamNotFound
		}
		return nil, fmt.Errorf("query team: %w", err)
	}

	return &team, nil
}

// GetTeamBySlug retrieves a team by slug
func (r *Repository) GetTeamBySlug(ctx context.Context, slug string) (*Team, error) {
	var team Team
	err := r.pool.QueryRow(ctx, `
		SELECT id, name, slug, owner_id, created_at, updated_at
		FROM teams
		WHERE slug = $1
	`, slug).Scan(&team.ID, &team.Name, &team.Slug, &team.OwnerID, &team.CreatedAt, &team.UpdatedAt)

	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrTeamNotFound
		}
		return nil, fmt.Errorf("query team: %w", err)
	}

	return &team, nil
}

// UpdateTeam updates a team
func (r *Repository) UpdateTeam(ctx context.Context, team *Team) error {
	result, err := r.pool.Exec(ctx, `
		UPDATE teams SET name = $2, slug = $3, owner_id = $4 WHERE id = $1
	`, team.ID, team.Name, team.Slug, team.OwnerID)

	if err != nil {
		return fmt.Errorf("update team: %w", err)
	}

	if result.RowsAffected() == 0 {
		return ErrTeamNotFound
	}

	return nil
}

// DeleteTeam deletes a team
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

// GetTeamsByUserID retrieves all teams a user belongs to
func (r *Repository) GetTeamsByUserID(ctx context.Context, userID string) ([]*Team, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT t.id, t.name, t.slug, t.owner_id, t.created_at, t.updated_at
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
		if err := rows.Scan(&team.ID, &team.Name, &team.Slug, &team.OwnerID, &team.CreatedAt, &team.UpdatedAt); err != nil {
			return nil, fmt.Errorf("scan team: %w", err)
		}
		teams = append(teams, &team)
	}

	return teams, nil
}

// AddTeamMember adds a user to a team
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

// UpdateTeamMemberRole updates a team member's role
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

// RemoveTeamMember removes a user from a team
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

// GetTeamMembers retrieves all members of a team
func (r *Repository) GetTeamMembers(ctx context.Context, teamID string) ([]*TeamMemberWithUser, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT tm.id, tm.team_id, tm.user_id, tm.role, tm.joined_at,
		       u.id, u.email, u.name, u.avatar_url, u.email_verified, u.is_admin, u.created_at, u.updated_at
		FROM team_members tm
		INNER JOIN users u ON tm.user_id = u.id
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
			&m.UserID2, &m.Email, &m.Name, &m.AvatarURL,
			&m.EmailVerified, &m.IsAdmin, &m.CreatedAt, &m.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan member: %w", err)
		}

		members = append(members, &m)
	}

	return members, nil
}

// GetTeamMember retrieves a specific team member
func (r *Repository) GetTeamMember(ctx context.Context, teamID, userID string) (*TeamMember, error) {
	var m TeamMember
	err := r.pool.QueryRow(ctx, `
		SELECT id, team_id, user_id, role, joined_at
		FROM team_members
		WHERE team_id = $1 AND user_id = $2
	`, teamID, userID).Scan(&m.ID, &m.TeamID, &m.UserID, &m.Role, &m.JoinedAt)

	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrMemberNotFound
		}
		return nil, fmt.Errorf("query member: %w", err)
	}

	return &m, nil
}

// generateSlug generates a URL-friendly slug from a name
func generateSlug(name string) string {
	slug := strings.ToLower(name)
	slug = strings.ReplaceAll(slug, " ", "-")
	// Remove non-alphanumeric characters except hyphens
	var result strings.Builder
	for _, r := range slug {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '-' {
			result.WriteRune(r)
		}
	}
	return result.String()
}

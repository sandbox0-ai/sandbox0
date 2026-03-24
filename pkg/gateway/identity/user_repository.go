package identity

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// CreateUser creates a new user.
func (r *Repository) CreateUser(ctx context.Context, user *User) error {
	err := r.pool.QueryRow(ctx, `
		INSERT INTO users (email, name, avatar_url, password_hash, default_team_id, email_verified, is_admin)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		RETURNING id, created_at, updated_at
	`, user.Email, user.Name, user.AvatarURL, user.PasswordHash, user.DefaultTeamID,
		user.EmailVerified, user.IsAdmin,
	).Scan(&user.ID, &user.CreatedAt, &user.UpdatedAt)
	if err != nil {
		if isDuplicateKeyError(err) {
			return ErrUserAlreadyExists
		}
		return fmt.Errorf("insert user: %w", err)
	}
	return nil
}

// CreateUserWithDefaultTeam creates a user and a default team in one transaction.
func (r *Repository) CreateUserWithDefaultTeam(ctx context.Context, user *User, teamName string, homeRegionID *string) (*Team, *TeamMember, error) {
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, nil, fmt.Errorf("begin tx: %w", err)
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback(ctx)
		}
	}()

	err = tx.QueryRow(ctx, `
		INSERT INTO users (email, name, avatar_url, password_hash, default_team_id, email_verified, is_admin)
		VALUES ($1, $2, $3, $4, NULL, $5, $6)
		RETURNING id, created_at, updated_at
	`, user.Email, user.Name, user.AvatarURL, user.PasswordHash, user.EmailVerified, user.IsAdmin,
	).Scan(&user.ID, &user.CreatedAt, &user.UpdatedAt)
	if err != nil {
		if isDuplicateKeyError(err) {
			return nil, nil, ErrUserAlreadyExists
		}
		return nil, nil, fmt.Errorf("insert user: %w", err)
	}

	team := &Team{
		Name:         teamName,
		Slug:         fmt.Sprintf("user-%s", user.ID),
		OwnerID:      &user.ID,
		HomeRegionID: homeRegionID,
	}
	err = tx.QueryRow(ctx, `
		INSERT INTO teams (name, slug, owner_id, home_region_id)
		VALUES ($1, $2, $3, $4)
		RETURNING id, created_at, updated_at
	`, team.Name, team.Slug, team.OwnerID, team.HomeRegionID).Scan(&team.ID, &team.CreatedAt, &team.UpdatedAt)
	if err != nil {
		return nil, nil, fmt.Errorf("insert team: %w", err)
	}

	member := &TeamMember{
		TeamID: team.ID,
		UserID: user.ID,
		Role:   "admin",
	}
	err = tx.QueryRow(ctx, `
		INSERT INTO team_members (team_id, user_id, role)
		VALUES ($1, $2, $3)
		RETURNING id, joined_at
	`, member.TeamID, member.UserID, member.Role).Scan(&member.ID, &member.JoinedAt)
	if err != nil {
		return nil, nil, fmt.Errorf("insert team member: %w", err)
	}

	_, err = tx.Exec(ctx, `
		UPDATE users SET default_team_id = $2 WHERE id = $1
	`, user.ID, team.ID)
	if err != nil {
		return nil, nil, fmt.Errorf("update user default team: %w", err)
	}
	user.DefaultTeamID = &team.ID
	user.DefaultTeam = team

	if err = tx.Commit(ctx); err != nil {
		return nil, nil, fmt.Errorf("commit tx: %w", err)
	}
	return team, member, nil
}

// GetUserByID retrieves a user by ID.
func (r *Repository) GetUserByID(ctx context.Context, id string) (*User, error) {
	return r.scanUser(ctx, `
		SELECT u.id, u.email, u.name, u.avatar_url, u.password_hash, u.default_team_id,
		       u.email_verified, u.is_admin, u.created_at, u.updated_at,
		       dt.id, dt.name, dt.slug, dt.owner_id, dt.home_region_id, dt.created_at, dt.updated_at
		FROM users u
		LEFT JOIN teams dt ON dt.id = u.default_team_id
		WHERE u.id = $1
	`, id)
}

// GetUserByEmail retrieves a user by email.
func (r *Repository) GetUserByEmail(ctx context.Context, email string) (*User, error) {
	return r.scanUser(ctx, `
		SELECT u.id, u.email, u.name, u.avatar_url, u.password_hash, u.default_team_id,
		       u.email_verified, u.is_admin, u.created_at, u.updated_at,
		       dt.id, dt.name, dt.slug, dt.owner_id, dt.home_region_id, dt.created_at, dt.updated_at
		FROM users u
		LEFT JOIN teams dt ON dt.id = u.default_team_id
		WHERE u.email = $1
	`, email)
}

// UpdateUser updates a user.
func (r *Repository) UpdateUser(ctx context.Context, user *User) error {
	result, err := r.pool.Exec(ctx, `
		UPDATE users
		SET name = $2, avatar_url = $3, default_team_id = $4,
		    email_verified = $5, is_admin = $6
		WHERE id = $1
	`, user.ID, user.Name, user.AvatarURL, user.DefaultTeamID,
		user.EmailVerified, user.IsAdmin)
	if err != nil {
		return fmt.Errorf("update user: %w", err)
	}
	if result.RowsAffected() == 0 {
		return ErrUserNotFound
	}
	return nil
}

// UpdateUserPassword updates a user's password.
func (r *Repository) UpdateUserPassword(ctx context.Context, userID, passwordHash string) error {
	result, err := r.pool.Exec(ctx, `
		UPDATE users SET password_hash = $2 WHERE id = $1
	`, userID, passwordHash)
	if err != nil {
		return fmt.Errorf("update password: %w", err)
	}
	if result.RowsAffected() == 0 {
		return ErrUserNotFound
	}
	return nil
}

// DeleteUser deletes a user.
func (r *Repository) DeleteUser(ctx context.Context, id string) error {
	result, err := r.pool.Exec(ctx, `DELETE FROM users WHERE id = $1`, id)
	if err != nil {
		return fmt.Errorf("delete user: %w", err)
	}
	if result.RowsAffected() == 0 {
		return ErrUserNotFound
	}
	return nil
}

// CountUsers returns the total number of users.
func (r *Repository) CountUsers(ctx context.Context) (int64, error) {
	var count int64
	if err := r.pool.QueryRow(ctx, `SELECT COUNT(*) FROM users`).Scan(&count); err != nil {
		return 0, fmt.Errorf("count users: %w", err)
	}
	return count, nil
}

func (r *Repository) scanUser(ctx context.Context, query string, arg any) (*User, error) {
	var user User
	var defaultTeamID sql.NullString
	var defaultTeamName sql.NullString
	var defaultTeamSlug sql.NullString
	var defaultTeamOwnerID sql.NullString
	var defaultTeamHomeRegionID sql.NullString
	var defaultTeamCreatedAt sql.NullTime
	var defaultTeamUpdatedAt sql.NullTime

	err := r.pool.QueryRow(ctx, query, arg).Scan(
		&user.ID,
		&user.Email,
		&user.Name,
		&user.AvatarURL,
		&user.PasswordHash,
		&user.DefaultTeamID,
		&user.EmailVerified,
		&user.IsAdmin,
		&user.CreatedAt,
		&user.UpdatedAt,
		&defaultTeamID,
		&defaultTeamName,
		&defaultTeamSlug,
		&defaultTeamOwnerID,
		&defaultTeamHomeRegionID,
		&defaultTeamCreatedAt,
		&defaultTeamUpdatedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrUserNotFound
		}
		return nil, fmt.Errorf("query user: %w", err)
	}
	if defaultTeamID.Valid {
		user.DefaultTeam = &Team{
			ID:           defaultTeamID.String,
			Name:         defaultTeamName.String,
			Slug:         defaultTeamSlug.String,
			OwnerID:      nullStringPtr(defaultTeamOwnerID),
			HomeRegionID: nullStringPtr(defaultTeamHomeRegionID),
			CreatedAt:    nullTime(defaultTeamCreatedAt),
			UpdatedAt:    nullTime(defaultTeamUpdatedAt),
		}
	}
	return &user, nil
}

func nullStringPtr(value sql.NullString) *string {
	if !value.Valid {
		return nil
	}
	v := value.String
	return &v
}

func nullTime(value sql.NullTime) time.Time {
	if !value.Valid {
		return time.Time{}
	}
	return value.Time
}

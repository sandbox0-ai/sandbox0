package identity

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

// CreateTeamWithOwner creates the team and its admin owner membership in one
// transaction so no externally visible team can be missing its owner grant.
func (r *Repository) CreateTeamWithOwner(ctx context.Context, team *Team) (*TeamMember, error) {
	if team == nil || team.OwnerID == nil || strings.TrimSpace(*team.OwnerID) == "" {
		return nil, fmt.Errorf("team owner is required")
	}
	if team.Slug == "" {
		team.Slug = generateSlug(team.Name)
	}
	if err := validateTeamForPersistence(team); err != nil {
		return nil, err
	}

	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, fmt.Errorf("begin create team with owner: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	ownerID := strings.TrimSpace(*team.OwnerID)
	if err := lockIdentityScopesTx(ctx, tx, identityUserScope(ownerID)); err != nil {
		return nil, err
	}
	if limits, guarded := r.identityResourceLimits(); guarded {
		if err := ensureTeamsOwnedLimitTx(ctx, tx, ownerID, limits.MaxTeamsOwnedPerUser); err != nil {
			return nil, err
		}
		if err := ensureUserMembershipLimitTx(
			ctx,
			tx,
			ownerID,
			limits.MaxTeamMembershipsPerUser,
		); err != nil {
			return nil, err
		}
	}
	if err := r.insertTeam(ctx, tx, team); err != nil {
		return nil, err
	}

	member := &TeamMember{
		TeamID: team.ID,
		UserID: ownerID,
		Role:   "admin",
	}
	if err := insertTeamMemberTx(ctx, tx, member); err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit create team with owner: %w", err)
	}
	return member, nil
}

type teamInsertQuerier interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}

func (r *Repository) insertTeam(ctx context.Context, q teamInsertQuerier, team *Team) error {
	if err := validateTeamForPersistence(team); err != nil {
		return err
	}
	err := q.QueryRow(ctx, `
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

func ensureTeamsOwnedLimitTx(ctx context.Context, tx pgx.Tx, userID string, limit int64) error {
	var count int64
	if err := tx.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM teams
		WHERE owner_id = $1
	`, userID).Scan(&count); err != nil {
		return fmt.Errorf("count teams owned by user: %w", err)
	}
	if count >= limit {
		return &IdentityResourceLimitExceededError{
			Scope:    "user",
			ScopeID:  userID,
			Resource: IdentityLimitResourceTeamsOwned,
			Limit:    limit,
		}
	}
	return nil
}

func ensureUserMembershipLimitTx(ctx context.Context, tx pgx.Tx, userID string, limit int64) error {
	var count int64
	if err := tx.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM team_members
		WHERE user_id = $1
	`, userID).Scan(&count); err != nil {
		return fmt.Errorf("count user team memberships: %w", err)
	}
	if count >= limit {
		return &IdentityResourceLimitExceededError{
			Scope:    "user",
			ScopeID:  userID,
			Resource: IdentityLimitResourceTeamMemberships,
			Limit:    limit,
		}
	}
	return nil
}

func ensureTeamMemberLimitTx(ctx context.Context, tx pgx.Tx, teamID string, limit int64) error {
	var count int64
	if err := tx.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM team_members
		WHERE team_id = $1
	`, teamID).Scan(&count); err != nil {
		return fmt.Errorf("count team members: %w", err)
	}
	if count >= limit {
		return &IdentityResourceLimitExceededError{
			Scope:    "team",
			ScopeID:  teamID,
			Resource: IdentityLimitResourceTeamMembers,
			Limit:    limit,
		}
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
	if err := validateTeamForPersistence(team); err != nil {
		return err
	}
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin update team: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := lockIdentityScopesTx(ctx, tx, identityTeamScope(team.ID)); err != nil {
		return err
	}
	err = tx.QueryRow(ctx, `
		UPDATE teams
		SET name = $2, slug = $3, home_region_id = $4
		WHERE id = $1
		RETURNING updated_at
	`, team.ID, team.Name, team.Slug, team.HomeRegionID).Scan(&team.UpdatedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrTeamNotFound
		}
		if isDuplicateKeyError(err) {
			return ErrTeamAlreadyExists
		}
		return fmt.Errorf("update team: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit update team: %w", err)
	}
	return nil
}

// DeleteTeamOwnedBy serializes deletion with every identity mutation for the
// team and deletes only while expectedOwnerID is still the current owner.
func (r *Repository) DeleteTeamOwnedBy(
	ctx context.Context,
	id string,
	expectedOwnerID string,
) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin owned team deletion: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if err := lockIdentityScopesTx(ctx, tx, identityTeamScope(id)); err != nil {
		return err
	}

	var (
		currentOwnerID   *string
		deletionFencedAt *time.Time
	)
	if err := tx.QueryRow(ctx, `
		SELECT owner_id::text, deletion_fenced_at
		FROM teams
		WHERE id = $1
		FOR UPDATE
	`, id).Scan(&currentOwnerID, &deletionFencedAt); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrTeamNotFound
		}
		return fmt.Errorf("lock team for deletion: %w", err)
	}
	if currentOwnerID == nil || *currentOwnerID != expectedOwnerID {
		return ErrTeamOwnerChanged
	}
	if deletionFencedAt == nil {
		return ErrTeamDeletionNotFenced
	}

	result, err := tx.Exec(ctx, `
		DELETE FROM teams
		WHERE id = $1 AND owner_id = $2 AND deletion_fenced_at IS NOT NULL
	`, id, expectedOwnerID)
	if err != nil {
		return fmt.Errorf("delete owned team: %w", err)
	}
	if result.RowsAffected() != 1 {
		return ErrTeamOwnerChanged
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit owned team deletion: %w", err)
	}
	return nil
}

// FenceTeamDeletionOwnedBy linearizes the identity deletion decision before
// the quota tombstone is created. Owner transfer takes the same team lock and
// refuses a fenced team, so either transfer commits before this owner check or
// deletion prevents all later transfers.
func (r *Repository) FenceTeamDeletionOwnedBy(
	ctx context.Context,
	id string,
	expectedOwnerID string,
) error {
	return r.setTeamDeletionFence(ctx, id, expectedOwnerID, true)
}

// UnfenceTeamDeletionOwnedBy clears a fence only for a failure known to have
// happened before the durable quota tombstone transaction committed.
func (r *Repository) UnfenceTeamDeletionOwnedBy(
	ctx context.Context,
	id string,
	expectedOwnerID string,
) error {
	return r.setTeamDeletionFence(ctx, id, expectedOwnerID, false)
}

func (r *Repository) setTeamDeletionFence(
	ctx context.Context,
	id string,
	expectedOwnerID string,
	fenced bool,
) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin team deletion fence update: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := lockIdentityScopesTx(ctx, tx, identityTeamScope(id)); err != nil {
		return err
	}

	var currentOwnerID *string
	if err := tx.QueryRow(ctx, `
		SELECT owner_id::text
		FROM teams
		WHERE id = $1
		FOR UPDATE
	`, id).Scan(&currentOwnerID); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrTeamNotFound
		}
		return fmt.Errorf("lock team for deletion fence update: %w", err)
	}
	if currentOwnerID == nil || *currentOwnerID != expectedOwnerID {
		return ErrTeamOwnerChanged
	}

	if _, err := tx.Exec(ctx, `
		UPDATE teams
		SET deletion_fenced_at = CASE
			WHEN $2::boolean THEN COALESCE(deletion_fenced_at, NOW())
			ELSE NULL
		END
		WHERE id = $1
	`, id, fenced); err != nil {
		return fmt.Errorf("update team deletion fence: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit team deletion fence update: %w", err)
	}
	return nil
}

// TransferTeamOwner sets a team member as the team owner and ensures they have admin role.
func (r *Repository) TransferTeamOwner(
	ctx context.Context,
	teamID string,
	expectedOwnerID string,
	userID string,
) (*Team, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin transfer team owner: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if err := lockIdentityScopesTx(
		ctx,
		tx,
		identityTeamScope(teamID),
		identityUserScope(userID),
	); err != nil {
		return nil, err
	}

	var (
		currentOwnerID   *string
		deletionFencedAt *time.Time
	)
	if err := tx.QueryRow(ctx, `
		SELECT owner_id::text, deletion_fenced_at
		FROM teams
		WHERE id = $1
		FOR UPDATE
	`, teamID).Scan(&currentOwnerID, &deletionFencedAt); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrTeamNotFound
		}
		return nil, fmt.Errorf("lock team owner: %w", err)
	}
	if currentOwnerID == nil || *currentOwnerID != expectedOwnerID {
		return nil, ErrTeamOwnerChanged
	}
	if deletionFencedAt != nil {
		return nil, ErrTeamDeletionInProgress
	}

	var memberID string
	if err := tx.QueryRow(ctx, `
		SELECT id
		FROM team_members
		WHERE team_id = $1 AND user_id = $2
		FOR UPDATE
	`, teamID, userID).Scan(&memberID); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrMemberNotFound
		}
		return nil, fmt.Errorf("query owner member: %w", err)
	}

	if (currentOwnerID == nil || *currentOwnerID != userID) && r.identityResourceGuard != nil {
		if err := ensureTeamsOwnedLimitTx(
			ctx,
			tx,
			userID,
			r.identityResourceGuard.MaxTeamsOwnedPerUser,
		); err != nil {
			return nil, err
		}
	}

	if _, err := tx.Exec(ctx, `
		UPDATE team_members
		SET role = 'admin'
		WHERE team_id = $1 AND user_id = $2
	`, teamID, userID); err != nil {
		return nil, fmt.Errorf("promote owner member: %w", err)
	}

	var team Team
	err = tx.QueryRow(ctx, `
		UPDATE teams
		SET owner_id = $2
		WHERE id = $1
		RETURNING id, name, slug, owner_id, home_region_id, created_at, updated_at
	`, teamID, userID).Scan(
		&team.ID,
		&team.Name,
		&team.Slug,
		&team.OwnerID,
		&team.HomeRegionID,
		&team.CreatedAt,
		&team.UpdatedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrTeamNotFound
		}
		return nil, fmt.Errorf("update team owner: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit transfer team owner: %w", err)
	}
	return &team, nil
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
	if err := validateTeamMemberForPersistence(member); err != nil {
		return err
	}
	limits, guarded := r.identityResourceLimits()
	if !guarded {
		return insertTeamMemberTx(ctx, r.pool, member)
	}

	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin add team member: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if err := lockIdentityScopesTx(
		ctx,
		tx,
		identityTeamScope(member.TeamID),
		identityUserScope(member.UserID),
	); err != nil {
		return err
	}
	if err := ensureTeamMemberLimitTx(ctx, tx, member.TeamID, limits.MaxMembersPerTeam); err != nil {
		return err
	}
	if err := ensureUserMembershipLimitTx(
		ctx,
		tx,
		member.UserID,
		limits.MaxTeamMembershipsPerUser,
	); err != nil {
		return err
	}
	if err := insertTeamMemberTx(ctx, tx, member); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit add team member: %w", err)
	}
	return nil
}

func insertTeamMemberTx(ctx context.Context, q teamInsertQuerier, member *TeamMember) error {
	if err := validateTeamMemberForPersistence(member); err != nil {
		return err
	}
	err := q.QueryRow(ctx, `
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
	if err := validateIdentityFieldSize(
		"team_member_role",
		role,
		MaxIdentityTeamRoleBytes,
	); err != nil {
		return err
	}
	if r.identityResourceGuard == nil {
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

	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin update member role: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := lockIdentityScopesTx(
		ctx,
		tx,
		identityTeamScope(teamID),
		identityUserScope(userID),
	); err != nil {
		return err
	}

	var currentRole string
	var ownerID *string
	if err := tx.QueryRow(ctx, `
		SELECT tm.role, t.owner_id::text
		FROM team_members tm
		INNER JOIN teams t ON t.id = tm.team_id
		WHERE tm.team_id = $1 AND tm.user_id = $2
		FOR UPDATE OF tm, t
	`, teamID, userID).Scan(&currentRole, &ownerID); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrMemberNotFound
		}
		return fmt.Errorf("lock member role: %w", err)
	}
	if role != "admin" && ownerID != nil && *ownerID == userID {
		return ErrCannotDemoteTeamOwner
	}
	if role != "admin" && currentRole == "admin" {
		var adminCount int64
		if err := tx.QueryRow(ctx, `
			SELECT COUNT(*)
			FROM team_members
			WHERE team_id = $1 AND role = 'admin'
		`, teamID).Scan(&adminCount); err != nil {
			return fmt.Errorf("count team admins: %w", err)
		}
		if adminCount <= 1 {
			return ErrCannotRemoveLastTeamAdmin
		}
	}
	if _, err := tx.Exec(ctx, `
		UPDATE team_members
		SET role = $3
		WHERE team_id = $1 AND user_id = $2
	`, teamID, userID, role); err != nil {
		return fmt.Errorf("update member role: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit update member role: %w", err)
	}
	return nil
}

// RemoveTeamMember removes a user from a team.
func (r *Repository) RemoveTeamMember(ctx context.Context, teamID, userID string) error {
	if r.identityResourceGuard == nil {
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

	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin remove team member: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := lockIdentityScopesTx(
		ctx,
		tx,
		identityTeamScope(teamID),
		identityUserScope(userID),
	); err != nil {
		return err
	}

	var role string
	var ownerID *string
	if err := tx.QueryRow(ctx, `
		SELECT tm.role, t.owner_id::text
		FROM team_members tm
		INNER JOIN teams t ON t.id = tm.team_id
		WHERE tm.team_id = $1 AND tm.user_id = $2
		FOR UPDATE OF tm, t
	`, teamID, userID).Scan(&role, &ownerID); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrMemberNotFound
		}
		return fmt.Errorf("lock removed team member: %w", err)
	}
	if ownerID != nil && *ownerID == userID {
		return ErrCannotRemoveTeamOwner
	}
	if role == "admin" {
		var adminCount int64
		if err := tx.QueryRow(ctx, `
			SELECT COUNT(*)
			FROM team_members
			WHERE team_id = $1 AND role = 'admin'
		`, teamID).Scan(&adminCount); err != nil {
			return fmt.Errorf("count team admins: %w", err)
		}
		if adminCount <= 1 {
			return ErrCannotRemoveLastTeamAdmin
		}
	}
	if _, err := tx.Exec(ctx, `
		DELETE FROM team_members
		WHERE team_id = $1 AND user_id = $2
	`, teamID, userID); err != nil {
		return fmt.Errorf("remove member: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit remove team member: %w", err)
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
	return scanTeamMembers(rows)
}

// SearchTeamMembers retrieves members whose profile fields match query.
func (r *Repository) SearchTeamMembers(ctx context.Context, teamID, query string) ([]*TeamMemberWithUser, error) {
	query = strings.TrimSpace(query)
	if query == "" {
		return r.GetTeamMembers(ctx, teamID)
	}
	if err := validateIdentityFieldSize(
		"team_member_search",
		query,
		MaxIdentityMemberSearchBytes,
	); err != nil {
		return nil, err
	}

	rows, err := r.pool.Query(ctx, `
		SELECT tm.id, tm.team_id, tm.user_id, tm.role, tm.joined_at,
		       u.id, u.email, u.name, u.avatar_url, u.email_verified, u.is_admin, u.created_at, u.updated_at
		FROM team_members tm
		INNER JOIN users u ON u.id = tm.user_id
		WHERE tm.team_id = $1
		  AND (
		    u.email ILIKE $2 ESCAPE '\'
		    OR u.name ILIKE $2 ESCAPE '\'
		    OR tm.user_id::text ILIKE $2 ESCAPE '\'
		  )
		ORDER BY tm.joined_at
	`, teamID, likeContainsPattern(query))
	if err != nil {
		return nil, fmt.Errorf("search team members: %w", err)
	}
	return scanTeamMembers(rows)
}

func scanTeamMembers(rows pgx.Rows) ([]*TeamMemberWithUser, error) {
	defer rows.Close()
	members := make([]*TeamMemberWithUser, 0)
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
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate team members: %w", err)
	}
	return members, nil
}

func likeContainsPattern(value string) string {
	replacer := strings.NewReplacer(`\`, `\\`, `%`, `\%`, `_`, `\_`)
	return "%" + replacer.Replace(value) + "%"
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

package identity

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func TestIdentityResourceGuardConcurrentCreateTeamWithOwner(t *testing.T) {
	pool, _ := newGatewayIdentityTestPool(t)
	if pool == nil {
		return
	}

	ctx := context.Background()
	repo := NewRepository(pool, WithIdentityResourceGuard(IdentityResourceGuardLimits{
		MaxTeamsOwnedPerUser:      2,
		MaxTeamMembershipsPerUser: 8,
	}))
	owner := &User{Email: "guard-owner@example.com", Name: "Guard Owner"}
	if err := repo.CreateUser(ctx, owner); err != nil {
		t.Fatalf("create owner: %v", err)
	}

	const attempts = 12
	start := make(chan struct{})
	results := make(chan error, attempts)
	var workers sync.WaitGroup
	for i := 0; i < attempts; i++ {
		workers.Add(1)
		go func(index int) {
			defer workers.Done()
			<-start
			ownerID := owner.ID
			_, err := repo.CreateTeamWithOwner(ctx, &Team{
				Name:    fmt.Sprintf("Concurrent Team %02d", index),
				Slug:    fmt.Sprintf("concurrent-team-%02d", index),
				OwnerID: &ownerID,
			})
			results <- err
		}(i)
	}
	close(start)
	workers.Wait()
	close(results)

	var created, rejected int
	for err := range results {
		switch {
		case err == nil:
			created++
		case IsIdentityResourceLimitExceeded(err):
			var limitErr *IdentityResourceLimitExceededError
			if !errors.As(err, &limitErr) {
				t.Fatalf("limit error has unexpected type: %v", err)
			}
			if limitErr.Resource != IdentityLimitResourceTeamsOwned || limitErr.Limit != 2 {
				t.Errorf("limit error = %+v, want teams_owned limit 2", limitErr)
			}
			rejected++
		default:
			t.Errorf("create team returned unexpected error: %v", err)
		}
	}
	if created != 2 || rejected != attempts-2 {
		t.Fatalf("created/rejected = %d/%d, want 2/%d", created, rejected, attempts-2)
	}

	assertIdentityQueryCount(t, pool, `
		SELECT COUNT(*)
		FROM teams
		WHERE owner_id = $1
	`, 2, owner.ID)
	assertIdentityQueryCount(t, pool, `
		SELECT COUNT(*)
		FROM teams team
		LEFT JOIN team_members member
		  ON member.team_id = team.id
		 AND member.user_id = team.owner_id
		 AND member.role = 'admin'
		WHERE team.owner_id = $1
		  AND member.id IS NULL
	`, 0, owner.ID)
}

func TestIdentityResourceGuardConcurrentTeamMembershipCapsAndOwnerInvariants(t *testing.T) {
	pool, _ := newGatewayIdentityTestPool(t)
	if pool == nil {
		return
	}

	ctx := context.Background()
	repo := NewRepository(pool, WithIdentityResourceGuard(IdentityResourceGuardLimits{
		MaxTeamsOwnedPerUser:      16,
		MaxMembersPerTeam:         3,
		MaxTeamMembershipsPerUser: 2,
	}))
	owner := createIdentityGuardTestUser(t, repo, "member-cap-owner@example.com")
	ownerID := owner.ID
	team := &Team{Name: "Member Cap Team", Slug: "member-cap-team", OwnerID: &ownerID}
	if _, err := repo.CreateTeamWithOwner(ctx, team); err != nil {
		t.Fatalf("create member-cap team: %v", err)
	}

	const candidates = 8
	start := make(chan struct{})
	results := make(chan error, candidates)
	var workers sync.WaitGroup
	for i := 0; i < candidates; i++ {
		user := createIdentityGuardTestUser(
			t,
			repo,
			fmt.Sprintf("member-cap-candidate-%02d@example.com", i),
		)
		workers.Add(1)
		go func(userID string) {
			defer workers.Done()
			<-start
			results <- repo.AddTeamMember(ctx, &TeamMember{
				TeamID: team.ID,
				UserID: userID,
				Role:   "viewer",
			})
		}(user.ID)
	}
	close(start)
	workers.Wait()
	close(results)

	var added, rejected int
	for err := range results {
		switch {
		case err == nil:
			added++
		case IsIdentityResourceLimitExceeded(err):
			var limitErr *IdentityResourceLimitExceededError
			if !errors.As(err, &limitErr) {
				t.Fatalf("limit error has unexpected type: %v", err)
			}
			if limitErr.Resource != IdentityLimitResourceTeamMembers || limitErr.Limit != 3 {
				t.Errorf("limit error = %+v, want team_members limit 3", limitErr)
			}
			rejected++
		default:
			t.Errorf("add member returned unexpected error: %v", err)
		}
	}
	if added != 2 || rejected != candidates-2 {
		t.Fatalf("added/rejected = %d/%d, want 2/%d", added, rejected, candidates-2)
	}
	assertIdentityQueryCount(t, pool, `
		SELECT COUNT(*)
		FROM team_members
		WHERE team_id = $1
	`, 3, team.ID)

	if err := repo.RemoveTeamMember(ctx, team.ID, owner.ID); !errors.Is(err, ErrCannotRemoveTeamOwner) {
		t.Fatalf("remove owner error = %v, want %v", err, ErrCannotRemoveTeamOwner)
	}
	if err := repo.UpdateTeamMemberRole(ctx, team.ID, owner.ID, "viewer"); !errors.Is(err, ErrCannotDemoteTeamOwner) {
		t.Fatalf("demote owner error = %v, want %v", err, ErrCannotDemoteTeamOwner)
	}

	member := createIdentityGuardTestUser(t, repo, "membership-cap-user@example.com")
	const teamAttempts = 7
	teams := make([]*Team, 0, teamAttempts)
	for i := 0; i < teamAttempts; i++ {
		teamOwner := createIdentityGuardTestUser(
			t,
			repo,
			fmt.Sprintf("membership-team-owner-%02d@example.com", i),
		)
		teamOwnerID := teamOwner.ID
		candidateTeam := &Team{
			Name:    fmt.Sprintf("Membership Team %02d", i),
			Slug:    fmt.Sprintf("membership-team-%02d", i),
			OwnerID: &teamOwnerID,
		}
		if _, err := repo.CreateTeamWithOwner(ctx, candidateTeam); err != nil {
			t.Fatalf("create membership team %d: %v", i, err)
		}
		teams = append(teams, candidateTeam)
	}

	start = make(chan struct{})
	results = make(chan error, teamAttempts)
	workers = sync.WaitGroup{}
	for _, candidateTeam := range teams {
		workers.Add(1)
		go func(teamID string) {
			defer workers.Done()
			<-start
			results <- repo.AddTeamMember(ctx, &TeamMember{
				TeamID: teamID,
				UserID: member.ID,
				Role:   "viewer",
			})
		}(candidateTeam.ID)
	}
	close(start)
	workers.Wait()
	close(results)

	added, rejected = 0, 0
	for err := range results {
		switch {
		case err == nil:
			added++
		case IsIdentityResourceLimitExceeded(err):
			var limitErr *IdentityResourceLimitExceededError
			if !errors.As(err, &limitErr) {
				t.Fatalf("limit error has unexpected type: %v", err)
			}
			if limitErr.Resource != IdentityLimitResourceTeamMemberships || limitErr.Limit != 2 {
				t.Errorf("limit error = %+v, want team_memberships limit 2", limitErr)
			}
			rejected++
		default:
			t.Errorf("add cross-team member returned unexpected error: %v", err)
		}
	}
	if added != 2 || rejected != teamAttempts-2 {
		t.Fatalf("cross-team added/rejected = %d/%d, want 2/%d", added, rejected, teamAttempts-2)
	}
	assertIdentityQueryCount(t, pool, `
		SELECT COUNT(*)
		FROM team_members
		WHERE user_id = $1
	`, 2, member.ID)
}

func TestIdentityResourceGuardTransferOwnerHonorsOwnedTeamCap(t *testing.T) {
	pool, _ := newGatewayIdentityTestPool(t)
	if pool == nil {
		return
	}

	ctx := context.Background()
	repo := NewRepository(pool, WithIdentityResourceGuard(IdentityResourceGuardLimits{
		MaxTeamsOwnedPerUser:      1,
		MaxMembersPerTeam:         8,
		MaxTeamMembershipsPerUser: 8,
	}))
	currentOwner := createIdentityGuardTestUser(t, repo, "transfer-current@example.com")
	targetOwner := createIdentityGuardTestUser(t, repo, "transfer-target@example.com")

	currentOwnerID := currentOwner.ID
	sourceTeam := &Team{Name: "Transfer Source", Slug: "transfer-source", OwnerID: &currentOwnerID}
	if _, err := repo.CreateTeamWithOwner(ctx, sourceTeam); err != nil {
		t.Fatalf("create source team: %v", err)
	}
	targetOwnerID := targetOwner.ID
	targetTeam := &Team{Name: "Already Owned", Slug: "already-owned", OwnerID: &targetOwnerID}
	if _, err := repo.CreateTeamWithOwner(ctx, targetTeam); err != nil {
		t.Fatalf("create target-owned team: %v", err)
	}
	if err := repo.AddTeamMember(ctx, &TeamMember{
		TeamID: sourceTeam.ID,
		UserID: targetOwner.ID,
		Role:   "viewer",
	}); err != nil {
		t.Fatalf("add target owner as source member: %v", err)
	}

	_, err := repo.TransferTeamOwner(ctx, sourceTeam.ID, currentOwner.ID, targetOwner.ID)
	if !IsIdentityResourceLimitExceeded(err) {
		t.Fatalf("transfer owner error = %v, want identity limit", err)
	}
	var limitErr *IdentityResourceLimitExceededError
	if !errors.As(err, &limitErr) {
		t.Fatalf("transfer limit error has unexpected type: %v", err)
	}
	if limitErr.Resource != IdentityLimitResourceTeamsOwned || limitErr.Limit != 1 {
		t.Fatalf("transfer limit error = %+v, want teams_owned limit 1", limitErr)
	}

	reloaded, err := repo.GetTeamByID(ctx, sourceTeam.ID)
	if err != nil {
		t.Fatalf("reload source team: %v", err)
	}
	if reloaded.OwnerID == nil || *reloaded.OwnerID != currentOwner.ID {
		t.Fatalf("source owner = %v, want %s", reloaded.OwnerID, currentOwner.ID)
	}
	member, err := repo.GetTeamMember(ctx, sourceTeam.ID, targetOwner.ID)
	if err != nil {
		t.Fatalf("reload target member: %v", err)
	}
	if member.Role != "viewer" {
		t.Fatalf("target member role = %q, want viewer after rejected transfer", member.Role)
	}
}

func TestIdentityResourceGuardRefreshTokenEvictionAndSingleUseRotation(t *testing.T) {
	pool, _ := newGatewayIdentityTestPool(t)
	if pool == nil {
		return
	}

	ctx := context.Background()
	repo := NewRepository(pool, WithIdentityResourceGuard(IdentityResourceGuardLimits{
		MaxActiveRefreshTokensPerUser: 2,
	}))
	user := createIdentityGuardTestUser(t, repo, "refresh-guard-user@example.com")
	for i := 0; i < 3; i++ {
		if err := repo.CreateRefreshToken(ctx, &RefreshToken{
			UserID:    user.ID,
			TokenHash: fmt.Sprintf("refresh-token-%d", i),
			ExpiresAt: time.Now().Add(time.Hour),
		}); err != nil {
			t.Fatalf("create refresh token %d: %v", i, err)
		}
		time.Sleep(2 * time.Millisecond)
	}

	assertIdentityQueryCount(t, pool, `
		SELECT COUNT(*)
		FROM refresh_tokens
		WHERE user_id = $1
		  AND revoked = false
		  AND expires_at > NOW()
	`, 2, user.ID)
	if _, err := repo.ValidateRefreshToken(ctx, "refresh-token-0"); !errors.Is(err, ErrTokenNotFound) {
		t.Fatalf("validate evicted token error = %v, want %v", err, ErrTokenNotFound)
	}

	const rotations = 12
	type rotationResult struct {
		hash string
		err  error
	}
	start := make(chan struct{})
	results := make(chan rotationResult, rotations)
	var workers sync.WaitGroup
	for i := 0; i < rotations; i++ {
		workers.Add(1)
		go func(index int) {
			defer workers.Done()
			<-start
			hash := fmt.Sprintf("rotated-refresh-token-%02d", index)
			err := repo.RotateRefreshToken(ctx, "refresh-token-2", &RefreshToken{
				UserID:    user.ID,
				TokenHash: hash,
				ExpiresAt: time.Now().Add(time.Hour),
			})
			results <- rotationResult{hash: hash, err: err}
		}(i)
	}
	close(start)
	workers.Wait()
	close(results)

	var winner string
	var succeeded, rejected int
	for result := range results {
		switch {
		case result.err == nil:
			winner = result.hash
			succeeded++
		case errors.Is(result.err, ErrTokenNotFound):
			rejected++
		default:
			t.Errorf("rotate token returned unexpected error: %v", result.err)
		}
	}
	if succeeded != 1 || rejected != rotations-1 {
		t.Fatalf("rotation succeeded/rejected = %d/%d, want 1/%d", succeeded, rejected, rotations-1)
	}
	if _, err := repo.ValidateRefreshToken(ctx, "refresh-token-2"); !errors.Is(err, ErrTokenNotFound) {
		t.Fatalf("validate consumed token error = %v, want %v", err, ErrTokenNotFound)
	}
	if _, err := repo.ValidateRefreshToken(ctx, winner); err != nil {
		t.Fatalf("validate winning replacement: %v", err)
	}
	assertIdentityQueryCount(t, pool, `
		SELECT COUNT(*)
		FROM refresh_tokens
		WHERE user_id = $1
		  AND revoked = false
		  AND expires_at > NOW()
	`, 2, user.ID)
}

func TestIdentityResourceGuardConcurrentWebLoginCodeCap(t *testing.T) {
	pool, _ := newGatewayIdentityTestPool(t)
	if pool == nil {
		return
	}

	ctx := context.Background()
	repo := NewRepository(pool, WithIdentityResourceGuard(IdentityResourceGuardLimits{
		MaxActiveWebLoginCodesPerUser: 2,
	}))
	user := createIdentityGuardTestUser(t, repo, "web-code-guard-user@example.com")

	const attempts = 12
	start := make(chan struct{})
	results := make(chan error, attempts)
	var workers sync.WaitGroup
	for i := 0; i < attempts; i++ {
		workers.Add(1)
		go func(index int) {
			defer workers.Done()
			<-start
			results <- repo.CreateWebLoginCode(ctx, &WebLoginCode{
				CodeHash:  fmt.Sprintf("web-login-code-%02d", index),
				UserID:    user.ID,
				ReturnURL: "https://dashboard.example.test/callback",
				ExpiresAt: time.Now().Add(time.Minute),
			})
		}(i)
	}
	close(start)
	workers.Wait()
	close(results)

	for err := range results {
		if err != nil {
			t.Errorf("create web login code: %v", err)
		}
	}
	assertIdentityQueryCount(t, pool, `
		SELECT COUNT(*)
		FROM web_login_codes
		WHERE user_id = $1
		  AND consumed_at IS NULL
		  AND expires_at > NOW()
	`, 2, user.ID)
}

func TestIdentityResourceGuardConcurrentLinkedIdentityCap(t *testing.T) {
	pool, _ := newGatewayIdentityTestPool(t)
	if pool == nil {
		return
	}

	ctx := context.Background()
	repo := NewRepository(pool, WithIdentityResourceGuard(IdentityResourceGuardLimits{
		MaxLinkedIdentitiesPerUser: 2,
	}))
	user := createIdentityGuardTestUser(t, repo, "linked-identity-guard@example.com")

	const attempts = 10
	start := make(chan struct{})
	results := make(chan error, attempts)
	var workers sync.WaitGroup
	for i := 0; i < attempts; i++ {
		workers.Add(1)
		go func(index int) {
			defer workers.Done()
			<-start
			results <- repo.CreateUserIdentity(ctx, &UserIdentity{
				UserID:   user.ID,
				Provider: fmt.Sprintf("provider-%02d", index),
				Subject:  fmt.Sprintf("subject-%02d", index),
			})
		}(i)
	}
	close(start)
	workers.Wait()
	close(results)

	var created, rejected int
	for err := range results {
		switch {
		case err == nil:
			created++
		case IsIdentityResourceLimitExceeded(err):
			rejected++
		default:
			t.Errorf("create linked identity: %v", err)
		}
	}
	if created != 2 || rejected != attempts-2 {
		t.Fatalf("linked identities created/rejected = %d/%d, want 2/%d", created, rejected, attempts-2)
	}
	assertIdentityQueryCount(t, pool, `
		SELECT COUNT(*)
		FROM user_identities
		WHERE user_id = $1
	`, 2, user.ID)
}

func TestIdentityResourceGuardConcurrentBatchCleanup(t *testing.T) {
	pool, _ := newGatewayIdentityTestPool(t)
	if pool == nil {
		return
	}

	ctx := context.Background()
	repo := NewRepository(pool)
	user := createIdentityGuardTestUser(t, repo, "cleanup-guard-user@example.com")

	const expiredRows = 17
	if _, err := pool.Exec(ctx, `
		INSERT INTO refresh_tokens (user_id, token_hash, expires_at)
		SELECT $1, 'cleanup-expired-refresh-' || value, NOW() - INTERVAL '1 minute'
		FROM generate_series(1, $2) AS value
	`, user.ID, expiredRows); err != nil {
		t.Fatalf("seed expired refresh tokens: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO refresh_tokens (user_id, token_hash, expires_at)
		VALUES ($1, 'cleanup-live-refresh', NOW() + INTERVAL '1 hour')
	`, user.ID); err != nil {
		t.Fatalf("seed live refresh token: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO web_login_codes (code_hash, user_id, return_url, expires_at)
		SELECT
			'cleanup-expired-web-' || value,
			$1,
			'https://dashboard.example.test/callback',
			NOW() - INTERVAL '1 minute'
		FROM generate_series(1, $2) AS value
	`, user.ID, expiredRows); err != nil {
		t.Fatalf("seed expired web login codes: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO web_login_codes (code_hash, user_id, return_url, expires_at)
		VALUES (
			'cleanup-live-web',
			$1,
			'https://dashboard.example.test/callback',
			NOW() + INTERVAL '1 hour'
		)
	`, user.ID); err != nil {
		t.Fatalf("seed live web login code: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO device_auth_sessions (
			provider,
			device_code,
			user_code,
			verification_uri,
			interval_seconds,
			expires_at
		)
		SELECT
			'test',
			'cleanup-expired-device-' || value,
			'EXPIRED-' || value,
			'https://idp.example.test/device',
			5,
			NOW() - INTERVAL '1 minute'
		FROM generate_series(1, $1) AS value
	`, expiredRows); err != nil {
		t.Fatalf("seed expired device auth sessions: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO device_auth_sessions (
			provider,
			device_code,
			user_code,
			verification_uri,
			interval_seconds,
			expires_at
		)
		VALUES (
			'test',
			'cleanup-live-device',
			'LIVE',
			'https://idp.example.test/device',
			5,
			NOW() + INTERVAL '1 hour'
		)
	`); err != nil {
		t.Fatalf("seed live device auth session: %v", err)
	}

	runConcurrentIdentityCleanup(t, "refresh tokens", expiredRows, repo.CleanupExpiredTokensBatch)
	runConcurrentIdentityCleanup(t, "web login codes", expiredRows, repo.CleanupExpiredWebLoginCodesBatch)
	runConcurrentIdentityCleanup(t, "device auth sessions", expiredRows, repo.CleanupExpiredDeviceAuthSessionsBatch)

	assertIdentityQueryCount(t, pool, `SELECT COUNT(*) FROM refresh_tokens`, 1)
	assertIdentityQueryCount(t, pool, `SELECT COUNT(*) FROM web_login_codes`, 1)
	assertIdentityQueryCount(t, pool, `SELECT COUNT(*) FROM device_auth_sessions`, 1)
}

func createIdentityGuardTestUser(t *testing.T, repo *Repository, email string) *User {
	t.Helper()
	user := &User{Email: email, Name: email}
	if err := repo.CreateUser(context.Background(), user); err != nil {
		t.Fatalf("create user %s: %v", email, err)
	}
	return user
}

func assertIdentityQueryCount(
	t *testing.T,
	pool *pgxpool.Pool,
	query string,
	want int64,
	args ...any,
) {
	t.Helper()
	var got int64
	if err := pool.QueryRow(context.Background(), query, args...).Scan(&got); err != nil {
		t.Fatalf("query count: %v", err)
	}
	if got != want {
		t.Fatalf("query count = %d, want %d", got, want)
	}
}

func runConcurrentIdentityCleanup(
	t *testing.T,
	name string,
	want int64,
	cleanup func(context.Context, int) (int64, error),
) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	const workers = 2
	start := make(chan struct{})
	errs := make(chan error, workers)
	var deleted atomic.Int64
	var wait sync.WaitGroup
	for i := 0; i < workers; i++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			<-start
			for {
				count, err := cleanup(ctx, 3)
				if err != nil {
					errs <- err
					return
				}
				deleted.Add(count)
				if count == 0 {
					return
				}
			}
		}()
	}
	close(start)
	wait.Wait()
	close(errs)
	for err := range errs {
		t.Errorf("cleanup %s: %v", name, err)
	}
	if got := deleted.Load(); got != want {
		t.Fatalf("cleanup %s deleted %d rows, want %d", name, got, want)
	}
}

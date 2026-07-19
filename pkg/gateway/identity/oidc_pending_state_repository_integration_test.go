package identity

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestOIDCPendingStateConcurrentCapAndSingleUseConsume(t *testing.T) {
	pool, _ := newGatewayIdentityTestPool(t)
	if pool == nil {
		return
	}

	ctx := context.Background()
	repo := NewRepository(pool, WithIdentityResourceGuard(IdentityResourceGuardLimits{
		MaxPendingOIDCStates: 3,
	}))

	const attempts = 12
	type createResult struct {
		state *OIDCPendingState
		err   error
	}
	start := make(chan struct{})
	results := make(chan createResult, attempts)
	var workers sync.WaitGroup
	for i := 0; i < attempts; i++ {
		workers.Add(1)
		go func(index int) {
			defer workers.Done()
			<-start
			state := &OIDCPendingState{
				StateHash:       fmt.Sprintf("oidc-state-hash-%02d", index),
				Provider:        "test-provider",
				CodeVerifier:    fmt.Sprintf("code-verifier-%02d", index),
				ReturnURL:       "https://dashboard.example.test/callback",
				WebLoginHandoff: true,
				ExpiresAt:       time.Now().Add(time.Minute),
			}
			results <- createResult{
				state: state,
				err:   repo.CreateOIDCPendingState(ctx, state),
			}
		}(i)
	}
	close(start)
	workers.Wait()
	close(results)

	var created []*OIDCPendingState
	var rejected int
	for result := range results {
		switch {
		case result.err == nil:
			created = append(created, result.state)
		case IsIdentityResourceLimitExceeded(result.err):
			var limitErr *IdentityResourceLimitExceededError
			if !errors.As(result.err, &limitErr) {
				t.Fatalf("limit error has unexpected type: %v", result.err)
			}
			if limitErr.Resource != IdentityLimitResourceOIDCStates || limitErr.Limit != 3 {
				t.Errorf("limit error = %+v, want pending_oidc_states limit 3", limitErr)
			}
			rejected++
		default:
			t.Errorf("create OIDC pending state returned unexpected error: %v", result.err)
		}
	}
	if len(created) != 3 || rejected != attempts-3 {
		t.Fatalf(
			"OIDC states created/rejected = %d/%d, want 3/%d",
			len(created),
			rejected,
			attempts-3,
		)
	}
	assertIdentityQueryCount(t, pool, `
		SELECT COUNT(*)
		FROM oidc_pending_states
		WHERE expires_at > NOW()
	`, 3)

	winnerState := created[0]
	const consumers = 10
	type consumeResult struct {
		state *OIDCPendingState
		err   error
	}
	start = make(chan struct{})
	consumeResults := make(chan consumeResult, consumers)
	workers = sync.WaitGroup{}
	for i := 0; i < consumers; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			<-start
			state, err := repo.ConsumeOIDCPendingState(ctx, winnerState.StateHash)
			consumeResults <- consumeResult{state: state, err: err}
		}()
	}
	close(start)
	workers.Wait()
	close(consumeResults)

	var consumed *OIDCPendingState
	var consumedCount, notFoundCount int
	for result := range consumeResults {
		switch {
		case result.err == nil:
			consumed = result.state
			consumedCount++
		case errors.Is(result.err, ErrOIDCPendingStateNotFound):
			notFoundCount++
		default:
			t.Errorf("consume OIDC pending state returned unexpected error: %v", result.err)
		}
	}
	if consumedCount != 1 || notFoundCount != consumers-1 {
		t.Fatalf(
			"OIDC state consumed/not-found = %d/%d, want 1/%d",
			consumedCount,
			notFoundCount,
			consumers-1,
		)
	}
	if consumed == nil ||
		consumed.Provider != winnerState.Provider ||
		consumed.CodeVerifier != winnerState.CodeVerifier ||
		consumed.ReturnURL != winnerState.ReturnURL ||
		!consumed.WebLoginHandoff ||
		consumed.CreatedAt.IsZero() {
		t.Fatalf("consumed OIDC pending state = %+v, want persisted state", consumed)
	}
	assertIdentityQueryCount(t, pool, `
		SELECT COUNT(*)
		FROM oidc_pending_states
		WHERE state_hash = $1
	`, 0, winnerState.StateHash)
}

func TestOIDCPendingStateConcurrentBatchCleanupAndAggregateCleanup(t *testing.T) {
	pool, _ := newGatewayIdentityTestPool(t)
	if pool == nil {
		return
	}

	ctx := context.Background()
	repo := NewRepository(pool)
	if got := repo.oidcPendingStateLimit(); got != DefaultMaxPendingOIDCStates {
		t.Fatalf(
			"default pending OIDC state limit = %d, want %d",
			got,
			DefaultMaxPendingOIDCStates,
		)
	}

	const expiredRows = 17
	if _, err := pool.Exec(ctx, `
		INSERT INTO oidc_pending_states (
			state_hash,
			provider,
			code_verifier,
			return_url,
			web_login_handoff,
			expires_at,
			created_at
		)
		SELECT
			'cleanup-expired-oidc-' || value,
			'test-provider',
			'cleanup-code-verifier-' || value,
			'https://dashboard.example.test/callback',
			true,
			NOW() - INTERVAL '1 minute',
			NOW() - INTERVAL '2 minutes'
		FROM generate_series(1, $1) AS value
	`, expiredRows); err != nil {
		t.Fatalf("seed expired OIDC pending states: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO oidc_pending_states (
			state_hash,
			provider,
			code_verifier,
			return_url,
			web_login_handoff,
			expires_at
		)
		VALUES (
			'cleanup-live-oidc',
			'test-provider',
			'cleanup-live-code-verifier',
			'https://dashboard.example.test/callback',
			true,
			NOW() + INTERVAL '1 hour'
		)
	`); err != nil {
		t.Fatalf("seed live OIDC pending state: %v", err)
	}

	runConcurrentIdentityCleanup(
		t,
		"OIDC pending states",
		expiredRows,
		repo.CleanupExpiredOIDCPendingStatesBatch,
	)
	assertIdentityQueryCount(t, pool, `SELECT COUNT(*) FROM oidc_pending_states`, 1)

	if _, err := pool.Exec(ctx, `
		INSERT INTO oidc_pending_states (
			state_hash,
			provider,
			code_verifier,
			return_url,
			expires_at,
			created_at
		)
		VALUES (
			'aggregate-cleanup-expired-oidc',
			'test-provider',
			'aggregate-cleanup-code-verifier',
			'',
			NOW() - INTERVAL '1 minute',
			NOW() - INTERVAL '2 minutes'
		)
	`); err != nil {
		t.Fatalf("seed aggregate-cleanup OIDC pending state: %v", err)
	}
	if err := repo.CleanupIdentitySessionsBatch(ctx, 1); err != nil {
		t.Fatalf("aggregate identity session cleanup: %v", err)
	}
	assertIdentityQueryCount(t, pool, `SELECT COUNT(*) FROM oidc_pending_states`, 1)
}

func TestOIDCPendingStateRejectsOversizedPersistentFields(t *testing.T) {
	pool, _ := newGatewayIdentityTestPool(t)
	if pool == nil {
		return
	}

	repo := NewRepository(pool)
	err := repo.CreateOIDCPendingState(context.Background(), &OIDCPendingState{
		StateHash:    "state-hash",
		Provider:     "test-provider",
		CodeVerifier: "code-verifier",
		ReturnURL:    string(make([]byte, MaxIdentityReturnURLBytes+1)),
		ExpiresAt:    time.Now().Add(time.Minute),
	})
	if !IsIdentityPayloadTooLarge(err) {
		t.Fatalf("oversized OIDC return URL error = %v, want payload-too-large", err)
	}
	assertIdentityQueryCount(t, pool, `SELECT COUNT(*) FROM oidc_pending_states`, 0)
}

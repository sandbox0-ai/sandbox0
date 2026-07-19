package identity

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

// CreateOIDCPendingState persists one bounded, server-side OIDC authorization
// state. The platform-scoped advisory lock makes the active-state count exact
// across all identity gateway replicas.
func (r *Repository) CreateOIDCPendingState(
	ctx context.Context,
	state *OIDCPendingState,
) error {
	if err := validateOIDCPendingState(state); err != nil {
		return err
	}

	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin create OIDC pending state: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if err := lockIdentityScopesTx(ctx, tx, identityOIDCPendingStatesScope); err != nil {
		return err
	}

	limit := r.oidcPendingStateLimit()
	var active int64
	if err := tx.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM oidc_pending_states
		WHERE expires_at > NOW()
	`).Scan(&active); err != nil {
		return fmt.Errorf("count active OIDC pending states: %w", err)
	}
	if active >= limit {
		return &IdentityResourceLimitExceededError{
			Scope:    "platform",
			ScopeID:  "identity",
			Resource: IdentityLimitResourceOIDCStates,
			Limit:    limit,
		}
	}

	if err := tx.QueryRow(ctx, `
		INSERT INTO oidc_pending_states (
			state_hash,
			provider,
			code_verifier,
			return_url,
			web_login_handoff,
			expires_at
		)
		VALUES ($1, $2, $3, $4, $5, $6)
		RETURNING created_at
	`,
		state.StateHash,
		state.Provider,
		state.CodeVerifier,
		state.ReturnURL,
		state.WebLoginHandoff,
		state.ExpiresAt,
	).Scan(&state.CreatedAt); err != nil {
		if isDuplicateKeyError(err) {
			return ErrOIDCPendingStateAlreadyExists
		}
		return fmt.Errorf("insert OIDC pending state: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit create OIDC pending state: %w", err)
	}
	return nil
}

// ConsumeOIDCPendingState atomically deletes and returns one non-expired state.
// DELETE ... RETURNING guarantees that concurrent callbacks can have one winner
// across all gateway replicas.
func (r *Repository) ConsumeOIDCPendingState(
	ctx context.Context,
	stateHash string,
) (*OIDCPendingState, error) {
	if err := validateIdentityFieldSize(
		"state_hash",
		stateHash,
		MaxIdentityOIDCStateHashBytes,
	); err != nil {
		return nil, err
	}
	if strings.TrimSpace(stateHash) == "" {
		return nil, fmt.Errorf("%w: state hash is required", ErrInvalidOIDCPendingState)
	}

	var state OIDCPendingState
	err := r.pool.QueryRow(ctx, `
		DELETE FROM oidc_pending_states
		WHERE state_hash = $1
		  AND expires_at > NOW()
		RETURNING
			state_hash,
			provider,
			code_verifier,
			return_url,
			web_login_handoff,
			expires_at,
			created_at
	`, stateHash).Scan(
		&state.StateHash,
		&state.Provider,
		&state.CodeVerifier,
		&state.ReturnURL,
		&state.WebLoginHandoff,
		&state.ExpiresAt,
		&state.CreatedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrOIDCPendingStateNotFound
		}
		return nil, fmt.Errorf("consume OIDC pending state: %w", err)
	}
	return &state, nil
}

// CleanupExpiredOIDCPendingStates removes expired OIDC authorization states.
func (r *Repository) CleanupExpiredOIDCPendingStates(ctx context.Context) (int64, error) {
	return r.CleanupExpiredOIDCPendingStatesBatch(ctx, DefaultIdentitySessionCleanupBatchSize)
}

// CleanupExpiredOIDCPendingStatesBatch deletes one multi-replica-safe batch.
func (r *Repository) CleanupExpiredOIDCPendingStatesBatch(
	ctx context.Context,
	batchSize int,
) (int64, error) {
	if batchSize <= 0 {
		batchSize = DefaultIdentitySessionCleanupBatchSize
	}
	result, err := r.pool.Exec(ctx, `
		WITH doomed AS (
			SELECT state_hash
			FROM oidc_pending_states
			WHERE expires_at <= NOW()
			ORDER BY expires_at, created_at, state_hash
			LIMIT $1
			FOR UPDATE SKIP LOCKED
		)
		DELETE FROM oidc_pending_states state
		USING doomed
		WHERE state.state_hash = doomed.state_hash
	`, batchSize)
	if err != nil {
		return 0, fmt.Errorf("cleanup OIDC pending states: %w", err)
	}
	return result.RowsAffected(), nil
}

func (r *Repository) oidcPendingStateLimit() int64 {
	if limits, enabled := r.identityResourceLimits(); enabled {
		return limits.withDefaults().MaxPendingOIDCStates
	}
	return DefaultIdentityResourceGuardLimits().MaxPendingOIDCStates
}

func validateOIDCPendingState(state *OIDCPendingState) error {
	if state == nil {
		return fmt.Errorf("%w: state is required", ErrInvalidOIDCPendingState)
	}
	required := []struct {
		field    string
		value    string
		maxBytes int
	}{
		{
			field:    "state_hash",
			value:    state.StateHash,
			maxBytes: MaxIdentityOIDCStateHashBytes,
		},
		{
			field:    "provider",
			value:    state.Provider,
			maxBytes: MaxIdentityOIDCProviderBytes,
		},
		{
			field:    "code_verifier",
			value:    state.CodeVerifier,
			maxBytes: MaxIdentityOIDCCodeVerifierBytes,
		},
	}
	for _, candidate := range required {
		if strings.TrimSpace(candidate.value) == "" {
			return fmt.Errorf(
				"%w: %s is required",
				ErrInvalidOIDCPendingState,
				candidate.field,
			)
		}
		if err := validateIdentityFieldSize(
			candidate.field,
			candidate.value,
			candidate.maxBytes,
		); err != nil {
			return err
		}
	}
	if err := validateIdentityFieldSize(
		"return_url",
		state.ReturnURL,
		MaxIdentityReturnURLBytes,
	); err != nil {
		return err
	}
	if !state.ExpiresAt.After(time.Now()) {
		return fmt.Errorf("%w: expires_at must be in the future", ErrInvalidOIDCPendingState)
	}
	return nil
}

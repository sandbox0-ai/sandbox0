package identity

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// CreateRefreshToken creates a new refresh token.
func (r *Repository) CreateRefreshToken(ctx context.Context, token *RefreshToken) error {
	if r.identityResourceGuard == nil {
		return insertRefreshToken(ctx, r.pool, token)
	}

	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin create refresh token: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := lockIdentityScopesTx(ctx, tx, identityUserScope(token.UserID)); err != nil {
		return err
	}
	if err := deleteInactiveRefreshTokensForUserTx(ctx, tx, token.UserID); err != nil {
		return err
	}
	if err := retainNewestActiveRefreshTokensTx(
		ctx,
		tx,
		token.UserID,
		r.identityResourceGuard.MaxActiveRefreshTokensPerUser-1,
	); err != nil {
		return err
	}
	if err := insertRefreshToken(ctx, tx, token); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit create refresh token: %w", err)
	}
	return nil
}

type refreshTokenInsertQuerier interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}

func insertRefreshToken(ctx context.Context, q refreshTokenInsertQuerier, token *RefreshToken) error {
	err := q.QueryRow(ctx, `
			INSERT INTO refresh_tokens (user_id, token_hash, expires_at)
			VALUES ($1, $2, $3)
			RETURNING id, created_at
	`, token.UserID, token.TokenHash, token.ExpiresAt,
	).Scan(&token.ID, &token.CreatedAt)
	if err != nil {
		return fmt.Errorf("insert refresh token: %w", err)
	}
	return nil
}

// RotateRefreshToken consumes one refresh token and creates its replacement in
// the same user-scoped transaction. A token can win this transaction only once.
func (r *Repository) RotateRefreshToken(
	ctx context.Context,
	currentTokenHash string,
	replacement *RefreshToken,
) error {
	if replacement == nil || replacement.UserID == "" {
		return fmt.Errorf("replacement refresh token user is required")
	}
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin rotate refresh token: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := lockIdentityScopesTx(ctx, tx, identityUserScope(replacement.UserID)); err != nil {
		return err
	}

	var storedUserID string
	var expiresAt time.Time
	var revoked bool
	if err := tx.QueryRow(ctx, `
		SELECT user_id, expires_at, revoked
		FROM refresh_tokens
		WHERE token_hash = $1
		FOR UPDATE
	`, currentTokenHash).Scan(&storedUserID, &expiresAt, &revoked); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrTokenNotFound
		}
		return fmt.Errorf("lock refresh token for rotation: %w", err)
	}
	if storedUserID != replacement.UserID {
		return ErrTokenNotFound
	}
	if revoked {
		return ErrTokenRevoked
	}
	if !expiresAt.After(time.Now()) {
		return ErrTokenExpired
	}
	if _, err := tx.Exec(ctx, `
		DELETE FROM refresh_tokens
		WHERE token_hash = $1
	`, currentTokenHash); err != nil {
		return fmt.Errorf("consume refresh token: %w", err)
	}

	if r.identityResourceGuard != nil {
		if err := deleteInactiveRefreshTokensForUserTx(ctx, tx, replacement.UserID); err != nil {
			return err
		}
		if err := retainNewestActiveRefreshTokensTx(
			ctx,
			tx,
			replacement.UserID,
			r.identityResourceGuard.MaxActiveRefreshTokensPerUser-1,
		); err != nil {
			return err
		}
	}
	if err := insertRefreshToken(ctx, tx, replacement); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit rotate refresh token: %w", err)
	}
	return nil
}

func deleteInactiveRefreshTokensForUserTx(ctx context.Context, tx pgx.Tx, userID string) error {
	if _, err := tx.Exec(ctx, `
		DELETE FROM refresh_tokens
		WHERE user_id = $1
		  AND (expires_at <= NOW() OR revoked = true)
	`, userID); err != nil {
		return fmt.Errorf("delete inactive user refresh tokens: %w", err)
	}
	return nil
}

func retainNewestActiveRefreshTokensTx(
	ctx context.Context,
	tx pgx.Tx,
	userID string,
	keep int64,
) error {
	if keep < 0 {
		keep = 0
	}
	if _, err := tx.Exec(ctx, `
		WITH stale AS (
			SELECT id
			FROM refresh_tokens
			WHERE user_id = $1
			  AND revoked = false
			  AND expires_at > NOW()
			ORDER BY created_at DESC, id DESC
			OFFSET $2
			FOR UPDATE
		)
		DELETE FROM refresh_tokens token
		USING stale
		WHERE token.id = stale.id
	`, userID, keep); err != nil {
		return fmt.Errorf("prune active user refresh tokens: %w", err)
	}
	return nil
}

// ValidateRefreshToken validates a refresh token and returns it.
func (r *Repository) ValidateRefreshToken(ctx context.Context, tokenHash string) (*RefreshToken, error) {
	var token RefreshToken
	err := r.pool.QueryRow(ctx, `
		SELECT id, user_id, token_hash, expires_at, revoked, created_at
		FROM refresh_tokens
		WHERE token_hash = $1
	`, tokenHash).Scan(
		&token.ID, &token.UserID, &token.TokenHash,
		&token.ExpiresAt, &token.Revoked, &token.CreatedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrTokenNotFound
		}
		return nil, fmt.Errorf("query token: %w", err)
	}

	if token.Revoked {
		return nil, ErrTokenRevoked
	}
	if time.Now().After(token.ExpiresAt) {
		return nil, ErrTokenExpired
	}
	return &token, nil
}

// RevokeRefreshToken revokes a refresh token.
func (r *Repository) RevokeRefreshToken(ctx context.Context, tokenHash string) error {
	result, err := r.pool.Exec(ctx, `
		UPDATE refresh_tokens SET revoked = true WHERE token_hash = $1
	`, tokenHash)
	if err != nil {
		return fmt.Errorf("revoke token: %w", err)
	}
	if result.RowsAffected() == 0 {
		return ErrTokenNotFound
	}
	return nil
}

// RevokeAllUserRefreshTokens revokes all refresh tokens for a user.
func (r *Repository) RevokeAllUserRefreshTokens(ctx context.Context, userID string) error {
	if r.identityResourceGuard == nil {
		_, err := r.pool.Exec(ctx, `
			UPDATE refresh_tokens SET revoked = true WHERE user_id = $1
		`, userID)
		if err != nil {
			return fmt.Errorf("revoke all tokens: %w", err)
		}
		return nil
	}
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin revoke all refresh tokens: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := lockIdentityScopesTx(ctx, tx, identityUserScope(userID)); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `
		UPDATE refresh_tokens SET revoked = true WHERE user_id = $1
	`, userID); err != nil {
		return fmt.Errorf("revoke all tokens: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit revoke all refresh tokens: %w", err)
	}
	return nil
}

// CleanupExpiredTokens removes expired refresh tokens.
func (r *Repository) CleanupExpiredTokens(ctx context.Context) (int64, error) {
	return r.CleanupExpiredTokensBatch(ctx, 1_000)
}

// CleanupExpiredTokensBatch deletes one lock-safe batch. SKIP LOCKED lets
// multiple gateway replicas run the same cleanup loop without blocking.
func (r *Repository) CleanupExpiredTokensBatch(ctx context.Context, batchSize int) (int64, error) {
	if batchSize <= 0 {
		batchSize = 1_000
	}
	result, err := r.pool.Exec(ctx, `
		WITH doomed AS (
			SELECT id
			FROM refresh_tokens
			WHERE expires_at <= NOW() OR revoked = true
			ORDER BY created_at, id
			LIMIT $1
			FOR UPDATE SKIP LOCKED
		)
		DELETE FROM refresh_tokens token
		USING doomed
		WHERE token.id = doomed.id
	`, batchSize)
	if err != nil {
		return 0, fmt.Errorf("cleanup tokens: %w", err)
	}
	return result.RowsAffected(), nil
}

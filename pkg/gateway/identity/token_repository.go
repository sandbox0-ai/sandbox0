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
	err := r.pool.QueryRow(ctx, `
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

// RotateRefreshToken atomically revokes a live refresh token and stores its replacement.
// Only one concurrent caller can successfully consume a given token hash.
func (r *Repository) RotateRefreshToken(ctx context.Context, tokenHash string, replacement *RefreshToken) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin refresh token rotation: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var userID string
	err = tx.QueryRow(ctx, `
		UPDATE refresh_tokens
		SET revoked = true
		WHERE token_hash = $1 AND revoked = false AND expires_at > NOW()
		RETURNING user_id
	`, tokenHash).Scan(&userID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrTokenRevoked
		}
		return fmt.Errorf("consume refresh token: %w", err)
	}
	if replacement == nil || userID != replacement.UserID {
		return ErrTokenNotFound
	}

	err = tx.QueryRow(ctx, `
		INSERT INTO refresh_tokens (user_id, token_hash, expires_at)
		VALUES ($1, $2, $3)
		RETURNING id, created_at
	`, replacement.UserID, replacement.TokenHash, replacement.ExpiresAt,
	).Scan(&replacement.ID, &replacement.CreatedAt)
	if err != nil {
		return fmt.Errorf("insert replacement refresh token: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit refresh token rotation: %w", err)
	}
	return nil
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
	_, err := r.pool.Exec(ctx, `
		UPDATE refresh_tokens SET revoked = true WHERE user_id = $1
	`, userID)
	if err != nil {
		return fmt.Errorf("revoke all tokens: %w", err)
	}
	return nil
}

// CleanupExpiredTokens removes expired refresh tokens.
func (r *Repository) CleanupExpiredTokens(ctx context.Context) (int64, error) {
	result, err := r.pool.Exec(ctx, `
		DELETE FROM refresh_tokens WHERE expires_at < NOW() OR revoked = true
	`)
	if err != nil {
		return 0, fmt.Errorf("cleanup tokens: %w", err)
	}
	return result.RowsAffected(), nil
}

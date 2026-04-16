package identity

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// CreateWebLoginCode persists a one-time browser login handoff code.
func (r *Repository) CreateWebLoginCode(ctx context.Context, code *WebLoginCode) error {
	err := r.pool.QueryRow(ctx, `
		INSERT INTO web_login_codes (code_hash, user_id, return_url, expires_at)
		VALUES ($1, $2, $3, $4)
		RETURNING id, created_at
	`, code.CodeHash, code.UserID, code.ReturnURL, code.ExpiresAt,
	).Scan(&code.ID, &code.CreatedAt)
	if err != nil {
		return fmt.Errorf("insert web login code: %w", err)
	}
	return nil
}

// ConsumeWebLoginCode consumes a non-expired code bound to the given return URL.
func (r *Repository) ConsumeWebLoginCode(ctx context.Context, codeHash, returnURL string) (*WebLoginCode, error) {
	var code WebLoginCode
	err := r.pool.QueryRow(ctx, `
		UPDATE web_login_codes
		SET consumed_at = NOW()
		WHERE code_hash = $1
		  AND return_url = $2
		  AND consumed_at IS NULL
		  AND expires_at > NOW()
		RETURNING id, code_hash, user_id, return_url, expires_at, consumed_at, created_at
	`, codeHash, returnURL).Scan(
		&code.ID,
		&code.CodeHash,
		&code.UserID,
		&code.ReturnURL,
		&code.ExpiresAt,
		&code.ConsumedAt,
		&code.CreatedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrWebLoginCodeNotFound
		}
		return nil, fmt.Errorf("consume web login code: %w", err)
	}
	return &code, nil
}

// CleanupExpiredWebLoginCodes removes expired or already consumed handoff codes.
func (r *Repository) CleanupExpiredWebLoginCodes(ctx context.Context) (int64, error) {
	result, err := r.pool.Exec(ctx, `
		DELETE FROM web_login_codes WHERE expires_at < NOW() OR consumed_at IS NOT NULL
	`)
	if err != nil {
		return 0, fmt.Errorf("cleanup web login codes: %w", err)
	}
	return result.RowsAffected(), nil
}

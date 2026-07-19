package identity

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// CreateWebLoginCode persists a one-time browser login handoff code.
func (r *Repository) CreateWebLoginCode(ctx context.Context, code *WebLoginCode) error {
	if err := validateIdentityFieldSize(
		"return_url",
		code.ReturnURL,
		MaxIdentityReturnURLBytes,
	); err != nil {
		return err
	}
	if r.identityResourceGuard == nil {
		return insertWebLoginCode(ctx, r.pool, code)
	}

	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin create web login code: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := lockIdentityScopesTx(ctx, tx, identityUserScope(code.UserID)); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `
		DELETE FROM web_login_codes
		WHERE user_id = $1
		  AND (expires_at <= NOW() OR consumed_at IS NOT NULL)
	`, code.UserID); err != nil {
		return fmt.Errorf("delete inactive user web login codes: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		WITH stale AS (
			SELECT id
			FROM web_login_codes
			WHERE user_id = $1
			  AND consumed_at IS NULL
			  AND expires_at > NOW()
			ORDER BY created_at DESC, id DESC
			OFFSET $2
			FOR UPDATE
		)
		DELETE FROM web_login_codes code
		USING stale
		WHERE code.id = stale.id
	`, code.UserID, r.identityResourceGuard.MaxActiveWebLoginCodesPerUser-1); err != nil {
		return fmt.Errorf("prune active user web login codes: %w", err)
	}
	if err := insertWebLoginCode(ctx, tx, code); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit create web login code: %w", err)
	}
	return nil
}

type webLoginCodeInsertQuerier interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}

func insertWebLoginCode(ctx context.Context, q webLoginCodeInsertQuerier, code *WebLoginCode) error {
	err := q.QueryRow(ctx, `
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
	return r.CleanupExpiredWebLoginCodesBatch(ctx, 1_000)
}

// CleanupExpiredWebLoginCodesBatch deletes one multi-replica-safe batch.
func (r *Repository) CleanupExpiredWebLoginCodesBatch(ctx context.Context, batchSize int) (int64, error) {
	if batchSize <= 0 {
		batchSize = 1_000
	}
	result, err := r.pool.Exec(ctx, `
		WITH doomed AS (
			SELECT id
			FROM web_login_codes
			WHERE expires_at <= NOW() OR consumed_at IS NOT NULL
			ORDER BY created_at, id
			LIMIT $1
			FOR UPDATE SKIP LOCKED
		)
		DELETE FROM web_login_codes code
		USING doomed
		WHERE code.id = doomed.id
	`, batchSize)
	if err != nil {
		return 0, fmt.Errorf("cleanup web login codes: %w", err)
	}
	return result.RowsAffected(), nil
}

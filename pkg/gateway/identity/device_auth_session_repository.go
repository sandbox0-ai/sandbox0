package identity

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// CreateDeviceAuthSession persists a pending device authorization session.
func (r *Repository) CreateDeviceAuthSession(ctx context.Context, session *DeviceAuthSession) error {
	if err := validateIdentityFieldSize(
		"device_code",
		session.DeviceCode,
		MaxIdentityDeviceCodeBytes,
	); err != nil {
		return err
	}
	if err := validateIdentityFieldSize(
		"verification_uri",
		session.VerificationURI,
		MaxIdentityVerificationURIBytes,
	); err != nil {
		return err
	}
	if err := validateIdentityFieldSize(
		"verification_uri_complete",
		session.VerificationURIComplete,
		MaxIdentityVerificationURIBytes,
	); err != nil {
		return err
	}
	if r.identityResourceGuard == nil {
		return insertDeviceAuthSession(ctx, r.pool, session)
	}

	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin create device auth session: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := lockIdentityScopesTx(ctx, tx, identityDeviceSessionsScope); err != nil {
		return err
	}
	var active int64
	if err := tx.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM device_auth_sessions
		WHERE consumed_at IS NULL
		  AND expires_at > NOW()
	`).Scan(&active); err != nil {
		return fmt.Errorf("count active device auth sessions: %w", err)
	}
	if active >= r.identityResourceGuard.MaxActiveDeviceAuthSessions {
		return &IdentityResourceLimitExceededError{
			Scope:    "platform",
			ScopeID:  "identity",
			Resource: IdentityLimitResourceDeviceSessions,
			Limit:    r.identityResourceGuard.MaxActiveDeviceAuthSessions,
		}
	}
	if err := insertDeviceAuthSession(ctx, tx, session); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit create device auth session: %w", err)
	}
	return nil
}

type deviceAuthSessionInsertQuerier interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}

func insertDeviceAuthSession(
	ctx context.Context,
	q deviceAuthSessionInsertQuerier,
	session *DeviceAuthSession,
) error {
	err := q.QueryRow(ctx, `
			INSERT INTO device_auth_sessions (
			provider,
			device_code,
			user_code,
			verification_uri,
			verification_uri_complete,
			interval_seconds,
			expires_at
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		RETURNING id, created_at, updated_at
	`,
		session.Provider,
		session.DeviceCode,
		session.UserCode,
		session.VerificationURI,
		nullString(session.VerificationURIComplete),
		session.IntervalSeconds,
		session.ExpiresAt,
	).Scan(&session.ID, &session.CreatedAt, &session.UpdatedAt)
	if err != nil {
		return fmt.Errorf("insert device auth session: %w", err)
	}
	return nil
}

// GetDeviceAuthSessionByID loads a pending device authorization session.
func (r *Repository) GetDeviceAuthSessionByID(ctx context.Context, id string) (*DeviceAuthSession, error) {
	var (
		session                    DeviceAuthSession
		verificationURICompleteRaw *string
	)
	err := r.pool.QueryRow(ctx, `
		SELECT id, provider, device_code, user_code, verification_uri, verification_uri_complete,
			interval_seconds, expires_at, consumed_at, created_at, updated_at
		FROM device_auth_sessions
		WHERE id = $1
	`, id).Scan(
		&session.ID,
		&session.Provider,
		&session.DeviceCode,
		&session.UserCode,
		&session.VerificationURI,
		&verificationURICompleteRaw,
		&session.IntervalSeconds,
		&session.ExpiresAt,
		&session.ConsumedAt,
		&session.CreatedAt,
		&session.UpdatedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrDeviceAuthSessionNotFound
		}
		return nil, fmt.Errorf("query device auth session: %w", err)
	}
	if verificationURICompleteRaw != nil {
		session.VerificationURIComplete = *verificationURICompleteRaw
	}
	if session.ConsumedAt != nil {
		return nil, ErrDeviceAuthSessionConsumed
	}
	if time.Now().After(session.ExpiresAt) {
		return nil, ErrDeviceAuthSessionExpired
	}
	return &session, nil
}

// MarkDeviceAuthSessionConsumed marks a session as consumed exactly once.
func (r *Repository) MarkDeviceAuthSessionConsumed(ctx context.Context, id string) error {
	now := time.Now()
	result, err := r.pool.Exec(ctx, `
		UPDATE device_auth_sessions
		SET consumed_at = $2
		WHERE id = $1 AND consumed_at IS NULL AND expires_at > $2
	`, id, now)
	if err != nil {
		return fmt.Errorf("mark device auth session consumed: %w", err)
	}
	if result.RowsAffected() == 0 {
		session, getErr := r.GetDeviceAuthSessionByID(ctx, id)
		if getErr == nil && session != nil {
			return ErrDeviceAuthSessionConsumed
		}
		return getErr
	}
	return nil
}

// CleanupExpiredDeviceAuthSessions removes expired or consumed device auth sessions.
func (r *Repository) CleanupExpiredDeviceAuthSessions(ctx context.Context) (int64, error) {
	return r.CleanupExpiredDeviceAuthSessionsBatch(ctx, 1_000)
}

// CleanupExpiredDeviceAuthSessionsBatch deletes one multi-replica-safe batch.
func (r *Repository) CleanupExpiredDeviceAuthSessionsBatch(
	ctx context.Context,
	batchSize int,
) (int64, error) {
	if batchSize <= 0 {
		batchSize = 1_000
	}
	result, err := r.pool.Exec(ctx, `
		WITH doomed AS (
			SELECT id
			FROM device_auth_sessions
			WHERE expires_at <= NOW() OR consumed_at IS NOT NULL
			ORDER BY created_at, id
			LIMIT $1
			FOR UPDATE SKIP LOCKED
		)
		DELETE FROM device_auth_sessions session
		USING doomed
		WHERE session.id = doomed.id
	`, batchSize)
	if err != nil {
		return 0, fmt.Errorf("cleanup device auth sessions: %w", err)
	}
	return result.RowsAffected(), nil
}

func nullString(value string) *string {
	if value == "" {
		return nil
	}
	return &value
}

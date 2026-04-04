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
	err := r.pool.QueryRow(ctx, `
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
	result, err := r.pool.Exec(ctx, `
		DELETE FROM device_auth_sessions
		WHERE expires_at < NOW() OR consumed_at IS NOT NULL
	`)
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

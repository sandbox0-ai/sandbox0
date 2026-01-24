package db

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
)

var (
	ErrIdentityNotFound      = errors.New("identity not found")
	ErrIdentityAlreadyExists = errors.New("identity already exists")
)

// CreateUserIdentity creates a new user identity (OIDC mapping)
func (r *Repository) CreateUserIdentity(ctx context.Context, identity *UserIdentity) error {
	err := r.pool.QueryRow(ctx, `
		INSERT INTO user_identities (user_id, provider, subject, raw_claims)
		VALUES ($1, $2, $3, $4)
		RETURNING id, created_at, updated_at
	`, identity.UserID, identity.Provider, identity.Subject, identity.RawClaims,
	).Scan(&identity.ID, &identity.CreatedAt, &identity.UpdatedAt)

	if err != nil {
		if isDuplicateKeyError(err) {
			return ErrIdentityAlreadyExists
		}
		return fmt.Errorf("insert identity: %w", err)
	}

	return nil
}

// GetUserIdentityByProviderSubject retrieves an identity by provider and subject
func (r *Repository) GetUserIdentityByProviderSubject(ctx context.Context, provider, subject string) (*UserIdentity, error) {
	var identity UserIdentity
	err := r.pool.QueryRow(ctx, `
		SELECT id, user_id, provider, subject, raw_claims, created_at, updated_at
		FROM user_identities
		WHERE provider = $1 AND subject = $2
	`, provider, subject).Scan(
		&identity.ID, &identity.UserID, &identity.Provider,
		&identity.Subject, &identity.RawClaims, &identity.CreatedAt, &identity.UpdatedAt,
	)

	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrIdentityNotFound
		}
		return nil, fmt.Errorf("query identity: %w", err)
	}

	return &identity, nil
}

// GetUserIdentitiesByUserID retrieves all identities for a user
func (r *Repository) GetUserIdentitiesByUserID(ctx context.Context, userID string) ([]*UserIdentity, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT id, user_id, provider, subject, raw_claims, created_at, updated_at
		FROM user_identities
		WHERE user_id = $1
		ORDER BY created_at
	`, userID)
	if err != nil {
		return nil, fmt.Errorf("query identities: %w", err)
	}
	defer rows.Close()

	var identities []*UserIdentity
	for rows.Next() {
		var identity UserIdentity
		if err := rows.Scan(
			&identity.ID, &identity.UserID, &identity.Provider,
			&identity.Subject, &identity.RawClaims, &identity.CreatedAt, &identity.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan identity: %w", err)
		}
		identities = append(identities, &identity)
	}

	return identities, nil
}

// UpdateUserIdentityClaims updates the raw claims for an identity
func (r *Repository) UpdateUserIdentityClaims(ctx context.Context, id string, rawClaims json.RawMessage) error {
	result, err := r.pool.Exec(ctx, `
		UPDATE user_identities SET raw_claims = $2 WHERE id = $1
	`, id, rawClaims)

	if err != nil {
		return fmt.Errorf("update claims: %w", err)
	}

	if result.RowsAffected() == 0 {
		return ErrIdentityNotFound
	}

	return nil
}

// DeleteUserIdentity deletes an identity
func (r *Repository) DeleteUserIdentity(ctx context.Context, id string) error {
	result, err := r.pool.Exec(ctx, `DELETE FROM user_identities WHERE id = $1`, id)
	if err != nil {
		return fmt.Errorf("delete identity: %w", err)
	}

	if result.RowsAffected() == 0 {
		return ErrIdentityNotFound
	}

	return nil
}

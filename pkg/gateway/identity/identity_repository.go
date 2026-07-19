package identity

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// CreateUserIdentity creates a new user identity.
func (r *Repository) CreateUserIdentity(ctx context.Context, identity *UserIdentity) error {
	if err := validateIdentityBytesSize(
		"raw_claims",
		identity.RawClaims,
		MaxIdentityRawClaimsBytes,
	); err != nil {
		return err
	}
	limits, guarded := r.identityResourceLimits()
	if !guarded {
		return insertUserIdentity(ctx, r.pool, identity)
	}

	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin create user identity: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := lockIdentityScopesTx(ctx, tx, identityUserScope(identity.UserID)); err != nil {
		return err
	}
	var count int64
	if err := tx.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM user_identities
		WHERE user_id = $1
	`, identity.UserID).Scan(&count); err != nil {
		return fmt.Errorf("count linked user identities: %w", err)
	}
	if count >= limits.MaxLinkedIdentitiesPerUser {
		return &IdentityResourceLimitExceededError{
			Scope:    "user",
			ScopeID:  identity.UserID,
			Resource: IdentityLimitResourceLinkedIdentities,
			Limit:    limits.MaxLinkedIdentitiesPerUser,
		}
	}
	if err := insertUserIdentity(ctx, tx, identity); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit create user identity: %w", err)
	}
	return nil
}

type identityInsertQuerier interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}

func insertUserIdentity(ctx context.Context, q identityInsertQuerier, identity *UserIdentity) error {
	err := q.QueryRow(ctx, `
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

// GetUserIdentityByProviderSubject retrieves an identity by provider and subject.
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

// GetUserIdentitiesByUserID retrieves all identities for a user.
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

// UpdateUserIdentityClaims updates the raw claims for an identity.
func (r *Repository) UpdateUserIdentityClaims(ctx context.Context, id string, rawClaims []byte) error {
	if err := validateIdentityBytesSize("raw_claims", rawClaims, MaxIdentityRawClaimsBytes); err != nil {
		return err
	}
	result, err := r.pool.Exec(ctx, `
		UPDATE user_identities SET raw_claims = $2 WHERE id = $1
	`, id, rawClaims)
	if err != nil {
		return fmt.Errorf("update identity claims: %w", err)
	}
	if result.RowsAffected() == 0 {
		return ErrIdentityNotFound
	}
	return nil
}

// DeleteUserIdentity deletes an identity.
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

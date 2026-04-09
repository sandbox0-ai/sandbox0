package identity

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// CreateUserSSHPublicKey stores one SSH public key for a user.
func (r *Repository) CreateUserSSHPublicKey(ctx context.Context, key *UserSSHPublicKey) error {
	err := r.pool.QueryRow(ctx, `
		INSERT INTO user_ssh_public_keys (user_id, name, public_key, key_type, fingerprint_sha256, comment)
		VALUES ($1, $2, $3, $4, $5, $6)
		RETURNING id, created_at, updated_at
	`, key.UserID, key.Name, key.PublicKey, key.KeyType, key.FingerprintSHA256, key.Comment,
	).Scan(&key.ID, &key.CreatedAt, &key.UpdatedAt)
	if err != nil {
		if isDuplicateKeyError(err) {
			return ErrSSHPublicKeyAlreadyExists
		}
		return fmt.Errorf("insert ssh public key: %w", err)
	}
	return nil
}

// ListUserSSHPublicKeysByUserID lists SSH public keys uploaded by one user.
func (r *Repository) ListUserSSHPublicKeysByUserID(ctx context.Context, userID string) ([]*UserSSHPublicKey, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT id, user_id, name, public_key, key_type, fingerprint_sha256, comment, created_at, updated_at
		FROM user_ssh_public_keys
		WHERE user_id = $1
		ORDER BY created_at, id
	`, userID)
	if err != nil {
		return nil, fmt.Errorf("query ssh public keys: %w", err)
	}
	defer rows.Close()

	var keys []*UserSSHPublicKey
	for rows.Next() {
		var key UserSSHPublicKey
		if err := rows.Scan(
			&key.ID,
			&key.UserID,
			&key.Name,
			&key.PublicKey,
			&key.KeyType,
			&key.FingerprintSHA256,
			&key.Comment,
			&key.CreatedAt,
			&key.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan ssh public key: %w", err)
		}
		keys = append(keys, &key)
	}
	return keys, nil
}

// GetUserSSHPublicKeyByFingerprint resolves one SSH public key by normalized fingerprint.
func (r *Repository) GetUserSSHPublicKeyByFingerprint(ctx context.Context, fingerprint string) (*UserSSHPublicKey, error) {
	var key UserSSHPublicKey
	err := r.pool.QueryRow(ctx, `
		SELECT id, user_id, name, public_key, key_type, fingerprint_sha256, comment, created_at, updated_at
		FROM user_ssh_public_keys
		WHERE fingerprint_sha256 = $1
	`, fingerprint).Scan(
		&key.ID,
		&key.UserID,
		&key.Name,
		&key.PublicKey,
		&key.KeyType,
		&key.FingerprintSHA256,
		&key.Comment,
		&key.CreatedAt,
		&key.UpdatedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrSSHPublicKeyNotFound
		}
		return nil, fmt.Errorf("query ssh public key by fingerprint: %w", err)
	}
	return &key, nil
}

// DeleteUserSSHPublicKey deletes one SSH public key owned by a user.
func (r *Repository) DeleteUserSSHPublicKey(ctx context.Context, userID, keyID string) error {
	result, err := r.pool.Exec(ctx, `
		DELETE FROM user_ssh_public_keys
		WHERE id = $1 AND user_id = $2
	`, keyID, userID)
	if err != nil {
		return fmt.Errorf("delete ssh public key: %w", err)
	}
	if result.RowsAffected() == 0 {
		return ErrSSHPublicKeyNotFound
	}
	return nil
}

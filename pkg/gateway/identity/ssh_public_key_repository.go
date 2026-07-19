package identity

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/sandbox0-ai/sandbox0/pkg/teamquota"
)

// CreateUserSSHPublicKey stores one SSH public key for a user in a team.
func (r *Repository) CreateUserSSHPublicKey(ctx context.Context, key *UserSSHPublicKey) error {
	if err := ValidateUserSSHPublicKey(key); err != nil {
		return err
	}
	key.ID = uuid.NewString()
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin ssh public key transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := lockSSHPublicKeyUser(ctx, tx, key.UserID); err != nil {
		return err
	}
	quotaRef, err := teamquota.ReserveControlPlaneObjectTargetTx(
		ctx,
		r.teamQuotaStore,
		tx,
		teamquota.ControlPlaneObjectOwner(key.TeamID, teamquota.ControlPlaneOwnerKindSSHPublicKey, key.ID),
		"create_ssh_public_key",
		1,
	)
	if err != nil {
		return err
	}
	err = tx.QueryRow(ctx, `
		INSERT INTO user_ssh_public_keys (id, team_id, user_id, name, public_key, key_type, fingerprint_sha256, comment)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		RETURNING id, created_at, updated_at
	`, key.ID, key.TeamID, key.UserID, key.Name, key.PublicKey, key.KeyType, key.FingerprintSHA256, key.Comment,
	).Scan(&key.ID, &key.CreatedAt, &key.UpdatedAt)
	if err != nil {
		if isDuplicateKeyError(err) {
			return ErrSSHPublicKeyAlreadyExists
		}
		return fmt.Errorf("insert ssh public key: %w", err)
	}
	if err := teamquota.CommitControlPlaneObjectTargetTx(ctx, r.teamQuotaStore, tx, quotaRef); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit ssh public key: %w", err)
	}
	return nil
}

// ListUserSSHPublicKeysByTeamAndUserID lists SSH public keys uploaded by one user in one team.
func (r *Repository) ListUserSSHPublicKeysByTeamAndUserID(ctx context.Context, teamID, userID string) ([]*UserSSHPublicKey, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT id, COALESCE(team_id, ''), user_id, name, public_key, key_type, fingerprint_sha256, comment, created_at, updated_at
		FROM user_ssh_public_keys
		WHERE team_id = $1 AND user_id = $2
		ORDER BY created_at, id
	`, teamID, userID)
	if err != nil {
		return nil, fmt.Errorf("query ssh public keys: %w", err)
	}
	defer rows.Close()

	var keys []*UserSSHPublicKey
	for rows.Next() {
		var key UserSSHPublicKey
		if err := rows.Scan(
			&key.ID,
			&key.TeamID,
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

// ListUserSSHPublicKeysByFingerprint lists team-scoped SSH public keys by normalized fingerprint.
func (r *Repository) ListUserSSHPublicKeysByFingerprint(ctx context.Context, fingerprint string) ([]*UserSSHPublicKey, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT id, COALESCE(team_id, ''), user_id, name, public_key, key_type, fingerprint_sha256, comment, created_at, updated_at
		FROM user_ssh_public_keys
		WHERE fingerprint_sha256 = $1 AND COALESCE(team_id, '') <> ''
		ORDER BY team_id, created_at, id
	`, fingerprint)
	if err != nil {
		return nil, fmt.Errorf("query ssh public keys by fingerprint: %w", err)
	}
	defer rows.Close()

	keys := make([]*UserSSHPublicKey, 0)
	for rows.Next() {
		var key UserSSHPublicKey
		if err := rows.Scan(
			&key.ID,
			&key.TeamID,
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
		SELECT id, COALESCE(team_id, ''), user_id, name, public_key, key_type, fingerprint_sha256, comment, created_at, updated_at
		FROM user_ssh_public_keys
		WHERE fingerprint_sha256 = $1
		ORDER BY created_at, id
		LIMIT 1
	`, fingerprint).Scan(
		&key.ID,
		&key.TeamID,
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

// DeleteUserSSHPublicKeyByTeamAndUserID deletes one SSH public key owned by a user in a team.
func (r *Repository) DeleteUserSSHPublicKeyByTeamAndUserID(ctx context.Context, teamID, userID, keyID string) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin ssh public key delete transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if err := lockSSHPublicKeyUser(ctx, tx, userID); err != nil {
		return err
	}
	quotaRef, err := teamquota.BeginControlPlaneObjectReleaseTx(
		ctx,
		r.teamQuotaStore,
		tx,
		teamquota.ControlPlaneObjectOwner(teamID, teamquota.ControlPlaneOwnerKindSSHPublicKey, keyID),
		"delete_ssh_public_key",
		0,
	)
	if err != nil {
		return err
	}
	result, err := tx.Exec(ctx, `
		DELETE FROM user_ssh_public_keys
		WHERE id = $1 AND team_id = $2 AND user_id = $3
	`, keyID, teamID, userID)
	if err != nil {
		return fmt.Errorf("delete ssh public key: %w", err)
	}
	if result.RowsAffected() == 0 {
		return ErrSSHPublicKeyNotFound
	}
	if err := teamquota.ConfirmControlPlaneObjectReleaseTx(ctx, r.teamQuotaStore, tx, quotaRef); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit ssh public key deletion: %w", err)
	}
	return nil
}

func lockSSHPublicKeyUser(ctx context.Context, tx pgx.Tx, userID string) error {
	if _, err := tx.Exec(
		ctx,
		`SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`,
		"gateway:ssh-public-keys:user:"+userID,
	); err != nil {
		return &teamquota.UnavailableError{
			Operation: "lock user ssh public key mutations",
			Err:       err,
		}
	}
	return nil
}

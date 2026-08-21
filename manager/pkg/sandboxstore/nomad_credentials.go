package sandboxstore

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/credentialbinding"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/egressauthstore"
)

// SandboxCredentialBindingTx is the optional transactional capability used
// when a paused policy and its external binding projection must publish as one
// PostgreSQL commit.
type SandboxCredentialBindingTx interface {
	ReplaceNomadSandboxCredentialBindings(context.Context, string, string, []egressauthstore.CredentialBinding) (string, error)
}

// GetNomadSandboxCredentialBindings returns current materialization only when
// it still matches the source-version-independent digest bound to admission.
func (s *PGSandboxStore) GetNomadSandboxCredentialBindings(
	ctx context.Context,
	teamID, sandboxID string,
) (*NomadSandboxCredentialBindings, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("sandbox store is not configured")
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead})
	if err != nil {
		return nil, fmt.Errorf("begin credential binding read tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	result, err := getNomadSandboxCredentialBindingsTx(ctx, tx, teamID, sandboxID)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit credential binding read tx: %w", err)
	}
	return result, nil
}

func getNomadSandboxCredentialBindingsTx(
	ctx context.Context,
	tx pgx.Tx,
	teamID, sandboxID string,
) (*NomadSandboxCredentialBindings, error) {
	var digest string
	if err := tx.QueryRow(ctx, `
		SELECT claim.credential_binding_digest
		FROM manager.sandbox_runtime_claims AS claim
		JOIN manager.sandboxes AS sandbox ON sandbox.sandbox_id = claim.sandbox_id
		WHERE claim.sandbox_id = $1 AND sandbox.team_id = $2
	`, sandboxID, teamID).Scan(&digest); err != nil {
		if err == pgx.ErrNoRows {
			return nil, fmt.Errorf("%w: credential binding claim is missing", ErrSandboxClaimReservationConflict)
		}
		return nil, fmt.Errorf("load credential binding digest: %w", err)
	}
	record, err := egressauthstore.GetCurrentBindings(ctx, tx, teamID, sandboxID)
	if err != nil {
		return nil, err
	}
	var bindings []egressauthstore.CredentialBinding
	if record != nil {
		bindings = record.Bindings
	}
	if actual := credentialbinding.DigestStore(bindings); actual != digest {
		return nil, fmt.Errorf("%w: credential binding projection digest changed", ErrSandboxClaimReservationConflict)
	}
	return &NomadSandboxCredentialBindings{Digest: digest, Bindings: credentialbinding.CloneStore(bindings)}, nil
}

func cloneNomadSandboxCredentialBindingsTx(
	ctx context.Context,
	tx pgx.Tx,
	teamID, sourceSandboxID, targetSandboxID string,
) (string, error) {
	source, err := getNomadSandboxCredentialBindingsTx(ctx, tx, teamID, sourceSandboxID)
	if err != nil {
		return "", fmt.Errorf("load source sandbox credential bindings: %w", err)
	}
	if _, err := egressauthstore.ReplaceCurrentBindingsTx(
		ctx, tx, teamID, targetSandboxID, source.Bindings, time.Time{},
	); err != nil {
		return "", fmt.Errorf("clone sandbox credential bindings: %w", err)
	}
	return source.Digest, nil
}

func (t sandboxStoreTx) ReplaceNomadSandboxCredentialBindings(
	ctx context.Context,
	teamID, sandboxID string,
	bindings []egressauthstore.CredentialBinding,
) (string, error) {
	digest := credentialbinding.DigestStore(bindings)
	if _, err := egressauthstore.ReplaceCurrentBindingsTx(ctx, t.tx, teamID, sandboxID, bindings, time.Time{}); err != nil {
		return "", err
	}
	tag, err := t.tx.Exec(ctx, `
		UPDATE manager.sandbox_runtime_claims AS claim
		SET credential_binding_digest = $3
		FROM manager.sandboxes AS sandbox
		WHERE claim.sandbox_id = $1
			AND sandbox.sandbox_id = claim.sandbox_id
			AND sandbox.team_id = $2
			AND sandbox.runtime_backend = $4
			AND sandbox.desired_state = $5
			AND sandbox.deleted_at IS NULL
			AND claim.phase = $6
	`, sandboxID, teamID, digest, SandboxRuntimeBackendNomad,
		SandboxDesiredStatePaused, SandboxRuntimeClaimPhaseReady)
	if err != nil {
		return "", fmt.Errorf("bind paused sandbox credential digest: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return "", fmt.Errorf("%w: paused sandbox credential authority changed", ErrSandboxClaimReservationConflict)
	}
	return digest, nil
}

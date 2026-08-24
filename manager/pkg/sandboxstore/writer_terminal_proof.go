package sandboxstore

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

const (
	// RootFSWriterTerminalProofRetention exceeds the node's 48-hour external
	// crash proof window so delayed or restarted ctld reconcilers can verify a
	// terminal writer after its unreferenced filesystem has been collected.
	RootFSWriterTerminalProofRetention = 72 * time.Hour
	MaxRootFSWriterTerminalProofPrune  = 1000
)

// RootFSWriterTerminalProof is the minimum immutable writer identity retained
// after an unreferenced terminal grant and filesystem are physically deleted.
type RootFSWriterTerminalProof struct {
	GrantID        string
	SandboxID      string
	WriterEpoch    int64
	BindingVersion int
	BindingDigest  []byte
	NodeUID        string
	State          string
	ExpiresAt      time.Time
	CreatedAt      time.Time
}

func (s *PGSandboxStore) GetRootFSWriterTerminalProof(
	ctx context.Context,
	grantID string,
) (*RootFSWriterTerminalProof, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("rootfs writer terminal proof store is not configured")
	}
	grantID = strings.TrimSpace(grantID)
	if grantID == "" {
		return nil, fmt.Errorf("grant ID is required")
	}
	var proof RootFSWriterTerminalProof
	err := s.pool.QueryRow(ctx, `
		SELECT grant_id, sandbox_id, writer_epoch, binding_version,
			binding_digest, node_uid, state, expires_at, created_at
		FROM manager.rootfs_writer_terminal_proofs
		WHERE grant_id = $1 AND expires_at > NOW()
	`, grantID).Scan(
		&proof.GrantID, &proof.SandboxID, &proof.WriterEpoch, &proof.BindingVersion,
		&proof.BindingDigest, &proof.NodeUID, &proof.State, &proof.ExpiresAt, &proof.CreatedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, fmt.Errorf("%w: %s", ErrRootFSWriterGrantNotFound, grantID)
	}
	if err != nil {
		return nil, fmt.Errorf("get rootfs writer terminal proof: %w", err)
	}
	proof.BindingDigest = append([]byte(nil), proof.BindingDigest...)
	return &proof, nil
}

func (s *PGSandboxStore) PruneExpiredRootFSWriterTerminalProofs(ctx context.Context, limit int) (int64, error) {
	if s == nil || s.pool == nil {
		return 0, fmt.Errorf("rootfs writer terminal proof store is not configured")
	}
	if limit <= 0 || limit > MaxRootFSWriterTerminalProofPrune {
		return 0, fmt.Errorf("rootfs writer terminal proof prune limit must be between 1 and %d", MaxRootFSWriterTerminalProofPrune)
	}
	result, err := s.pool.Exec(ctx, `
		DELETE FROM manager.rootfs_writer_terminal_proofs
		WHERE grant_id IN (
			SELECT grant_id
			FROM manager.rootfs_writer_terminal_proofs
			WHERE expires_at <= NOW()
			ORDER BY expires_at, grant_id
			LIMIT $1
		)
	`, limit)
	if err != nil {
		return 0, fmt.Errorf("prune rootfs writer terminal proofs: %w", err)
	}
	return result.RowsAffected(), nil
}

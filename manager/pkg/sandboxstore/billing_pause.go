package sandboxstore

import (
	"context"
	"fmt"
)

// BillingPauseCandidate binds one active sandbox to the current regional
// admission version that requires a checkpoint pause.
type BillingPauseCandidate struct {
	SandboxID        string
	AdmissionVersion int64
}

// ListBillingPauseCandidates scans in bounded keyset order. The pause request
// rechecks this version under the sandbox lock before it changes lifecycle.
func (s *PGSandboxStore) ListBillingPauseCandidates(
	ctx context.Context, clusterID, afterSandboxID string, limit int,
) ([]BillingPauseCandidate, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("sandbox store is not configured")
	}
	if limit <= 0 || limit > 1000 {
		return nil, fmt.Errorf("billing pause batch limit must be between 1 and 1000")
	}
	if clusterID == "" {
		return nil, fmt.Errorf("billing pause cluster id is required")
	}
	// A standalone manager or an older gateway schema has no hosted billing
	// projection. Keep its normal runtime path available during rollout.
	var available bool
	if err := s.pool.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM pg_attribute
			WHERE attrelid = to_regclass('shared_gateway.team_admission_states')
			  AND attname = 'pause_required' AND NOT attisdropped
		)
	`).Scan(&available); err != nil {
		return nil, fmt.Errorf("check billing pause projection: %w", err)
	}
	if !available {
		return nil, nil
	}
	rows, err := s.pool.Query(ctx, `
		SELECT sandbox.sandbox_id, admission.version
		FROM manager.sandboxes AS sandbox
		JOIN shared_gateway.team_admission_states AS admission ON admission.team_id::TEXT = sandbox.team_id
		WHERE sandbox.sandbox_id > $1
		  AND sandbox.cluster_id = $2
		  AND sandbox.deleted_at IS NULL
		  AND sandbox.desired_state = $3
		  AND admission.state = 'restricted'
		  AND admission.pause_required
		ORDER BY sandbox.sandbox_id
		LIMIT $4
	`, afterSandboxID, clusterID, SandboxDesiredStateActive, limit)
	if err != nil {
		return nil, fmt.Errorf("list billing pause candidates: %w", err)
	}
	defer rows.Close()
	var candidates []BillingPauseCandidate
	for rows.Next() {
		var candidate BillingPauseCandidate
		if err := rows.Scan(&candidate.SandboxID, &candidate.AdmissionVersion); err != nil {
			return nil, fmt.Errorf("scan billing pause candidate: %w", err)
		}
		candidates = append(candidates, candidate)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate billing pause candidates: %w", err)
	}
	return candidates, nil
}

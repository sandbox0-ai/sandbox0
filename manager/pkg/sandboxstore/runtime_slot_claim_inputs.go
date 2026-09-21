package sandboxstore

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/sandbox0-ai/sandbox0/pkg/runtimecontrol"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
	v1alpha1 "github.com/sandbox0-ai/sandbox0/pkg/sandboxspec"
)

// RuntimeSlotClaimInputs is an on-demand projection, deliberately excluded
// from heartbeat/inventory rows. Environment and webhook payloads can contain
// secrets and must not be logged or returned through public slot status.
type RuntimeSlotClaimInputs struct {
	Runtime       runtimecontrol.Assignment
	NetworkPolicy string
}

func nullableRuntimeClaimInput(value string) any {
	if value == "" {
		return nil
	}
	return value
}

func validateRuntimeSlotClaimInputs(request *AcquireRuntimeSlotRequest) error {
	if request.RuntimeAssignmentPayload == "" && request.NetworkPolicy == "" {
		// Older claims remain usable, but cannot establish migration input
		// custody retroactively from a possibly changed template.
		return nil
	}
	if len(request.RuntimeAssignmentPayload) == 0 || len(request.RuntimeAssignmentPayload) > protocol.MaxRuntimeAssignmentBytes ||
		len(request.NetworkPolicy) == 0 || len(request.NetworkPolicy) > protocol.MaxNetworkPolicyBytes {
		return fmt.Errorf("%w: runtime launch inputs exceed bounds", ErrRuntimeSlotConflict)
	}
	var assignment runtimecontrol.Assignment
	if json.Unmarshal([]byte(request.RuntimeAssignmentPayload), &assignment) != nil {
		return fmt.Errorf("%w: invalid runtime launch assignment", ErrRuntimeSlotConflict)
	}
	revision, err := assignment.Revision()
	canonical, marshalErr := json.Marshal(assignment)
	if err != nil || marshalErr != nil || string(canonical) != request.RuntimeAssignmentPayload ||
		revision != request.RuntimeAssignmentRevision || assignment.SandboxID != request.SandboxID || assignment.TeamID == "" {
		return fmt.Errorf("%w: runtime launch assignment changed identity", ErrRuntimeSlotConflict)
	}
	policy, err := v1alpha1.ParseNetworkPolicyFromAnnotationStrict(request.NetworkPolicy)
	if err != nil || policy == nil || policy.Version != "v1" || policy.SandboxID != assignment.SandboxID || policy.TeamID != assignment.TeamID ||
		(policy.Mode != v1alpha1.NetworkModeAllowAll && policy.Mode != v1alpha1.NetworkModeBlockAll) ||
		protocol.NetworkPolicyDigest(request.NetworkPolicy) != request.NetworkPolicyDigest {
		return fmt.Errorf("%w: runtime launch policy changed identity", ErrRuntimeSlotConflict)
	}
	return nil
}

func matchRuntimeSlotClaimInputs(ctx context.Context, tx pgx.Tx, slotID string, request *AcquireRuntimeSlotRequest) error {
	if request.RuntimeAssignmentPayload == "" && request.NetworkPolicy == "" {
		return nil
	}
	var matches bool
	if err := tx.QueryRow(ctx, `SELECT COALESCE(claim_runtime_assignment=$2 AND claim_network_policy=$3,false)
        FROM manager.runtime_slots WHERE slot_id=$1`, slotID, request.RuntimeAssignmentPayload, request.NetworkPolicy).Scan(&matches); err != nil {
		return err
	}
	if !matches {
		return fmt.Errorf("%w: claim retry changed retained launch inputs", ErrRuntimeSlotConflict)
	}
	return nil
}

// GetRuntimeSlotClaimInputs returns nil for legacy digest-only claims. Callers
// must recheck active generation, lifecycle, writer and placement when issuing
// migration authority; reading these historical bytes grants no execution.
func (s *PGSandboxStore) GetRuntimeSlotClaimInputs(ctx context.Context, slotID string) (*RuntimeSlotClaimInputs, error) {
	var assignment, policy *string
	var request AcquireRuntimeSlotRequest
	var team string
	err := s.pool.QueryRow(ctx, `SELECT r.claim_runtime_assignment,r.claim_network_policy,r.sandbox_id,s.team_id,
        r.claim_runtime_assignment_revision,r.claim_network_policy_digest
        FROM manager.runtime_slots r JOIN manager.sandboxes s ON s.sandbox_id=r.sandbox_id
        WHERE r.slot_id=$1`, slotID).Scan(&assignment, &policy, &request.SandboxID, &team,
		&request.RuntimeAssignmentRevision, &request.NetworkPolicyDigest)
	if err == pgx.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if assignment == nil || policy == nil {
		return nil, nil
	}
	request.RuntimeAssignmentPayload, request.NetworkPolicy = *assignment, *policy
	if err := validateRuntimeSlotClaimInputs(&request); err != nil {
		return nil, err
	}
	result := &RuntimeSlotClaimInputs{NetworkPolicy: *policy}
	if json.Unmarshal([]byte(*assignment), &result.Runtime) != nil || result.Runtime.TeamID != team {
		return nil, ErrRuntimeSlotConflict
	}
	return result, nil
}

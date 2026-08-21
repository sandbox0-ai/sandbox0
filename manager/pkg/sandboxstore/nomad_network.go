package sandboxstore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshandoff"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
)

const (
	NomadSandboxNetworkMutationPhasePending  = "pending"
	NomadSandboxNetworkMutationPhaseApplied  = "applied"
	NomadSandboxNetworkMutationPhaseCanceled = "canceled"

	MaxNomadSandboxNetworkMutationScan = 1_000
)

var (
	ErrNomadSandboxNetworkMutationConflict = errors.New("nomad sandbox network mutation conflict")
	ErrNomadSandboxNetworkMutationNotReady = errors.New("nomad sandbox network mutation is not ready")
)

// NomadSandboxNetworkMutation is one durable desired/apply/ack transaction
// for an exact active runtime-slot network incarnation. RequestPolicy remains
// unpublished until the node's applied token is committed in PostgreSQL.
type NomadSandboxNetworkMutation struct {
	SandboxID           string
	OperationID         string
	SlotID              string
	SlotRevision        int64
	TeamID              string
	ClusterID           string
	AllocationID        string
	AllocationNamespace string
	NodeID              string
	NodeUID             string
	NodeBootID          string
	NetNSIdentity       string
	ClaimID             string
	CurrentPolicyDigest string
	DesiredPolicy       string
	DesiredPolicyDigest string
	RequestPolicy       *v1alpha1.SandboxNetworkPolicy
	Phase               string
	AppliedPolicyToken  *rootfshandoff.NetworkPolicyToken
	AppliedTokenDigest  []byte
	CancellationReason  string
	AppliedAt           time.Time
	CanceledAt          time.Time
	CreatedAt           time.Time
	UpdatedAt           time.Time
}

// BeginNomadSandboxNetworkMutationRequest binds desired policy bytes to the
// currently applied policy and lets PostgreSQL capture the exact active slot.
type BeginNomadSandboxNetworkMutationRequest struct {
	SandboxID                   string
	OperationID                 string
	ExpectedTeamID              string
	ExpectedCurrentPolicyDigest string
	DesiredPolicy               string
	DesiredPolicyDigest         string
	RequestPolicy               *v1alpha1.SandboxNetworkPolicy
}

// BeginNomadSandboxNetworkMutation stores desired policy without publishing
// it as sandbox config. Same-operation retries recover the existing row;
// another pending operation is serialized as a conflict.
func (s *PGSandboxStore) BeginNomadSandboxNetworkMutation(
	ctx context.Context,
	request *BeginNomadSandboxNetworkMutationRequest,
) (*NomadSandboxNetworkMutation, error) {
	normalized, requestPolicy, err := normalizeBeginNomadSandboxNetworkMutationRequest(request)
	if err != nil {
		return nil, err
	}
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("sandbox store is not configured")
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, fmt.Errorf("begin Nomad sandbox network mutation tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	record, err := lockNomadSandboxClaimRecord(ctx, tx, normalized.SandboxID)
	if err != nil {
		return nil, err
	}
	if record.TeamID != normalized.ExpectedTeamID || record.DesiredState != SandboxDesiredStateActive ||
		!record.DeletedAt.IsZero() {
		return nil, fmt.Errorf("%w: sandbox is not an active team-owned Nomad runtime",
			ErrNomadSandboxNetworkMutationConflict)
	}
	if active, activeErr := getActiveLifecycleTxn(ctx, tx, record.ID); activeErr != nil {
		return nil, activeErr
	} else if active != nil {
		return nil, fmt.Errorf("%w: sandbox lifecycle %s is %s",
			ErrNomadSandboxNetworkMutationConflict, active.Kind, active.Phase)
	}
	slot, err := lockActiveNomadSandboxNetworkSlot(ctx, tx, record)
	if err != nil {
		return nil, err
	}
	if slot.ClaimNetworkPolicyDigest != normalized.ExpectedCurrentPolicyDigest {
		return nil, fmt.Errorf("%w: current policy does not match the active runtime slot",
			ErrNomadSandboxNetworkMutationConflict)
	}

	existing, err := getNomadSandboxNetworkMutationForUpdate(ctx, tx, record.ID)
	if err != nil {
		return nil, err
	}
	if existing != nil && existing.Phase == NomadSandboxNetworkMutationPhasePending {
		if !nomadSandboxNetworkMutationBeginMatches(existing, normalized, requestPolicy, slot) {
			return nil, fmt.Errorf("%w: another policy update is pending",
				ErrNomadSandboxNetworkMutationConflict)
		}
		if err := tx.Commit(ctx); err != nil {
			return nil, fmt.Errorf("commit Nomad sandbox network mutation retry: %w", err)
		}
		return existing, nil
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO manager.sandbox_network_mutations (
			sandbox_id, operation_id, slot_id, slot_revision, team_id,
			cluster_id, allocation_id, allocation_namespace,
			node_id, node_uid, node_boot_id, netns_identity, claim_id,
			current_policy_digest, desired_policy, desired_policy_digest,
			request_policy, phase, created_at, updated_at
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13,
			$14, $15, $16, $17::jsonb, $18, NOW(), NOW()
		)
		ON CONFLICT (sandbox_id) DO UPDATE SET
			operation_id = EXCLUDED.operation_id,
			slot_id = EXCLUDED.slot_id,
			slot_revision = EXCLUDED.slot_revision,
			team_id = EXCLUDED.team_id,
			cluster_id = EXCLUDED.cluster_id,
			allocation_id = EXCLUDED.allocation_id,
			allocation_namespace = EXCLUDED.allocation_namespace,
			node_id = EXCLUDED.node_id,
			node_uid = EXCLUDED.node_uid,
			node_boot_id = EXCLUDED.node_boot_id,
			netns_identity = EXCLUDED.netns_identity,
			claim_id = EXCLUDED.claim_id,
			current_policy_digest = EXCLUDED.current_policy_digest,
			desired_policy = EXCLUDED.desired_policy,
			desired_policy_digest = EXCLUDED.desired_policy_digest,
			request_policy = EXCLUDED.request_policy,
			phase = EXCLUDED.phase,
			applied_policy_token = NULL,
			applied_token_digest = ''::bytea,
			cancellation_reason = '',
			applied_at = NULL,
			canceled_at = NULL,
			created_at = NOW(),
			updated_at = NOW()
	`, record.ID, normalized.OperationID, slot.ID, slot.Revision, record.TeamID,
		slot.ClusterID, slot.AllocationID, slot.AllocationNamespace,
		slot.NodeID, slot.NodeUID, slot.NodeBootID, slot.NetNSIdentity, slot.ClaimID,
		normalized.ExpectedCurrentPolicyDigest, normalized.DesiredPolicy,
		normalized.DesiredPolicyDigest, string(requestPolicy),
		NomadSandboxNetworkMutationPhasePending)
	if err != nil {
		return nil, mapNomadSandboxNetworkMutationConflict("persist Nomad sandbox network mutation", err)
	}
	mutation, err := getNomadSandboxNetworkMutationForUpdate(ctx, tx, record.ID)
	if err != nil {
		return nil, err
	}
	if mutation == nil {
		return nil, fmt.Errorf("persisted Nomad sandbox network mutation is missing")
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, mapNomadSandboxNetworkMutationConflict("commit Nomad sandbox network mutation", err)
	}
	return mutation, nil
}

// PrepareNomadSandboxNetworkMutation returns the exact pending operation after
// rechecking every runtime owner under locks. Lifecycle or allocation drift
// cancels the desired policy before any new node command is dispatched.
func (s *PGSandboxStore) PrepareNomadSandboxNetworkMutation(
	ctx context.Context,
	sandboxID string,
) (*NomadSandboxNetworkMutation, error) {
	sandboxID = strings.TrimSpace(sandboxID)
	if sandboxID == "" || len(sandboxID) > 512 {
		return nil, fmt.Errorf("sandbox_id is required and must not exceed 512 bytes")
	}
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("sandbox store is not configured")
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, fmt.Errorf("begin Nomad sandbox network preparation tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	record, err := lockNomadSandboxClaimRecord(ctx, tx, sandboxID)
	if err != nil {
		return nil, err
	}
	mutation, err := getNomadSandboxNetworkMutation(ctx, tx, sandboxID)
	if err != nil || mutation == nil {
		return nil, err
	}
	if mutation.Phase != NomadSandboxNetworkMutationPhasePending {
		if err := tx.Commit(ctx); err != nil {
			return nil, err
		}
		return mutation, nil
	}
	slot, slotErr := lockRuntimeSlotByID(ctx, tx, mutation.SlotID)
	mutation, err = getNomadSandboxNetworkMutationForUpdate(ctx, tx, sandboxID)
	if err != nil {
		return nil, err
	}
	if slotErr != nil && !errors.Is(slotErr, ErrRuntimeSlotNotFound) {
		return nil, slotErr
	}
	cancelReason, err := nomadSandboxNetworkMutationCancellationReason(ctx, tx, record, slot, slotErr, mutation)
	if err != nil {
		return nil, err
	}
	if cancelReason != "" {
		if err := cancelNomadSandboxNetworkMutation(ctx, tx, mutation.OperationID, cancelReason); err != nil {
			return nil, err
		}
		mutation, err = getNomadSandboxNetworkMutationForUpdate(ctx, tx, sandboxID)
		if err != nil {
			return nil, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit Nomad sandbox network preparation: %w", err)
	}
	return mutation, nil
}

// CommitNomadSandboxNetworkMutation publishes request config only after the
// exact node token is revalidated and committed with the runtime-slot digest.
func (s *PGSandboxStore) CommitNomadSandboxNetworkMutation(
	ctx context.Context,
	sandboxID, operationID string,
	token rootfshandoff.NetworkPolicyToken,
) (*NomadSandboxNetworkMutation, error) {
	sandboxID = strings.TrimSpace(sandboxID)
	operationID = strings.TrimSpace(operationID)
	if sandboxID == "" || operationID == "" || len(sandboxID) > 512 || len(operationID) > 512 {
		return nil, fmt.Errorf("sandbox_id and operation_id are required and must not exceed 512 bytes")
	}
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("sandbox store is not configured")
	}
	tokenPayload, tokenDigest, err := canonicalNomadSandboxNetworkPolicyToken(token)
	if err != nil {
		return nil, err
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, fmt.Errorf("begin Nomad sandbox network commit tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	record, err := lockNomadSandboxClaimRecord(ctx, tx, sandboxID)
	if err != nil {
		return nil, err
	}
	mutation, err := getNomadSandboxNetworkMutation(ctx, tx, sandboxID)
	if err != nil || mutation == nil {
		if err == nil {
			err = fmt.Errorf("%w: mutation is missing", ErrNomadSandboxNetworkMutationConflict)
		}
		return nil, err
	}
	slot, slotErr := lockRuntimeSlotByID(ctx, tx, mutation.SlotID)
	mutation, err = getNomadSandboxNetworkMutationForUpdate(ctx, tx, sandboxID)
	if err != nil {
		return nil, err
	}
	if slotErr != nil && !errors.Is(slotErr, ErrRuntimeSlotNotFound) {
		return nil, slotErr
	}
	if mutation.OperationID != operationID {
		return nil, fmt.Errorf("%w: operation identity changed", ErrNomadSandboxNetworkMutationConflict)
	}
	if mutation.Phase == NomadSandboxNetworkMutationPhaseApplied {
		if !bytes.Equal(mutation.AppliedTokenDigest, tokenDigest) || mutation.AppliedPolicyToken == nil {
			return nil, fmt.Errorf("%w: applied token changed", ErrNomadSandboxNetworkMutationConflict)
		}
		if err := tx.Commit(ctx); err != nil {
			return nil, err
		}
		return mutation, nil
	}
	if mutation.Phase != NomadSandboxNetworkMutationPhasePending {
		return nil, fmt.Errorf("%w: mutation is %s", ErrNomadSandboxNetworkMutationConflict, mutation.Phase)
	}
	reason, err := nomadSandboxNetworkMutationCancellationReason(ctx, tx, record, slot, slotErr, mutation)
	if err != nil {
		return nil, err
	}
	if reason != "" {
		if cancelErr := cancelNomadSandboxNetworkMutation(ctx, tx, operationID, reason); cancelErr != nil {
			return nil, cancelErr
		}
		if err := tx.Commit(ctx); err != nil {
			return nil, err
		}
		return nil, fmt.Errorf("%w: %s", ErrNomadSandboxNetworkMutationConflict, reason)
	}
	if err := validateNomadSandboxNetworkPolicyToken(token, mutation); err != nil {
		return nil, err
	}

	tag, err := tx.Exec(ctx, `
		UPDATE manager.runtime_slots
		SET claim_network_policy_digest = $2,
			revision = revision + 1,
			updated_at = NOW()
		WHERE slot_id = $1 AND revision = $3 AND state = $4
	`, mutation.SlotID, mutation.DesiredPolicyDigest, mutation.SlotRevision, RuntimeSlotStateActive)
	if err != nil {
		return nil, fmt.Errorf("publish runtime slot network policy: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return nil, fmt.Errorf("%w: runtime slot changed during network acknowledgement",
			ErrNomadSandboxNetworkMutationConflict)
	}
	tag, err = tx.Exec(ctx, `
		UPDATE manager.sandboxes
		SET config = jsonb_set(config, '{network}', $2::jsonb, TRUE),
			updated_at = NOW()
		WHERE sandbox_id = $1 AND runtime_backend = $3
			AND desired_state = $4 AND deleted_at IS NULL
	`, sandboxID, string(mustMarshalNetworkPolicy(mutation.RequestPolicy)),
		SandboxRuntimeBackendNomad, SandboxDesiredStateActive)
	if err != nil {
		return nil, fmt.Errorf("publish Nomad sandbox network config: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return nil, fmt.Errorf("%w: sandbox changed during network acknowledgement",
			ErrNomadSandboxNetworkMutationConflict)
	}
	tag, err = tx.Exec(ctx, `
		UPDATE manager.sandbox_network_mutations
		SET phase = $2,
			applied_policy_token = $3::jsonb,
			applied_token_digest = $4,
			applied_at = NOW(),
			updated_at = NOW()
		WHERE sandbox_id = $1 AND operation_id = $5 AND phase = $6
	`, sandboxID, NomadSandboxNetworkMutationPhaseApplied, string(tokenPayload), tokenDigest,
		operationID, NomadSandboxNetworkMutationPhasePending)
	if err != nil {
		return nil, fmt.Errorf("acknowledge Nomad sandbox network mutation: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return nil, fmt.Errorf("%w: mutation changed during acknowledgement",
			ErrNomadSandboxNetworkMutationConflict)
	}
	result, err := getNomadSandboxNetworkMutationForUpdate(ctx, tx, sandboxID)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, mapNomadSandboxNetworkMutationConflict("commit Nomad sandbox network acknowledgement", err)
	}
	return result, nil
}

// ListPendingNomadSandboxNetworkMutations feeds a plugin-independent retry
// controller. Operations stay discoverable across manager replica loss.
func (s *PGSandboxStore) ListPendingNomadSandboxNetworkMutations(
	ctx context.Context,
	limit int,
) ([]*NomadSandboxNetworkMutation, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("sandbox store is not configured")
	}
	if limit <= 0 || limit > MaxNomadSandboxNetworkMutationScan {
		return nil, fmt.Errorf("Nomad sandbox network mutation limit must be between 1 and %d",
			MaxNomadSandboxNetworkMutationScan)
	}
	rows, err := s.pool.Query(ctx, nomadSandboxNetworkMutationSelectSQL()+`
		WHERE phase = $1
		ORDER BY updated_at, sandbox_id
		LIMIT $2
	`, NomadSandboxNetworkMutationPhasePending, limit)
	if err != nil {
		return nil, fmt.Errorf("list pending Nomad sandbox network mutations: %w", err)
	}
	defer rows.Close()
	result := make([]*NomadSandboxNetworkMutation, 0, limit)
	for rows.Next() {
		mutation, scanErr := scanNomadSandboxNetworkMutation(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		result = append(result, mutation)
	}
	return result, rows.Err()
}

// GetNomadSandboxNetworkMutation returns the latest transaction retained for
// one sandbox. It is primarily useful for observability and exact tests.
func (s *PGSandboxStore) GetNomadSandboxNetworkMutation(
	ctx context.Context,
	sandboxID string,
) (*NomadSandboxNetworkMutation, error) {
	sandboxID = strings.TrimSpace(sandboxID)
	if sandboxID == "" {
		return nil, fmt.Errorf("sandbox_id is required")
	}
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("sandbox store is not configured")
	}
	return getNomadSandboxNetworkMutation(ctx, s.pool, sandboxID)
}

func lockActiveNomadSandboxNetworkSlot(ctx context.Context, tx pgx.Tx, record *SandboxRecord) (*RuntimeSlot, error) {
	if record == nil || record.CurrentPodName == "" || record.CurrentPodNamespace == "" {
		return nil, fmt.Errorf("%w: active sandbox allocation is missing", ErrNomadSandboxNetworkMutationNotReady)
	}
	slot, err := scanRuntimeSlot(tx.QueryRow(ctx, runtimeSlotSelectSQL()+`
		WHERE sandbox_id = $1 AND state <> $2
		FOR UPDATE
	`, record.ID, RuntimeSlotStateTerminal))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, fmt.Errorf("%w: active runtime slot is missing", ErrNomadSandboxNetworkMutationNotReady)
	}
	if err != nil {
		return nil, err
	}
	if !nomadSandboxNetworkSlotMatchesRecord(slot, record) {
		return nil, fmt.Errorf("%w: active runtime slot identity changed", ErrNomadSandboxNetworkMutationNotReady)
	}
	return slot, nil
}

func lockRuntimeSlotByID(ctx context.Context, tx pgx.Tx, slotID string) (*RuntimeSlot, error) {
	slot, err := scanRuntimeSlot(tx.QueryRow(ctx, runtimeSlotSelectSQL()+`
		WHERE slot_id = $1 FOR UPDATE
	`, slotID))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrRuntimeSlotNotFound
	}
	return slot, err
}

func nomadSandboxNetworkSlotMatchesRecord(slot *RuntimeSlot, record *SandboxRecord) bool {
	return slot != nil && record != nil && slot.State == RuntimeSlotStateActive &&
		slot.SandboxID == record.ID && slot.AllocationID == record.CurrentPodName &&
		slot.AllocationNamespace == record.CurrentPodNamespace && slot.ClaimID != "" &&
		slot.ClusterID != "" && slot.NodeID != "" && slot.NodeUID != "" &&
		slot.NodeBootID != "" && slot.NetNSIdentity != ""
}

func nomadSandboxNetworkMutationCancellationReason(
	ctx context.Context,
	tx pgx.Tx,
	record *SandboxRecord,
	slot *RuntimeSlot,
	slotErr error,
	mutation *NomadSandboxNetworkMutation,
) (string, error) {
	if record == nil || mutation == nil || record.RuntimeBackend != SandboxRuntimeBackendNomad ||
		record.DesiredState != SandboxDesiredStateActive || !record.DeletedAt.IsZero() {
		return "sandbox is no longer active", nil
	}
	if active, err := getActiveLifecycleTxn(ctx, tx, record.ID); err != nil {
		return "", fmt.Errorf("verify sandbox lifecycle before network mutation: %w", err)
	} else if active != nil {
		return "sandbox lifecycle preempted network update", nil
	}
	if slotErr != nil || !nomadSandboxNetworkSlotMatchesRecord(slot, record) ||
		!nomadSandboxNetworkMutationSlotMatches(mutation, slot) {
		return "runtime slot incarnation changed", nil
	}
	if slot.Revision != mutation.SlotRevision ||
		slot.ClaimNetworkPolicyDigest != mutation.CurrentPolicyDigest {
		return "runtime slot policy authority changed", nil
	}
	return "", nil
}

func cancelNomadSandboxNetworkMutation(ctx context.Context, tx pgx.Tx, operationID, reason string) error {
	reason = strings.TrimSpace(reason)
	if len(reason) > 1024 {
		reason = reason[:1024]
	}
	tag, err := tx.Exec(ctx, `
		UPDATE manager.sandbox_network_mutations
		SET phase = $2,
			cancellation_reason = $3,
			canceled_at = NOW(),
			updated_at = NOW()
		WHERE operation_id = $1 AND phase = $4
	`, operationID, NomadSandboxNetworkMutationPhaseCanceled, reason,
		NomadSandboxNetworkMutationPhasePending)
	if err != nil {
		return fmt.Errorf("cancel Nomad sandbox network mutation: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("%w: pending mutation changed during cancellation",
			ErrNomadSandboxNetworkMutationConflict)
	}
	return nil
}

func cancelPendingNomadSandboxNetworkMutationForSandbox(
	ctx context.Context,
	exec rootFSStateExecutor,
	sandboxID, reason string,
) error {
	reason = strings.TrimSpace(reason)
	if reason == "" {
		return fmt.Errorf("Nomad sandbox network cancellation reason is required")
	}
	if len(reason) > 1024 {
		reason = reason[:1024]
	}
	_, err := exec.Exec(ctx, `
		UPDATE manager.sandbox_network_mutations
		SET phase = $2,
			cancellation_reason = $3,
			canceled_at = NOW(),
			updated_at = NOW()
		WHERE sandbox_id = $1 AND phase = $4
	`, sandboxID, NomadSandboxNetworkMutationPhaseCanceled, reason,
		NomadSandboxNetworkMutationPhasePending)
	if err != nil {
		return fmt.Errorf("preempt pending Nomad sandbox network mutation: %w", err)
	}
	return nil
}

func validateNomadSandboxNetworkPolicyToken(
	token rootfshandoff.NetworkPolicyToken,
	mutation *NomadSandboxNetworkMutation,
) error {
	if err := token.Validate(); err != nil {
		return fmt.Errorf("applied network policy token: %w", err)
	}
	if mutation == nil || token.PodUID != mutation.AllocationID || token.ClaimID != mutation.ClaimID ||
		token.NetNSIdentity != mutation.NetNSIdentity || token.PolicyDigest != mutation.DesiredPolicyDigest ||
		token.PodSandboxID != protocol.RuntimeSlotNetworkIncarnationID(protocol.NodeNetworkPrepareControlRequest{
			SlotID: mutation.SlotID, ClusterID: mutation.ClusterID,
			AllocationID: mutation.AllocationID, NodeID: mutation.NodeID,
			NodeUID: mutation.NodeUID, NodeBootID: mutation.NodeBootID,
			NetNSIdentity: mutation.NetNSIdentity,
		}) {
		return fmt.Errorf("%w: applied token belongs to another runtime slot",
			ErrNomadSandboxNetworkMutationConflict)
	}
	if _, err := protocol.NomadProcdAddress(token.PodIP); err != nil {
		return fmt.Errorf("applied network policy token: %w", err)
	}
	return nil
}

func canonicalNomadSandboxNetworkPolicyToken(token rootfshandoff.NetworkPolicyToken) ([]byte, []byte, error) {
	if err := token.Validate(); err != nil {
		return nil, nil, fmt.Errorf("applied network policy token: %w", err)
	}
	payload, err := json.Marshal(token)
	if err != nil {
		return nil, nil, fmt.Errorf("marshal applied network policy token: %w", err)
	}
	digest := sha256.Sum256(payload)
	return payload, digest[:], nil
}

func normalizeBeginNomadSandboxNetworkMutationRequest(
	request *BeginNomadSandboxNetworkMutationRequest,
) (*BeginNomadSandboxNetworkMutationRequest, []byte, error) {
	if request == nil || request.RequestPolicy == nil {
		return nil, nil, fmt.Errorf("Nomad sandbox network mutation request and request policy are required")
	}
	normalized := *request
	for name, value := range map[string]string{
		"sandbox_id":       normalized.SandboxID,
		"operation_id":     normalized.OperationID,
		"expected_team_id": normalized.ExpectedTeamID,
	} {
		if value == "" || value != strings.TrimSpace(value) || len(value) > 512 {
			return nil, nil, fmt.Errorf("%s is required, canonical, and at most 512 bytes", name)
		}
	}
	if len(normalized.DesiredPolicy) == 0 || len(normalized.DesiredPolicy) > protocol.MaxNetworkPolicyBytes {
		return nil, nil, fmt.Errorf("desired policy must contain 1..%d bytes", protocol.MaxNetworkPolicyBytes)
	}
	if normalized.DesiredPolicyDigest != protocol.NetworkPolicyDigest(normalized.DesiredPolicy) {
		return nil, nil, fmt.Errorf("desired policy digest does not match policy bytes")
	}
	policySpec, err := v1alpha1.ParseNetworkPolicyFromAnnotationStrict(normalized.DesiredPolicy)
	if err != nil || policySpec == nil || policySpec.Version != "v1" ||
		policySpec.SandboxID != normalized.SandboxID || policySpec.TeamID != normalized.ExpectedTeamID ||
		(policySpec.Mode != v1alpha1.NetworkModeAllowAll && policySpec.Mode != v1alpha1.NetworkModeBlockAll) {
		return nil, nil, fmt.Errorf("desired policy must be v1 and match the requested sandbox and team")
	}
	for name, value := range map[string]string{
		"expected_current_policy_digest": normalized.ExpectedCurrentPolicyDigest,
		"desired_policy_digest":          normalized.DesiredPolicyDigest,
	} {
		if _, err := normalizeRuntimeSlotDigest(name, value); err != nil {
			return nil, nil, err
		}
	}
	requestPolicy, err := json.Marshal(normalized.RequestPolicy)
	if err != nil {
		return nil, nil, fmt.Errorf("marshal requested network policy: %w", err)
	}
	if len(requestPolicy) == 0 || len(requestPolicy) > protocol.MaxNetworkPolicyBytes {
		return nil, nil, fmt.Errorf("requested network policy must contain 1..%d bytes", protocol.MaxNetworkPolicyBytes)
	}
	var canonical v1alpha1.SandboxNetworkPolicy
	if err := json.Unmarshal(requestPolicy, &canonical); err != nil {
		return nil, nil, fmt.Errorf("canonicalize requested network policy: %w", err)
	}
	normalized.RequestPolicy = &canonical
	return &normalized, requestPolicy, nil
}

func nomadSandboxNetworkMutationBeginMatches(
	mutation *NomadSandboxNetworkMutation,
	request *BeginNomadSandboxNetworkMutationRequest,
	requestPolicy []byte,
	slot *RuntimeSlot,
) bool {
	storedPolicy := mustMarshalNetworkPolicy(mutation.RequestPolicy)
	return mutation != nil && request != nil && slot != nil &&
		mutation.OperationID == request.OperationID && mutation.SandboxID == request.SandboxID &&
		mutation.TeamID == request.ExpectedTeamID &&
		mutation.CurrentPolicyDigest == request.ExpectedCurrentPolicyDigest &&
		mutation.DesiredPolicy == request.DesiredPolicy &&
		mutation.DesiredPolicyDigest == request.DesiredPolicyDigest &&
		bytes.Equal(storedPolicy, requestPolicy) && nomadSandboxNetworkMutationSlotMatches(mutation, slot) &&
		mutation.SlotRevision == slot.Revision
}

func nomadSandboxNetworkMutationSlotMatches(mutation *NomadSandboxNetworkMutation, slot *RuntimeSlot) bool {
	return mutation != nil && slot != nil && mutation.SlotID == slot.ID &&
		mutation.ClusterID == slot.ClusterID && mutation.AllocationID == slot.AllocationID &&
		mutation.AllocationNamespace == slot.AllocationNamespace && mutation.NodeID == slot.NodeID &&
		mutation.NodeUID == slot.NodeUID && mutation.NodeBootID == slot.NodeBootID &&
		mutation.NetNSIdentity == slot.NetNSIdentity && mutation.ClaimID == slot.ClaimID &&
		mutation.SandboxID == slot.SandboxID
}

func mustMarshalNetworkPolicy(policy *v1alpha1.SandboxNetworkPolicy) []byte {
	payload, _ := json.Marshal(policy)
	return payload
}

func nomadSandboxNetworkMutationSelectSQL() string {
	return `
		SELECT sandbox_id, operation_id, slot_id, slot_revision, team_id,
			cluster_id, allocation_id, allocation_namespace,
			node_id, node_uid, node_boot_id, netns_identity, claim_id,
			current_policy_digest, desired_policy, desired_policy_digest,
			request_policy, phase, applied_policy_token, applied_token_digest,
			cancellation_reason, applied_at, canceled_at, created_at, updated_at
		FROM manager.sandbox_network_mutations`
}

type nomadSandboxNetworkMutationScanner interface {
	Scan(...any) error
}

func scanNomadSandboxNetworkMutation(scanner nomadSandboxNetworkMutationScanner) (*NomadSandboxNetworkMutation, error) {
	var mutation NomadSandboxNetworkMutation
	var requestPolicy, appliedToken []byte
	var appliedAt, canceledAt pgtype.Timestamptz
	if err := scanner.Scan(
		&mutation.SandboxID, &mutation.OperationID, &mutation.SlotID, &mutation.SlotRevision,
		&mutation.TeamID, &mutation.ClusterID, &mutation.AllocationID, &mutation.AllocationNamespace,
		&mutation.NodeID, &mutation.NodeUID, &mutation.NodeBootID, &mutation.NetNSIdentity,
		&mutation.ClaimID, &mutation.CurrentPolicyDigest, &mutation.DesiredPolicy,
		&mutation.DesiredPolicyDigest, &requestPolicy, &mutation.Phase, &appliedToken,
		&mutation.AppliedTokenDigest, &mutation.CancellationReason, &appliedAt, &canceledAt,
		&mutation.CreatedAt, &mutation.UpdatedAt,
	); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("scan Nomad sandbox network mutation: %w", err)
	}
	if err := json.Unmarshal(requestPolicy, &mutation.RequestPolicy); err != nil || mutation.RequestPolicy == nil {
		return nil, fmt.Errorf("decode requested network policy: %w", err)
	}
	if len(appliedToken) > 0 {
		var token rootfshandoff.NetworkPolicyToken
		if err := json.Unmarshal(appliedToken, &token); err != nil {
			return nil, fmt.Errorf("decode applied network policy token: %w", err)
		}
		mutation.AppliedPolicyToken = &token
	}
	mutation.AppliedTokenDigest = append([]byte(nil), mutation.AppliedTokenDigest...)
	if appliedAt.Valid {
		mutation.AppliedAt = appliedAt.Time
	}
	if canceledAt.Valid {
		mutation.CanceledAt = canceledAt.Time
	}
	return &mutation, nil
}

func getNomadSandboxNetworkMutation(
	ctx context.Context,
	exec interface {
		QueryRow(context.Context, string, ...any) pgx.Row
	},
	sandboxID string,
) (*NomadSandboxNetworkMutation, error) {
	return scanNomadSandboxNetworkMutation(exec.QueryRow(ctx,
		nomadSandboxNetworkMutationSelectSQL()+` WHERE sandbox_id = $1`, sandboxID))
}

func getNomadSandboxNetworkMutationForUpdate(
	ctx context.Context,
	tx pgx.Tx,
	sandboxID string,
) (*NomadSandboxNetworkMutation, error) {
	return scanNomadSandboxNetworkMutation(tx.QueryRow(ctx,
		nomadSandboxNetworkMutationSelectSQL()+` WHERE sandbox_id = $1 FOR UPDATE`, sandboxID))
}

func mapNomadSandboxNetworkMutationConflict(operation string, err error) error {
	mapped := mapRuntimeSlotConflict(operation, err)
	if errors.Is(mapped, ErrRuntimeSlotConflict) {
		return fmt.Errorf("%w: %v", ErrNomadSandboxNetworkMutationConflict, mapped)
	}
	return mapped
}

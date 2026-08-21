package service

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/networkpolicy"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/runtimeslotclaim"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	"github.com/sandbox0-ai/sandbox0/pkg/managerapi"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshandoff"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// NomadSandboxNetworkPolicyService owns the durable paused policy that is
// consumed by the next exact runtime-slot claim.
type NomadSandboxNetworkPolicyService struct {
	store         NomadSandboxMutationStore
	mutationStore NomadSandboxNetworkMutationStore
	policies      *networkpolicy.NetworkPolicyService
	preparer      runtimeslotclaim.NetworkPreparer
	enqueuerMu    sync.RWMutex
	enqueuer      NomadSandboxNetworkMutationEnqueuer
}

// NomadSandboxNetworkMutationStore is the PostgreSQL desired/apply/ack
// authority required for active allocation policy updates.
type NomadSandboxNetworkMutationStore interface {
	NomadSandboxMutationStore
	BeginNomadSandboxNetworkMutation(context.Context, *sandboxstore.BeginNomadSandboxNetworkMutationRequest) (*sandboxstore.NomadSandboxNetworkMutation, error)
	PrepareNomadSandboxNetworkMutation(context.Context, string) (*sandboxstore.NomadSandboxNetworkMutation, error)
	CommitNomadSandboxNetworkMutation(context.Context, string, string, rootfshandoff.NetworkPolicyToken) (*sandboxstore.NomadSandboxNetworkMutation, error)
	ListPendingNomadSandboxNetworkMutations(context.Context, int) ([]*sandboxstore.NomadSandboxNetworkMutation, error)
}

// NomadSandboxNetworkMutationEnqueuer requests immediate background retry
// after an API-scoped synchronous attempt becomes ambiguous or unavailable.
type NomadSandboxNetworkMutationEnqueuer interface {
	EnqueueSandboxNetworkMutation(string)
}

// NewNomadSandboxNetworkPolicyService creates a policy service that never
// mutates a Kubernetes runtime object.
func NewNomadSandboxNetworkPolicyService(
	store NomadSandboxMutationStore,
	policies *networkpolicy.NetworkPolicyService,
	preparers ...runtimeslotclaim.NetworkPreparer,
) (*NomadSandboxNetworkPolicyService, error) {
	if store == nil || policies == nil {
		return nil, fmt.Errorf("Nomad sandbox store and network policy builder are required")
	}
	var preparer runtimeslotclaim.NetworkPreparer
	if len(preparers) > 1 {
		return nil, fmt.Errorf("only one Nomad network preparer may be configured")
	}
	if len(preparers) == 1 {
		preparer = preparers[0]
	}
	mutationStore, _ := store.(NomadSandboxNetworkMutationStore)
	return &NomadSandboxNetworkPolicyService{
		store: store, mutationStore: mutationStore, policies: policies, preparer: preparer,
	}, nil
}

// SetMutationEnqueuer installs the process-local fast retry path. Durable
// discovery does not depend on this hook.
func (s *NomadSandboxNetworkPolicyService) SetMutationEnqueuer(enqueuer NomadSandboxNetworkMutationEnqueuer) {
	if s == nil {
		return
	}
	s.enqueuerMu.Lock()
	s.enqueuer = enqueuer
	s.enqueuerMu.Unlock()
}

// SupportsNetworkPolicy reports that durable Nomad network policy is enabled.
func (s *NomadSandboxNetworkPolicyService) SupportsNetworkPolicy() bool {
	return s != nil && s.store != nil && s.policies != nil
}

// GetNetworkPolicy returns the effective durable policy. Active sandboxes are
// exposed only when the exact slot digest matches that policy.
func (s *NomadSandboxNetworkPolicyService) GetNetworkPolicy(
	ctx context.Context,
	sandboxID string,
) (*v1alpha1.SandboxNetworkPolicy, error) {
	sandboxID = strings.TrimSpace(sandboxID)
	record, err := s.store.GetSandbox(ctx, sandboxID)
	if err != nil {
		return nil, err
	}
	if err := validateNomadNetworkPolicyRecord(record, sandboxID); err != nil {
		return nil, err
	}
	state, annotation, err := s.buildPolicy(record, record.Config.Network)
	if err != nil {
		return nil, err
	}
	if record.DesiredState == sandboxstore.SandboxDesiredStateActive {
		slot, err := s.store.GetRuntimeSlotBySandboxID(ctx, sandboxID)
		if err != nil {
			if errors.Is(err, sandboxstore.ErrRuntimeSlotNotFound) {
				return nil, fmt.Errorf("%w: active Nomad sandbox has no runtime slot", ErrDataPlaneNotReady)
			}
			return nil, err
		}
		projected, projectionErr := projectNomadSandboxSlot(record, nil, sandboxRecordToSandbox(record), slot)
		if projectionErr != nil || projected.Status != managerapi.SandboxStatusRunning ||
			slot == nil || slot.SandboxID != sandboxID || slot.State != sandboxstore.RuntimeSlotStateActive ||
			slot.AllocationID != record.CurrentPodName || slot.AllocationNamespace != record.CurrentPodNamespace ||
			slot.ClaimNetworkPolicyDigest != protocol.NetworkPolicyDigest(annotation) {
			return nil, fmt.Errorf("%w: active Nomad network policy does not match the exact runtime slot", ErrDataPlaneNotReady)
		}
	}
	return sandboxNetworkPolicyFromState(state), nil
}

// UpdateNetworkPolicy persists a policy only while the sandbox is paused.
// In-place active updates remain unavailable until they have a durable
// runtime-slot desired/apply/ack transaction.
func (s *NomadSandboxNetworkPolicyService) UpdateNetworkPolicy(
	ctx context.Context,
	sandboxID string,
	policy *v1alpha1.SandboxNetworkPolicy,
) (*v1alpha1.SandboxNetworkPolicy, error) {
	if policy == nil {
		return nil, fmt.Errorf("network policy is required")
	}
	sandboxID = strings.TrimSpace(sandboxID)
	record, err := s.store.GetSandbox(ctx, sandboxID)
	if err != nil {
		return nil, err
	}
	if err := validateNomadNetworkPolicyRecord(record, sandboxID); err != nil {
		return nil, err
	}
	if record.DesiredState == sandboxstore.SandboxDesiredStateActive {
		return s.updateActiveNetworkPolicy(ctx, record, policy)
	}
	var effective *networkpolicy.BuildNetworkPolicyResult
	err = s.store.WithSandboxLock(ctx, sandboxID, func(lockCtx context.Context, tx sandboxstore.SandboxStoreTx, record *sandboxstore.SandboxRecord) error {
		if err := validateNomadNetworkPolicyRecord(record, sandboxID); err != nil {
			return err
		}
		if record.DesiredState == sandboxstore.SandboxDesiredStateActive {
			return apierrors.NewConflict(schema.GroupResource{Resource: "sandbox"}, sandboxID,
				fmt.Errorf("sandbox resumed while updating its paused network policy"))
		}
		activeTxn, err := tx.GetActiveLifecycleTxn(lockCtx, sandboxID)
		if err != nil {
			return err
		}
		if activeTxn != nil {
			return apierrors.NewConflict(schema.GroupResource{Resource: "sandbox"}, sandboxID,
				fmt.Errorf("sandbox lifecycle %s is %s", activeTxn.Kind, activeTxn.Phase))
		}
		request := s.buildRequest(record, policy)
		if err := s.policies.ValidateNetworkPolicyRequest(request); err != nil {
			return fmt.Errorf("%w: %v", ErrInvalidNetworkPolicy, err)
		}
		effective = s.policies.BuildNetworkPolicyState(request)
		if effective == nil || effective.PolicySpec == nil {
			return fmt.Errorf("build Nomad network policy")
		}
		if len(effective.CredentialBindings) > 0 {
			return fmt.Errorf("%w: Nomad credential binding projection is not configured", ErrSandboxRuntimeUpdateUnavailable)
		}
		updated := cloneSandboxRecordForLifecycle(record)
		config := CloneSandboxConfig(&record.Config)
		if config == nil {
			config = &sandboxstore.SandboxConfig{}
		}
		config.Network = sanitizedNetworkPolicyForPersistence(policy)
		updated.Config = *config
		return tx.SaveSandbox(lockCtx, updated)
	})
	if err != nil {
		return nil, fmt.Errorf("update Nomad sandbox network policy: %w", err)
	}
	return sandboxNetworkPolicyFromState(effective), nil
}

func (s *NomadSandboxNetworkPolicyService) updateActiveNetworkPolicy(
	ctx context.Context,
	record *sandboxstore.SandboxRecord,
	policy *v1alpha1.SandboxNetworkPolicy,
) (*v1alpha1.SandboxNetworkPolicy, error) {
	if s == nil || s.mutationStore == nil || s.preparer == nil {
		return nil, fmt.Errorf("%w: active Nomad network mutation authority is not configured",
			ErrSandboxRuntimeUpdateUnavailable)
	}
	request := s.buildRequest(record, policy)
	if err := s.policies.ValidateNetworkPolicyRequest(request); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidNetworkPolicy, err)
	}
	effective := s.policies.BuildNetworkPolicyState(request)
	if effective == nil || effective.PolicySpec == nil {
		return nil, fmt.Errorf("build Nomad network policy")
	}
	if len(effective.CredentialBindings) > 0 {
		return nil, fmt.Errorf("%w: Nomad credential binding projection is not configured",
			ErrSandboxRuntimeUpdateUnavailable)
	}
	desiredPolicy, err := v1alpha1.NetworkPolicyToAnnotation(effective.PolicySpec)
	if err != nil {
		return nil, fmt.Errorf("serialize desired Nomad network policy: %w", err)
	}
	if len(desiredPolicy) > protocol.MaxNetworkPolicyBytes {
		return nil, fmt.Errorf("%w: effective network policy exceeds %d bytes",
			ErrInvalidNetworkPolicy, protocol.MaxNetworkPolicyBytes)
	}
	_, currentPolicy, err := s.buildPolicy(record, record.Config.Network)
	if err != nil {
		return nil, err
	}
	currentDigest := protocol.NetworkPolicyDigest(currentPolicy)
	slot, err := s.store.GetRuntimeSlotBySandboxID(ctx, record.ID)
	if err != nil {
		return nil, fmt.Errorf("%w: load active Nomad runtime slot: %v", ErrDataPlaneNotReady, err)
	}
	if !activeNomadNetworkSlotMatchesRecord(slot, record) ||
		slot.ClaimNetworkPolicyDigest != currentDigest {
		return nil, fmt.Errorf("%w: active Nomad network policy does not match the exact runtime slot",
			ErrDataPlaneNotReady)
	}
	desiredRequest := sanitizedNetworkPolicyForPersistence(policy)
	currentRequestPayload, err := json.Marshal(record.Config.Network)
	if err != nil {
		return nil, fmt.Errorf("serialize current Nomad network request: %w", err)
	}
	desiredRequestPayload, err := json.Marshal(desiredRequest)
	if err != nil {
		return nil, fmt.Errorf("serialize desired Nomad network request: %w", err)
	}
	if len(desiredRequestPayload) > protocol.MaxNetworkPolicyBytes {
		return nil, fmt.Errorf("%w: requested network policy exceeds %d bytes",
			ErrInvalidNetworkPolicy, protocol.MaxNetworkPolicyBytes)
	}
	if bytes.Equal(currentRequestPayload, desiredRequestPayload) {
		return sandboxNetworkPolicyFromState(effective), nil
	}
	operationID := nomadSandboxNetworkMutationOperationID(
		record.ID, slot, currentDigest, protocol.NetworkPolicyDigest(desiredPolicy),
		currentRequestPayload, desiredRequestPayload,
	)
	_, err = s.mutationStore.BeginNomadSandboxNetworkMutation(ctx,
		&sandboxstore.BeginNomadSandboxNetworkMutationRequest{
			SandboxID: record.ID, OperationID: operationID, ExpectedTeamID: record.TeamID,
			ExpectedCurrentPolicyDigest: currentDigest,
			DesiredPolicy:               desiredPolicy, DesiredPolicyDigest: protocol.NetworkPolicyDigest(desiredPolicy),
			RequestPolicy: desiredRequest,
		})
	if err != nil {
		return nil, mapNomadSandboxNetworkMutationError(record.ID, "begin", err)
	}
	if err := s.CompleteNomadSandboxNetworkMutation(ctx, record.ID); err != nil {
		s.enqueueMutation(record.ID)
		if apierrors.IsConflict(err) {
			return nil, err
		}
		return nil, fmt.Errorf("%w: active Nomad network update is durably pending: %v",
			ErrSandboxRuntimeUpdateUnavailable, err)
	}
	return sandboxNetworkPolicyFromState(effective), nil
}

// CompleteNomadSandboxNetworkMutation executes one exact pending node command
// and commits its applied token. It is safe across API response loss, manager
// restarts, and concurrent retries because operation identity is durable.
func (s *NomadSandboxNetworkPolicyService) CompleteNomadSandboxNetworkMutation(
	ctx context.Context,
	sandboxID string,
) error {
	if s == nil || s.mutationStore == nil || s.preparer == nil {
		return fmt.Errorf("Nomad sandbox network mutation authority is not configured")
	}
	mutation, err := s.mutationStore.PrepareNomadSandboxNetworkMutation(ctx, sandboxID)
	if err != nil {
		return mapNomadSandboxNetworkMutationError(sandboxID, "prepare", err)
	}
	if mutation == nil || mutation.Phase == sandboxstore.NomadSandboxNetworkMutationPhaseApplied {
		return nil
	}
	if mutation.Phase == sandboxstore.NomadSandboxNetworkMutationPhaseCanceled {
		return apierrors.NewConflict(schema.GroupResource{Resource: "sandbox"}, sandboxID,
			fmt.Errorf("network update was canceled: %s", mutation.CancellationReason))
	}
	token, err := s.preparer.Prepare(ctx, runtimeslotclaim.NetworkPrepareRequest{
		OperationID: mutation.OperationID, ClaimID: mutation.ClaimID,
		SlotID: mutation.SlotID, ClusterID: mutation.ClusterID,
		AllocationID: mutation.AllocationID, NodeID: mutation.NodeID,
		NodeUID: mutation.NodeUID, NodeBootID: mutation.NodeBootID,
		NetNSIdentity: mutation.NetNSIdentity, NetworkPolicy: mutation.DesiredPolicy,
		PolicyDigest: mutation.DesiredPolicyDigest,
	})
	if err != nil {
		return fmt.Errorf("dispatch active Nomad network policy: %w", err)
	}
	if err := validateNomadSandboxNetworkMutationToken(mutation, token); err != nil {
		return err
	}
	_, err = s.mutationStore.CommitNomadSandboxNetworkMutation(
		ctx, sandboxID, mutation.OperationID, token,
	)
	if err != nil {
		return mapNomadSandboxNetworkMutationError(sandboxID, "acknowledge", err)
	}
	return nil
}

func (s *NomadSandboxNetworkPolicyService) enqueueMutation(sandboxID string) {
	s.enqueuerMu.RLock()
	enqueuer := s.enqueuer
	s.enqueuerMu.RUnlock()
	if enqueuer != nil {
		enqueuer.EnqueueSandboxNetworkMutation(sandboxID)
	}
}

func activeNomadNetworkSlotMatchesRecord(slot *sandboxstore.RuntimeSlot, record *sandboxstore.SandboxRecord) bool {
	return slot != nil && record != nil && slot.State == sandboxstore.RuntimeSlotStateActive &&
		slot.SandboxID == record.ID && slot.AllocationID == record.CurrentPodName &&
		slot.AllocationNamespace == record.CurrentPodNamespace && slot.ClaimID != "" &&
		slot.ClusterID != "" && slot.NodeID != "" && slot.NodeUID != "" &&
		slot.NodeBootID != "" && slot.NetNSIdentity != ""
}

func nomadSandboxNetworkMutationOperationID(
	sandboxID string,
	slot *sandboxstore.RuntimeSlot,
	currentDigest, desiredDigest string,
	currentRequest, desiredRequest []byte,
) string {
	payload := struct {
		Version        int    `json:"version"`
		SandboxID      string `json:"sandbox_id"`
		SlotID         string `json:"slot_id"`
		SlotRevision   int64  `json:"slot_revision"`
		AllocationID   string `json:"allocation_id"`
		NodeBootID     string `json:"node_boot_id"`
		CurrentDigest  string `json:"current_digest"`
		DesiredDigest  string `json:"desired_digest"`
		CurrentRequest string `json:"current_request"`
		DesiredRequest string `json:"desired_request"`
	}{
		Version: 1, SandboxID: sandboxID, SlotID: slot.ID, SlotRevision: slot.Revision,
		AllocationID: slot.AllocationID, NodeBootID: slot.NodeBootID,
		CurrentDigest: currentDigest, DesiredDigest: desiredDigest,
		CurrentRequest: string(currentRequest), DesiredRequest: string(desiredRequest),
	}
	encoded, _ := json.Marshal(payload)
	digest := sha256.Sum256(encoded)
	return "network-" + hex.EncodeToString(digest[:])
}

func validateNomadSandboxNetworkMutationToken(
	mutation *sandboxstore.NomadSandboxNetworkMutation,
	token rootfshandoff.NetworkPolicyToken,
) error {
	if mutation == nil {
		return fmt.Errorf("Nomad sandbox network mutation is missing")
	}
	if err := token.Validate(); err != nil {
		return fmt.Errorf("applied Nomad network token: %w", err)
	}
	if token.PodUID != mutation.AllocationID || token.ClaimID != mutation.ClaimID ||
		token.NetNSIdentity != mutation.NetNSIdentity || token.PolicyDigest != mutation.DesiredPolicyDigest ||
		token.PodSandboxID != protocol.RuntimeSlotNetworkIncarnationID(protocol.NodeNetworkPrepareControlRequest{
			SlotID: mutation.SlotID, ClusterID: mutation.ClusterID,
			AllocationID: mutation.AllocationID, NodeID: mutation.NodeID,
			NodeUID: mutation.NodeUID, NodeBootID: mutation.NodeBootID,
			NetNSIdentity: mutation.NetNSIdentity,
		}) {
		return fmt.Errorf("applied Nomad network token belongs to another runtime slot")
	}
	if _, err := protocol.NomadProcdAddress(token.PodIP); err != nil {
		return fmt.Errorf("applied Nomad network token: %w", err)
	}
	return nil
}

func mapNomadSandboxNetworkMutationError(sandboxID, action string, err error) error {
	switch {
	case errors.Is(err, sandboxstore.ErrNomadSandboxNetworkMutationConflict):
		return apierrors.NewConflict(schema.GroupResource{Resource: "sandbox"}, sandboxID,
			fmt.Errorf("%s Nomad network update: %w", action, err))
	case errors.Is(err, sandboxstore.ErrNomadSandboxNetworkMutationNotReady),
		errors.Is(err, sandboxstore.ErrRuntimeSlotNotFound):
		return fmt.Errorf("%w: %s Nomad network update: %v", ErrDataPlaneNotReady, action, err)
	default:
		return fmt.Errorf("%s Nomad network update: %w", action, err)
	}
}

func (s *NomadSandboxNetworkPolicyService) buildPolicy(
	record *sandboxstore.SandboxRecord,
	policy *v1alpha1.SandboxNetworkPolicy,
) (*networkpolicy.BuildNetworkPolicyResult, string, error) {
	request := s.buildRequest(record, policy)
	state := s.policies.BuildNetworkPolicyState(request)
	if state == nil || state.PolicySpec == nil {
		return nil, "", fmt.Errorf("build Nomad network policy")
	}
	if len(state.CredentialBindings) > 0 {
		return nil, "", fmt.Errorf("%w: Nomad credential binding projection is not configured", ErrDataPlaneNotReady)
	}
	annotation, err := v1alpha1.NetworkPolicyToAnnotation(state.PolicySpec)
	if err != nil {
		return nil, "", fmt.Errorf("serialize Nomad network policy: %w", err)
	}
	return state, annotation, nil
}

func (s *NomadSandboxNetworkPolicyService) buildRequest(
	record *sandboxstore.SandboxRecord,
	policy *v1alpha1.SandboxNetworkPolicy,
) *networkpolicy.BuildNetworkPolicyRequest {
	if record.Config.Webhook != nil && strings.TrimSpace(record.Config.Webhook.URL) != "" {
		policy = AppendWebhookNetworkPolicy(policy, record.Config.Webhook.URL)
	}
	return &networkpolicy.BuildNetworkPolicyRequest{
		SandboxID: record.ID, TeamID: record.TeamID,
		TemplateSpec: record.TemplateSpec.Network, RequestSpec: policy,
		TemplateBindings: templateCredentialBindings(record.TemplateSpec.Network),
		RequestBindings:  requestCredentialBindings(&sandboxstore.SandboxConfig{Network: policy}),
	}
}

func validateNomadNetworkPolicyRecord(record *sandboxstore.SandboxRecord, sandboxID string) error {
	if record == nil || record.DesiredState == sandboxstore.SandboxDesiredStateDeleted || !record.DeletedAt.IsZero() {
		return apierrors.NewNotFound(schema.GroupResource{Resource: "sandbox"}, sandboxID)
	}
	if record.RuntimeBackend != sandboxstore.SandboxRuntimeBackendNomad {
		return apierrors.NewConflict(schema.GroupResource{Resource: "sandbox"}, sandboxID,
			fmt.Errorf("sandbox is not owned by the Nomad runtime"))
	}
	if record.DesiredState != sandboxstore.SandboxDesiredStateActive &&
		record.DesiredState != sandboxstore.SandboxDesiredStatePaused {
		return apierrors.NewConflict(schema.GroupResource{Resource: "sandbox"}, sandboxID,
			fmt.Errorf("sandbox state %s does not expose network policy", record.DesiredState))
	}
	return nil
}

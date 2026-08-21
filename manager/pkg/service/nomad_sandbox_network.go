package service

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/networkpolicy"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	"github.com/sandbox0-ai/sandbox0/pkg/managerapi"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// NomadSandboxNetworkPolicyService owns the durable paused policy that is
// consumed by the next exact runtime-slot claim.
type NomadSandboxNetworkPolicyService struct {
	store    NomadSandboxMutationStore
	policies *networkpolicy.NetworkPolicyService
}

// NewNomadSandboxNetworkPolicyService creates a policy service that never
// mutates a Kubernetes runtime object.
func NewNomadSandboxNetworkPolicyService(
	store NomadSandboxMutationStore,
	policies *networkpolicy.NetworkPolicyService,
) (*NomadSandboxNetworkPolicyService, error) {
	if store == nil || policies == nil {
		return nil, fmt.Errorf("Nomad sandbox store and network policy builder are required")
	}
	return &NomadSandboxNetworkPolicyService{store: store, policies: policies}, nil
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
	var effective *networkpolicy.BuildNetworkPolicyResult
	err := s.store.WithSandboxLock(ctx, sandboxID, func(lockCtx context.Context, tx sandboxstore.SandboxStoreTx, record *sandboxstore.SandboxRecord) error {
		if err := validateNomadNetworkPolicyRecord(record, sandboxID); err != nil {
			return err
		}
		if record.DesiredState == sandboxstore.SandboxDesiredStateActive {
			return fmt.Errorf("%w: active Nomad network policy updates require runtime-slot apply acknowledgement", ErrSandboxRuntimeUpdateUnavailable)
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

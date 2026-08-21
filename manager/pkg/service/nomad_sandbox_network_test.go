package service

import (
	"context"
	"crypto/sha256"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/networkpolicy"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
	"go.uber.org/zap"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
)

func TestNomadSandboxNetworkPolicyServiceReadsAndUpdatesPausedPolicy(t *testing.T) {
	store := nomadNetworkPolicyTestStore(sandboxstore.SandboxDesiredStatePaused)
	service, err := NewNomadSandboxNetworkPolicyService(store, networkpolicy.NewNetworkPolicyService(zap.NewNop()))
	if err != nil {
		t.Fatal(err)
	}

	policy, err := service.GetNetworkPolicy(context.Background(), "sandbox-a")
	if err != nil {
		t.Fatalf("GetNetworkPolicy() error = %v", err)
	}
	if policy.Mode != v1alpha1.NetworkModeBlockAll || policy.Egress == nil ||
		len(policy.Egress.AllowedCIDRs) != 1 || len(policy.Egress.AllowedDomains) != 1 {
		t.Fatalf("effective policy = %+v", policy)
	}
	updated, err := service.UpdateNetworkPolicy(context.Background(), "sandbox-a", &v1alpha1.SandboxNetworkPolicy{
		Mode: v1alpha1.NetworkModeAllowAll,
		Egress: &v1alpha1.NetworkEgressPolicy{
			DeniedDomains: []string{"blocked.example.com"},
		},
	})
	if err != nil {
		t.Fatalf("UpdateNetworkPolicy() error = %v", err)
	}
	if updated.Mode != v1alpha1.NetworkModeAllowAll || updated.Egress == nil ||
		len(updated.Egress.DeniedDomains) != 1 || updated.Egress.DeniedDomains[0] != "blocked.example.com" {
		t.Fatalf("updated policy = %+v", updated)
	}
	record, err := store.GetSandbox(context.Background(), "sandbox-a")
	if err != nil {
		t.Fatal(err)
	}
	if record.Config.Network == nil || record.Config.Network.Mode != v1alpha1.NetworkModeAllowAll ||
		record.Config.Network.CredentialBindings != nil {
		t.Fatalf("persisted request policy = %+v", record.Config.Network)
	}
}

func TestNomadSandboxNetworkPolicyServiceFencesActiveSlotDigest(t *testing.T) {
	store := nomadNetworkPolicyTestStore(sandboxstore.SandboxDesiredStateActive)
	service, err := NewNomadSandboxNetworkPolicyService(store, networkpolicy.NewNetworkPolicyService(zap.NewNop()))
	if err != nil {
		t.Fatal(err)
	}
	record := store.records["sandbox-a"]
	_, annotation, err := service.buildPolicy(record, record.Config.Network)
	if err != nil {
		t.Fatal(err)
	}
	store.runtimeSlots["sandbox-a"] = &sandboxstore.RuntimeSlot{
		ID: "slot-a", SandboxID: "sandbox-a", AllocationID: record.CurrentPodName,
		AllocationNamespace: record.CurrentPodNamespace, State: sandboxstore.RuntimeSlotStateActive,
		ClaimNetworkPolicyDigest: protocol.NetworkPolicyDigest(annotation),
		ProcdInstanceID:          "procd-a", ProcdAddress: "http://192.0.2.2:49983",
		CommandReadyDigest: make([]byte, sha256.Size), CommandReadyAt: record.UpdatedAt,
		AuthorityObservedAt: record.UpdatedAt, HeartbeatExpiresAt: record.UpdatedAt.Add(time.Minute),
	}
	if _, err := service.GetNetworkPolicy(context.Background(), "sandbox-a"); err != nil {
		t.Fatalf("GetNetworkPolicy() error = %v", err)
	}
	store.runtimeSlots["sandbox-a"].ClaimNetworkPolicyDigest = protocol.NetworkPolicyDigest(`{"different":true}`)
	if _, err := service.GetNetworkPolicy(context.Background(), "sandbox-a"); !errors.Is(err, ErrDataPlaneNotReady) {
		t.Fatalf("mismatched digest error = %v", err)
	}
}

func TestNomadSandboxNetworkPolicyServiceRejectsUnsafeUpdates(t *testing.T) {
	if _, err := NewNomadSandboxNetworkPolicyService(nil, networkpolicy.NewNetworkPolicyService(zap.NewNop())); err == nil {
		t.Fatal("nil network store was accepted")
	}
	store := nomadNetworkPolicyTestStore(sandboxstore.SandboxDesiredStateActive)
	service, err := NewNomadSandboxNetworkPolicyService(store, networkpolicy.NewNetworkPolicyService(zap.NewNop()))
	if err != nil {
		t.Fatal(err)
	}
	before, err := store.GetSandbox(context.Background(), "sandbox-a")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.UpdateNetworkPolicy(context.Background(), "sandbox-a", &v1alpha1.SandboxNetworkPolicy{Mode: v1alpha1.NetworkModeBlockAll}); !errors.Is(err, ErrSandboxRuntimeUpdateUnavailable) {
		t.Fatalf("active update error = %v", err)
	}
	after, err := store.GetSandbox(context.Background(), "sandbox-a")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before, after) {
		t.Fatal("rejected active update changed the durable record")
	}

	store.records["sandbox-a"].DesiredState = sandboxstore.SandboxDesiredStatePaused
	store.lifecycleTxns["resume-a"] = &sandboxstore.SandboxLifecycleTxn{
		ID: "resume-a", SandboxID: "sandbox-a", Kind: sandboxstore.SandboxLifecycleKindResume,
		Phase: sandboxstore.SandboxLifecyclePhasePreparing,
	}
	if _, err := service.UpdateNetworkPolicy(context.Background(), "sandbox-a", &v1alpha1.SandboxNetworkPolicy{Mode: v1alpha1.NetworkModeBlockAll}); !apierrors.IsConflict(err) {
		t.Fatalf("lifecycle update error = %v", err)
	}
	delete(store.lifecycleTxns, "resume-a")

	credentialPolicy := &v1alpha1.SandboxNetworkPolicy{
		Mode: v1alpha1.NetworkModeBlockAll,
		Egress: &v1alpha1.NetworkEgressPolicy{CredentialRules: []v1alpha1.EgressCredentialRule{{
			Name: "api", CredentialRef: "api-auth", Protocol: v1alpha1.EgressAuthProtocolHTTPS,
			Domains: []string{"api.example.com"},
		}}},
		CredentialBindings: testCredentialBindings("api-auth", "Bearer token"),
	}
	if _, err := service.UpdateNetworkPolicy(context.Background(), "sandbox-a", credentialPolicy); !errors.Is(err, ErrSandboxRuntimeUpdateUnavailable) {
		t.Fatalf("credential update error = %v", err)
	}
	store.records["sandbox-a"].RuntimeBackend = sandboxstore.SandboxRuntimeBackendKubernetes
	if _, err := service.GetNetworkPolicy(context.Background(), "sandbox-a"); !apierrors.IsConflict(err) {
		t.Fatalf("foreign runtime get error = %v", err)
	}
	if _, err := service.UpdateNetworkPolicy(context.Background(), "sandbox-a", nil); err == nil {
		t.Fatal("nil network policy was accepted")
	}
}

func TestAppendWebhookNetworkPolicyDoesNotMutateInput(t *testing.T) {
	original := &v1alpha1.SandboxNetworkPolicy{Mode: v1alpha1.NetworkModeBlockAll}
	result := AppendWebhookNetworkPolicy(original, "https://hooks.example.com/path")
	if result == original || original.Egress != nil || result.Egress == nil ||
		len(result.Egress.AllowedDomains) != 1 || result.Egress.AllowedDomains[0] != "hooks.example.com" {
		t.Fatalf("webhook policy = %+v, original = %+v", result, original)
	}
	if got := AppendWebhookNetworkPolicy(original, "://invalid"); got != original {
		t.Fatal("invalid webhook URL changed the request policy")
	}
}

func nomadNetworkPolicyTestStore(desiredState string) *memorySandboxStore {
	now := time.Date(2026, time.August, 21, 6, 0, 0, 0, time.UTC)
	return &memorySandboxStore{
		records: map[string]*sandboxstore.SandboxRecord{
			"sandbox-a": {
				ID: "sandbox-a", TeamID: "team-a", RuntimeBackend: sandboxstore.SandboxRuntimeBackendNomad,
				DesiredState: desiredState, CurrentPodNamespace: "default", CurrentPodName: "allocation-a",
				TemplateSpec: v1alpha1.SandboxTemplateSpec{Network: &v1alpha1.SandboxNetworkPolicy{
					Mode:   v1alpha1.NetworkModeBlockAll,
					Egress: &v1alpha1.NetworkEgressPolicy{AllowedCIDRs: []string{"192.0.2.0/24"}},
				}},
				Config: sandboxstore.SandboxConfig{Network: &v1alpha1.SandboxNetworkPolicy{
					Egress: &v1alpha1.NetworkEgressPolicy{AllowedDomains: []string{"api.example.com"}},
				}},
				CreatedAt: now, UpdatedAt: now,
			},
		},
		lifecycleTxns: map[string]*sandboxstore.SandboxLifecycleTxn{},
		runtimeSlots:  map[string]*sandboxstore.RuntimeSlot{},
	}
}

package service

import (
	"context"
	"crypto/sha256"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/credentialbinding"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/egressauthstore"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/networkpolicy"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/runtimeslotclaim"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshandoff"
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
	_, annotation, _, err := service.buildStoredPolicy(context.Background(), record, record.Config.Network)
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

func TestNomadSandboxNetworkPolicyServiceFencesUpdatesAndPublishesPausedCredentials(t *testing.T) {
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
	credentialResult, err := service.UpdateNetworkPolicy(context.Background(), "sandbox-a", credentialPolicy)
	if err != nil {
		t.Fatalf("credential update error = %v", err)
	}
	if len(credentialResult.CredentialBindings) != 1 ||
		credentialResult.CredentialBindings[0].Ref != "api-auth" {
		t.Fatalf("credential update result = %+v", credentialResult)
	}
	store.records["sandbox-a"].RuntimeBackend = sandboxstore.SandboxRuntimeBackendKubernetes
	if _, err := service.GetNetworkPolicy(context.Background(), "sandbox-a"); !apierrors.IsConflict(err) {
		t.Fatalf("foreign runtime get error = %v", err)
	}
	if _, err := service.UpdateNetworkPolicy(context.Background(), "sandbox-a", nil); err == nil {
		t.Fatal("nil network policy was accepted")
	}
}

func TestNomadSandboxNetworkPolicyServiceActiveUpdateSurvivesResponseLossAndRestart(t *testing.T) {
	base := nomadNetworkPolicyTestStore(sandboxstore.SandboxDesiredStateActive)
	store := &nomadNetworkMutationTestStore{memorySandboxStore: base}
	failing := &nomadNetworkPreparerTest{err: errors.New("node channel disconnected")}
	service, err := NewNomadSandboxNetworkPolicyService(
		store, networkpolicy.NewNetworkPolicyService(zap.NewNop()), failing,
	)
	if err != nil {
		t.Fatal(err)
	}
	record := base.records["sandbox-a"]
	_, currentAnnotation, _, err := service.buildStoredPolicy(context.Background(), record, record.Config.Network)
	if err != nil {
		t.Fatal(err)
	}
	base.runtimeSlots["sandbox-a"] = activeNomadNetworkTestSlot(record, currentAnnotation)
	enqueuer := &nomadNetworkEnqueuerTest{}
	service.SetMutationEnqueuer(enqueuer)
	desired := &v1alpha1.SandboxNetworkPolicy{
		Mode: v1alpha1.NetworkModeAllowAll,
		Egress: &v1alpha1.NetworkEgressPolicy{
			DeniedDomains: []string{"blocked.example.com"},
		},
	}
	_, err = service.UpdateNetworkPolicy(context.Background(), "sandbox-a", desired)
	if !errors.Is(err, ErrSandboxRuntimeUpdateUnavailable) {
		t.Fatalf("active update error = %v", err)
	}
	if store.mutation == nil || store.mutation.Phase != sandboxstore.NomadSandboxNetworkMutationPhasePending {
		t.Fatalf("durable mutation = %+v", store.mutation)
	}
	if record := base.records["sandbox-a"]; record.Config.Network.Mode == v1alpha1.NetworkModeAllowAll {
		t.Fatal("unacknowledged desired config was published")
	}
	oldPolicy, err := service.GetNetworkPolicy(context.Background(), "sandbox-a")
	if err != nil {
		t.Fatalf("GetNetworkPolicy() during pending error = %v", err)
	}
	if oldPolicy.Mode != v1alpha1.NetworkModeBlockAll {
		t.Fatalf("policy during pending = %+v", oldPolicy)
	}
	if len(enqueuer.sandboxIDs) != 1 || enqueuer.sandboxIDs[0] != "sandbox-a" {
		t.Fatalf("enqueued sandboxes = %v", enqueuer.sandboxIDs)
	}

	recoveredPreparer := &nomadNetworkPreparerTest{}
	recovered, err := NewNomadSandboxNetworkPolicyService(
		store, networkpolicy.NewNetworkPolicyService(zap.NewNop()), recoveredPreparer,
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := recovered.CompleteNomadSandboxNetworkMutation(context.Background(), "sandbox-a"); err != nil {
		t.Fatalf("CompleteNomadSandboxNetworkMutation() error = %v", err)
	}
	if store.mutation.Phase != sandboxstore.NomadSandboxNetworkMutationPhaseApplied ||
		store.mutation.AppliedPolicyToken == nil {
		t.Fatalf("applied mutation = %+v", store.mutation)
	}
	applied, err := recovered.GetNetworkPolicy(context.Background(), "sandbox-a")
	if err != nil {
		t.Fatalf("GetNetworkPolicy() after recovery error = %v", err)
	}
	if applied.Mode != v1alpha1.NetworkModeAllowAll || applied.Egress == nil ||
		len(applied.Egress.DeniedDomains) != 1 {
		t.Fatalf("applied policy = %+v", applied)
	}
	if len(recoveredPreparer.requests) != 1 ||
		recoveredPreparer.requests[0].OperationID != store.mutation.OperationID {
		t.Fatalf("recovered prepare requests = %+v", recoveredPreparer.requests)
	}

	if _, err := recovered.UpdateNetworkPolicy(context.Background(), "sandbox-a", desired); err != nil {
		t.Fatalf("idempotent active UpdateNetworkPolicy() error = %v", err)
	}
	if len(recoveredPreparer.requests) != 1 {
		t.Fatalf("idempotent update dispatched %d network prepares", len(recoveredPreparer.requests))
	}
}

func TestNomadSandboxNetworkPolicyServiceRejectsMismatchedAppliedToken(t *testing.T) {
	base := nomadNetworkPolicyTestStore(sandboxstore.SandboxDesiredStateActive)
	store := &nomadNetworkMutationTestStore{memorySandboxStore: base}
	preparer := &nomadNetworkPreparerTest{mutateToken: func(token *rootfshandoff.NetworkPolicyToken) {
		token.NetNSIdentity = "another-netns"
	}}
	service, err := NewNomadSandboxNetworkPolicyService(
		store, networkpolicy.NewNetworkPolicyService(zap.NewNop()), preparer,
	)
	if err != nil {
		t.Fatal(err)
	}
	record := base.records["sandbox-a"]
	_, currentAnnotation, _, err := service.buildStoredPolicy(context.Background(), record, record.Config.Network)
	if err != nil {
		t.Fatal(err)
	}
	base.runtimeSlots["sandbox-a"] = activeNomadNetworkTestSlot(record, currentAnnotation)
	_, err = service.UpdateNetworkPolicy(context.Background(), "sandbox-a", &v1alpha1.SandboxNetworkPolicy{
		Mode: v1alpha1.NetworkModeAllowAll,
	})
	if !errors.Is(err, ErrSandboxRuntimeUpdateUnavailable) {
		t.Fatalf("mismatched token update error = %v", err)
	}
	if store.mutation == nil || store.mutation.Phase != sandboxstore.NomadSandboxNetworkMutationPhasePending {
		t.Fatalf("mismatched token mutation = %+v", store.mutation)
	}
	if base.records["sandbox-a"].Config.Network.Mode == v1alpha1.NetworkModeAllowAll {
		t.Fatal("mismatched token published desired config")
	}
}

func TestNomadSandboxNetworkPolicyServiceActiveBindingOnlyUpdateWaitsForAck(t *testing.T) {
	base := nomadNetworkPolicyTestStore(sandboxstore.SandboxDesiredStateActive)
	record := base.records["sandbox-a"]
	credentialRules := []v1alpha1.EgressCredentialRule{{
		Name: "api", CredentialRef: "api-auth", Protocol: v1alpha1.EgressAuthProtocolHTTP,
		Domains: []string{"api.example.com"},
	}}
	record.Config.Network = &v1alpha1.SandboxNetworkPolicy{
		Mode:   v1alpha1.NetworkModeBlockAll,
		Egress: &v1alpha1.NetworkEgressPolicy{CredentialRules: credentialRules},
	}
	oldBinding := testCredentialBindings("api-auth", "Bearer {{.token}}")
	oldBinding[0].SourceRef = "source-old"
	base.credentialBindings = map[string][]egressauthstore.CredentialBinding{
		record.ID: credentialbinding.ToStore(oldBinding),
	}
	base.credentialDigests = map[string]string{
		record.ID: credentialbinding.DigestPublic(oldBinding),
	}
	store := &nomadNetworkMutationTestStore{memorySandboxStore: base}
	preparer := &nomadNetworkPreparerTest{}
	service, err := NewNomadSandboxNetworkPolicyService(
		store, networkpolicy.NewNetworkPolicyService(zap.NewNop()), preparer,
	)
	if err != nil {
		t.Fatal(err)
	}
	_, currentAnnotation, _, err := service.buildStoredPolicy(context.Background(), record, record.Config.Network)
	if err != nil {
		t.Fatal(err)
	}
	base.runtimeSlots[record.ID] = activeNomadNetworkTestSlot(record, currentAnnotation)

	newBinding := testCredentialBindings("api-auth", "Bearer {{.token}}")
	newBinding[0].SourceRef = "source-new"
	desired := &v1alpha1.SandboxNetworkPolicy{
		Mode:               v1alpha1.NetworkModeBlockAll,
		Egress:             &v1alpha1.NetworkEgressPolicy{CredentialRules: credentialRules},
		CredentialBindings: newBinding,
	}
	updated, err := service.UpdateNetworkPolicy(context.Background(), record.ID, desired)
	if err != nil {
		t.Fatal(err)
	}
	if len(preparer.requests) != 1 {
		t.Fatalf("network prepare calls = %d", len(preparer.requests))
	}
	if preparer.requests[0].PolicyDigest == protocol.NetworkPolicyDigest(currentAnnotation) {
		t.Fatal("binding-only update retained the old policy digest")
	}
	if len(updated.CredentialBindings) != 1 || updated.CredentialBindings[0].SourceRef != "source-new" {
		t.Fatalf("updated credential policy = %+v", updated)
	}
	if got := base.credentialBindings[record.ID]; len(got) != 1 || got[0].SourceRef != "source-new" {
		t.Fatalf("published credential bindings = %+v", got)
	}
	if base.records[record.ID].Config.Network.CredentialBindings != nil {
		t.Fatal("credential bindings leaked into sandbox config")
	}
	loaded, err := service.GetNetworkPolicy(context.Background(), record.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(loaded.CredentialBindings) != 1 || loaded.CredentialBindings[0].SourceRef != "source-new" {
		t.Fatalf("loaded credential policy = %+v", loaded)
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

type nomadNetworkMutationTestStore struct {
	*memorySandboxStore
	mutation        *sandboxstore.NomadSandboxNetworkMutation
	pendingBindings []egressauthstore.CredentialBinding
}

func (s *nomadNetworkMutationTestStore) BeginNomadSandboxNetworkMutation(
	_ context.Context,
	request *sandboxstore.BeginNomadSandboxNetworkMutationRequest,
) (*sandboxstore.NomadSandboxNetworkMutation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.mutation != nil && s.mutation.Phase == sandboxstore.NomadSandboxNetworkMutationPhasePending {
		if s.mutation.OperationID != request.OperationID {
			return nil, sandboxstore.ErrNomadSandboxNetworkMutationConflict
		}
		return cloneNomadNetworkMutation(s.mutation), nil
	}
	record := s.records[request.SandboxID]
	slot := s.runtimeSlots[request.SandboxID]
	if record == nil || slot == nil || record.TeamID != request.ExpectedTeamID ||
		slot.ClaimNetworkPolicyDigest != request.ExpectedCurrentPolicyDigest {
		return nil, sandboxstore.ErrNomadSandboxNetworkMutationConflict
	}
	s.mutation = &sandboxstore.NomadSandboxNetworkMutation{
		SandboxID: request.SandboxID, OperationID: request.OperationID,
		SlotID: slot.ID, SlotRevision: slot.Revision, TeamID: record.TeamID,
		ClusterID: slot.ClusterID, AllocationID: slot.AllocationID,
		AllocationNamespace: slot.AllocationNamespace, NodeID: slot.NodeID,
		NodeUID: slot.NodeUID, NodeBootID: slot.NodeBootID, NetNSIdentity: slot.NetNSIdentity,
		ClaimID: slot.ClaimID, CurrentPolicyDigest: request.ExpectedCurrentPolicyDigest,
		DesiredPolicy: request.DesiredPolicy, DesiredPolicyDigest: request.DesiredPolicyDigest,
		CredentialBindingDigest: credentialbinding.DigestStore(request.CredentialBindings),
		RequestPolicy:           sanitizedNetworkPolicyForPersistence(request.RequestPolicy),
		Phase:                   sandboxstore.NomadSandboxNetworkMutationPhasePending,
	}
	s.pendingBindings = credentialbinding.CloneStore(request.CredentialBindings)
	return cloneNomadNetworkMutation(s.mutation), nil
}

func (s *nomadNetworkMutationTestStore) PrepareNomadSandboxNetworkMutation(
	_ context.Context,
	sandboxID string,
) (*sandboxstore.NomadSandboxNetworkMutation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.mutation == nil || s.mutation.SandboxID != sandboxID {
		return nil, nil
	}
	record := s.records[sandboxID]
	slot := s.runtimeSlots[sandboxID]
	if s.mutation.Phase == sandboxstore.NomadSandboxNetworkMutationPhasePending &&
		(record == nil || record.DesiredState != sandboxstore.SandboxDesiredStateActive ||
			slot == nil || slot.ID != s.mutation.SlotID || slot.Revision != s.mutation.SlotRevision ||
			slot.ClaimNetworkPolicyDigest != s.mutation.CurrentPolicyDigest) {
		s.mutation.Phase = sandboxstore.NomadSandboxNetworkMutationPhaseCanceled
		s.mutation.CancellationReason = "runtime slot incarnation changed"
	}
	return cloneNomadNetworkMutation(s.mutation), nil
}

func (s *nomadNetworkMutationTestStore) CommitNomadSandboxNetworkMutation(
	_ context.Context,
	sandboxID, operationID string,
	token rootfshandoff.NetworkPolicyToken,
) (*sandboxstore.NomadSandboxNetworkMutation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.mutation == nil || s.mutation.SandboxID != sandboxID || s.mutation.OperationID != operationID {
		return nil, sandboxstore.ErrNomadSandboxNetworkMutationConflict
	}
	if s.mutation.Phase == sandboxstore.NomadSandboxNetworkMutationPhaseApplied {
		return cloneNomadNetworkMutation(s.mutation), nil
	}
	if s.mutation.Phase != sandboxstore.NomadSandboxNetworkMutationPhasePending {
		return nil, sandboxstore.ErrNomadSandboxNetworkMutationConflict
	}
	record := s.records[sandboxID]
	slot := s.runtimeSlots[sandboxID]
	if record == nil || slot == nil || slot.Revision != s.mutation.SlotRevision {
		return nil, sandboxstore.ErrNomadSandboxNetworkMutationConflict
	}
	record.Config.Network = sanitizedNetworkPolicyForPersistence(s.mutation.RequestPolicy)
	if s.credentialBindings == nil {
		s.credentialBindings = make(map[string][]egressauthstore.CredentialBinding)
	}
	if s.credentialDigests == nil {
		s.credentialDigests = make(map[string]string)
	}
	s.credentialBindings[sandboxID] = credentialbinding.CloneStore(s.pendingBindings)
	s.credentialDigests[sandboxID] = s.mutation.CredentialBindingDigest
	s.pendingBindings = nil
	slot.ClaimNetworkPolicyDigest = s.mutation.DesiredPolicyDigest
	slot.Revision++
	tokenCopy := token
	s.mutation.AppliedPolicyToken = &tokenCopy
	s.mutation.Phase = sandboxstore.NomadSandboxNetworkMutationPhaseApplied
	return cloneNomadNetworkMutation(s.mutation), nil
}

func (s *nomadNetworkMutationTestStore) ListPendingNomadSandboxNetworkMutations(
	_ context.Context,
	_ int,
) ([]*sandboxstore.NomadSandboxNetworkMutation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.mutation == nil || s.mutation.Phase != sandboxstore.NomadSandboxNetworkMutationPhasePending {
		return nil, nil
	}
	return []*sandboxstore.NomadSandboxNetworkMutation{cloneNomadNetworkMutation(s.mutation)}, nil
}

func cloneNomadNetworkMutation(
	mutation *sandboxstore.NomadSandboxNetworkMutation,
) *sandboxstore.NomadSandboxNetworkMutation {
	if mutation == nil {
		return nil
	}
	clone := *mutation
	clone.RequestPolicy = sanitizedNetworkPolicyForPersistence(mutation.RequestPolicy)
	if mutation.AppliedPolicyToken != nil {
		token := *mutation.AppliedPolicyToken
		clone.AppliedPolicyToken = &token
	}
	clone.AppliedTokenDigest = append([]byte(nil), mutation.AppliedTokenDigest...)
	return &clone
}

type nomadNetworkPreparerTest struct {
	mu          sync.Mutex
	err         error
	mutateToken func(*rootfshandoff.NetworkPolicyToken)
	requests    []runtimeslotclaim.NetworkPrepareRequest
}

func (p *nomadNetworkPreparerTest) Prepare(
	_ context.Context,
	request runtimeslotclaim.NetworkPrepareRequest,
) (rootfshandoff.NetworkPolicyToken, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.requests = append(p.requests, request)
	if p.err != nil {
		return rootfshandoff.NetworkPolicyToken{}, p.err
	}
	token := rootfshandoff.NetworkPolicyToken{
		PodUID: request.AllocationID,
		PodSandboxID: protocol.RuntimeSlotNetworkIncarnationID(protocol.NodeNetworkPrepareControlRequest{
			SlotID: request.SlotID, ClusterID: request.ClusterID, AllocationID: request.AllocationID,
			NodeID: request.NodeID, NodeUID: request.NodeUID, NodeBootID: request.NodeBootID,
			NetNSIdentity: request.NetNSIdentity,
		}),
		ClaimID: request.ClaimID, NetworkEpoch: 2, PolicyDigest: request.PolicyDigest,
		PodIP: "192.0.2.2", CtldGeneration: "ctld-generation-2",
		NetNSIdentity: request.NetNSIdentity,
	}
	if p.mutateToken != nil {
		p.mutateToken(&token)
	}
	return token, nil
}

type nomadNetworkEnqueuerTest struct {
	sandboxIDs []string
}

func (e *nomadNetworkEnqueuerTest) EnqueueSandboxNetworkMutation(sandboxID string) {
	e.sandboxIDs = append(e.sandboxIDs, sandboxID)
}

func activeNomadNetworkTestSlot(
	record *sandboxstore.SandboxRecord,
	currentPolicy string,
) *sandboxstore.RuntimeSlot {
	return &sandboxstore.RuntimeSlot{
		ID: "slot-a", SandboxID: record.ID, AllocationID: record.CurrentPodName,
		AllocationNamespace: record.CurrentPodNamespace, State: sandboxstore.RuntimeSlotStateActive,
		Revision: 7, ClusterID: "cluster-a", NodeID: "node-a", NodeUID: "node-uid-a",
		NodeBootID: "node-boot-a", NetNSIdentity: "netns-a", ClaimID: "claim-a",
		ClaimNetworkPolicyDigest: protocol.NetworkPolicyDigest(currentPolicy),
		ProcdInstanceID:          "procd-a", ProcdAddress: "http://192.0.2.2:49983",
		CommandReadyDigest: make([]byte, sha256.Size), CommandReadyAt: record.UpdatedAt,
		AuthorityObservedAt: record.UpdatedAt, HeartbeatExpiresAt: record.UpdatedAt.Add(time.Minute),
	}
}

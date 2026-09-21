package runtimeslotclaim

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/opencontainers/go-digest"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshandoff"
	"github.com/sandbox0-ai/sandbox0/pkg/runtimecheckpoint"
	"github.com/sandbox0-ai/sandbox0/pkg/runtimecontrol"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
	"github.com/stretchr/testify/require"
)

type migrationPlannerStore struct {
	*fakeStore
	image                                    protocol.MigrationImagePrepareRequest
	fence                                    protocol.MigrationSourceFenceRequest
	proof                                    protocol.MigrationSourceFenceProof
	restored                                 *protocol.MigrationRestoreRequest
	authorizeErr                             error
	acquireCalls, issueCalls, authorizeCalls int
}

func (s *migrationPlannerStore) AcquireRuntimeSlot(context.Context, *sandboxstore.AcquireRuntimeSlotRequest) (*sandboxstore.RuntimeSlot, error) {
	return nil, errors.New("migration invoked ordinary slot selection")
}
func (s *migrationPlannerStore) IssueAndBindRuntimeSlotWriterGrant(context.Context, *sandboxstore.IssueRootFSWriterGrantRequest, *sandboxstore.BindRuntimeSlotWriterGrantRequest) (*sandboxstore.IssueAndBindRuntimeSlotWriterGrantResult, error) {
	return nil, errors.New("migration invoked ordinary writer authority")
}
func (s *migrationPlannerStore) AcquireNomadSandboxMigrationTarget(_ context.Context, a runtimecontrol.MigrationAssignment) (*sandboxstore.RuntimeSlot, error) {
	s.acquireCalls++
	s.mu.Lock()
	defer s.mu.Unlock()
	d, _ := a.Digest()
	want, _ := s.image.Publication.Assignment.Digest()
	if d != want {
		return nil, errors.New("assignment changed")
	}
	s.slot.AuthorityObservedAt = time.Now().UTC()
	copy := *s.slot
	return &copy, nil
}
func (s *migrationPlannerStore) IssueNomadSandboxMigrationTargetWriter(ctx context.Context, a runtimecontrol.MigrationAssignment, r *sandboxstore.IssueRootFSWriterGrantRequest) (*sandboxstore.IssueAndBindRuntimeSlotWriterGrantResult, error) {
	s.issueCalls++
	if r.OperationID != a.OperationID || r.ClaimID != s.image.Resources.ClaimID {
		return nil, errors.New("migration writer changed identity")
	}
	return s.fakeStore.IssueAndBindRuntimeSlotWriterGrant(ctx, r, &sandboxstore.BindRuntimeSlotWriterGrantRequest{SlotID: s.slot.ID, OperationID: a.OperationID, ClaimID: r.ClaimID, GrantID: r.GrantID})
}
func (s *migrationPlannerStore) AuthorizeNomadSandboxMigrationRestore(_ context.Context, a runtimecontrol.MigrationAssignment, stage rootfshandoff.StageRequest) (*protocol.MigrationRestoreRequest, error) {
	s.authorizeCalls++
	if s.authorizeErr != nil {
		return nil, s.authorizeErr
	}
	imageDigest, _ := s.image.Digest()
	prepared := protocol.MigrationImagePrepared{RequestDigest: imageDigest, ManifestDigest: s.image.Receipt.Reference.ManifestDigest, TotalBytes: 1024}
	request := protocol.MigrationRestoreRequest{Image: s.image, Prepared: prepared, Fence: s.fence, Proof: s.proof, Stage: stage}
	want, err := request.Digest()
	if err != nil {
		return nil, err
	}
	if s.restored != nil {
		actual, _ := s.restored.Digest()
		if want != actual {
			return nil, errors.New("restore command changed on retry")
		}
	}
	s.restored = &request
	return &request, nil
}

type migrationPlannerNode struct {
	*fakeNode
	store       *migrationPlannerStore
	state       protocol.MigrationRestoreState
	lost        bool
	wrongDigest bool
	missing     bool
}

func (n *migrationPlannerNode) Claim(ctx context.Context, target NodeTarget, request protocol.NodeClaimControlRequest) (protocol.NodeControlResponse, error) {
	if n.store.restored == nil || request.MigrationRestore == nil {
		return protocol.NodeControlResponse{}, errors.New("claim preceded restore authorization")
	}
	response, err := n.fakeNode.Claim(ctx, target, request)
	if err != nil {
		return response, err
	}
	if n.missing {
		return response, nil
	}
	digest, err := request.MigrationRestore.Digest()
	if err != nil {
		return response, err
	}
	if n.wrongDigest {
		digest = strings.Repeat("f", 64)
	}
	response.MigrationRestore = &protocol.MigrationRestoreObservation{Request: *request.MigrationRestore, RequestDigest: digest, State: n.state}
	if n.lost {
		n.lost = false
		return protocol.NodeControlResponse{}, errors.New("lost restore reply")
	}
	return response, nil
}

func migrationPlannerFixture(t *testing.T) (*plannerFixture, *migrationPlannerStore, *migrationPlannerNode) {
	t.Helper()
	f := newPlannerFixture(t)
	source := f.request.Runtime
	source.RuntimeGeneration--
	sourceRevision, err := source.Revision()
	require.NoError(t, err)
	a := runtimecontrol.MigrationAssignment{OperationID: f.request.OperationID, SourceGeneration: source.RuntimeGeneration, SourceRevision: sourceRevision, Target: f.request.Runtime}
	assignmentDigest, err := a.Digest()
	require.NoError(t, err)
	captureRequest := protocol.MigrationCaptureRequest{Target: protocol.NodeChannelTarget{SlotID: "source-slot", ClusterID: f.store.slot.ClusterID, AllocationID: "source-allocation", NodeID: "source-node", NodeUID: "source-uid", NodeBootID: "source-boot", ControlEndpoint: "unix:///source.sock"},
		OperationID: a.OperationID, LifecycleEpoch: 2, SandboxID: a.Target.SandboxID, SourceGeneration: a.SourceGeneration, AssignmentRevision: a.SourceRevision,
		BindingDigest: strings.Repeat("a", 64), ResourceLeaseDigest: strings.Repeat("b", 64), ProcdInstanceID: "preserved-procd"}
	captureDigest, err := captureRequest.Digest()
	require.NoError(t, err)
	f.store.generation.ID = "migration-" + captureDigest
	f.store.filesystem.HeadGenerationID = f.store.generation.ID
	generation, err := generationDescriptor(f.store.filesystem, f.store.generation)
	require.NoError(t, err)
	cut, err := rootfshandoff.NewMigrationRootFSCut(rootfshandoff.MigrationRootFSCutRequest{OperationID: a.OperationID, CaptureRequestDigest: captureDigest, SourceBindingDigest: captureRequest.BindingDigest, GenerationID: generation.GenerationID}, *generation, 1)
	require.NoError(t, err)
	publication := protocol.MigrationPublicationRequest{Capture: protocol.MigrationCapture{Request: captureRequest, RequestDigest: captureDigest, State: protocol.MigrationCaptureComplete, RootFS: &cut}, Assignment: a,
		CompatibilityDigest: f.request.CompatibilityDigest, CPUFeaturesDigest: digest.FromString("fixture-cpu").String()}
	binding, err := publication.Binding()
	require.NoError(t, err)
	bindingDigest, err := binding.Digest()
	require.NoError(t, err)
	pubDigest, err := publication.Digest()
	require.NoError(t, err)
	receipt := protocol.MigrationPublication{RequestDigest: pubDigest, Binding: binding, Reference: runtimecheckpoint.Reference{BindingDigest: bindingDigest, ManifestDigest: digest.FromString("manifest").String()}}
	revision, err := a.Target.Revision()
	require.NoError(t, err)
	slot, err := f.store.AcquireRuntimeSlot(t.Context(), &sandboxstore.AcquireRuntimeSlotRequest{OperationID: a.OperationID, ClaimID: "migration-" + assignmentDigest, SandboxID: a.Target.SandboxID,
		FilesystemID: f.store.filesystem.ID, SourceGenerationID: generation.GenerationID, CompatibilityDigest: f.request.CompatibilityDigest, ClusterID: f.request.ClusterID,
		RuntimeAssignmentRevision: revision, NetworkPolicyDigest: protocol.NetworkPolicyDigest(f.request.NetworkPolicy), ClaimTTL: sandboxstore.DefaultRuntimeSlotClaimTTL, Resources: f.request.Resources})
	require.NoError(t, err)
	image := protocol.MigrationImagePrepareRequest{Publication: publication, Receipt: receipt, Resources: slot.ResourceLease, Target: protocol.NodeChannelTarget{SlotID: slot.ID, ClusterID: slot.ClusterID, AllocationID: slot.AllocationID, NodeID: slot.NodeID, NodeUID: slot.NodeUID, NodeBootID: slot.NodeBootID, ControlEndpoint: slot.ControlEndpoint}}
	require.NoError(t, image.Validate())
	fence := protocol.MigrationSourceFenceRequest{PublicationRequest: publication, Publication: receipt}
	detach, err := fence.RootFSRequest()
	require.NoError(t, err)
	root, err := rootfshandoff.NewMigrationRootFSDetachProof(detach, rootfshandoff.CrashFenceSessionObservation{Parent: digest.FromString("source-parent").String(), RootFSID: generation.FilesystemID, WriterEpoch: generation.WriterEpoch,
		OperationID: a.OperationID, BindingDigest: captureRequest.BindingDigest, SessionState: rootfshandoff.StateTombstoned, BranchPath: "/source.wal", DeviceBound: true, DevicePath: "/dev/nbd0",
		LiveSessionAbsent: true, MergedMountAbsent: true, XFSMountAbsent: true, ObservedAt: time.Now().UTC().Format(time.RFC3339Nano)})
	require.NoError(t, err)
	fenceDigest, err := fence.Digest()
	require.NoError(t, err)
	proof := protocol.MigrationSourceFenceProof{RequestDigest: fenceDigest, RootFS: root, ContainerID: protocol.NomadRunscContainerID("source-slot"), MountNamespaceID: "mnt:source", ContainerAbsent: true, StableMountAbsent: true}
	proof.Digest, err = proof.ProofDigest()
	require.NoError(t, err)
	require.NoError(t, proof.ValidateFor(fence))
	s := &migrationPlannerStore{fakeStore: f.store, image: image, fence: fence, proof: proof}
	n := &migrationPlannerNode{fakeNode: f.node, store: s, state: protocol.MigrationRestoreComplete}
	f.planner.store = s
	f.planner.node = n
	return f, s, n
}

func TestPlannerMigrationReusesExactWriterAndNeverProbesBeforeHandover(t *testing.T) {
	f, s, n := migrationPlannerFixture(t)
	s.issueLost = true
	_, err := f.planner.RestoreMigration(t.Context(), s.image, f.request.NetworkPolicy)
	require.ErrorContains(t, err, "writer issue response lost")
	require.Empty(t, n.claims)
	n.lost = true
	_, err = f.planner.RestoreMigration(t.Context(), s.image, f.request.NetworkPolicy)
	require.ErrorContains(t, err, "lost restore reply")
	first := cloneNodeClaim(n.claims[0])
	actual, err := f.planner.RestoreMigration(t.Context(), s.image, f.request.NetworkPolicy)
	require.NoError(t, err)
	require.Equal(t, protocol.MigrationRestoreComplete, actual.State)
	require.Equal(t, first, n.claims[1], "all deterministic identities and bearer token must survive retries")
	require.Empty(t, actual.Request.Stage.Identity.WriterGrantToken, "durable authority cannot contain the bearer token")
	require.NotEmpty(t, n.claims[1].Stage.Identity.WriterGrantToken)
	require.Equal(t, int64(8), s.filesystem.WriterEpoch, "response loss cannot issue a second writer epoch")
	require.Empty(t, f.prober.addresses)
	require.Empty(t, n.commands)
	require.Empty(t, f.observer.observations, "migration must not inflate ordinary claim latency samples")
	require.Equal(t, 3, s.acquireCalls)
	require.Equal(t, 2, s.authorizeCalls)
}

func TestPlannerMigrationRejectsMissingAuthorityAndChangedCustody(t *testing.T) {
	for _, failure := range []string{"unsupported", "authorize", "target", "resources", "policy", "missing", "uncertain", "digest"} {
		t.Run(failure, func(t *testing.T) {
			f, s, n := migrationPlannerFixture(t)
			policy := f.request.NetworkPolicy
			switch failure {
			case "unsupported":
				f.planner.store = f.store
			case "authorize":
				s.authorizeErr = errors.New("restore authorization unavailable")
			case "target":
				s.slot.ControlEndpoint = "unix:///other.sock"
			case "resources":
				s.slot.ResourceLease.CPUSetCPUs = "1"
			case "policy":
				policy = strings.ReplaceAll(policy, "allow-all", "block-all")
			case "missing":
				n.missing = true
			case "uncertain":
				n.state = protocol.MigrationRestoreUncertain
			case "digest":
				n.wrongDigest = true
			}
			_, err := f.planner.RestoreMigration(t.Context(), s.image, policy)
			require.Error(t, err)
			if failure != "missing" && failure != "uncertain" && failure != "digest" {
				require.Empty(t, n.claims)
			}
			if failure == "unsupported" || failure == "target" || failure == "resources" || failure == "policy" {
				require.Zero(t, s.issueCalls)
				require.Empty(t, f.network.requests)
			}
			require.Empty(t, n.commands)
			require.Empty(t, f.prober.addresses)
		})
	}
}

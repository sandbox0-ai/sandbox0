package runtimeslotclaim

import (
	"context"
	"encoding/hex"
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

type checkpointPlannerStore struct {
	*fakeStore
	image    protocol.MigrationImagePrepareRequest
	evidence sandboxstore.NomadCheckpointRestoreEvidence
	fence    protocol.MigrationSourceFenceRequest
	proof    protocol.MigrationSourceFenceProof
	fail     string
}

func (s *checkpointPlannerStore) GetNomadCheckpointRestorePreparation(context.Context, protocol.CheckpointRestoreAuthority, string) (*sandboxstore.NomadCheckpointRestoreEvidence, error) {
	e := s.evidence
	return &e, nil
}
func (s *checkpointPlannerStore) AuthorizeNomadCheckpointRestoreCPU(_ context.Context, a protocol.CheckpointRestoreAuthority, _ string) (*protocol.MigrationCPUPreflightRequest, error) {
	r := &protocol.MigrationCPUPreflightRequest{Checkpoint: &a.Assignment, Target: s.image.Target, Destination: s.image.Target, DestinationResources: s.slot.ResourceLease,
		Source: s.image.Publication.Capture.Request, SourceResources: s.image.Publication.CPULaunch.Resources, Launch: s.image.Publication.CPULaunch}
	s.evidence.CPURequest = r
	return r, nil
}
func (s *checkpointPlannerStore) CommitNomadCheckpointRestoreCPU(_ context.Context, _ protocol.MigrationCPUPreflightRequest, _ protocol.CheckpointRestoreAuthority, r protocol.MigrationCPUPreflight) error {
	if s.fail == "cpu-commit" {
		return errors.New(s.fail)
	}
	s.evidence.CPU = &r
	return nil
}
func (s *checkpointPlannerStore) AuthorizeNomadCheckpointRestoreImage(context.Context, protocol.CheckpointRestoreAuthority, string) (*protocol.MigrationImagePrepareRequest, error) {
	if s.evidence.CPU == nil {
		return nil, errors.New("missing CPU")
	}
	s.image.Resources = s.slot.ResourceLease
	s.evidence.Image = &s.image
	return &s.image, nil
}
func (s *checkpointPlannerStore) CommitNomadCheckpointRestoreImage(_ context.Context, _ protocol.MigrationImagePrepareRequest, r protocol.MigrationImagePrepared) error {
	if s.fail == "image-commit" {
		return errors.New(s.fail)
	}
	s.evidence.Prepared = &r
	return nil
}
func (s *checkpointPlannerStore) IssueAndBindRuntimeSlotWriterGrant(ctx context.Context, r *sandboxstore.IssueRootFSWriterGrantRequest, b *sandboxstore.BindRuntimeSlotWriterGrantRequest) (*sandboxstore.IssueAndBindRuntimeSlotWriterGrantResult, error) {
	if s.evidence.Prepared == nil {
		return nil, errors.New("writer preceded image")
	}
	if r.OperationID != s.image.OperationID() {
		return nil, errors.New("writer changed restore operation")
	}
	return s.fakeStore.IssueAndBindRuntimeSlotWriterGrant(ctx, r, b)
}
func (s *checkpointPlannerStore) AuthorizeNomadCheckpointRestore(_ context.Context, _ protocol.CheckpointRestoreAuthority, _ string, stage rootfshandoff.StageRequest) (*protocol.MigrationRestoreRequest, error) {
	if s.fail == "restore-authorize" {
		return nil, errors.New(s.fail)
	}
	r := &protocol.MigrationRestoreRequest{Image: s.image, Prepared: *s.evidence.Prepared, Fence: s.fence, Proof: s.proof, Stage: stage}
	return r, r.Validate()
}

type checkpointPlannerNode struct {
	*fakeNode
	preflights, downloads int
	fail                  string
}

func (n *checkpointPlannerNode) PreflightMigrationCPU(_ context.Context, r protocol.MigrationCPUPreflightRequest) (*protocol.MigrationCPUPreflight, error) {
	n.preflights++
	if n.fail == "cpu" {
		return nil, errors.New(n.fail)
	}
	d, err := r.Digest()
	if err != nil {
		return nil, err
	}
	receipt := &protocol.MigrationCPUPreflight{RequestDigest: d, Launch: *r.Launch, Observation: r.Launch.Observation}
	if n.fail == "cpu-receipt" {
		receipt.RequestDigest = strings.Repeat("f", 64)
	}
	return receipt, nil
}
func (n *checkpointPlannerNode) PrepareMigrationImage(_ context.Context, r protocol.MigrationImagePrepareRequest) (*protocol.MigrationImagePrepared, error) {
	n.downloads++
	if n.fail == "image" {
		return nil, errors.New(n.fail)
	}
	d, err := r.Digest()
	if err != nil {
		return nil, err
	}
	receipt := &protocol.MigrationImagePrepared{RequestDigest: d, ManifestDigest: r.Receipt.Reference.ManifestDigest, TotalBytes: 1024}
	if n.fail == "image-receipt" {
		receipt.RequestDigest = strings.Repeat("f", 64)
	}
	return receipt, nil
}
func (n *checkpointPlannerNode) Claim(ctx context.Context, target NodeTarget, r protocol.NodeClaimControlRequest) (protocol.NodeControlResponse, error) {
	response, err := n.fakeNode.Claim(ctx, target, r)
	if err != nil {
		return response, err
	}
	if r.MigrationRestore == nil {
		return response, errors.New("checkpoint invoked cold start")
	}
	d, err := r.MigrationRestore.Digest()
	if err != nil {
		return response, err
	}
	response.MigrationRestore = &protocol.MigrationRestoreObservation{Request: *r.MigrationRestore, RequestDigest: d, State: protocol.MigrationRestoreComplete}
	switch n.fail {
	case "lost":
		n.fail = ""
		return protocol.NodeControlResponse{}, errors.New("lost restore reply")
	case "missing":
		response.MigrationRestore = nil
	case "uncertain":
		response.MigrationRestore.State = protocol.MigrationRestoreUncertain
	}
	return response, nil
}

func checkpointPlannerFixture(t *testing.T, kind runtimecontrol.CheckpointRestoreKind) (*plannerFixture, *checkpointPlannerStore, *checkpointPlannerNode) {
	t.Helper()
	source := newPlannerFixture(t)
	result, err := source.planner.Claim(t.Context(), source.request)
	require.NoError(t, err)
	target := protocol.NodeChannelTarget{SlotID: source.store.slot.ID, ClusterID: source.store.slot.ClusterID, AllocationID: source.store.slot.AllocationID, NodeID: source.store.slot.NodeID, NodeUID: source.store.slot.NodeUID, NodeBootID: source.store.slot.NodeBootID, ControlEndpoint: source.store.slot.ControlEndpoint}
	observation := protocol.MigrationCPUObservation{CPUSet: "0-7", Profile: protocol.MigrationCPUProfile{Version: 1, Architecture: "amd64", RunscVersion: "stock-test", Features: []string{"sse2"}, CacheLineBytes: 64, XStateLayoutDigest: digest.FromString("xstate").String()}}
	launch, err := protocol.BindMigrationCPULaunch(target, source.node.claims[0], observation, digest.FromString("stock-runsc").String())
	require.NoError(t, err)
	c := protocol.MigrationCaptureRequest{Target: target, OperationID: "capture-1", LifecycleEpoch: 2, SandboxID: source.request.SandboxID, SourceGeneration: source.request.Runtime.RuntimeGeneration, AssignmentRevision: launch.AssignmentRevision, BindingDigest: launch.BindingDigest, ResourceLeaseDigest: launch.ResourceLeaseDigest, ProcdInstanceID: "0daa8273-94ad-47a6-9e2d-54ed58d15fd7"}
	captureDigest, err := c.Digest()
	require.NoError(t, err)
	f := newPlannerFixture(t)
	f.request.OperationID = "memory-resume"
	f.request.Runtime.RuntimeGeneration++
	if kind == runtimecontrol.CheckpointFork {
		f.request.SandboxID = "child"
		f.request.Runtime.SandboxID = "child"
		f.request.Runtime.RuntimeGeneration = 1
		f.request.Runtime.EnvVars[runtimecontrol.EnvSandboxID] = "child"
		f.request.NetworkPolicy = strings.ReplaceAll(f.request.NetworkPolicy, "sandbox-1", "child")
		f.store.filesystem.ID = "child"
	}
	f.store.slot.ID = "target-slot"
	f.store.slot.AllocationID = "target-allocation"
	f.store.slot.ControlEndpoint = "unix:///target.sock"
	f.store.generation.ID = "migration-" + captureDigest
	f.store.filesystem.HeadGenerationID = f.store.generation.ID
	generation, err := generationDescriptor(source.store.filesystem, source.store.generation)
	require.NoError(t, err)
	generation.GenerationID = f.store.generation.ID
	cut, err := rootfshandoff.NewMigrationRootFSCut(rootfshandoff.MigrationRootFSCutRequest{OperationID: c.OperationID, CaptureRequestDigest: captureDigest, SourceBindingDigest: c.BindingDigest, GenerationID: generation.GenerationID}, *generation, 1)
	require.NoError(t, err)
	profileDigest, err := launch.GuestCPUProfile().Digest()
	require.NoError(t, err)
	publication := protocol.MigrationPublicationRequest{CheckpointSource: &source.request.Runtime, CPULaunch: launch, CompatibilityDigest: f.request.CompatibilityDigest, CPUFeaturesDigest: profileDigest, Capture: protocol.MigrationCapture{Request: c, RequestDigest: captureDigest, State: protocol.MigrationCaptureComplete, RootFS: &cut}}
	binding, err := publication.Binding()
	require.NoError(t, err)
	bd, err := binding.Digest()
	require.NoError(t, err)
	pd, err := publication.Digest()
	require.NoError(t, err)
	receipt := protocol.MigrationPublication{RequestDigest: pd, Binding: binding, Reference: runtimecheckpoint.Reference{BindingDigest: bd, ManifestDigest: digest.FromString("manifest").String()}}
	capture, err := runtimecontrol.NewCheckpointCaptureAssignment(c.OperationID, source.request.Runtime)
	require.NoError(t, err)
	a := protocol.CheckpointRestoreAuthority{Assignment: runtimecontrol.CheckpointRestoreAssignment{OperationID: f.request.OperationID, Kind: kind, Capture: capture, Target: f.request.Runtime}, LifecycleEpoch: 3, Retained: protocol.CheckpointRetained{CheckpointID: c.OperationID, PublicationRequestDigest: pd, Reference: receipt.Reference}}
	require.NoError(t, a.ValidateFor(publication, receipt))
	image := protocol.MigrationImagePrepareRequest{Publication: publication, Receipt: receipt, Checkpoint: &a, Target: protocol.NodeChannelTarget{SlotID: f.store.slot.ID, ClusterID: f.store.slot.ClusterID, AllocationID: f.store.slot.AllocationID, NodeID: f.store.slot.NodeID, NodeUID: f.store.slot.NodeUID, NodeBootID: f.store.slot.NodeBootID, ControlEndpoint: f.store.slot.ControlEndpoint}}
	fence := protocol.MigrationSourceFenceRequest{PublicationRequest: publication, Publication: receipt}
	detach, err := fence.RootFSRequest()
	require.NoError(t, err)
	root, err := rootfshandoff.NewMigrationRootFSDetachProof(detach, rootfshandoff.CrashFenceSessionObservation{Parent: result.Stage.Parent, RootFSID: generation.FilesystemID, WriterEpoch: generation.WriterEpoch, OperationID: c.OperationID, BindingDigest: c.BindingDigest, SessionState: rootfshandoff.StateTombstoned, BranchPath: "/source.wal", DeviceBound: true, DevicePath: "/dev/nbd0", LiveSessionAbsent: true, MergedMountAbsent: true, XFSMountAbsent: true, ObservedAt: time.Now().UTC().Format(time.RFC3339Nano)})
	require.NoError(t, err)
	fd, err := fence.Digest()
	require.NoError(t, err)
	proof := protocol.MigrationSourceFenceProof{RequestDigest: fd, RootFS: root, ContainerID: protocol.NomadRunscContainerID(target.SlotID), MountNamespaceID: "mnt:source", ContainerAbsent: true, StableMountAbsent: true}
	proof.Digest, err = proof.ProofDigest()
	require.NoError(t, err)
	require.NoError(t, proof.ValidateFor(fence))
	s := &checkpointPlannerStore{fakeStore: f.store, image: image, fence: fence, proof: proof}
	n := &checkpointPlannerNode{fakeNode: f.node}
	f.planner.store = s
	f.planner.node = n
	return f, s, n
}

func TestPlannerCheckpointRetriesShareImageAndExactWriter(t *testing.T) {
	for _, kind := range []runtimecontrol.CheckpointRestoreKind{runtimecontrol.CheckpointResume, runtimecontrol.CheckpointFork} {
		t.Run(string(kind), func(t *testing.T) {
			f, s, n := checkpointPlannerFixture(t, kind)
			var observations []Observation
			f.planner.checkpointObserver = func(o Observation) { observations = append(observations, o) }
			s.issueLost = true
			_, err := f.planner.RestoreCheckpoint(t.Context(), f.request, *s.image.Checkpoint)
			require.ErrorContains(t, err, "writer issue response lost")
			require.Empty(t, n.claims)
			n.fail = "lost"
			_, err = f.planner.RestoreCheckpoint(t.Context(), f.request, *s.image.Checkpoint)
			require.ErrorContains(t, err, "lost restore reply")
			first := cloneNodeClaim(n.claims[0])
			result, err := f.planner.RestoreCheckpoint(t.Context(), f.request, *s.image.Checkpoint)
			require.NoError(t, err)
			require.Equal(t, first, n.claims[1])
			require.Equal(t, 1, n.preflights)
			require.Equal(t, 1, n.downloads)
			require.True(t, s.acquires[0].MemoryRestore)
			require.Equal(t, sandboxstore.MemoryRuntimeSlotClaimTTL, result.Slot.ClaimTTL)
			require.Equal(t, int64(8), s.filesystem.WriterEpoch)
			require.Empty(t, result.CommandProof)
			require.Empty(t, f.prober.addresses)
			require.Empty(t, n.commands)
			require.Empty(t, f.observer.observations)
			require.Len(t, observations, 3)
			require.False(t, observations[2].WithinSLO)
			require.True(t, observations[2].Succeeded)
			require.Equal(t, s.filesystem.ID, result.Stage.Generation.FilesystemID)
			require.Empty(t, result.MigrationRestore.Request.Stage.Identity.WriterGrantToken)
			binding, err := result.Stage.BindingDigest()
			require.NoError(t, err)
			require.Equal(t, hex.EncodeToString(binding[:]), hex.EncodeToString(s.grant.BindingDigest))
		})
	}
}

func TestPlannerCheckpointFailuresCannotIssueEarlyWriterOrColdStart(t *testing.T) {
	for _, failure := range []string{"cpu", "cpu-receipt", "cpu-commit", "image", "image-receipt", "image-commit", "restore-authorize", "missing", "uncertain", "unsupported-store", "unsupported-node", "assignment"} {
		t.Run(failure, func(t *testing.T) {
			f, s, n := checkpointPlannerFixture(t, runtimecontrol.CheckpointResume)
			s.fail = failure
			n.fail = failure
			switch failure {
			case "unsupported-store":
				f.planner.store = f.store
			case "unsupported-node":
				f.planner.node = f.node
			case "assignment":
				f.request.Runtime.SecurityClass = "privileged"
			}
			_, err := f.planner.RestoreCheckpoint(t.Context(), f.request, *s.image.Checkpoint)
			require.Error(t, err)
			require.Empty(t, f.prober.addresses)
			require.Empty(t, n.commands)
			if strings.HasPrefix(failure, "cpu") || strings.HasPrefix(failure, "image") || strings.HasPrefix(failure, "unsupported") || failure == "assignment" {
				require.Empty(t, s.issues)
				require.Empty(t, n.claims)
			}
		})
	}
}

func TestPlannerCheckpointDownloadRetryRetainsCPUAndRejectsChangedCustody(t *testing.T) {
	f, s, n := checkpointPlannerFixture(t, runtimecontrol.CheckpointResume)
	s.fail = "image-commit"
	_, err := f.planner.RestoreCheckpoint(t.Context(), f.request, *s.image.Checkpoint)
	require.ErrorContains(t, err, "image-commit")
	require.Empty(t, s.issues)
	require.Equal(t, 1, n.preflights)
	require.Equal(t, 1, n.downloads)
	s.fail = ""
	original := s.image.Target
	s.image.Target.ControlEndpoint = "unix:///replacement.sock"
	_, err = f.planner.RestoreCheckpoint(t.Context(), f.request, *s.image.Checkpoint)
	require.Error(t, err)
	require.Equal(t, 1, n.downloads, "changed placement cannot receive a download")
	require.Empty(t, s.issues)
	s.image.Target = original
	result, err := f.planner.RestoreCheckpoint(t.Context(), f.request, *s.image.Checkpoint)
	require.NoError(t, err)
	require.NotNil(t, result.MigrationRestore)
	require.Equal(t, 1, n.preflights, "durable image authorization survives lost preparation acknowledgement")
	require.Equal(t, 2, n.downloads, "same request recovers the node's durable preparation receipt")
}

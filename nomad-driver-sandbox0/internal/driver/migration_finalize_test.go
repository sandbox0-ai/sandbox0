package driver

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/containerd/errdefs"
	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/nomad/plugins/drivers"
	"github.com/opencontainers/go-digest"
	"github.com/sandbox0-ai/sandbox0/pkg/nomadruntime"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshandoff"
	"github.com/sandbox0-ai/sandbox0/pkg/runtimecheckpoint"
	"github.com/sandbox0-ai/sandbox0/pkg/runtimecontrol"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
	"github.com/stretchr/testify/require"
)

type finalizationCustodian struct {
	*migrationCustodian
	receipt *protocol.MigrationSourceFinalizationReceipt
	err     error
}

func (c *finalizationCustodian) GetMigrationSourceFinalization(context.Context, string) (*protocol.MigrationSourceFinalizationReceipt, error) {
	return c.receipt, c.err
}

func (c *finalizationCustodian) RuntimeInfo(ctx context.Context) (nomadruntime.RuntimeInfo, error) {
	return c.RootFSRuntime.(rootFSRuntimeInfoProvider).RuntimeInfo(ctx)
}

func migrationFinalizationHandleFixture(t *testing.T) (*taskHandle, *finalizationCustodian, *migrationRunsc) {
	return configuredMigrationFinalizationHandleFixture(t, nil)
}

func configuredMigrationFinalizationHandleFixture(t *testing.T, configure func(*taskHandle, *protocol.MigrationCaptureRequest)) (*taskHandle, *finalizationCustodian, *migrationRunsc) {
	t.Helper()
	h, capture, runner, custody := migrationHandleFixture(t)
	if configure != nil {
		configure(h, &capture)
	}
	h.containerID = protocol.NomadRunscContainerID(capture.Target.SlotID)
	stage := *h.stage
	target := capture.Target
	resources, err := protocol.NewRuntimeResourceLease("source-claim", stage.Identity.ClaimID, target.SlotID, target.ClusterID, target.NodeID, target.NodeUID, target.NodeBootID, protocol.RuntimeResourceRequest{Version: protocol.RuntimeResourceRequestVersion, CPUMillicores: 1000, MemoryBytes: 128 << 20, PIDsLimit: 1024}, "0", "0")
	require.NoError(t, err)
	rd, err := resources.Digest()
	require.NoError(t, err)
	capture.ResourceLeaseDigest = strings.TrimPrefix(rd, "sha256:")
	source := runtimecontrol.Assignment{SandboxID: capture.SandboxID, TeamID: "team", RuntimeGeneration: 1, SecurityClass: "standard"}
	capture.AssignmentRevision, err = source.Revision()
	require.NoError(t, err)
	h.claim.ResourceLeaseDigest = capture.ResourceLeaseDigest
	h.claim.RuntimeRevision = capture.AssignmentRevision
	cd, err := capture.Digest()
	require.NoError(t, err)
	generation := *stage.Generation
	generation.WriterEpoch = stage.Identity.WriterEpoch
	generation.GenerationID = "migration-" + cd
	cut, err := rootfshandoff.NewMigrationRootFSCut(rootfshandoff.MigrationRootFSCutRequest{OperationID: capture.OperationID, CaptureRequestDigest: cd, SourceBindingDigest: capture.BindingDigest, GenerationID: generation.GenerationID}, generation, 1)
	require.NoError(t, err)
	source.RuntimeGeneration++
	publication := protocol.MigrationPublicationRequest{Capture: protocol.MigrationCapture{Request: capture, RequestDigest: cd, State: protocol.MigrationCaptureComplete, RootFS: &cut},
		Assignment:          runtimecontrol.MigrationAssignment{OperationID: capture.OperationID, SourceGeneration: 1, SourceRevision: capture.AssignmentRevision, Target: source},
		CompatibilityDigest: digest.FromString("compatibility").String(), CPUFeaturesDigest: digest.FromString("cpu").String()}
	pd, err := publication.Digest()
	require.NoError(t, err)
	binding, err := publication.Binding()
	require.NoError(t, err)
	bd, err := binding.Digest()
	require.NoError(t, err)
	published := protocol.MigrationPublication{RequestDigest: pd, Binding: binding, Reference: runtimecheckpoint.Reference{BindingDigest: bd, ManifestDigest: digest.FromString("manifest").String()}}
	fence := protocol.MigrationSourceFenceRequest{PublicationRequest: publication, Publication: published}
	detach, err := fence.RootFSRequest()
	require.NoError(t, err)
	detached, err := rootfshandoff.NewMigrationRootFSDetachProof(detach, rootfshandoff.CrashFenceSessionObservation{Parent: stage.Parent, RootFSID: generation.FilesystemID, WriterEpoch: generation.WriterEpoch, OperationID: capture.OperationID, BindingDigest: capture.BindingDigest,
		SessionState: rootfshandoff.StateTombstoned, BranchPath: "/private/source.wal", DeviceBound: true, DevicePath: "/dev/fake0", LiveSessionAbsent: true, MergedMountAbsent: true, XFSMountAbsent: true, ObservedAt: time.Now().UTC().Format(time.RFC3339Nano)})
	require.NoError(t, err)
	fd, err := fence.Digest()
	require.NoError(t, err)
	fenced := protocol.MigrationSourceFenceProof{RequestDigest: fd, RootFS: detached, ContainerID: h.containerID, MountNamespaceID: "mnt:source", ContainerAbsent: true, StableMountAbsent: true}
	fenced.Digest, err = fenced.ProofDigest()
	require.NoError(t, err)
	adoption := protocol.MigrationAdoptionRequest{Target: protocol.NodeChannelTarget{SlotID: "target-slot", ClusterID: target.ClusterID, NodeID: "target-node", NodeUID: "target-uid", NodeBootID: "target-boot", AllocationID: "target-allocation", ControlEndpoint: "unix:///target/control.sock"}, OperationID: capture.OperationID, ClaimID: "target-claim", SandboxID: capture.SandboxID, RuntimeGeneration: 2, ProcdInstanceID: capture.ProcdInstanceID, RestoreDigest: strings.Repeat("a", 64), CommandReadyDigest: strings.Repeat("b", 64)}
	ad, err := adoption.Digest()
	require.NoError(t, err)
	cleanup := protocol.NodeCleanupControlRequest{OperationID: protocol.MigrationSourceCleanupOperationID(capture.OperationID), WriterOperationID: capture.OperationID, WriterRetireKind: protocol.WriterRetireKindMigration,
		SlotID: target.SlotID, ClusterID: target.ClusterID, AllocationID: target.AllocationID, NodeID: target.NodeID, NodeUID: target.NodeUID, NodeBootID: target.NodeBootID, NetNSIdentity: stage.ExpectedPolicyToken.NetNSIdentity,
		RunscContainerID: h.containerID, WriterGrantID: stage.Identity.WriterGrantID, WriterAuthorityDigest: fenced.Digest, Resources: resources, ResourceLeaseDigest: capture.ResourceLeaseDigest}
	request := protocol.MigrationSourceFinalizeRequest{Fence: fence, SourceProof: fenced, Adoption: protocol.MigrationAdoptionReceipt{Request: adoption, Proof: protocol.MigrationAdoptionProof{RequestDigest: ad, ImageAbsent: true}}, Cleanup: cleanup}
	rootRequest, err := request.RootFSRequest()
	require.NoError(t, err)
	root := rootfshandoff.MigrationRootFSFinalizeProof{Request: rootRequest, Parent: stage.Parent, BindingDigest: capture.BindingDigest, BranchAbsent: true, MountDirectoriesAbsent: true}
	root.Digest, err = root.ProofDigest()
	require.NoError(t, err)
	c := cleanup
	proof := protocol.NodeCleanupControlProof{Version: protocol.NodeCleanupProofVersion, OperationID: c.OperationID, WriterOperationID: c.WriterOperationID, WriterRetireKind: c.WriterRetireKind,
		SlotID: c.SlotID, ClusterID: c.ClusterID, AllocationID: c.AllocationID, NodeID: c.NodeID, NodeUID: c.NodeUID, NodeBootID: c.NodeBootID, NetNSIdentity: c.NetNSIdentity,
		RunscContainerID: c.RunscContainerID, WriterGrantID: c.WriterGrantID, WriterAuthorityDigest: c.WriterAuthorityDigest, RootFSOperationID: c.WriterOperationID, RootFSProofDigest: root.Digest,
		Resources: c.Resources, ResourceLeaseID: c.Resources.LeaseID, ResourceLeaseDigest: c.ResourceLeaseDigest, RunscAbsent: true, StableMountAbsent: true, RootFSWriterAbsent: true, NetworkPolicyAbsent: true, ResourceCgroupAbsent: true}
	proof.ProofDigest, err = proof.Digest()
	require.NoError(t, err)
	requestDigest, err := request.Digest()
	require.NoError(t, err)
	receipt := &protocol.MigrationSourceFinalizationReceipt{Request: request, Proof: protocol.MigrationSourceFinalizeProof{RequestDigest: requestDigest, RootFS: root, ImageAbsent: true, Cleanup: proof}}
	require.NoError(t, receipt.Validate())
	localCapture := publication.Capture
	localCapture.RootFS = nil
	h.migration = &localCapture
	h.phase = phaseMigrating
	custody.custody = &nomadruntime.MigrationCaptureCustody{Capture: publication.Capture, Finalization: &nomadruntime.MigrationSourceFinalization{}}
	finalizer := &finalizationCustodian{migrationCustodian: custody, receipt: receipt}
	h.rootfs = finalizer
	require.NoError(t, h.persist())
	return h, finalizer, runner
}

func TestMigrationFinalizationRecoversWarmNomadHandleAfterBundleDeletion(t *testing.T) {
	fixture := newRuntimeSlotPluginFixture(t)
	t.Cleanup(fixture.plugin.cancel)
	h, c, runner := configuredMigrationFinalizationHandleFixture(t, func(h *taskHandle, capture *protocol.MigrationCaptureRequest) {
		h.taskConfig = fixture.task
		h.bundleDir = filepath.Join(fixture.task.TaskDir().Dir, "gvisor-bundle")
		h.rootMount = filepath.Join(h.bundleDir, "rootfs")
		h.socketPath = controlSocketPath(fixture.config.ControlDir, fixture.task.ID)
		capture.Target.AllocationID = fixture.task.AllocID
		capture.Target.ControlEndpoint = "unix://" + h.socketPath
		require.NoError(t, os.MkdirAll(h.bundleDir, 0700))
	})
	state := h.PersistedState()
	state.Claim, state.Migration = nil, nil
	state.RootMounted, state.Phase = false, phaseWarm
	nomadHandle := drivers.NewTaskHandle(taskHandleVersion)
	nomadHandle.Config = fixture.task
	require.NoError(t, nomadHandle.SetDriverState(state))
	nomadHandle = roundTripNomadTaskHandleDatabase(t, nomadHandle)
	require.NoError(t, h.Close(true))
	fixture.plugin.rootfs = c
	fixture.plugin.newRunner = func(PluginConfig) Runsc { return runner }
	// Model repeated crashes after bundle removal, before Nomad forgets its
	// original handle. Neither recovery may register or reopen a control socket.
	for range 2 {
		require.NoError(t, fixture.plugin.RecoverTask(nomadHandle))
		recovered, ok := fixture.plugin.tasks.Get(fixture.task.ID)
		require.True(t, ok)
		require.False(t, recovered.IsRunning())
		require.Nil(t, recovered.stage)
		require.Nil(t, recovered.claim)
		require.False(t, recovered.rootMounted)
		require.NoDirExists(t, recovered.bundleDir)
		require.NoFileExists(t, recovered.socketPath)
		result := <-recovered.WaitChannel(t.Context())
		require.NoError(t, result.Err)
		require.Zero(t, result.ExitCode)
		require.NoError(t, fixture.plugin.StopTask(fixture.task.ID, 0, "KILL"))
		require.NoError(t, fixture.plugin.DestroyTask(fixture.task.ID, true))
	}
	calls, _, _, _ := fixture.authority.snapshot()
	require.Empty(t, calls)
	require.Empty(t, runner.callsSnapshot())
}

func TestMigrationFinalizationWarmRecoveryRequiresExactCompletedReceipt(t *testing.T) {
	for _, failure := range []string{"slot", "allocation", "node", "socket", "container", "pending", "unavailable", "incomplete-proof", "partial-claim", "mounted-without-claim", "changed-capture"} {
		t.Run(failure, func(t *testing.T) {
			h, c, runner := migrationFinalizationHandleFixture(t)
			state := h.PersistedState()
			state.TaskConfig = state.TaskConfig.Copy()
			state.Claim, state.Migration = nil, nil
			state.RootMounted, state.Phase = false, phaseWarm
			require.NoError(t, os.RemoveAll(h.bundleDir))
			switch failure {
			case "slot":
				state.TaskConfig.ID = "another-slot"
			case "allocation":
				state.TaskConfig.AllocID = "another-allocation"
			case "node":
				state.TaskConfig.NodeID = "another-node"
			case "socket":
				h.socketPath += ".changed"
			case "container":
				h.containerID += "changed"
			case "pending":
				c.receipt = nil
			case "unavailable":
				c.err = errdefs.ErrUnavailable
			case "incomplete-proof":
				c.receipt.Proof.Cleanup.ResourceCgroupAbsent = false
			case "partial-claim":
				state.Claim = &claimMetadata{SandboxID: "unverified"}
			case "mounted-without-claim":
				state.RootMounted = true
			case "changed-capture":
				state.Migration = cloneMigrationCapture(h.migration)
				state.Migration.Request.OperationID = "another-operation"
				var err error
				state.Migration.RequestDigest, err = state.Migration.Request.Digest()
				require.NoError(t, err)
			}
			require.Error(t, h.Recover(state))
			require.False(t, h.migrationFinalized)
			require.Empty(t, runner.callsSnapshot())
			require.NoDirExists(t, h.bundleDir)
		})
	}
}

func TestMigrationFinalizationPermitsStopAndCloseWithoutRepeatingPhysicalCleanup(t *testing.T) {
	for _, stop := range []bool{false, true} {
		t.Run(map[bool]string{false: "direct-close", true: "stop-then-close"}[stop], func(t *testing.T) {
			h, _, runner := migrationFinalizationHandleFixture(t)
			if stop {
				require.NoError(t, h.Stop(0, "KILL"))
				require.False(t, h.IsRunning())
				result := <-h.WaitChannel(t.Context())
				require.NoError(t, result.Err)
				require.Zero(t, result.ExitCode)
				_, err := h.CaptureMigration(t.Context(), h.migration.Request)
				require.ErrorIs(t, err, errdefs.ErrFailedPrecondition)
				require.True(t, h.fenceMigrationExecution(errors.New("late lease loss")))
				require.Equal(t, phaseExited, h.PersistedState().Phase)
			}
			require.NoError(t, h.Close(true))
			require.NoError(t, h.Close(true))
			require.NoDirExists(t, h.bundleDir)
			require.Empty(t, runner.callsSnapshot())
			_, unmounts := h.mounter.(*fakeMounter).snapshot()
			require.Empty(t, unmounts)
		})
	}
}

func TestMigrationFinalizationRejectsIncompleteUnavailableAndChangedReceipt(t *testing.T) {
	for _, failure := range []string{"pending", "unavailable", "invalid-proof", "different-source", "in-flight"} {
		t.Run(failure, func(t *testing.T) {
			h, c, runner := migrationFinalizationHandleFixture(t)
			switch failure {
			case "pending":
				c.receipt = nil
			case "unavailable":
				c.err = errdefs.ErrUnavailable
			case "invalid-proof":
				c.receipt.Proof.Cleanup.ResourceCgroupAbsent = false
			case "different-source":
				h.claim.ProcdInstanceID = "another-process"
			case "in-flight":
				h.migrationInFlight = true
			}
			require.Error(t, h.Stop(0, "KILL"))
			require.Error(t, h.Close(true))
			require.DirExists(t, h.bundleDir)
			require.Empty(t, runner.callsSnapshot())
			require.True(t, h.IsRunning())
		})
	}
}

func TestMigrationFinalizationRecoveryUsesCtldBeforeGenericCrashCleanup(t *testing.T) {
	for _, stale := range []bool{false, true} {
		t.Run(map[bool]string{false: "persisted-exit", true: "pre-capture-state"}[stale], func(t *testing.T) {
			h, _, runner := migrationFinalizationHandleFixture(t)
			state := h.PersistedState()
			if stale {
				state.Migration = nil
				state.Phase = phaseActive
				require.NoError(t, writePersistedState(state, h.statePath()))
			} else {
				require.NoError(t, h.Stop(0, "KILL"))
				state = h.PersistedState()
			}
			h.migrationFinalized = false
			h.migration = nil
			require.NoError(t, h.Recover(state))
			require.False(t, h.IsRunning())
			require.Equal(t, phaseExited, h.PersistedState().Phase)
			require.NoError(t, h.Close(true))
			require.Empty(t, runner.callsSnapshot())
		})
	}
}

func TestMigrationFinalizationNomadGCRespectsProofAndRetriesBundleCleanup(t *testing.T) {
	h, c, runner := migrationFinalizationHandleFixture(t)
	plugin := newPlugin(hclog.NewNullLogger(), func(PluginConfig) Runsc { return runner }).(*Plugin)
	t.Cleanup(plugin.cancel)
	plugin.tasks.Set(h.taskConfig.ID, h)
	receipt := c.receipt
	c.receipt = nil
	require.Error(t, plugin.DestroyTask(h.taskConfig.ID, true))
	_, exists := plugin.tasks.Get(h.taskConfig.ID)
	require.True(t, exists, "Nomad cannot force away unresolved migration custody")
	c.receipt = receipt
	require.NoError(t, plugin.StopTask(h.taskConfig.ID, 0, "KILL"))
	// Simulate local bundle cleanup failure after the physical node receipt.
	// The handle must remain available for GC retry without repeating ctld work.
	require.NoError(t, os.Mkdir(h.socketPath, 0700))
	require.NoError(t, os.WriteFile(filepath.Join(h.socketPath, "busy"), []byte("x"), 0600))
	require.Error(t, plugin.DestroyTask(h.taskConfig.ID, false))
	_, exists = plugin.tasks.Get(h.taskConfig.ID)
	require.True(t, exists)
	require.NoError(t, plugin.DestroyTask(h.taskConfig.ID, false))
	_, exists = plugin.tasks.Get(h.taskConfig.ID)
	require.False(t, exists)
	require.Empty(t, runner.callsSnapshot())
}

package driver

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/containerd/errdefs"
	"github.com/opencontainers/go-digest"
	"github.com/sandbox0-ai/sandbox0/pkg/nomadruntime"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshandoff"
	rootfssession "github.com/sandbox0-ai/sandbox0/pkg/rootfssession"
	"github.com/sandbox0-ai/sandbox0/pkg/runtimecheckpoint"
	"github.com/sandbox0-ai/sandbox0/pkg/runtimecontrol"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
	"github.com/stretchr/testify/require"
)

type restoreRunsc struct {
	*migrationRunsc
	beforeRestore func()
	restoreErr    error
	ignoreCancel  bool
}

func (r *restoreRunsc) Restore(ctx context.Context, _, _ string) error {
	r.record("restore")
	if r.beforeRestore != nil {
		r.beforeRestore()
	}
	if err := ctx.Err(); err != nil && !r.ignoreCancel {
		return err
	}
	r.setState("running")
	return r.restoreErr
}
func (r *restoreRunsc) State(ctx context.Context, id string) (RunscState, error) {
	state, err := r.fakeRunsc.State(ctx, id)
	state.ID = id
	return state, err
}

type restoreCustodian struct {
	RootFSRuntime
	mu                   sync.Mutex
	custody              nomadruntime.MigrationDestinationCustody
	failComplete         bool
	adoptionHook         func()
	loseAdoptionResponse bool
	adoptionReadError    error
	adoptionReads        int
}

func (c *restoreCustodian) GetMigrationDestination(context.Context, string) (*nomadruntime.MigrationDestinationCustody, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.adoptionReads++
	if c.adoptionReadError != nil {
		return nil, c.adoptionReadError
	}
	copy := c.custody
	if copy.Restore != nil {
		restore := *copy.Restore
		copy.Restore = &restore
	}
	return &copy, nil
}
func (c *restoreCustodian) RecordMigrationRestore(_ context.Context, observation protocol.MigrationRestoreObservation) error {
	if err := observation.Validate(); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.custody.Adoption != nil || c.custody.Failure != nil {
		return errdefs.ErrFailedPrecondition
	}
	prior := c.custody.Restore
	if prior != nil && (prior.RequestDigest != observation.RequestDigest || prior.State == protocol.MigrationRestoreUncertain && observation.State != prior.State) {
		return errdefs.ErrFailedPrecondition
	}
	if c.failComplete && observation.State == protocol.MigrationRestoreComplete {
		return errors.New("completion response lost")
	}
	c.custody.Restore = &observation
	return nil
}

func migrationRestoreHandleFixture(t *testing.T) (*taskHandle, ClaimRequest, *restoreRunsc, *restoreCustodian, *runtimeSlotPluginFixture) {
	t.Helper()
	return migrationRestoreHandleCPUFixture(t, true)
}

func migrationRestoreHandleCPUFixture(t *testing.T, retainCPUHistory bool) (*taskHandle, ClaimRequest, *restoreRunsc, *restoreCustodian, *runtimeSlotPluginFixture) {
	t.Helper()
	fixture := newRuntimeSlotPluginFixture(t)
	handle, stage, token, policy, _ := prepareRuntimeSlotClaim(t, fixture)
	source := runtimeSlotAssignment()
	sourceRevision, err := source.Revision()
	require.NoError(t, err)
	targetAssignment := *source
	targetAssignment.RuntimeGeneration++
	targetRevision, err := targetAssignment.Revision()
	require.NoError(t, err)
	captureRequest := protocol.MigrationCaptureRequest{
		Target:      protocol.NodeChannelTarget{SlotID: "source-slot", ClusterID: "cluster-1", NodeID: "source-node", NodeUID: "source-uid", NodeBootID: "source-boot", AllocationID: "source-allocation", ControlEndpoint: "unix:///source/control.sock"},
		OperationID: "operation-1", LifecycleEpoch: 2, SandboxID: source.SandboxID, SourceGeneration: source.RuntimeGeneration,
		AssignmentRevision: sourceRevision, BindingDigest: strings.Repeat("ab", 32), ResourceLeaseDigest: strings.Repeat("cd", 32), ProcdInstanceID: "source-procd",
	}
	launch := migrationExecutionCPUFixture(t, &captureRequest, "source-launch")
	profile, err := launch.Observation.Profile.Digest()
	require.NoError(t, err)
	captureDigest, err := captureRequest.Digest()
	require.NoError(t, err)
	generation := *stage.Generation
	generation.GenerationID, generation.WriterEpoch = "migration-"+captureDigest, 1
	cut, err := rootfshandoff.NewMigrationRootFSCut(rootfshandoff.MigrationRootFSCutRequest{OperationID: "operation-1", CaptureRequestDigest: captureDigest, SourceBindingDigest: captureRequest.BindingDigest, GenerationID: generation.GenerationID}, generation, 1)
	require.NoError(t, err)
	publication := protocol.MigrationPublicationRequest{
		Capture:             protocol.MigrationCapture{Request: captureRequest, RequestDigest: captureDigest, State: protocol.MigrationCaptureComplete, RootFS: &cut},
		Assignment:          runtimecontrol.MigrationAssignment{OperationID: "operation-1", SourceGeneration: 1, SourceRevision: sourceRevision, Target: targetAssignment},
		CompatibilityDigest: digest.FromString("compatibility").String(), CPUFeaturesDigest: profile, CPULaunch: launch,
	}
	if !retainCPUHistory {
		publication.CPULaunch = nil
	}
	binding, err := publication.Binding()
	require.NoError(t, err)
	bindingDigest, err := binding.Digest()
	require.NoError(t, err)
	publicationDigest, err := publication.Digest()
	require.NoError(t, err)
	receipt := protocol.MigrationPublication{RequestDigest: publicationDigest, Binding: binding, Reference: runtimecheckpoint.Reference{BindingDigest: bindingDigest, ManifestDigest: digest.FromString("manifest").String()}}
	resources := runtimeSlotResourceLease(t, fixture, stage)
	image := protocol.MigrationImagePrepareRequest{Publication: publication, Receipt: receipt, Resources: resources,
		Target: protocol.NodeChannelTarget{SlotID: fixture.task.ID, AllocationID: fixture.task.AllocID, ClusterID: "cluster-1", NodeID: fixture.task.NodeID, NodeUID: stage.Identity.NodeUID, NodeBootID: stage.Identity.BootID, ControlEndpoint: "unix://" + handle.socketPath}}
	imageDigest, err := image.Digest()
	require.NoError(t, err)
	prepared := protocol.MigrationImagePrepared{RequestDigest: imageDigest, ManifestDigest: receipt.Reference.ManifestDigest, TotalBytes: 128}
	fence := protocol.MigrationSourceFenceRequest{PublicationRequest: publication, Publication: receipt}
	detach, err := fence.RootFSRequest()
	require.NoError(t, err)
	rootProof, err := rootfshandoff.NewMigrationRootFSDetachProof(detach, rootfshandoff.CrashFenceSessionObservation{Parent: stage.Parent, RootFSID: generation.FilesystemID, WriterEpoch: 1,
		OperationID: "operation-1", BindingDigest: captureRequest.BindingDigest, SessionState: rootfshandoff.StateTombstoned, BranchPath: "/private/source.wal", DeviceBound: true, DevicePath: "/dev/fake0",
		LiveSessionAbsent: true, MergedMountAbsent: true, XFSMountAbsent: true, ObservedAt: time.Now().UTC().Format(time.RFC3339Nano)})
	require.NoError(t, err)
	fenceDigest, err := fence.Digest()
	require.NoError(t, err)
	proof := protocol.MigrationSourceFenceProof{RequestDigest: fenceDigest, RootFS: rootProof, ContainerID: protocol.NomadRunscContainerID("source-slot"), MountNamespaceID: "mnt:source", ContainerAbsent: true, StableMountAbsent: true}
	proof.Digest, err = proof.ProofDigest()
	require.NoError(t, err)
	stage.InitialGeneration, stage.Generation = generation.GenerationID, &generation
	stage.Identity.WriterEpoch, stage.Identity.RuntimeGeneration = 2, "2"
	stage.Labels[protocol.RuntimeAssignmentRevisionLabel] = targetRevision
	restore := protocol.MigrationRestoreRequest{Image: image, Prepared: prepared, Fence: fence, Proof: proof, Stage: stage.WithoutWriterGrantToken()}
	require.NoError(t, restore.Validate())
	claim := ClaimRequest{OperationID: "operation-1", ClaimID: "claim-1", PolicyToken: token, WriterEpoch: "2", Stage: &stage, NetworkPolicy: policy, Runtime: &targetAssignment, Resources: resources, MigrationRestore: &restore}
	require.NoError(t, claim.ValidateRegional())
	runner := &restoreRunsc{migrationRunsc: &migrationRunsc{fakeRunsc: fixture.runner, executable: launch.ExecutableDigest, cpuObservation: launch.Observation}}
	custodian := &restoreCustodian{RootFSRuntime: fixture.rootfs, custody: nomadruntime.MigrationDestinationCustody{Request: image, RequestDigest: imageDigest, ImageDirectory: t.TempDir(), Prepared: &prepared}}
	handle.runner, handle.rootfs = runner, custodian
	restoreDigest, err := restore.Digest()
	require.NoError(t, err)
	fixture.authority.mu.Lock()
	fixture.authority.adoption = &protocol.MigrationAdoptionRequest{Target: image.Target, OperationID: publication.Assignment.OperationID,
		ClaimID: resources.ClaimID, SandboxID: targetAssignment.SandboxID, RuntimeGeneration: targetAssignment.RuntimeGeneration,
		ProcdInstanceID: captureRequest.ProcdInstanceID, RestoreDigest: restoreDigest}
	fixture.authority.mu.Unlock()
	t.Cleanup(func() {
		handle.stopExitWatch()
		handle.stopConsumerRenewal()
		fixture.plugin.cancel()
		handle.stopControl()
		handle.mu.Lock()
		handle.closed = true
		handle.mu.Unlock()
	})
	return handle, claim, runner, custodian, fixture
}

func TestMigrationRestoreClaimExecutesOnceAndNeverStartsEntrypoint(t *testing.T) {
	handle, claim, runner, custodian, fixture := migrationRestoreHandleFixture(t)
	entered, resume := make(chan struct{}), make(chan struct{})
	runner.beforeRestore = func() { close(entered); <-resume }
	first := make(chan error, 1)
	go func() { first <- handle.Claim(claim) }()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("restore did not start")
	}
	custody, err := custodian.GetMigrationDestination(t.Context(), fixture.task.ID)
	require.NoError(t, err)
	require.Equal(t, protocol.MigrationRestoreExecuting, custody.Restore.State)
	second := make(chan error, 1)
	go func() { second <- handle.Claim(claim) }()
	close(resume)
	require.NoError(t, <-first)
	require.NoError(t, <-second)
	require.NoError(t, handle.Claim(claim), "lost response retries must not restore twice")
	require.Equal(t, 1, countMigrationCall(runner.callsSnapshot(), "restore"))
	require.Equal(t, 1, countMigrationCall(runner.callsSnapshot(), "create"))
	require.NotContains(t, runner.callsSnapshot(), "start")
	starts := fixture.authority.startingSnapshot()
	require.NotEmpty(t, starts)
	want, err := claim.MigrationRestore.Digest()
	require.NoError(t, err)
	require.Equal(t, want, starts[0].MigrationRestoreDigest)
	require.ErrorIs(t, handle.Close(true), errdefs.ErrFailedPrecondition)
	require.ErrorIs(t, handle.Signal("TERM"), errdefs.ErrFailedPrecondition)
	wrongTarget := claim.MigrationRestore.Image.Target
	wrongTarget.ControlEndpoint = "unix:///replacement.sock"
	_, err = protocol.NewNodeChannelClaimCommand(wrongTarget, claim)
	require.ErrorContains(t, err, "exact destination")
}

func TestMigrationRestoreFailureFencesWithoutFallbackOrCleanup(t *testing.T) {
	for _, responseLoss := range []bool{false, true} {
		name := "runsc response"
		if responseLoss {
			name = "journal completion"
		}
		t.Run(name, func(t *testing.T) {
			handle, claim, runner, custodian, _ := migrationRestoreHandleFixture(t)
			if responseLoss {
				custodian.failComplete = true
			} else {
				runner.restoreErr = errors.New("restore response lost")
			}
			require.Error(t, handle.Claim(claim))
			require.Eventually(t, func() bool { return contains(runner.callsSnapshot(), "kill:KILL") }, 3*time.Second, 10*time.Millisecond)
			require.Error(t, handle.Claim(claim))
			require.Equal(t, 1, countMigrationCall(runner.callsSnapshot(), "restore"))
			require.NotContains(t, runner.callsSnapshot(), "start")
			require.NotContains(t, runner.callsSnapshot(), "delete:force")
			require.True(t, handle.PersistedState().RootMounted)
			custody, err := custodian.GetMigrationDestination(t.Context(), claim.Resources.SlotID)
			require.NoError(t, err)
			require.Equal(t, protocol.MigrationRestoreUncertain, custody.Restore.State)
		})
	}
}

func TestMigrationRestoreRecoveryUsesCtldCustodyWithStaleLocalState(t *testing.T) {
	handle, claim, runner, custodian, _ := migrationRestoreHandleFixture(t)
	require.NoError(t, handle.Claim(claim))
	handle.stopExitWatch()
	handle.stopConsumerRenewal()
	state := handle.PersistedState()
	state.Claim = nil
	handle.mu.Lock()
	handle.claim = nil
	handle.mu.Unlock()
	handled, err := handle.recoverMigrationRestore(state)
	require.True(t, handled)
	require.NoError(t, err)
	require.Contains(t, runner.callsSnapshot(), "kill:KILL")
	require.NotContains(t, runner.callsSnapshot(), "delete:force")
	require.Equal(t, phaseMigrating, handle.PersistedState().Phase)
	custody, err := custodian.GetMigrationDestination(t.Context(), claim.Resources.SlotID)
	require.NoError(t, err)
	require.Equal(t, protocol.MigrationRestoreUncertain, custody.Restore.State)
	require.Error(t, handle.Claim(claim))
	require.Equal(t, 1, countMigrationCall(runner.callsSnapshot(), "restore"))
}

func TestMigrationFailureRecoveryFencesStaleWarmHandle(t *testing.T) {
	for _, restored := range []bool{false, true} {
		t.Run(map[bool]string{false: "before-restore", true: "after-restore"}[restored], func(t *testing.T) {
			handle, claim, runner, custodian, _ := migrationRestoreHandleFixture(t)
			if restored {
				require.NoError(t, handle.Claim(claim))
				handle.stopExitWatch()
				handle.stopConsumerRenewal()
			} else {
				runner.mu.Lock()
				runner.stateErr = errdefs.ErrNotFound
				runner.mu.Unlock()
			}
			request := protocol.MigrationFailureRequest{Restore: *claim.MigrationRestore, Reason: protocol.MigrationFailureTermination}
			digest, err := request.Digest()
			require.NoError(t, err)
			custodian.mu.Lock()
			custodian.custody.Failure = &nomadruntime.MigrationFailureStopCustody{Request: request, RequestDigest: digest}
			custodian.mu.Unlock()
			state := handle.PersistedState()
			state.Claim = nil
			state.RootMounted = false
			handle.mu.Lock()
			handle.claim = nil
			handle.mu.Unlock()
			handled, err := handle.recoverMigrationRestore(state)
			require.True(t, handled)
			require.NoError(t, err)
			require.Equal(t, phaseMigrating, handle.PersistedState().Phase)
			require.Error(t, handle.Claim(claim))
			_, err = handle.migrationRestoreReceipt(t.Context(), *claim.MigrationRestore)
			require.ErrorIs(t, err, errdefs.ErrFailedPrecondition)
			want := 0
			if restored {
				want = 1
			}
			require.Equal(t, want, countMigrationCall(runner.callsSnapshot(), "restore"))
			require.NotContains(t, runner.callsSnapshot(), "start")
		})
	}
}

func TestMigrationRestoreLeaseFenceCannotAcknowledgeActiveRetry(t *testing.T) {
	handle, claim, runner, custodian, _ := migrationRestoreHandleFixture(t)
	require.NoError(t, handle.Claim(claim))
	require.True(t, handle.fenceMigrationExecution(errors.New("writer lease expired")))
	require.Error(t, handle.Claim(claim))
	require.Eventually(t, func() bool { return contains(runner.callsSnapshot(), "kill:KILL") }, 3*time.Second, 10*time.Millisecond)
	custody, err := custodian.GetMigrationDestination(t.Context(), claim.Resources.SlotID)
	require.NoError(t, err)
	require.Equal(t, protocol.MigrationRestoreUncertain, custody.Restore.State)
	require.Equal(t, phaseMigrating, handle.PersistedState().Phase)
}

func TestMigrationRestoreLeaseFenceWaitsForInFlightRestore(t *testing.T) {
	handle, claim, runner, _, _ := migrationRestoreHandleFixture(t)
	entered, resume := make(chan struct{}), make(chan struct{})
	runner.ignoreCancel = true
	runner.beforeRestore = func() { close(entered); <-resume }
	finished := make(chan error, 1)
	go func() { finished <- handle.Claim(claim) }()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("restore did not start")
	}
	require.True(t, handle.fenceMigrationExecution(errors.New("node authority expired")))
	require.Eventually(t, func() bool { return contains(runner.callsSnapshot(), "kill:KILL") }, 3*time.Second, 10*time.Millisecond)
	// A cancelled runsc client may report after execution resumes. The first
	// stopped observation cannot finish fencing while Restore is in flight.
	close(resume)
	require.Error(t, <-finished)
	require.Eventually(t, func() bool { return countMigrationCall(runner.callsSnapshot(), "kill:KILL") >= 2 }, 3*time.Second, 10*time.Millisecond)
	require.NotContains(t, runner.callsSnapshot(), "start")
	require.NotContains(t, runner.callsSnapshot(), "delete:force")
}

func TestMigrationHandoverClaimSocketReturnsExactCtldReceipt(t *testing.T) {
	handle, claim, runner, _, fixture := migrationRestoreHandleFixture(t)
	client, err := protocol.NewNodeClient(protocol.NodeClientConfig{AllowedSocketRoot: fixture.config.ControlDir})
	require.NoError(t, err)
	result, err := client.Claim(t.Context(), "unix://"+handle.socketPath, claim)
	require.NoError(t, err)
	require.NoError(t, result.ValidateClaimResult(claim))
	require.NotNil(t, result.MigrationRestore)
	require.Equal(t, protocol.MigrationRestoreComplete, result.MigrationRestore.State)
	require.Equal(t, *claim.MigrationRestore, result.MigrationRestore.Request)
	command, err := protocol.NewNodeChannelClaimCommand(claim.MigrationRestore.Image.Target, claim)
	require.NoError(t, err)
	channelResult := protocol.NodeChannelResult{Version: protocol.NodeChannelVersion, RequestID: command.RequestID, Kind: command.Kind, ControlResponse: &result}
	require.NoError(t, channelResult.ValidateFor(command))
	result.MigrationRestore = nil
	require.Error(t, channelResult.ValidateFor(command), "ordinary activation cannot authorize a restored generation")
	retry, err := client.Claim(t.Context(), "unix://"+handle.socketPath, claim)
	require.NoError(t, err)
	require.NotNil(t, retry.MigrationRestore)
	require.Equal(t, 1, countMigrationCall(runner.callsSnapshot(), "restore"))
}

func TestMigrationHandoverCommandReadinessBindsOriginalProcdAndRestore(t *testing.T) {
	handle, claim, _, custodian, fixture := migrationRestoreHandleFixture(t)
	require.NoError(t, handle.Claim(claim))
	proof := commandReadyProof(fixture, *claim.Stage)
	require.ErrorIs(t, handle.CommandReady(CommandReadyRequest{Proof: proof}), errdefs.ErrFailedPrecondition, "new procd cannot satisfy memory-preserving handover")
	require.Empty(t, fixture.authority.commandSnapshot())
	proof.ProcdInstanceID = claim.MigrationRestore.Image.Publication.Capture.Request.ProcdInstanceID
	require.NoError(t, handle.CommandReady(CommandReadyRequest{Proof: proof}))
	requests := fixture.authority.commandSnapshot()
	require.Len(t, requests, 1)
	want, err := claim.MigrationRestore.Digest()
	require.NoError(t, err)
	require.Equal(t, want, requests[0].MigrationRestoreDigest)
	custodian.mu.Lock()
	custodian.custody.Restore.State = protocol.MigrationRestoreUncertain
	custodian.mu.Unlock()
	require.ErrorIs(t, handle.CommandReady(CommandReadyRequest{Proof: proof}), errdefs.ErrFailedPrecondition)
	require.Len(t, fixture.authority.commandSnapshot(), 1, "uncertain custody cannot reuse an earlier ready observation")
}

// restorePreparationGate exposes the two independent node operations without
// changing the custody fixture's serialization of durable observations.
type restorePreparationGate struct {
	*restoreCustodian
	intent func(context.Context) error
	attach func(context.Context) error
}

func (c *restorePreparationGate) RecordMigrationRestore(ctx context.Context, observation protocol.MigrationRestoreObservation) error {
	if observation.State == protocol.MigrationRestoreIntent && c.intent != nil {
		if err := c.intent(ctx); err != nil {
			return err
		}
	}
	return c.restoreCustodian.RecordMigrationRestore(ctx, observation)
}

func (c *restorePreparationGate) Ensure(ctx context.Context, stage rootfshandoff.StageRequest, loss func(error)) (rootfssession.Mount, error) {
	if c.attach != nil {
		if err := c.attach(ctx); err != nil {
			return rootfssession.Mount{}, err
		}
	}
	return c.RootFSRuntime.Ensure(ctx, stage, loss)
}

func TestMigrationRestoreOverlapsImageVerificationAndRootFSAttach(t *testing.T) {
	for _, first := range []string{"image", "rootfs"} {
		t.Run(first+" finishes first", func(t *testing.T) {
			h, claim, runner, custodian, _ := migrationRestoreHandleFixture(t)
			imageEntered, attachEntered := make(chan struct{}), make(chan struct{})
			imageRelease, attachRelease := make(chan struct{}), make(chan struct{})
			var imageOnce, attachOnce sync.Once
			releaseImage := func() { imageOnce.Do(func() { close(imageRelease) }) }
			releaseAttach := func() { attachOnce.Do(func() { close(attachRelease) }) }
			t.Cleanup(releaseImage)
			t.Cleanup(releaseAttach)
			h.rootfs = &restorePreparationGate{restoreCustodian: custodian,
				intent: func(ctx context.Context) error {
					close(imageEntered)
					select {
					case <-imageRelease:
						return nil
					case <-ctx.Done():
						return ctx.Err()
					}
				},
				attach: func(ctx context.Context) error {
					close(attachEntered)
					select {
					case <-attachRelease:
						return nil
					case <-ctx.Done():
						return ctx.Err()
					}
				},
			}
			done := make(chan error, 1)
			go func() { done <- h.Claim(claim) }()
			for _, entered := range []chan struct{}{imageEntered, attachEntered} {
				select {
				case <-entered:
				case <-time.After(5 * time.Second):
					t.Fatal("preparation did not overlap")
				}
			}
			if first == "image" {
				releaseImage()
			} else {
				releaseAttach()
			}
			require.NotContains(t, runner.callsSnapshot(), "create", "both preparation operations must finish before execution")
			select {
			case err := <-done:
				t.Fatalf("claim completed before preparation: %v", err)
			default:
			}
			releaseImage()
			releaseAttach()
			select {
			case err := <-done:
				require.NoError(t, err)
			case <-time.After(5 * time.Second):
				t.Fatal("claim did not finish")
			}
			require.Equal(t, 1, countMigrationCall(runner.callsSnapshot(), "restore"))
			require.NotContains(t, runner.callsSnapshot(), "start")
		})
	}
}

func TestMigrationRestorePreparationFailureCancelsAndJoinsPeer(t *testing.T) {
	for _, failing := range []string{"image", "rootfs"} {
		t.Run(failing, func(t *testing.T) {
			h, claim, runner, custodian, fixture := migrationRestoreHandleFixture(t)
			peerEntered, peerCancelled, peerRelease := make(chan struct{}), make(chan struct{}), make(chan struct{})
			var once sync.Once
			release := func() { once.Do(func() { close(peerRelease) }) }
			t.Cleanup(release)
			failure := errors.New("injected preparation failure")
			fail := func(ctx context.Context) error {
				select {
				case <-peerEntered:
					return failure
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			peer := func(ctx context.Context) error {
				close(peerEntered)
				<-ctx.Done()
				close(peerCancelled)
				<-peerRelease
				// A successful late node reply still cannot authorize execution.
				return nil
			}
			gate := &restorePreparationGate{restoreCustodian: custodian, intent: fail, attach: peer}
			if failing == "rootfs" {
				gate.intent, gate.attach = peer, fail
			}
			h.rootfs = gate
			done := make(chan error, 1)
			go func() { done <- h.Claim(claim) }()
			select {
			case <-peerCancelled:
			case <-time.After(5 * time.Second):
				t.Fatal("peer was not cancelled")
			}
			select {
			case err := <-done:
				t.Fatalf("claim did not join cancelled peer: %v", err)
			default:
			}
			release()
			select {
			case err := <-done:
				require.ErrorIs(t, err, failure)
			case <-time.After(5 * time.Second):
				t.Fatal("claim did not finish")
			}
			require.NotContains(t, runner.callsSnapshot(), "create")
			require.NotContains(t, runner.callsSnapshot(), "restore")
			require.NotContains(t, runner.callsSnapshot(), "start")
			custody, err := custodian.GetMigrationDestination(t.Context(), fixture.task.ID)
			require.NoError(t, err)
			require.Equal(t, protocol.MigrationRestoreUncertain, custody.Restore.State)
			h.mu.Lock()
			require.Equal(t, phaseMigrating, h.phase)
			require.True(t, h.rootMounted, "retain possibly consumed writer for regional cleanup")
			require.NotNil(t, h.claim)
			h.mu.Unlock()
			require.Error(t, h.Claim(claim), "failed migration carrier cannot be reused")
		})
	}
}

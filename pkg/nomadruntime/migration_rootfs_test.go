package nomadruntime

import (
	"context"
	"encoding/hex"
	"errors"
	"net"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	"github.com/containerd/errdefs"
	"github.com/opencontainers/go-digest"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsblock"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshandoff"
	rootfssession "github.com/sandbox0-ai/sandbox0/pkg/rootfssession"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
	"github.com/stretchr/testify/require"
)

type primingMigrationCutRuntime struct {
	*migrationCutTestRuntime
	primeStarted chan struct{}
	cutStarted   chan struct{}
	releasePrime chan struct{}
	primeExited  chan struct{}
	primeErr     error
}

func (r *primingMigrationCutRuntime) PrimeMigrationImageInventory(ctx context.Context, _ string) error {
	close(r.primeStarted)
	defer close(r.primeExited)
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-r.releasePrime:
		return r.primeErr
	}
}

func (r *primingMigrationCutRuntime) CaptureMigrationRootFS(ctx context.Context, stage rootfshandoff.StageRequest, request rootfshandoff.MigrationRootFSCutRequest) (rootfshandoff.MigrationRootFSCut, error) {
	select {
	case <-r.primeStarted:
	case <-ctx.Done():
		return rootfshandoff.MigrationRootFSCut{}, ctx.Err()
	}
	close(r.cutStarted)
	return r.migrationCutTestRuntime.CaptureMigrationRootFS(ctx, stage, request)
}

func TestMigrationRootFSInventoryOverlapJoinsBeforeReleasingCustody(t *testing.T) {
	for _, outcome := range []string{"success", "inventory failure", "cut failure", "cancellation"} {
		t.Run(outcome, func(t *testing.T) {
			d, capture, runtime, _ := migrationRootFSNodeFixture(t)
			capture.State = protocol.MigrationCaptureComplete
			require.NoError(t, d.RecordMigrationCapture(t.Context(), capture))
			r := &primingMigrationCutRuntime{migrationCutTestRuntime: runtime, primeStarted: make(chan struct{}),
				cutStarted: make(chan struct{}), releasePrime: make(chan struct{}), primeExited: make(chan struct{})}
			d.runtime, d.migrationPeer = r, &migrationPeer{}
			if outcome == "cut failure" {
				r.sealErr = errors.New("seal unavailable")
			}
			if outcome == "inventory failure" {
				r.primeErr = errors.New("optional inventory unavailable")
			}
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			done := make(chan error, 1)
			go func() { _, err := d.SealMigrationRootFS(ctx, capture.Request); done <- err }()
			select {
			case <-r.cutStarted:
			case <-ctx.Done():
				t.Fatal("RootFS sealing did not overlap the blocked inventory")
			}
			if outcome == "success" || outcome == "inventory failure" {
				select {
				case err := <-done:
					t.Fatalf("custody released before inventory joined: %v", err)
				default:
				}
				require.False(t, d.beginReconciliation(capture.Request.Target.SlotID, nil))
				close(r.releasePrime)
			} else if outcome == "cancellation" {
				cancel()
			}
			err := <-done
			if outcome == "cut failure" {
				require.ErrorContains(t, err, "seal unavailable")
			} else if outcome != "cancellation" {
				require.NoError(t, err, "optional hash failure must fall back to normal planning")
			}
			select {
			case <-r.primeExited:
			default:
				t.Fatal("inventory worker outlived source custody")
			}
			require.True(t, d.beginReconciliation(capture.Request.Target.SlotID, nil))
			d.endReconciliation(capture.Request.Target.SlotID)
		})
	}
}

type migrationCutTestRuntime struct {
	*fakeRootFSRuntime
	sealCalls int
	sealErr   error
}

func (r *migrationCutTestRuntime) CaptureMigrationRootFS(_ context.Context, stage rootfshandoff.StageRequest, request rootfshandoff.MigrationRootFSCutRequest) (rootfshandoff.MigrationRootFSCut, error) {
	r.sealCalls++
	if r.sealErr != nil {
		return rootfshandoff.MigrationRootFSCut{}, r.sealErr
	}
	generation := *stage.Generation
	generation.GenerationID = request.GenerationID
	generation.WriterEpoch = stage.Identity.WriterEpoch
	generation.LocatorVersion++
	return rootfshandoff.NewMigrationRootFSCut(request, generation, 3)
}

type migrationSourceTestRunsc struct{ *fakeRunsc }

func (r *migrationSourceTestRunsc) setState(state string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.state = state
}

func (r *migrationSourceTestRunsc) State(_ context.Context, id string) (RunscState, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, "state")
	return RunscState{ID: id, Status: r.state}, r.stateErr
}

func (r *migrationSourceTestRunsc) Kill(_ context.Context, _, signal string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, "kill:"+signal)
	r.state = "stopped"
	return nil
}

func migrationRootFSNodeFixture(t *testing.T) (*nodeRuntime, protocol.MigrationCapture, *migrationCutTestRuntime, *migrationSourceTestRunsc) {
	t.Helper()
	stage := testNomadNodeClaimControlRequest(t).Stage.WithoutWriterGrantToken()
	registration := testRuntimeSlotJournalRegistration(t, stage.Identity.SlotNonce)
	stage.Identity.AllocationID, stage.Identity.BootID = registration.AllocationID, registration.NodeBootID
	stage.ExpectedPolicyToken.AllocationID = registration.AllocationID
	stage.Identity.RuntimeGeneration = "1"
	stage.Identity.SourceOCIDigest = digest.FromString("migration-source-oci").String()
	blockHead := digest.FromString("migration-rootfs-block-head").String()
	descriptor, err := rootfsblock.EncodeDescriptor(rootfsblock.Descriptor{
		Version: rootfsblock.DescriptorVersion, LogicalSizeBytes: rootfsblock.LogicalBlockSize, BlockSizeBytes: rootfsblock.LogicalBlockSize,
		MappingRoot: rootfsblock.MappingRootLocator{Version: rootfsblock.MappingPageVersion, RootDigest: blockHead,
			Object: rootfsblock.ObjectRange{Key: "rootfs/maps/migration.page", Length: 1, Checksum: digest.FromString("page").String()}},
	})
	require.NoError(t, err)
	stage.Generation = &rootfshandoff.GenerationDescriptor{Version: rootfshandoff.GenerationDescriptorVersion,
		GenerationID: stage.InitialGeneration, FilesystemID: stage.Identity.RootFSID,
		SourceOCIDigest: stage.Identity.SourceOCIDigest, BaseArtifactDigest: digest.FromString("migration-artifact").String(),
		BaseBlockRoot: blockHead, CurrentBlockHead: blockHead, WriterEpoch: stage.Identity.WriterEpoch - 1,
		FormatGeneration: 2, DurabilityState: rootfsblock.DurabilityS3, LocatorVersion: 1, Descriptor: descriptor}
	require.NoError(t, stage.ValidateDurableBinding())
	journal, err := newRuntimeSlotJournal(filepath.Join(t.TempDir(), "slots.db"), time.Hour)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, journal.Close()) })
	require.NoError(t, journal.Register(registration))
	capture := migrationJournalRequest(t, registration)
	capture.Request.Target.NodeUID = stage.Identity.NodeUID
	binding, err := stage.BindingDigest()
	require.NoError(t, err)
	capture.Request.BindingDigest = hex.EncodeToString(binding[:])
	capture.RequestDigest, err = capture.Request.Digest()
	require.NoError(t, err)
	runtime := &migrationCutTestRuntime{fakeRootFSRuntime: &fakeRootFSRuntime{recoverySessions: []rootfssession.RecoverySession{{
		Stage: stage, Kind: rootfssession.RecoveryCrashAbandon, Live: true,
		Consumer: &rootfssession.ConsumerRegistration{ActiveKey: registration.SlotID, ContainerID: registration.RunscContainerID,
			HostMountNamespace: registration.MountNamespaceID, StableMount: registration.StableMount},
	}}}}
	runner := &migrationSourceTestRunsc{fakeRunsc: newFakeRunsc()}
	runner.setState("stopped")
	daemon := &nodeRuntime{migrationStaging: testMigrationStagingGuard{}, runtime: runtime, runner: runner, journal: journal,
		clusterID: registration.ClusterID, nodeID: registration.NodeID, nodeUID: stage.Identity.NodeUID}
	require.NoError(t, daemon.RecordMigrationCapture(t.Context(), capture))
	return daemon, capture, runtime, runner
}

func TestMigrationRootFSPrivateSealRequiresCapturedStoppedSource(t *testing.T) {
	daemon, capture, _, _ := migrationRootFSNodeFixture(t)
	socket := filepath.Join(t.TempDir(), "ctld.sock")
	listener, err := net.Listen("unix", socket)
	require.NoError(t, err)
	server := &http.Server{Handler: nodeRuntimeRPCHandler(nil, nil, nil, daemon)}
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() { _ = server.Close() })
	client, err := NewClient(socket)
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })
	_, err = client.SealMigrationRootFS(t.Context(), capture.Request)
	require.ErrorIs(t, err, errdefs.ErrFailedPrecondition)
	capture.State = protocol.MigrationCaptureComplete
	require.NoError(t, daemon.RecordMigrationCapture(t.Context(), capture))
	cut, err := client.SealMigrationRootFS(t.Context(), capture.Request)
	require.NoError(t, err)
	require.Equal(t, capture.RequestDigest, cut.Request.CaptureRequestDigest)
	stored, err := client.GetMigrationCapture(t.Context(), capture.Request.Target.SlotID)
	require.NoError(t, err)
	require.Equal(t, cut, *stored.Capture.RootFS)
	require.NoError(t, stored.Capture.Validate())
	require.ErrorIs(t, client.RecordMigrationCapture(t.Context(), stored.Capture), errdefs.ErrPermissionDenied,
		"the driver cannot fabricate filesystem-cut evidence")
	require.NoError(t, client.RecordMigrationCapture(t.Context(), capture), "memory-only retries preserve the filesystem cut")
	stored, err = client.GetMigrationCapture(t.Context(), capture.Request.Target.SlotID)
	require.NoError(t, err)
	require.Equal(t, cut, *stored.Capture.RootFS)
}

func TestMigrationRootFSRecoveryFencesExecutionWithoutCrashCleanup(t *testing.T) {
	daemon, capture, runtime, runner := migrationRootFSNodeFixture(t)
	capture.State = protocol.MigrationCaptureComplete
	require.NoError(t, daemon.RecordMigrationCapture(t.Context(), capture))
	runner.setState("running")
	require.NoError(t, daemon.reconcile(t.Context(), runtime.recoverySessions[0]))
	require.Contains(t, runner.callsSnapshot(), "kill:KILL")
	require.NotContains(t, runner.callsSnapshot(), "delete:force")
	require.Zero(t, runtime.crashCalls)
	require.Zero(t, runtime.retireCalls)
	require.Zero(t, runtime.externalReclaims)
	stored, err := daemon.GetMigrationCapture(t.Context(), capture.Request.Target.SlotID)
	require.NoError(t, err)
	require.True(t, stored.ExecutionInvalidated)
	require.Equal(t, protocol.MigrationCaptureUncertain, stored.Capture.State)
	require.NoError(t, daemon.RecordMigrationCapture(t.Context(), capture), "a stale driver cannot clear invalidation")
	require.NoError(t, daemon.RecordMigrationCapture(t.Context(), stored.Capture), "recovery may acknowledge the uncertainty")
	_, err = daemon.SealMigrationRootFS(t.Context(), capture.Request)
	require.ErrorIs(t, err, errdefs.ErrFailedPrecondition)
	require.Zero(t, runtime.sealCalls)
}

func TestMigrationRootFSAdmissionSharesExistingRecoveryLock(t *testing.T) {
	daemon, capture, runtime, _ := migrationRootFSNodeFixture(t)
	require.True(t, daemon.beginReconciliation(capture.Request.Target.SlotID, nil))
	capture.State = protocol.MigrationCaptureComplete
	require.ErrorIs(t, daemon.RecordMigrationCapture(t.Context(), capture), errdefs.ErrUnavailable)
	_, err := daemon.SealMigrationRootFS(t.Context(), capture.Request)
	require.ErrorIs(t, err, errdefs.ErrUnavailable)
	require.Zero(t, runtime.sealCalls)
	daemon.endReconciliation(capture.Request.Target.SlotID)
	require.NoError(t, daemon.RecordMigrationCapture(t.Context(), capture))
	runtime.recoverySessions[0].Stage.Identity.BootID = "new-boot"
	_, err = daemon.SealMigrationRootFS(t.Context(), capture.Request)
	require.ErrorIs(t, err, errdefs.ErrFailedPrecondition)
	require.Zero(t, runtime.sealCalls)
}

func TestMigrationRootFSCannotReviveAfterUnexpectedSourceExecution(t *testing.T) {
	daemon, capture, runtime, runner := migrationRootFSNodeFixture(t)
	capture.State = protocol.MigrationCaptureComplete
	require.NoError(t, daemon.RecordMigrationCapture(t.Context(), capture))
	runner.setState("running")
	_, err := daemon.SealMigrationRootFS(t.Context(), capture.Request)
	require.ErrorIs(t, err, errdefs.ErrFailedPrecondition)
	require.NotContains(t, runner.callsSnapshot(), "kill:KILL")
	runner.setState("stopped")
	_, err = daemon.SealMigrationRootFS(t.Context(), capture.Request)
	require.ErrorIs(t, err, errdefs.ErrFailedPrecondition, "a later stopped observation cannot repair an invalid temporal cut")
	require.Zero(t, runtime.sealCalls)
}

func TestMigrationRootFSLostOwnerInvalidatesCaptureWithoutExecution(t *testing.T) {
	d, capture, runtime, runner := migrationRootFSNodeFixture(t)
	capture.State = protocol.MigrationCaptureComplete
	require.NoError(t, d.RecordMigrationCapture(t.Context(), capture))
	runtime.sealErr = rootfssession.ErrMigrationCutOwnerLost
	_, err := d.SealMigrationRootFS(t.Context(), capture.Request)
	require.ErrorIs(t, err, rootfssession.ErrMigrationCutOwnerLost)
	custody, err := d.GetMigrationCapture(t.Context(), capture.Request.Target.SlotID)
	require.NoError(t, err)
	require.True(t, custody.ExecutionInvalidated)
	require.Equal(t, protocol.MigrationCaptureUncertain, custody.Capture.State)
	require.Nil(t, custody.Capture.RootFS)
	require.NotContains(t, runner.callsSnapshot(), "kill:KILL")
	_, err = d.SealMigrationRootFS(t.Context(), capture.Request)
	require.ErrorIs(t, err, errdefs.ErrFailedPrecondition)
	require.Equal(t, 1, runtime.sealCalls)
}

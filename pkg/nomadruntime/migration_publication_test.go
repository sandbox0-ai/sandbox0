package nomadruntime

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/containerd/errdefs"
	"github.com/opencontainers/go-digest"
	"github.com/sandbox0-ai/sandbox0/pkg/objectstore"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshandoff"
	"github.com/sandbox0-ai/sandbox0/pkg/runtimecheckpoint"
	"github.com/sandbox0-ai/sandbox0/pkg/runtimecontrol"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
	"github.com/stretchr/testify/require"
)

type migrationImageTestRuntime struct {
	*migrationCutTestRuntime
	store   *runtimecheckpoint.Store
	err     error
	calls   int
	started chan struct{}
	resume  chan struct{}
}

func (r *migrationImageTestRuntime) PublishMigrationImage(ctx context.Context, binding runtimecheckpoint.Binding, directory string) (runtimecheckpoint.Reference, error) {
	r.calls++
	if r.started != nil {
		close(r.started)
		select {
		case <-r.resume:
		case <-ctx.Done():
			return runtimecheckpoint.Reference{}, ctx.Err()
		}
	}
	if r.err != nil {
		return runtimecheckpoint.Reference{}, r.err
	}
	return r.store.Publish(ctx, binding, directory)
}

func migrationImageNodeFixture(t *testing.T, beforeCapture ...func(*nodeRuntime, protocol.MigrationCaptureRequest)) (*nodeRuntime, protocol.MigrationPublicationRequest, *migrationImageTestRuntime) {
	t.Helper()
	daemon, capture, rootfs, _ := migrationRootFSNodeFixture(t)
	prior, err := daemon.journal.Get(capture.Request.Target.SlotID)
	require.NoError(t, err)
	// Replace the fixture's deliberately opaque revision with a real assignment
	// before it becomes capture custody.
	require.NoError(t, daemon.journal.Close())
	journal, err := newRuntimeSlotJournal(filepath.Join(t.TempDir(), "images.db"), time.Hour)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, journal.Close()) })
	daemon.journal = journal
	registration := prior.Registration
	registration.NetNSIdentity = rootfs.recoverySessions[0].Stage.ExpectedPolicyToken.NetNSIdentity
	require.NoError(t, journal.Register(registration))
	source := runtimecontrol.Assignment{SandboxID: capture.Request.SandboxID, TeamID: "team", RuntimeGeneration: capture.Request.SourceGeneration, SecurityClass: "standard"}
	capture.Request.AssignmentRevision, err = source.Revision()
	require.NoError(t, err)
	resources := migrationSourceTestResources(t, rootfs.recoverySessions[0].Stage, capture.Request.Target)
	resourceDigest, err := resources.Digest()
	require.NoError(t, err)
	capture.Request.ResourceLeaseDigest = strings.TrimPrefix(resourceDigest, "sha256:")
	binding, err := rootfs.recoverySessions[0].Stage.BindingDigest()
	require.NoError(t, err)
	capture.Request.BindingDigest = hex.EncodeToString(binding[:])
	capture.RequestDigest, err = capture.Request.Digest()
	require.NoError(t, err)
	for _, prepare := range beforeCapture {
		prepare(daemon, capture.Request)
	}
	require.NoError(t, daemon.RecordMigrationCapture(t.Context(), capture))
	capture.State = protocol.MigrationCaptureComplete
	require.NoError(t, daemon.RecordMigrationCapture(t.Context(), capture))
	cut, err := daemon.SealMigrationRootFS(t.Context(), capture.Request)
	require.NoError(t, err)
	capture.RootFS = &cut
	source.RuntimeGeneration++
	request := protocol.MigrationPublicationRequest{Capture: capture,
		Assignment:          runtimecontrol.MigrationAssignment{OperationID: capture.Request.OperationID, SourceGeneration: capture.Request.SourceGeneration, SourceRevision: capture.Request.AssignmentRevision, Target: source},
		CompatibilityDigest: digest.FromString("runtime-shape").String(), CPUFeaturesDigest: digest.FromString("eligibility-fixture").String()}
	_, err = request.Digest()
	require.NoError(t, err)
	store, err := runtimecheckpoint.New(objectstore.NewMemoryStore(""), runtimecheckpoint.ChunkBytes)
	require.NoError(t, err)
	runtime := &migrationImageTestRuntime{migrationCutTestRuntime: rootfs, store: store}
	daemon.runtime = runtime
	parent, cancel := context.WithCancel(t.Context())
	daemon.migrationContext = parent
	t.Cleanup(func() { cancel(); daemon.wg.Wait() })
	custody, err := daemon.GetMigrationCapture(t.Context(), capture.Request.Target.SlotID)
	require.NoError(t, err)
	require.NoError(t, os.Mkdir(custody.ImageDirectory, 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(custody.ImageDirectory, "checkpoint.img"), []byte("retained-memory"), 0o600))
	return daemon, request, runtime
}

func TestMigrationImagePublicationRetainsExactIntentAcrossRestart(t *testing.T) {
	daemon, request, runtime := migrationImageNodeFixture(t)
	runtime.err = errors.New("object storage unavailable")
	_, err := daemon.PublishMigration(t.Context(), request)
	require.ErrorContains(t, err, "object storage unavailable")
	custody, err := daemon.GetMigrationCapture(t.Context(), request.Capture.Request.Target.SlotID)
	require.NoError(t, err)
	require.Equal(t, request, *custody.PublicationRequest)
	require.Nil(t, custody.Publication)
	changed := request
	changed.CPUFeaturesDigest = digest.FromString("replacement-cpu").String()
	_, err = daemon.PublishMigration(t.Context(), changed)
	require.ErrorIs(t, err, errdefs.ErrAlreadyExists)
	require.Equal(t, 1, runtime.calls)
	// Reopen the actual Bolt journal; the retry cannot depend on process memory.
	journalPath := daemon.journal.db.Path()
	require.NoError(t, daemon.journal.Close())
	reopened, err := newRuntimeSlotJournal(journalPath, time.Hour)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, reopened.Close()) })
	daemon.journal = reopened
	runtime.err = nil
	receipt, err := daemon.PublishMigration(t.Context(), request)
	require.NoError(t, err)
	require.NoError(t, receipt.ValidateFor(request))
	again, err := daemon.PublishMigration(t.Context(), request)
	require.NoError(t, err)
	require.Equal(t, receipt, again)
	require.Equal(t, 2, runtime.calls, "acknowledged uploads do not reread staging")
	destination := filepath.Join(t.TempDir(), "downloaded")
	_, err = runtime.store.Download(t.Context(), receipt.Binding, receipt.Reference, destination)
	require.NoError(t, err)
	data, err := os.ReadFile(filepath.Join(destination, "checkpoint.img"))
	require.NoError(t, err)
	require.Equal(t, "retained-memory", string(data))
	// Publication cannot erase source custody or authorize generic reclamation.
	require.NoError(t, daemon.reconcile(t.Context(), runtime.recoverySessions[0]))
	require.Zero(t, runtime.crashCalls)
	require.Zero(t, runtime.retireCalls)
}

func TestMigrationImagePublicationSurvivesTransportCancellation(t *testing.T) {
	daemon, request, runtime := migrationImageNodeFixture(t)
	runtime.started, runtime.resume = make(chan struct{}), make(chan struct{})
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { _, err := daemon.PublishMigration(ctx, request); done <- err }()
	select {
	case <-runtime.started:
	case <-time.After(5 * time.Second):
		t.Fatal("upload did not start")
	}
	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
	custody, err := daemon.GetMigrationCapture(t.Context(), request.Capture.Request.Target.SlotID)
	require.NoError(t, err)
	// The caller can now reuse its request; the independent worker must own
	// the nested descriptor and assignment rather than aliasing that memory.
	request.Capture.RootFS.Sequence++
	close(runtime.resume)
	receipt, err := daemon.PublishMigration(t.Context(), *custody.PublicationRequest)
	require.NoError(t, err)
	require.NoError(t, receipt.ValidateFor(*custody.PublicationRequest))
	require.Equal(t, 1, runtime.calls)
}

func TestMigrationImagePublicationRejectsMismatchedAndInvalidatedCuts(t *testing.T) {
	daemon, request, runtime := migrationImageNodeFixture(t)
	encoded, err := json.Marshal(request)
	require.NoError(t, err)
	var changed protocol.MigrationPublicationRequest
	require.NoError(t, json.Unmarshal(encoded, &changed))
	changed.Capture.RootFS.Sequence++
	_, err = daemon.PublishMigration(t.Context(), changed)
	require.Error(t, err)
	require.NoError(t, daemon.journal.invalidateMigrationExecution(request.Capture.Request.Target.SlotID))
	_, err = daemon.PublishMigration(t.Context(), request)
	require.ErrorIs(t, err, errdefs.ErrFailedPrecondition)
	require.Zero(t, runtime.calls)
}

func TestMigrationImagePublicationShutdownRetainsRetryableIntent(t *testing.T) {
	daemon, request, runtime := migrationImageNodeFixture(t)
	runtime.started, runtime.resume = make(chan struct{}), make(chan struct{})
	ctx, cancel := context.WithCancel(t.Context())
	daemon.migrationContext = ctx
	done := make(chan error, 1)
	go func() { _, err := daemon.PublishMigration(t.Context(), request); done <- err }()
	select {
	case <-runtime.started:
	case <-time.After(5 * time.Second):
		t.Fatal("upload did not start")
	}
	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
	custody, err := daemon.GetMigrationCapture(t.Context(), request.Capture.Request.Target.SlotID)
	require.NoError(t, err)
	require.NotNil(t, custody.PublicationRequest)
	require.Nil(t, custody.Publication)
}

func TestMigrationImagePublicationRejectsExecutionDuringUpload(t *testing.T) {
	daemon, request, runtime := migrationImageNodeFixture(t)
	runtime.started, runtime.resume = make(chan struct{}), make(chan struct{})
	done := make(chan error, 1)
	go func() { _, err := daemon.PublishMigration(t.Context(), request); done <- err }()
	select {
	case <-runtime.started:
	case <-time.After(5 * time.Second):
		t.Fatal("upload did not start")
	}
	runner := daemon.runner.(*migrationSourceTestRunsc)
	runner.setState("running")
	close(runtime.resume)
	require.ErrorIs(t, <-done, errdefs.ErrFailedPrecondition)
	runner.setState("stopped")
	_, err := daemon.PublishMigration(t.Context(), request)
	require.ErrorIs(t, err, errdefs.ErrFailedPrecondition)
	record, err := daemon.journal.Get(request.Capture.Request.Target.SlotID)
	require.NoError(t, err)
	require.True(t, record.Migration.ExecutionInvalidated)
	require.Nil(t, record.Migration.Publication, "uploaded objects alone cannot become a usable receipt")
	require.Equal(t, 1, runtime.calls)
}

func migrationSourceTestResources(t *testing.T, stage rootfshandoff.StageRequest, target protocol.NodeChannelTarget) protocol.RuntimeResourceLease {
	t.Helper()
	lease, err := protocol.NewRuntimeResourceLease("source-claim-operation", stage.Identity.ClaimID, target.SlotID, target.ClusterID, target.NodeID, target.NodeUID, target.NodeBootID,
		protocol.RuntimeResourceRequest{Version: protocol.RuntimeResourceRequestVersion, CPUMillicores: 1000, MemoryBytes: 128 << 20, PIDsLimit: 1024}, "0", "0")
	require.NoError(t, err)
	return lease
}

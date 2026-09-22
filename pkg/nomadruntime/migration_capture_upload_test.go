package nomadruntime

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/opencontainers/go-digest"
	"github.com/sandbox0-ai/sandbox0/pkg/objectstore"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshandoff"
	"github.com/sandbox0-ai/sandbox0/pkg/runtimecheckpoint"
	"github.com/sandbox0-ai/sandbox0/pkg/runtimecontrol"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
	"github.com/stretchr/testify/require"
)

type captureUploadNodeRuntime struct {
	*migrationCutTestRuntime
	uploads *rootfsRuntime
	opens   atomic.Int64
}

func (r *captureUploadNodeRuntime) OpenMigrationCaptureUpload(ctx context.Context, s runtimecheckpoint.CaptureScope, b int64) (*runtimecheckpoint.CaptureStager, error) {
	r.opens.Add(1)
	return r.uploads.OpenMigrationCaptureUpload(ctx, s, b)
}
func (r *captureUploadNodeRuntime) PlanMigrationCaptureUpload(ctx context.Context, s *runtimecheckpoint.CaptureStager, b runtimecheckpoint.Binding, p string) (runtimecheckpoint.LocalImagePlan, error) {
	return r.uploads.PlanMigrationCaptureUpload(ctx, s, b, p)
}
func (r *captureUploadNodeRuntime) PublishMigrationImage(ctx context.Context, b runtimecheckpoint.Binding, p string) (runtimecheckpoint.Reference, error) {
	return r.uploads.PublishMigrationImage(ctx, b, p)
}

func (r *captureUploadNodeRuntime) PrimeMigrationImageInventory(ctx context.Context, directory string) error {
	return r.uploads.PrimeMigrationImageInventory(ctx, directory)
}
func (r *captureUploadNodeRuntime) PrimeMigrationCaptureInventory(ctx context.Context, stage *runtimecheckpoint.CaptureStager, directory string) error {
	return r.uploads.PrimeMigrationCaptureInventory(ctx, stage, directory)
}

func captureUploadNodeFixture(t *testing.T, objects objectstore.Store, beforeReserve ...func(*nodeRuntime, *protocol.MigrationStagingRequest)) (*nodeRuntime, protocol.MigrationCapture, protocol.MigrationStagingRequest, *captureUploadNodeRuntime, context.CancelFunc) {
	t.Helper()
	d, capture, backend, _ := migrationRootFSNodeFixture(t)
	prior, err := d.journal.Get(capture.Request.Target.SlotID)
	require.NoError(t, err)
	require.NoError(t, d.journal.Close())
	j, err := newRuntimeSlotJournal(filepath.Join(t.TempDir(), "capture-upload.db"), time.Hour)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, j.Close()) })
	d.journal = j
	registration := prior.Registration
	registration.NetNSIdentity = backend.recoverySessions[0].Stage.ExpectedPolicyToken.NetNSIdentity
	require.NoError(t, j.Register(registration))
	source := runtimecontrol.Assignment{SandboxID: capture.Request.SandboxID, TeamID: "team", RuntimeGeneration: capture.Request.SourceGeneration, SecurityClass: "standard"}
	capture.Request.AssignmentRevision, err = source.Revision()
	require.NoError(t, err)
	resources := migrationSourceTestResources(t, backend.recoverySessions[0].Stage, capture.Request.Target)
	rd, err := resources.Digest()
	require.NoError(t, err)
	capture.Request.ResourceLeaseDigest = strings.TrimPrefix(rd, "sha256:")
	capture.RequestDigest, err = capture.Request.Digest()
	require.NoError(t, err)
	r := migrationStagingSourceRequest(t, registration)
	r.Source = capture.Request
	r.Target = capture.Request.Target
	r.Bytes = 16 << 20
	r.CaptureUpload, err = protocol.NewMigrationCaptureUpload(r.Source, "team", digest.FromString("runtime-shape").String(), digest.FromString("eligibility-fixture").String(), r.Bytes)
	require.NoError(t, err)
	store, err := runtimecheckpoint.New(objects, 16<<20)
	require.NoError(t, err)
	runtime := &captureUploadNodeRuntime{migrationCutTestRuntime: backend, uploads: &rootfsRuntime{checkpoints: store}}
	d.runtime = runtime
	parent, cancel := context.WithCancel(t.Context())
	d.migrationContext = parent
	t.Cleanup(func() { cancel(); d.wg.Wait() })
	for _, prepare := range beforeReserve {
		prepare(d, &r)
	}
	_, err = d.ReserveMigrationStaging(t.Context(), r)
	require.NoError(t, err)
	return d, capture, r, runtime, cancel
}

func TestNodeCaptureUploadsOnlyAfterIntentAndPublishesVerifiedVersionTwo(t *testing.T) {
	objects := objectstore.NewMemoryStore("")
	d, capture, staging, runtime, _ := captureUploadNodeFixture(t, objects)
	require.Zero(t, runtime.opens.Load(), "staging alone cannot start uploads")
	require.NoError(t, d.RecordMigrationCapture(t.Context(), capture))
	record, err := d.journal.Get(capture.Request.Target.SlotID)
	require.NoError(t, err)
	require.Equal(t, runtimeSlotCaptureUploadJournalVersion, record.Version)
	bad := record
	bad.Version = runtimeSlotStagingJournalVersion
	payload, err := json.Marshal(bad)
	require.NoError(t, err)
	_, err = decodeRuntimeSlotJournalRecord(payload)
	require.ErrorContains(t, err, "non-downgradable")
	data := bytes.Repeat([]byte{7}, runtimecheckpoint.ChunkBytes)
	require.NoError(t, os.Mkdir(record.Migration.ImageDirectory, 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(record.Migration.ImageDirectory, "pages.img"), data, 0o600))
	key := "runtime-checkpoints/capture-v1/" + strings.TrimPrefix(staging.CaptureUpload.ScopeDigest, "sha256:") + "/chunks/" + digest.FromBytes(data).Encoded()
	require.Eventually(t, func() bool { _, err := objects.Head(key); return err == nil }, 5*time.Second, 10*time.Millisecond)
	require.NoError(t, d.RecordMigrationCapture(t.Context(), capture), "lost intent replies join the same worker")
	require.EqualValues(t, 1, runtime.opens.Load())
	other := capture
	other.Request.OperationID = "other-operation"
	other.RequestDigest, err = other.Request.Digest()
	require.NoError(t, err)
	require.Error(t, d.RecordMigrationCapture(t.Context(), other))
	capture.State = protocol.MigrationCaptureComplete
	require.NoError(t, d.RecordMigrationCapture(t.Context(), capture))
	require.False(t, d.hasMigrationCaptureUpload(capture))
	cut, err := d.SealMigrationRootFS(t.Context(), capture.Request)
	require.NoError(t, err)
	capture.RootFS = &cut
	source := runtimecontrol.Assignment{SandboxID: capture.Request.SandboxID, TeamID: "team", RuntimeGeneration: capture.Request.SourceGeneration + 1, SecurityClass: "standard"}
	request := protocol.MigrationPublicationRequest{Capture: capture, Assignment: runtimecontrol.MigrationAssignment{OperationID: capture.Request.OperationID, SourceGeneration: capture.Request.SourceGeneration, SourceRevision: capture.Request.AssignmentRevision, Target: source}, CompatibilityDigest: staging.CaptureUpload.CompatibilityDigest, CPUFeaturesDigest: staging.CaptureUpload.CPUFeaturesDigest}
	receipt, err := d.PublishMigration(t.Context(), request)
	require.NoError(t, err)
	require.NoError(t, receipt.ValidateFor(request))
	manifest, err := runtime.uploads.checkpoints.Download(t.Context(), receipt.Binding, receipt.Reference, filepath.Join(t.TempDir(), "downloaded"))
	require.NoError(t, err)
	require.Equal(t, runtimecheckpoint.StagedManifestVersion, manifest.Version)
	recovered := &rootfsRuntime{checkpoints: runtime.uploads.checkpoints}
	scope, err := staging.CaptureUpload.Scope(staging.Source)
	require.NoError(t, err)
	stage, err := recovered.OpenMigrationCaptureUpload(t.Context(), scope, staging.CaptureUpload.MaxBytes)
	require.NoError(t, err)
	retry, err := stage.Publish(t.Context(), receipt.Binding, record.Migration.ImageDirectory)
	require.NoError(t, err)
	require.Equal(t, receipt.Reference, retry)
}

type blockedCaptureUploadStore struct {
	objectstore.ContextConditionalStore
	cleanup                 objectstore.ContextCleanupStore
	started, canceled, exit chan struct{}
}

func (s *blockedCaptureUploadStore) ListContext(ctx context.Context, p, a, token, delimiter string, n int64) ([]objectstore.Info, bool, string, error) {
	return s.cleanup.ListContext(ctx, p, a, token, delimiter, n)
}
func (s *blockedCaptureUploadStore) DeleteContext(ctx context.Context, k string) error {
	return s.cleanup.DeleteContext(ctx, k)
}
func (s *blockedCaptureUploadStore) PutIfAbsentContext(ctx context.Context, key string, r io.Reader) (bool, error) {
	if strings.Contains(key, "/chunks/") {
		close(s.started)
		<-ctx.Done()
		close(s.canceled)
		<-s.exit
		return false, ctx.Err()
	}
	return s.ContextConditionalStore.PutIfAbsentContext(ctx, key, r)
}

func TestCaptureOutcomeAndPrimaryLossJoinUploadBeforeReleasingNodeCustody(t *testing.T) {
	for _, primaryLoss := range []bool{false, true} {
		name := "outcome"
		if primaryLoss {
			name = "primary loss"
		}
		t.Run(name, func(t *testing.T) {
			raw := objectstore.NewMemoryStore("")
			objects := &blockedCaptureUploadStore{ContextConditionalStore: raw.(objectstore.ContextConditionalStore), cleanup: raw.(objectstore.ContextCleanupStore), started: make(chan struct{}), canceled: make(chan struct{}), exit: make(chan struct{})}
			var once sync.Once
			defer once.Do(func() { close(objects.exit) })
			d, capture, _, _, cancel := captureUploadNodeFixture(t, objects)
			require.NoError(t, d.RecordMigrationCapture(t.Context(), capture))
			record, err := d.journal.Get(capture.Request.Target.SlotID)
			require.NoError(t, err)
			require.NoError(t, os.Mkdir(record.Migration.ImageDirectory, 0o700))
			require.NoError(t, os.WriteFile(filepath.Join(record.Migration.ImageDirectory, "pages.img"), bytes.Repeat([]byte{1}, runtimecheckpoint.ChunkBytes), 0o600))
			select {
			case <-objects.started:
			case <-time.After(5 * time.Second):
				t.Fatal("capture upload did not start")
			}
			done := make(chan error, 1)
			if primaryLoss {
				cancel()
				go func() { d.wg.Wait(); done <- nil }()
			} else {
				capture.State = protocol.MigrationCaptureComplete
				go func() { done <- d.RecordMigrationCapture(t.Context(), capture) }()
			}
			select {
			case <-objects.canceled:
			case <-time.After(5 * time.Second):
				t.Fatal("capture upload was not canceled")
			}
			require.False(t, d.beginReconciliation(capture.Request.Target.SlotID, nil), "network cancellation alone cannot release readers")
			select {
			case err := <-done:
				t.Fatalf("custody released before upload joined: %v", err)
			default:
			}
			once.Do(func() { close(objects.exit) })
			require.NoError(t, <-done)
			require.True(t, d.beginReconciliation(capture.Request.Target.SlotID, nil))
			d.endReconciliation(capture.Request.Target.SlotID)
			record, err = d.journal.Get(capture.Request.Target.SlotID)
			require.NoError(t, err)
			if primaryLoss {
				require.Equal(t, protocol.MigrationCaptureIntent, record.Migration.Capture.State)
			} else {
				require.Equal(t, protocol.MigrationCaptureComplete, record.Migration.Capture.State)
			}
		})
	}
}

type sealingCaptureUploadRuntime struct {
	*captureUploadNodeRuntime
	uploadStarted <-chan struct{}
	cutStarted    chan struct{}
}

func (r *sealingCaptureUploadRuntime) CaptureMigrationRootFS(ctx context.Context, stage rootfshandoff.StageRequest, request rootfshandoff.MigrationRootFSCutRequest) (rootfshandoff.MigrationRootFSCut, error) {
	select {
	case <-r.uploadStarted:
	case <-ctx.Done():
		return rootfshandoff.MigrationRootFSCut{}, ctx.Err()
	}
	close(r.cutStarted)
	return r.migrationCutTestRuntime.CaptureMigrationRootFS(ctx, stage, request)
}

func TestRootFSSealOverlapsFinalChunkUploadAndJoinsTimedOutIO(t *testing.T) {
	raw := objectstore.NewMemoryStore("")
	objects := &blockedCaptureUploadStore{ContextConditionalStore: raw.(objectstore.ContextConditionalStore), cleanup: raw.(objectstore.ContextCleanupStore),
		started: make(chan struct{}), canceled: make(chan struct{}), exit: make(chan struct{})}
	var release sync.Once
	defer release.Do(func() { close(objects.exit) })
	d, capture, staging, runtime, _ := captureUploadNodeFixture(t, objects)
	require.NoError(t, d.RecordMigrationCapture(t.Context(), capture))
	record, err := d.journal.Get(capture.Request.Target.SlotID)
	require.NoError(t, err)
	directory := record.Migration.ImageDirectory
	require.NoError(t, os.Mkdir(directory, 0o700))
	// Growing upload deliberately ignores a short final chunk. Sealing's
	// inspection must stage it under the existing capture grant instead.
	require.NoError(t, os.WriteFile(filepath.Join(directory, "pages.img"), []byte("complete short execution state"), 0o600))
	capture.State = protocol.MigrationCaptureComplete
	require.NoError(t, d.RecordMigrationCapture(t.Context(), capture))
	r := &sealingCaptureUploadRuntime{captureUploadNodeRuntime: runtime, uploadStarted: objects.started, cutStarted: make(chan struct{})}
	d.runtime, d.migrationPeer = r, &migrationPeer{}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := d.SealMigrationRootFS(ctx, capture.Request); done <- err }()
	select {
	case <-r.cutStarted:
	case <-ctx.Done():
		t.Fatal("RootFS sealing did not overlap final chunk upload")
	}
	select {
	case <-objects.canceled:
	case <-ctx.Done():
		t.Fatal("optional final upload was not bounded")
	}
	require.False(t, d.beginReconciliation(capture.Request.Target.SlotID, nil), "I/O cancellation alone cannot release custody")
	select {
	case err := <-done:
		t.Fatalf("RootFS seal returned before upload exited: %v", err)
	default:
	}
	release.Do(func() { close(objects.exit) })
	require.NoError(t, <-done, "optional upload failure cannot invalidate a successful durable RootFS cut")
	_, found := runtime.uploads.takeMigrationInventory(directory)
	require.False(t, found, "failed overlap must fall back to normal publication")
	record, err = d.journal.Get(capture.Request.Target.SlotID)
	require.NoError(t, err)
	require.NotNil(t, record.Migration.Capture.RootFS)
	require.Nil(t, record.Migration.Publication)
	require.False(t, record.MigrationStaging.Released)
	require.Equal(t, staging.CaptureUpload, record.MigrationStaging.Request.CaptureUpload)
	require.True(t, d.beginReconciliation(capture.Request.Target.SlotID, nil))
	d.endReconciliation(capture.Request.Target.SlotID)
}

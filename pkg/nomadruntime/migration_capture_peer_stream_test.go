package nomadruntime

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/opencontainers/go-digest"
	"github.com/sandbox0-ai/sandbox0/pkg/objectstore"
	"github.com/sandbox0-ai/sandbox0/pkg/runtimecheckpoint"
	"github.com/sandbox0-ai/sandbox0/pkg/runtimecontrol"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
	"github.com/stretchr/testify/require"
)

type capturePeerSourceRuntime struct{ *captureUploadNodeRuntime }

func (r *capturePeerSourceRuntime) PlanMigrationImage(ctx context.Context, b runtimecheckpoint.Binding, dir string) (runtimecheckpoint.LocalImagePlan, error) {
	return r.uploads.PlanMigrationImage(ctx, b, dir)
}
func (r *capturePeerSourceRuntime) PublishPlannedMigrationImage(ctx context.Context, b runtimecheckpoint.Binding, p runtimecheckpoint.LocalImagePlan, dir string) (runtimecheckpoint.Reference, error) {
	return r.uploads.PublishPlannedMigrationImage(ctx, b, p, dir)
}
func (r *capturePeerSourceRuntime) WritePlannedMigrationPeerImage(ctx context.Context, b runtimecheckpoint.Binding, p runtimecheckpoint.LocalImagePlan, dir string, w io.Writer) error {
	return r.uploads.WritePlannedMigrationPeerImage(ctx, b, p, dir, w)
}

func (r *capturePeerSourceRuntime) WriteMigrationCapturePeerFinal(ctx context.Context, b runtimecheckpoint.Binding, ref runtimecheckpoint.Reference,
	p *runtimecheckpoint.LocalImagePlan, dir string, inventory runtimecheckpoint.CapturePeerInventory, w io.Writer) error {
	return r.uploads.WriteMigrationCapturePeerFinal(ctx, b, ref, p, dir, inventory, w)
}
func (r *capturePeerSourceRuntime) WriteMigrationPeerImage(ctx context.Context, b runtimecheckpoint.Binding, ref runtimecheckpoint.Reference, dir string, w io.Writer) error {
	return r.uploads.WriteMigrationPeerImage(ctx, b, ref, dir, w)
}

type capturePeerCountWriter struct {
	http.ResponseWriter
	count *atomic.Int64
}

func (w capturePeerCountWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }
func (w capturePeerCountWriter) Write(p []byte) (int, error) {
	n, err := w.ResponseWriter.Write(p)
	w.count.Add(int64(n))
	return n, err
}

func capturePeerTestServer(t *testing.T, d *nodeRuntime, count *atomic.Int64) *httptest.Server {
	t.Helper()
	identity, err := runtimecheckpoint.NewPeerIdentity()
	require.NoError(t, err)
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		d.serveMigrationPeer(capturePeerCountWriter{w, count}, r)
	}))
	server.TLS = &tls.Config{MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{identity}, ClientAuth: tls.RequireAnyClientCert}
	server.StartTLS()
	endpoint, err := runtimecheckpoint.NewPeerEndpoint(strings.TrimPrefix(server.URL, "https://"), identity)
	require.NoError(t, err)
	d.migrationPeer = &migrationPeer{identity: identity, endpoint: endpoint}
	t.Cleanup(func() {
		server.CloseClientConnections()
		server.Close()
		require.NoError(t, d.closeMigrationCapturePeerCaches())
	})
	return server
}

type capturePeerNodeFixture struct {
	source, target *nodeRuntime
	capture        protocol.MigrationCapture
	grant          protocol.MigrationCapturePeerRequest
	image          protocol.MigrationImagePrepareRequest
	downloads      *migrationImageDownloadTestRuntime
	objects        objectstore.Store
	output         *atomic.Int64
}

func newCapturePeerNodeFixture(t *testing.T, stores ...objectstore.Store) capturePeerNodeFixture {
	t.Helper()
	target, image, downloads := migrationImageDestinationFixture(t)
	output := &atomic.Int64{}
	objects := objectstore.NewMemoryStore("")
	if len(stores) > 0 {
		objects = stores[0]
	}
	source, capture, staging, runtime, _ := captureUploadNodeFixture(t, objects, func(source *nodeRuntime, r *protocol.MigrationStagingRequest) {
		capturePeerTestServer(t, source, output)
		capturePeerTestServer(t, target, &atomic.Int64{})
		r.Destination = image.Target
		digest, err := image.Resources.Digest()
		require.NoError(t, err)
		r.DestinationResourceLeaseDigest = strings.TrimPrefix(digest, "sha256:")
		r.Inodes = runtimecheckpoint.MaxFiles * 8
	})
	source.runtime = &capturePeerSourceRuntime{runtime}
	downloads.store = runtime.uploads.checkpoints
	local, err := source.ReserveMigrationStaging(t.Context(), staging)
	require.NoError(t, err)
	destination := staging
	destination.Target = destination.Destination
	remote, err := target.ReserveMigrationStaging(t.Context(), destination)
	require.NoError(t, err)
	grant := protocol.MigrationCapturePeerRequest{Staging: destination, Source: *local, Destination: *remote}
	_, err = target.PrepareMigrationCapturePeer(t.Context(), grant)
	require.NoError(t, err)
	grant.Staging = staging
	_, err = source.PrepareMigrationCapturePeer(t.Context(), grant)
	require.NoError(t, err)
	return capturePeerNodeFixture{source, target, capture, grant, image, downloads, objects, output}
}

func (f capturePeerNodeFixture) seal(t *testing.T, data []byte) protocol.MigrationPublicationRequest {
	t.Helper()
	capture := f.capture
	capture.State = protocol.MigrationCaptureComplete
	require.NoError(t, f.source.RecordMigrationCapture(t.Context(), capture))
	// Join the interrupted tentative receiver before inspecting its reuse hints.
	require.NoError(t, f.target.beginExternalReconciliation(t.Context(), f.image.Target.SlotID))
	f.target.endReconciliation(f.image.Target.SlotID)
	source, err := f.source.journal.Get(capture.Request.Target.SlotID)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(source.Migration.ImageDirectory, "pages.img"), data, 0o600))
	cut, err := f.source.SealMigrationRootFS(t.Context(), capture.Request)
	require.NoError(t, err)
	capture.RootFS = &cut
	assignment := runtimecontrol.Assignment{SandboxID: capture.Request.SandboxID, TeamID: "team", RuntimeGeneration: capture.Request.SourceGeneration + 1, SecurityClass: "standard"}
	publication := protocol.MigrationPublicationRequest{Capture: capture,
		Assignment:          runtimecontrol.MigrationAssignment{OperationID: capture.Request.OperationID, SourceGeneration: capture.Request.SourceGeneration, SourceRevision: capture.Request.AssignmentRevision, Target: assignment},
		CompatibilityDigest: f.grant.Staging.CaptureUpload.CompatibilityDigest, CPUFeaturesDigest: f.grant.Staging.CaptureUpload.CPUFeaturesDigest,
		DestinationPeerCertificateSHA256: f.grant.Destination.PeerCertificateSHA256}
	return publication
}

func (f capturePeerNodeFixture) publish(t *testing.T, data []byte) (protocol.MigrationImagePrefetchRequest, protocol.MigrationImagePrepareRequest) {
	t.Helper()
	publication := f.seal(t, data)
	receipt, err := f.source.PublishMigration(t.Context(), publication)
	require.NoError(t, err)
	image := f.image
	image.Publication, image.Receipt = publication, *receipt
	require.NoError(t, image.Validate())
	destination := f.grant.Staging
	destination.Target = destination.Destination
	prefetch := protocol.MigrationImagePrefetchRequest{Staging: destination, Publication: publication,
		Plan: protocol.MigrationPublicationPlan{RequestDigest: receipt.RequestDigest, Binding: receipt.Binding, Reference: receipt.Reference, Peer: receipt.Peer}}
	require.NoError(t, prefetch.Validate())
	return prefetch, image
}

func (f capturePeerNodeFixture) stream(t *testing.T, data []byte) string {
	t.Helper()
	require.NoError(t, f.source.RecordMigrationCapture(t.Context(), f.capture))
	record, err := f.source.journal.Get(f.capture.Request.Target.SlotID)
	require.NoError(t, err)
	require.NoError(t, os.Mkdir(record.Migration.ImageDirectory, 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(record.Migration.ImageDirectory, "pages.img"), data, 0o600))
	target, err := f.target.journal.Get(f.image.Target.SlotID)
	require.NoError(t, err)
	cache := target.MigrationStaging.CapturePeer.ImageDirectory
	require.Eventually(t, func() bool {
		info, err := os.Stat(filepath.Join(cache, "pages.img"))
		return err == nil && info.Size() == int64(len(data))
	}, 5*time.Second, 10*time.Millisecond, "actual source capture uploader did not stream to target")
	return cache
}

func TestNodeCapturePeerStreamsAndRepairsIntoPublishedPreparation(t *testing.T) {
	for _, fault := range []string{"unchanged", "changed-final-chunk", "corrupt-cache", "restarted-cache", "legacy-repair-protocol"} {
		t.Run(fault, func(t *testing.T) {
			f := newCapturePeerNodeFixture(t)
			data := bytes.Repeat([]byte{7}, runtimecheckpoint.ChunkBytes)
			cacheDir := f.stream(t, data)
			if fault == "changed-final-chunk" {
				data[13] = 8
			}
			prefetch, image := f.publish(t, data)
			record, err := f.target.journal.Get(image.Target.SlotID)
			require.NoError(t, err)
			cache := f.target.capturePeerCache(image.Target.SlotID, record.MigrationStaging.CapturePeer.RequestDigest)
			require.NotNil(t, cache)
			inventory, err := cache.Inventory(t.Context())
			require.NoError(t, err)
			require.Len(t, inventory.Files, 1)
			if fault == "corrupt-cache" {
				require.NoError(t, os.WriteFile(filepath.Join(cacheDir, "pages.img"), bytes.Repeat([]byte{9}, len(data)), 0o600))
			}
			if fault == "restarted-cache" {
				require.NoError(t, f.target.closeMigrationCapturePeerCaches())
			}
			if fault == "legacy-repair-protocol" {
				f.source.runtime = &legacyCapturePeerSourceRuntime{f.source.runtime.(*capturePeerSourceRuntime)}
			}
			filled, err := f.target.PrefetchMigrationImage(t.Context(), prefetch)
			require.NoError(t, err)
			require.NoError(t, filled.ValidateFor(prefetch))
			if fault == "unchanged" {
				require.Less(t, f.output.Load(), int64(32<<10), "final stream must reuse the tentative 8 MiB chunk")
			} else {
				require.GreaterOrEqual(t, f.output.Load(), int64(len(data)), "changed, corrupt or lost caches require full repair/fallback")
			}
			prepared, err := f.target.PrepareMigrationImage(t.Context(), image)
			require.NoError(t, err)
			require.NoError(t, prepared.ValidateFor(image))
			require.Equal(t, 1, f.downloads.verifyCalls, "prefetch must still verify the committed regional manifest")
			require.Zero(t, f.downloads.downloadCalls)
			record, err = f.target.journal.Get(image.Target.SlotID)
			require.NoError(t, err)
			require.True(t, record.MigrationStaging.CapturePeer.Removed)
			require.True(t, record.MigrationStaging.Prefetch.Removed)
			require.Nil(t, f.target.capturePeerCache(image.Target.SlotID, record.MigrationStaging.CapturePeer.RequestDigest))
			actual, err := os.ReadFile(filepath.Join(record.MigrationDestination.ImageDirectory, "pages.img"))
			require.NoError(t, err)
			require.Equal(t, data, actual)
			_, err = os.Stat(cacheDir)
			require.ErrorIs(t, err, os.ErrNotExist)
		})
	}
}

func TestNodeCapturePeerRejectsWrongClientAndGrantBeforeCacheCreation(t *testing.T) {
	f := newCapturePeerNodeFixture(t)
	grant := f.grant
	grant.Staging.Target = grant.Staging.Destination
	want, err := grant.Digest()
	require.NoError(t, err)
	other, err := runtimecheckpoint.NewPeerIdentity()
	require.NoError(t, err)
	for _, test := range []struct {
		identity tls.Certificate
		digest   string
	}{{other, want}, {f.source.migrationPeer.identity, strings.Repeat("f", 64)}} {
		client, err := runtimecheckpoint.NewPeerClient(grant.Destination.Peer, test.identity)
		require.NoError(t, err)
		defer client.CloseIdleConnections()
		payload, err := json.Marshal(migrationCapturePeerRead{SlotID: grant.Staging.Target.SlotID, RequestDigest: test.digest})
		require.NoError(t, err)
		request, err := http.NewRequestWithContext(t.Context(), http.MethodPost, grant.Destination.Peer.Address+migrationCapturePeerPath, bytes.NewReader(nil))
		require.NoError(t, err)
		request.Header.Set(migrationCapturePeerHeader, string(payload))
		response, err := client.Do(request)
		require.NoError(t, err)
		require.Equal(t, http.StatusForbidden, response.StatusCode)
		require.NoError(t, response.Body.Close())
	}
	record, err := f.target.journal.Get(grant.Staging.Target.SlotID)
	require.NoError(t, err)
	_, err = os.Stat(record.MigrationStaging.CapturePeer.ImageDirectory)
	require.ErrorIs(t, err, os.ErrNotExist)
}

func TestNodeCapturePeerReleaseJoinsReceiverWhileRegionalUploadContinues(t *testing.T) {
	f := newCapturePeerNodeFixture(t)
	data := bytes.Repeat([]byte{7}, runtimecheckpoint.ChunkBytes)
	cacheDir := f.stream(t, data)
	request := f.grant.Staging
	request.Target = request.Destination
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	require.NoError(t, f.target.ReleaseMigrationStaging(ctx, request))
	record, err := f.target.journal.Get(request.Target.SlotID)
	require.NoError(t, err)
	require.True(t, record.MigrationStaging.Released)
	require.True(t, record.MigrationStaging.CapturePeer.Removed)
	require.Nil(t, f.target.capturePeerCache(request.Target.SlotID, record.MigrationStaging.CapturePeer.RequestDigest))
	_, err = os.Stat(cacheDir)
	require.ErrorIs(t, err, os.ErrNotExist)
	// A peer rejection interrupts only its optional stream, not regional data
	// custody or newly produced source chunks within the original byte budget.
	source, err := f.source.journal.Get(f.capture.Request.Target.SlotID)
	require.NoError(t, err)
	data = bytes.Repeat([]byte{8}, runtimecheckpoint.ChunkBytes)
	file, err := os.OpenFile(filepath.Join(source.Migration.ImageDirectory, "pages.img"), os.O_APPEND|os.O_WRONLY, 0)
	require.NoError(t, err)
	_, err = file.Write(data)
	require.NoError(t, err)
	require.NoError(t, file.Close())
	key := "runtime-checkpoints/capture-v1/" + strings.TrimPrefix(f.grant.Staging.CaptureUpload.ScopeDigest, "sha256:") + "/chunks/" + digest.FromBytes(data).Encoded()
	require.Eventually(t, func() bool { _, err := f.objects.Head(key); return err == nil }, 5*time.Second, 10*time.Millisecond)
	capture := f.capture
	capture.State = protocol.MigrationCaptureComplete
	require.NoError(t, f.source.RecordMigrationCapture(t.Context(), capture))
}

type capturePeerPublicationBarrier struct {
	objectstore.ContextConditionalStore
	cleanup objectstore.ContextCleanupStore
	started chan struct{}
	resume  chan struct{}
}

func (s *capturePeerPublicationBarrier) ListContext(ctx context.Context, p, a, token, delimiter string, n int64) ([]objectstore.Info, bool, string, error) {
	return s.cleanup.ListContext(ctx, p, a, token, delimiter, n)
}
func (s *capturePeerPublicationBarrier) DeleteContext(ctx context.Context, key string) error {
	return s.cleanup.DeleteContext(ctx, key)
}
func (s *capturePeerPublicationBarrier) PutIfAbsentContext(ctx context.Context, key string, input io.Reader) (bool, error) {
	if strings.HasSuffix(key, "/manifest.json") {
		close(s.started)
		select {
		case <-s.resume:
		case <-ctx.Done():
			return false, ctx.Err()
		}
	}
	return s.ContextConditionalStore.PutIfAbsentContext(ctx, key, input)
}

func TestNodeCapturePeerRepairOverlapsPublicationWithoutGrantingExecution(t *testing.T) {
	raw := objectstore.NewMemoryStore("")
	objects := &capturePeerPublicationBarrier{ContextConditionalStore: raw.(objectstore.ContextConditionalStore),
		cleanup: raw.(objectstore.ContextCleanupStore), started: make(chan struct{}), resume: make(chan struct{})}
	f := newCapturePeerNodeFixture(t, objects)
	// Register after fixture cleanup so a test failure cannot strand its worker.
	t.Cleanup(func() {
		select {
		case <-objects.resume:
		default:
			close(objects.resume)
		}
	})
	data := bytes.Repeat([]byte{4}, runtimecheckpoint.ChunkBytes)
	f.stream(t, data)
	publication := f.seal(t, data)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	plan, err := f.source.PlanMigrationPublication(ctx, publication)
	require.NoError(t, err)
	select {
	case <-objects.started:
	case <-ctx.Done():
		t.Fatal("publication did not reach its commit barrier")
	}
	destination := f.grant.Staging
	destination.Target = destination.Destination
	prefetch := protocol.MigrationImagePrefetchRequest{Staging: destination, Publication: publication, Plan: *plan}
	filled, err := f.target.PrefetchMigrationImage(ctx, prefetch)
	require.NoError(t, err)
	require.NoError(t, filled.ValidateFor(prefetch))
	require.Less(t, f.output.Load(), int64(32<<10))
	record, err := f.target.journal.Get(destination.Target.SlotID)
	require.NoError(t, err)
	require.Nil(t, record.MigrationDestination, "repair is disposable prefetch, not prepared execution custody")
	source, err := f.source.journal.Get(publication.Capture.Request.Target.SlotID)
	require.NoError(t, err)
	require.Nil(t, source.Migration.Publication, "regional publication has not committed")
	close(objects.resume)
	receipt, err := f.source.PublishMigration(ctx, publication)
	require.NoError(t, err)
	image := f.image
	image.Publication, image.Receipt = publication, *receipt
	prepared, err := f.target.PrepareMigrationImage(ctx, image)
	require.NoError(t, err)
	require.NoError(t, prepared.ValidateFor(image))
	require.Equal(t, 1, f.downloads.verifyCalls)
	require.Zero(t, f.downloads.downloadCalls)
}

// Simulate an older source's repair header while retaining the shared full-image
// fallback. Version mismatch must release the optional cache before retrying.
type legacyCapturePeerSourceRuntime struct{ *capturePeerSourceRuntime }

func (r *legacyCapturePeerSourceRuntime) WriteMigrationCapturePeerFinal(ctx context.Context, b runtimecheckpoint.Binding, ref runtimecheckpoint.Reference,
	p *runtimecheckpoint.LocalImagePlan, dir string, inventory runtimecheckpoint.CapturePeerInventory, w io.Writer) error {
	return r.capturePeerSourceRuntime.WriteMigrationCapturePeerFinal(ctx, b, ref, p, dir, inventory, legacyCapturePeerWriter{w})
}

type legacyCapturePeerWriter struct{ io.Writer }

func (w legacyCapturePeerWriter) Write(p []byte) (int, error) {
	if len(p) == 12 && string(p[:8]) == "S0CKPT\x00\x03" {
		p = bytes.Clone(p)
		p[7] = 2
	}
	return w.Writer.Write(p)
}

func TestNodeCapturePeerRepairRetriesPublicationCustodyHandoff(t *testing.T) {
	f := newCapturePeerNodeFixture(t)
	data := bytes.Repeat([]byte{7}, runtimecheckpoint.ChunkBytes)
	f.stream(t, data)
	prefetch, image := f.publish(t, data)
	slot := f.capture.Request.Target.SlotID
	require.True(t, f.source.beginReconciliation(slot, nil))
	held := true
	defer func() {
		if held {
			f.source.endReconciliation(slot)
		}
	}()
	before := f.output.Load()
	done := make(chan error, 1)
	go func() { _, err := f.target.PrefetchMigrationImage(t.Context(), prefetch); done <- err }()
	// The first authenticated request sees the same 503 as the small window
	// between closing publication reads and releasing source reconciliation.
	require.Eventually(t, func() bool { return f.output.Load() > before }, time.Second, time.Millisecond)
	f.source.endReconciliation(slot)
	held = false
	require.NoError(t, <-done)
	require.Less(t, f.output.Load()-before, int64(32<<10), "handoff must preserve chunk reuse")
	prepared, err := f.target.PrepareMigrationImage(t.Context(), image)
	require.NoError(t, err)
	require.NoError(t, prepared.ValidateFor(image))
	require.Equal(t, 1, f.downloads.verifyCalls)
	require.Zero(t, f.downloads.downloadCalls)
}

func TestNodeCapturePeerPersistentBusyRemainsBoundedAndFallsBack(t *testing.T) {
	f := newCapturePeerNodeFixture(t)
	data := bytes.Repeat([]byte{9}, runtimecheckpoint.ChunkBytes)
	f.stream(t, data)
	prefetch, image := f.publish(t, data)
	slot := f.capture.Request.Target.SlotID
	require.True(t, f.source.beginReconciliation(slot, nil))
	defer f.source.endReconciliation(slot)
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	_, err := f.target.PrefetchMigrationImage(ctx, prefetch)
	require.Error(t, err)
	require.NoError(t, ctx.Err(), "busy retry must finish before the operation deadline")
	record, err := f.target.journal.Get(image.Target.SlotID)
	require.NoError(t, err)
	require.True(t, record.MigrationStaging.CapturePeer.Removed)
	require.Nil(t, record.MigrationStaging.Prefetch.Receipt)
	prepared, err := f.target.PrepareMigrationImage(t.Context(), image)
	require.NoError(t, err)
	require.NoError(t, prepared.ValidateFor(image))
	require.Equal(t, 1, f.downloads.downloadCalls, "published regional image remains the recovery path")
}

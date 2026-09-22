package nomadruntime

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/runtimecheckpoint"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
	"github.com/stretchr/testify/require"
)

type plannedMigrationPeerTestRuntime struct {
	*migrationImageTestRuntime
	uploadStarted chan struct{}
	uploadResume  chan struct{}
	uploadError   error
	write         func(context.Context, io.Writer) error
}

func (r *plannedMigrationPeerTestRuntime) PlanMigrationImage(ctx context.Context, binding runtimecheckpoint.Binding, directory string) (runtimecheckpoint.LocalImagePlan, error) {
	return r.store.PlanLocal(ctx, binding, directory)
}

func (r *plannedMigrationPeerTestRuntime) PublishPlannedMigrationImage(ctx context.Context, binding runtimecheckpoint.Binding, plan runtimecheckpoint.LocalImagePlan, directory string) (runtimecheckpoint.Reference, error) {
	close(r.uploadStarted)
	select {
	case <-ctx.Done():
		return runtimecheckpoint.Reference{}, ctx.Err()
	case <-r.uploadResume:
	}
	if r.uploadError != nil {
		return runtimecheckpoint.Reference{}, r.uploadError
	}
	return r.store.PublishPlanned(ctx, binding, plan, directory)
}

func (r *plannedMigrationPeerTestRuntime) WritePlannedMigrationPeerImage(ctx context.Context, binding runtimecheckpoint.Binding, plan runtimecheckpoint.LocalImagePlan, directory string, output io.Writer) error {
	if r.write != nil {
		return r.write(ctx, output)
	}
	return r.store.WritePlannedPeerImage(ctx, binding, plan, directory, output)
}

type plannedMigrationPeerFixture struct {
	source   *nodeRuntime
	request  protocol.MigrationPublicationRequest
	runtime  *plannedMigrationPeerTestRuntime
	client   tls.Certificate
	plan     runtimecheckpoint.LocalImagePlan
	result   chan error
	release  func()
	observed chan *protocol.MigrationPublicationPlan
}

func newPlannedMigrationPeerFixture(t *testing.T, configure func(*plannedMigrationPeerTestRuntime), startWithPlan ...bool) plannedMigrationPeerFixture {
	t.Helper()
	source, request, images := migrationImageNodeFixture(t)
	parent, cancel := context.WithCancel(t.Context())
	source.migrationContext = parent
	client, err := runtimecheckpoint.NewPeerIdentity()
	require.NoError(t, err)
	identity, err := runtimecheckpoint.NewPeerIdentity()
	require.NoError(t, err)
	server := httptest.NewUnstartedServer(http.HandlerFunc(source.serveMigrationPeer))
	server.TLS = &tls.Config{MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{identity}, ClientAuth: tls.RequireAnyClientCert}
	server.StartTLS()
	endpoint, err := runtimecheckpoint.NewPeerEndpoint(strings.TrimPrefix(server.URL, "https://"), identity)
	require.NoError(t, err)
	source.migrationPeer = &migrationPeer{identity: identity, endpoint: endpoint}
	request.DestinationPeerCertificateSHA256 = runtimecheckpoint.PeerCertificateDigest(client)
	runtime := &plannedMigrationPeerTestRuntime{migrationImageTestRuntime: images,
		uploadStarted: make(chan struct{}), uploadResume: make(chan struct{})}
	if configure != nil {
		configure(runtime)
	}
	source.runtime = runtime
	binding, err := request.Binding()
	require.NoError(t, err)
	custody, err := source.GetMigrationCapture(t.Context(), request.Capture.Request.Target.SlotID)
	require.NoError(t, err)
	plan, err := images.store.PlanLocal(t.Context(), binding, custody.ImageDirectory)
	require.NoError(t, err)
	result := make(chan error, 1)
	observed := make(chan *protocol.MigrationPublicationPlan, 1)
	go func() {
		if len(startWithPlan) > 0 && startWithPlan[0] {
			plan, err := source.PlanMigrationPublication(parent, request)
			if err != nil {
				result <- err
				return
			}
			observed <- plan
		}
		_, err := source.PublishMigration(parent, request)
		result <- err
	}()
	t.Cleanup(func() { cancel(); server.Close(); source.wg.Wait() })
	select {
	case <-runtime.uploadStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("planned publication did not start")
	}
	var once sync.Once
	return plannedMigrationPeerFixture{source: source, request: request, runtime: runtime, client: client, plan: plan,
		result: result, observed: observed, release: func() { once.Do(func() { close(runtime.uploadResume) }) }}
}

func TestMigrationPublicationPlanStartsUploadAndReturnsBeforeDurability(t *testing.T) {
	f := newPlannedMigrationPeerFixture(t, nil, true)
	var first *protocol.MigrationPublicationPlan
	select {
	case first = <-f.observed:
	case <-time.After(time.Second):
		t.Fatal("planning waited for regional upload")
	}
	require.NoError(t, first.ValidateFor(f.request))
	require.Equal(t, f.plan.Reference, first.Reference)
	again, err := f.source.PlanMigrationPublication(t.Context(), f.request)
	require.NoError(t, err)
	require.Equal(t, first, again)
	first.Reference.ManifestDigest = strings.Repeat("f", 64)
	third, err := f.source.PlanMigrationPublication(t.Context(), f.request)
	require.NoError(t, err)
	require.Equal(t, again, third, "callers cannot mutate the worker's shared plan")
	custody, err := f.source.GetMigrationCapture(t.Context(), f.request.Capture.Request.Target.SlotID)
	require.NoError(t, err)
	require.Nil(t, custody.Publication)
	f.release()
	require.NoError(t, <-f.result)
	// A subsequent observer may read the already-journaled publication rather
	// than the original worker. It still receives the distinct plan type.
	after, err := f.source.PlanMigrationPublication(t.Context(), f.request)
	require.NoError(t, err)
	require.Equal(t, again, after)
}

func TestMigrationPublicationPlanRejectsUnsupportedAndChangedRequests(t *testing.T) {
	source, request, _ := migrationImageNodeFixture(t)
	_, err := source.PlanMigrationPublication(t.Context(), request)
	require.Error(t, err, "disabled peer transport must not start an upload")
	require.Empty(t, source.migrationPublications)
	f := newPlannedMigrationPeerFixture(t, nil)
	changed := f.request
	changed.CPUFeaturesDigest = "sha256:" + strings.Repeat("f", 64)
	_, err = f.source.PlanMigrationPublication(t.Context(), changed)
	require.Error(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err = f.source.PlanMigrationPublication(ctx, f.request)
	require.ErrorIs(t, err, context.Canceled)
	f.release()
	require.NoError(t, <-f.result, "a canceled observer does not cancel the shared upload")
}

func (f plannedMigrationPeerFixture) read(t *testing.T, identity tls.Certificate, requestDigest string, planned bool) *http.Response {
	t.Helper()
	client, err := runtimecheckpoint.NewPeerClient(f.source.migrationPeer.endpoint, identity)
	require.NoError(t, err)
	t.Cleanup(client.CloseIdleConnections)
	payload, err := json.Marshal(migrationPeerRead{SlotID: f.request.Capture.Request.Target.SlotID, RequestDigest: requestDigest, Planned: planned})
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	t.Cleanup(cancel)
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, f.source.migrationPeer.endpoint.Address+runtimecheckpoint.PeerImagePath, bytes.NewReader(payload))
	require.NoError(t, err)
	response, err := client.Do(request)
	require.NoError(t, err)
	t.Cleanup(func() { _ = response.Body.Close() })
	return response
}

func TestMigrationPeerTransfersDuringRegionalPublication(t *testing.T) {
	f := newPlannedMigrationPeerFixture(t, nil)
	want, err := f.request.Digest()
	require.NoError(t, err)
	response := f.read(t, f.client, want, true)
	require.Equal(t, http.StatusOK, response.StatusCode)
	directory := filepath.Join(t.TempDir(), "prefetched")
	_, err = runtimecheckpoint.ReceivePeerImage(t.Context(), f.plan.Manifest.Binding, f.plan.Reference, directory, response.Body, runtimecheckpoint.ChunkBytes, func(bytes int64, inodes uint64) error {
		if bytes > runtimecheckpoint.ChunkBytes || inodes > 16 {
			return errors.New("test staging budget exceeded")
		}
		return nil
	})
	require.NoError(t, err)
	data, err := os.ReadFile(filepath.Join(directory, "checkpoint.img"))
	require.NoError(t, err)
	require.Equal(t, "retained-memory", string(data))
	custody, err := f.source.GetMigrationCapture(t.Context(), f.request.Capture.Request.Target.SlotID)
	require.NoError(t, err)
	require.Nil(t, custody.Publication, "peer completion cannot imply durable publication")
	_, err = f.runtime.store.Download(t.Context(), f.plan.Manifest.Binding, f.plan.Reference, filepath.Join(t.TempDir(), "unpublished"))
	require.Error(t, err, "the region still has no published manifest")
	f.release()
	require.NoError(t, <-f.result)
	custody, err = f.source.GetMigrationCapture(t.Context(), f.request.Capture.Request.Target.SlotID)
	require.NoError(t, err)
	require.Equal(t, f.plan.Reference, custody.Publication.Reference)
	_, err = f.runtime.store.Download(t.Context(), f.plan.Manifest.Binding, f.plan.Reference, filepath.Join(t.TempDir(), "published"))
	require.NoError(t, err)
}

func TestMigrationPlannedPeerRejectsWrongIdentityAndOperation(t *testing.T) {
	f := newPlannedMigrationPeerFixture(t, nil)
	want, err := f.request.Digest()
	require.NoError(t, err)
	other, err := runtimecheckpoint.NewPeerIdentity()
	require.NoError(t, err)
	for _, test := range []struct {
		name     string
		identity tls.Certificate
		digest   string
		planned  bool
		status   int
	}{
		{"other destination", other, want, true, http.StatusForbidden},
		{"other operation", f.client, strings.Repeat("f", 64), true, http.StatusForbidden},
		{"ordinary read before publication", f.client, want, false, http.StatusServiceUnavailable},
	} {
		t.Run(test.name, func(t *testing.T) {
			response := f.read(t, test.identity, test.digest, test.planned)
			require.Equal(t, test.status, response.StatusCode)
			body, err := io.ReadAll(response.Body)
			require.NoError(t, err)
			require.NotContains(t, string(body), "retained-memory")
		})
	}
	f.release()
	require.NoError(t, <-f.result)
	response := f.read(t, f.client, want, true)
	require.Equal(t, http.StatusOK, response.StatusCode, "late prefetch reads use the exact durable publication")
	body, err := io.ReadAll(response.Body)
	require.NoError(t, err)
	require.Contains(t, string(body), "retained-memory")
}

func TestMigrationPublicationFailureInterruptsBlockedPeer(t *testing.T) {
	finished := make(chan error, 1)
	f := newPlannedMigrationPeerFixture(t, func(r *plannedMigrationPeerTestRuntime) {
		r.uploadError = errors.New("regional upload failed")
		r.write = func(_ context.Context, output io.Writer) error {
			block := make([]byte, 64<<10)
			for {
				if _, err := output.Write(block); err != nil {
					finished <- err
					return err
				}
			}
		}
	})
	want, err := f.request.Digest()
	require.NoError(t, err)
	response := f.read(t, f.client, want, true)
	require.Equal(t, http.StatusOK, response.StatusCode)
	// Keep the socket open and do not read. The failed publication must expire
	// the source write deadline, then join the reader before freeing custody.
	f.release()
	select {
	case err := <-f.result:
		require.ErrorContains(t, err, "regional upload failed")
	case <-time.After(time.Second):
		t.Fatal("publication did not interrupt and join its blocked reader")
	}
	require.Error(t, <-finished)
	slot := f.request.Capture.Request.Target.SlotID
	require.True(t, f.source.beginReconciliation(slot, nil))
	f.source.endReconciliation(slot)
	custody, err := f.source.GetMigrationCapture(t.Context(), slot)
	require.NoError(t, err)
	require.Nil(t, custody.Publication)
}

func TestMigrationPublicationRetainsCustodyUntilSuccessfulReaderExits(t *testing.T) {
	started, exit := make(chan struct{}), make(chan struct{})
	var once sync.Once
	t.Cleanup(func() { once.Do(func() { close(exit) }) })
	f := newPlannedMigrationPeerFixture(t, func(r *plannedMigrationPeerTestRuntime) {
		r.write = func(ctx context.Context, output io.Writer) error {
			_, err := output.Write(make([]byte, 64<<10))
			if err != nil {
				return err
			}
			close(started)
			select {
			case <-exit:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	})
	want, err := f.request.Digest()
	require.NoError(t, err)
	response := f.read(t, f.client, want, true)
	require.Equal(t, http.StatusOK, response.StatusCode)
	<-started
	f.release()
	slot := f.request.Capture.Request.Target.SlotID
	require.Eventually(t, func() bool {
		custody, err := f.source.GetMigrationCapture(t.Context(), slot)
		return err == nil && custody.Publication != nil
	}, time.Second, time.Millisecond)
	require.False(t, f.source.beginReconciliation(slot, nil), "published bytes remain pinned until the reader exits")
	select {
	case <-f.result:
		t.Fatal("publication returned while a reader retained its source image")
	default:
	}
	once.Do(func() { close(exit) })
	require.NoError(t, <-f.result)
	require.True(t, f.source.beginReconciliation(slot, nil))
	f.source.endReconciliation(slot)
}

func TestMigrationPublicationCleanupPreemptsReaderAfterDurableUpload(t *testing.T) {
	f := newPlannedMigrationPeerFixture(t, func(r *plannedMigrationPeerTestRuntime) {
		r.write = func(_ context.Context, output io.Writer) error {
			block := make([]byte, 64<<10)
			for {
				if _, err := output.Write(block); err != nil {
					return err
				}
			}
		}
	})
	want, err := f.request.Digest()
	require.NoError(t, err)
	response := f.read(t, f.client, want, true)
	require.Equal(t, http.StatusOK, response.StatusCode)
	f.release()
	slot := f.request.Capture.Request.Target.SlotID
	require.Eventually(t, func() bool {
		f.source.mu.Lock()
		defer f.source.mu.Unlock()
		state := f.source.inflight[slot]
		return state != nil && state.cancel != nil
	}, time.Second, time.Millisecond)
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	require.NoError(t, f.source.beginExternalReconciliation(ctx, slot))
	defer f.source.endReconciliation(slot)
	require.NoError(t, <-f.result)
	custody, err := f.source.GetMigrationCapture(t.Context(), slot)
	require.NoError(t, err)
	require.NotNil(t, custody.Publication, "preempting a transport cannot erase durable publication")
}

type closingMigrationPeerTestRuntime struct {
	nodeRuntimeBackend
	started  chan struct{}
	canceled chan struct{}
	leave    chan struct{}
}

func (r *closingMigrationPeerTestRuntime) WriteMigrationPeerImage(ctx context.Context, _ runtimecheckpoint.Binding, _ runtimecheckpoint.Reference, _ string, output io.Writer) error {
	if _, err := output.Write(make([]byte, 64<<10)); err != nil {
		return err
	}
	close(r.started)
	<-ctx.Done()
	close(r.canceled)
	<-r.leave // Model unwinding an in-flight disk read after cancellation.
	return ctx.Err()
}

func TestMigrationPeerCloseJoinsHandlerBeforeRuntimeShutdown(t *testing.T) {
	source, target, request, _, oldServer := migrationPeerFixture(t)
	oldServer.Close()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	address := listener.Addr().String()
	require.NoError(t, listener.Close())
	peer, err := source.startMigrationPeer(t.Context(), address)
	require.NoError(t, err)
	t.Cleanup(peer.close)
	source.migrationPeer = peer
	request.Receipt.Peer = peer.endpoint
	runtime := &closingMigrationPeerTestRuntime{nodeRuntimeBackend: source.runtime, started: make(chan struct{}), canceled: make(chan struct{}), leave: make(chan struct{})}
	var once sync.Once
	t.Cleanup(func() { once.Do(func() { close(runtime.leave) }) })
	source.runtime = runtime
	client, err := runtimecheckpoint.NewPeerClient(peer.endpoint, target.migrationPeer.identity)
	require.NoError(t, err)
	defer client.CloseIdleConnections()
	payload, err := json.Marshal(migrationPeerRead{SlotID: request.Publication.Capture.Request.Target.SlotID, RequestDigest: request.Receipt.RequestDigest})
	require.NoError(t, err)
	response, err := client.Post(peer.endpoint.Address+runtimecheckpoint.PeerImagePath, "application/json", bytes.NewReader(payload))
	require.NoError(t, err)
	defer response.Body.Close()
	require.Equal(t, http.StatusOK, response.StatusCode)
	_, err = io.CopyN(io.Discard, response.Body, 64<<10)
	require.NoError(t, err)
	<-runtime.started
	closed := make(chan struct{})
	go func() { peer.close(); close(closed) }()
	select {
	case <-runtime.canceled:
	case <-time.After(time.Second):
		t.Fatal("peer shutdown did not cancel its reader")
	}
	select {
	case <-closed:
		t.Fatal("peer shutdown returned before its handler stopped using the runtime")
	default:
	}
	once.Do(func() { close(runtime.leave) })
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("peer shutdown did not join its reader")
	}
	require.False(t, source.reconciliationInFlight(request.Publication.Capture.Request.Target.SlotID))
	late := httptest.NewRecorder()
	peer.handler(source).ServeHTTP(late, httptest.NewRequest(http.MethodPost, runtimecheckpoint.PeerImagePath, nil))
	require.Equal(t, http.StatusServiceUnavailable, late.Code)
}

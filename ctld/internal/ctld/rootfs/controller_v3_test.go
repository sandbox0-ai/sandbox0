package rootfs

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
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
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sandbox0-ai/sandbox0/ctld/internal/ctld/rootfsstore"
	"github.com/sandbox0-ai/sandbox0/pkg/ctldapi"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshead"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/objectstore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestControllerRootFSSyncSealAndMaterialize(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	upperdir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(upperdir, "initial.txt"), []byte("initial"), 0o640))
	require.NoError(t, os.Mkdir(filepath.Join(upperdir, "tmp"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(upperdir, "tmp", "excluded.txt"), []byte("excluded"), 0o600))

	base, baseConfig := testRootFSBase(t)
	info := rootFSInfo("gvisor")
	info.Snapshotter = rootfshead.SnapshotterName
	runtime := &fakeV3Runtime{
		fakeRuntime: &fakeRuntime{info: info},
		upperdir:    upperdir,
		base:        base,
		baseConfig:  baseConfig,
	}
	store := objectstore.NewMemoryStore(t.Name())
	registry := prometheus.NewRegistry()
	leases := &fakeCaptureLeases{}
	controller := NewController(Config{Context: ctx, Runtime: runtime, Store: store, WatchFenceRoot: t.TempDir(), CaptureLeases: leases, MetricsRegistry: registry})
	request := httptest.NewRequest(http.MethodPost, "/", nil)

	bound, status := controller.BindRootFSSync(request, ctldapi.BindRootFSSyncRequest{
		Target:            rootFSTarget(),
		SandboxID:         "sandbox-1",
		TeamID:            "team-1",
		RuntimeGeneration: 7,
	})
	require.Equal(t, http.StatusOK, status, bound.Error)
	require.Eventually(t, func() bool {
		current, currentStatus := controller.GetRootFSSyncStatus(request, ctldapi.GetRootFSSyncStatusRequest{
			SandboxID:         "sandbox-1",
			RuntimeGeneration: 7,
		})
		return currentStatus == http.StatusOK && current.Status.InitialScanComplete && current.Status.LastError == ""
	}, 5*time.Second, 10*time.Millisecond)

	require.NoError(t, os.WriteFile(filepath.Join(upperdir, "final.txt"), []byte("final state"), 0o600))
	sealed, status := controller.SealRootFSHead(request, ctldapi.SealRootFSHeadRequest{
		SandboxID:                 "sandbox-1",
		TeamID:                    "team-1",
		HeadID:                    "head-1",
		ExpectedRuntimeGeneration: 7,
	})
	require.Equal(t, http.StatusOK, status, sealed.Error)
	require.NoError(t, sealed.Reference.Validate())
	require.NoError(t, sealed.Image.Validate())
	assert.Equal(t, base, sealed.Head.Base)
	encodedSeal, err := json.Marshal(sealed)
	require.NoError(t, err)
	assert.NotContains(t, string(encodedSeal), "referenced_objects")
	assert.Less(t, len(encodedSeal), 8<<10, "seal response must contain only bounded metadata")
	assert.Equal(t, float64(1), rootFSSyncGaugeValue(t, registry, "ctld_rootfs_sync_sessions"))
	assert.Equal(t, float64(1), rootFSSyncGaugeValue(t, registry, "ctld_rootfs_sync_sealed_sessions"))

	head, err := rootfsstore.LoadHead(ctx, store, sealed.Reference)
	require.NoError(t, err)
	finalEntry, found, err := lookupV3TestEntry(ctx, store, sealed.Reference, head.Root, "final.txt")
	require.NoError(t, err)
	require.True(t, found)
	payload, err := readV3TestFile(ctx, store, sealed.Reference, finalEntry)
	require.NoError(t, err)
	assert.Equal(t, "final state", string(payload))
	_, found, err = lookupV3TestEntry(ctx, store, sealed.Reference, head.Root, "tmp")
	require.NoError(t, err)
	assert.False(t, found)

	retried, status := controller.SealRootFSHead(request, ctldapi.SealRootFSHeadRequest{
		SandboxID:                 "sandbox-1",
		TeamID:                    "team-1",
		HeadID:                    "head-1",
		ExpectedRuntimeGeneration: 7,
	})
	require.Equal(t, http.StatusOK, status, retried.Error)
	assert.Equal(t, sealed.Reference, retried.Reference)
	_, status = controller.SealRootFSHead(request, ctldapi.SealRootFSHeadRequest{
		SandboxID:                 "sandbox-1",
		TeamID:                    "team-1",
		HeadID:                    "head-2",
		ExpectedRuntimeGeneration: 7,
	})
	assert.Equal(t, http.StatusConflict, status)
	rebound, status := controller.BindRootFSSync(request, ctldapi.BindRootFSSyncRequest{
		Target:            rootFSTarget(),
		SandboxID:         "sandbox-1",
		TeamID:            "team-1",
		RuntimeGeneration: 7,
		Parent:            &sealed.Reference,
	})
	require.Equal(t, http.StatusOK, status, rebound.Error)
	require.NotNil(t, rebound.Status.SealedReference)
	assert.Equal(t, sealed.Reference, *rebound.Status.SealedReference)
	acknowledged, status := controller.AcknowledgeRootFSHead(request, ctldapi.AcknowledgeRootFSHeadRequest{
		SandboxID:         "sandbox-1",
		TeamID:            "team-1",
		RuntimeGeneration: 7,
		HeadID:            "head-1",
		Published:         true,
		RuntimeContinues:  true,
	})
	require.Equal(t, http.StatusOK, status, acknowledged.Error)
	assert.True(t, acknowledged.Acknowledged)
	assert.Equal(t, int64(1), leases.resetCalls.Load())
	assert.Equal(t, float64(0), rootFSSyncGaugeValue(t, registry, "ctld_rootfs_sync_sealed_sessions"))
	assert.Equal(t, float64(1), rootFSSyncGaugeValue(t, registry, "ctld_rootfs_sync_full_reconcile_sessions"))

	require.NoError(t, os.WriteFile(filepath.Join(upperdir, "after-snapshot.txt"), []byte("after snapshot"), 0o600))
	require.Eventually(t, func() bool {
		current, currentStatus := controller.GetRootFSSyncStatus(request, ctldapi.GetRootFSSyncStatusRequest{
			SandboxID:         "sandbox-1",
			RuntimeGeneration: 7,
		})
		return currentStatus == http.StatusOK && current.Status.DirtyPaths == 0 && current.Status.ActiveCaptures == 0
	}, 5*time.Second, 10*time.Millisecond)
	secondSeal, status := controller.SealRootFSHead(request, ctldapi.SealRootFSHeadRequest{
		SandboxID:                 "sandbox-1",
		TeamID:                    "team-1",
		HeadID:                    "head-2",
		ExpectedRuntimeGeneration: 7,
		ExpectedParent:            &sealed.Reference,
	})
	require.Equal(t, http.StatusOK, status, secondSeal.Error)
	secondHead, err := rootfsstore.LoadHead(ctx, store, secondSeal.Reference)
	require.NoError(t, err)
	afterEntry, found, err := lookupV3TestEntry(ctx, store, secondSeal.Reference, secondHead.Root, "after-snapshot.txt")
	require.NoError(t, err)
	require.True(t, found)
	afterPayload, err := readV3TestFile(ctx, store, secondSeal.Reference, afterEntry)
	require.NoError(t, err)
	assert.Equal(t, "after snapshot", string(afterPayload))

	materialized, status := controller.MaterializeRootFSHead(request, ctldapi.MaterializeRootFSHeadRequest{
		Reference: sealed.Reference,
		Image:     sealed.Image,
	})
	require.Equal(t, http.StatusOK, status, materialized.Error)
	assert.True(t, materialized.Materialized)
	assert.Equal(t, sealed.Reference, runtime.materializedReference)
	assert.Equal(t, base, runtime.materializedBase)
	_, err = rootfshead.DecodeImageEnvelope(runtime.materializedEnvelope)
	require.NoError(t, err)
	stopped, status := controller.AcknowledgeRootFSHead(request, ctldapi.AcknowledgeRootFSHeadRequest{
		SandboxID: "sandbox-1", TeamID: "team-1", RuntimeGeneration: 7,
		HeadID: "head-2", Published: true, RuntimeContinues: false,
	})
	require.Equal(t, http.StatusOK, status, stopped.Error)
	assert.Equal(t, int64(1), leases.releaseCalls.Load())
	assert.Equal(t, int64(1), leases.resetCalls.Load(), "stopped generations release their lease instead of checkpointing it")
}

func TestControllerBindRootFSSyncReleasesLeaseWhenSessionStartFails(t *testing.T) {
	base, baseConfig := testRootFSBase(t)
	info := rootFSInfo("gvisor")
	info.Snapshotter = rootfshead.SnapshotterName
	leases := &fakeCaptureLeases{}
	controller := NewController(Config{
		Context: context.Background(),
		Runtime: &fakeV3Runtime{
			fakeRuntime: &fakeRuntime{info: info},
			upperdir:    t.TempDir(),
			base:        base,
			baseConfig:  baseConfig,
		},
		Store:          objectstore.NewMemoryStore(t.Name()),
		WatchFenceRoot: "relative-path-is-invalid",
		CaptureLeases:  leases,
	})

	response, status := controller.BindRootFSSync(httptest.NewRequest(http.MethodPut, "/", nil), ctldapi.BindRootFSSyncRequest{
		Target:            rootFSTarget(),
		SandboxID:         "sandbox-1",
		TeamID:            "team-1",
		RuntimeGeneration: 1,
	})

	assert.Equal(t, http.StatusInternalServerError, status)
	assert.Contains(t, response.Error, "watcher fence root")
	assert.Equal(t, int64(1), leases.ensureCalls.Load())
	assert.Equal(t, int64(1), leases.releaseCalls.Load())
}

func TestControllerBindRootFSSyncAttachesPortalBackingsBeforeStartingSession(t *testing.T) {
	base, baseConfig := testRootFSBase(t)
	info := rootFSInfo("gvisor")
	info.Snapshotter = rootfshead.SnapshotterName
	upperdir := t.TempDir()
	mergedRoot := t.TempDir()
	attacher := &fakePortalBackingAttacher{}
	controller := NewController(Config{
		Context: context.Background(),
		Runtime: &fakeV3Runtime{
			fakeRuntime: &fakeRuntime{info: info},
			upperdir:    upperdir,
			mergedRoot:  mergedRoot,
			base:        base,
			baseConfig:  baseConfig,
		},
		PortalBackings: attacher,
		Store:          objectstore.NewMemoryStore(t.Name()),
		WatchFenceRoot: t.TempDir(),
		CaptureLeases:  &fakeCaptureLeases{},
	})

	response, status := controller.BindRootFSSync(httptest.NewRequest(http.MethodPut, "/", nil), ctldapi.BindRootFSSyncRequest{
		Target:            rootFSTarget(),
		SandboxID:         "sandbox-1",
		TeamID:            "team-1",
		RuntimeGeneration: 1,
	})

	require.Equal(t, http.StatusOK, status, response.Error)
	assert.Equal(t, "pod-uid", attacher.podUID)
	assert.Equal(t, mergedRoot, attacher.mergedRoot)
}

func TestControllerBindRootFSSyncFailsWhenPortalBackingAttachmentFails(t *testing.T) {
	base, baseConfig := testRootFSBase(t)
	info := rootFSInfo("gvisor")
	info.Snapshotter = rootfshead.SnapshotterName
	controller := NewController(Config{
		Context: context.Background(),
		Runtime: &fakeV3Runtime{
			fakeRuntime: &fakeRuntime{info: info},
			upperdir:    t.TempDir(),
			base:        base,
			baseConfig:  baseConfig,
		},
		PortalBackings: &fakePortalBackingAttacher{err: errors.New("portal attachment failed")},
		Store:          objectstore.NewMemoryStore(t.Name()),
		WatchFenceRoot: t.TempDir(),
		CaptureLeases:  &fakeCaptureLeases{},
	})

	response, status := controller.BindRootFSSync(httptest.NewRequest(http.MethodPut, "/", nil), ctldapi.BindRootFSSyncRequest{
		Target:            rootFSTarget(),
		SandboxID:         "sandbox-1",
		TeamID:            "team-1",
		RuntimeGeneration: 1,
	})

	assert.Equal(t, http.StatusInternalServerError, status)
	assert.Contains(t, response.Error, "portal attachment failed")
}

func TestControllerExposesAndAbandonsHeadWhenMarkerUploadFails(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	base, baseConfig := testRootFSBase(t)
	info := rootFSInfo("gvisor")
	store := &failingRootFSStore{Store: objectstore.NewMemoryStore(t.Name()), failMarkers: true}
	controller := NewController(Config{
		Context: ctx,
		Runtime: &fakeV3Runtime{
			fakeRuntime: &fakeRuntime{info: info},
			upperdir:    t.TempDir(),
			base:        base,
			baseConfig:  baseConfig,
		},
		Store: store, WatchFenceRoot: t.TempDir(), CaptureLeases: &fakeCaptureLeases{},
	})
	request := httptest.NewRequest(http.MethodPut, "/", nil)
	bound, status := controller.BindRootFSSync(request, ctldapi.BindRootFSSyncRequest{
		Target: rootFSTarget(), SandboxID: "sandbox-1", TeamID: "team-1", RuntimeGeneration: 1,
	})
	require.Equal(t, http.StatusOK, status, bound.Error)
	require.Eventually(t, func() bool {
		current, currentStatus := controller.GetRootFSSyncStatus(request, ctldapi.GetRootFSSyncStatusRequest{SandboxID: "sandbox-1", RuntimeGeneration: 1})
		return currentStatus == http.StatusOK && current.Status.InitialScanComplete
	}, 5*time.Second, 10*time.Millisecond)

	partial, status := controller.SealRootFSHead(request, ctldapi.SealRootFSHeadRequest{
		SandboxID: "sandbox-1", TeamID: "team-1", HeadID: "failed-head", ExpectedRuntimeGeneration: 1,
	})
	require.Equal(t, http.StatusServiceUnavailable, status)
	assert.Equal(t, "failed-head", partial.Reference.HeadID)
	current, status := controller.GetRootFSSyncStatus(request, ctldapi.GetRootFSSyncStatusRequest{SandboxID: "sandbox-1", RuntimeGeneration: 1})
	require.Equal(t, http.StatusOK, status)
	require.NotNil(t, current.Status.SealedReference)
	assert.Equal(t, partial.Reference, *current.Status.SealedReference)

	acknowledged, status := controller.AcknowledgeRootFSHead(request, ctldapi.AcknowledgeRootFSHeadRequest{
		SandboxID: "sandbox-1", TeamID: "team-1", RuntimeGeneration: 1,
		HeadID: "failed-head", Published: false, RuntimeContinues: true,
	})
	require.Equal(t, http.StatusOK, status, acknowledged.Error)
	store.failMarkers = false
	retried, status := controller.SealRootFSHead(request, ctldapi.SealRootFSHeadRequest{
		SandboxID: "sandbox-1", TeamID: "team-1", HeadID: "replacement-head", ExpectedRuntimeGeneration: 1,
	})
	require.Equal(t, http.StatusOK, status, retried.Error)
	assert.Equal(t, "replacement-head", retried.Reference.HeadID)
}

func TestControllerReturnsServiceUnavailableWhenSealCannotReachObjectStore(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	base, baseConfig := testRootFSBase(t)
	store := &failingRootFSStore{Store: objectstore.NewMemoryStore(t.Name())}
	controller := NewController(Config{
		Context: ctx,
		Runtime: &fakeV3Runtime{
			fakeRuntime: &fakeRuntime{info: rootFSInfo("gvisor")},
			upperdir:    t.TempDir(),
			base:        base,
			baseConfig:  baseConfig,
		},
		Store: store, WatchFenceRoot: t.TempDir(), CaptureLeases: &fakeCaptureLeases{},
	})
	request := httptest.NewRequest(http.MethodPut, "/", nil)
	bound, status := controller.BindRootFSSync(request, ctldapi.BindRootFSSyncRequest{
		Target: rootFSTarget(), SandboxID: "sandbox-1", TeamID: "team-1", RuntimeGeneration: 1,
	})
	require.Equal(t, http.StatusOK, status, bound.Error)
	require.Eventually(t, func() bool {
		current, currentStatus := controller.GetRootFSSyncStatus(request, ctldapi.GetRootFSSyncStatusRequest{SandboxID: "sandbox-1", RuntimeGeneration: 1})
		return currentStatus == http.StatusOK && current.Status.InitialScanComplete
	}, 5*time.Second, 10*time.Millisecond)

	store.failHeads.Store(true)
	response, status := controller.SealRootFSHead(request, ctldapi.SealRootFSHeadRequest{
		SandboxID: "sandbox-1", TeamID: "team-1", HeadID: "unavailable-head", ExpectedRuntimeGeneration: 1,
	})
	assert.Equal(t, http.StatusServiceUnavailable, status)
	assert.Contains(t, response.Error, rootfsstore.ErrBackendUnavailable.Error())

	store.failHeads.Store(false)
	retried, status := controller.SealRootFSHead(request, ctldapi.SealRootFSHeadRequest{
		SandboxID: "sandbox-1", TeamID: "team-1", HeadID: "recovered-head", ExpectedRuntimeGeneration: 1,
	})
	require.Equal(t, http.StatusOK, status, retried.Error)
	assert.Equal(t, "recovered-head", retried.Reference.HeadID)
}

func TestControllerBindRootFSSyncRejectsCrossTeamParent(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	base, baseConfig := testRootFSBase(t)
	info := rootFSInfo("gvisor")
	info.Snapshotter = rootfshead.SnapshotterName
	runtime := &fakeV3Runtime{
		fakeRuntime: &fakeRuntime{info: info},
		upperdir:    t.TempDir(),
		base:        base,
		baseConfig:  baseConfig,
	}
	store := objectstore.NewMemoryStore(t.Name())
	controller := NewController(Config{Context: ctx, Runtime: runtime, Store: store, WatchFenceRoot: t.TempDir(), CaptureLeases: &fakeCaptureLeases{}})
	request := httptest.NewRequest(http.MethodPost, "/", nil)

	_, status := controller.BindRootFSSync(request, ctldapi.BindRootFSSyncRequest{
		Target:            rootFSTarget(),
		SandboxID:         "source",
		TeamID:            "team-1",
		RuntimeGeneration: 1,
	})
	require.Equal(t, http.StatusOK, status)
	require.Eventually(t, func() bool {
		current, currentStatus := controller.GetRootFSSyncStatus(request, ctldapi.GetRootFSSyncStatusRequest{
			SandboxID: "source", RuntimeGeneration: 1,
		})
		return currentStatus == http.StatusOK && current.Status.InitialScanComplete
	}, 5*time.Second, 10*time.Millisecond)
	sealed, status := controller.SealRootFSHead(request, ctldapi.SealRootFSHeadRequest{
		SandboxID:                 "source",
		TeamID:                    "team-1",
		HeadID:                    "source-head",
		ExpectedRuntimeGeneration: 1,
	})
	require.Equal(t, http.StatusOK, status, sealed.Error)

	response, status := controller.BindRootFSSync(request, ctldapi.BindRootFSSyncRequest{
		Target:            rootFSTarget(),
		SandboxID:         "target",
		TeamID:            "team-2",
		RuntimeGeneration: 2,
		Parent:            &sealed.Reference,
	})
	assert.Equal(t, http.StatusForbidden, status)
	assert.Contains(t, response.Error, "tenant and public ImageFS scopes")
}

func TestControllerBindRootFSSyncAllowsPublicImageFSParent(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	base, baseConfig := testRootFSBase(t)
	info := rootFSInfo("gvisor")
	info.Snapshotter = rootfshead.SnapshotterName
	runtime := &fakeV3Runtime{
		fakeRuntime: &fakeRuntime{info: info}, upperdir: t.TempDir(),
		base: base, baseConfig: baseConfig,
	}
	store := objectstore.NewMemoryStore(t.Name())
	controller := NewController(Config{Context: ctx, Runtime: runtime, Store: store, WatchFenceRoot: t.TempDir(), CaptureLeases: &fakeCaptureLeases{}})
	request := httptest.NewRequest(http.MethodPost, "/", nil)

	_, status := controller.BindRootFSSync(request, ctldapi.BindRootFSSyncRequest{
		Target: rootFSTarget(), SandboxID: "imagefs", TeamID: rootfshead.PublicImageFSTeamID, RuntimeGeneration: 1,
	})
	require.Equal(t, http.StatusOK, status)
	require.Eventually(t, func() bool {
		current, currentStatus := controller.GetRootFSSyncStatus(request, ctldapi.GetRootFSSyncStatusRequest{SandboxID: "imagefs", RuntimeGeneration: 1})
		return currentStatus == http.StatusOK && current.Status.InitialScanComplete
	}, 5*time.Second, 10*time.Millisecond)
	sealed, status := controller.SealRootFSHead(request, ctldapi.SealRootFSHeadRequest{
		SandboxID: "imagefs", TeamID: rootfshead.PublicImageFSTeamID, HeadID: "public-imagefs-head", ExpectedRuntimeGeneration: 1,
	})
	require.Equal(t, http.StatusOK, status, sealed.Error)

	bound, status := controller.BindRootFSSync(request, ctldapi.BindRootFSSyncRequest{
		Target: rootFSTarget(), SandboxID: "tenant-sandbox", TeamID: "team-1", RuntimeGeneration: 2, Parent: &sealed.Reference,
	})
	require.Equal(t, http.StatusOK, status, bound.Error)
}

type fakeV3Runtime struct {
	*fakeRuntime
	upperdir              string
	mergedRoot            string
	base                  rootfshead.BaseIdentity
	baseConfig            []byte
	materializedReference rootfshead.HeadReference
	materializedBase      rootfshead.BaseIdentity
	materializedImage     rootfshead.ImageReference
	materializedEnvelope  []byte
	materializedMarker    []byte
}

type fakeRuntime struct {
	info ctldapi.RootFSInfo
}

type fakeCaptureLeases struct {
	ensureCalls     atomic.Int64
	beginCalls      atomic.Int64
	checkpointCalls atomic.Int64
	resetCalls      atomic.Int64
	releaseCalls    atomic.Int64
}

type fakePortalBackingAttacher struct {
	podUID     string
	mergedRoot string
	err        error
}

func (a *fakePortalBackingAttacher) AttachRootFSBackings(_ context.Context, podUID, mergedRoot string) error {
	a.podUID = podUID
	a.mergedRoot = mergedRoot
	return a.err
}

type failingRootFSStore struct {
	objectstore.Store
	failMarkers bool
	failHeads   atomic.Bool
}

func (s *failingRootFSStore) Head(key string) (objectstore.Info, error) {
	if s.failHeads.Load() {
		return objectstore.Info{}, errors.New("injected object store head failure")
	}
	return s.Store.Head(key)
}

func (s *failingRootFSStore) Put(key string, reader io.Reader) error {
	if s.failMarkers && strings.Contains(key, "/markers/") {
		return errors.New("injected marker upload failure")
	}
	return s.Store.Put(key, reader)
}

func (l *fakeCaptureLeases) EnsureCapture(_ context.Context, _, _ string, _ int64) (string, error) {
	l.ensureCalls.Add(1)
	return "capture-prefix", nil
}

func (l *fakeCaptureLeases) BeginCapture(context.Context, string, string, int64) error {
	l.beginCalls.Add(1)
	return nil
}

func (l *fakeCaptureLeases) CheckpointCapture(context.Context, string, string, int64, []rootfshead.Object) error {
	l.checkpointCalls.Add(1)
	return nil
}

func (l *fakeCaptureLeases) ResetCapture(context.Context, string, string, int64) error {
	l.resetCalls.Add(1)
	return nil
}

func (l *fakeCaptureLeases) ReleaseCapture(context.Context, string, string, int64) error {
	l.releaseCalls.Add(1)
	return nil
}

func rootFSSyncGaugeValue(t *testing.T, registry *prometheus.Registry, name string) float64 {
	t.Helper()
	families, err := registry.Gather()
	require.NoError(t, err)
	for _, family := range families {
		if family.GetName() == name {
			require.Len(t, family.Metric, 1)
			return family.Metric[0].GetGauge().GetValue()
		}
	}
	t.Fatalf("metric %s was not gathered", name)
	return 0
}

func (r *fakeRuntime) Inspect(context.Context, ctldapi.RootFSContainerRef) (ctldapi.RootFSInfo, error) {
	return r.info, nil
}

func rootFSTarget() ctldapi.RootFSContainerRef {
	return ctldapi.RootFSContainerRef{
		Namespace: "sandbox-default", PodName: "sandbox-pod", PodUID: "pod-uid", ContainerName: "sandbox",
	}
}

func rootFSInfo(runtime string) ctldapi.RootFSInfo {
	return ctldapi.RootFSInfo{
		ContainerID: "container-id", ContainerName: "sandbox", PodNamespace: "sandbox-default",
		PodName: "sandbox-pod", PodUID: "pod-uid", Runtime: runtime, RuntimeHandler: "runsc-default",
		Snapshotter: rootfshead.SnapshotterName, SnapshotKey: "active-snapshot",
	}
}

func (r *fakeV3Runtime) ActiveUpperdir(context.Context, ctldapi.RootFSInfo) (string, error) {
	return r.upperdir, nil
}

func (r *fakeV3Runtime) ActiveMergedRoot(context.Context, ctldapi.RootFSInfo, string) (string, error) {
	if r.mergedRoot != "" {
		return r.mergedRoot, nil
	}
	return r.upperdir, nil
}

func (r *fakeV3Runtime) BaseIdentityAndConfig(context.Context, ctldapi.RootFSInfo, *rootfshead.BaseIdentity) (rootfshead.BaseIdentity, []byte, error) {
	return r.base, append([]byte(nil), r.baseConfig...), nil
}

func (r *fakeV3Runtime) EnsureBaseImage(context.Context, string) (rootfshead.BaseIdentity, error) {
	return r.base, nil
}

func (r *fakeV3Runtime) MaterializeRootFSHead(_ context.Context, reference rootfshead.HeadReference, base rootfshead.BaseIdentity, image rootfshead.ImageReference, _ string, envelope, marker []byte) error {
	r.materializedReference = reference
	r.materializedBase = base
	r.materializedImage = image
	r.materializedEnvelope = append([]byte(nil), envelope...)
	r.materializedMarker = append([]byte(nil), marker...)
	return nil
}

func testRootFSBase(t *testing.T) (rootfshead.BaseIdentity, []byte) {
	t.Helper()
	diffID := digest.FromString("base layer")
	base := rootfshead.BaseIdentity{
		ImageReference: "docker.io/library/busybox:1.36",
		ManifestDigest: digest.FromString("base manifest").String(),
		ChainID:        diffID.String(),
		OS:             "linux",
		Architecture:   "amd64",
	}
	config := ocispec.Image{
		Platform: ocispec.Platform{OS: base.OS, Architecture: base.Architecture},
		RootFS:   ocispec.RootFS{Type: "layers", DiffIDs: []digest.Digest{diffID}},
	}
	payload, err := json.Marshal(config)
	require.NoError(t, err)
	return base, payload
}

func lookupV3TestEntry(ctx context.Context, store objectstore.Store, reference rootfshead.HeadReference, directory rootfshead.Entry, name string) (rootfshead.Entry, bool, error) {
	prefix, err := rootfsstore.PrefixFromObject(reference.Manifest)
	if err != nil {
		return rootfshead.Entry{}, false, err
	}
	indexPayload, err := rootfsstore.Read(ctx, store, prefix, *directory.Directory)
	if err != nil {
		return rootfshead.Entry{}, false, err
	}
	index, err := rootfshead.DecodeDirectoryIndex(bytes.NewReader(indexPayload))
	if err != nil {
		return rootfshead.Entry{}, false, err
	}
	bucket := rootfshead.NameBucket(name)
	for _, shardObject := range index.Shards {
		if shardObject.Bucket != bucket {
			continue
		}
		shardPayload, err := rootfsstore.Read(ctx, store, prefix, shardObject.Object)
		if err != nil {
			return rootfshead.Entry{}, false, err
		}
		shard, err := rootfshead.DecodeDirectoryShard(bytes.NewReader(shardPayload))
		if err != nil {
			return rootfshead.Entry{}, false, err
		}
		for _, entry := range shard.Entries {
			if entry.Name == name {
				return entry, true, nil
			}
		}
	}
	return rootfshead.Entry{}, false, nil
}

func readV3TestFile(ctx context.Context, store objectstore.Store, reference rootfshead.HeadReference, entry rootfshead.Entry) ([]byte, error) {
	prefix, err := rootfsstore.PrefixFromObject(reference.Manifest)
	if err != nil {
		return nil, err
	}
	manifestPayload, err := rootfsstore.Read(ctx, store, prefix, *entry.File)
	if err != nil {
		return nil, err
	}
	manifest, err := rootfshead.DecodeFileManifest(bytes.NewReader(manifestPayload))
	if err != nil {
		return nil, err
	}
	payload := make([]byte, manifest.Size)
	for _, extent := range manifest.Extents {
		chunk, err := rootfsstore.Read(ctx, store, prefix, extent.Object)
		if err != nil {
			return nil, err
		}
		copy(payload[extent.Offset:extent.Offset+extent.Length], chunk[extent.ObjectOffset:extent.ObjectOffset+extent.Length])
	}
	return payload, nil
}

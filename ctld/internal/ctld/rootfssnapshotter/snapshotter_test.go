package rootfssnapshotter

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/containerd/containerd/v2/core/metadata"
	"github.com/containerd/containerd/v2/core/snapshots"
	"github.com/containerd/containerd/v2/pkg/namespaces"
	contentlocal "github.com/containerd/containerd/v2/plugins/content/local"
	"github.com/containerd/containerd/v2/plugins/snapshots/overlay"
	"github.com/containerd/errdefs"
	"github.com/opencontainers/go-digest"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshead"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/objectstore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"
)

func TestSnapshotterCommitsOrdinaryLayerWithoutMarker(t *testing.T) {
	ctx := namespaces.WithNamespace(context.Background(), "test")
	delegate, err := overlay.NewSnapshotter(t.TempDir(), overlay.WithUpperdirLabel)
	require.NoError(t, err)
	snapshotter, err := NewSnapshotter(delegate, objectstore.NewMemoryStore(""))
	require.NoError(t, err)
	mounter := &recordingHeadMounter{}
	snapshotter.mount = mounter.mount
	t.Cleanup(func() { _ = snapshotter.Close() })

	reference := digest.FromString("ordinary-chain").String()
	_, err = snapshotter.Prepare(ctx, "k8s.io/1/extract", "")
	require.NoError(t, err)
	name := "k8s.io/2/" + reference
	require.NoError(t, snapshotter.Commit(ctx, name, "k8s.io/1/extract"))

	info, err := delegate.Stat(ctx, name)
	require.NoError(t, err)
	assert.Equal(t, reference, info.Labels[labelSnapshotReference])
	assert.Empty(t, info.Parent)
	assert.Empty(t, mounter.paths)
}

func TestServiceDefersOverlayRemovalUntilCleanup(t *testing.T) {
	ctx := namespaces.WithNamespace(context.Background(), "test")
	service, err := NewService(ServiceConfig{
		Root:       t.TempDir(),
		SocketPath: filepath.Join(t.TempDir(), "snapshotter.sock"),
		Store:      objectstore.NewMemoryStore(""),
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = service.Close() })

	const key = "container-active"
	_, err = service.snapshotter.Prepare(ctx, key, "")
	require.NoError(t, err)
	first, err := service.snapshotter.Stat(ctx, key)
	require.NoError(t, err)
	firstUpper := first.Labels[labelOverlayUpperdir]
	require.NotEmpty(t, firstUpper)
	require.NoError(t, os.WriteFile(filepath.Join(firstUpper, "payload"), bytes.Repeat([]byte("x"), 1<<20), 0o600))
	firstSnapshotDir := filepath.Dir(firstUpper)

	require.NoError(t, service.snapshotter.Remove(ctx, key))
	_, err = service.snapshotter.Stat(ctx, key)
	assert.True(t, errdefs.IsNotFound(err))
	require.DirExists(t, firstSnapshotDir, "Remove must detach metadata without walking a large upper")

	// The public key is reusable before physical cleanup. Cleanup must remove
	// only the detached directory and leave the replacement snapshot intact.
	_, err = service.snapshotter.Prepare(ctx, key, "")
	require.NoError(t, err)
	second, err := service.snapshotter.Stat(ctx, key)
	require.NoError(t, err)
	secondUpper := second.Labels[labelOverlayUpperdir]
	require.NotEqual(t, firstUpper, secondUpper)

	require.NoError(t, service.snapshotter.Cleanup(ctx))
	assert.NoDirExists(t, firstSnapshotDir)
	require.DirExists(t, filepath.Dir(secondUpper))
	require.NoError(t, service.snapshotter.Remove(ctx, key))
	require.NoError(t, service.snapshotter.Cleanup(ctx))
}

func TestSnapshotterCommitsStoredHeadOnCanonicalBase(t *testing.T) {
	ctx := namespaces.WithNamespace(context.Background(), "test")
	delegate, err := overlay.NewSnapshotter(t.TempDir(), overlay.WithUpperdirLabel)
	require.NoError(t, err)
	baseReference := digest.FromString("base-chain").String()
	physicalBase := commitOpaqueBackendSnapshot(t, ctx, delegate, 2, baseReference)
	store := objectstore.NewMemoryStore("")
	markerObject, annotation := putStoredHead(t, store, baseReference)

	snapshotter, err := NewSnapshotter(delegate, store)
	require.NoError(t, err)
	mounter := &recordingHeadMounter{}
	snapshotter.mount = mounter.mount
	t.Cleanup(func() { _ = snapshotter.Close() })

	active := "k8s.io/3/extract-head"
	_, err = snapshotter.Prepare(ctx, active, "")
	require.NoError(t, err)
	physicalHead := "k8s.io/4/" + markerObject.Digest
	require.NoError(t, snapshotter.Commit(ctx, physicalHead, active))

	info, err := delegate.Stat(ctx, physicalHead)
	require.NoError(t, err)
	assert.Equal(t, physicalBase, info.Parent)
	assert.Equal(t, markerObject.Digest, info.Labels[labelSnapshotReference])
	assert.Equal(t, baseReference, info.Labels[labelHeadBaseReference])
	assert.Equal(t, annotation, info.Labels[rootfshead.AnnotationHead])
	assert.Empty(t, mounter.paths, "an unused cached head must remain unmounted")
	require.NoError(t, snapshotter.Recover(ctx))
	assert.Empty(t, mounter.paths, "recovery must not mount an unused cached head")

	firstChild := "k8s.io/5/container-active"
	mounts, err := snapshotter.Prepare(ctx, firstChild, physicalHead)
	require.NoError(t, err)
	assert.NotEmpty(t, mounts)
	secondChild := "k8s.io/6/container-active"
	_, err = snapshotter.View(ctx, secondChild, physicalHead)
	require.NoError(t, err)
	require.Len(t, mounter.paths, 1)
	assert.NotEmpty(t, mounter.paths[0])
	require.NoError(t, snapshotter.Remove(ctx, firstChild))
	assert.False(t, mounter.mounts[0].unmounted, "the head must remain mounted for another child")

	mounter.mounts[0].onUnmount = func() {
		_, statErr := delegate.Stat(ctx, physicalHead)
		assert.NoError(t, statErr, "cached head must outlive its container child")
		_, statErr = delegate.Stat(ctx, secondChild)
		assert.True(t, errdefs.IsNotFound(statErr), "child must be removed before releasing its head mount")
	}
	require.NoError(t, snapshotter.Remove(ctx, secondChild))
	assert.True(t, mounter.mounts[0].unmounted)
	require.NoError(t, snapshotter.Remove(ctx, physicalHead))
	_, err = delegate.Stat(ctx, physicalHead)
	assert.True(t, errdefs.IsNotFound(err))
}

func TestSnapshotterRebasesHeadWithInheritedDescriptorAnnotation(t *testing.T) {
	ctx := namespaces.WithNamespace(context.Background(), "test")
	delegate, err := overlay.NewSnapshotter(t.TempDir(), overlay.WithUpperdirLabel)
	require.NoError(t, err)
	baseReference := digest.FromString("base-chain").String()
	physicalBase := commitOpaqueBackendSnapshot(t, ctx, delegate, 2, baseReference)
	store := objectstore.NewMemoryStore("")
	markerObject, annotation := putStoredHead(t, store, baseReference)

	snapshotter, err := NewSnapshotter(delegate, store)
	require.NoError(t, err)
	t.Cleanup(func() { _ = snapshotter.Close() })

	active := "k8s.io/3/extract-head"
	_, err = snapshotter.Prepare(ctx, active, "", snapshots.WithLabels(map[string]string{
		rootfshead.AnnotationHead: annotation,
	}))
	require.NoError(t, err)
	physicalHead := "k8s.io/4/" + markerObject.Digest
	require.NoError(t, snapshotter.Commit(ctx, physicalHead, active))

	info, err := delegate.Stat(ctx, physicalHead)
	require.NoError(t, err)
	assert.Equal(t, physicalBase, info.Parent)
	assert.Equal(t, baseReference, info.Labels[labelHeadBaseReference])
	assert.Equal(t, annotation, info.Labels[rootfshead.AnnotationHead])
}

func TestSnapshotterAttachesDistinctHeadsConcurrently(t *testing.T) {
	ctx := namespaces.WithNamespace(context.Background(), "test")
	delegate, err := overlay.NewSnapshotter(t.TempDir(), overlay.WithUpperdirLabel)
	require.NoError(t, err)
	baseReference := digest.FromString("base-chain").String()
	commitOpaqueBackendSnapshot(t, ctx, delegate, 2, baseReference)
	store := objectstore.NewMemoryStore("")
	firstMarker, _ := putStoredHeadWithID(t, store, baseReference, "first-head")
	secondMarker, _ := putStoredHeadWithID(t, store, baseReference, "second-head")

	snapshotter, err := NewSnapshotter(delegate, store)
	require.NoError(t, err)
	mounter := newConcurrentHeadMounter()
	snapshotter.mount = mounter.mount
	t.Cleanup(func() {
		mounter.releaseAll()
		_ = snapshotter.Close()
	})

	firstHead := "k8s.io/4/" + firstMarker.Digest
	secondHead := "k8s.io/5/" + secondMarker.Digest
	for active, head := range map[string]string{
		"k8s.io/active/first":  firstHead,
		"k8s.io/active/second": secondHead,
	} {
		_, err = snapshotter.Prepare(ctx, active, "")
		require.NoError(t, err)
		require.NoError(t, snapshotter.Commit(ctx, head, active))
	}

	results := make(chan error, 2)
	go func() {
		_, prepareErr := snapshotter.Prepare(ctx, "k8s.io/child/first", firstHead)
		results <- prepareErr
	}()
	go func() {
		_, prepareErr := snapshotter.Prepare(ctx, "k8s.io/child/second", secondHead)
		results <- prepareErr
	}()

	deadline := time.NewTimer(2 * time.Second)
	defer deadline.Stop()
	for attached := 0; attached < 2; attached++ {
		select {
		case <-mounter.started:
		case <-deadline.C:
			mounter.releaseAll()
			for completed := 0; completed < 2; completed++ {
				<-results
			}
			t.Fatal("distinct rootfs heads were serialized behind one snapshotter lock")
		}
	}
	mounter.releaseAll()
	for completed := 0; completed < 2; completed++ {
		require.NoError(t, <-results)
	}
	assert.Equal(t, 2, mounter.maxConcurrent())
}

func TestSnapshotterStoredMarkerDoesNotDependOnObjectHeadSize(t *testing.T) {
	ctx := namespaces.WithNamespace(context.Background(), "test")
	delegate, err := overlay.NewSnapshotter(t.TempDir(), overlay.WithUpperdirLabel)
	require.NoError(t, err)
	baseReference := digest.FromString("base-chain").String()
	commitOpaqueBackendSnapshot(t, ctx, delegate, 2, baseReference)
	underlying := objectstore.NewMemoryStore("")
	markerObject, _ := putStoredHead(t, underlying, baseReference)
	store := &headRejectingStore{Store: underlying}
	snapshotter, err := NewSnapshotter(delegate, store)
	require.NoError(t, err)
	snapshotter.mount = (&recordingHeadMounter{}).mount
	t.Cleanup(func() { _ = snapshotter.Close() })

	_, err = snapshotter.Prepare(ctx, "k8s.io/3/extract", "")
	require.NoError(t, err)
	require.NoError(t, snapshotter.Commit(ctx, "k8s.io/4/"+markerObject.Digest, "k8s.io/3/extract"))
	assert.Zero(t, store.headCalls, "marker discovery must use bounded plaintext reads")
}

func TestSnapshotterRejectsHeadWithoutLocalBaseSnapshot(t *testing.T) {
	ctx := namespaces.WithNamespace(context.Background(), "test")
	delegate, err := overlay.NewSnapshotter(t.TempDir(), overlay.WithUpperdirLabel)
	require.NoError(t, err)
	store := objectstore.NewMemoryStore("")
	markerObject, _ := putStoredHead(t, store, digest.FromString("missing-base").String())
	snapshotter, err := NewSnapshotter(delegate, store)
	require.NoError(t, err)
	mounter := &recordingHeadMounter{}
	snapshotter.mount = mounter.mount
	t.Cleanup(func() { _ = snapshotter.Close() })

	_, err = snapshotter.Prepare(ctx, "k8s.io/1/extract", "")
	require.NoError(t, err)
	err = snapshotter.Commit(ctx, "k8s.io/2/"+markerObject.Digest, "k8s.io/1/extract")

	require.Error(t, err)
	assert.ErrorContains(t, err, "resolve rootfs head base snapshot")
	assert.True(t, errdefs.IsNotFound(err))
	assert.Empty(t, mounter.paths)
}

func TestSnapshotterRecoverMountsPersistedHeads(t *testing.T) {
	ctx := namespaces.WithNamespace(context.Background(), "test")
	delegate, err := overlay.NewSnapshotter(t.TempDir(), overlay.WithUpperdirLabel)
	require.NoError(t, err)
	baseReference := digest.FromString("base-chain").String()
	commitOpaqueBackendSnapshot(t, ctx, delegate, 2, baseReference)
	store := objectstore.NewMemoryStore("")
	markerObject, _ := putStoredHead(t, store, baseReference)
	snapshotter, err := NewSnapshotter(delegate, store)
	require.NoError(t, err)
	initial := &recordingHeadMounter{}
	snapshotter.mount = initial.mount
	t.Cleanup(func() { _ = snapshotter.Close() })

	_, err = snapshotter.Prepare(ctx, "k8s.io/3/extract", "")
	require.NoError(t, err)
	physicalHead := "k8s.io/4/" + markerObject.Digest
	require.NoError(t, snapshotter.Commit(ctx, physicalHead, "k8s.io/3/extract"))
	_, err = snapshotter.Prepare(ctx, "k8s.io/5/container-active", physicalHead)
	require.NoError(t, err)
	require.NoError(t, initial.mounts[0].Unmount())
	snapshotter.mu.Lock()
	snapshotter.mounted = make(map[string]mountedHead)
	snapshotter.children = make(map[string]string)
	snapshotter.mu.Unlock()
	recovered := &recordingHeadMounter{}
	snapshotter.mount = recovered.mount

	require.NoError(t, snapshotter.Recover(ctx))
	require.Len(t, recovered.paths, 1)
	assert.NotEmpty(t, recovered.paths[0])
}

func TestSnapshotterRecoverAllowsFreshDelegate(t *testing.T) {
	ctx := namespaces.WithNamespace(context.Background(), "test")
	delegate, err := overlay.NewSnapshotter(t.TempDir(), overlay.WithUpperdirLabel)
	require.NoError(t, err)
	snapshotter, err := NewSnapshotter(delegate, objectstore.NewMemoryStore(""))
	require.NoError(t, err)
	t.Cleanup(func() { _ = snapshotter.Close() })

	require.NoError(t, snapshotter.Recover(ctx))
}

func TestSnapshotterWorksBehindContainerdMetadataWrapperWithoutPrepareLabels(t *testing.T) {
	ctx := namespaces.WithNamespace(context.Background(), "k8s.io")
	delegate, err := overlay.NewSnapshotter(t.TempDir(), overlay.WithUpperdirLabel)
	require.NoError(t, err)
	store := objectstore.NewMemoryStore("")
	snapshotter, err := NewSnapshotter(delegate, store)
	require.NoError(t, err)
	mounter := &recordingHeadMounter{}
	snapshotter.mount = mounter.mount
	t.Cleanup(func() { _ = snapshotter.Close() })

	contentStore, err := contentlocal.NewStore(t.TempDir())
	require.NoError(t, err)
	database, err := bolt.Open(filepath.Join(t.TempDir(), "metadata.db"), 0o600, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = database.Close() })
	metadataDB := metadata.NewDB(database, contentStore, map[string]snapshots.Snapshotter{
		rootfshead.SnapshotterName: snapshotter,
	})
	require.NoError(t, metadataDB.Init(ctx))
	wrapped := metadataDB.Snapshotter(rootfshead.SnapshotterName)

	baseReference := digest.FromString("base-chain").String()
	_, err = wrapped.Prepare(ctx, "extract-base", "")
	require.NoError(t, err)
	require.NoError(t, wrapped.Commit(ctx, baseReference, "extract-base"))
	baseInfo, err := snapshotter.resolveBackendSnapshot(ctx, baseReference)
	require.NoError(t, err)
	assert.NotEqual(t, baseReference, baseInfo.Name, "metadata wrapper must use an opaque backend key")
	assert.Equal(t, baseReference, backendSnapshotReference(baseInfo.Name))

	markerObject, annotation := putStoredHead(t, store, baseReference)
	_, err = wrapped.Prepare(ctx, "extract-head", "")
	require.NoError(t, err)
	require.NoError(t, wrapped.Commit(ctx, markerObject.Digest, "extract-head"))

	wrappedHead, err := wrapped.Stat(ctx, markerObject.Digest)
	require.NoError(t, err)
	assert.Empty(t, wrappedHead.Parent, "the public metadata graph must remain marker-only")
	physicalHead, err := snapshotter.resolveBackendSnapshot(ctx, markerObject.Digest)
	require.NoError(t, err)
	assert.Equal(t, baseInfo.Name, physicalHead.Parent)
	assert.Equal(t, annotation, physicalHead.Labels[rootfshead.AnnotationHead])
	assert.Empty(t, mounter.paths, "metadata-only unpack must not pin an unused FUSE mount")

	mounts, err := wrapped.Prepare(ctx, "container-active", markerObject.Digest)
	require.NoError(t, err)
	assert.NotEmpty(t, mounts)
	require.Len(t, mounter.paths, 1)
}

func putStoredHead(t *testing.T, store objectstore.Store, baseReference string) (rootfshead.Object, string) {
	t.Helper()
	return putStoredHeadWithID(t, store, baseReference, "head-layer")
}

func putStoredHeadWithID(t *testing.T, store objectstore.Store, baseReference, headID string) (rootfshead.Object, string) {
	t.Helper()
	_, reference := sealUpperHead(t, store, headID, baseReference, func(root string) {
		require.NoError(t, os.MkdirAll(filepath.Join(root, "workspace"), 0o755))
		require.NoError(t, os.WriteFile(filepath.Join(root, "workspace", "file"), []byte(headID), 0o644))
	})
	annotation, err := rootfshead.EncodeHeadAnnotation(reference)
	require.NoError(t, err)
	markerObject, markerPayload, err := rootfshead.MarkerObject(reference)
	require.NoError(t, err)
	require.NoError(t, store.Put(markerObject.Key, bytes.NewReader(markerPayload)))
	return markerObject, annotation
}

func commitOpaqueBackendSnapshot(
	t *testing.T,
	ctx context.Context,
	delegate snapshots.Snapshotter,
	id int,
	reference string,
) string {
	t.Helper()
	active := "k8s.io/active/" + reference
	_, err := delegate.Prepare(ctx, active, "")
	require.NoError(t, err)
	name := "k8s.io/" + strconv.Itoa(id) + "/" + reference
	require.NoError(t, delegate.Commit(ctx, name, active))
	return name
}

type headRejectingStore struct {
	objectstore.Store
	headCalls int
}

func (s *headRejectingStore) Head(string) (objectstore.Info, error) {
	s.headCalls++
	return objectstore.Info{}, errors.New("Head must not be used for marker discovery")
}

type recordingHeadMounter struct {
	paths  []string
	mounts []*recordingHeadMount
}

type concurrentHeadMounter struct {
	mu          sync.Mutex
	active      int
	maximum     int
	started     chan struct{}
	release     chan struct{}
	releaseOnce sync.Once
}

func newConcurrentHeadMounter() *concurrentHeadMounter {
	return &concurrentHeadMounter{
		started: make(chan struct{}, 2),
		release: make(chan struct{}),
	}
}

func (m *concurrentHeadMounter) mount(_ string, tree *LayerTree) (mountedHead, error) {
	if tree == nil {
		return nil, assert.AnError
	}
	m.mu.Lock()
	m.active++
	if m.active > m.maximum {
		m.maximum = m.active
	}
	m.mu.Unlock()
	m.started <- struct{}{}
	<-m.release
	m.mu.Lock()
	m.active--
	m.mu.Unlock()
	return &recordingHeadMount{}, nil
}

func (m *concurrentHeadMounter) releaseAll() {
	m.releaseOnce.Do(func() { close(m.release) })
}

func (m *concurrentHeadMounter) maxConcurrent() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.maximum
}

func (m *recordingHeadMounter) mount(path string, tree *LayerTree) (mountedHead, error) {
	if tree == nil {
		return nil, assert.AnError
	}
	mount := &recordingHeadMount{}
	m.paths = append(m.paths, path)
	m.mounts = append(m.mounts, mount)
	return mount, nil
}

type recordingHeadMount struct {
	unmounted bool
	healthErr error
	onUnmount func()
}

func (m *recordingHeadMount) Unmount() error {
	if m.onUnmount != nil {
		m.onUnmount()
	}
	m.unmounted = true
	return nil
}

func (m *recordingHeadMount) HealthError() error {
	return m.healthErr
}

func TestSnapshotterHealthReportsMountedHeadFailure(t *testing.T) {
	delegate, err := overlay.NewSnapshotter(t.TempDir(), overlay.WithUpperdirLabel)
	require.NoError(t, err)
	snapshotter, err := NewSnapshotter(delegate, objectstore.NewMemoryStore(""))
	require.NoError(t, err)
	t.Cleanup(func() { _ = snapshotter.Close() })

	mount := &recordingHeadMount{healthErr: assert.AnError}
	snapshotter.mounted["broken-head"] = mount

	require.ErrorIs(t, snapshotter.HealthError(), assert.AnError)
}

func TestSnapshotterPreservesWhiteoutMetadataInMountedTree(t *testing.T) {
	entry := newTreeEntry(rootfshead.Entry{
		Name: "deleted", Inode: "whiteout", Kind: rootfshead.EntryWhiteout, Mode: 0o600,
	}, false)
	assert.Equal(t, rootfshead.EntryWhiteout, entry.entry.Kind)
	assert.Equal(t, uint32(0), entry.entry.Rdev)
	assert.Equal(t, uint32(0o600), entry.mode&0o7777)
}

func TestBackendSnapshotReference(t *testing.T) {
	reference := digest.FromString("chain").String()
	assert.Equal(t, reference, backendSnapshotReference("k8s.io/42/"+reference))
	assert.Empty(t, backendSnapshotReference("k8s.io/42/not-a-digest"))
	assert.Empty(t, backendSnapshotReference(""))
}

func TestProbeMountedTreeAcceptsPopulatedAndEmptyDirectories(t *testing.T) {
	populated := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(populated, "entry"), []byte("value"), 0o600))
	require.NoError(t, probeMountedTree(context.Background(), populated))
	require.NoError(t, probeMountedTree(context.Background(), t.TempDir()))
}

func TestProbeMountedTreeReportsOpenFailure(t *testing.T) {
	err := probeMountedTree(context.Background(), filepath.Join(t.TempDir(), "missing"))
	require.Error(t, err)
}

func TestSnapshotterCloseIsIdempotent(t *testing.T) {
	delegate, err := overlay.NewSnapshotter(t.TempDir(), overlay.WithUpperdirLabel)
	require.NoError(t, err)
	snapshotter, err := NewSnapshotter(delegate, objectstore.NewMemoryStore(""))
	require.NoError(t, err)
	assert.NoError(t, snapshotter.Close())
	assert.NoError(t, snapshotter.Close())
	assert.Error(t, snapshotter.HealthError())
}

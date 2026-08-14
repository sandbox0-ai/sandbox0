package rootfssnapshotter

import (
	"context"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/containerd/containerd/v2/core/snapshots"
	"github.com/containerd/containerd/v2/plugins/snapshots/overlay"
	"github.com/containerd/errdefs"
	"github.com/opencontainers/go-digest"
	"github.com/sandbox0-ai/sandbox0/ctld/internal/ctld/rootfsreader"
	"github.com/sandbox0-ai/sandbox0/ctld/internal/ctld/rootfsstore"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshead"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/objectstore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

func TestMarkerCommitPinsDurableBaseAndMountsLazily(t *testing.T) {
	fixture := newSnapshotterFixture(t)
	fixture.commitHead(t)
	info, err := fixture.delegate.Stat(context.Background(), fixture.headChain)
	require.NoError(t, err)
	assert.Equal(t, fixture.baseChain, info.Parent)
	assert.NotEmpty(t, info.Labels[rootfshead.AnnotationHead])
	assert.Equal(t, fixture.baseChain, info.Labels[rootfshead.LabelBaseChainID])
	assert.Equal(t, 0, fixture.mounts.count())

	_, err = fixture.snapshotter.Prepare(context.Background(), "sandbox-child", fixture.headChain)
	require.NoError(t, err)
	assert.Equal(t, 1, fixture.mounts.count())
	_, err = fixture.snapshotter.Mounts(context.Background(), "sandbox-child")
	require.NoError(t, err)

	require.NoError(t, fixture.snapshotter.Remove(context.Background(), "sandbox-child"))
	assert.Equal(t, 1, fixture.mounts.unmountCount())
}

func TestMarkerCommitRejectsMissingDurableBaseLabel(t *testing.T) {
	fixture := newSnapshotterFixture(t)
	_, err := fixture.snapshotter.Prepare(context.Background(), "prepare-head", fixture.baseChain, snapshots.WithLabels(map[string]string{
		rootfshead.AnnotationHead: fixture.annotation,
	}))
	require.NoError(t, err)

	err = fixture.snapshotter.Commit(context.Background(), fixture.headChain, "prepare-head")
	assert.ErrorContains(t, err, "declares base")
}

func TestCommittedHeadWithoutDurableBaseLabelCannotMount(t *testing.T) {
	fixture := newSnapshotterFixture(t)
	_, err := fixture.delegate.Prepare(context.Background(), "prepare-broken-head", fixture.baseChain, snapshots.WithLabels(map[string]string{
		rootfshead.AnnotationHead: fixture.annotation,
	}))
	require.NoError(t, err)
	require.NoError(t, fixture.delegate.Commit(context.Background(), "broken-head", "prepare-broken-head", snapshots.WithLabels(map[string]string{
		rootfshead.AnnotationHead: fixture.annotation,
	})))

	_, err = fixture.snapshotter.Prepare(context.Background(), "sandbox-child", "broken-head")
	assert.ErrorContains(t, err, "conflicts with snapshot label")
}

func TestOrdinarySnapshotsPassThroughWithoutFUSE(t *testing.T) {
	fixture := newSnapshotterFixture(t)
	_, err := fixture.snapshotter.Prepare(context.Background(), "ordinary", fixture.baseChain)
	require.NoError(t, err)
	assert.Equal(t, 0, fixture.mounts.count())
	require.NoError(t, fixture.snapshotter.Remove(context.Background(), "ordinary"))
}

func TestCloseWaitsForOrdinaryDelegateOperation(t *testing.T) {
	delegate, err := overlay.NewSnapshotter(t.TempDir(), overlay.WithUpperdirLabel)
	require.NoError(t, err)
	_, err = delegate.Prepare(context.Background(), "prepare-base", "")
	require.NoError(t, err)
	require.NoError(t, delegate.Commit(context.Background(), "base", "prepare-base"))
	started := make(chan struct{})
	release := make(chan struct{})
	blocking := &blockingSnapshotter{Snapshotter: delegate, statStarted: started, releaseStat: release}
	snapshotter, err := New(Config{Delegate: blocking, Store: objectstore.NewMemoryStore(t.Name())})
	require.NoError(t, err)

	statDone := make(chan error, 1)
	go func() {
		_, statErr := snapshotter.Stat(context.Background(), "base")
		statDone <- statErr
	}()
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("ordinary delegate Stat did not start")
	}
	closeDone := make(chan error, 1)
	go func() { closeDone <- snapshotter.Close() }()
	select {
	case err := <-closeDone:
		t.Fatalf("Close returned while ordinary delegate operation was active: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	require.NoError(t, <-statDone)
	require.NoError(t, <-closeDone)
}

func TestUpdatePreservesReservedHeadLabelsAndRejectsDirectMutation(t *testing.T) {
	fixture := newSnapshotterFixture(t)
	fixture.commitHead(t)
	updated, err := fixture.snapshotter.Update(context.Background(), snapshots.Info{
		Name: fixture.headChain,
		Labels: map[string]string{
			"example.test/user": "kept",
		},
	})
	require.NoError(t, err)
	assert.Equal(t, "kept", updated.Labels["example.test/user"])
	assert.Equal(t, fixture.annotation, updated.Labels[rootfshead.AnnotationHead])
	assert.Equal(t, fixture.baseChain, updated.Labels[rootfshead.LabelBaseChainID])

	_, err = fixture.snapshotter.Update(context.Background(), snapshots.Info{
		Name: fixture.headChain,
		Labels: map[string]string{
			rootfshead.AnnotationHead: "corrupt",
		},
	}, "labels."+rootfshead.AnnotationHead)
	assert.Error(t, err)
	assert.True(t, errdefs.IsInvalidArgument(err))
}

func TestUpdateCannotTurnOrdinarySnapshotIntoRootFSHead(t *testing.T) {
	fixture := newSnapshotterFixture(t)
	before, err := fixture.snapshotter.Stat(context.Background(), fixture.baseChain)
	require.NoError(t, err)
	require.NotEmpty(t, before.Labels[labelOverlayUpperdir])
	updated, err := fixture.snapshotter.Update(context.Background(), snapshots.Info{
		Name: fixture.baseChain,
		Labels: map[string]string{
			rootfshead.AnnotationHead:   fixture.annotation,
			rootfshead.LabelBaseChainID: fixture.baseChain,
		},
	})
	require.NoError(t, err)
	assert.Empty(t, updated.Labels[rootfshead.AnnotationHead])
	assert.Empty(t, updated.Labels[rootfshead.LabelBaseChainID])
	assert.Equal(t, before.Labels[labelOverlayUpperdir], updated.Labels[labelOverlayUpperdir])
}

func TestCommittedHeadChildRetainsMountReferenceUnderNewName(t *testing.T) {
	fixture := newSnapshotterFixture(t)
	fixture.commitHead(t)
	_, err := fixture.snapshotter.Prepare(context.Background(), "active-child", fixture.headChain)
	require.NoError(t, err)
	require.NoError(t, fixture.snapshotter.Commit(context.Background(), "committed-child", "active-child"))
	fixture.snapshotter.mu.Lock()
	assert.Empty(t, fixture.snapshotter.children["active-child"])
	assert.Equal(t, fixture.headChain, fixture.snapshotter.children["committed-child"])
	fixture.snapshotter.mu.Unlock()
	require.NoError(t, fixture.snapshotter.Remove(context.Background(), "committed-child"))
	assert.Equal(t, 1, fixture.mounts.unmountCount())
}

func TestHeadCannotBeRemovedWithLiveChild(t *testing.T) {
	fixture := newSnapshotterFixture(t)
	fixture.commitHead(t)
	_, err := fixture.snapshotter.Prepare(context.Background(), "sandbox-child", fixture.headChain)
	require.NoError(t, err)
	err = fixture.snapshotter.Remove(context.Background(), fixture.headChain)
	assert.Error(t, err)
	assert.True(t, errdefs.IsFailedPrecondition(err))
}

func TestRemovingLastChildSerializesWithNewChildMount(t *testing.T) {
	fixture := newSnapshotterFixture(t)
	fixture.commitHead(t)
	_, err := fixture.snapshotter.Prepare(context.Background(), "sandbox-child-1", fixture.headChain)
	require.NoError(t, err)
	unmountStarted := make(chan struct{})
	allowUnmount := make(chan struct{})
	fixture.mounts.mu.Lock()
	fixture.mounts.unmountStarted = unmountStarted
	fixture.mounts.allowUnmount = allowUnmount
	fixture.mounts.mu.Unlock()

	removeDone := make(chan error, 1)
	go func() { removeDone <- fixture.snapshotter.Remove(context.Background(), "sandbox-child-1") }()
	select {
	case <-unmountStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("last-child unmount did not start")
	}
	prepareDone := make(chan error, 1)
	go func() {
		_, prepareErr := fixture.snapshotter.Prepare(context.Background(), "sandbox-child-2", fixture.headChain)
		prepareDone <- prepareErr
	}()
	select {
	case err := <-prepareDone:
		t.Fatalf("new child passed the Head lock while the old lower was unmounting: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	close(allowUnmount)
	require.NoError(t, <-removeDone)
	require.NoError(t, <-prepareDone)
	assert.Equal(t, 2, fixture.mounts.count())
}

func TestRemovingActiveMarkerSerializesWithCommit(t *testing.T) {
	fixture := newSnapshotterFixture(t)
	_, err := fixture.snapshotter.Prepare(context.Background(), "prepare-head", fixture.baseChain, snapshots.WithLabels(map[string]string{
		rootfshead.AnnotationHead:   fixture.annotation,
		rootfshead.LabelBaseChainID: fixture.baseChain,
		labelSnapshotReference:      fixture.headChain,
	}))
	require.NoError(t, err)
	commitStarted := make(chan struct{})
	releaseCommit := make(chan struct{})
	fixture.snapshotter.delegate = &blockingCommitSnapshotter{
		Snapshotter: fixture.delegate,
		started:     commitStarted,
		release:     releaseCommit,
	}

	commitDone := make(chan error, 1)
	go func() {
		commitDone <- fixture.snapshotter.Commit(context.Background(), fixture.headChain, "prepare-head")
	}()
	select {
	case <-commitStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("marker commit did not reach delegate commit")
	}
	removeDone := make(chan error, 1)
	go func() { removeDone <- fixture.snapshotter.Remove(context.Background(), "prepare-head") }()
	select {
	case err := <-removeDone:
		t.Fatalf("Remove passed the active marker lock during commit: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	close(releaseCommit)
	require.NoError(t, <-commitDone)
	assert.Error(t, <-removeDone)
	_, err = fixture.snapshotter.Stat(context.Background(), fixture.headChain)
	require.NoError(t, err)
}

func TestRecoverRemountsOnlyHeadsWithChildren(t *testing.T) {
	fixture := newSnapshotterFixture(t)
	fixture.commitHead(t)
	_, err := fixture.snapshotter.Prepare(context.Background(), "sandbox-child", fixture.headChain)
	require.NoError(t, err)
	fixture.snapshotter.mu.Lock()
	clear(fixture.snapshotter.mounted)
	clear(fixture.snapshotter.children)
	fixture.snapshotter.mu.Unlock()
	fixture.mounts.reset()

	require.NoError(t, fixture.snapshotter.Recover(context.Background()))
	assert.Equal(t, 1, fixture.mounts.count())
	fixture.snapshotter.mu.Lock()
	assert.Equal(t, fixture.headChain, fixture.snapshotter.children["sandbox-child"])
	fixture.snapshotter.mu.Unlock()
}

func TestPrepareFailsWhenHeadObjectIsCorrupt(t *testing.T) {
	fixture := newSnapshotterFixture(t)
	fixture.commitHead(t)
	require.NoError(t, fixture.store.Put(fixture.reference.Manifest.Key, stringReader("corrupt")))
	metadata := rootfsreaderCacheDisabled(fixture.snapshotter)
	fixture.snapshotter.metadata = metadata
	_, err := fixture.snapshotter.Prepare(context.Background(), "sandbox-child", fixture.headChain)
	assert.ErrorContains(t, err, "failed size or digest validation")
}

func TestFuseMountUnmountLazilyDetachesWithoutWaitingForServer(t *testing.T) {
	done := make(chan struct{})
	called := false
	mounted := &fuseMount{
		target: "/rootfs/head",
		done:   done,
		detach: func(target string, flags int) error {
			called = true
			assert.Equal(t, "/rootfs/head", target)
			assert.Equal(t, unix.MNT_DETACH, flags)
			return nil
		},
	}

	require.NoError(t, mounted.Unmount())
	assert.True(t, called)
	select {
	case <-done:
		t.Fatal("lazy detach must not wait for or close the FUSE server loop")
	default:
	}
}

func TestFuseMountUnmountToleratesAlreadyDetachedTarget(t *testing.T) {
	for _, detachErr := range []error{syscall.EINVAL, syscall.ENOENT} {
		mounted := &fuseMount{
			target: "/rootfs/head",
			detach: func(string, int) error { return detachErr },
		}
		require.NoError(t, mounted.Unmount())
	}
}

func TestFuseMountUnmountReturnsDetachFailure(t *testing.T) {
	mounted := &fuseMount{
		target: "/rootfs/head",
		detach: func(string, int) error { return syscall.EPERM },
	}

	assert.ErrorContains(t, mounted.Unmount(), "detach rootfs FUSE Head")
}

type snapshotterFixture struct {
	delegate    snapshots.Snapshotter
	snapshotter *Snapshotter
	store       objectstore.Store
	mounts      *fakeMounts
	reference   rootfshead.HeadReference
	annotation  string
	baseChain   string
	headChain   string
}

type blockingSnapshotter struct {
	snapshots.Snapshotter
	statStarted chan struct{}
	releaseStat chan struct{}
	once        sync.Once
}

type blockingCommitSnapshotter struct {
	snapshots.Snapshotter
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (s *blockingCommitSnapshotter) Commit(ctx context.Context, name, key string, opts ...snapshots.Opt) error {
	s.once.Do(func() { close(s.started) })
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-s.release:
		return s.Snapshotter.Commit(ctx, name, key, opts...)
	}
}

func (s *blockingSnapshotter) Stat(ctx context.Context, key string) (snapshots.Info, error) {
	s.once.Do(func() { close(s.statStarted) })
	select {
	case <-ctx.Done():
		return snapshots.Info{}, ctx.Err()
	case <-s.releaseStat:
		return s.Snapshotter.Stat(ctx, key)
	}
}

func newSnapshotterFixture(t *testing.T) *snapshotterFixture {
	t.Helper()
	delegate, err := overlay.NewSnapshotter(t.TempDir(), overlay.WithUpperdirLabel)
	require.NoError(t, err)
	baseChain := digest.FromString("base-chain").String()
	_, err = delegate.Prepare(context.Background(), "prepare-base", "")
	require.NoError(t, err)
	require.NoError(t, delegate.Commit(context.Background(), baseChain, "prepare-base"))
	store := objectstore.NewMemoryStore(t.Name() + "-" + digest.FromString(t.TempDir()).Encoded())
	writer, err := rootfsstore.NewTeamWriter(store, "snapshotter-team")
	require.NoError(t, err)
	indexPayload, err := rootfshead.EncodeDirectoryIndex(rootfshead.DirectoryIndex{Version: rootfshead.Version})
	require.NoError(t, err)
	indexObject, err := writer.Put(context.Background(), rootfshead.DirectoryIndexMediaType, indexPayload)
	require.NoError(t, err)
	head := rootfshead.Head{
		Version: rootfshead.Version,
		HeadID:  "head-snapshotter",
		Base: rootfshead.BaseIdentity{
			ImageReference: "registry.example/base@" + digest.FromString("manifest").String(),
			ManifestDigest: digest.FromString("manifest").String(),
			ChainID:        baseChain,
			OS:             "linux",
			Architecture:   "amd64",
		},
		Root: rootfshead.Entry{
			Inode:     "root",
			Kind:      rootfshead.EntryDirectory,
			Mode:      0o040755,
			Nlink:     2,
			Directory: &indexObject,
		},
	}
	headPayload, err := rootfshead.EncodeHead(head)
	require.NoError(t, err)
	headObject, err := writer.Put(context.Background(), rootfshead.HeadMediaType, headPayload)
	require.NoError(t, err)
	reference := rootfshead.HeadReference{Version: rootfshead.Version, HeadID: head.HeadID, Manifest: headObject}
	annotation, err := rootfshead.EncodeHeadAnnotation(reference)
	require.NoError(t, err)
	snapshotter, err := New(Config{Delegate: delegate, Store: store, MetadataCacheBytes: -1})
	require.NoError(t, err)
	mounts := &fakeMounts{}
	snapshotter.mount = mounts.mount
	t.Cleanup(func() { require.NoError(t, snapshotter.Close()) })
	return &snapshotterFixture{
		delegate:    delegate,
		snapshotter: snapshotter,
		store:       store,
		mounts:      mounts,
		reference:   reference,
		annotation:  annotation,
		baseChain:   baseChain,
		headChain:   digest.FromString("head-chain").String(),
	}
}

func (f *snapshotterFixture) commitHead(t *testing.T) {
	t.Helper()
	_, err := f.snapshotter.Prepare(context.Background(), "prepare-head", f.baseChain, snapshots.WithLabels(map[string]string{
		rootfshead.AnnotationHead:   f.annotation,
		rootfshead.LabelBaseChainID: f.baseChain,
		labelSnapshotReference:      f.headChain,
	}))
	require.NoError(t, err)
	require.NoError(t, f.snapshotter.Commit(context.Background(), f.headChain, "prepare-head"))
}

type fakeMounts struct {
	mu             sync.Mutex
	mounted        int
	unmounted      int
	unmountStarted chan struct{}
	allowUnmount   chan struct{}
}

func (m *fakeMounts) mount(string, *rootfsreader.Reader, rootfshead.Head) (mountedHead, error) {
	m.mu.Lock()
	m.mounted++
	m.mu.Unlock()
	return &fakeMountedHead{owner: m}, nil
}

func (m *fakeMounts) count() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.mounted
}

func (m *fakeMounts) unmountCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.unmounted
}

func (m *fakeMounts) reset() {
	m.mu.Lock()
	m.mounted = 0
	m.unmounted = 0
	m.mu.Unlock()
}

type fakeMountedHead struct {
	owner *fakeMounts
}

func (m *fakeMountedHead) Unmount() error {
	m.owner.mu.Lock()
	m.owner.unmounted++
	started := m.owner.unmountStarted
	allow := m.owner.allowUnmount
	m.owner.unmountStarted = nil
	m.owner.allowUnmount = nil
	m.owner.mu.Unlock()
	if started != nil {
		close(started)
	}
	if allow != nil {
		<-allow
	}
	return nil
}

func (*fakeMountedHead) HealthError() error { return nil }

func stringReader(value string) *strings.Reader {
	return strings.NewReader(value)
}

func rootfsreaderCacheDisabled(*Snapshotter) *rootfsreader.MetadataCache {
	return rootfsreader.NewMetadataCache(-1)
}

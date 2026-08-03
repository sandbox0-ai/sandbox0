package rootfssnapshotter

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/containerd/containerd/v2/core/mount"
	"github.com/containerd/containerd/v2/core/snapshots"
	"github.com/containerd/errdefs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/opencontainers/go-digest"
	"github.com/sandbox0-ai/sandbox0/pkg/fuseportal"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshead"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/objectstore"
)

const (
	labelSnapshotReference = "containerd.io/snapshot.ref"
	labelOverlayUpperdir   = "containerd.io/snapshot/overlay.upperdir"
	labelHeadBaseReference = "containerd.io/snapshot/sandbox0.rootfs-base"
)

type mountedHead interface {
	Unmount() error
}

type mountedHeadHealth interface {
	HealthError() error
}

type headMounter func(string, *LayerTree) (mountedHead, error)

// Snapshotter delegates normal OCI layers to overlayfs and intercepts only a
// metadata-only rootfs head marker. The marker becomes a committed remote
// lower layer while every running container still receives a normal writable
// overlay upperdir.
type Snapshotter struct {
	delegate snapshots.Snapshotter
	store    objectstore.Store
	mount    headMounter
	chunks   *chunkCache
	observer *Observer

	mu        sync.Mutex
	ops       sync.WaitGroup
	mounted   map[string]mountedHead
	children  map[string]string
	headLocks map[string]*headOperationLock
	closed    bool
}

type headOperationLock struct {
	mu   sync.Mutex
	refs int
}

type SnapshotterOption func(*Snapshotter)

func WithObserver(observer *Observer) SnapshotterOption {
	return func(snapshotter *Snapshotter) {
		snapshotter.observer = observer
	}
}

func NewSnapshotter(delegate snapshots.Snapshotter, store objectstore.Store, options ...SnapshotterOption) (*Snapshotter, error) {
	if delegate == nil {
		return nil, fmt.Errorf("overlay snapshotter is required")
	}
	if store == nil {
		return nil, fmt.Errorf("rootfs object store is required")
	}
	snapshotter := &Snapshotter{
		delegate:  delegate,
		store:     store,
		mount:     mountLayerTree,
		chunks:    newChunkCache(defaultChunkCacheBytes),
		mounted:   make(map[string]mountedHead),
		children:  make(map[string]string),
		headLocks: make(map[string]*headOperationLock),
	}
	for _, option := range options {
		if option != nil {
			option(snapshotter)
		}
	}
	return snapshotter, nil
}

func (s *Snapshotter) Stat(ctx context.Context, key string) (snapshots.Info, error) {
	return s.delegate.Stat(ctx, key)
}

func (s *Snapshotter) Update(ctx context.Context, info snapshots.Info, fieldpaths ...string) (snapshots.Info, error) {
	return s.delegate.Update(ctx, info, fieldpaths...)
}

func (s *Snapshotter) Usage(ctx context.Context, key string) (snapshots.Usage, error) {
	return s.delegate.Usage(ctx, key)
}

func (s *Snapshotter) Mounts(ctx context.Context, key string) ([]mount.Mount, error) {
	return s.delegate.Mounts(ctx, key)
}

func (s *Snapshotter) Prepare(ctx context.Context, key, parent string, opts ...snapshots.Opt) ([]mount.Mount, error) {
	return s.createChild(ctx, key, parent, s.delegate.Prepare, opts...)
}

type childCreator func(context.Context, string, string, ...snapshots.Opt) ([]mount.Mount, error)

func (s *Snapshotter) createChild(
	ctx context.Context,
	key string,
	parent string,
	create childCreator,
	opts ...snapshots.Opt,
) ([]mount.Mount, error) {
	if strings.TrimSpace(parent) == "" {
		return create(ctx, key, parent, opts...)
	}
	parentInfo, err := s.delegate.Stat(ctx, parent)
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(parentInfo.Labels[rootfshead.AnnotationHead]) == "" {
		return create(ctx, key, parent, opts...)
	}
	if !s.beginOperation() {
		return nil, fmt.Errorf("rootfs snapshotter is closed")
	}
	defer s.ops.Done()
	releaseHead := s.acquireHeadOperation(parent)
	defer releaseHead()
	if err := s.ensureMounted(ctx, parentInfo); err != nil {
		return nil, fmt.Errorf("mount rootfs head parent %s: %w", parent, err)
	}
	mounts, err := create(ctx, key, parent, opts...)
	if err != nil {
		return nil, errors.Join(err, s.unmountHeadIfUnused(parent))
	}
	s.mu.Lock()
	if existing := s.children[key]; existing != "" && existing != parent {
		s.mu.Unlock()
		_ = s.delegate.Remove(ctx, key)
		return nil, fmt.Errorf("snapshot child %s already references rootfs head %s", key, existing)
	}
	s.children[key] = parent
	s.mu.Unlock()
	return mounts, nil
}

func (s *Snapshotter) annotationFromStoredMarker(ctx context.Context, target string) (string, bool, error) {
	if s == nil || s.store == nil {
		return "", false, nil
	}
	if err := ctx.Err(); err != nil {
		return "", false, err
	}
	key, err := rootfshead.MarkerObjectKey(target)
	if err != nil {
		return "", false, nil
	}
	reader, err := s.store.Get(key, 0, rootfshead.MaxMarkerBytes+1)
	if err != nil {
		if objectstore.IsNotFound(err) {
			return "", false, nil
		}
		return "", false, fmt.Errorf("open stored rootfs head marker %s: %w", target, err)
	}
	defer reader.Close()
	payload, err := io.ReadAll(io.LimitReader(reader, rootfshead.MaxMarkerBytes+1))
	if err != nil {
		return "", false, fmt.Errorf("read stored rootfs head marker %s: %w", target, err)
	}
	if len(payload) == 0 || int64(len(payload)) > rootfshead.MaxMarkerBytes {
		return "", false, fmt.Errorf("stored rootfs head marker %s has invalid size %d", target, len(payload))
	}
	digestValue, err := digest.Parse(strings.TrimSpace(target))
	if err != nil || digest.FromBytes(payload) != digestValue {
		return "", false, fmt.Errorf("stored rootfs head marker %s failed digest validation", target)
	}
	return markerAnnotation(bytes.NewReader(payload), target)
}

func markerAnnotation(reader io.Reader, target string) (string, bool, error) {
	reference, err := rootfshead.DecodeMarker(reader)
	if errors.Is(err, rootfshead.ErrNotMarker) {
		return "", false, nil
	}
	if err != nil {
		return "", false, fmt.Errorf("decode rootfs head marker %s: %w", target, err)
	}
	annotation, err := rootfshead.EncodeHeadAnnotation(reference)
	if err != nil {
		return "", false, err
	}
	return annotation, true, nil
}

func (s *Snapshotter) resolveBackendSnapshot(ctx context.Context, reference string) (snapshots.Info, error) {
	reference = strings.TrimSpace(reference)
	if reference == "" {
		return snapshots.Info{}, fmt.Errorf("snapshot reference is required")
	}
	// Direct delegates commonly use the canonical reference as the physical
	// key. Avoid an O(number of local snapshots) walk on every head commit.
	if info, err := s.delegate.Stat(ctx, reference); err == nil {
		if info.Kind == snapshots.KindCommitted {
			return info, nil
		}
	} else if !errdefs.IsNotFound(err) {
		return snapshots.Info{}, err
	}
	var matches []snapshots.Info
	err := s.delegate.Walk(ctx, func(_ context.Context, info snapshots.Info) error {
		if info.Kind == snapshots.KindCommitted &&
			(strings.TrimSpace(info.Labels[labelSnapshotReference]) == reference || backendSnapshotReference(info.Name) == reference) {
			matches = append(matches, info)
		}
		return nil
	})
	if err != nil {
		return snapshots.Info{}, err
	}
	if len(matches) == 0 {
		return snapshots.Info{}, fmt.Errorf("snapshot reference %s: %w", reference, errdefs.ErrNotFound)
	}
	sort.Slice(matches, func(i, j int) bool { return matches[i].Name < matches[j].Name })
	return matches[0], nil
}

func backendSnapshotReference(name string) string {
	candidate := strings.TrimSpace(name)
	if separator := strings.LastIndex(candidate, "/"); separator >= 0 {
		candidate = candidate[separator+1:]
	}
	parsed, err := digest.Parse(candidate)
	if err != nil {
		return ""
	}
	return parsed.String()
}

func (s *Snapshotter) View(ctx context.Context, key, parent string, opts ...snapshots.Opt) ([]mount.Mount, error) {
	return s.createChild(ctx, key, parent, s.delegate.View, opts...)
}

func (s *Snapshotter) Commit(ctx context.Context, name, key string, opts ...snapshots.Opt) error {
	activeInfo, err := s.delegate.Stat(ctx, key)
	if err != nil {
		return err
	}
	labels, err := labelsFromOpts(opts)
	if err != nil {
		return err
	}
	reference := strings.TrimSpace(labels[labelSnapshotReference])
	if reference == "" {
		reference = strings.TrimSpace(activeInfo.Labels[labelSnapshotReference])
	}
	if reference == "" {
		reference = backendSnapshotReference(name)
	}
	if reference != "" {
		labels[labelSnapshotReference] = reference
	}
	if reference != "" && (activeInfo.Parent == "" || strings.TrimSpace(activeInfo.Labels[rootfshead.AnnotationHead]) != "") {
		annotation, marker, err := s.annotationFromStoredMarker(ctx, reference)
		if err != nil {
			return err
		}
		if marker {
			if activeAnnotation := strings.TrimSpace(activeInfo.Labels[rootfshead.AnnotationHead]); activeAnnotation != "" && activeAnnotation != annotation {
				return fmt.Errorf("rootfs head active snapshot has conflicting metadata")
			}
			started := time.Now()
			if !s.beginOperation() {
				return fmt.Errorf("rootfs snapshotter is closed")
			}
			defer s.ops.Done()
			releaseHead := s.acquireHeadOperation(name)
			defer releaseHead()
			err := s.commitHead(ctx, name, key, activeInfo, reference, annotation, labels, opts)
			s.observer.Observe("commit_head", reference, started, err)
			return err
		}
	}
	if reference != "" {
		opts = append(opts, snapshots.WithLabels(labels))
	}
	return s.delegate.Commit(ctx, name, key, opts...)
}

func (s *Snapshotter) commitHead(
	ctx context.Context,
	name string,
	key string,
	activeInfo snapshots.Info,
	reference string,
	annotation string,
	labels map[string]string,
	opts []snapshots.Opt,
) error {
	headReference, err := rootfshead.DecodeHeadAnnotation(annotation)
	if err != nil {
		return fmt.Errorf("decode rootfs head annotation: %w", err)
	}
	head, err := loadHead(ctx, s.store, headReference)
	if err != nil {
		return err
	}
	baseInfo, err := s.resolveBackendSnapshot(ctx, head.BaseSnapshotKey)
	if err != nil {
		return fmt.Errorf("resolve rootfs head base snapshot %s: %w", head.BaseSnapshotKey, err)
	}
	originalLabels := cloneLabels(activeInfo.Labels)
	delete(originalLabels, labelOverlayUpperdir)
	activeLabels := cloneLabels(originalLabels)
	for label, value := range labels {
		activeLabels[label] = value
	}
	activeLabels[labelSnapshotReference] = reference
	activeLabels[labelHeadBaseReference] = head.BaseSnapshotKey
	activeLabels[rootfshead.AnnotationHead] = annotation

	existing := strings.TrimSpace(activeInfo.Labels[rootfshead.AnnotationHead])
	if existing != "" && existing != annotation {
		return fmt.Errorf("rootfs head active snapshot has conflicting metadata")
	}
	declaredBase := strings.TrimSpace(activeInfo.Labels[labelHeadBaseReference])
	if declaredBase != "" && declaredBase != head.BaseSnapshotKey {
		return fmt.Errorf("rootfs head active snapshot has conflicting base %q", declaredBase)
	}
	if activeInfo.Parent == "" {
		// containerd inherits descriptor annotations onto the unpack snapshot.
		// The head annotation alone does not mean the empty marker layer has
		// already been physically rebased onto its canonical OCI base.
		if err := s.delegate.Remove(ctx, key); err != nil {
			return fmt.Errorf("remove filesystem-empty rootfs marker snapshot: %w", err)
		}
		if _, err := s.delegate.Prepare(ctx, key, baseInfo.Name, snapshots.WithLabels(activeLabels)); err != nil {
			_, restoreErr := s.delegate.Prepare(ctx, key, activeInfo.Parent, snapshots.WithLabels(originalLabels))
			return errors.Join(fmt.Errorf("rebase rootfs head snapshot onto %s: %w", head.BaseSnapshotKey, err), restoreErr)
		}
		activeInfo, err = s.delegate.Stat(ctx, key)
		if err != nil {
			return fmt.Errorf("inspect rebased rootfs head snapshot: %w", err)
		}
	}
	if activeInfo.Parent != baseInfo.Name {
		return fmt.Errorf("rootfs head active snapshot has physical parent %q, expected %q", activeInfo.Parent, baseInfo.Name)
	}

	upperdir := strings.TrimSpace(activeInfo.Labels[labelOverlayUpperdir])
	if upperdir == "" {
		return fmt.Errorf("rebased rootfs head snapshot has no overlay upperdir")
	}
	commitLabels := cloneLabels(activeLabels)
	delete(commitLabels, labelOverlayUpperdir)
	commitOpts := append(append([]snapshots.Opt(nil), opts...), snapshots.WithLabels(commitLabels))
	if err := s.delegate.Commit(ctx, name, key, commitOpts...); err != nil {
		return err
	}
	return nil
}

func (s *Snapshotter) Remove(ctx context.Context, key string) error {
	if !s.beginOperation() {
		return fmt.Errorf("rootfs snapshotter is closed")
	}
	defer s.ops.Done()

	info, statErr := s.delegate.Stat(ctx, key)
	if statErr != nil && !errdefs.IsNotFound(statErr) {
		return statErr
	}
	isHead := statErr == nil && info.Kind == snapshots.KindCommitted && strings.TrimSpace(info.Labels[rootfshead.AnnotationHead]) != ""
	if !isHead {
		s.mu.Lock()
		parent := s.children[key]
		s.mu.Unlock()
		if parent != "" {
			releaseHead := s.acquireHeadOperation(parent)
			defer releaseHead()
		}
		if err := s.delegate.Remove(ctx, key); err != nil {
			return err
		}
		s.mu.Lock()
		if s.children[key] == parent {
			delete(s.children, key)
		}
		s.mu.Unlock()
		return s.unmountHeadIfUnused(parent)
	}
	releaseHead := s.acquireHeadOperation(key)
	defer releaseHead()
	s.mu.Lock()
	if s.headInUseLocked(key) {
		s.mu.Unlock()
		return fmt.Errorf("cannot remove rootfs head snapshot %s with live children: %w", key, errdefs.ErrFailedPrecondition)
	}
	s.mu.Unlock()
	if err := s.unmountHeadIfUnused(key); err != nil {
		return err
	}
	if err := s.delegate.Remove(ctx, key); err != nil {
		remountErr := s.ensureMounted(ctx, info)
		return errors.Join(err, remountErr)
	}
	return nil
}

func (s *Snapshotter) headInUseLocked(head string) bool {
	if strings.TrimSpace(head) == "" {
		return false
	}
	for _, parent := range s.children {
		if parent == head {
			return true
		}
	}
	return false
}

func (s *Snapshotter) unmountHeadIfUnused(head string) error {
	s.mu.Lock()
	if strings.TrimSpace(head) == "" || s.headInUseLocked(head) {
		s.mu.Unlock()
		return nil
	}
	mounted := s.mounted[head]
	if mounted == nil {
		s.mu.Unlock()
		return nil
	}
	delete(s.mounted, head)
	s.mu.Unlock()
	if err := mounted.Unmount(); err != nil {
		s.mu.Lock()
		if s.mounted[head] == nil {
			s.mounted[head] = mounted
		}
		s.mu.Unlock()
		return err
	}
	return nil
}

func (s *Snapshotter) Walk(ctx context.Context, fn snapshots.WalkFunc, filterStrings ...string) error {
	return s.delegate.Walk(ctx, fn, filterStrings...)
}

// Recover remounts only head layers with live child snapshots after process
// startup. Cached, unused head images remain unmounted until Prepare or View.
func (s *Snapshotter) Recover(ctx context.Context) error {
	var infos []snapshots.Info
	if err := s.delegate.Walk(ctx, func(_ context.Context, info snapshots.Info) error {
		infos = append(infos, info)
		return nil
	}); err != nil {
		// containerd's overlay metadata buckets are created by the first write.
		// A brand-new delegate therefore reports NotFound when it is walked.
		if errdefs.IsNotFound(err) {
			return nil
		}
		return fmt.Errorf("list persisted rootfs head snapshots: %w", err)
	}
	heads := make(map[string]snapshots.Info)
	for _, info := range infos {
		if info.Kind == snapshots.KindCommitted && strings.TrimSpace(info.Labels[rootfshead.AnnotationHead]) != "" {
			heads[info.Name] = info
		}
	}
	children := make(map[string]string)
	usedHeads := make(map[string]struct{})
	for _, info := range infos {
		if _, ok := heads[info.Parent]; !ok {
			continue
		}
		children[info.Name] = info.Parent
		usedHeads[info.Parent] = struct{}{}
	}
	orderedHeads := make([]string, 0, len(usedHeads))
	for head := range usedHeads {
		orderedHeads = append(orderedHeads, head)
	}
	sort.Strings(orderedHeads)
	s.mu.Lock()
	s.children = children
	s.mu.Unlock()
	for _, name := range orderedHeads {
		info := heads[name]
		releaseHead := s.acquireHeadOperation(name)
		err := s.ensureMounted(ctx, info)
		releaseHead()
		if err != nil {
			return fmt.Errorf("recover rootfs head snapshot %s: %w", info.Name, err)
		}
	}
	return nil
}

func (s *Snapshotter) ensureMounted(ctx context.Context, info snapshots.Info) (resultErr error) {
	s.mu.Lock()
	if s.mounted[info.Name] != nil {
		s.mu.Unlock()
		return nil
	}
	if s.closed {
		s.mu.Unlock()
		return fmt.Errorf("rootfs snapshotter is closed")
	}
	s.mu.Unlock()
	started := time.Now()
	defer func() {
		s.observer.Observe("attach_head", info.Name, started, resultErr)
	}()
	annotation := strings.TrimSpace(info.Labels[rootfshead.AnnotationHead])
	if annotation == "" {
		return fmt.Errorf("rootfs head annotation is missing")
	}
	reference, err := rootfshead.DecodeHeadAnnotation(annotation)
	if err != nil {
		return err
	}
	head, err := loadHead(ctx, s.store, reference)
	if err != nil {
		return err
	}
	tree, err := loadLayerTree(ctx, s.store, head, s.chunks)
	if err != nil {
		return err
	}
	upperdir := strings.TrimSpace(info.Labels[labelOverlayUpperdir])
	if upperdir == "" {
		refreshed, err := s.delegate.Stat(ctx, info.Name)
		if err != nil {
			return err
		}
		upperdir = strings.TrimSpace(refreshed.Labels[labelOverlayUpperdir])
	}
	if upperdir == "" {
		return fmt.Errorf("committed rootfs head snapshot has no overlay upperdir")
	}
	mounted, err := s.mount(upperdir, tree)
	if err != nil {
		return err
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return errors.Join(fmt.Errorf("rootfs snapshotter is closed"), mounted.Unmount())
	}
	if existing := s.mounted[info.Name]; existing != nil {
		s.mu.Unlock()
		return mounted.Unmount()
	}
	s.mounted[info.Name] = mounted
	s.mu.Unlock()
	return nil
}

func (s *Snapshotter) beginOperation() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return false
	}
	s.ops.Add(1)
	return true
}

func (s *Snapshotter) acquireHeadOperation(head string) func() {
	head = strings.TrimSpace(head)
	s.mu.Lock()
	lock := s.headLocks[head]
	if lock == nil {
		lock = &headOperationLock{}
		s.headLocks[head] = lock
	}
	lock.refs++
	s.mu.Unlock()
	lock.mu.Lock()
	return func() {
		lock.mu.Unlock()
		s.mu.Lock()
		lock.refs--
		if lock.refs == 0 && s.headLocks[head] == lock {
			delete(s.headLocks, head)
		}
		s.mu.Unlock()
	}
}

// HealthError reports a failed FUSE server without restarting the snapshotter
// process and disrupting unrelated live mounts.
func (s *Snapshotter) HealthError() error {
	if s == nil {
		return fmt.Errorf("rootfs snapshotter is nil")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return fmt.Errorf("rootfs snapshotter is closed")
	}
	for name, mounted := range s.mounted {
		health, ok := mounted.(mountedHeadHealth)
		if !ok {
			continue
		}
		if err := health.HealthError(); err != nil {
			return fmt.Errorf("rootfs head snapshot %s mount failed: %w", name, err)
		}
	}
	return nil
}

func (s *Snapshotter) Close() error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	s.mu.Unlock()
	s.ops.Wait()
	s.mu.Lock()
	mounts := make([]mountedHead, 0, len(s.mounted))
	for _, mounted := range s.mounted {
		mounts = append(mounts, mounted)
	}
	s.mounted = make(map[string]mountedHead)
	s.children = make(map[string]string)
	s.headLocks = make(map[string]*headOperationLock)
	s.mu.Unlock()
	var closeErr error
	for _, mounted := range mounts {
		closeErr = errors.Join(closeErr, mounted.Unmount())
	}
	closeErr = errors.Join(closeErr, s.delegate.Close())
	return closeErr
}

func labelsFromOpts(opts []snapshots.Opt) (map[string]string, error) {
	info := snapshots.Info{}
	for _, opt := range opts {
		if opt == nil {
			continue
		}
		if err := opt(&info); err != nil {
			return nil, err
		}
	}
	return cloneLabels(info.Labels), nil
}

func cloneLabels(labels map[string]string) map[string]string {
	cloned := make(map[string]string, len(labels))
	for key, value := range labels {
		cloned[key] = value
	}
	return cloned
}

type fuseHeadMount struct {
	server   *fuseportal.Server
	done     chan struct{}
	mu       sync.Mutex
	serveErr error
}

func mountLayerTree(target string, tree *LayerTree) (mountedHead, error) {
	if tree == nil {
		return nil, fmt.Errorf("rootfs layer tree is required")
	}
	server, err := fuseportal.Mount(tree.rawFileSystem(), target, &fuse.MountOptions{
		FsName:        "sandbox0-rootfs-head",
		Name:          "sandbox0-rootfs",
		AllowOther:    os.Getuid() == 0,
		DirectMount:   true,
		MaxBackground: 128,
		MaxWrite:      1024 * 1024,
		Options:       []string{"default_permissions", "ro"},
		// Linux 6.17 rejects ALLOW_IDMAP without a compatible stacked fs.
		DisabledCapabilities: fuse.CAP_ALLOW_IDMAP,
	})
	if err != nil {
		return nil, err
	}
	mounted := &fuseHeadMount{server: server, done: make(chan struct{})}
	go func() {
		err := server.Serve()
		mounted.mu.Lock()
		mounted.serveErr = err
		mounted.mu.Unlock()
		close(mounted.done)
	}()
	readyCtx, cancelReady := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelReady()
	if err := server.WaitReady(readyCtx); err != nil {
		return nil, errors.Join(fmt.Errorf("start rootfs FUSE server: %w", err), mounted.Unmount())
	}
	return mounted, nil
}

func (m *fuseHeadMount) Unmount() error {
	if m == nil || m.server == nil {
		return nil
	}
	return m.server.Unmount()
}

func (m *fuseHeadMount) HealthError() error {
	if m == nil {
		return fmt.Errorf("FUSE mount is nil")
	}
	select {
	case <-m.done:
		m.mu.Lock()
		defer m.mu.Unlock()
		if m.serveErr != nil {
			return m.serveErr
		}
		return fmt.Errorf("FUSE server stopped")
	default:
		return nil
	}
}

var _ snapshots.Snapshotter = (*Snapshotter)(nil)

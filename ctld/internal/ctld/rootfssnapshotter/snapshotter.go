// Package rootfssnapshotter implements the external sandbox0 snapshotter.
package rootfssnapshotter

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"sync"
	"syscall"

	"github.com/containerd/containerd/v2/core/mount"
	"github.com/containerd/containerd/v2/core/snapshots"
	"github.com/containerd/errdefs"
	"github.com/hanwen/go-fuse/v2/fuse"
	ctldrootfs "github.com/sandbox0-ai/sandbox0/ctld/internal/ctld/rootfs"
	"github.com/sandbox0-ai/sandbox0/ctld/internal/ctld/rootfsfuse"
	"github.com/sandbox0-ai/sandbox0/ctld/internal/ctld/rootfsreader"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshead"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/objectstore"
	"golang.org/x/sys/unix"
)

const (
	labelSnapshotReference = "containerd.io/snapshot.ref"
	labelOverlayUpperdir   = "containerd.io/snapshot/overlay.upperdir"
)

type mountedHead interface {
	Unmount() error
	HealthError() error
}

type headMounter func(string, *rootfsreader.Reader, rootfshead.Head) (mountedHead, error)

type Config struct {
	Delegate           snapshots.Snapshotter
	Store              objectstore.Store
	ObjectCache        *ctldrootfs.ObjectCache
	MetadataCacheBytes int64
}

// Snapshotter delegates ordinary OCI snapshots to overlayfs. A marker layer
// is rebased onto its durable OCI base and its empty upperdir is replaced by
// the immutable FUSE Head before writable children are created.
type Snapshotter struct {
	delegate snapshots.Snapshotter
	store    objectstore.Store
	objects  *ctldrootfs.ObjectCache
	metadata *rootfsreader.MetadataCache
	mount    headMounter

	mu        sync.Mutex
	ops       sync.WaitGroup
	closed    bool
	mounted   map[string]mountedHead
	children  map[string]string
	headLocks map[string]*headLock
}

type headLock struct {
	mutex sync.Mutex
	refs  int
}

func New(cfg Config) (*Snapshotter, error) {
	if cfg.Delegate == nil {
		return nil, fmt.Errorf("rootfs snapshotter overlay delegate is required")
	}
	if cfg.Store == nil {
		return nil, fmt.Errorf("rootfs snapshotter object store is required")
	}
	metadataBytes := cfg.MetadataCacheBytes
	if metadataBytes == 0 {
		metadataBytes = 128 << 20
	}
	return &Snapshotter{
		delegate:  cfg.Delegate,
		store:     cfg.Store,
		objects:   cfg.ObjectCache,
		metadata:  rootfsreader.NewMetadataCache(metadataBytes),
		mount:     mountFUSEHead,
		mounted:   make(map[string]mountedHead),
		children:  make(map[string]string),
		headLocks: make(map[string]*headLock),
	}, nil
}

func (s *Snapshotter) Stat(ctx context.Context, key string) (snapshots.Info, error) {
	if !s.begin() {
		return snapshots.Info{}, fmt.Errorf("rootfs snapshotter is closed")
	}
	defer s.ops.Done()
	return s.delegate.Stat(ctx, key)
}

func (s *Snapshotter) Update(ctx context.Context, info snapshots.Info, fields ...string) (snapshots.Info, error) {
	if !s.begin() {
		return snapshots.Info{}, fmt.Errorf("rootfs snapshotter is closed")
	}
	defer s.ops.Done()
	current, err := s.delegate.Stat(ctx, info.Name)
	if err != nil {
		return snapshots.Info{}, err
	}
	info.Labels = cloneLabels(info.Labels)
	broadLabels := len(fields) == 0
	for _, field := range fields {
		if field == "labels" {
			broadLabels = true
		}
		for _, reserved := range reservedSnapshotLabels() {
			if field != "labels."+reserved {
				continue
			}
			return snapshots.Info{}, fmt.Errorf("cannot update reserved rootfs snapshot label %q: %w", reserved, errdefs.ErrInvalidArgument)
		}
	}
	if broadLabels {
		for _, reserved := range reservedSnapshotLabels() {
			if value, ok := current.Labels[reserved]; ok {
				info.Labels[reserved] = value
			} else {
				delete(info.Labels, reserved)
			}
		}
	}
	return s.delegate.Update(ctx, info, fields...)
}

func reservedSnapshotLabels() []string {
	return []string{rootfshead.AnnotationHead, rootfshead.LabelBaseChainID, labelOverlayUpperdir}
}

func (s *Snapshotter) Usage(ctx context.Context, key string) (snapshots.Usage, error) {
	if !s.begin() {
		return snapshots.Usage{}, fmt.Errorf("rootfs snapshotter is closed")
	}
	defer s.ops.Done()
	return s.delegate.Usage(ctx, key)
}

func (s *Snapshotter) Mounts(ctx context.Context, key string) ([]mount.Mount, error) {
	if !s.begin() {
		return nil, fmt.Errorf("rootfs snapshotter is closed")
	}
	defer s.ops.Done()
	return s.delegate.Mounts(ctx, key)
}

func (s *Snapshotter) Prepare(ctx context.Context, key, parent string, opts ...snapshots.Opt) ([]mount.Mount, error) {
	if !s.begin() {
		return nil, fmt.Errorf("rootfs snapshotter is closed")
	}
	defer s.ops.Done()
	return s.createChild(ctx, s.delegate.Prepare, key, parent, opts...)
}

func (s *Snapshotter) View(ctx context.Context, key, parent string, opts ...snapshots.Opt) ([]mount.Mount, error) {
	if !s.begin() {
		return nil, fmt.Errorf("rootfs snapshotter is closed")
	}
	defer s.ops.Done()
	return s.createChild(ctx, s.delegate.View, key, parent, opts...)
}

type createSnapshot func(context.Context, string, string, ...snapshots.Opt) ([]mount.Mount, error)

func (s *Snapshotter) createChild(ctx context.Context, create createSnapshot, key, parent string, opts ...snapshots.Opt) ([]mount.Mount, error) {
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
	unlock := s.lockHead(parent)
	defer unlock()
	if err := s.ensureMounted(ctx, parentInfo); err != nil {
		return nil, fmt.Errorf("attach rootfs head %s: %w", parent, err)
	}
	mounts, err := create(ctx, key, parent, opts...)
	if err != nil {
		return nil, errors.Join(err, s.unmountIfUnused(parent))
	}
	s.mu.Lock()
	if existing := s.children[key]; existing != "" && existing != parent {
		s.mu.Unlock()
		_ = s.delegate.Remove(ctx, key)
		return nil, fmt.Errorf("snapshot %s already references rootfs head %s", key, existing)
	}
	s.children[key] = parent
	s.mu.Unlock()
	return mounts, nil
}

func (s *Snapshotter) Commit(ctx context.Context, name, key string, opts ...snapshots.Opt) error {
	if !s.begin() {
		return fmt.Errorf("rootfs snapshotter is closed")
	}
	defer s.ops.Done()
	active, err := s.delegate.Stat(ctx, key)
	if err != nil {
		return err
	}
	annotation := strings.TrimSpace(active.Labels[rootfshead.AnnotationHead])
	if annotation == "" {
		return s.commitChild(ctx, name, key, opts...)
	}
	// Remove addresses the active key while Commit renames it to name. Lock the
	// active key so a marker cannot be removed halfway through its rebase.
	unlock := s.lockHead(key)
	defer unlock()
	return s.commitHead(ctx, name, key, active, annotation, opts...)
}

func (s *Snapshotter) commitChild(ctx context.Context, name, key string, opts ...snapshots.Opt) error {
	s.mu.Lock()
	parent := s.children[key]
	s.mu.Unlock()
	if parent == "" {
		return s.delegate.Commit(ctx, name, key, opts...)
	}
	unlock := s.lockHead(parent)
	defer unlock()
	if err := s.delegate.Commit(ctx, name, key, opts...); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if current := s.children[key]; current != parent {
		return fmt.Errorf("snapshot %s rootfs parent changed during commit", key)
	}
	if existing := s.children[name]; existing != "" && existing != parent {
		return fmt.Errorf("committed snapshot %s already references rootfs head %s", name, existing)
	}
	delete(s.children, key)
	s.children[name] = parent
	return nil
}

func (s *Snapshotter) commitHead(
	ctx context.Context,
	name string,
	key string,
	active snapshots.Info,
	annotation string,
	opts ...snapshots.Opt,
) error {
	reference, err := rootfshead.DecodeHeadAnnotation(annotation)
	if err != nil {
		return fmt.Errorf("decode rootfs Head marker annotation: %w", err)
	}
	reader, head, err := rootfsreader.NewForHead(ctx, rootfsreader.ReaderConfig{
		Store:               s.store,
		ObjectCache:         s.objects,
		SharedMetadataCache: s.metadata,
	}, reference)
	if err != nil {
		return fmt.Errorf("load rootfs Head %s: %w", reference.HeadID, err)
	}
	_ = reader
	if active.Parent == "" {
		return fmt.Errorf("rootfs marker must be prepared on its canonical base")
	}
	declaredBase := strings.TrimSpace(active.Labels[rootfshead.LabelBaseChainID])
	if declaredBase != head.Base.ChainID {
		return fmt.Errorf("rootfs marker declares base %q, expected %q", declaredBase, head.Base.ChainID)
	}
	labels, err := mergeLabels(active.Labels, opts)
	if err != nil {
		return err
	}
	delete(labels, labelOverlayUpperdir)
	labels[rootfshead.AnnotationHead] = annotation
	labels[rootfshead.LabelBaseChainID] = head.Base.ChainID
	if existing := strings.TrimSpace(active.Labels[rootfshead.AnnotationHead]); existing != "" && existing != annotation {
		return fmt.Errorf("rootfs marker has conflicting Head metadata")
	}
	return s.delegate.Commit(ctx, name, key, snapshots.WithLabels(labels))
}

func (s *Snapshotter) Remove(ctx context.Context, key string) error {
	if !s.begin() {
		return fmt.Errorf("rootfs snapshotter is closed")
	}
	defer s.ops.Done()
	info, err := s.delegate.Stat(ctx, key)
	if err != nil {
		return err
	}
	if strings.TrimSpace(info.Labels[rootfshead.AnnotationHead]) != "" {
		unlock := s.lockHead(key)
		defer unlock()
		// Commit may have completed while Remove waited for the active key.
		// Re-read state before deciding whether this is an active marker or a
		// committed Head with live children.
		info, err = s.delegate.Stat(ctx, key)
		if err != nil {
			return err
		}
		if info.Kind != snapshots.KindCommitted {
			return s.delegate.Remove(ctx, key)
		}
		s.mu.Lock()
		inUse := s.headInUseLocked(key)
		s.mu.Unlock()
		if inUse {
			return fmt.Errorf("rootfs Head %s has live children: %w", key, errdefs.ErrFailedPrecondition)
		}
		if err := s.unmountIfUnused(key); err != nil {
			return err
		}
		if err := s.delegate.Remove(ctx, key); err != nil {
			_ = s.ensureMounted(ctx, info)
			return err
		}
		return nil
	}
	s.mu.Lock()
	parent := s.children[key]
	s.mu.Unlock()
	var unlockParent func()
	if parent != "" {
		unlockParent = s.lockHead(parent)
		defer unlockParent()
		// The child may have been removed while waiting for the Head lock.
		s.mu.Lock()
		parent = s.children[key]
		s.mu.Unlock()
	}
	if err := s.delegate.Remove(ctx, key); err != nil {
		return err
	}
	s.mu.Lock()
	delete(s.children, key)
	s.mu.Unlock()
	return s.unmountIfUnused(parent)
}

func (s *Snapshotter) Walk(ctx context.Context, fn snapshots.WalkFunc, filters ...string) error {
	if !s.begin() {
		return fmt.Errorf("rootfs snapshotter is closed")
	}
	defer s.ops.Done()
	return s.delegate.Walk(ctx, fn, filters...)
}

func (s *Snapshotter) Cleanup(ctx context.Context) error {
	if !s.begin() {
		return fmt.Errorf("rootfs snapshotter is closed")
	}
	defer s.ops.Done()
	cleaner, ok := s.delegate.(snapshots.Cleaner)
	if !ok {
		return fmt.Errorf("rootfs overlay cleanup: %w", errdefs.ErrNotImplemented)
	}
	return cleaner.Cleanup(ctx)
}

// Recover restores FUSE mounts before readiness for every Head that still has
// a delegate child. This makes a snapshotter restart explicit and observable.
func (s *Snapshotter) Recover(ctx context.Context) error {
	if !s.begin() {
		return fmt.Errorf("rootfs snapshotter is closed")
	}
	defer s.ops.Done()
	var infos []snapshots.Info
	if err := s.delegate.Walk(ctx, func(_ context.Context, info snapshots.Info) error {
		infos = append(infos, info)
		return nil
	}); err != nil {
		if errdefs.IsNotFound(err) {
			return nil
		}
		return err
	}
	heads := make(map[string]snapshots.Info)
	for _, info := range infos {
		if info.Kind == snapshots.KindCommitted && strings.TrimSpace(info.Labels[rootfshead.AnnotationHead]) != "" {
			heads[info.Name] = info
		}
	}
	children := make(map[string]string)
	used := make(map[string]struct{})
	for _, info := range infos {
		if _, ok := heads[info.Parent]; !ok {
			continue
		}
		children[info.Name] = info.Parent
		used[info.Parent] = struct{}{}
	}
	s.mu.Lock()
	s.children = children
	s.mu.Unlock()
	names := make([]string, 0, len(used))
	for name := range used {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		unlock := s.lockHead(name)
		err := s.ensureMounted(ctx, heads[name])
		unlock()
		if err != nil {
			return fmt.Errorf("recover rootfs Head %s: %w", name, err)
		}
	}
	return nil
}

func (s *Snapshotter) HealthError() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return fmt.Errorf("rootfs snapshotter is closed")
	}
	for name, mounted := range s.mounted {
		if err := mounted.HealthError(); err != nil {
			return fmt.Errorf("rootfs Head %s: %w", name, err)
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
	clear(s.mounted)
	clear(s.children)
	s.mu.Unlock()
	var result error
	for _, mounted := range mounts {
		result = errors.Join(result, mounted.Unmount())
	}
	return errors.Join(result, s.delegate.Close())
}

func (s *Snapshotter) ensureMounted(ctx context.Context, info snapshots.Info) error {
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
	annotation := strings.TrimSpace(info.Labels[rootfshead.AnnotationHead])
	reference, err := rootfshead.DecodeHeadAnnotation(annotation)
	if err != nil {
		return err
	}
	reader, head, err := rootfsreader.NewForHead(ctx, rootfsreader.ReaderConfig{
		Store:               s.store,
		ObjectCache:         s.objects,
		SharedMetadataCache: s.metadata,
	}, reference)
	if err != nil {
		return err
	}
	if declared := strings.TrimSpace(info.Labels[rootfshead.LabelBaseChainID]); declared != head.Base.ChainID {
		return fmt.Errorf("rootfs Head base %q conflicts with snapshot label %q", head.Base.ChainID, declared)
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
		return fmt.Errorf("rootfs Head snapshot %s has no overlay upperdir", info.Name)
	}
	mounted, err := s.mount(upperdir, reader, head)
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

func (s *Snapshotter) unmountIfUnused(head string) error {
	head = strings.TrimSpace(head)
	if head == "" {
		return nil
	}
	s.mu.Lock()
	if s.headInUseLocked(head) {
		s.mu.Unlock()
		return nil
	}
	mounted := s.mounted[head]
	delete(s.mounted, head)
	s.mu.Unlock()
	if mounted == nil {
		return nil
	}
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

func (s *Snapshotter) headInUseLocked(head string) bool {
	for _, parent := range s.children {
		if parent == head {
			return true
		}
	}
	return false
}

func (s *Snapshotter) begin() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return false
	}
	s.ops.Add(1)
	return true
}

func (s *Snapshotter) lockHead(name string) func() {
	s.mu.Lock()
	lock := s.headLocks[name]
	if lock == nil {
		lock = &headLock{}
		s.headLocks[name] = lock
	}
	lock.refs++
	s.mu.Unlock()
	lock.mutex.Lock()
	return func() {
		lock.mutex.Unlock()
		s.mu.Lock()
		lock.refs--
		if lock.refs == 0 && s.headLocks[name] == lock {
			delete(s.headLocks, name)
		}
		s.mu.Unlock()
	}
}

func mergeLabels(existing map[string]string, opts []snapshots.Opt) (map[string]string, error) {
	info := snapshots.Info{Labels: cloneLabels(existing)}
	for _, opt := range opts {
		if opt != nil {
			if err := opt(&info); err != nil {
				return nil, err
			}
		}
	}
	return info.Labels, nil
}

func cloneLabels(values map[string]string) map[string]string {
	cloned := make(map[string]string, len(values))
	for key, value := range values {
		cloned[key] = value
	}
	return cloned
}

type fuseMount struct {
	server *fuse.Server
	done   chan struct{}
}

func mountFUSEHead(target string, reader *rootfsreader.Reader, head rootfshead.Head) (mountedHead, error) {
	// A crashed snapshotter can leave a disconnected FUSE mount propagated to
	// the host. This target belongs exclusively to the committed Head snapshot,
	// so detach any untracked predecessor before reconnecting it.
	if err := unix.Unmount(target, unix.MNT_DETACH); err != nil && !errors.Is(err, syscall.EINVAL) && !errors.Is(err, syscall.ENOENT) {
		return nil, fmt.Errorf("detach stale rootfs FUSE Head: %w", err)
	}
	server, err := rootfsfuse.Mount(target, reader, head)
	if err != nil {
		return nil, err
	}
	mounted := &fuseMount{server: server, done: make(chan struct{})}
	go func() {
		server.Wait()
		close(mounted.done)
	}()
	directory, err := os.Open(target)
	if err == nil {
		_, readErr := directory.Readdirnames(1)
		if !errors.Is(readErr, io.EOF) {
			err = readErr
		}
		err = errors.Join(err, directory.Close())
	}
	if err != nil {
		return nil, errors.Join(fmt.Errorf("probe rootfs FUSE Head: %w", err), mounted.Unmount())
	}
	return mounted, nil
}

func (m *fuseMount) Unmount() error {
	if m == nil || m.server == nil {
		return nil
	}
	return m.server.Unmount()
}

func (m *fuseMount) HealthError() error {
	if m == nil {
		return fmt.Errorf("rootfs FUSE mount is nil")
	}
	select {
	case <-m.done:
		return fmt.Errorf("rootfs FUSE server stopped")
	default:
		return nil
	}
}

var _ snapshots.Snapshotter = (*Snapshotter)(nil)
var _ snapshots.Cleaner = (*Snapshotter)(nil)

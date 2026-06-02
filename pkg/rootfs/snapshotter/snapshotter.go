package snapshotter

import (
	"context"
	"fmt"
	"strings"

	"github.com/containerd/containerd/v2/core/mount"
	"github.com/containerd/containerd/v2/core/snapshots"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfs"
)

// MetadataResolver resolves Sandbox0 rootfs metadata for a containerd snapshot key.
type MetadataResolver interface {
	ResolveRootFSMetadata(ctx context.Context, snapshotKey string) (rootfs.Metadata, bool, error)
}

// Snapshotter delegates snapshot lifecycle to a base snapshotter and rewrites
// rootfs overlay mounts to use an s0fs-backed upperdir when Sandbox0 metadata
// requests it.
type Snapshotter struct {
	base        snapshots.Snapshotter
	resolver    MetadataResolver
	client      rootfs.PrepareClient
	ctldAddress string
	namespace   string
	mounter     OverlayMounter
}

type Option func(*Snapshotter)

func WithCtldAddress(address string) Option {
	return func(s *Snapshotter) {
		s.ctldAddress = strings.TrimSpace(address)
	}
}

func WithNamespace(namespace string) Option {
	return func(s *Snapshotter) {
		s.namespace = strings.TrimSpace(namespace)
	}
}

func WithOverlayMounter(mounter OverlayMounter) Option {
	return func(s *Snapshotter) {
		s.mounter = mounter
	}
}

func New(base snapshots.Snapshotter, resolver MetadataResolver, client rootfs.PrepareClient, opts ...Option) (*Snapshotter, error) {
	if base == nil {
		return nil, fmt.Errorf("base snapshotter is required")
	}
	s := &Snapshotter{
		base:     base,
		resolver: resolver,
		client:   client,
		mounter:  NewFuseOverlayMounter(""),
	}
	for _, opt := range opts {
		if opt != nil {
			opt(s)
		}
	}
	return s, nil
}

func (s *Snapshotter) Stat(ctx context.Context, key string) (snapshots.Info, error) {
	ctx = s.ensureNamespace(ctx)
	return s.base.Stat(ctx, key)
}

func (s *Snapshotter) Update(ctx context.Context, info snapshots.Info, fieldpaths ...string) (snapshots.Info, error) {
	ctx = s.ensureNamespace(ctx)
	return s.base.Update(ctx, info, fieldpaths...)
}

func (s *Snapshotter) Usage(ctx context.Context, key string) (snapshots.Usage, error) {
	ctx = s.ensureNamespace(ctx)
	return s.base.Usage(ctx, key)
}

func (s *Snapshotter) Prepare(ctx context.Context, key, parent string, opts ...snapshots.Opt) ([]mount.Mount, error) {
	ctx = s.ensureNamespace(ctx)
	mounts, err := s.base.Prepare(ctx, key, parent, opts...)
	if err != nil {
		return nil, err
	}
	return s.rewriteRootFSMounts(ctx, key, mounts)
}

func (s *Snapshotter) View(ctx context.Context, key, parent string, opts ...snapshots.Opt) ([]mount.Mount, error) {
	ctx = s.ensureNamespace(ctx)
	return s.base.View(ctx, key, parent, opts...)
}

func (s *Snapshotter) Commit(ctx context.Context, name, key string, opts ...snapshots.Opt) error {
	ctx = s.ensureNamespace(ctx)
	return s.base.Commit(ctx, name, key, opts...)
}

func (s *Snapshotter) Remove(ctx context.Context, key string) error {
	ctx = s.ensureNamespace(ctx)
	if s.mounter != nil {
		if err := s.mounter.Unmount(ctx, key); err != nil {
			return err
		}
	}
	return s.base.Remove(ctx, key)
}

func (s *Snapshotter) Walk(ctx context.Context, fn snapshots.WalkFunc, filters ...string) error {
	ctx = s.ensureNamespace(ctx)
	return s.base.Walk(ctx, fn, filters...)
}

func (s *Snapshotter) Close() error {
	var closeErr error
	if s.mounter != nil {
		closeErr = s.mounter.Close()
	}
	baseErr := s.base.Close()
	if closeErr != nil {
		return closeErr
	}
	return baseErr
}

func (s *Snapshotter) Mounts(ctx context.Context, key string) ([]mount.Mount, error) {
	ctx = s.ensureNamespace(ctx)
	mounts, err := s.base.Mounts(ctx, key)
	if err != nil {
		return nil, err
	}
	return s.rewriteRootFSMounts(ctx, key, mounts)
}

func (s *Snapshotter) rewriteRootFSMounts(ctx context.Context, key string, mounts []mount.Mount) ([]mount.Mount, error) {
	if s.resolver == nil {
		return mounts, nil
	}
	meta, ok, err := s.resolver.ResolveRootFSMetadata(ctx, key)
	if err != nil {
		return nil, err
	}
	if !ok || !meta.UsesS0FSUpperdir() {
		return mounts, nil
	}
	result, err := rootfs.PrepareAndRewriteOverlayMounts(ctx, s.client, s.ctldAddress, meta, toRootFSMounts(mounts))
	if err != nil {
		return nil, err
	}
	if result.Rewritten && result.RootFS != nil && s.mounter != nil {
		idx := firstOverlayMountIndex(result.Mounts)
		if idx < 0 {
			return nil, fmt.Errorf("rewritten overlay rootfs mount is required")
		}
		merged, err := s.mounter.Mount(ctx, key, result.Mounts[idx], result.RootFS)
		if err != nil {
			return nil, err
		}
		result.Mounts[idx] = merged
	}
	return applyRootFSMounts(mounts, result.Mounts), nil
}

func (s *Snapshotter) Cleanup(ctx context.Context) error {
	cleaner, ok := s.base.(snapshots.Cleaner)
	if !ok {
		return nil
	}
	ctx = s.ensureNamespace(ctx)
	return cleaner.Cleanup(ctx)
}

func (s *Snapshotter) ensureNamespace(ctx context.Context) context.Context {
	if s == nil {
		return ensureContainerdNamespace(ctx, "")
	}
	return ensureContainerdNamespace(ctx, s.namespace)
}

func toRootFSMounts(mounts []mount.Mount) []rootfs.Mount {
	if len(mounts) == 0 {
		return nil
	}
	out := make([]rootfs.Mount, len(mounts))
	for i := range mounts {
		out[i] = rootfs.Mount{
			Type:    mounts[i].Type,
			Source:  mounts[i].Source,
			Options: append([]string(nil), mounts[i].Options...),
		}
	}
	return out
}

func applyRootFSMounts(original []mount.Mount, rewritten []rootfs.Mount) []mount.Mount {
	if len(original) == 0 {
		return nil
	}
	out := make([]mount.Mount, len(original))
	copy(out, original)
	for i := range out {
		if i >= len(rewritten) {
			continue
		}
		out[i].Type = rewritten[i].Type
		out[i].Source = rewritten[i].Source
		out[i].Options = append([]string(nil), rewritten[i].Options...)
	}
	return out
}

func firstOverlayMountIndex(mounts []rootfs.Mount) int {
	for i := range mounts {
		if strings.TrimSpace(mounts[i].Type) == "overlay" {
			return i
		}
	}
	return -1
}

package rootfs

import (
	"context"
	"fmt"
	"testing"

	"github.com/containerd/containerd/v2/core/mount"
	"github.com/containerd/containerd/v2/core/snapshots"
	"github.com/containerd/errdefs"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshead"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEnsureRootFSHeadSnapshotCommitsExplicitMarker(t *testing.T) {
	snapshotter := newMarkerSnapshotter()
	snapshotter.infos["base"] = snapshots.Info{Name: "base", Kind: snapshots.KindCommitted}

	require.NoError(t, ensureRootFSHeadSnapshot(context.Background(), snapshotter, "head", "base", "annotation", "active"))
	info := snapshotter.infos["head"]
	assert.Equal(t, snapshots.KindCommitted, info.Kind)
	assert.Equal(t, "base", info.Parent)
	assert.Equal(t, "annotation", info.Labels[rootfshead.AnnotationHead])
	assert.Equal(t, "base", info.Labels[rootfshead.LabelBaseChainID])
	assert.NotContains(t, snapshotter.infos, "active")
	assert.Equal(t, 1, snapshotter.prepareCalls)
	assert.Equal(t, 1, snapshotter.commitCalls)
}

func TestEnsureRootFSHeadSnapshotIsIdempotent(t *testing.T) {
	snapshotter := newMarkerSnapshotter()
	snapshotter.infos["head"] = snapshots.Info{
		Name:   "head",
		Kind:   snapshots.KindCommitted,
		Parent: "base",
		Labels: map[string]string{
			rootfshead.AnnotationHead:   "annotation",
			rootfshead.LabelBaseChainID: "base",
		},
	}

	require.NoError(t, ensureRootFSHeadSnapshot(context.Background(), snapshotter, "head", "base", "annotation", "active"))
	assert.Zero(t, snapshotter.prepareCalls)
	assert.Zero(t, snapshotter.commitCalls)
}

func TestEnsureRootFSHeadSnapshotRejectsMissingBaseMetadata(t *testing.T) {
	snapshotter := newMarkerSnapshotter()
	snapshotter.infos["head"] = snapshots.Info{
		Name:   "head",
		Kind:   snapshots.KindCommitted,
		Parent: "base",
		Labels: map[string]string{rootfshead.AnnotationHead: "annotation"},
	}

	err := ensureRootFSHeadSnapshot(context.Background(), snapshotter, "head", "base", "annotation", "active")
	assert.ErrorContains(t, err, "conflicting base metadata")
	assert.Zero(t, snapshotter.prepareCalls)
}

func TestEnsureRootFSHeadSnapshotRejectsConflictingExistingSnapshot(t *testing.T) {
	snapshotter := newMarkerSnapshotter()
	snapshotter.infos["head"] = snapshots.Info{Name: "head", Kind: snapshots.KindCommitted}

	err := ensureRootFSHeadSnapshot(context.Background(), snapshotter, "head", "base", "annotation", "active")
	assert.ErrorContains(t, err, "has parent")
	assert.Zero(t, snapshotter.prepareCalls)
}

func TestEnsureRootFSHeadSnapshotHandlesConcurrentCommit(t *testing.T) {
	snapshotter := newMarkerSnapshotter()
	snapshotter.commitRace = &snapshots.Info{
		Name:   "head",
		Kind:   snapshots.KindCommitted,
		Parent: "base",
		Labels: map[string]string{
			rootfshead.AnnotationHead:   "annotation",
			rootfshead.LabelBaseChainID: "base",
		},
	}

	require.NoError(t, ensureRootFSHeadSnapshot(context.Background(), snapshotter, "head", "base", "annotation", "active"))
	assert.NotContains(t, snapshotter.infos, "active")
	assert.Equal(t, 1, snapshotter.removeCalls)
}

type markerSnapshotter struct {
	snapshots.Snapshotter
	infos        map[string]snapshots.Info
	commitRace   *snapshots.Info
	prepareCalls int
	commitCalls  int
	removeCalls  int
}

func newMarkerSnapshotter() *markerSnapshotter {
	return &markerSnapshotter{infos: make(map[string]snapshots.Info)}
}

func (s *markerSnapshotter) Stat(_ context.Context, key string) (snapshots.Info, error) {
	info, ok := s.infos[key]
	if !ok {
		return snapshots.Info{}, fmt.Errorf("snapshot %s: %w", key, errdefs.ErrNotFound)
	}
	return info, nil
}

func (s *markerSnapshotter) Prepare(_ context.Context, key, parent string, opts ...snapshots.Opt) ([]mount.Mount, error) {
	s.prepareCalls++
	if _, ok := s.infos[key]; ok {
		return nil, errdefs.ErrAlreadyExists
	}
	info := snapshots.Info{Name: key, Kind: snapshots.KindActive, Parent: parent}
	for _, opt := range opts {
		if err := opt(&info); err != nil {
			return nil, err
		}
	}
	s.infos[key] = info
	return nil, nil
}

func (s *markerSnapshotter) Commit(_ context.Context, name, key string, opts ...snapshots.Opt) error {
	s.commitCalls++
	if s.commitRace != nil {
		s.infos[name] = *s.commitRace
		s.commitRace = nil
		return errdefs.ErrAlreadyExists
	}
	active, ok := s.infos[key]
	if !ok {
		return errdefs.ErrNotFound
	}
	if _, ok := s.infos[name]; ok {
		return errdefs.ErrAlreadyExists
	}
	active.Name = name
	active.Kind = snapshots.KindCommitted
	for _, opt := range opts {
		if err := opt(&active); err != nil {
			return err
		}
	}
	delete(s.infos, key)
	s.infos[name] = active
	return nil
}

func (s *markerSnapshotter) Remove(_ context.Context, key string) error {
	s.removeCalls++
	if _, ok := s.infos[key]; !ok {
		return errdefs.ErrNotFound
	}
	delete(s.infos, key)
	return nil
}

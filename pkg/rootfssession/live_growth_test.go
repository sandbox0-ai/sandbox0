package session

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/containerd/errdefs"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsblock"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshandoff"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/rootfswriterauthority"
	"github.com/stretchr/testify/require"
)

type fakeGrowthHost struct {
	*fakeHostRuntime
	muGrowth      sync.Mutex
	usage         XFSUsage
	device, mount string
	grow          func(context.Context, int64) error
}

func (r *fakeGrowthHost) InspectXFS(device, mount string) (XFSUsage, error) {
	r.muGrowth.Lock()
	defer r.muGrowth.Unlock()
	if device != r.device || mount != r.mount {
		return XFSUsage{}, errors.New("wrong exact growth mount or device")
	}
	return r.usage, nil
}
func (r *fakeGrowthHost) GrowXFS(ctx context.Context, device, mount string, target int64) (XFSUsage, error) {
	if _, err := r.InspectXFS(device, mount); err != nil {
		return XFSUsage{}, err
	}
	if r.grow != nil {
		if err := r.grow(ctx, target); err != nil {
			return XFSUsage{}, err
		}
	}
	r.muGrowth.Lock()
	r.usage.SizeBytes = target
	usage := r.usage
	r.muGrowth.Unlock()
	return usage, nil
}

func newLiveGrowthFixture(t *testing.T, name string) (*Manager, *fakeGrowthHost, *nodeLiveAuthority, rootfshandoff.StageRequest, *rootfsblock.Branch) {
	t.Helper()
	base := t.TempDir()
	objects := newSessionObjectStore()
	runtime := &fakeGrowthHost{fakeHostRuntime: newFakeHostRuntime(objects), usage: XFSUsage{SizeBytes: rootfsblock.LogicalBlockSize, BlockBytes: rootfsblock.LogicalBlockSize}}
	stage := testStageRequest(t, objects, name)
	authority := &nodeLiveAuthority{state: "consumed", progress: protocol.LiveCommitProgress{CommittedGenerationID: stage.InitialGeneration, InstalledGenerationID: stage.InitialGeneration}}
	m, err := New(Config{StatePath: filepath.Join(base, "state.db"), BranchRoot: filepath.Join(base, "branches"), MountRoot: filepath.Join(base, "mounts"),
		MaxDirtyTailBytes: rootfsblock.LogicalBlockSize, DirtyTailRetirementReserveBytes: rootfsblock.LogicalBlockSize,
		Source: objects, Publisher: objects, Runtime: runtime, LiveCommitAuthority: authority})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, m.Close()) })
	_, err = m.Ensure(t.Context(), stage)
	require.NoError(t, err)
	stored, err := m.load(stage.Parent)
	require.NoError(t, err)
	runtime.device, runtime.mount = stored.DevicePath, stored.XFSRoot
	return m, runtime, authority, stage, m.live[stage.Parent].branch
}

func TestLiveGrowthKeepsReclaimAliveAfterControlCancellation(t *testing.T) {
	m, runtime, authority, stage, branch := newLiveGrowthFixture(t, "growth-cancel-pressure")
	_, err := branch.WriteAt([]byte("before"), 0)
	require.NoError(t, err)
	started := make(chan struct{})
	runtime.grow = func(_ context.Context, _ int64) error {
		close(started)
		// Emulate the kernel: cancellation does not interrupt this admitted
		// ioctl, whose metadata write blocks on the full normal WAL budget.
		_, err := branch.WriteAt([]byte("metadata"), rootfsblock.LogicalBlockSize)
		return err
	}
	authority.failObserve = true
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		grew, err := m.GrowLiveXFS(ctx, stage)
		if err == nil && !grew {
			err = errors.New("growth not performed")
		}
		done <- err
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("growth did not start")
	}
	require.Eventually(t, func() bool { return branch.DirtyTailPressure() != nil }, time.Second, time.Millisecond)
	require.ErrorIs(t, m.BeginRetire(stage.Parent, stage.Identity, "pause"), errdefs.ErrUnavailable)
	require.ErrorIs(t, m.Release(t.Context(), stage.Identity), errdefs.ErrUnavailable)
	_, err = m.PrepareLiveHandoff(t.Context())
	require.Error(t, err, "a live kernel grow cannot transfer its mount")
	cancel()
	authority.mu.Lock()
	authority.failObserve = false
	authority.mu.Unlock()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("cancelled control stranded the grow reclaim path")
	}
	require.Same(t, branch, m.live[stage.Parent].branch)
	data := make([]byte, 8)
	_, err = branch.ReadAt(data, rootfsblock.LogicalBlockSize)
	require.NoError(t, err)
	require.Equal(t, "metadata", string(data))
	stored, err := m.load(stage.Parent)
	require.NoError(t, err)
	require.Equal(t, stage.InitialGeneration, stored.Stage.InitialGeneration)
	require.NotEqual(t, stage.InitialGeneration, stored.LiveCommit.InstalledGenerationID)
	require.Equal(t, int64(rootfsblock.LogicalBlockSize), branch.DirtyTailUsage().DirtyBytes)
	m.mu.Lock()
	require.Empty(t, m.growing)
	m.mu.Unlock()
}

func TestLiveGrowthCanOverlapCommitButRejectsOtherCaptureAndWrongBinding(t *testing.T) {
	m, runtime, authority, stage, branch := newLiveGrowthFixture(t, "growth-live-cut")
	_, err := branch.WriteAt([]byte("cut"), 0)
	require.NoError(t, err)
	publishing, finishPublish := make(chan struct{}), make(chan struct{})
	var publishOnce sync.Once
	releasePublish := func() { publishOnce.Do(func() { close(finishPublish) }) }
	defer releasePublish()
	authority.beforePublish = func(LiveCommitPublication) { close(publishing); <-finishPublish }
	commitDone := make(chan error, 1)
	go func() { commitDone <- m.CommitLive(t.Context(), stage) }()
	select {
	case <-publishing:
	case <-time.After(time.Second):
		t.Fatal("commit did not start")
	}
	stages, err := m.GrowthSessions()
	require.NoError(t, err)
	require.Len(t, stages, 1, "live materialization does not freeze XFS")
	started, finishGrowth := make(chan struct{}), make(chan struct{})
	var growthOnce sync.Once
	releaseGrowth := func() { growthOnce.Do(func() { close(finishGrowth) }) }
	defer releaseGrowth()
	runtime.grow = func(context.Context, int64) error { close(started); <-finishGrowth; return nil }
	growthDone := make(chan error, 1)
	go func() { _, err := m.GrowLiveXFS(t.Context(), stage); growthDone <- err }()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("live commit blocked growth admission")
	}
	releasePublish()
	select {
	case err := <-commitDone:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("growth held a lock required by commit installation")
	}
	releaseGrowth()
	require.NoError(t, <-growthDone)
	m.mu.Lock()
	m.captures[stage.Parent] = true
	m.mu.Unlock()
	stages, err = m.GrowthSessions()
	require.NoError(t, err)
	require.Empty(t, stages)
	_, err = m.GrowLiveXFS(t.Context(), stage)
	require.ErrorIs(t, err, errdefs.ErrUnavailable)
	m.mu.Lock()
	delete(m.captures, stage.Parent)
	m.mu.Unlock()
	other := stage
	other.Identity.WriterGrantID = "foreign"
	_, err = m.GrowLiveXFS(t.Context(), other)
	require.ErrorIs(t, err, errdefs.ErrFailedPrecondition)
}

func TestLiveGrowthUnexpectedOwnerCloseWakesBlockedKernelWrite(t *testing.T) {
	m, runtime, authority, stage, branch := newLiveGrowthFixture(t, "growth-owner-close")
	_, err := branch.WriteAt([]byte("full"), 0)
	require.NoError(t, err)
	authority.failObserve = true
	runtime.grow = func(context.Context, int64) error {
		_, err := branch.WriteAt([]byte("blocked"), rootfsblock.LogicalBlockSize)
		return err
	}
	growthDone := make(chan error, 1)
	go func() { _, err := m.GrowLiveXFS(t.Context(), stage); growthDone <- err }()
	require.Eventually(t, func() bool { return branch.DirtyTailPressure() != nil }, time.Second, time.Millisecond)
	closed := make(chan error, 1)
	go func() { closed <- m.Close() }()
	select {
	case err := <-growthDone:
		require.ErrorIs(t, err, os.ErrClosed)
	case <-time.After(time.Second):
		t.Fatal("close did not release blocked growth write")
	}
	select {
	case err := <-closed:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("close waited indefinitely for growth")
	}
}

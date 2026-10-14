package session

import (
	"bytes"
	"context"
	"sync"
	"testing"
	"time"

	"github.com/containerd/errdefs"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsblock"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshandoff"
	"github.com/stretchr/testify/require"
)

type liveFreezeHost struct {
	*fakeHostRuntime
	freeze func() error
}

func (r *liveFreezeHost) FreezeXFS(string) error {
	r.record("freeze-xfs")
	return r.freeze()
}

func liveFreezeRequest(stage rootfshandoff.StageRequest) rootfshandoff.RunningForkCheckpointRequest {
	return rootfshandoff.RunningForkCheckpointRequest{OperationID: "live-freeze-fork", SourceSandboxID: stage.Identity.AllocationID,
		TargetSandboxID: "live-freeze-target", TargetGenerationID: "live-freeze-generation"}
}

func TestRunningForkFreezeReclaimsFullWALAndCapturesInstalledBase(t *testing.T) {
	m, host, authority, stage, branch := newLiveGrowthFixture(t, "freeze-reclaim")
	original := bytes.Repeat([]byte{23}, rootfsblock.LogicalBlockSize)
	_, err := branch.WriteAt(original, 0)
	require.NoError(t, err)
	started, flushed, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var once sync.Once
	releaseFreeze := func() { once.Do(func() { close(release) }) }
	defer releaseFreeze()
	m.runtime = &liveFreezeHost{fakeHostRuntime: host.fakeHostRuntime, freeze: func() error {
		close(started)
		_, err := branch.WriteAt([]byte("metadata"), rootfsblock.LogicalBlockSize)
		close(flushed)
		<-release
		return err
	}}
	authority.lostPublish, authority.lostInstall = true, true
	type result struct {
		checkpoint rootfshandoff.RunningForkCheckpointResult
		err        error
	}
	done := make(chan result, 1)
	go func() {
		checkpoint, err := m.CaptureRunningFork(t.Context(), stage, liveFreezeRequest(stage))
		done <- result{checkpoint, err}
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("freeze did not start")
	}
	require.Eventually(t, func() bool { return branch.DirtyTailPressure() != nil }, time.Second, time.Millisecond)
	require.NoError(t, m.ReconcileFreezes(t.Context()))
	require.Zero(t, host.count("thaw-xfs"), "recovery must not thaw an owned drain")
	require.NoError(t, m.ReconcileRunningForkCaptures(t.Context()))
	require.ErrorIs(t, m.BeginRetire(stage.Parent, stage.Identity, "pause"), errdefs.ErrUnavailable)
	_, err = m.PrepareLiveHandoff(t.Context())
	require.Error(t, err)
	select {
	case <-flushed:
	case <-time.After(8 * time.Second):
		t.Fatal("freeze held custody needed to reclaim its full WAL")
	}
	releaseFreeze()
	var captured result
	select {
	case captured = <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("freeze did not settle its accepted live cut")
	}
	require.NoError(t, captured.err)
	require.NoError(t, captured.checkpoint.Validate())
	require.Equal(t, uint64(2), captured.checkpoint.Proof.CheckpointSequence)
	descriptor, err := rootfsblock.DecodeDescriptor(captured.checkpoint.Generation.Descriptor)
	require.NoError(t, err)
	reader, err := rootfsblock.NewReader(m.source, descriptor, rootfsblock.DefaultReadCacheBytes)
	require.NoError(t, err)
	data := make([]byte, len(original))
	_, err = reader.ReadAt(data, 0)
	require.NoError(t, err)
	require.Equal(t, original, data, "rotated base must be included in the checkpoint")
	_, err = reader.ReadAt(data[:8], rootfsblock.LogicalBlockSize)
	require.NoError(t, err)
	require.Equal(t, "metadata", string(data[:8]))
	current, err := m.load(stage.Parent)
	require.NoError(t, err)
	require.Equal(t, stage.InitialGeneration, current.Stage.InitialGeneration)
	require.NotEqual(t, stage.InitialGeneration, current.LiveCommit.InstalledGenerationID)
	require.Nil(t, current.LiveCommit.Pending)
	require.Empty(t, current.FreezeOperationID)
	require.Equal(t, 1, host.count("thaw-xfs"))
	m.mu.Lock()
	require.Empty(t, m.captures)
	require.Empty(t, m.freezes)
	m.mu.Unlock()
}

func TestRunningForkCancelledRPCStillReclaimsAndThawsKernelDrain(t *testing.T) {
	m, host, authority, stage, branch := newLiveGrowthFixture(t, "freeze-cancel")
	_, err := branch.WriteAt([]byte("full"), 0)
	require.NoError(t, err)
	started := make(chan struct{})
	m.runtime = &liveFreezeHost{fakeHostRuntime: host.fakeHostRuntime, freeze: func() error {
		close(started)
		_, err := branch.WriteAt([]byte("metadata"), rootfsblock.LogicalBlockSize)
		return err
	}}
	authority.failObserve = true
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := m.CaptureRunningFork(ctx, stage, liveFreezeRequest(stage)); done <- err }()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("freeze did not start")
	}
	require.Eventually(t, func() bool { return branch.DirtyTailPressure() != nil }, time.Second, time.Millisecond)
	cancel()
	authority.mu.Lock()
	authority.failObserve = false
	authority.mu.Unlock()
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(5 * time.Second):
		t.Fatal("cancelled RPC stranded kernel flush")
	}
	require.Equal(t, 1, host.count("thaw-xfs"))
	current, err := m.load(stage.Parent)
	require.NoError(t, err)
	require.Nil(t, current.RunningForkRequest)
	require.Empty(t, current.FreezeOperationID)
	require.Same(t, branch, m.live[stage.Parent].branch)
}

func TestRunningForkOwnerCloseWakesBlockedKernelDrain(t *testing.T) {
	m, host, authority, stage, branch := newLiveGrowthFixture(t, "freeze-owner-close")
	_, err := branch.WriteAt([]byte("full"), 0)
	require.NoError(t, err)
	m.runtime = &liveFreezeHost{fakeHostRuntime: host.fakeHostRuntime, freeze: func() error {
		_, err := branch.WriteAt([]byte("metadata"), rootfsblock.LogicalBlockSize)
		return err
	}}
	authority.failObserve = true
	capture := make(chan error, 1)
	go func() { _, err := m.CaptureRunningFork(t.Context(), stage, liveFreezeRequest(stage)); capture <- err }()
	require.Eventually(t, func() bool { return branch.DirtyTailPressure() != nil }, time.Second, time.Millisecond)
	closed := make(chan error, 1)
	go func() { closed <- m.Close() }()
	select {
	case err := <-closed:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("owner close joined a freeze before waking its blocked writes")
	}
	require.Error(t, <-capture)
	require.Equal(t, 1, host.count("thaw-xfs"))
}

func TestRunningForkFailedLiveSettlementThawsAndRetainsPendingCut(t *testing.T) {
	m, host, authority, stage, branch := newLiveGrowthFixture(t, "freeze-settle-failure")
	_, err := branch.WriteAt([]byte("original"), 0)
	require.NoError(t, err)
	authority.lostInstall = true
	runtime := &liveFreezeHost{fakeHostRuntime: host.fakeHostRuntime, freeze: func() error {
		_, err := branch.WriteAt([]byte("metadata"), rootfsblock.LogicalBlockSize)
		authority.mu.Lock()
		authority.failObserve = true
		authority.mu.Unlock()
		return err
	}}
	m.runtime = runtime
	_, err = m.CaptureRunningFork(t.Context(), stage, liveFreezeRequest(stage))
	require.ErrorIs(t, err, errdefs.ErrUnavailable)
	require.Equal(t, 1, host.count("thaw-xfs"))
	current, err := m.load(stage.Parent)
	require.NoError(t, err)
	require.Nil(t, current.RunningForkRequest)
	require.Empty(t, current.FreezeOperationID)
	require.NotNil(t, current.LiveCommit.Pending, "failed fork must preserve the ambiguous installation")
	authority.mu.Lock()
	authority.failObserve = false
	authority.mu.Unlock()
	require.NoError(t, m.SettleLiveCommit(t.Context(), stage))
	data := make([]byte, 8)
	_, err = branch.ReadAt(data, 0)
	require.NoError(t, err)
	require.Equal(t, "original", string(data))
	_, err = branch.ReadAt(data, rootfsblock.LogicalBlockSize)
	require.NoError(t, err)
	require.Equal(t, "metadata", string(data))
	runtime.freeze = func() error { return nil }
	checkpoint, err := m.CaptureRunningFork(t.Context(), stage, liveFreezeRequest(stage))
	require.NoError(t, err)
	require.NoError(t, checkpoint.Validate())
}

func TestRunningForkRecoveredIntentCannotAuthorizeLiveCuts(t *testing.T) {
	m, host, authority, stage, branch := newLiveGrowthFixture(t, "freeze-recovered-intent")
	_, err := branch.WriteAt([]byte("original"), 0)
	require.NoError(t, err)
	current, err := m.load(stage.Parent)
	require.NoError(t, err)
	request := liveFreezeRequest(stage)
	current.RunningForkRequest, current.FreezeOperationID = &request, request.OperationID
	require.NoError(t, m.save(current))
	require.ErrorIs(t, m.CommitLive(t.Context(), stage), errdefs.ErrUnavailable,
		"a journaled freeze is not proof that this owner is draining it")
	require.Empty(t, authority.published)
	require.NoError(t, m.ReconcileFreezes(t.Context()))
	require.Equal(t, 1, host.count("thaw-xfs"))
	require.NoError(t, m.ReconcileRunningForkCaptures(t.Context()))
	require.NoError(t, m.CommitLive(t.Context(), stage))
}

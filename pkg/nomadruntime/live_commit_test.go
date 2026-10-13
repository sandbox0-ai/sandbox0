package nomadruntime

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/containerd/errdefs"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshandoff"
	rootfssession "github.com/sandbox0-ai/sandbox0/pkg/rootfssession"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

type scheduledLiveRuntime struct {
	*fakeRootFSRuntime
	enabled  bool
	sessions []rootfssession.LiveCommitSession
	started  chan string
	block    bool
	err      error
	muLive   sync.Mutex
	commits  []string
}

func (r *scheduledLiveRuntime) LiveCommitsEnabled() bool { return r.enabled }
func (r *scheduledLiveRuntime) LiveCommitSessions(time.Time) ([]rootfssession.LiveCommitSession, error) {
	return r.sessions, nil
}
func (r *scheduledLiveRuntime) CommitLiveRootFS(ctx context.Context, stage rootfshandoff.StageRequest) error {
	r.muLive.Lock()
	r.commits = append(r.commits, stage.Parent)
	r.muLive.Unlock()
	if r.started != nil {
		r.started <- stage.Parent
	}
	if r.block {
		<-ctx.Done()
		return ctx.Err()
	}
	return r.err
}

func liveSchedulerFixture(count int) (*nodeRuntime, *scheduledLiveRuntime) {
	r := &scheduledLiveRuntime{fakeRootFSRuntime: &fakeRootFSRuntime{}, enabled: true, started: make(chan string, count*4)}
	for i := range count {
		r.sessions = append(r.sessions, rootfssession.LiveCommitSession{Stage: rootfshandoff.StageRequest{Parent: fmt.Sprintf("parent-%02d", i)}, DirtyBytes: rootfssession.LiveCommitDirtyBytes})
	}
	d := &nodeRuntime{runtime: r, logger: newLogger(zap.NewNop()), trigger: make(chan string, 1)}
	return d, r
}

func TestLiveCommitSchedulerBoundsWorkersAndDoesNotPlanPressureRetirement(t *testing.T) {
	d, r := liveSchedulerFixture(10)
	r.block = true
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	d.scanLiveCommits(ctx)
	for range liveCommitConcurrency {
		select {
		case <-r.started:
		case <-time.After(time.Second):
			t.Fatal("worker did not start")
		}
	}
	d.scanLiveCommits(ctx)
	d.scanDirtyTailPressures(ctx)
	d.mu.Lock()
	require.Equal(t, liveCommitConcurrency, d.liveCommitActive)
	require.Len(t, d.inflight, liveCommitConcurrency)
	d.mu.Unlock()
	r.muLive.Lock()
	require.Len(t, r.commits, liveCommitConcurrency)
	r.muLive.Unlock()
	r.mu.Lock()
	require.Empty(t, r.pressurePlans)
	r.mu.Unlock()
	select {
	case <-d.trigger:
		t.Fatal("live pressure must not request retirement")
	default:
	}
	cancel()
	d.wg.Wait()
	d.mu.Lock()
	require.Zero(t, d.liveCommitActive)
	require.Empty(t, d.inflight)
	d.mu.Unlock()
}

func TestLiveCommitSchedulerFairnessRetryBackoffAndDisablement(t *testing.T) {
	d, r := liveSchedulerFixture(6)
	d.scanLiveCommits(t.Context())
	d.wg.Wait()
	d.scanLiveCommits(t.Context())
	d.wg.Wait()
	r.muLive.Lock()
	seen := make(map[string]bool)
	for _, parent := range r.commits {
		seen[parent] = true
	}
	require.Len(t, seen, 6)
	r.muLive.Unlock()
	d, r = liveSchedulerFixture(1)
	r.err = errdefs.ErrUnavailable
	d.scanLiveCommits(t.Context())
	d.wg.Wait()
	d.scanLiveCommits(t.Context())
	d.wg.Wait()
	r.muLive.Lock()
	require.Len(t, r.commits, 1)
	r.muLive.Unlock()
	d.mu.Lock()
	retry := d.liveCommitRetries["parent-00"]
	require.Equal(t, time.Second, retry.delay)
	retry.next = time.Time{}
	d.liveCommitRetries["parent-00"] = retry
	d.mu.Unlock()
	d.scanLiveCommits(t.Context())
	d.wg.Wait()
	r.muLive.Lock()
	require.Len(t, r.commits, 2)
	r.muLive.Unlock()
	r.enabled = false
	d.scanLiveCommits(t.Context())
	d.wg.Wait()
	r.muLive.Lock()
	require.Len(t, r.commits, 2)
	r.muLive.Unlock()
}

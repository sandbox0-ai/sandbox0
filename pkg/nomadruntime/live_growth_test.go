package nomadruntime

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/containerd/errdefs"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshandoff"
	"github.com/stretchr/testify/require"
)

type scheduledGrowthRuntime struct {
	*scheduledLiveRuntime
	autoGrow  bool
	stages    []rootfshandoff.StageRequest
	muGrowth  sync.Mutex
	inspected map[string]int
	started   chan string
	block     bool
	grew      bool
	err       error
}

func (r *scheduledGrowthRuntime) RootFSAutoGrowEnabled() bool { return r.autoGrow }
func (r *scheduledGrowthRuntime) GrowthSessions() ([]rootfshandoff.StageRequest, error) {
	return r.stages, nil
}
func (r *scheduledGrowthRuntime) GrowLiveRootFS(ctx context.Context, stage rootfshandoff.StageRequest) (bool, error) {
	r.muGrowth.Lock()
	r.inspected[stage.Parent]++
	r.muGrowth.Unlock()
	if r.started != nil {
		r.started <- stage.Parent
	}
	if r.block {
		<-ctx.Done()
		return false, ctx.Err()
	}
	return r.grew, r.err
}

func growthSchedulerFixture(count int) (*nodeRuntime, *scheduledGrowthRuntime) {
	d, live := liveSchedulerFixture(count)
	r := &scheduledGrowthRuntime{scheduledLiveRuntime: live, autoGrow: true, inspected: make(map[string]int)}
	for i := range count {
		r.stages = append(r.stages, rootfshandoff.StageRequest{Parent: fmt.Sprintf("parent-%02d", i)})
	}
	d.runtime = r
	return d, r
}

func TestRootFSGrowthWorkersAreBoundedAndIndependentOfCommitLane(t *testing.T) {
	d, r := growthSchedulerFixture(10)
	r.block, r.scheduledLiveRuntime.block = true, true
	r.started = make(chan string, 10)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	d.scanRootFSGrowth(ctx)
	for range rootFSGrowthConcurrency {
		select {
		case <-r.started:
		case <-time.After(time.Second):
			t.Fatal("growth worker did not start")
		}
	}
	d.scanRootFSGrowth(ctx)
	d.scanLiveCommits(ctx)
	for range liveCommitConcurrency {
		select {
		case <-r.scheduledLiveRuntime.started:
		case <-time.After(time.Second):
			t.Fatal("growth occupied commit lane")
		}
	}
	d.mu.Lock()
	require.Equal(t, rootFSGrowthConcurrency, d.growthActive)
	require.Equal(t, liveCommitConcurrency, d.liveCommitActive)
	require.Len(t, d.inflight, rootFSGrowthConcurrency+liveCommitConcurrency)
	d.mu.Unlock()
	cancel()
	d.wg.Wait()
	d.mu.Lock()
	require.Zero(t, d.growthActive)
	require.Empty(t, d.inflight)
	d.mu.Unlock()
}

func TestRootFSGrowthSweepVisitsIdleOwnersAndBacksOffPerParent(t *testing.T) {
	d, r := growthSchedulerFixture(100)
	d.scanRootFSGrowth(t.Context())
	d.wg.Wait()
	r.muGrowth.Lock()
	require.Len(t, r.inspected, 100, "idle filesystems must not take one tick per worker slot")
	r.muGrowth.Unlock()
	d, r = growthSchedulerFixture(1)
	r.err = errdefs.ErrUnavailable
	d.scanRootFSGrowth(t.Context())
	d.wg.Wait()
	d.scanRootFSGrowth(t.Context())
	d.wg.Wait()
	r.muGrowth.Lock()
	require.Equal(t, 1, r.inspected["parent-00"])
	r.muGrowth.Unlock()
	d.mu.Lock()
	require.Equal(t, time.Second, d.growthRetries["parent-00"].delay)
	d.mu.Unlock()
	r.stages = nil
	d.scanRootFSGrowth(t.Context())
	d.mu.Lock()
	require.Empty(t, d.growthRetries, "retired owners must not accumulate retry state")
	d.mu.Unlock()
	r.autoGrow = false
	r.stages = []rootfshandoff.StageRequest{{Parent: "disabled"}}
	d.scanRootFSGrowth(t.Context())
	d.wg.Wait()
	r.muGrowth.Lock()
	require.Zero(t, r.inspected["disabled"])
	r.muGrowth.Unlock()
}

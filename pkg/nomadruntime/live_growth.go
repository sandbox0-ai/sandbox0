package nomadruntime

import (
	"context"
	"errors"
	"time"

	"github.com/containerd/errdefs"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshandoff"
	rootfssession "github.com/sandbox0-ai/sandbox0/pkg/rootfssession"
)

type rootFSGrowthRuntime interface {
	RootFSAutoGrowEnabled() bool
	GrowthSessions() ([]rootfshandoff.StageRequest, error)
	GrowLiveRootFS(context.Context, rootfshandoff.StageRequest) (bool, error)
}

func (r *rootfsRuntime) RootFSAutoGrowEnabled() bool { return r != nil && r.autoGrow }
func (r *rootfsRuntime) GrowthSessions() ([]rootfshandoff.StageRequest, error) {
	return r.sessions.GrowthSessions()
}
func (r *rootfsRuntime) GrowLiveRootFS(ctx context.Context, stage rootfshandoff.StageRequest) (bool, error) {
	return r.sessions.GrowLiveXFS(ctx, stage)
}

const rootFSGrowthConcurrency = 2

// Growth uses a separate worker lane: a kernel grow operation blocked on WAL
// admission must not occupy the commit workers needed to unblock it. Each
// worker inspects idle candidates until it performs a growth or exhausts its
// snapshot, so large owner sets do not take one tick per two idle filesystems.
func (d *nodeRuntime) scanRootFSGrowth(ctx context.Context) {
	runtime, ok := d.runtime.(rootFSGrowthRuntime)
	if !ok || !runtime.RootFSAutoGrowEnabled() || ctx.Err() != nil {
		return
	}
	stages, err := runtime.GrowthSessions()
	if err != nil {
		d.logger.Error("list RootFS growth candidates", "error", err)
		return
	}
	d.mu.Lock()
	if d.growthRetries == nil {
		d.growthRetries = make(map[string]liveCommitRetry)
	}
	seen := make(map[string]bool, len(stages))
	for _, stage := range stages {
		seen[stage.Parent] = true
	}
	for parent := range d.growthRetries {
		if !seen[parent] {
			delete(d.growthRetries, parent)
		}
	}
	after := d.growthAfter
	for len(stages) != 0 && d.growthActive < rootFSGrowthConcurrency && ctx.Err() == nil {
		d.growthActive++
		d.wg.Add(1)
		go d.sweepRootFSGrowth(ctx, runtime, stages, after)
	}
	d.mu.Unlock()
}

func (d *nodeRuntime) sweepRootFSGrowth(ctx context.Context, runtime rootFSGrowthRuntime, stages []rootfshandoff.StageRequest, after string) {
	defer d.wg.Done()
	defer func() { d.mu.Lock(); d.growthActive--; d.mu.Unlock() }()
	start := 0
	for i, stage := range stages {
		if stage.Parent > after {
			start = i
			break
		}
	}
	for i := range stages {
		stage := stages[(start+i)%len(stages)]
		parent, key := stage.Parent, "rootfs-growth:"+stage.Parent
		workCtx, cancel := context.WithTimeout(ctx, rootFSSessionReconcileTimeout)
		d.mu.Lock()
		if ctx.Err() != nil {
			d.mu.Unlock()
			cancel()
			return
		}
		if time.Now().Before(d.growthRetries[parent].next) || !d.beginReconciliationLocked(key, cancel) {
			d.mu.Unlock()
			cancel()
			continue
		}
		d.growthAfter = parent
		d.mu.Unlock()
		grew, err := runtime.GrowLiveRootFS(workCtx, stage)
		cancel()
		d.mu.Lock()
		if err == nil {
			delete(d.growthRetries, parent)
		} else {
			retry := d.growthRetries[parent]
			retry.delay = min(rootfssession.LiveCommitInterval, max(time.Second, retry.delay*2))
			retry.next = time.Now().Add(retry.delay)
			d.growthRetries[parent] = retry
		}
		d.endReconciliationLocked(key)
		d.mu.Unlock()
		if err != nil && !errors.Is(err, context.Canceled) && !errdefs.IsUnavailable(err) {
			d.logger.Error("grow live RootFS", "parent", parent, "error", err)
		}
		if grew {
			return
		}
	}
}

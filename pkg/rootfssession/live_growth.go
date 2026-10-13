package session

import (
	"context"
	"encoding/hex"
	"fmt"
	"sort"
	"time"

	"github.com/containerd/errdefs"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsblock"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshandoff"
)

// GrowthSessions returns scheduling hints without issuing a potentially
// blocking filesystem ioctl on the node's control/reconciliation thread.
func (m *Manager) GrowthSessions() ([]rootfshandoff.StageRequest, error) {
	m.mu.Lock()
	parents := make([]string, 0, len(m.live))
	for parent, session := range m.live {
		if session != nil && !m.growing[parent] && (!m.captures[parent] || m.liveCommitting[parent]) && !m.closing && !m.handoff {
			parents = append(parents, parent)
		}
	}
	m.mu.Unlock()
	var stages []rootfshandoff.StageRequest
	for _, parent := range parents {
		current, err := m.load(parent)
		if err != nil {
			return nil, err
		}
		if canGrowLive(current) {
			stages = append(stages, cloneDurableStage(*current.Stage))
		}
	}
	sort.Slice(stages, func(i, j int) bool { return stages[i].Parent < stages[j].Parent })
	return stages, nil
}

func canGrowLive(current record) bool {
	return current.State == stateReady && current.Stage != nil && current.RetireOperationID == "" &&
		current.RunningForkRequest == nil && current.FreezeOperationID == "" && current.Migration == nil && current.CrashFence == nil
}

// GrowLiveXFS owns the exact mount across inspection and growth. It deliberately
// releases the parent mutex before either ioctl: growth metadata can encounter
// WAL admission pressure, and CommitLive must remain able to reclaim that WAL.
// The boolean reports actual growth, allowing bounded workers to inspect many
// idle filesystems in a sweep rather than waiting one tick per filesystem.
func (m *Manager) GrowLiveXFS(ctx context.Context, stage rootfshandoff.StageRequest) (bool, error) {
	stage = stage.WithoutWriterGrantToken()
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if err := stage.ValidateDurableBinding(); err != nil {
		return false, err
	}
	binding, err := stage.BindingDigest()
	if err != nil {
		return false, err
	}
	runtime, ok := m.runtime.(XFSGrowthRuntime)
	if !ok || m.liveCommitAuthority == nil {
		return false, fmt.Errorf("online growth requires host support and live commit authority: %w", errdefs.ErrUnavailable)
	}
	unlock := m.lock(stage.Parent)
	current, err := m.load(stage.Parent)
	if err != nil {
		unlock()
		return false, err
	}
	if !sameBinding(current, stage, hex.EncodeToString(binding[:])) {
		unlock()
		return false, errdefs.ErrFailedPrecondition
	}
	if !canGrowLive(current) {
		unlock()
		return false, errdefs.ErrUnavailable
	}
	m.mu.Lock()
	live := m.live[stage.Parent]
	if live == nil || live.device.Path() != current.DevicePath || m.growing[stage.Parent] ||
		(m.captures[stage.Parent] && !m.liveCommitting[stage.Parent]) || m.closing || m.handoff {
		m.mu.Unlock()
		unlock()
		return false, errdefs.ErrUnavailable
	}
	m.growing[stage.Parent] = true
	m.growthWG.Add(1)
	m.mu.Unlock()
	unlock()
	defer func() {
		m.mu.Lock()
		delete(m.growing, stage.Parent)
		m.mu.Unlock()
		m.growthWG.Done()
	}()

	// A Linux growth ioctl cannot be cancelled once it enters the kernel. Keep
	// its reclaim path alive even after the control context is cancelled for a
	// planned handoff. The primary session owner and backend remain unchanged.
	progressCtx, stopProgress := context.WithCancel(m.lifetime)
	progressDone := make(chan struct{})
	go func() {
		defer close(progressDone)
		m.maintainGrowthProgress(progressCtx, stage, live.branch)
	}()
	defer func() { stopProgress(); <-progressDone }()
	workCtx, cancel := context.WithCancel(ctx)
	stopOwner := context.AfterFunc(m.lifetime, cancel)
	defer func() { stopOwner(); cancel() }()
	usage, err := runtime.InspectXFS(current.DevicePath, current.XFSRoot)
	if err != nil {
		return false, err
	}
	target, err := XFSGrowthTarget(usage, int64(live.branch.Size()))
	if err != nil || target == 0 {
		return false, err
	}
	_, err = runtime.GrowXFS(workCtx, current.DevicePath, current.XFSRoot, target)
	return err == nil, err
}

func (m *Manager) maintainGrowthProgress(ctx context.Context, stage rootfshandoff.StageRequest, branch *rootfsblock.Branch) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		// Observe branch locks outside Bolt transactions, preserving rotation's
		// branch -> journal lock ordering.
		pressure := branch.DirtyTailPressure() != nil || branch.RotationFailure() != nil
		current, err := m.load(stage.Parent)
		if err != nil {
			continue
		}
		pending := current.LiveCommit != nil && (current.LiveCommit.Pending != nil || current.LiveCommit.ObsoleteBranch != nil)
		if pressure || pending {
			attempt, cancel := context.WithTimeout(ctx, 3*time.Minute)
			_ = m.CommitLive(attempt, stage)
			cancel()
		}
	}
}

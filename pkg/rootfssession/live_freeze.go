package session

import (
	"context"

	"github.com/sandbox0-ai/sandbox0/pkg/rootfshandoff"
)

// The permit is local owner custody, not a durable authorization. Recovery
// must thaw an interrupted barrier before another owner can reclaim its WAL.
type runningFreeze struct {
	operationID string
	draining    bool
}

func (f runningFreeze) matches(current record) bool {
	return f.operationID != "" && current.RunningForkRequest != nil &&
		current.RunningForkRequest.OperationID == f.operationID && current.FreezeOperationID == f.operationID
}

func (m *Manager) freezeRunningFork(ctx context.Context, stage rootfshandoff.StageRequest, current record, live *liveSession) (error, error) {
	// A kernel freeze cannot be cancelled once admitted. Its progress guardian
	// belongs to the session owner, surviving cancellation of the control RPC.
	progress, stop := context.WithCancel(m.lifetime)
	done := make(chan struct{})
	go func() {
		defer close(done)
		if m.liveCommitAuthority != nil {
			m.maintainKernelWriteProgress(progress, stage, live.branch)
		}
	}()
	freezeErr := m.runtime.FreezeXFS(current.XFSRoot)
	// Stop admitting new cuts before waiting for an existing commit. A frozen
	// branch may still have an ambiguous receipt which must settle before the
	// checkpoint selects its current installed reader and WAL suffix.
	m.mu.Lock()
	permit := m.freezes[stage.Parent]
	permit.draining = false
	m.freezes[stage.Parent] = permit
	committing := m.liveCommitDone[stage.Parent]
	m.mu.Unlock()
	stop()
	<-done
	if committing != nil {
		<-committing
	}
	if freezeErr != nil {
		return freezeErr, nil
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if m.liveCommitAuthority == nil {
		return nil, nil
	}
	return nil, m.SettleLiveCommit(ctx, stage)
}

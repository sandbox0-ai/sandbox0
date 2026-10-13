package session

import (
	"fmt"
	"sort"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/rootfsblock"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshandoff"
)

const LiveCommitInterval = 30 * time.Second
const LiveCommitDirtyBytes = 64 << 20

// LiveCommitSession is a disposable scheduling hint, not writer authority. The
// operation rechecks durable binding and custody before starting any upload.
type LiveCommitSession struct {
	Stage      rootfshandoff.StageRequest
	DirtyBytes int64
	Pending    bool
	Pressure   bool
}

func (m *Manager) LiveCommitSessions(now time.Time) ([]LiveCommitSession, error) {
	m.mu.Lock()
	live := make(map[string]*rootfsblock.Branch, len(m.live))
	for parent, session := range m.live {
		if session != nil && !m.captures[parent] && !m.closing && !m.handoff {
			live[parent] = session.branch
		}
	}
	m.mu.Unlock()
	var result []LiveCommitSession
	for parent, branch := range live {
		current, err := m.load(parent)
		if err != nil {
			return nil, err
		}
		if current.State != stateReady || current.Stage == nil || current.RunningForkRequest != nil || current.FreezeOperationID != "" || current.Migration != nil || current.CrashFence != nil {
			continue
		}
		usage := branch.DirtyTailUsage()
		pending := current.LiveCommit != nil && (current.LiveCommit.Pending != nil || current.LiveCommit.ObsoleteBranch != nil)
		pressure := branch.DirtyTailPressure() != nil
		lastText := current.CreatedAt
		if current.LiveCommit != nil && current.LiveCommit.LastInstalledAt != "" {
			lastText = current.LiveCommit.LastInstalledAt
		}
		last, err := time.Parse(time.RFC3339Nano, lastText)
		if err != nil {
			return nil, fmt.Errorf("invalid live commit scheduling timestamp: %w", err)
		}
		threshold := min(int64(LiveCommitDirtyBytes), max(int64(rootfsblock.LogicalBlockSize), m.maxDirty/4))
		if pending || usage.DirtyBytes > 0 && (pressure || usage.DirtyBytes >= threshold || now.Sub(last) >= LiveCommitInterval) {
			result = append(result, LiveCommitSession{Stage: cloneDurableStage(*current.Stage), DirtyBytes: usage.DirtyBytes, Pending: pending, Pressure: pressure})
		}
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Stage.Parent < result[j].Stage.Parent })
	return result, nil
}

// PendingLiveCommitStages is used only after background/control work has
// joined, so a planned handoff can finish rather than abandon installation.
func (m *Manager) PendingLiveCommitStages() ([]rootfshandoff.StageRequest, error) {
	sessions, err := m.LiveCommitSessions(time.Now())
	if err != nil {
		return nil, err
	}
	var result []rootfshandoff.StageRequest
	for _, session := range sessions {
		if session.Pending {
			result = append(result, session.Stage)
		}
	}
	return result, nil
}

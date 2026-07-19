package http

import (
	"context"
	"sync"

	"github.com/sandbox0-ai/sandbox0/pkg/teamquota"
	"github.com/sandbox0-ai/sandbox0/pkg/teamquota/activeconnections"
)

type testWatchActiveConnectionQuota struct {
	mu sync.Mutex

	maxPerTeam         int
	used               map[string]int
	acquired           []string
	leases             []*testWatchActiveConnectionLease
	nextReleaseStarted chan struct{}
	nextReleaseGate    <-chan struct{}
}

func (q *testWatchActiveConnectionQuota) Acquire(
	_ context.Context,
	teamID string,
) (activeconnections.Lease, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.used == nil {
		q.used = make(map[string]int)
	}
	if q.maxPerTeam > 0 && q.used[teamID] >= q.maxPerTeam {
		return nil, &teamquota.ConcurrencyExceededError{
			TeamID: teamID,
			Key:    teamquota.KeyActiveConnectionCount,
			Limit:  int64(q.maxPerTeam),
			Used:   int64(q.used[teamID]),
		}
	}
	lease := &testWatchActiveConnectionLease{
		quota:          q,
		teamID:         teamID,
		done:           make(chan struct{}),
		releaseStarted: q.nextReleaseStarted,
		releaseGate:    q.nextReleaseGate,
	}
	q.nextReleaseStarted = nil
	q.nextReleaseGate = nil
	q.used[teamID]++
	q.acquired = append(q.acquired, teamID)
	q.leases = append(q.leases, lease)
	return lease, nil
}

func (*testWatchActiveConnectionQuota) Close() error { return nil }

func (q *testWatchActiveConnectionQuota) release(teamID string) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.used[teamID] > 1 {
		q.used[teamID]--
	} else {
		delete(q.used, teamID)
	}
}

func (q *testWatchActiveConnectionQuota) usage(teamID string) int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.used[teamID]
}

func (q *testWatchActiveConnectionQuota) acquisitionCount() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.acquired)
}

func (q *testWatchActiveConnectionQuota) leaseAt(index int) *testWatchActiveConnectionLease {
	q.mu.Lock()
	defer q.mu.Unlock()
	if index < 0 || index >= len(q.leases) {
		return nil
	}
	return q.leases[index]
}

type testWatchActiveConnectionLease struct {
	quota          *testWatchActiveConnectionQuota
	teamID         string
	done           chan struct{}
	releaseStarted chan struct{}
	releaseGate    <-chan struct{}

	releaseSignalOnce sync.Once
	once              sync.Once
	mu                sync.RWMutex
	err               error
}

func (l *testWatchActiveConnectionLease) Done() <-chan struct{} {
	return l.done
}

func (l *testWatchActiveConnectionLease) Err() error {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.err
}

func (l *testWatchActiveConnectionLease) Release(ctx context.Context) error {
	if l.releaseStarted != nil {
		l.releaseSignalOnce.Do(func() {
			close(l.releaseStarted)
		})
	}
	if l.releaseGate != nil {
		select {
		case <-l.releaseGate:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	l.finish(nil)
	return nil
}

func (l *testWatchActiveConnectionLease) lose(err error) {
	l.finish(err)
}

func (l *testWatchActiveConnectionLease) finish(err error) {
	l.once.Do(func() {
		l.mu.Lock()
		l.err = err
		l.mu.Unlock()
		if l.quota != nil {
			l.quota.release(l.teamID)
		}
		close(l.done)
	})
}

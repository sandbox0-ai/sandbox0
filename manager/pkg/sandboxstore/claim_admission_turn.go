package sandboxstore

import (
	"context"
	"sync"
)

// claimAdmissionTurns keeps a burst's same-team quota waiters out of the
// shared database pool. PostgreSQL's transaction lock remains the authority,
// including for other manager processes, resumes and forks.
type claimAdmissionTurns struct {
	mu    sync.Mutex
	teams map[string]*claimAdmissionTurn
}

type claimAdmissionTurn struct {
	owner      chan struct{}
	references int
}

func (q *claimAdmissionTurns) acquire(ctx context.Context, team string) (func(), error) {
	q.mu.Lock()
	if q.teams == nil {
		q.teams = make(map[string]*claimAdmissionTurn)
	}
	turn := q.teams[team]
	if turn == nil {
		turn = &claimAdmissionTurn{owner: make(chan struct{}, 1)}
		q.teams[team] = turn
	}
	turn.references++
	q.mu.Unlock()

	forget := func() {
		q.mu.Lock()
		defer q.mu.Unlock()
		turn.references--
		if turn.references == 0 {
			delete(q.teams, team)
		}
	}
	select {
	case turn.owner <- struct{}{}:
		return func() {
			<-turn.owner
			forget()
		}, nil
	case <-ctx.Done():
		forget()
		return nil, ctx.Err()
	}
}

package rootfsblock

import (
	"container/list"
	"context"
	"errors"
	"sync"
)

// sourceReadAdmission bounds distinct source buffers before allocation. It
// remains work-conserving for a single Reader, but rotates waiting Readers on
// every grant so one generation's backlog cannot monopolize a shared cache.
// Active I/O is never preempted; this is not a wall-clock latency guarantee.
type sourceReadAdmission struct {
	mu     sync.Mutex
	active int
	owners map[*Reader]*sourceReadOwner
	order  list.List
}

type sourceReadOwner struct {
	reader  *Reader
	turn    *list.Element
	waiters list.List
}

type sourceReadWaiter struct {
	ctx     context.Context
	ready   chan struct{}
	owner   *sourceReadOwner
	element *list.Element
	granted bool
	err     error
}

// Empty queues release their Reader reference immediately. Cancellation needs
// no background goroutine and does not wait for unrelated I/O to finish.
func (a *sourceReadAdmission) removeLocked(w *sourceReadWaiter) {
	q := w.owner
	q.waiters.Remove(w.element)
	w.element = nil
	if q.waiters.Len() == 0 {
		a.order.Remove(q.turn)
		q.turn = nil
		delete(a.owners, q.reader)
	}
}

func (a *sourceReadAdmission) dispatchLocked() {
	for a.active < maxConcurrentSourceReads && a.order.Len() > 0 {
		q := a.order.Front().Value.(*sourceReadOwner)
		w := q.waiters.Front().Value.(*sourceReadWaiter)
		a.removeLocked(w)
		// A canceled waiter consumes neither a grant nor its Reader's turn.
		if err := w.ctx.Err(); err != nil {
			w.err = err
			close(w.ready)
			continue
		}
		if q.turn != nil {
			a.order.MoveToBack(q.turn)
		}
		w.granted = true
		a.active++
		close(w.ready)
	}
}

func (a *sourceReadAdmission) release() func() {
	var once sync.Once
	return func() {
		once.Do(func() {
			a.mu.Lock()
			defer a.mu.Unlock()
			if a.active <= 0 {
				panic("source read admission release without grant")
			}
			a.active--
			a.dispatchLocked()
		})
	}
}

func (a *sourceReadAdmission) acquire(ctx context.Context, reader *Reader) (func(), error) {
	if ctx == nil || reader == nil {
		return nil, errors.New("source read admission requires a lifetime and Reader")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	a.mu.Lock()
	if err := ctx.Err(); err != nil {
		a.mu.Unlock()
		return nil, err
	}
	if a.active < maxConcurrentSourceReads && a.order.Len() == 0 {
		a.active++
		a.mu.Unlock()
		release := a.release()
		if err := ctx.Err(); err != nil {
			release()
			return nil, err
		}
		return release, nil
	}
	if a.owners == nil {
		a.owners = make(map[*Reader]*sourceReadOwner)
	}
	q := a.owners[reader]
	if q == nil {
		q = &sourceReadOwner{reader: reader}
		q.turn = a.order.PushBack(q)
		a.owners[reader] = q
	}
	w := &sourceReadWaiter{ctx: ctx, ready: make(chan struct{}), owner: q}
	w.element = q.waiters.PushBack(w)
	a.dispatchLocked()
	a.mu.Unlock()
	select {
	case <-ctx.Done():
	case <-w.ready:
	}
	a.mu.Lock()
	if err := ctx.Err(); err != nil {
		if w.granted {
			a.active--
			a.dispatchLocked()
		} else if w.element != nil {
			a.removeLocked(w)
		}
		a.mu.Unlock()
		return nil, err
	}
	granted, err := w.granted, w.err
	a.mu.Unlock()
	if !granted {
		if err == nil {
			err = errors.New("source read admission woke without a grant")
		}
		return nil, err
	}
	return a.release(), nil
}

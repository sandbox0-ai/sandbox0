package nomadmigration

import "sync"

// Progress wakes this manager's migration lanes after a committed transition.
// It carries no operation state or authority: every awakened lane rereads
// PostgreSQL. Periodic reconciliation still recovers missed and remote commits.
// A channel generation broadcasts to all lanes, unlike a shared queue where
// an unrelated lane could consume the only notification.
type Progress struct {
	mu      sync.Mutex
	changed chan struct{}
}

func (p *Progress) watch() <-chan struct{} {
	if p == nil {
		return nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.changed == nil {
		p.changed = make(chan struct{})
	}
	return p.changed
}

func (p *Progress) notify() {
	if p == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.changed != nil {
		close(p.changed)
	}
	p.changed = make(chan struct{})
}

// SetProgress connects a lane before Run starts. It must not race with Run.
func (c *Coordinator) SetProgress(progress *Progress) { c.progress = progress }

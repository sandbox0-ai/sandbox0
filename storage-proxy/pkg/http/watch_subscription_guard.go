package http

import (
	"sync"
)

const maxVolumeFileWatchSubscriptionsPerServer = 4096

type volumeWatchSubscriptionLimit int

const (
	volumeWatchSubscriptionAllowed volumeWatchSubscriptionLimit = iota
	volumeWatchSubscriptionServerLimit
)

// volumeWatchSubscriptionGuard bounds subscriptions retained by one storage
// process. Its zero value uses the production limits.
type volumeWatchSubscriptionGuard struct {
	mu sync.Mutex

	maxGlobal int
	global    int
}

func (g *volumeWatchSubscriptionGuard) acquire() (func(), volumeWatchSubscriptionLimit) {
	g.mu.Lock()
	maxGlobal := g.maxGlobal
	if maxGlobal <= 0 {
		maxGlobal = maxVolumeFileWatchSubscriptionsPerServer
	}
	if g.global >= maxGlobal {
		g.mu.Unlock()
		return nil, volumeWatchSubscriptionServerLimit
	}
	g.global++
	g.mu.Unlock()

	var once sync.Once
	return func() {
		once.Do(func() {
			g.release()
		})
	}, volumeWatchSubscriptionAllowed
}

func (g *volumeWatchSubscriptionGuard) release() {
	g.mu.Lock()
	defer g.mu.Unlock()

	if g.global > 0 {
		g.global--
	}
}

func (g *volumeWatchSubscriptionGuard) count() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.global
}

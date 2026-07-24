package quota

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/sync/singleflight"
)

const (
	DefaultPolicyCacheTTL = 5 * time.Second
	policyChangeChannel   = "quota_policy_changed"
)

type policyCacheKey struct {
	teamID    string
	dimension Dimension
}

type policyCacheEntry struct {
	policy    *Policy
	expiresAt time.Time
}

// CachedPolicyStore caches resolved policies and invalidates the cache through
// PostgreSQL LISTEN/NOTIFY. The TTL bounds staleness if the listener reconnects.
type CachedPolicyStore struct {
	source PolicyStore
	ttl    time.Duration

	mu      sync.RWMutex
	entries map[policyCacheKey]policyCacheEntry
	fetches singleflight.Group

	cancel context.CancelFunc
	done   chan struct{}
}

func NewCachedPolicyStore(ctx context.Context, pool *pgxpool.Pool, source PolicyStore, ttl time.Duration) (*CachedPolicyStore, error) {
	if source == nil {
		return nil, fmt.Errorf("quota policy source is required")
	}
	if ttl <= 0 {
		ttl = DefaultPolicyCacheTTL
	}
	store := &CachedPolicyStore{
		source:  source,
		ttl:     ttl,
		entries: make(map[policyCacheKey]policyCacheEntry),
	}
	if pool != nil {
		listenCtx, cancel := context.WithCancel(ctx)
		store.cancel = cancel
		store.done = make(chan struct{})
		go store.listen(listenCtx, pool)
	}
	return store, nil
}

func (s *CachedPolicyStore) GetPolicy(ctx context.Context, teamID string, dimension Dimension) (*Policy, error) {
	if s == nil || s.source == nil {
		return nil, nil
	}
	teamID = strings.TrimSpace(teamID)
	key := policyCacheKey{teamID: teamID, dimension: dimension}
	now := time.Now()

	s.mu.RLock()
	entry, ok := s.entries[key]
	s.mu.RUnlock()
	if ok && now.Before(entry.expiresAt) {
		return clonePolicy(entry.policy), nil
	}

	value, err, _ := s.fetches.Do(teamID+"\x00"+string(dimension), func() (any, error) {
		now := time.Now()
		s.mu.RLock()
		entry, ok := s.entries[key]
		s.mu.RUnlock()
		if ok && now.Before(entry.expiresAt) {
			return clonePolicy(entry.policy), nil
		}

		policy, err := s.source.GetPolicy(ctx, teamID, dimension)
		if err != nil {
			return nil, err
		}
		s.mu.Lock()
		s.entries[key] = policyCacheEntry{
			policy:    clonePolicy(policy),
			expiresAt: time.Now().Add(s.ttl),
		}
		s.mu.Unlock()
		return clonePolicy(policy), nil
	})
	if err != nil {
		return nil, err
	}
	policy, ok := value.(*Policy)
	if !ok {
		return nil, fmt.Errorf("quota policy source returned %T", value)
	}
	return clonePolicy(policy), nil
}

func (s *CachedPolicyStore) Invalidate() {
	if s == nil {
		return
	}
	s.mu.Lock()
	clear(s.entries)
	s.mu.Unlock()
}

func (s *CachedPolicyStore) Close() error {
	if s == nil || s.cancel == nil {
		return nil
	}
	s.cancel()
	if s.done != nil {
		<-s.done
	}
	return nil
}

func (s *CachedPolicyStore) listen(ctx context.Context, pool *pgxpool.Pool) {
	defer close(s.done)
	for ctx.Err() == nil {
		conn, err := pool.Acquire(ctx)
		if err != nil {
			if !waitForPolicyListenerRetry(ctx) {
				return
			}
			continue
		}
		_, err = conn.Exec(ctx, "LISTEN "+policyChangeChannel)
		if err == nil {
			for ctx.Err() == nil {
				if _, err = conn.Conn().WaitForNotification(ctx); err != nil {
					break
				}
				s.Invalidate()
			}
		}
		conn.Release()
		if ctx.Err() == nil && !waitForPolicyListenerRetry(ctx) {
			return
		}
	}
}

func waitForPolicyListenerRetry(ctx context.Context) bool {
	timer := time.NewTimer(time.Second)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func clonePolicy(policy *Policy) *Policy {
	if policy == nil {
		return nil
	}
	out := *policy
	return &out
}

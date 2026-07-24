package quota

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type countingPolicyStore struct {
	calls   atomic.Int64
	policy  *Policy
	started chan struct{}
	release chan struct{}
}

func (s *countingPolicyStore) GetPolicy(context.Context, string, Dimension) (*Policy, error) {
	s.calls.Add(1)
	if s.started != nil {
		select {
		case s.started <- struct{}{}:
		default:
		}
	}
	if s.release != nil {
		<-s.release
	}
	return clonePolicy(s.policy), nil
}

func TestCachedPolicyStoreCachesAndInvalidatesResolvedPolicy(t *testing.T) {
	source := &countingPolicyStore{policy: &Policy{
		TeamID:     "team-1",
		Dimension:  DimensionAPIRequests,
		Kind:       KindRate,
		LimitValue: 100,
		IntervalMS: 1000,
		BurstValue: 200,
		Source:     SourceRegionDefault,
	}}
	store, err := NewCachedPolicyStore(context.Background(), nil, source, time.Minute)
	if err != nil {
		t.Fatalf("NewCachedPolicyStore: %v", err)
	}
	first, err := store.GetPolicy(context.Background(), "team-1", DimensionAPIRequests)
	if err != nil {
		t.Fatalf("GetPolicy(first): %v", err)
	}
	first.LimitValue = 1
	second, err := store.GetPolicy(context.Background(), "team-1", DimensionAPIRequests)
	if err != nil {
		t.Fatalf("GetPolicy(second): %v", err)
	}
	if source.calls.Load() != 1 || second.LimitValue != 100 {
		t.Fatalf("cached policy = %+v calls=%d, want cloned cached value", second, source.calls.Load())
	}

	store.Invalidate()
	if _, err := store.GetPolicy(context.Background(), "team-1", DimensionAPIRequests); err != nil {
		t.Fatalf("GetPolicy(after invalidate): %v", err)
	}
	if source.calls.Load() != 2 {
		t.Fatalf("source calls = %d, want 2", source.calls.Load())
	}
}

func TestCachedPolicyStoreCoalescesConcurrentMisses(t *testing.T) {
	source := &countingPolicyStore{
		policy: &Policy{
			TeamID:     "team-1",
			Dimension:  DimensionActiveSandboxes,
			Kind:       KindCapacity,
			LimitValue: 120,
			Source:     SourceTeamOverride,
		},
		started: make(chan struct{}, 1),
		release: make(chan struct{}),
	}
	store, err := NewCachedPolicyStore(context.Background(), nil, source, time.Minute)
	if err != nil {
		t.Fatalf("NewCachedPolicyStore: %v", err)
	}

	const callers = 32
	start := make(chan struct{})
	errs := make(chan error, callers)
	var wg sync.WaitGroup
	wg.Add(callers)
	for range callers {
		go func() {
			defer wg.Done()
			<-start
			policy, err := store.GetPolicy(context.Background(), "team-1", DimensionActiveSandboxes)
			if err == nil && (policy == nil || policy.LimitValue != 120) {
				t.Errorf("policy = %+v, want active sandbox limit 120", policy)
			}
			errs <- err
		}()
	}
	close(start)
	<-source.started
	time.Sleep(20 * time.Millisecond)
	close(source.release)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("GetPolicy: %v", err)
		}
	}
	if source.calls.Load() != 1 {
		t.Fatalf("source calls = %d, want 1", source.calls.Load())
	}
}

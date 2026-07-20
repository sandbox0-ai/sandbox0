package quota

import (
	"context"
	"testing"
	"time"
)

type countingPolicyStore struct {
	calls  int
	policy *Policy
}

func (s *countingPolicyStore) GetPolicy(context.Context, string, Dimension) (*Policy, error) {
	s.calls++
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
	if source.calls != 1 || second.LimitValue != 100 {
		t.Fatalf("cached policy = %+v calls=%d, want cloned cached value", second, source.calls)
	}

	store.Invalidate()
	if _, err := store.GetPolicy(context.Background(), "team-1", DimensionAPIRequests); err != nil {
		t.Fatalf("GetPolicy(after invalidate): %v", err)
	}
	if source.calls != 2 {
		t.Fatalf("source calls = %d, want 2", source.calls)
	}
}

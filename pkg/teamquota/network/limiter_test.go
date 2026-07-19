package network

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/teamquota"
	"github.com/sandbox0-ai/sandbox0/pkg/teamquota/guard"
	"github.com/sandbox0-ai/sandbox0/pkg/tokenbucket"
)

func TestLimiterChunksCostsLargerThanBurst(t *testing.T) {
	resolver := &fakePolicyResolver{policy: networkPolicy(
		teamquota.KeyNetworkEgressBytes,
		3,
	)}
	bucket := &fakeBucket{decision: tokenbucket.Decision{Allowed: true}}
	limiter := newTestLimiter(t, resolver, &fakeAdmissionMarker{}, bucket)

	if err := limiter.WaitN(
		context.Background(),
		"team-1",
		teamquota.KeyNetworkEgressBytes,
		8,
	); err != nil {
		t.Fatalf("WaitN() error = %v", err)
	}
	if !equalInt64s(bucket.costs, []int64{3, 3, 3}) {
		t.Fatalf("bucket costs = %#v, want bounded byte-credit grants [3 3 3]", bucket.costs)
	}
}

func TestLimiterWaitIsCancellable(t *testing.T) {
	bucket := &fakeBucket{decision: tokenbucket.Decision{
		Allowed:    false,
		RetryAfter: time.Hour,
	}}
	limiter := newTestLimiter(
		t,
		&fakePolicyResolver{policy: networkPolicy(
			teamquota.KeyNetworkIngressBytes,
			10,
		)},
		&fakeAdmissionMarker{},
		bucket,
	)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := limiter.WaitN(
		ctx,
		"team-1",
		teamquota.KeyNetworkIngressBytes,
		1,
	)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("WaitN() error = %v, want context.Canceled", err)
	}
}

func TestLimiterTakesDiscreteNetworkOperationsImmediately(t *testing.T) {
	tests := []struct {
		name        string
		decision    tokenbucket.Decision
		bucketErr   error
		wantAllowed bool
		wantErr     bool
		wantCosts   []int64
	}{
		{
			name:        "allowed",
			decision:    tokenbucket.Decision{Allowed: true, Remaining: 99},
			wantAllowed: true,
			wantCosts:   []int64{16},
		},
		{
			name:      "denied with exact fallback",
			decision:  tokenbucket.Decision{RetryAfter: time.Second},
			wantCosts: []int64{16, 1},
		},
		{
			name:      "backend failure",
			bucketErr: errors.New("redis unavailable"),
			wantErr:   true,
			wantCosts: []int64{16},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bucket := &fakeBucket{decision: tt.decision, err: tt.bucketErr}
			policy := networkPolicy(teamquota.KeyNetworkOperations, 100)
			limiter := newTestLimiter(
				t,
				&fakePolicyResolver{policy: policy},
				&fakeAdmissionMarker{},
				bucket,
			)
			decision, err := limiter.Take(
				context.Background(),
				"team-1",
				teamquota.KeyNetworkOperations,
				1,
			)
			if tt.wantErr {
				if !teamquota.IsUnavailable(err) {
					t.Fatalf("Take() error = %v, want unavailable", err)
				}
			} else if err != nil {
				t.Fatalf("Take() error = %v", err)
			}
			if decision.Allowed != tt.wantAllowed {
				t.Fatalf("Take() decision = %+v, want allowed %v", decision, tt.wantAllowed)
			}
			if !equalInt64s(bucket.costs, tt.wantCosts) {
				t.Fatalf("bucket costs = %#v, want %#v", bucket.costs, tt.wantCosts)
			}
			limiter.waiters.mu.Lock()
			waiters := limiter.waiters.global
			limiter.waiters.mu.Unlock()
			if waiters != 0 {
				t.Fatalf("immediate Take() waiter count = %d, want zero", waiters)
			}
		})
	}
}

func TestLimiterFailsClosedBeforeIO(t *testing.T) {
	tests := []struct {
		name     string
		teamID   string
		resolver *fakePolicyResolver
		marker   *fakeAdmissionMarker
		bucket   *fakeBucket
	}{
		{
			name:     "missing team",
			resolver: &fakePolicyResolver{},
			marker:   &fakeAdmissionMarker{},
			bucket:   &fakeBucket{},
		},
		{
			name:   "policy failure",
			teamID: "team-1",
			resolver: &fakePolicyResolver{
				err: errors.New("postgres unavailable"),
			},
			marker: &fakeAdmissionMarker{},
			bucket: &fakeBucket{},
		},
		{
			name:     "admission marker failure",
			teamID:   "team-1",
			resolver: &fakePolicyResolver{},
			marker: &fakeAdmissionMarker{
				err: errors.New("redis unavailable"),
			},
			bucket: &fakeBucket{},
		},
		{
			name:   "bucket failure",
			teamID: "team-1",
			resolver: &fakePolicyResolver{policy: networkPolicy(
				teamquota.KeyNetworkEgressBytes,
				10,
			)},
			marker: &fakeAdmissionMarker{},
			bucket: &fakeBucket{err: errors.New("redis unavailable")},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			limiter := newTestLimiter(t, tt.resolver, tt.marker, tt.bucket)
			var destination bytes.Buffer
			writer := limiter.Writer(
				context.Background(),
				tt.teamID,
				teamquota.KeyNetworkEgressBytes,
				&destination,
			)
			if _, err := writer.Write([]byte("payload")); !teamquota.IsUnavailable(err) {
				t.Fatalf("Write() error = %v, want unavailable", err)
			}
			if destination.Len() != 0 {
				t.Fatalf("destination bytes = %d, want 0", destination.Len())
			}
		})
	}
}

func TestLimitedReaderAndWriterUseExactDirectionalKeys(t *testing.T) {
	resolver := &fakePolicyResolver{
		policies: map[teamquota.Key]*teamquota.Policy{
			teamquota.KeyNetworkIngressBytes: networkPolicy(
				teamquota.KeyNetworkIngressBytes,
				4,
			),
			teamquota.KeyNetworkEgressBytes: networkPolicy(
				teamquota.KeyNetworkEgressBytes,
				4,
			),
		},
	}
	bucket := &fakeBucket{decision: tokenbucket.Decision{Allowed: true}}
	limiter := newTestLimiter(t, resolver, &fakeAdmissionMarker{}, bucket)

	reader := limiter.Reader(
		context.Background(),
		"team-1",
		teamquota.KeyNetworkIngressBytes,
		bytes.NewBufferString("abcdef"),
	)
	data, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("ReadAll() error = %v", err)
	}
	if string(data) != "abcdef" {
		t.Fatalf("reader data = %q, want abcdef", data)
	}

	var destination bytes.Buffer
	writer := limiter.Writer(
		context.Background(),
		"team-1",
		teamquota.KeyNetworkEgressBytes,
		&destination,
	)
	if _, err := writer.Write([]byte("abcdef")); err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	if destination.String() != "abcdef" {
		t.Fatalf("writer data = %q, want abcdef", destination.String())
	}

	if !containsSuffix(bucket.keys, ":network_ingress_bytes") {
		t.Fatalf("bucket keys = %#v, missing ingress key", bucket.keys)
	}
	if !containsSuffix(bucket.keys, ":network_egress_bytes") {
		t.Fatalf("bucket keys = %#v, missing egress key", bucket.keys)
	}
}

func TestLimiterCloseIsIdempotent(t *testing.T) {
	marker := &fakeAdmissionMarker{}
	bucket := &fakeBucket{}
	limiter := newTestLimiter(
		t,
		&fakePolicyResolver{},
		marker,
		bucket,
	)
	if err := limiter.Close(); err != nil {
		t.Fatalf("first Close() error = %v", err)
	}
	if err := limiter.Close(); err != nil {
		t.Fatalf("second Close() error = %v", err)
	}
	if marker.closeCalls != 1 || bucket.closeCalls != 1 {
		t.Fatalf(
			"close calls = marker %d bucket %d, want 1 each",
			marker.closeCalls,
			bucket.closeCalls,
		)
	}
}

func TestLimiterBoundsGlobalAndPerTeamWaitersAndReleasesOnCancel(t *testing.T) {
	entered := make(chan string, 2)
	wait := func(ctx context.Context, _ time.Duration) error {
		teamID, _ := ctx.Value(waiterTestTeamKey{}).(string)
		entered <- teamID
		<-ctx.Done()
		return ctx.Err()
	}
	policy := networkPolicy(teamquota.KeyNetworkEgressBytes, 10)
	policy.TeamID = ""
	limiter, err := New(
		&fakePolicyResolver{policy: policy},
		&fakeAdmissionMarker{},
		deniedGuardedBucket{},
		Config{
			RegionID:         "region-1",
			PolicyCacheTTL:   time.Hour,
			MaxGlobalWaiters: 2,
			MaxTeamWaiters:   1,
			MaxWait:          time.Hour,
			Wait:             wait,
		},
	)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	start := func(teamID string) (context.CancelFunc, <-chan error) {
		ctx, cancel := context.WithCancel(context.WithValue(
			context.Background(),
			waiterTestTeamKey{},
			teamID,
		))
		result := make(chan error, 1)
		go func() {
			result <- limiter.WaitN(
				ctx,
				teamID,
				teamquota.KeyNetworkEgressBytes,
				1,
			)
		}()
		return cancel, result
	}

	cancelA, resultA := start("team-a")
	if got := <-entered; got != "team-a" {
		t.Fatalf("first entered team = %q", got)
	}
	err = limiter.WaitN(
		context.Background(),
		"team-a",
		teamquota.KeyNetworkEgressBytes,
		1,
	)
	if !errors.Is(err, ErrWaiterSaturated) || !teamquota.IsUnavailable(err) {
		t.Fatalf("per-team saturation error = %v", err)
	}

	cancelB, resultB := start("team-b")
	if got := <-entered; got != "team-b" {
		t.Fatalf("second entered team = %q", got)
	}
	err = limiter.WaitN(
		context.Background(),
		"team-c",
		teamquota.KeyNetworkEgressBytes,
		1,
	)
	if !errors.Is(err, ErrWaiterSaturated) || !teamquota.IsUnavailable(err) {
		t.Fatalf("global saturation error = %v", err)
	}

	cancelA()
	cancelB()
	if err := <-resultA; !errors.Is(err, context.Canceled) {
		t.Fatalf("team-a cancellation error = %v", err)
	}
	if err := <-resultB; !errors.Is(err, context.Canceled) {
		t.Fatalf("team-b cancellation error = %v", err)
	}
	limiter.waiters.mu.Lock()
	global := limiter.waiters.global
	teams := len(limiter.waiters.teams)
	limiter.waiters.mu.Unlock()
	if global != 0 || teams != 0 {
		t.Fatalf("waiters after cancellation = global %d teams %d, want zero", global, teams)
	}
}

func TestLimiterMaximumWaitUsesOneBoundedTimerBudget(t *testing.T) {
	clock := &networkManualClock{now: time.Unix(1_700_000_000, 0)}
	var (
		waitMu    sync.Mutex
		waitCalls []time.Duration
	)
	policy := networkPolicy(teamquota.KeyNetworkIngressBytes, 10)
	policy.TeamID = ""
	limiter, err := New(
		&fakePolicyResolver{policy: policy},
		&fakeAdmissionMarker{},
		deniedGuardedBucket{},
		Config{
			RegionID:         "region-1",
			PolicyCacheTTL:   time.Hour,
			MaxGlobalWaiters: 1,
			MaxTeamWaiters:   1,
			MaxWait:          5 * time.Second,
			Now:              clock.Now,
			Wait: func(_ context.Context, delay time.Duration) error {
				waitMu.Lock()
				waitCalls = append(waitCalls, delay)
				waitMu.Unlock()
				clock.Advance(delay)
				return nil
			},
		},
	)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	err = limiter.WaitN(
		context.Background(),
		"team-a",
		teamquota.KeyNetworkIngressBytes,
		1,
	)
	if !errors.Is(err, ErrWaitTimeout) || !teamquota.IsUnavailable(err) {
		t.Fatalf("WaitN() timeout error = %v", err)
	}
	waitMu.Lock()
	gotCalls := append([]time.Duration(nil), waitCalls...)
	waitMu.Unlock()
	if len(gotCalls) != 1 || gotCalls[0] != 5*time.Second {
		t.Fatalf("wait calls = %#v, want one bounded 5s wait", gotCalls)
	}
	limiter.waiters.mu.Lock()
	global := limiter.waiters.global
	limiter.waiters.mu.Unlock()
	if global != 0 {
		t.Fatalf("global waiters after timeout = %d, want zero", global)
	}
}

func TestLimiterWaiterDefaultsAreBounded(t *testing.T) {
	limiter := newTestLimiter(
		t,
		&fakePolicyResolver{},
		&fakeAdmissionMarker{},
		&fakeBucket{},
	)
	if limiter.waiters.globalLimit != 4096 ||
		limiter.waiters.teamLimit != 256 ||
		limiter.maxWait != 5*time.Second {
		t.Fatalf(
			"waiter defaults = global %d team %d wait %s",
			limiter.waiters.globalLimit,
			limiter.waiters.teamLimit,
			limiter.maxWait,
		)
	}
}

func newTestLimiter(
	t *testing.T,
	resolver *fakePolicyResolver,
	marker *fakeAdmissionMarker,
	bucket *fakeBucket,
) *Limiter {
	t.Helper()
	limiter, err := New(resolver, marker, bucket, Config{
		RegionID:       "region-1",
		PolicyCacheTTL: time.Minute,
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	return limiter
}

func networkPolicy(key teamquota.Key, burst int64) *teamquota.Policy {
	return &teamquota.Policy{
		TeamID:         "team-1",
		Key:            key,
		Kind:           teamquota.KindRate,
		Revision:       1,
		Tokens:         burst,
		IntervalMillis: 1000,
		Burst:          burst,
	}
}

type fakePolicyResolver struct {
	policy   *teamquota.Policy
	policies map[teamquota.Key]*teamquota.Policy
	err      error
}

func (r *fakePolicyResolver) EffectivePolicy(
	_ context.Context,
	_ string,
	key teamquota.Key,
) (*teamquota.Policy, error) {
	if r.err != nil {
		return nil, r.err
	}
	policy := r.policy
	if r.policies != nil {
		policy = r.policies[key]
	}
	if policy == nil {
		return nil, nil
	}
	copied := *policy
	return &copied, nil
}

type fakeBucket struct {
	decision   tokenbucket.Decision
	err        error
	keys       []string
	costs      []int64
	closeCalls int
}

func (b *fakeBucket) TakeN(
	_ context.Context,
	key string,
	_ tokenbucket.Policy,
	cost int64,
) (tokenbucket.Decision, error) {
	b.keys = append(b.keys, key)
	b.costs = append(b.costs, cost)
	return b.decision, b.err
}

func (b *fakeBucket) TakeNGuarded(
	ctx context.Context,
	key string,
	_ string,
	policy tokenbucket.Policy,
	_ guard.Version,
	_ time.Time,
	cost int64,
) (tokenbucket.Decision, error) {
	return b.TakeN(ctx, key, policy, cost)
}

func (*fakeBucket) ReadPolicyGuard(context.Context) (guard.State, error) {
	return guard.State{
		Phase: guard.PhaseStable,
		Version: guard.Version{
			EnforcementEpoch: 1,
			RedisGeneration:  1,
		},
	}, nil
}

func (b *fakeBucket) Close() error {
	b.closeCalls++
	return nil
}

type fakeAdmissionMarker struct {
	disabled   bool
	err        error
	closeCalls int
}

type deniedGuardedBucket struct{}

func (deniedGuardedBucket) TakeN(
	context.Context,
	string,
	tokenbucket.Policy,
	int64,
) (tokenbucket.Decision, error) {
	return tokenbucket.Decision{RetryAfter: time.Hour}, nil
}

func (deniedGuardedBucket) TakeNGuarded(
	context.Context,
	string,
	string,
	tokenbucket.Policy,
	guard.Version,
	time.Time,
	int64,
) (tokenbucket.Decision, error) {
	return tokenbucket.Decision{RetryAfter: time.Hour}, nil
}

func (deniedGuardedBucket) ReadPolicyGuard(context.Context) (guard.State, error) {
	return guard.State{
		Phase:   guard.PhaseStable,
		Version: guard.Version{EnforcementEpoch: 1, RedisGeneration: 1},
	}, nil
}

func (deniedGuardedBucket) Close() error { return nil }

type waiterTestTeamKey struct{}

type networkManualClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *networkManualClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *networkManualClock) Advance(delay time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(delay)
	c.mu.Unlock()
}

func (m *fakeAdmissionMarker) Disabled(context.Context, string) (bool, error) {
	return m.disabled, m.err
}

func (m *fakeAdmissionMarker) RedisKey(teamID string) (string, error) {
	if m.err != nil {
		return "", m.err
	}
	return "test:team-admission:" + teamID, nil
}

func (m *fakeAdmissionMarker) Recover(context.Context, string) error {
	return m.err
}

func (*fakeAdmissionMarker) Disable(context.Context, string) error {
	return nil
}

func (*fakeAdmissionMarker) Forget(context.Context, string) error {
	return nil
}

func (m *fakeAdmissionMarker) Close() error {
	m.closeCalls++
	return nil
}

func equalInt64s(left, right []int64) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}

func containsSuffix(values []string, suffix string) bool {
	for _, value := range values {
		if len(value) >= len(suffix) &&
			value[len(value)-len(suffix):] == suffix {
			return true
		}
	}
	return false
}

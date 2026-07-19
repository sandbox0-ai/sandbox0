package concurrency

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"

	"github.com/sandbox0-ai/sandbox0/pkg/teamquota"
	"github.com/sandbox0-ai/sandbox0/pkg/teamquota/distributed"
	"github.com/sandbox0-ai/sandbox0/pkg/teamquota/guard"
)

func TestLeaseRenewFailureFailsClosedAndReleasesMember(t *testing.T) {
	resolver := newConcurrencyResolver(2)
	marker := &testAdmissionMarker{}
	store := &testLeaseStore{
		acquireDecision: leaseDecision{allowed: true, used: 1},
		renewErr:        errors.New("redis unavailable"),
	}
	limiter := newTestLimiter(t, resolver, marker, store)

	lease, err := limiter.Acquire(context.Background(), "team-a", teamquota.KeyActiveConnectionCount)
	if err != nil {
		t.Fatalf("Acquire() error = %v", err)
	}
	waitLeaseDone(t, lease)
	if !teamquota.IsUnavailable(lease.Err()) {
		t.Fatalf("Lease.Err() = %v, want unavailable", lease.Err())
	}
	if got := store.releaseCallCount(); got != 1 {
		t.Fatalf("Release() calls = %d, want 1", got)
	}
}

func TestLeaseReleaseIsIdempotentAndDoesNotReportLoss(t *testing.T) {
	resolver := newConcurrencyResolver(2)
	store := &testLeaseStore{
		acquireDecision: leaseDecision{allowed: true, used: 1},
		renewDecision:   renewed,
	}
	limiter := newTestLimiter(t, resolver, &testAdmissionMarker{}, store)
	lease, err := limiter.Acquire(context.Background(), "team-a", teamquota.KeyActiveConnectionCount)
	if err != nil {
		t.Fatalf("Acquire() error = %v", err)
	}

	if err := lease.Release(context.Background()); err != nil {
		t.Fatalf("first Release() error = %v", err)
	}
	if err := lease.Release(context.Background()); err != nil {
		t.Fatalf("second Release() error = %v", err)
	}
	if lease.Err() != nil {
		t.Fatalf("Lease.Err() = %v, want nil after explicit release", lease.Err())
	}
	if got := store.releaseCallCount(); got != 1 {
		t.Fatalf("store Release() calls = %d, want 1", got)
	}
}

func TestLeaseParentCancellationReleasesMember(t *testing.T) {
	resolver := newConcurrencyResolver(2)
	store := &testLeaseStore{
		acquireDecision: leaseDecision{allowed: true, used: 1},
		renewDecision:   renewed,
	}
	limiter := newTestLimiter(t, resolver, &testAdmissionMarker{}, store)
	ctx, cancel := context.WithCancel(context.Background())
	lease, err := limiter.Acquire(ctx, "team-a", teamquota.KeyActiveConnectionCount)
	if err != nil {
		t.Fatalf("Acquire() error = %v", err)
	}
	cancel()

	waitLeaseDone(t, lease)
	if !errors.Is(lease.Err(), context.Canceled) {
		t.Fatalf("Lease.Err() = %v, want context canceled", lease.Err())
	}
	if got := store.releaseCallCount(); got != 1 {
		t.Fatalf("store Release() calls = %d, want 1", got)
	}
}

func TestLeaseRenewWaitsForPendingPolicyPublication(t *testing.T) {
	redisServer := miniredis.RunT(t)
	redisStore := newTestRedisStore(t, redisServer)
	store := &observedRenewStore{
		RedisStore: redisStore,
		pending:    make(chan struct{}),
		renewed:    make(chan struct{}),
	}
	resolver := newConcurrencyResolver(2)
	limiter, err := newLimiter(
		resolver,
		&testAdmissionMarker{},
		store,
		Config{
			RegionID:       "region-a",
			PolicyCacheTTL: time.Hour,
			LeaseTTL:       500 * time.Millisecond,
			RenewInterval:  100 * time.Millisecond,
		},
	)
	if err != nil {
		t.Fatalf("newLimiter() error = %v", err)
	}
	lease, err := limiter.Acquire(
		context.Background(),
		"team-a",
		teamquota.KeyActiveConnectionCount,
	)
	if err != nil {
		t.Fatalf("Acquire() error = %v", err)
	}

	setTestGuard(t, redisServer, guard.State{
		Phase:           guard.PhasePending,
		Version:         testGuardVersion,
		PendingToken:    "policy-lowering",
		QuarantineUntil: time.Time{},
	})
	select {
	case <-store.pending:
	case <-time.After(300 * time.Millisecond):
		t.Fatal("renewal did not observe pending policy guard")
	}
	select {
	case <-lease.Done():
		t.Fatalf("lease failed while policy publication was transiently pending: %v", lease.Err())
	default:
	}

	resolver.setLimit(1)
	setTestGuard(t, redisServer, guard.State{
		Phase: guard.PhaseStable,
		Version: guard.Version{
			EnforcementEpoch: 2,
			RedisGeneration:  1,
		},
	})
	select {
	case <-store.renewed:
	case <-time.After(300 * time.Millisecond):
		t.Fatal("lease did not renew after stable policy publication")
	}
	select {
	case <-lease.Done():
		t.Fatalf("lease failed after stable policy publication: %v", lease.Err())
	default:
	}
	if err := lease.Release(context.Background()); err != nil {
		t.Fatalf("Release() error = %v", err)
	}
}

func TestLeaseDisabledTeamFailsClosedOnRenewal(t *testing.T) {
	resolver := newConcurrencyResolver(2)
	store := &testLeaseStore{
		acquireDecision: leaseDecision{allowed: true, used: 1},
		renewDecision:   renewed,
	}
	limiter := newTestLimiter(t, resolver, &testAdmissionMarker{}, store)
	lease, err := limiter.Acquire(context.Background(), "team-a", teamquota.KeyActiveConnectionCount)
	if err != nil {
		t.Fatalf("Acquire() error = %v", err)
	}
	store.setRenewError(errAdmissionDisabled)

	waitLeaseDone(t, lease)
	if !teamquota.IsTeamAdmissionDisabled(lease.Err()) {
		t.Fatalf("Lease.Err() = %v, want team admission disabled", lease.Err())
	}
}

func TestLimiterPolicyLoweringClosesOnlyExcessStableLease(t *testing.T) {
	const renewInterval = 100 * time.Millisecond

	redisServer := miniredis.RunT(t)
	redisServer.SetTime(time.Unix(1_700_000_000, 0))
	store := newTestRedisStore(t, redisServer)
	resolver := newConcurrencyResolver(2)
	limiter, err := newLimiter(
		resolver,
		&testAdmissionMarker{},
		store,
		Config{
			RegionID:       "region-a",
			PolicyCacheTTL: 0,
			LeaseTTL:       time.Second,
			RenewInterval:  renewInterval,
		},
	)
	if err != nil {
		t.Fatalf("newLimiter() error = %v", err)
	}

	first, err := limiter.Acquire(context.Background(), "team-a", teamquota.KeyActiveConnectionCount)
	if err != nil {
		t.Fatalf("Acquire(first) error = %v", err)
	}
	second, err := limiter.Acquire(context.Background(), "team-a", teamquota.KeyActiveConnectionCount)
	if err != nil {
		t.Fatalf("Acquire(second) error = %v", err)
	}
	resolver.setLimit(1)

	var lost, survivor *Lease
	select {
	case <-first.Done():
		lost, survivor = first, second
	case <-second.Done():
		lost, survivor = second, first
	case <-time.After(2 * time.Second):
		t.Fatal("no lease was shed after lowering policy")
	}
	if !teamquota.IsConcurrencyExceeded(lost.Err()) {
		t.Fatalf("shed Lease.Err() = %v, want concurrency exceeded", lost.Err())
	}
	select {
	case <-survivor.Done():
		t.Fatalf("both leases were shed after lowering to one; survivor error = %v", survivor.Err())
	case <-time.After(5 * renewInterval):
	}
	if used, err := limiter.Usage(context.Background(), "team-a", teamquota.KeyActiveConnectionCount); err != nil || used != 1 {
		t.Fatalf("Usage() = (%d, %v), want (1, nil)", used, err)
	}
	if err := survivor.Release(context.Background()); err != nil {
		t.Fatalf("Release(survivor) error = %v", err)
	}
}

func TestLimiterRedisLossEndsActiveLease(t *testing.T) {
	redisServer := miniredis.RunT(t)
	store := newTestRedisStore(t, redisServer)
	limiter := newTestLimiter(
		t,
		newConcurrencyResolver(1),
		&testAdmissionMarker{},
		store,
	)
	lease, err := limiter.Acquire(context.Background(), "team-a", teamquota.KeyActiveConnectionCount)
	if err != nil {
		t.Fatalf("Acquire() error = %v", err)
	}
	redisServer.Close()

	waitLeaseDone(t, lease)
	if !teamquota.IsUnavailable(lease.Err()) {
		t.Fatalf("Lease.Err() = %v, want unavailable", lease.Err())
	}
}

func TestLeaseRenewDeadlineFailsClosedAcrossBlockingCycleStages(t *testing.T) {
	type fixture struct {
		resolver Resolver
		marker   distributed.AtomicAdmissionMarker
		store    leaseStore
	}
	tests := []struct {
		name  string
		setup func(started chan struct{}, unblock chan struct{}, returned chan struct{}) fixture
	}{
		{
			name: "admission marker",
			setup: func(started chan struct{}, unblock chan struct{}, returned chan struct{}) fixture {
				return fixture{
					resolver: newConcurrencyResolver(1),
					marker:   newBlockingRenewalMarker(started, unblock, returned),
					store: &testLeaseStore{
						acquireDecision: leaseDecision{allowed: true, used: 1},
						renewDecision:   renewed,
					},
				}
			},
		},
		{
			name: "policy resolver",
			setup: func(started chan struct{}, unblock chan struct{}, returned chan struct{}) fixture {
				return fixture{
					resolver: newBlockingPolicyResolver(1, started, unblock, returned),
					marker:   &testAdmissionMarker{},
					store: &testLeaseStore{
						acquireDecision: leaseDecision{allowed: true, used: 1},
						renewDecision:   renewed,
					},
				}
			},
		},
		{
			name: "Redis renew",
			setup: func(started chan struct{}, unblock chan struct{}, returned chan struct{}) fixture {
				return fixture{
					resolver: newConcurrencyResolver(1),
					marker:   &testAdmissionMarker{},
					store: newBlockingLeaseStore(
						blockRenew,
						started,
						unblock,
						returned,
					),
				}
			},
		},
		{
			name: "Redis usage",
			setup: func(started chan struct{}, unblock chan struct{}, returned chan struct{}) fixture {
				return fixture{
					resolver: newConcurrencyResolver(1),
					marker:   &testAdmissionMarker{},
					store: newBlockingLeaseStore(
						blockUsage,
						started,
						unblock,
						returned,
					),
				}
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			started := make(chan struct{})
			unblock := make(chan struct{})
			returned := make(chan struct{})
			fixture := test.setup(started, unblock, returned)
			limiter := newRenewDeadlineTestLimiter(
				t,
				fixture.resolver,
				fixture.marker,
				fixture.store,
			)
			assertBlockingRenewalFailsBeforeTTL(t, limiter, started, unblock, returned)
		})
	}
}

func TestLeaseDoneDoesNotWaitForBlockingReleaseCleanup(t *testing.T) {
	releaseStarted := make(chan struct{})
	unblockRelease := make(chan struct{})
	store := &blockingReleaseLeaseStore{
		testLeaseStore: testLeaseStore{
			acquireDecision: leaseDecision{allowed: true, used: 1},
			renewErr:        errors.New("Redis renew failed"),
		},
		started: releaseStarted,
		unblock: unblockRelease,
	}
	limiter := newRenewDeadlineTestLimiter(
		t,
		newConcurrencyResolver(1),
		&testAdmissionMarker{},
		store,
	)
	lease, err := limiter.Acquire(
		context.Background(),
		"team-a",
		teamquota.KeyActiveConnectionCount,
	)
	if err != nil {
		t.Fatalf("Acquire() error = %v", err)
	}
	var unblockOnce sync.Once
	unblock := func() {
		unblockOnce.Do(func() { close(unblockRelease) })
	}
	defer unblock()

	select {
	case <-releaseStarted:
	case <-time.After(time.Second):
		t.Fatal("release cleanup did not start")
	}
	select {
	case <-lease.Done():
	default:
		t.Fatal("Lease.Done() remained open while release cleanup was blocked")
	}

	unblock()
	if err := lease.Release(context.Background()); err != nil {
		t.Fatalf("Release() error = %v", err)
	}
}

func TestNewLimiterValidatesHeartbeatAndRedisRequirements(t *testing.T) {
	resolver := newConcurrencyResolver(1)
	marker := &testAdmissionMarker{}
	store := &testLeaseStore{}
	for _, cfg := range []Config{
		{RegionID: ""},
		{RegionID: "region-a", LeaseTTL: 2 * time.Millisecond, RenewInterval: time.Millisecond},
		{RegionID: "region-a", LeaseTTL: 10 * time.Millisecond, RenewInterval: 5 * time.Millisecond},
		{RegionID: "region-a", LeaseTTL: 11 * time.Millisecond, RenewInterval: 6 * time.Millisecond},
		{RegionID: "region-a", LeaseTTL: 10*time.Millisecond + time.Microsecond, RenewInterval: time.Millisecond},
	} {
		if _, err := newLimiter(resolver, marker, store, cfg); err == nil {
			t.Fatalf("newLimiter(%+v) error = nil, want validation error", cfg)
		}
	}
	if _, err := NewRedisLimiter(context.Background(), resolver, Config{RegionID: "region-a"}); err == nil {
		t.Fatal("NewRedisLimiter() error = nil, want missing Redis error")
	}
}

func TestNewLimiterDerivesConservativeRenewDeadlineAtBoundaries(t *testing.T) {
	tests := []struct {
		name         string
		cfg          Config
		wantTTL      time.Duration
		wantInterval time.Duration
		wantDeadline time.Duration
	}{
		{
			name:         "defaults",
			cfg:          Config{RegionID: "region-a"},
			wantTTL:      defaultLeaseTTL,
			wantInterval: defaultRenewInterval,
			wantDeadline: 10 * time.Second,
		},
		{
			name: "minimum whole millisecond ratio",
			cfg: Config{
				RegionID:      "region-a",
				LeaseTTL:      3 * time.Millisecond,
				RenewInterval: time.Millisecond,
			},
			wantTTL:      3 * time.Millisecond,
			wantInterval: time.Millisecond,
			wantDeadline: 2 * time.Millisecond,
		},
		{
			name: "closest valid half TTL interval",
			cfg: Config{
				RegionID:      "region-a",
				LeaseTTL:      11 * time.Millisecond,
				RenewInterval: 5 * time.Millisecond,
			},
			wantTTL:      11 * time.Millisecond,
			wantInterval: 5 * time.Millisecond,
			wantDeadline: 8 * time.Millisecond,
		},
		{
			name: "long TTL caps cycle at one interval",
			cfg: Config{
				RegionID:      "region-a",
				LeaseTTL:      30 * time.Millisecond,
				RenewInterval: 5 * time.Millisecond,
			},
			wantTTL:      30 * time.Millisecond,
			wantInterval: 5 * time.Millisecond,
			wantDeadline: 10 * time.Millisecond,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			limiter, err := newLimiter(
				newConcurrencyResolver(1),
				&testAdmissionMarker{},
				&testLeaseStore{},
				test.cfg,
			)
			if err != nil {
				t.Fatalf("newLimiter() error = %v", err)
			}
			if limiter.leaseTTL != test.wantTTL ||
				limiter.renewInterval != test.wantInterval ||
				limiter.renewDeadlineOffset != test.wantDeadline {
				t.Fatalf(
					"heartbeat timing = ttl %s interval %s deadline %s, want %s %s %s",
					limiter.leaseTTL,
					limiter.renewInterval,
					limiter.renewDeadlineOffset,
					test.wantTTL,
					test.wantInterval,
					test.wantDeadline,
				)
			}
			cycleBudget := limiter.renewDeadlineOffset - limiter.renewInterval
			expirySafety := limiter.leaseTTL - limiter.renewDeadlineOffset
			if cycleBudget <= 0 ||
				cycleBudget > limiter.renewInterval ||
				cycleBudget > expirySafety {
				t.Fatalf(
					"cycle budget %s is invalid for interval %s and expiry safety %s",
					cycleBudget,
					limiter.renewInterval,
					expirySafety,
				)
			}
		})
	}
}

func TestLimiterCloseIsIdempotent(t *testing.T) {
	resolver := newConcurrencyResolver(1)
	marker := &testAdmissionMarker{}
	store := &testLeaseStore{}
	limiter := newTestLimiter(t, resolver, marker, store)
	limiter.closeMarker = true

	if err := limiter.Close(); err != nil {
		t.Fatalf("first Close() error = %v", err)
	}
	if err := limiter.Close(); err != nil {
		t.Fatalf("second Close() error = %v", err)
	}
	if marker.closeCallCount() != 1 || store.closeCallCount() != 1 {
		t.Fatalf(
			"Close() calls = marker %d store %d, want 1 each",
			marker.closeCallCount(),
			store.closeCallCount(),
		)
	}
}

const (
	testLeaseTTL      = 120 * time.Millisecond
	testRenewInterval = 30 * time.Millisecond

	renewDeadlineTestLeaseTTL      = 300 * time.Millisecond
	renewDeadlineTestInterval      = 50 * time.Millisecond
	renewDeadlineTestLatestFailure = renewDeadlineTestLeaseTTL - 25*time.Millisecond
)

func newTestLimiter(
	t *testing.T,
	resolver Resolver,
	marker distributed.AtomicAdmissionMarker,
	store leaseStore,
) *Limiter {
	t.Helper()
	limiter, err := newLimiter(resolver, marker, store, Config{
		RegionID:       "region-a",
		PolicyCacheTTL: 0,
		LeaseTTL:       testLeaseTTL,
		RenewInterval:  testRenewInterval,
	})
	if err != nil {
		t.Fatalf("newLimiter() error = %v", err)
	}
	return limiter
}

func newRenewDeadlineTestLimiter(
	t *testing.T,
	resolver Resolver,
	marker distributed.AtomicAdmissionMarker,
	store leaseStore,
) *Limiter {
	t.Helper()
	limiter, err := newLimiter(resolver, marker, store, Config{
		RegionID:       "region-a",
		PolicyCacheTTL: 0,
		LeaseTTL:       renewDeadlineTestLeaseTTL,
		RenewInterval:  renewDeadlineTestInterval,
	})
	if err != nil {
		t.Fatalf("newLimiter() error = %v", err)
	}
	return limiter
}

func assertBlockingRenewalFailsBeforeTTL(
	t *testing.T,
	limiter *Limiter,
	started <-chan struct{},
	unblock chan struct{},
	returned <-chan struct{},
) {
	t.Helper()
	acquiredAt := time.Now()
	lease, err := limiter.Acquire(
		context.Background(),
		"team-a",
		teamquota.KeyActiveConnectionCount,
	)
	if err != nil {
		t.Fatalf("Acquire() error = %v", err)
	}
	var unblockOnce sync.Once
	unblockBackend := func() {
		unblockOnce.Do(func() { close(unblock) })
	}
	defer unblockBackend()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("renewal cycle did not reach blocking backend")
	}

	connectionCtx, cancelConnection := context.WithCancel(context.Background())
	defer cancelConnection()
	go func() {
		<-lease.Done()
		cancelConnection()
	}()
	select {
	case <-connectionCtx.Done():
	case <-time.After(renewDeadlineTestLatestFailure):
		t.Fatal("Lease.Done() did not fail closed before the Redis TTL")
	}
	elapsed := time.Since(acquiredAt)
	if elapsed >= renewDeadlineTestLatestFailure {
		t.Fatalf(
			"Lease.Done() closed after %s, want before %s (Redis TTL %s)",
			elapsed,
			renewDeadlineTestLatestFailure,
			renewDeadlineTestLeaseTTL,
		)
	}
	if !teamquota.IsUnavailable(lease.Err()) ||
		!errors.Is(lease.Err(), context.DeadlineExceeded) {
		t.Fatalf("Lease.Err() = %v, want unavailable deadline exceeded", lease.Err())
	}

	unblockBackend()
	select {
	case <-returned:
	case <-time.After(time.Second):
		t.Fatal("blocked renewal backend did not return after test unblock")
	}
	if err := lease.Release(context.Background()); err != nil {
		t.Fatalf("Release() error = %v", err)
	}
}

func waitLeaseDone(t *testing.T, lease *Lease) {
	t.Helper()
	select {
	case <-lease.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("lease did not finish")
	}
}

type testConcurrencyResolver struct {
	mu       sync.RWMutex
	limit    int64
	disabled bool
	err      error
}

func newConcurrencyResolver(limit int64) *testConcurrencyResolver {
	return &testConcurrencyResolver{limit: limit}
}

func (r *testConcurrencyResolver) EffectivePolicy(
	_ context.Context,
	teamID string,
	key teamquota.Key,
) (*teamquota.Policy, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.err != nil {
		return nil, r.err
	}
	return &teamquota.Policy{
		TeamID: teamID,
		Key:    key,
		Kind:   teamquota.KindConcurrency,
		Limit:  r.limit,
	}, nil
}

func (r *testConcurrencyResolver) TeamAdmissionDisabled(context.Context, string) (bool, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.disabled, r.err
}

func (r *testConcurrencyResolver) setLimit(limit int64) {
	r.mu.Lock()
	r.limit = limit
	r.mu.Unlock()
}

type blockingPolicyResolver struct {
	*testConcurrencyResolver

	mu       sync.Mutex
	calls    int
	started  chan struct{}
	unblock  chan struct{}
	returned chan struct{}
}

func newBlockingPolicyResolver(
	limit int64,
	started chan struct{},
	unblock chan struct{},
	returned chan struct{},
) *blockingPolicyResolver {
	return &blockingPolicyResolver{
		testConcurrencyResolver: newConcurrencyResolver(limit),
		started:                 started,
		unblock:                 unblock,
		returned:                returned,
	}
}

func (r *blockingPolicyResolver) EffectivePolicy(
	ctx context.Context,
	teamID string,
	key teamquota.Key,
) (*teamquota.Policy, error) {
	r.mu.Lock()
	r.calls++
	call := r.calls
	r.mu.Unlock()
	if call == 1 {
		return r.testConcurrencyResolver.EffectivePolicy(ctx, teamID, key)
	}
	close(r.started)
	<-r.unblock
	defer close(r.returned)
	return nil, ctx.Err()
}

type testAdmissionMarker struct {
	mu       sync.RWMutex
	disabled bool
	err      error
	closes   int
}

func (m *testAdmissionMarker) Disabled(context.Context, string) (bool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.disabled, m.err
}

func (m *testAdmissionMarker) RedisKey(string) (string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return testAdmissionKey, m.err
}

func (m *testAdmissionMarker) Recover(context.Context, string) error { return nil }
func (m *testAdmissionMarker) Forget(context.Context, string) error  { return nil }
func (m *testAdmissionMarker) Disable(context.Context, string) error { return nil }
func (m *testAdmissionMarker) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closes++
	return nil
}

func (m *testAdmissionMarker) closeCallCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.closes
}

type blockingRenewalMarker struct {
	testAdmissionMarker

	mu       sync.Mutex
	calls    int
	started  chan struct{}
	unblock  chan struct{}
	returned chan struct{}
}

func newBlockingRenewalMarker(
	started chan struct{},
	unblock chan struct{},
	returned chan struct{},
) *blockingRenewalMarker {
	return &blockingRenewalMarker{
		started:  started,
		unblock:  unblock,
		returned: returned,
	}
}

func (m *blockingRenewalMarker) RedisKey(_ string) (string, error) {
	m.mu.Lock()
	m.calls++
	call := m.calls
	m.mu.Unlock()
	if call == 1 {
		return testAdmissionKey, nil
	}
	close(m.started)
	<-m.unblock
	defer close(m.returned)
	return "", context.DeadlineExceeded
}

type testLeaseStore struct {
	mu sync.Mutex

	acquireDecision leaseDecision
	acquireErr      error
	renewDecision   renewDecision
	renewErr        error
	usage           int64
	usageErr        error
	releaseErr      error
	releaseCalls    int
	closeCalls      int
}

func (s *testLeaseStore) Acquire(
	context.Context,
	string,
	string,
	string,
	int64,
	time.Duration,
	guard.Version,
) (leaseDecision, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.acquireDecision, s.acquireErr
}

func (s *testLeaseStore) Renew(
	context.Context,
	string,
	string,
	string,
	int64,
	time.Duration,
	guard.Version,
) (renewDecision, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.renewDecision, s.renewErr
}

func (s *testLeaseStore) setRenewError(err error) {
	s.mu.Lock()
	s.renewErr = err
	s.mu.Unlock()
}

func (s *testLeaseStore) Release(context.Context, string, string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.releaseCalls++
	return s.releaseErr
}

func (s *testLeaseStore) Usage(context.Context, string, time.Duration) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.usage, s.usageErr
}

func (s *testLeaseStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closeCalls++
	return nil
}

func (s *testLeaseStore) ReadPolicyGuard(context.Context) (guard.State, error) {
	return guard.State{
		Phase:   guard.PhaseStable,
		Version: testGuardVersion,
	}, nil
}

func (s *testLeaseStore) releaseCallCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.releaseCalls
}

func (s *testLeaseStore) closeCallCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closeCalls
}

type blockingLeaseOperation int

const (
	blockRenew blockingLeaseOperation = iota
	blockUsage
)

type blockingLeaseStore struct {
	testLeaseStore

	operation blockingLeaseOperation
	started   chan struct{}
	unblock   chan struct{}
	returned  chan struct{}
}

func newBlockingLeaseStore(
	operation blockingLeaseOperation,
	started chan struct{},
	unblock chan struct{},
	returned chan struct{},
) *blockingLeaseStore {
	renewDecision := renewed
	if operation == blockUsage {
		renewDecision = renewOverLimit
	}
	return &blockingLeaseStore{
		testLeaseStore: testLeaseStore{
			acquireDecision: leaseDecision{allowed: true, used: 1},
			renewDecision:   renewDecision,
		},
		operation: operation,
		started:   started,
		unblock:   unblock,
		returned:  returned,
	}
}

func (s *blockingLeaseStore) Renew(
	ctx context.Context,
	key string,
	leaseID string,
	admissionKey string,
	limit int64,
	leaseTTL time.Duration,
	version guard.Version,
) (renewDecision, error) {
	if s.operation != blockRenew {
		return s.testLeaseStore.Renew(ctx, key, leaseID, admissionKey, limit, leaseTTL, version)
	}
	close(s.started)
	<-s.unblock
	defer close(s.returned)
	return renewLost, ctx.Err()
}

func (s *blockingLeaseStore) Usage(
	ctx context.Context,
	key string,
	leaseTTL time.Duration,
) (int64, error) {
	if s.operation != blockUsage {
		return s.testLeaseStore.Usage(ctx, key, leaseTTL)
	}
	close(s.started)
	<-s.unblock
	defer close(s.returned)
	return 0, ctx.Err()
}

type blockingReleaseLeaseStore struct {
	testLeaseStore
	started chan struct{}
	unblock chan struct{}
}

func (s *blockingReleaseLeaseStore) Release(context.Context, string, string) error {
	close(s.started)
	<-s.unblock
	return nil
}

type observedRenewStore struct {
	*RedisStore

	pendingOnce sync.Once
	renewedOnce sync.Once
	pending     chan struct{}
	renewed     chan struct{}
}

func (s *observedRenewStore) Renew(
	ctx context.Context,
	key string,
	leaseID string,
	admissionKey string,
	limit int64,
	leaseTTL time.Duration,
	version guard.Version,
) (renewDecision, error) {
	decision, err := s.RedisStore.Renew(ctx, key, leaseID, admissionKey, limit, leaseTTL, version)
	if errors.Is(err, guard.ErrPending) {
		s.pendingOnce.Do(func() { close(s.pending) })
	}
	if err == nil && decision == renewed {
		s.renewedOnce.Do(func() { close(s.renewed) })
	}
	return decision, err
}

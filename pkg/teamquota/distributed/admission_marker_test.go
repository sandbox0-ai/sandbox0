package distributed

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
)

func TestRedisAdmissionMarkerIsNonExpiringAndRecoversAfterRedisLoss(t *testing.T) {
	redisServer := miniredis.RunT(t)
	resolver := &fakeAdmissionStateResolver{}
	marker := newTestAdmissionMarker(t, redisServer, resolver)
	ctx := context.Background()

	disabled, err := marker.Disabled(ctx, "team-a")
	if err != nil || disabled {
		t.Fatalf("initial Disabled() = (%v, %v), want active", disabled, err)
	}
	if resolver.callCount() != 1 {
		t.Fatalf("resolver calls = %d, want 1", resolver.callCount())
	}
	if ttl := redisServer.TTL(marker.redisKey("team-a")); ttl != 0 {
		t.Fatalf("active marker TTL = %s, want non-expiring", ttl)
	}

	resolver.setDisabled(true)
	disabled, err = marker.Disabled(ctx, "team-a")
	if err != nil || disabled {
		t.Fatalf("cached Disabled() = (%v, %v), want active", disabled, err)
	}
	if resolver.callCount() != 1 {
		t.Fatalf("resolver calls = %d, want non-expiring active cache", resolver.callCount())
	}

	redisServer.FlushAll()
	disabled, err = marker.Disabled(ctx, "team-a")
	if err != nil || !disabled {
		t.Fatalf("post-loss Disabled() = (%v, %v), want durable disabled recovery", disabled, err)
	}
	if resolver.callCount() != 2 {
		t.Fatalf("resolver calls = %d, want one post-loss recovery", resolver.callCount())
	}
	if ttl := redisServer.TTL(marker.redisKey("team-a")); ttl != 0 {
		t.Fatalf("disabled marker TTL = %s, want tombstone-lifecycle ownership", ttl)
	}
}

func TestRedisAdmissionMarkerRecoveryCollapsesAcrossProcesses(t *testing.T) {
	redisServer := miniredis.RunT(t)
	started := make(chan struct{})
	release := make(chan struct{})
	var startOnce sync.Once
	resolver := &fakeAdmissionStateResolver{
		onResolve: func() {
			startOnce.Do(func() { close(started) })
			<-release
		},
	}
	first := newTestAdmissionMarker(t, redisServer, resolver)
	second := newTestAdmissionMarker(t, redisServer, resolver)

	const callers = 32
	errs := make(chan error, callers)
	for i := 0; i < callers; i++ {
		marker := first
		if i%2 == 1 {
			marker = second
		}
		go func() {
			disabled, err := marker.Disabled(context.Background(), "team-a")
			if err == nil && disabled {
				err = errors.New("active team resolved as disabled")
			}
			errs <- err
		}()
	}
	<-started
	close(release)
	for i := 0; i < callers; i++ {
		if err := <-errs; err != nil {
			t.Fatalf("Disabled() error = %v", err)
		}
	}
	if resolver.callCount() != 1 {
		t.Fatalf("resolver calls = %d, want one distributed recovery", resolver.callCount())
	}
}

func TestRedisAdmissionMarkerNegativeRecoveryCacheAvoidsRedisAndPostgresFlood(t *testing.T) {
	redisServer := miniredis.RunT(t)
	resolverErr := errors.New("postgres unavailable")
	resolver := &fakeAdmissionStateResolver{err: resolverErr}
	marker := newTestAdmissionMarker(t, redisServer, resolver)

	if _, err := marker.Disabled(context.Background(), "team-a"); !errors.Is(err, resolverErr) {
		t.Fatalf("initial Disabled() error = %v, want resolver error", err)
	}
	if resolver.callCount() != 1 {
		t.Fatalf("resolver calls = %d, want 1", resolver.callCount())
	}

	// A cached recovery failure is checked before RedisKey returns. Closing
	// Redis proves repeated attempts neither touch Redis nor PostgreSQL.
	redisServer.Close()
	for i := 0; i < 20; i++ {
		if _, err := marker.Disabled(context.Background(), "team-a"); !errors.Is(err, resolverErr) {
			t.Fatalf("cached Disabled(%d) error = %v, want resolver error", i, err)
		}
	}
	if resolver.callCount() != 1 {
		t.Fatalf("resolver calls = %d, want negative-cache collapse", resolver.callCount())
	}
}

func TestRedisAdmissionMarkerDisabledWriteWinsRecoveryRace(t *testing.T) {
	redisServer := miniredis.RunT(t)
	resolver := &fakeAdmissionStateResolver{}
	marker := newTestAdmissionMarker(t, redisServer, resolver)
	resolver.onResolve = func() {
		if err := marker.Disable(context.Background(), "team-a"); err != nil {
			t.Errorf("Disable() during resolver race: %v", err)
		}
	}

	disabled, err := marker.Disabled(context.Background(), "team-a")
	if err != nil || !disabled {
		t.Fatalf("Disabled() race = (%v, %v), want disabled", disabled, err)
	}
	value, err := redisServer.Get(marker.redisKey("team-a"))
	if err != nil {
		t.Fatalf("get final marker: %v", err)
	}
	if value != admissionMarkerDisabled {
		t.Fatalf("final marker = %q, want %q", value, admissionMarkerDisabled)
	}
}

func TestRedisAdmissionMarkerFailsClosedOnCorruptionAndForgetsAfterPrune(t *testing.T) {
	redisServer := miniredis.RunT(t)
	resolver := &fakeAdmissionStateResolver{}
	marker := newTestAdmissionMarker(t, redisServer, resolver)
	key := marker.redisKey("team-a")
	redisServer.Set(key, "corrupt")

	if _, err := marker.Disabled(context.Background(), "team-a"); err == nil {
		t.Fatal("Disabled() corrupt marker error = nil")
	}
	if err := marker.Disable(context.Background(), "team-a"); err != nil {
		t.Fatalf("Disable() error = %v", err)
	}
	if err := marker.Forget(context.Background(), "team-a"); err != nil {
		t.Fatalf("Forget() error = %v", err)
	}
	if redisServer.Exists(key) {
		t.Fatal("marker still exists after durable tombstone prune")
	}
}

func newTestAdmissionMarker(
	t *testing.T,
	redisServer *miniredis.Miniredis,
	resolver *fakeAdmissionStateResolver,
) *RedisAdmissionMarker {
	t.Helper()
	marker, err := NewRedisAdmissionMarker(context.Background(), resolver, AdmissionMarkerConfig{
		RegionID:           "sg",
		RedisURL:           "redis://" + redisServer.Addr() + "/0",
		KeyPrefix:          "test:teamquota",
		Timeout:            time.Second,
		RecoveryFailureTTL: time.Second,
		RecoveryLockTTL:    time.Second,
	})
	if err != nil {
		t.Fatalf("NewRedisAdmissionMarker() error = %v", err)
	}
	t.Cleanup(func() {
		_ = marker.Close()
	})
	return marker
}

type fakeAdmissionStateResolver struct {
	mu        sync.Mutex
	disabled  bool
	err       error
	calls     int
	onResolve func()
}

func (r *fakeAdmissionStateResolver) TeamAdmissionDisabled(context.Context, string) (bool, error) {
	r.mu.Lock()
	r.calls++
	disabled := r.disabled
	err := r.err
	onResolve := r.onResolve
	r.mu.Unlock()
	if onResolve != nil {
		onResolve()
	}
	return disabled, err
}

func (r *fakeAdmissionStateResolver) callCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.calls
}

func (r *fakeAdmissionStateResolver) setDisabled(disabled bool) {
	r.mu.Lock()
	r.disabled = disabled
	r.mu.Unlock()
}

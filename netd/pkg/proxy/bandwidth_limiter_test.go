package proxy

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	"github.com/sandbox0-ai/sandbox0/netd/pkg/policy"
	"github.com/sandbox0-ai/sandbox0/pkg/teamquota"
	"github.com/sandbox0-ai/sandbox0/pkg/teamquota/guard"
	"github.com/sandbox0-ai/sandbox0/pkg/tokenbucket"
)

func TestBandwidthLimiterRetainsPerSandboxLocalQoS(t *testing.T) {
	limiter := newBandwidthLimiter(&config.NetdConfig{
		EgressBandwidthBytesPerSecond: 100,
		BandwidthBurstBytes:           100,
	}, nil)
	now := time.Unix(100, 0)
	limiter.now = func() time.Time { return now }

	first := &policy.CompiledPolicy{SandboxID: "sandbox-1", TeamID: "team-1"}
	if delay := limiter.reserve(first, bandwidthEgress, 100, 100); delay != 0 {
		t.Fatalf("first reservation delay = %s, want 0", delay)
	}
	if delay := limiter.reserve(first, bandwidthEgress, 100, 100); delay < 999*time.Millisecond || delay > time.Second {
		t.Fatalf("second reservation delay = %s, want about 1s", delay)
	}
	second := &policy.CompiledPolicy{SandboxID: "sandbox-2", TeamID: "team-1"}
	if delay := limiter.reserve(second, bandwidthEgress, 100, 100); delay != 0 {
		t.Fatalf("other sandbox reservation delay = %s, want 0", delay)
	}
}

func TestBandwidthLimiterBoundsHistoricalSandboxBuckets(t *testing.T) {
	limiter := newBandwidthLimiter(&config.NetdConfig{
		EgressBandwidthBytesPerSecond: 100,
		BandwidthBurstBytes:           1000,
	}, nil)
	now := time.Unix(100, 0)
	limiter.now = func() time.Time { return now }

	for i := 0; i < maxLocalBandwidthBuckets+100; i++ {
		compiled := &policy.CompiledPolicy{
			SandboxID: fmt.Sprintf("sandbox-%05d", i),
			TeamID:    "team-1",
		}
		if delay := limiter.reserve(compiled, bandwidthEgress, 1, 100); delay != 0 {
			t.Fatalf("reservation %d delay = %s, want 0", i, delay)
		}
	}

	if got := len(limiter.buckets); got > maxLocalBandwidthBuckets {
		t.Fatalf("local bucket count = %d, want <= %d", got, maxLocalBandwidthBuckets)
	}
	overflow := limiter.buckets[bandwidthKey{direction: bandwidthEgress, overflow: true}]
	if overflow == nil {
		t.Fatal("overflow bucket is missing")
	}
}

func TestBandwidthLimiterPrunesFullyReplenishedIdleBuckets(t *testing.T) {
	limiter := newBandwidthLimiter(&config.NetdConfig{
		EgressBandwidthBytesPerSecond: 100,
		BandwidthBurstBytes:           100,
	}, nil)
	now := time.Unix(100, 0)
	limiter.now = func() time.Time { return now }
	stale := &policy.CompiledPolicy{SandboxID: "stale", TeamID: "team-1"}
	if delay := limiter.reserve(stale, bandwidthEgress, 100, 100); delay != 0 {
		t.Fatalf("stale reservation delay = %s, want 0", delay)
	}

	now = now.Add(localBandwidthBucketTTL + time.Second)
	fresh := &policy.CompiledPolicy{SandboxID: "fresh", TeamID: "team-1"}
	if delay := limiter.reserve(fresh, bandwidthEgress, 1, 100); delay != 0 {
		t.Fatalf("fresh reservation delay = %s, want 0", delay)
	}

	if _, ok := limiter.buckets[bandwidthLimitKey(stale, bandwidthEgress)]; ok {
		t.Fatal("idle bucket was not pruned")
	}
}

func TestBandwidthLimitedWriterConsumesAndForwardsEffectiveBurstChunks(t *testing.T) {
	resolver := &fakeNetworkPolicyResolver{policy: networkRatePolicy(teamquota.KeyNetworkEgressBytes, 3)}
	bucket := &fakeNetworkBucket{decision: tokenbucket.Decision{Allowed: true}}
	teamLimiter, err := NewTeamNetworkQuota("region-1", resolver, &fakeNetworkAdmissionMarker{}, bucket, time.Minute)
	if err != nil {
		t.Fatalf("NewTeamNetworkQuota() error = %v", err)
	}
	limiter := newBandwidthLimiter(&config.NetdConfig{}, teamLimiter)
	dst := &observingWriter{bucket: bucket}
	writer := limiter.limitedWriter(
		context.Background(),
		dst,
		&policy.CompiledPolicy{SandboxID: "sandbox-1", TeamID: "team-1"},
		bandwidthEgress,
	)

	payload := bytes.Repeat([]byte("x"), 8)
	n, err := writer.Write(payload)
	if err != nil || n != len(payload) {
		t.Fatalf("Write() = %d, %v; want %d, nil", n, err, len(payload))
	}
	if got, want := dst.writeSizes, []int{3, 3, 2}; !equalInts(got, want) {
		t.Fatalf("write sizes = %v, want %v", got, want)
	}
	if got, want := dst.takeCounts, []int{1, 2, 3}; !equalInts(got, want) {
		t.Fatalf("token takes observed before writes = %v, want %v", got, want)
	}
	if got, want := bucket.costs, []int64{3, 3, 3}; !equalInt64s(got, want) {
		t.Fatalf("token costs = %v, want %v", got, want)
	}
	wantKey := "team-quota:v1:8:region-1:6:team-1:network_egress_bytes"
	for _, key := range bucket.keys {
		if key != wantKey {
			t.Fatalf("bucket key = %q, want %q", key, wantKey)
		}
	}
}

func TestBandwidthLimitedWriterSharesRegionTeamBucketAcrossSandboxes(t *testing.T) {
	resolver := &fakeNetworkPolicyResolver{policy: networkRatePolicy(teamquota.KeyNetworkEgressBytes, 10)}
	bucket := &fakeNetworkBucket{decision: tokenbucket.Decision{Allowed: true}}
	teamLimiter, err := NewTeamNetworkQuota("region-1", resolver, &fakeNetworkAdmissionMarker{}, bucket, time.Minute)
	if err != nil {
		t.Fatalf("NewTeamNetworkQuota() error = %v", err)
	}
	limiter := newBandwidthLimiter(&config.NetdConfig{}, teamLimiter)

	for _, sandboxID := range []string{"sandbox-a", "sandbox-b"} {
		writer := limiter.limitedWriter(
			context.Background(),
			io.Discard,
			&policy.CompiledPolicy{SandboxID: sandboxID, TeamID: "team-1"},
			bandwidthEgress,
		)
		if _, err := writer.Write([]byte("x")); err != nil {
			t.Fatalf("Write(%s) error = %v", sandboxID, err)
		}
	}
	if len(bucket.keys) != 1 || bucket.keys[0] != "team-quota:v1:8:region-1:6:team-1:network_egress_bytes" {
		t.Fatalf("bucket keys = %v, want one region/team/direction scope", bucket.keys)
	}
}

func TestTeamNetworkQuotaRejectsDisabledTeamAtomicallyWithTokenMutation(t *testing.T) {
	resolver := &fakeNetworkPolicyResolver{policy: networkRatePolicy(teamquota.KeyNetworkEgressBytes, 10)}
	marker := &fakeNetworkAdmissionMarker{disabled: true}
	bucket := &fakeNetworkBucket{decision: tokenbucket.Decision{Allowed: true}}
	teamLimiter, err := NewTeamNetworkQuota("region-1", resolver, marker, bucket, time.Minute)
	if err != nil {
		t.Fatalf("NewTeamNetworkQuota() error = %v", err)
	}

	if err := teamLimiter.waitN(context.Background(), "team-1", bandwidthEgress, 1); err == nil {
		t.Fatal("waitN() error = nil, want disabled admission failure")
	}
	if marker.disabledCalls != 0 {
		t.Fatalf("non-atomic admission marker calls = %d, want 0", marker.disabledCalls)
	}
	if marker.redisKeyCalls != 1 {
		t.Fatalf("atomic admission marker key calls = %d, want 1", marker.redisKeyCalls)
	}
	if len(bucket.costs) != 0 {
		t.Fatalf("token bucket costs = %v, want no token operation", bucket.costs)
	}
}

func TestTeamNetworkQuotaCloseReleasesDistributedClients(t *testing.T) {
	resolver := &fakeNetworkPolicyResolver{policy: networkRatePolicy(teamquota.KeyNetworkEgressBytes, 10)}
	marker := &fakeNetworkAdmissionMarker{}
	bucket := &fakeNetworkBucket{decision: tokenbucket.Decision{Allowed: true}}
	teamLimiter, err := NewTeamNetworkQuota("region-1", resolver, marker, bucket, time.Minute)
	if err != nil {
		t.Fatalf("NewTeamNetworkQuota() error = %v", err)
	}

	if err := teamLimiter.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if marker.closeCalls != 1 {
		t.Fatalf("admission marker close calls = %d, want 1", marker.closeCalls)
	}
	if bucket.closeCalls != 1 {
		t.Fatalf("token bucket close calls = %d, want 1", bucket.closeCalls)
	}
}

func TestNetworkQuotaDirectionKeys(t *testing.T) {
	tests := []struct {
		direction bandwidthDirection
		want      teamquota.Key
	}{
		{direction: bandwidthEgress, want: teamquota.KeyNetworkEgressBytes},
		{direction: bandwidthIngress, want: teamquota.KeyNetworkIngressBytes},
	}
	for _, tt := range tests {
		got, err := networkQuotaKey(tt.direction)
		if err != nil {
			t.Fatalf("networkQuotaKey(%q) error = %v", tt.direction, err)
		}
		if got != tt.want {
			t.Fatalf("networkQuotaKey(%q) = %q, want %q", tt.direction, got, tt.want)
		}
	}
}

func TestBandwidthLimitedWriterFailsClosed(t *testing.T) {
	tests := []struct {
		name     string
		teamID   string
		resolver *fakeNetworkPolicyResolver
		bucket   *fakeNetworkBucket
	}{
		{
			name:     "missing team",
			resolver: &fakeNetworkPolicyResolver{policy: networkRatePolicy(teamquota.KeyNetworkEgressBytes, 10)},
			bucket:   &fakeNetworkBucket{decision: tokenbucket.Decision{Allowed: true}},
		},
		{
			name:     "missing policy",
			teamID:   "team-1",
			resolver: &fakeNetworkPolicyResolver{},
			bucket:   &fakeNetworkBucket{decision: tokenbucket.Decision{Allowed: true}},
		},
		{
			name:     "postgres failure",
			teamID:   "team-1",
			resolver: &fakeNetworkPolicyResolver{err: errors.New("postgres down")},
			bucket:   &fakeNetworkBucket{decision: tokenbucket.Decision{Allowed: true}},
		},
		{
			name:     "redis failure",
			teamID:   "team-1",
			resolver: &fakeNetworkPolicyResolver{policy: networkRatePolicy(teamquota.KeyNetworkEgressBytes, 10)},
			bucket:   &fakeNetworkBucket{err: errors.New("redis down")},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			teamLimiter, err := NewTeamNetworkQuota("region-1", tt.resolver, &fakeNetworkAdmissionMarker{}, tt.bucket, time.Minute)
			if err != nil {
				t.Fatalf("NewTeamNetworkQuota() error = %v", err)
			}
			limiter := newBandwidthLimiter(&config.NetdConfig{}, teamLimiter)
			var dst bytes.Buffer
			writer := limiter.limitedWriter(
				context.Background(),
				&dst,
				&policy.CompiledPolicy{SandboxID: "sandbox-1", TeamID: tt.teamID},
				bandwidthEgress,
			)
			if _, err := writer.Write([]byte("x")); err == nil {
				t.Fatal("Write() error = nil, want fail-closed error")
			}
			if dst.Len() != 0 {
				t.Fatalf("forwarded bytes = %d, want 0", dst.Len())
			}
		})
	}
}

func TestBandwidthLimiterWaitIsCancellable(t *testing.T) {
	resolver := &fakeNetworkPolicyResolver{policy: networkRatePolicy(teamquota.KeyNetworkEgressBytes, 10)}
	bucket := &fakeNetworkBucket{decision: tokenbucket.Decision{
		Allowed:    false,
		RetryAfter: time.Hour,
	}}
	teamLimiter, err := NewTeamNetworkQuota("region-1", resolver, &fakeNetworkAdmissionMarker{}, bucket, time.Minute)
	if err != nil {
		t.Fatalf("NewTeamNetworkQuota() error = %v", err)
	}
	limiter := newBandwidthLimiter(&config.NetdConfig{}, teamLimiter)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err = limiter.wait(ctx, &policy.CompiledPolicy{TeamID: "team-1"}, bandwidthEgress, 1)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("wait() error = %v, want context.Canceled", err)
	}
}

func networkRatePolicy(key teamquota.Key, burst int64) *teamquota.Policy {
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

type fakeNetworkPolicyResolver struct {
	policy *teamquota.Policy
	err    error
	calls  int
}

func (r *fakeNetworkPolicyResolver) EffectivePolicy(_ context.Context, _ string, _ teamquota.Key) (*teamquota.Policy, error) {
	r.calls++
	if r.policy == nil {
		return nil, r.err
	}
	policy := *r.policy
	return &policy, r.err
}

type fakeNetworkBucket struct {
	decision   tokenbucket.Decision
	err        error
	keys       []string
	costs      []int64
	closeCalls int
}

func (b *fakeNetworkBucket) TakeN(_ context.Context, key string, _ tokenbucket.Policy, cost int64) (tokenbucket.Decision, error) {
	b.keys = append(b.keys, key)
	b.costs = append(b.costs, cost)
	return b.decision, b.err
}

func (b *fakeNetworkBucket) TakeNGuarded(
	ctx context.Context,
	key string,
	admissionKey string,
	policy tokenbucket.Policy,
	_ guard.Version,
	_ time.Time,
	cost int64,
) (tokenbucket.Decision, error) {
	if strings.Contains(admissionKey, ":disabled:") {
		return tokenbucket.Decision{}, tokenbucket.ErrAdmissionDisabled
	}
	return b.TakeN(ctx, key, policy, cost)
}

func (*fakeNetworkBucket) ReadPolicyGuard(context.Context) (guard.State, error) {
	return guard.State{
		Phase: guard.PhaseStable,
		Version: guard.Version{
			EnforcementEpoch: 1,
			RedisGeneration:  1,
		},
	}, nil
}

func (b *fakeNetworkBucket) Close() error {
	b.closeCalls++
	return nil
}

type fakeNetworkAdmissionMarker struct {
	disabled      bool
	err           error
	disabledCalls int
	redisKeyCalls int
	closeCalls    int
}

func (m *fakeNetworkAdmissionMarker) Disabled(context.Context, string) (bool, error) {
	m.disabledCalls++
	return m.disabled, m.err
}

func (*fakeNetworkAdmissionMarker) Disable(context.Context, string) error { return nil }

func (m *fakeNetworkAdmissionMarker) RedisKey(teamID string) (string, error) {
	m.redisKeyCalls++
	if m.err != nil {
		return "", m.err
	}
	if m.disabled {
		return "admission:disabled:" + teamID, nil
	}
	return "admission:" + teamID, nil
}

func (*fakeNetworkAdmissionMarker) Recover(context.Context, string) error { return nil }
func (*fakeNetworkAdmissionMarker) Forget(context.Context, string) error  { return nil }

func (m *fakeNetworkAdmissionMarker) Close() error {
	m.closeCalls++
	return nil
}

type observingWriter struct {
	bucket     *fakeNetworkBucket
	writeSizes []int
	takeCounts []int
}

func (w *observingWriter) Write(p []byte) (int, error) {
	w.writeSizes = append(w.writeSizes, len(p))
	w.takeCounts = append(w.takeCounts, len(w.bucket.costs))
	return len(p), nil
}

func equalInts(left, right []int) bool {
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

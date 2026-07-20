package proxy

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	"github.com/sandbox0-ai/sandbox0/netd/pkg/policy"
	"github.com/sandbox0-ai/sandbox0/pkg/quota"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type bandwidthPolicyStore struct {
	policies map[quota.Dimension]*quota.Policy
}

func (s bandwidthPolicyStore) GetPolicy(_ context.Context, teamID string, dimension quota.Dimension) (*quota.Policy, error) {
	policy := s.policies[dimension]
	if policy == nil {
		return nil, nil
	}
	out := *policy
	out.TeamID = teamID
	return &out, nil
}

func TestBandwidthLimitedWriterThrottlesPerSandbox(t *testing.T) {
	limiter, err := newBandwidthLimiter(context.Background(), &config.NetdConfig{
		EgressBandwidthBytesPerSecond: 100,
		BandwidthBurstBytes:           100,
	})
	if err != nil {
		t.Fatalf("newBandwidthLimiter() error = %v", err)
	}
	defer limiter.Close()
	var slept []time.Duration
	limiter.sleep = func(delay time.Duration) { slept = append(slept, delay) }

	var buf bytes.Buffer
	writer := limiter.limitedWriter(&buf, &policy.CompiledPolicy{
		SandboxID: "sandbox-1",
		TeamID:    "team-1",
	}, bandwidthEgress)
	payload := bytes.Repeat([]byte("x"), 200)
	if n, err := writer.Write(payload); err != nil || n != len(payload) {
		t.Fatalf("Write() = %d, %v; want %d, nil", n, err, len(payload))
	}
	if !bytes.Equal(buf.Bytes(), payload) {
		t.Fatal("writer did not forward the full payload")
	}
	if len(slept) != 1 || slept[0] < 900*time.Millisecond || slept[0] > time.Second {
		t.Fatalf("sleep calls = %v, want one delay close to 1s", slept)
	}
}

func TestBandwidthLimitedWriterSkipsUnconfiguredDirection(t *testing.T) {
	limiter, err := newBandwidthLimiter(context.Background(), &config.NetdConfig{
		EgressBandwidthBytesPerSecond: 100,
	})
	if err != nil {
		t.Fatalf("newBandwidthLimiter() error = %v", err)
	}
	defer limiter.Close()
	limiter.sleep = func(delay time.Duration) {
		t.Fatalf("unexpected sleep %s", delay)
	}

	var buf bytes.Buffer
	writer := limiter.limitedWriter(&buf, &policy.CompiledPolicy{SandboxID: "sandbox-1"}, bandwidthIngress)
	payload := bytes.Repeat([]byte("x"), 200)
	if n, err := writer.Write(payload); err != nil || n != len(payload) {
		t.Fatalf("Write() = %d, %v; want %d, nil", n, err, len(payload))
	}
}

func TestTeamBandwidthQuotaIsSharedAcrossClusters(t *testing.T) {
	redisServer := miniredis.RunT(t)
	policies := bandwidthPolicyStore{policies: map[quota.Dimension]*quota.Policy{
		quota.DimensionNetworkEgress: {
			Dimension:  quota.DimensionNetworkEgress,
			Kind:       quota.KindRate,
			LimitValue: 100,
			IntervalMS: 1000,
			BurstValue: 100,
			Source:     quota.SourceRegionDefault,
		},
	}}
	first := newTestTeamLimiter(t, redisServer, "cluster-1", policies)
	defer first.Close()
	second := newTestTeamLimiter(t, redisServer, "cluster-2", policies)
	defer second.Close()

	var slept []time.Duration
	first.sleep = func(delay time.Duration) { slept = append(slept, delay) }
	second.sleep = func(delay time.Duration) { slept = append(slept, delay) }
	payload := bytes.Repeat([]byte("x"), 100)
	for index, limiter := range []*bandwidthLimiter{first, second} {
		writer := limiter.limitedWriter(&bytes.Buffer{}, &policy.CompiledPolicy{
			SandboxID: "sandbox-" + string(rune('1'+index)),
			TeamID:    "team-1",
		}, bandwidthEgress)
		if n, err := writer.Write(payload); err != nil || n != len(payload) {
			t.Fatalf("Write(%d) = %d, %v; want %d, nil", index, n, err, len(payload))
		}
	}
	if len(slept) != 1 || slept[0] < 900*time.Millisecond || slept[0] > time.Second {
		t.Fatalf("sleep calls = %v, want one shared-team delay close to 1s", slept)
	}
}

func TestTeamBandwidthQuotaSeparatesTeams(t *testing.T) {
	redisServer := miniredis.RunT(t)
	policies := bandwidthPolicyStore{policies: map[quota.Dimension]*quota.Policy{
		quota.DimensionNetworkEgress: {
			Dimension:  quota.DimensionNetworkEgress,
			Kind:       quota.KindRate,
			LimitValue: 100,
			IntervalMS: 1000,
			BurstValue: 100,
			Source:     quota.SourceRegionDefault,
		},
	}}
	limiter := newTestTeamLimiter(t, redisServer, "cluster-1", policies)
	defer limiter.Close()
	limiter.sleep = func(delay time.Duration) {
		t.Fatalf("unexpected sleep %s", delay)
	}

	payload := bytes.Repeat([]byte("x"), 100)
	for _, compiled := range []*policy.CompiledPolicy{
		{SandboxID: "sandbox-1", TeamID: "team-1"},
		{SandboxID: "sandbox-2", TeamID: "team-2"},
	} {
		writer := limiter.limitedWriter(&bytes.Buffer{}, compiled, bandwidthEgress)
		if n, err := writer.Write(payload); err != nil || n != len(payload) {
			t.Fatalf("Write(%s) = %d, %v; want %d, nil", compiled.TeamID, n, err, len(payload))
		}
	}
}

func TestTeamBandwidthQuotaDropsDatagramWithoutTokens(t *testing.T) {
	redisServer := miniredis.RunT(t)
	policies := bandwidthPolicyStore{policies: map[quota.Dimension]*quota.Policy{
		quota.DimensionNetworkEgress: {
			Dimension:  quota.DimensionNetworkEgress,
			Kind:       quota.KindRate,
			LimitValue: 100,
			IntervalMS: 1000,
			BurstValue: 100,
			Source:     quota.SourceTeamOverride,
		},
	}}
	limiter := newTestTeamLimiter(t, redisServer, "cluster-1", policies)
	defer limiter.Close()
	compiled := &policy.CompiledPolicy{SandboxID: "sandbox-1", TeamID: "team-1"}
	if err := limiter.waitDatagram(context.Background(), compiled, bandwidthEgress, 100); err != nil {
		t.Fatalf("first datagram: %v", err)
	}
	if err := limiter.waitDatagram(context.Background(), compiled, bandwidthEgress, 100); err == nil {
		t.Fatal("second datagram error = nil, want bounded drop")
	}
}

func TestTeamBandwidthQuotaFailsClosedWhenRedisIsUnavailable(t *testing.T) {
	_, err := newQuotaTeamBandwidthLimiter(context.Background(), &config.NetdConfig{
		RedisURL:      "redis://127.0.0.1:1/0",
		RedisTimeout:  metav1.Duration{Duration: time.Millisecond},
		RedisFailOpen: true,
	}, bandwidthPolicyStore{})
	if err == nil {
		t.Fatal("newQuotaTeamBandwidthLimiter() error = nil, want redis connection error")
	}
}

func newTestTeamLimiter(t *testing.T, redisServer *miniredis.Miniredis, clusterID string, policies quota.PolicyStore) *bandwidthLimiter {
	t.Helper()
	team, err := newQuotaTeamBandwidthLimiter(context.Background(), &config.NetdConfig{
		RedisURL:       "redis://" + redisServer.Addr() + "/0",
		RedisKeyPrefix: "test",
		RedisTimeout:   metav1.Duration{Duration: time.Second},
		RegionID:       "region-1",
		ClusterID:      clusterID,
	}, policies)
	if err != nil {
		t.Fatalf("newQuotaTeamBandwidthLimiter() error = %v", err)
	}
	return &bandwidthLimiter{team: team, sleep: time.Sleep}
}

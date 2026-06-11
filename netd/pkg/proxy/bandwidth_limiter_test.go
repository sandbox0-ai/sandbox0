package proxy

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	"github.com/sandbox0-ai/sandbox0/netd/pkg/policy"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestBandwidthLimitedWriterThrottlesPerSandbox(t *testing.T) {
	limiter, err := newBandwidthLimiter(context.Background(), &config.NetdConfig{
		EgressBandwidthBytesPerSecond: 100,
		BandwidthBurstBytes:           100,
	})
	if err != nil {
		t.Fatalf("newBandwidthLimiter() error = %v", err)
	}
	var slept []time.Duration
	limiter.now = func() time.Time { return time.Unix(0, 0) }
	limiter.sleep = func(d time.Duration) { slept = append(slept, d) }

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
	limiter.sleep = func(d time.Duration) {
		t.Fatalf("unexpected sleep %s", d)
	}

	var buf bytes.Buffer
	writer := limiter.limitedWriter(&buf, &policy.CompiledPolicy{SandboxID: "sandbox-1"}, bandwidthIngress)
	payload := bytes.Repeat([]byte("x"), 200)
	if n, err := writer.Write(payload); err != nil || n != len(payload) {
		t.Fatalf("Write() = %d, %v; want %d, nil", n, err, len(payload))
	}
}

func TestBandwidthLimitedWriterThrottlesRedisTeamAcrossSandboxes(t *testing.T) {
	redisServer := miniredis.RunT(t)
	limiter, err := newBandwidthLimiter(context.Background(), &config.NetdConfig{
		RedisURL:                           "redis://" + redisServer.Addr() + "/0",
		RedisKeyPrefix:                     "test",
		RedisTimeout:                       metav1.Duration{Duration: time.Second},
		RedisFailOpen:                      false,
		RegionID:                           "region-1",
		ClusterID:                          "cluster-1",
		TeamEgressBandwidthBytesPerSecond:  100,
		TeamIngressBandwidthBytesPerSecond: 100,
		TeamBandwidthBurstBytes:            100,
		EgressBandwidthBytesPerSecond:      0,
		IngressBandwidthBytesPerSecond:     0,
		BandwidthBurstBytes:                0,
	})
	if err != nil {
		t.Fatalf("newBandwidthLimiter() error = %v", err)
	}
	defer limiter.Close()

	var slept []time.Duration
	limiter.sleep = func(d time.Duration) { slept = append(slept, d) }

	first := limiter.limitedWriter(&bytes.Buffer{}, &policy.CompiledPolicy{SandboxID: "sandbox-1", TeamID: "team-1"}, bandwidthEgress)
	second := limiter.limitedWriter(&bytes.Buffer{}, &policy.CompiledPolicy{SandboxID: "sandbox-2", TeamID: "team-1"}, bandwidthEgress)
	payload := bytes.Repeat([]byte("x"), 100)
	if n, err := first.Write(payload); err != nil || n != len(payload) {
		t.Fatalf("first Write() = %d, %v; want %d, nil", n, err, len(payload))
	}
	if n, err := second.Write(payload); err != nil || n != len(payload) {
		t.Fatalf("second Write() = %d, %v; want %d, nil", n, err, len(payload))
	}
	if len(slept) != 1 || slept[0] < 900*time.Millisecond || slept[0] > time.Second {
		t.Fatalf("sleep calls = %v, want one delay close to 1s", slept)
	}
}

func TestBandwidthLimitedWriterSeparatesRedisTeamBuckets(t *testing.T) {
	redisServer := miniredis.RunT(t)
	limiter, err := newBandwidthLimiter(context.Background(), &config.NetdConfig{
		RedisURL:                          "redis://" + redisServer.Addr() + "/0",
		RedisKeyPrefix:                    "test",
		RedisTimeout:                      metav1.Duration{Duration: time.Second},
		RedisFailOpen:                     false,
		RegionID:                          "region-1",
		ClusterID:                         "cluster-1",
		TeamEgressBandwidthBytesPerSecond: 100,
		TeamBandwidthBurstBytes:           100,
	})
	if err != nil {
		t.Fatalf("newBandwidthLimiter() error = %v", err)
	}
	defer limiter.Close()
	limiter.sleep = func(d time.Duration) {
		t.Fatalf("unexpected sleep %s", d)
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

func TestRedisTeamBandwidthLimiterFailOpenAllowsRedisErrors(t *testing.T) {
	limiter, err := newBandwidthLimiter(context.Background(), &config.NetdConfig{
		RedisURL:                          "redis://127.0.0.1:1/0",
		RedisTimeout:                      metav1.Duration{Duration: time.Millisecond},
		RedisFailOpen:                     true,
		TeamEgressBandwidthBytesPerSecond: 100,
		TeamBandwidthBurstBytes:           100,
	})
	if err != nil {
		t.Fatalf("newBandwidthLimiter() error = %v", err)
	}
	defer limiter.Close()

	writer := limiter.limitedWriter(&bytes.Buffer{}, &policy.CompiledPolicy{SandboxID: "sandbox-1", TeamID: "team-1"}, bandwidthEgress)
	payload := bytes.Repeat([]byte("x"), 100)
	if n, err := writer.Write(payload); err != nil || n != len(payload) {
		t.Fatalf("Write() = %d, %v; want %d, nil", n, err, len(payload))
	}
}

func TestRedisTeamBandwidthLimiterFailClosedRejectsRedisConnectErrors(t *testing.T) {
	_, err := newBandwidthLimiter(context.Background(), &config.NetdConfig{
		RedisURL:                          "redis://127.0.0.1:1/0",
		RedisTimeout:                      metav1.Duration{Duration: time.Millisecond},
		RedisFailOpen:                     false,
		TeamEgressBandwidthBytesPerSecond: 100,
		TeamBandwidthBurstBytes:           100,
	})
	if err == nil {
		t.Fatal("newBandwidthLimiter() error = nil, want redis connection error")
	}
}

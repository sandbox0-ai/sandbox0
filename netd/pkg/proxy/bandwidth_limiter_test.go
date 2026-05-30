package proxy

import (
	"bytes"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	"github.com/sandbox0-ai/sandbox0/netd/pkg/policy"
)

func TestBandwidthLimitedWriterThrottlesPerSandbox(t *testing.T) {
	limiter := newBandwidthLimiter(&config.NetdConfig{
		EgressBandwidthBytesPerSecond: 100,
		BandwidthBurstBytes:           100,
	})
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
	if len(slept) != 1 || slept[0] != time.Second {
		t.Fatalf("sleep calls = %v, want [1s]", slept)
	}
}

func TestBandwidthLimitedWriterSkipsUnconfiguredDirection(t *testing.T) {
	limiter := newBandwidthLimiter(&config.NetdConfig{
		EgressBandwidthBytesPerSecond: 100,
	})
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

package proxy

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	"github.com/sandbox0-ai/sandbox0/netd/pkg/policy"
	"github.com/sandbox0-ai/sandbox0/pkg/teamquota"
	"github.com/sandbox0-ai/sandbox0/pkg/tokenbucket"
)

func TestUpstreamOnWireConnFailsClosedBeforeWritingWhenQuotaIsUnavailable(t *testing.T) {
	quotaErr := errors.New("quota backend unavailable")
	teamLimiter := &credentialTeamBandwidthLimiter{err: quotaErr}
	server := &Server{cfg: &config.NetdConfig{}}
	server.bandwidthLimiter = newBandwidthLimiter(server.cfg, teamLimiter)

	proxyConn, upstreamConn := net.Pipe()
	defer upstreamConn.Close()
	onWireConn := server.wrapUpstreamOnWireConn(
		context.Background(),
		proxyConn,
		&policy.CompiledPolicy{TeamID: "team-1"},
		nil,
	)
	defer onWireConn.Close()

	if _, err := onWireConn.Write([]byte("must-not-pass")); !errors.Is(err, quotaErr) {
		t.Fatalf("Write() error = %v, want %v", err, quotaErr)
	}
	if err := upstreamConn.SetReadDeadline(time.Now().Add(25 * time.Millisecond)); err != nil {
		t.Fatalf("SetReadDeadline() error = %v", err)
	}
	buffer := make([]byte, 1)
	if _, err := upstreamConn.Read(buffer); err == nil {
		t.Fatal("upstream received bytes after quota admission failed")
	}
}

func TestUpstreamOnWireConnFailsClosedBeforeWritingWhenQuotaIsExhausted(t *testing.T) {
	resolver := &fakeNetworkPolicyResolver{policy: networkRatePolicy(teamquota.KeyNetworkEgressBytes, 10)}
	bucket := &fakeNetworkBucket{decision: tokenbucket.Decision{
		Allowed:    false,
		RetryAfter: time.Hour,
	}}
	teamLimiter, err := NewTeamNetworkQuota(
		"region-1",
		resolver,
		&fakeNetworkAdmissionMarker{},
		bucket,
		time.Minute,
	)
	if err != nil {
		t.Fatalf("NewTeamNetworkQuota() error = %v", err)
	}
	server := &Server{cfg: &config.NetdConfig{}}
	server.bandwidthLimiter = newBandwidthLimiter(server.cfg, teamLimiter)

	proxyConn, upstreamConn := net.Pipe()
	defer upstreamConn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	onWireConn := server.wrapUpstreamOnWireConn(
		ctx,
		proxyConn,
		&policy.CompiledPolicy{TeamID: "team-1"},
		nil,
	)
	defer onWireConn.Close()

	if _, err := onWireConn.Write([]byte("must-not-pass")); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Write() error = %v, want context deadline exceeded", err)
	}
	if err := upstreamConn.SetReadDeadline(time.Now().Add(25 * time.Millisecond)); err != nil {
		t.Fatalf("SetReadDeadline() error = %v", err)
	}
	buffer := make([]byte, 1)
	if _, err := upstreamConn.Read(buffer); err == nil {
		t.Fatal("upstream received bytes while quota was exhausted")
	}
}

func TestUpstreamOnWireConnDoesNotExposeReadBytesWhenQuotaIsUnavailable(t *testing.T) {
	quotaErr := errors.New("quota backend unavailable")
	teamLimiter := &credentialTeamBandwidthLimiter{err: quotaErr}
	usage := &credentialUsageRecorder{}
	server := &Server{
		cfg:           &config.NetdConfig{},
		usageRecorder: usage,
	}
	server.bandwidthLimiter = newBandwidthLimiter(server.cfg, teamLimiter)

	proxyConn, upstreamConn := net.Pipe()
	onWireConn := server.wrapUpstreamOnWireConn(
		context.Background(),
		proxyConn,
		&policy.CompiledPolicy{TeamID: "team-1"},
		nil,
	)
	writeDone := make(chan error, 1)
	go func() {
		_, err := upstreamConn.Write([]byte("must-not-pass"))
		writeDone <- err
	}()

	buffer := make([]byte, len("must-not-pass"))
	n, err := onWireConn.Read(buffer)
	if !errors.Is(err, quotaErr) {
		t.Fatalf("Read() error = %v, want %v", err, quotaErr)
	}
	if n != 0 {
		t.Fatalf("Read() bytes = %d, want 0 after quota admission failed", n)
	}
	if err := onWireConn.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if err := upstreamConn.Close(); err != nil {
		t.Fatalf("close upstream connection: %v", err)
	}
	if err := <-writeDone; err != nil {
		t.Fatalf("write upstream bytes: %v", err)
	}
	if got, want := usage.ingress.Load(), int64(len("must-not-pass")); got != want {
		t.Fatalf("recorded physical ingress bytes = %d, want %d", got, want)
	}
}

func assertOnWireQuotaMatchesUsage(t *testing.T, usage *credentialUsageRecorder, limiter *credentialTeamBandwidthLimiter, minimumEgress, minimumIngress int64) {
	t.Helper()
	if got, want := limiter.egress.Load(), usage.egress.Load(); got != want {
		t.Fatalf("egress quota bytes = %d, on-wire usage = %d; plaintext was charged more than once", got, want)
	}
	if got, want := limiter.ingress.Load(), usage.ingress.Load(); got != want {
		t.Fatalf("ingress quota bytes = %d, on-wire usage = %d; plaintext was charged more than once", got, want)
	}
	if got := usage.egress.Load(); got <= minimumEgress {
		t.Fatalf("on-wire egress bytes = %d, want more than application payload %d to include protocol overhead", got, minimumEgress)
	}
	if got := usage.ingress.Load(); got <= minimumIngress {
		t.Fatalf("on-wire ingress bytes = %d, want more than application payload %d to include protocol overhead", got, minimumIngress)
	}
}

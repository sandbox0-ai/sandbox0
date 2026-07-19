package proxy

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/netd/pkg/policy"
	"github.com/sandbox0-ai/sandbox0/netd/pkg/watcher"
	"github.com/sandbox0-ai/sandbox0/pkg/teamquota"
	"github.com/sandbox0-ai/sandbox0/pkg/tokenbucket"
	"go.uber.org/zap"
)

func TestNetworkOperationQuotaDenialPrecedesTCPAndUDPClassification(t *testing.T) {
	store := networkOperationPolicyStore(t, "10.0.0.2", "team-1")
	bucket := &fakeNetworkBucket{
		decision: tokenbucket.Decision{
			Allowed:    false,
			RetryAfter: time.Second,
		},
	}
	quota, err := NewTeamNetworkQuota(
		"region-1",
		&fakeNetworkPolicyResolver{
			policy: networkRatePolicy(teamquota.KeyNetworkOperations, 16),
		},
		&fakeNetworkAdmissionMarker{},
		bucket,
		time.Minute,
	)
	if err != nil {
		t.Fatalf("NewTeamNetworkQuota() error = %v", err)
	}
	t.Cleanup(func() { _ = quota.Close() })

	tcpCounter := &countingTCPClassifier{}
	udpCounter := &countingUDPClassifier{}
	activeConnections := newFakeActiveConnectionQuota(10)
	server := &Server{
		store:             store,
		logger:            zap.NewNop(),
		teamNetworkQuota:  quota,
		activeConnections: activeConnections,
		tcpClassifiers:    []tcpClassifier{tcpCounter},
		udpClassifiers:    []udpClassifier{udpCounter},
		metrics:           &proxyMetricsRegistry{},
	}

	tcpConn := &networkOperationTestConn{
		remote: &net.TCPAddr{IP: net.ParseIP("10.0.0.2"), Port: 41000},
	}
	server.handleTCPConn(context.Background(), tcpConn)
	server.handleUDPDatagram(
		context.Background(),
		nil,
		&net.UDPAddr{IP: net.ParseIP("10.0.0.2"), Port: 41001},
		[]byte("datagram"),
		net.ParseIP("1.1.1.1"),
		53,
	)

	if tcpCounter.calls != 0 {
		t.Fatalf("TCP classifier calls = %d, want 0", tcpCounter.calls)
	}
	if udpCounter.calls != 0 {
		t.Fatalf("UDP classifier calls = %d, want 0", udpCounter.calls)
	}
	if tcpConn.readCalls != 0 {
		t.Fatalf("TCP reads = %d, want 0", tcpConn.readCalls)
	}
	activeConnections.mu.Lock()
	activeAcquires := activeConnections.acquires
	activeConnections.mu.Unlock()
	if activeAcquires != 0 {
		t.Fatalf("active connection quota acquires = %d, want 0", activeAcquires)
	}
	wantCosts := []int64{16, 1, 16, 1}
	if len(bucket.costs) != len(wantCosts) {
		t.Fatalf("guarded operation costs = %v, want %v", bucket.costs, wantCosts)
	}
	for i := range wantCosts {
		if bucket.costs[i] != wantCosts[i] {
			t.Fatalf("guarded operation costs = %v, want %v", bucket.costs, wantCosts)
		}
	}
}

func TestNetworkOperationQuotaBackendFailurePrecedesUDPClassification(t *testing.T) {
	store := networkOperationPolicyStore(t, "10.0.0.3", "team-2")
	quota, err := NewTeamNetworkQuota(
		"region-1",
		&fakeNetworkPolicyResolver{
			policy: networkRatePolicy(teamquota.KeyNetworkOperations, 16),
		},
		&fakeNetworkAdmissionMarker{},
		&fakeNetworkBucket{err: errors.New("redis unavailable")},
		time.Minute,
	)
	if err != nil {
		t.Fatalf("NewTeamNetworkQuota() error = %v", err)
	}
	t.Cleanup(func() { _ = quota.Close() })

	classifier := &countingUDPClassifier{}
	server := &Server{
		store:            store,
		logger:           zap.NewNop(),
		teamNetworkQuota: quota,
		udpClassifiers:   []udpClassifier{classifier},
		metrics:          &proxyMetricsRegistry{},
	}
	server.handleUDPDatagram(
		context.Background(),
		nil,
		&net.UDPAddr{IP: net.ParseIP("10.0.0.3"), Port: 41002},
		[]byte("datagram"),
		net.ParseIP("1.1.1.1"),
		53,
	)
	if classifier.calls != 0 {
		t.Fatalf("UDP classifier calls = %d, want 0", classifier.calls)
	}
}

func networkOperationPolicyStore(
	t *testing.T,
	podIP string,
	teamID string,
) *policy.Store {
	t.Helper()
	annotation, err := json.Marshal(&v1alpha1.NetworkPolicySpec{
		SandboxID: "sandbox-1",
		TeamID:    teamID,
		Mode:      v1alpha1.NetworkModeAllowAll,
	})
	if err != nil {
		t.Fatalf("marshal network policy: %v", err)
	}
	store := policy.NewStore(zap.NewNop())
	store.UpsertFromSandbox(&watcher.SandboxInfo{
		Namespace:     "default",
		Name:          "sandbox-1",
		PodIP:         podIP,
		SandboxID:     "sandbox-1",
		TeamID:        teamID,
		NetworkPolicy: string(annotation),
	})
	if compiled := store.GetByIP(podIP); compiled == nil || compiled.TeamID != teamID {
		t.Fatalf("compiled policy = %#v, want team %q", compiled, teamID)
	}
	return store
}

type countingTCPClassifier struct {
	calls int
}

func (*countingTCPClassifier) Name() string { return "counting-tcp" }

func (c *countingTCPClassifier) Classify(
	*tcpClassifyContext,
) (*classificationResult, tcpClassifierDecision) {
	c.calls++
	return nil, tcpClassifierNoMatch
}

type countingUDPClassifier struct {
	calls int
}

func (*countingUDPClassifier) Name() string { return "counting-udp" }

func (c *countingUDPClassifier) Classify(
	*udpClassifyContext,
) (*classificationResult, bool) {
	c.calls++
	return nil, false
}

type networkOperationTestConn struct {
	remote    net.Addr
	readCalls int
}

func (c *networkOperationTestConn) Read([]byte) (int, error) {
	c.readCalls++
	return 0, io.EOF
}

func (*networkOperationTestConn) Write(payload []byte) (int, error) { return len(payload), nil }
func (*networkOperationTestConn) Close() error                      { return nil }
func (*networkOperationTestConn) LocalAddr() net.Addr               { return nil }
func (c *networkOperationTestConn) RemoteAddr() net.Addr            { return c.remote }
func (*networkOperationTestConn) SetDeadline(time.Time) error       { return nil }
func (*networkOperationTestConn) SetReadDeadline(time.Time) error   { return nil }
func (*networkOperationTestConn) SetWriteDeadline(time.Time) error  { return nil }

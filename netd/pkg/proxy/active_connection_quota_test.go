package proxy

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	"github.com/sandbox0-ai/sandbox0/netd/pkg/policy"
	"github.com/sandbox0-ai/sandbox0/pkg/teamquota"
	"github.com/sandbox0-ai/sandbox0/pkg/teamquota/activeconnections"
	"go.uber.org/zap"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestUDPSessionsUseOneExactLeasePerLogicalSession(t *testing.T) {
	upstream, err := net.ListenUDP(
		"udp4",
		&net.UDPAddr{IP: net.ParseIP("127.0.0.1")},
	)
	if err != nil {
		t.Fatalf("ListenUDP() error = %v", err)
	}
	defer upstream.Close()

	quota := newFakeActiveConnectionQuota(1)
	server := newActiveConnectionTestServer(quota)
	defer server.closeUDPSessions()
	compiled := &policy.CompiledPolicy{
		TeamID:    "team-1",
		SandboxID: "sandbox-1",
	}
	firstReq := udpQuotaRequest(compiled, upstream.LocalAddr().(*net.UDPAddr), 41001)
	first, err := server.ensureUDPSession(firstReq)
	if err != nil {
		t.Fatalf("ensure first UDP session: %v", err)
	}
	if err := first.Forward([]byte("first")); err != nil {
		t.Fatalf("first Forward() error = %v", err)
	}
	waitForFakeQuotaActive(t, quota, 1)

	secondReq := udpQuotaRequest(compiled, upstream.LocalAddr().(*net.UDPAddr), 41002)
	second, err := server.ensureUDPSession(secondReq)
	if err != nil {
		t.Fatalf("ensure second UDP session: %v", err)
	}
	err = second.Forward([]byte("denied"))
	if !teamquota.IsConcurrencyExceeded(err) {
		t.Fatalf("second Forward() error = %v, want concurrency exceeded", err)
	}

	_ = upstream.SetReadDeadline(time.Now().Add(time.Second))
	buffer := make([]byte, 64)
	n, _, err := upstream.ReadFromUDP(buffer)
	if err != nil {
		t.Fatalf("read first upstream datagram: %v", err)
	}
	if got := string(buffer[:n]); got != "first" {
		t.Fatalf("first upstream datagram = %q, want first", got)
	}
	_ = upstream.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
	if _, _, err := upstream.ReadFromUDP(buffer); err == nil {
		t.Fatal("denied UDP session reached upstream")
	}

	first.close()
	waitForFakeQuotaActive(t, quota, 0)
	replacement, err := server.ensureUDPSession(secondReq)
	if err != nil {
		t.Fatalf("ensure replacement UDP session: %v", err)
	}
	if err := replacement.Forward([]byte("replacement")); err != nil {
		t.Fatalf("replacement Forward() error = %v", err)
	}
	waitForFakeQuotaActive(t, quota, 1)
}

func TestUDPSessionConcurrentInitializationAcquiresOneLease(t *testing.T) {
	upstream, err := net.ListenUDP(
		"udp4",
		&net.UDPAddr{IP: net.ParseIP("127.0.0.1")},
	)
	if err != nil {
		t.Fatalf("ListenUDP() error = %v", err)
	}
	defer upstream.Close()

	quota := newFakeActiveConnectionQuota(2)
	server := newActiveConnectionTestServer(quota)
	defer server.closeUDPSessions()
	session, err := server.ensureUDPSession(udpQuotaRequest(
		&policy.CompiledPolicy{
			TeamID:    "team-1",
			SandboxID: "sandbox-1",
		},
		upstream.LocalAddr().(*net.UDPAddr),
		41501,
	))
	if err != nil {
		t.Fatalf("ensure UDP session: %v", err)
	}

	start := make(chan struct{})
	var connections [2]*net.UDPConn
	var initErrors [2]error
	var wg sync.WaitGroup
	for i := range connections {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			<-start
			connections[index], _, _, initErrors[index] = session.ensureUpstream()
		}(i)
	}
	close(start)
	wg.Wait()

	for i, err := range initErrors {
		if err != nil {
			t.Fatalf("ensureUpstream() call %d error = %v", i, err)
		}
	}
	if connections[0] == nil || connections[0] != connections[1] {
		t.Fatalf(
			"ensureUpstream() connections = [%p, %p], want the same non-nil connection",
			connections[0],
			connections[1],
		)
	}
	quota.mu.Lock()
	acquires := quota.acquires
	active := quota.active
	quota.mu.Unlock()
	if acquires != 1 || active != 1 {
		t.Fatalf(
			"quota state after concurrent initialization = acquires %d, active %d; want 1, 1",
			acquires,
			active,
		)
	}
}

func TestUDPSessionClosesPromptlyWhenLeaseIsLost(t *testing.T) {
	upstream, err := net.ListenUDP(
		"udp4",
		&net.UDPAddr{IP: net.ParseIP("127.0.0.1")},
	)
	if err != nil {
		t.Fatalf("ListenUDP() error = %v", err)
	}
	defer upstream.Close()

	quota := newFakeActiveConnectionQuota(2)
	server := newActiveConnectionTestServer(quota)
	defer server.closeUDPSessions()
	req := udpQuotaRequest(
		&policy.CompiledPolicy{
			TeamID:    "team-1",
			SandboxID: "sandbox-1",
		},
		upstream.LocalAddr().(*net.UDPAddr),
		42001,
	)
	session, err := server.ensureUDPSession(req)
	if err != nil {
		t.Fatalf("ensure UDP session: %v", err)
	}
	if err := session.Forward([]byte("payload")); err != nil {
		t.Fatalf("Forward() error = %v", err)
	}
	waitForFakeQuotaActive(t, quota, 1)
	lease := quota.leaseAt(0)
	lease.lose(&teamquota.UnavailableError{
		Operation: "renew active connection lease",
		Err:       errors.New("redis unavailable"),
	})

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if session.isClosed() {
			waitForFakeQuotaActive(t, quota, 0)
			server.udpSessionMu.Lock()
			remaining := len(server.udpSessions)
			server.udpSessionMu.Unlock()
			if remaining != 0 {
				t.Fatalf("UDP session map entries = %d, want 0", remaining)
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("UDP session remained open after its Team Quota lease was lost")
}

func TestTCPConnectionClosesPromptlyWhenLeaseIsLost(t *testing.T) {
	quota := newFakeActiveConnectionQuota(1)
	server := newActiveConnectionTestServer(quota)
	lease, err := quota.Acquire(context.Background(), "team-1")
	if err != nil {
		t.Fatalf("Acquire() error = %v", err)
	}
	proxyConn, clientConn := net.Pipe()
	defer clientConn.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go server.closeTCPConnectionOnLeaseLoss(
		ctx,
		lease,
		proxyConn,
		&policy.CompiledPolicy{
			TeamID:    "team-1",
			SandboxID: "sandbox-1",
		},
		cancel,
	)
	quota.leaseAt(0).lose(&teamquota.UnavailableError{
		Operation: "renew active connection lease",
		Err:       errors.New("lease member missing"),
	})

	_ = clientConn.SetReadDeadline(time.Now().Add(time.Second))
	if _, err := clientConn.Read(make([]byte, 1)); err == nil {
		t.Fatal("TCP connection remained open after lease loss")
	}
}

func TestTCPLeaseLossClosesSilentUpstreamAndFinishesRelay(t *testing.T) {
	quota := newFakeActiveConnectionQuota(1)
	server := newActiveConnectionTestServer(quota)
	lease, err := quota.Acquire(context.Background(), "team-1")
	if err != nil {
		t.Fatalf("Acquire() error = %v", err)
	}
	downstream, downstreamPeer := net.Pipe()
	defer downstream.Close()
	defer downstreamPeer.Close()
	upstream, upstreamPeer := net.Pipe()
	defer upstream.Close()
	defer upstreamPeer.Close()
	connectionCtx, cancelConnection := context.WithCancel(context.Background())
	defer cancelConnection()
	compiled := &policy.CompiledPolicy{
		TeamID:    "team-1",
		SandboxID: "sandbox-1",
	}

	relayDone := make(chan error, 1)
	go func() {
		var relayErr error
		defer func() {
			server.releaseActiveConnectionLease(lease, "tcp", compiled)
			relayDone <- relayErr
		}()
		go server.closeTCPConnectionOnLeaseLoss(
			connectionCtx,
			lease,
			downstream,
			compiled,
			cancelConnection,
		)
		relayErr = server.pipeWithReaders(
			connectionCtx,
			downstream,
			upstream,
			downstream,
			upstream,
			compiled,
			nil,
		)
	}()

	quota.leaseAt(0).lose(&teamquota.UnavailableError{
		Operation: "renew active connection lease",
		Err:       errors.New("lease member missing"),
	})

	_ = downstreamPeer.SetReadDeadline(time.Now().Add(time.Second))
	if _, err := downstreamPeer.Read(make([]byte, 1)); err == nil {
		t.Fatal("downstream connection remained open after lease loss")
	}
	_ = upstreamPeer.SetReadDeadline(time.Now().Add(time.Second))
	if _, err := upstreamPeer.Read(make([]byte, 1)); err == nil {
		t.Fatal("silent upstream connection remained open after lease loss")
	}
	select {
	case err := <-relayDone:
		if err != nil && !errors.Is(err, io.ErrClosedPipe) {
			t.Fatalf("pipeWithReaders() error = %v, want local-close error", err)
		}
	case <-time.After(time.Second):
		t.Fatal("TCP relay did not finish after lease loss")
	}
}

func TestTCPRelayAllowsNilContext(t *testing.T) {
	server := newActiveConnectionTestServer(newFakeActiveConnectionQuota(1))
	downstream, downstreamPeer := net.Pipe()
	defer downstreamPeer.Close()
	upstream, upstreamPeer := net.Pipe()
	defer upstreamPeer.Close()

	relayDone := make(chan struct{})
	go func() {
		_ = server.pipeWithReaders(
			nil, //nolint:staticcheck // This test verifies the nil-context fallback.
			downstream,
			upstream,
			downstream,
			upstream,
			nil,
			nil,
		)
		close(relayDone)
	}()
	_ = downstream.Close()
	_ = upstream.Close()

	select {
	case <-relayDone:
	case <-time.After(time.Second):
		t.Fatal("TCP relay with nil context did not finish after connections closed")
	}
}

func TestActiveConnectionQuotaRejectsMissingTeamAndBackendFailure(t *testing.T) {
	tests := []struct {
		name     string
		compiled *policy.CompiledPolicy
		quota    *fakeActiveConnectionQuota
	}{
		{
			name:     "missing policy",
			compiled: nil,
			quota:    newFakeActiveConnectionQuota(1),
		},
		{
			name:     "missing team",
			compiled: &policy.CompiledPolicy{SandboxID: "sandbox-1"},
			quota:    newFakeActiveConnectionQuota(1),
		},
		{
			name: "backend unavailable",
			compiled: &policy.CompiledPolicy{
				TeamID:    "team-1",
				SandboxID: "sandbox-1",
			},
			quota: &fakeActiveConnectionQuota{
				limit: 1,
				err: &teamquota.UnavailableError{
					Operation: "acquire active connection lease",
					Err:       errors.New("redis unavailable"),
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := newActiveConnectionTestServer(tt.quota)
			lease, err := server.acquireActiveConnectionLease(
				context.Background(),
				tt.compiled,
			)
			if lease != nil {
				t.Fatalf("lease = %#v, want nil", lease)
			}
			if !teamquota.IsUnavailable(err) {
				t.Fatalf("error = %v, want unavailable", err)
			}
		})
	}
}

func newActiveConnectionTestServer(
	quota *fakeActiveConnectionQuota,
) *Server {
	return &Server{
		cfg: &config.NetdConfig{
			ProxyUpstreamTimeout: metav1.Duration{
				Duration: 200 * time.Millisecond,
			},
		},
		logger:            zap.NewNop(),
		activeConnections: quota,
		udpSessions:       make(map[udpSessionKey]*udpSession),
	}
}

func udpQuotaRequest(
	compiled *policy.CompiledPolicy,
	destination *net.UDPAddr,
	sourcePort int,
) *adapterRequest {
	return &adapterRequest{
		Context:   context.Background(),
		Compiled:  compiled,
		SrcIP:     "10.0.0.2",
		DestIP:    cloneIP(destination.IP),
		DestPort:  destination.Port,
		UDPSource: &net.UDPAddr{IP: net.ParseIP("10.0.0.2"), Port: sourcePort},
	}
}

type fakeActiveConnectionQuota struct {
	mu       sync.Mutex
	limit    int
	active   int
	acquires int
	leases   []*fakeActiveConnectionLease
	err      error
}

func newFakeActiveConnectionQuota(limit int) *fakeActiveConnectionQuota {
	return &fakeActiveConnectionQuota{limit: limit}
}

func (q *fakeActiveConnectionQuota) Acquire(
	_ context.Context,
	teamID string,
) (activeconnections.Lease, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.acquires++
	if q.err != nil {
		return nil, q.err
	}
	if q.limit >= 0 && q.active >= q.limit {
		return nil, &teamquota.ConcurrencyExceededError{
			TeamID: teamID,
			Key:    teamquota.KeyActiveConnectionCount,
			Limit:  int64(q.limit),
			Used:   int64(q.active),
		}
	}
	lease := &fakeActiveConnectionLease{
		owner: q,
		done:  make(chan struct{}),
	}
	q.active++
	q.leases = append(q.leases, lease)
	return lease, nil
}

func (*fakeActiveConnectionQuota) Close() error {
	return nil
}

func (q *fakeActiveConnectionQuota) leaseAt(index int) *fakeActiveConnectionLease {
	q.mu.Lock()
	defer q.mu.Unlock()
	if index < 0 || index >= len(q.leases) {
		return nil
	}
	return q.leases[index]
}

type fakeActiveConnectionLease struct {
	owner *fakeActiveConnectionQuota
	done  chan struct{}

	once sync.Once
	mu   sync.RWMutex
	err  error
}

func (l *fakeActiveConnectionLease) Done() <-chan struct{} {
	return l.done
}

func (l *fakeActiveConnectionLease) Err() error {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.err
}

func (l *fakeActiveConnectionLease) Release(context.Context) error {
	l.finish(nil)
	return nil
}

func (l *fakeActiveConnectionLease) lose(err error) {
	l.finish(err)
}

func (l *fakeActiveConnectionLease) finish(err error) {
	l.once.Do(func() {
		l.mu.Lock()
		l.err = err
		l.mu.Unlock()
		l.owner.mu.Lock()
		l.owner.active--
		l.owner.mu.Unlock()
		close(l.done)
	})
}

func waitForFakeQuotaActive(
	t *testing.T,
	quota *fakeActiveConnectionQuota,
	want int,
) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		quota.mu.Lock()
		active := quota.active
		quota.mu.Unlock()
		if active == want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	quota.mu.Lock()
	active := quota.active
	quota.mu.Unlock()
	t.Fatalf("active leases = %d, want %d", active, want)
}

package server

import (
	"context"
	"errors"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	"github.com/sandbox0-ai/sandbox0/pkg/teamquota"
	"go.uber.org/zap/zaptest"
	sshcrypto "golang.org/x/crypto/ssh"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestSSHServerRequiresBothRegionSharedTeamQuotaEnforcers(t *testing.T) {
	cfg := &config.SSHGatewayConfig{
		SSHHostKeyPath: writeTestHostKey(t),
	}
	authorizer := &staticAuthorizer{}
	if _, err := NewServer(
		cfg,
		authorizer,
		nil,
		zaptest.NewLogger(t),
	); err == nil || !strings.Contains(err.Error(), "active connection") {
		t.Fatalf(
			"NewServer() without active quota error = %v, want required",
			err,
		)
	}
	if _, err := NewServer(
		cfg,
		authorizer,
		nil,
		zaptest.NewLogger(t),
		WithActiveConnectionQuota(newFakeActiveConnectionQuota(1)),
	); err == nil || !strings.Contains(err.Error(), "network byte") {
		t.Fatalf(
			"NewServer() without network quota error = %v, want required",
			err,
		)
	}
}

func TestSSHConnectionAndChannelsUseExactActiveConnectionLeases(t *testing.T) {
	quota := newFakeActiveConnectionQuota(2)
	client, stop := startQuotaTestSSHClient(t, &config.SSHGatewayConfig{}, quota)
	defer stop()

	waitForActiveConnectionQuota(t, quota, 1)
	first, err := client.NewSession()
	if err != nil {
		t.Fatalf("first NewSession() error = %v", err)
	}
	defer first.Close()
	waitForActiveConnectionQuota(t, quota, 2)

	if _, err := client.NewSession(); err == nil {
		t.Fatal("second NewSession() error = nil, want exact-limit rejection")
	}
	active, maximum, acquires := quota.snapshot()
	if active != 2 || maximum != 2 || acquires != 3 {
		t.Fatalf(
			"quota snapshot = active %d max %d acquires %d, want 2 2 3",
			active,
			maximum,
			acquires,
		)
	}

	if err := first.Close(); err != nil && !errors.Is(err, io.EOF) {
		t.Fatalf("Close(first) error = %v", err)
	}
	waitForActiveConnectionQuota(t, quota, 1)
	replacement, err := client.NewSession()
	if err != nil {
		t.Fatalf("replacement NewSession() error = %v", err)
	}
	if err := replacement.Close(); err != nil && !errors.Is(err, io.EOF) {
		t.Fatalf("Close(replacement) error = %v", err)
	}
	waitForActiveConnectionQuota(t, quota, 1)

	if err := client.Close(); err != nil {
		t.Fatalf("Close(client) error = %v", err)
	}
	waitForActiveConnectionQuota(t, quota, 0)
}

func TestSSHPerConnectionChannelGuardIsIndependentFromTeamQuota(t *testing.T) {
	quota := newFakeActiveConnectionQuota(10)
	client, stop := startQuotaTestSSHClient(t, &config.SSHGatewayConfig{
		PlatformMaxConcurrentChannelsPerConnection: 1,
	}, quota)
	defer stop()

	first, err := client.NewSession()
	if err != nil {
		t.Fatalf("first NewSession() error = %v", err)
	}
	defer first.Close()
	waitForActiveConnectionQuota(t, quota, 2)

	if _, err := client.NewSession(); err == nil {
		t.Fatal("second NewSession() error = nil, want platform guard rejection")
	}
	active, maximum, acquires := quota.snapshot()
	if active != 2 || maximum != 2 || acquires != 2 {
		t.Fatalf(
			"quota snapshot = active %d max %d acquires %d, want 2 2 2",
			active,
			maximum,
			acquires,
		)
	}
}

func TestSSHChannelClosesPromptlyWhenLeaseIsLost(t *testing.T) {
	quota := newFakeActiveConnectionQuota(10)
	client, stop := startQuotaTestSSHClient(t, &config.SSHGatewayConfig{}, quota)
	defer stop()

	session, err := client.NewSession()
	if err != nil {
		t.Fatalf("NewSession() error = %v", err)
	}
	defer session.Close()
	waitForActiveConnectionQuota(t, quota, 2)
	channelLease := quota.leaseAt(1)
	if channelLease == nil {
		t.Fatal("channel lease is missing")
	}
	channelLease.lose(&teamquota.UnavailableError{
		Operation: "renew active connection lease",
		Err:       errors.New("redis unavailable"),
	})

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		_, err := session.SendRequest("keepalive@openssh.com", true, nil)
		if err != nil {
			waitForActiveConnectionQuota(t, quota, 1)
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("SSH channel remained usable after its Team Quota lease was lost")
}

func TestSSHConnectionClosesPromptlyWhenBaseLeaseIsLost(t *testing.T) {
	quota := newFakeActiveConnectionQuota(10)
	client, stop := startQuotaTestSSHClient(t, &config.SSHGatewayConfig{}, quota)
	defer stop()

	session, err := client.NewSession()
	if err != nil {
		t.Fatalf("NewSession() error = %v", err)
	}
	defer session.Close()
	waitForActiveConnectionQuota(t, quota, 2)
	connectionLease := quota.leaseAt(0)
	if connectionLease == nil {
		t.Fatal("connection lease is missing")
	}
	connectionLease.lose(&teamquota.UnavailableError{
		Operation: "renew active connection lease",
		Err:       errors.New("lease member missing"),
	})

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := client.NewSession(); err != nil {
			waitForActiveConnectionQuota(t, quota, 0)
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("SSH connection remained usable after its Team Quota lease was lost")
}

func TestSSHMissingTeamAndQuotaBackendFailureFailClosed(t *testing.T) {
	tests := []struct {
		name   string
		target *SessionTarget
		quota  *fakeActiveConnectionQuota
	}{
		{
			name: "missing team",
			target: &SessionTarget{
				SandboxID: "sandbox-1",
				UserID:    "user-1",
			},
			quota: newFakeActiveConnectionQuota(10),
		},
		{
			name: "quota backend unavailable",
			target: &SessionTarget{
				SandboxID: "sandbox-1",
				UserID:    "user-1",
				TeamID:    "team-1",
			},
			quota: &fakeActiveConnectionQuota{
				limit: 10,
				err: &teamquota.UnavailableError{
					Operation: "acquire active connection lease",
					Err:       errors.New("redis unavailable"),
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, listener, stop := startQuotaTestSSHServer(
				t,
				&config.SSHGatewayConfig{},
				tt.target,
				tt.quota,
			)
			defer stop()
			_, clientConfig := newSSHClientConfig(t)
			client, err := sshcrypto.Dial(
				"tcp",
				listener.Addr().String(),
				clientConfig,
			)
			if err == nil {
				defer client.Close()
				if _, err := client.NewSession(); err == nil {
					t.Fatal("NewSession() error = nil, want fail-closed connection")
				}
			}
			active, _, _ := tt.quota.snapshot()
			if active != 0 {
				t.Fatalf("active leases = %d, want 0", active)
			}
		})
	}
}

func TestSSHHandshakePlatformGuardRejectsSlowExcessAndReleases(t *testing.T) {
	quota := newFakeActiveConnectionQuota(10)
	server, listener, stop := startQuotaTestSSHServer(
		t,
		&config.SSHGatewayConfig{
			PlatformMaxConcurrentHandshakes: 1,
			PlatformHandshakeTimeout: metav1.Duration{
				Duration: 400 * time.Millisecond,
			},
		},
		&SessionTarget{
			SandboxID: "sandbox-1",
			UserID:    "user-1",
			TeamID:    "team-1",
		},
		quota,
	)
	defer stop()

	slow, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		t.Fatalf("dial slow handshake: %v", err)
	}
	defer slow.Close()
	waitForHandshakeSlots(t, server, 1)

	excess, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		t.Fatalf("dial excess handshake: %v", err)
	}
	_ = excess.SetReadDeadline(time.Now().Add(time.Second))
	if _, err := excess.Read(make([]byte, 1)); err == nil {
		t.Fatal("excess handshake remained open, want platform guard rejection")
	}
	_ = excess.Close()

	_ = slow.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := io.ReadAll(slow); err != nil {
		if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
			t.Fatal("slow handshake remained open past platform timeout")
		}
	}
	waitForHandshakeSlots(t, server, 0)

	_, clientConfig := newSSHClientConfig(t)
	client, err := sshcrypto.Dial(
		"tcp",
		listener.Addr().String(),
		clientConfig,
	)
	if err != nil {
		t.Fatalf("SSH dial after handshake release error = %v", err)
	}
	_ = client.Close()
}

func startQuotaTestSSHClient(
	t *testing.T,
	cfg *config.SSHGatewayConfig,
	quota *fakeActiveConnectionQuota,
) (*sshcrypto.Client, func()) {
	t.Helper()
	return startQuotaTestSSHClientWithTarget(
		t,
		cfg,
		&SessionTarget{
			SandboxID: "sandbox-1",
			UserID:    "user-1",
			TeamID:    "team-1",
		},
		quota,
	)
}

func startQuotaTestSSHClientWithTarget(
	t *testing.T,
	cfg *config.SSHGatewayConfig,
	target *SessionTarget,
	quota *fakeActiveConnectionQuota,
) (*sshcrypto.Client, func()) {
	t.Helper()
	_, listener, stopServer := startQuotaTestSSHServer(
		t,
		cfg,
		target,
		quota,
	)
	_, clientConfig := newSSHClientConfig(t)
	client, err := sshcrypto.Dial("tcp", listener.Addr().String(), clientConfig)
	if err != nil {
		stopServer()
		t.Fatalf("ssh.Dial() error = %v", err)
	}
	return client, func() {
		_ = client.Close()
		stopServer()
	}
}

func startQuotaTestSSHServer(
	t *testing.T,
	cfg *config.SSHGatewayConfig,
	target *SessionTarget,
	quota *fakeActiveConnectionQuota,
) (*Server, net.Listener, func()) {
	t.Helper()
	if cfg == nil {
		cfg = &config.SSHGatewayConfig{}
	}
	cfg.SSHHostKeyPath = writeTestHostKey(t)
	server, err := NewServer(
		cfg,
		&staticAuthorizer{target: target},
		nil,
		zaptest.NewLogger(t),
		WithActiveConnectionQuota(quota),
		WithNetworkByteQuota(passthroughNetworkByteQuota{}),
	)
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen() error = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		errCh <- server.Serve(ctx, listener)
	}()
	return server, listener, func() {
		cancel()
		_ = listener.Close()
		if err := <-errCh; err != nil && !errors.Is(err, net.ErrClosed) {
			t.Fatalf("Serve() error = %v", err)
		}
	}
}

func waitForActiveConnectionQuota(
	t *testing.T,
	quota *fakeActiveConnectionQuota,
	want int,
) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		active, _, _ := quota.snapshot()
		if active == want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	active, _, _ := quota.snapshot()
	t.Fatalf("active connection leases = %d, want %d", active, want)
}

func waitForHandshakeSlots(t *testing.T, server *Server, want int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if len(server.handshakeSlots) == want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf(
		"active platform handshake slots = %d, want %d",
		len(server.handshakeSlots),
		want,
	)
}

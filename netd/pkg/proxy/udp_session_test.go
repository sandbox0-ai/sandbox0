package proxy

import (
	"context"
	"net"
	"testing"

	"github.com/sandbox0-ai/sandbox0/netd/pkg/policy"
	"go.uber.org/zap"
)

func TestUDPSessionIdentityIsPartOfReuseKey(t *testing.T) {
	server := &Server{
		logger:      zap.NewNop(),
		udpSessions: make(map[udpSessionKey]*udpSession),
	}
	first, err := server.ensureUDPSession(testUDPSessionRequest(
		"team-a",
		"sandbox-a",
		"10.244.0.10",
	))
	if err != nil {
		t.Fatalf("ensure first UDP session: %v", err)
	}
	second, err := server.ensureUDPSession(testUDPSessionRequest(
		"team-b",
		"sandbox-b",
		"10.244.0.10",
	))
	if err != nil {
		t.Fatalf("ensure second UDP session: %v", err)
	}
	t.Cleanup(func() {
		first.close()
		second.close()
	})

	if first == second {
		t.Fatal("different sandbox identities reused one UDP session")
	}
	if first.key.TeamID != "team-a" || first.key.SandboxID != "sandbox-a" {
		t.Fatalf("first session identity = (%q, %q)", first.key.TeamID, first.key.SandboxID)
	}
	if second.key.TeamID != "team-b" || second.key.SandboxID != "sandbox-b" {
		t.Fatalf("second session identity = (%q, %q)", second.key.TeamID, second.key.SandboxID)
	}
	server.udpSessionMu.Lock()
	count := len(server.udpSessions)
	server.udpSessionMu.Unlock()
	if count != 2 {
		t.Fatalf("UDP session count = %d, want 2", count)
	}
}

func TestForgetSandboxUDPSessionsClosesOnlyMatchingSourceIP(t *testing.T) {
	server := &Server{
		logger:      zap.NewNop(),
		udpSessions: make(map[udpSessionKey]*udpSession),
	}
	first, err := server.ensureUDPSession(testUDPSessionRequest(
		"team-a",
		"sandbox-a",
		"10.244.0.10",
	))
	if err != nil {
		t.Fatalf("ensure first UDP session: %v", err)
	}
	second, err := server.ensureUDPSession(testUDPSessionRequest(
		"team-b",
		"sandbox-b",
		"10.244.0.11",
	))
	if err != nil {
		t.Fatalf("ensure second UDP session: %v", err)
	}
	t.Cleanup(func() {
		first.close()
		second.close()
	})

	server.ForgetSandboxUDPSessions("10.244.0.10")

	if !first.isClosed() {
		t.Fatal("matching UDP session remained open")
	}
	if second.isClosed() {
		t.Fatal("unrelated UDP session was closed")
	}
	server.udpSessionMu.Lock()
	count := len(server.udpSessions)
	server.udpSessionMu.Unlock()
	if count != 1 {
		t.Fatalf("remaining UDP session count = %d, want 1", count)
	}
}

func testUDPSessionRequest(teamID string, sandboxID string, sourceIP string) *adapterRequest {
	return &adapterRequest{
		Context: context.Background(),
		Compiled: &policy.CompiledPolicy{
			TeamID:    teamID,
			SandboxID: sandboxID,
		},
		UDPSource: &net.UDPAddr{
			IP:   net.ParseIP(sourceIP),
			Port: 40000,
		},
		DestIP:   net.ParseIP("203.0.113.10"),
		DestPort: 53,
	}
}

package proxy

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/netd/pkg/policy"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"github.com/sandbox0-ai/sandbox0/pkg/sandboxobservability"
	"go.uber.org/zap"
)

func TestHTTPExecutionScopeResolver(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get(internalauth.DefaultTokenHeader) == "" {
			t.Error("missing internal token")
		}
		if r.URL.Query().Get("transport") != "tcp" ||
			r.URL.Query().Get("local_ip") != "10.0.0.2" ||
			r.URL.Query().Get("local_port") != "45000" ||
			r.URL.Query().Get("remote_ip") != "8.8.8.8" ||
			r.URL.Query().Get("remote_port") != "443" {
			t.Errorf("unexpected query: %s", r.URL.RawQuery)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"execution_scope": sandboxobservability.ExecutionScope{
				Namespace:   "codex",
				Kind:        "native_session",
				ID:          "thread-1",
				Attribution: sandboxobservability.ExecutionScopeAttributionProcessEnvironment,
			},
		})
	}))
	defer server.Close()
	host, portText, err := net.SplitHostPort(server.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	port, err := strconv.Atoi(portText)
	if err != nil {
		t.Fatal(err)
	}
	_, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	generator := internalauth.NewGenerator(internalauth.GeneratorConfig{
		Caller:     internalauth.ServiceNetd,
		PrivateKey: privateKey,
		TTL:        time.Minute,
	})
	resolver := NewHTTPExecutionScopeResolver(port, generator, server.Client())
	scope, err := resolver.Resolve(context.Background(), executionScopeResolveRequest{
		SandboxIP:  host,
		TeamID:     "team-1",
		SandboxID:  "sandbox-1",
		Transport:  "tcp",
		LocalIP:    "10.0.0.2",
		LocalPort:  45000,
		RemoteIP:   "8.8.8.8",
		RemotePort: 443,
	})
	if err != nil {
		t.Fatalf("Resolve() error = %v", err)
	}
	if scope == nil || scope.ID != "thread-1" {
		t.Fatalf("Resolve() scope = %+v", scope)
	}
}

type queuedExecutionScopeResolver struct {
	scopes []*sandboxobservability.ExecutionScope
	err    error
	calls  int
}

func (r *queuedExecutionScopeResolver) Resolve(context.Context, executionScopeResolveRequest) (*sandboxobservability.ExecutionScope, error) {
	r.calls++
	if len(r.scopes) == 0 {
		return nil, r.err
	}
	scope := r.scopes[0]
	r.scopes = r.scopes[1:]
	return cloneExecutionScope(scope), nil
}

func TestUDPSessionRotatesWhenReusedTupleChangesExecutionScope(t *testing.T) {
	scope := func(id string) *sandboxobservability.ExecutionScope {
		return &sandboxobservability.ExecutionScope{
			Namespace:   "codex",
			Kind:        "native_session",
			ID:          id,
			Attribution: sandboxobservability.ExecutionScopeAttributionProcessEnvironment,
		}
	}
	server := &Server{
		logger: zap.NewNop(),
		auditor: newAuditLoggerFromWriter(nopWriteCloser{
			Writer: io.Discard,
		}),
		executionScopeResolver: &queuedExecutionScopeResolver{
			scopes: []*sandboxobservability.ExecutionScope{
				scope("thread-a"),
				scope("thread-b"),
			},
		},
	}
	request := func() *adapterRequest {
		return &adapterRequest{
			Compiled: &policy.CompiledPolicy{
				TeamID:    "team-1",
				SandboxID: "sandbox-1",
			},
			SrcIP:      "10.0.0.2",
			SourcePort: 53000,
			DestIP:     net.ParseIP("8.8.8.8"),
			DestPort:   53,
			UDPSource:  &net.UDPAddr{IP: net.ParseIP("10.0.0.2"), Port: 53000},
		}
	}

	first, err := server.ensureUDPSession(request())
	if err != nil {
		t.Fatalf("ensureUDPSession() first error = %v", err)
	}
	forceUDPScopeRevalidation(first)
	second, err := server.ensureUDPSession(request())
	if err != nil {
		t.Fatalf("ensureUDPSession() second error = %v", err)
	}
	if first == second {
		t.Fatal("ensureUDPSession() reused a flow after execution scope changed")
	}
	if !first.isClosed() {
		t.Fatal("previous UDP flow remains open after execution scope changed")
	}
	if got := second.ExecutionScope(); got == nil || got.ID != "thread-b" {
		t.Fatalf("replacement execution scope = %+v", got)
	}
}

func TestUDPSessionRotatesWhenReusedTupleBecomesUnattributed(t *testing.T) {
	scope := &sandboxobservability.ExecutionScope{
		Namespace:   "codex",
		Kind:        "native_session",
		ID:          "thread-a",
		Attribution: sandboxobservability.ExecutionScopeAttributionProcessEnvironment,
	}
	server := &Server{
		logger: zap.NewNop(),
		auditor: newAuditLoggerFromWriter(nopWriteCloser{
			Writer: io.Discard,
		}),
		executionScopeResolver: &queuedExecutionScopeResolver{
			scopes: []*sandboxobservability.ExecutionScope{scope, nil},
		},
	}
	request := func() *adapterRequest {
		return &adapterRequest{
			Compiled: &policy.CompiledPolicy{
				TeamID:    "team-1",
				SandboxID: "sandbox-1",
			},
			SrcIP:      "10.0.0.2",
			SourcePort: 53000,
			DestIP:     net.ParseIP("8.8.8.8"),
			DestPort:   53,
			UDPSource:  &net.UDPAddr{IP: net.ParseIP("10.0.0.2"), Port: 53000},
		}
	}

	first, err := server.ensureUDPSession(request())
	if err != nil {
		t.Fatalf("ensureUDPSession() attributed error = %v", err)
	}
	forceUDPScopeRevalidation(first)
	second, err := server.ensureUDPSession(request())
	if err != nil {
		t.Fatalf("ensureUDPSession() unattributed error = %v", err)
	}
	if first == second {
		t.Fatal("ensureUDPSession() reused an attributed flow after ownership became ambiguous")
	}
	if !first.isClosed() {
		t.Fatal("previous attributed UDP flow remains open after ownership became ambiguous")
	}
	if got := second.ExecutionScope(); got != nil {
		t.Fatalf("replacement execution scope = %+v, want nil", got)
	}
}

func TestUDPSessionDoesNotResolveEveryDatagram(t *testing.T) {
	scope := func(id string) *sandboxobservability.ExecutionScope {
		return &sandboxobservability.ExecutionScope{
			Namespace:   "codex",
			Kind:        "native_session",
			ID:          id,
			Attribution: sandboxobservability.ExecutionScopeAttributionProcessEnvironment,
		}
	}
	resolver := &queuedExecutionScopeResolver{
		scopes: []*sandboxobservability.ExecutionScope{
			scope("thread-a"),
			scope("thread-b"),
		},
	}
	server := &Server{
		logger: zap.NewNop(),
		auditor: newAuditLoggerFromWriter(nopWriteCloser{
			Writer: io.Discard,
		}),
		executionScopeResolver: resolver,
	}
	request := func() *adapterRequest {
		return &adapterRequest{
			Compiled: &policy.CompiledPolicy{
				TeamID:    "team-1",
				SandboxID: "sandbox-1",
			},
			SrcIP:      "10.0.0.2",
			SourcePort: 53000,
			DestIP:     net.ParseIP("8.8.8.8"),
			DestPort:   53,
			UDPSource:  &net.UDPAddr{IP: net.ParseIP("10.0.0.2"), Port: 53000},
		}
	}

	first, err := server.ensureUDPSession(request())
	if err != nil {
		t.Fatalf("ensureUDPSession() first error = %v", err)
	}
	second, err := server.ensureUDPSession(request())
	if err != nil {
		t.Fatalf("ensureUDPSession() second error = %v", err)
	}
	if first != second {
		t.Fatal("ensureUDPSession() replaced a freshly attributed flow")
	}
	if resolver.calls != 1 {
		t.Fatalf("resolver calls = %d, want 1", resolver.calls)
	}
}

func TestUDPSessionBoundsStaleScopeWhenRevalidationIsUnavailable(t *testing.T) {
	scope := &sandboxobservability.ExecutionScope{
		Namespace:   "codex",
		Kind:        "native_session",
		ID:          "thread-a",
		Attribution: sandboxobservability.ExecutionScopeAttributionProcessEnvironment,
	}
	resolver := &queuedExecutionScopeResolver{
		scopes: []*sandboxobservability.ExecutionScope{scope},
		err:    errors.New("procd unavailable"),
	}
	server := &Server{
		logger: zap.NewNop(),
		auditor: newAuditLoggerFromWriter(nopWriteCloser{
			Writer: io.Discard,
		}),
		executionScopeResolver: resolver,
	}
	request := func() *adapterRequest {
		return &adapterRequest{
			Compiled: &policy.CompiledPolicy{
				TeamID:    "team-1",
				SandboxID: "sandbox-1",
			},
			SrcIP:      "10.0.0.2",
			SourcePort: 53000,
			DestIP:     net.ParseIP("8.8.8.8"),
			DestPort:   53,
			UDPSource:  &net.UDPAddr{IP: net.ParseIP("10.0.0.2"), Port: 53000},
		}
	}

	first, err := server.ensureUDPSession(request())
	if err != nil {
		t.Fatalf("ensureUDPSession() first error = %v", err)
	}
	forceUDPScopeRevalidation(first)
	recent, err := server.ensureUDPSession(request())
	if err != nil {
		t.Fatalf("ensureUDPSession() recent revalidation error = %v", err)
	}
	if recent != first {
		t.Fatal("transient resolver failure replaced a recently confirmed flow")
	}

	first.mu.Lock()
	first.executionScopeResolvedAt = time.Now().Add(-udpExecutionScopeMaxStale)
	first.executionScopeNextResolve = time.Time{}
	first.mu.Unlock()
	stale, err := server.ensureUDPSession(request())
	if err != nil {
		t.Fatalf("ensureUDPSession() stale revalidation error = %v", err)
	}
	if stale == first {
		t.Fatal("stale attribution survived the maximum resolver failure window")
	}
	if got := stale.ExecutionScope(); got != nil {
		t.Fatalf("stale replacement execution scope = %+v, want nil", got)
	}
}

func forceUDPScopeRevalidation(session *udpSession) {
	session.mu.Lock()
	defer session.mu.Unlock()
	session.executionScopeNextResolve = time.Time{}
}

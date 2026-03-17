package proxy

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/netd/pkg/policy"
	"github.com/sandbox0-ai/sandbox0/pkg/egressauth"
)

type stubEgressAuthResolver struct {
	calls int
	resp  *egressauth.ResolveResponse
	err   error
}

func (s *stubEgressAuthResolver) Resolve(_ context.Context, _ *egressauth.ResolveRequest) (*egressauth.ResolveResponse, error) {
	s.calls++
	if s.err != nil {
		return nil, s.err
	}
	return cloneResolveResponse(s.resp), nil
}

func TestAttachEgressAuthUsesCacheBeforeResolver(t *testing.T) {
	expiresAt := time.Now().Add(time.Minute).UTC()
	cache := newMemoryEgressAuthCache()
	key := egressAuthCacheKey{
		SandboxID:       "sbx_123",
		AuthRef:         "example-api",
		Destination:     "api.example.com",
		DestinationPort: 80,
		Transport:       "tcp",
		Protocol:        "http",
	}
	cache.Put(key, &egressauth.ResolveResponse{
		AuthRef:   "example-api",
		Headers:   map[string]string{"Authorization": "Bearer cached"},
		ExpiresAt: &expiresAt,
	})
	resolver := &stubEgressAuthResolver{
		resp: &egressauth.ResolveResponse{
			AuthRef: "example-api",
		},
	}
	server := &Server{
		authResolver: resolver,
		authCache:    cache,
	}
	req := &adapterRequest{
		Compiled: &policy.CompiledPolicy{
			SandboxID: "sbx_123",
			TeamID:    "team_123",
		},
		DestIP:   net.ParseIP("8.8.8.8"),
		DestPort: 80,
		Host:     "api.example.com",
	}
	decision := trafficDecision{
		Action:          decisionActionUseAdapter,
		Transport:       "tcp",
		Protocol:        "http",
		MatchedAuthRule: &policy.CompiledEgressAuthRule{Name: "example-http", AuthRef: "example-api"},
	}

	server.attachEgressAuth(req, decision)

	if req.EgressAuth == nil || req.EgressAuth.Resolved == nil {
		t.Fatal("expected cached auth material")
	}
	if !req.EgressAuth.CacheHit {
		t.Fatal("expected cache hit")
	}
	if resolver.calls != 0 {
		t.Fatalf("expected resolver not to be called, got %d calls", resolver.calls)
	}
}

func TestAttachEgressAuthResolvesAndCaches(t *testing.T) {
	expiresAt := time.Now().Add(time.Minute).UTC()
	resolver := &stubEgressAuthResolver{
		resp: &egressauth.ResolveResponse{
			AuthRef:   "example-api",
			Headers:   map[string]string{"Authorization": "Bearer fresh"},
			ExpiresAt: &expiresAt,
		},
	}
	server := &Server{
		authResolver: resolver,
		authCache:    newMemoryEgressAuthCache(),
	}
	req := &adapterRequest{
		Compiled: &policy.CompiledPolicy{
			SandboxID: "sbx_123",
			TeamID:    "team_123",
		},
		DestIP:   net.ParseIP("8.8.8.8"),
		DestPort: 80,
		Host:     "api.example.com",
	}
	decision := trafficDecision{
		Action:          decisionActionUseAdapter,
		Transport:       "tcp",
		Protocol:        "http",
		MatchedAuthRule: &policy.CompiledEgressAuthRule{Name: "example-http", AuthRef: "example-api"},
	}

	server.attachEgressAuth(req, decision)

	if req.EgressAuth == nil || req.EgressAuth.Resolved == nil {
		t.Fatal("expected resolved auth material")
	}
	if req.EgressAuth.CacheHit {
		t.Fatal("expected non-cached resolve")
	}
	if resolver.calls != 1 {
		t.Fatalf("expected resolver call, got %d", resolver.calls)
	}

	nextReq := &adapterRequest{
		Compiled: req.Compiled,
		DestIP:   req.DestIP,
		DestPort: req.DestPort,
		Host:     req.Host,
	}
	server.attachEgressAuth(nextReq, decision)
	if nextReq.EgressAuth == nil || !nextReq.EgressAuth.CacheHit {
		t.Fatal("expected cached auth material on second request")
	}
}

func TestAttachEgressAuthRecordsResolveError(t *testing.T) {
	resolveErr := errors.New("broker unavailable")
	server := &Server{
		authResolver: &stubEgressAuthResolver{err: resolveErr},
		authCache:    newMemoryEgressAuthCache(),
	}
	req := &adapterRequest{
		Compiled: &policy.CompiledPolicy{
			SandboxID: "sbx_123",
			TeamID:    "team_123",
		},
		DestIP:   net.ParseIP("8.8.8.8"),
		DestPort: 80,
		Host:     "api.example.com",
	}
	decision := trafficDecision{
		Action:          decisionActionUseAdapter,
		Transport:       "tcp",
		Protocol:        "http",
		MatchedAuthRule: &policy.CompiledEgressAuthRule{Name: "example-http", AuthRef: "example-api"},
	}

	server.attachEgressAuth(req, decision)

	if req.EgressAuth == nil {
		t.Fatal("expected egress auth context")
	}
	if !req.EgressAuth.ResolveAttempt {
		t.Fatal("expected resolve attempt")
	}
	if !errors.Is(req.EgressAuth.ResolveError, resolveErr) {
		t.Fatalf("expected resolve error %v, got %v", resolveErr, req.EgressAuth.ResolveError)
	}
}

func TestAttachEgressAuthSkipsWhenDecisionHasNoRule(t *testing.T) {
	server := &Server{
		authResolver: &stubEgressAuthResolver{},
		authCache:    newMemoryEgressAuthCache(),
	}
	req := &adapterRequest{
		Compiled: &policy.CompiledPolicy{
			Mode: v1alpha1.NetworkModeAllowAll,
		},
	}

	server.attachEgressAuth(req, trafficDecision{Action: decisionActionUseAdapter, Transport: "tcp", Protocol: "http"})

	if req.EgressAuth != nil {
		t.Fatalf("expected no egress auth context, got %+v", req.EgressAuth)
	}
}

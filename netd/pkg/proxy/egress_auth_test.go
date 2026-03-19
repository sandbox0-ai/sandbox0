package proxy

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
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
		cfg:          &config.NetdConfig{EgressAuthEnabled: true},
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
		cfg:          &config.NetdConfig{EgressAuthEnabled: true},
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

func TestAttachEgressAuthRefreshesExpiredCacheEntry(t *testing.T) {
	now := time.Date(2026, time.March, 18, 12, 0, 0, 0, time.UTC)
	cache := newMemoryEgressAuthCache()
	cache.now = func() time.Time { return now }

	key := egressAuthCacheKey{
		SandboxID:       "sbx_123",
		AuthRef:         "example-api",
		Destination:     "api.example.com",
		DestinationPort: 80,
		Transport:       "tcp",
		Protocol:        "http",
	}
	expiredAt := now.Add(-time.Second)
	cache.Put(key, &egressauth.ResolveResponse{
		AuthRef:   "example-api",
		Headers:   map[string]string{"Authorization": "Bearer expired"},
		ExpiresAt: &expiredAt,
	})

	freshExpiresAt := now.Add(time.Minute)
	resolver := &stubEgressAuthResolver{
		resp: &egressauth.ResolveResponse{
			AuthRef:   "example-api",
			Headers:   map[string]string{"Authorization": "Bearer fresh"},
			ExpiresAt: &freshExpiresAt,
		},
	}
	server := &Server{
		cfg:          &config.NetdConfig{EgressAuthEnabled: true},
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
		t.Fatal("expected resolved auth material")
	}
	if req.EgressAuth.CacheHit {
		t.Fatal("expected expired cache entry to trigger re-resolve")
	}
	if resolver.calls != 1 {
		t.Fatalf("resolver calls = %d, want 1", resolver.calls)
	}
	if got := req.EgressAuth.Resolved.Headers["Authorization"]; got != "Bearer fresh" {
		t.Fatalf("authorization header = %q, want Bearer fresh", got)
	}

	nextReq := &adapterRequest{
		Compiled: req.Compiled,
		DestIP:   req.DestIP,
		DestPort: req.DestPort,
		Host:     req.Host,
	}
	server.attachEgressAuth(nextReq, decision)
	if nextReq.EgressAuth == nil || !nextReq.EgressAuth.CacheHit {
		t.Fatal("expected refreshed cache entry on second request")
	}
	if resolver.calls != 1 {
		t.Fatalf("resolver calls after refresh = %d, want 1", resolver.calls)
	}
}

func TestAttachEgressAuthRecordsResolveError(t *testing.T) {
	resolveErr := errors.New("broker unavailable")
	server := &Server{
		cfg:          &config.NetdConfig{EgressAuthEnabled: true},
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
		cfg:          &config.NetdConfig{EgressAuthEnabled: true},
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

func TestAttachEgressAuthBypassesWhenClusterFeatureDisabled(t *testing.T) {
	server := &Server{
		cfg:          &config.NetdConfig{EgressAuthEnabled: false},
		authResolver: &stubEgressAuthResolver{},
		authCache:    newMemoryEgressAuthCache(),
	}
	req := &adapterRequest{
		Compiled: &policy.CompiledPolicy{SandboxID: "sbx_123", TeamID: "team_123"},
		DestIP:   net.ParseIP("8.8.8.8"),
		DestPort: 443,
		Host:     "api.example.com",
	}
	decision := trafficDecision{
		Action:          decisionActionUseAdapter,
		Transport:       "tcp",
		Protocol:        "tls",
		MatchedAuthRule: &policy.CompiledEgressAuthRule{Name: "example-https", AuthRef: "example-api"},
	}

	server.attachEgressAuth(req, decision)

	if req.EgressAuth == nil {
		t.Fatal("expected egress auth context")
	}
	if !req.EgressAuth.ShouldBypass() {
		t.Fatal("expected auth enforcement to bypass")
	}
	if req.EgressAuth.BypassReason != "cluster_disabled" {
		t.Fatalf("bypass reason = %q, want cluster_disabled", req.EgressAuth.BypassReason)
	}
}

func TestAttachEgressAuthFailOpenBypassesOnResolverError(t *testing.T) {
	resolveErr := errors.New("broker unavailable")
	server := &Server{
		cfg: &config.NetdConfig{
			EgressAuthEnabled:       true,
			EgressAuthFailurePolicy: string(v1alpha1.EgressAuthFailurePolicyFailOpen),
		},
		authResolver: &stubEgressAuthResolver{err: resolveErr},
		authCache:    newMemoryEgressAuthCache(),
	}
	req := &adapterRequest{
		Compiled: &policy.CompiledPolicy{SandboxID: "sbx_123", TeamID: "team_123"},
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
	if !req.EgressAuth.ShouldBypass() {
		t.Fatal("expected fail-open bypass")
	}
	if req.EgressAuth.BypassReason != "resolve_error" {
		t.Fatalf("bypass reason = %q, want resolve_error", req.EgressAuth.BypassReason)
	}
	if !errors.Is(req.EgressAuth.ResolveError, resolveErr) {
		t.Fatalf("resolve error = %v, want %v", req.EgressAuth.ResolveError, resolveErr)
	}
}

func TestPrepareHTTPHeaderDirectivesRejectsUnsupportedDirective(t *testing.T) {
	ctx := &egressAuthContext{
		Rule: &policy.CompiledEgressAuthRule{Name: "example-http", AuthRef: "example-api"},
		Resolved: &egressauth.ResolveResponse{
			AuthRef: "example-api",
			Directives: []egressauth.ResolveDirective{{
				Kind: egressauth.ResolveDirectiveKindCustom,
			}},
		},
		FailurePolicy: string(v1alpha1.EgressAuthFailurePolicyFailClosed),
	}

	err := prepareHTTPHeaderDirectives(ctx, "http", true)

	if !errors.Is(err, errEgressAuthDirectiveUnsupported) {
		t.Fatalf("err = %v, want unsupported directive", err)
	}
	if ctx.ShouldBypass() {
		t.Fatal("expected fail-closed enforcement")
	}
	if ctx.EnforcementReason != "unsupported_directive" {
		t.Fatalf("enforcement reason = %q", ctx.EnforcementReason)
	}
}

func TestPrepareHTTPHeaderDirectivesFailOpenBypassesUnsupportedDirective(t *testing.T) {
	ctx := &egressAuthContext{
		Rule: &policy.CompiledEgressAuthRule{Name: "example-http", AuthRef: "example-api"},
		Resolved: &egressauth.ResolveResponse{
			AuthRef: "example-api",
			Directives: []egressauth.ResolveDirective{{
				Kind: egressauth.ResolveDirectiveKindCustom,
			}},
		},
		FailurePolicy: string(v1alpha1.EgressAuthFailurePolicyFailOpen),
	}

	err := prepareHTTPHeaderDirectives(ctx, "http", true)

	if !errors.Is(err, errEgressAuthDirectiveUnsupported) {
		t.Fatalf("err = %v, want unsupported directive", err)
	}
	if !ctx.ShouldBypass() {
		t.Fatal("expected fail-open bypass")
	}
	if ctx.BypassReason != "unsupported_directive" {
		t.Fatalf("bypass reason = %q", ctx.BypassReason)
	}
}

func TestPrepareTLSClientCertificateDirectivesLoadsMaterial(t *testing.T) {
	certPEM, keyPEM, err := newSelfSignedCertificateAuthority("sandbox0-client", time.Hour)
	if err != nil {
		t.Fatalf("new client cert: %v", err)
	}
	ctx := &egressAuthContext{
		Rule: &policy.CompiledEgressAuthRule{Name: "example-mtls", AuthRef: "example-cert"},
		Resolved: egressauth.NewTLSClientCertificateResolveResponse("example-cert", &egressauth.TLSClientCertificateDirective{
			CertificatePEM: string(certPEM),
			PrivateKeyPEM:  string(keyPEM),
		}, nil),
		FailurePolicy: string(v1alpha1.EgressAuthFailurePolicyFailClosed),
	}

	err = prepareTLSClientCertificateDirectives(ctx, "tls", true)
	if err != nil {
		t.Fatalf("prepare tls client certificate directives: %v", err)
	}
	if ctx.ResolvedTLSClientCertificate == nil {
		t.Fatal("expected tls client certificate material")
	}
}

func TestPrepareTLSClientCertificateDirectivesRejectsWhenTerminationUnavailable(t *testing.T) {
	certPEM, keyPEM, err := newSelfSignedCertificateAuthority("sandbox0-client", time.Hour)
	if err != nil {
		t.Fatalf("new client cert: %v", err)
	}
	ctx := &egressAuthContext{
		Rule: &policy.CompiledEgressAuthRule{Name: "example-mtls", AuthRef: "example-cert"},
		Resolved: egressauth.NewTLSClientCertificateResolveResponse("example-cert", &egressauth.TLSClientCertificateDirective{
			CertificatePEM: string(certPEM),
			PrivateKeyPEM:  string(keyPEM),
		}, nil),
		FailurePolicy: string(v1alpha1.EgressAuthFailurePolicyFailClosed),
	}

	err = prepareTLSClientCertificateDirectives(ctx, "tls", false)
	if !errors.Is(err, errEgressAuthDirectiveUnsupported) {
		t.Fatalf("err = %v, want unsupported directive", err)
	}
	if ctx.EnforcementReason != "unsupported_directive" {
		t.Fatalf("enforcement reason = %q", ctx.EnforcementReason)
	}
}

package proxy

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/netd/pkg/policy"
	"github.com/sandbox0-ai/sandbox0/pkg/egressauth"
)

type egressAuthContext struct {
	Rule                         *policy.CompiledEgressAuthRule
	Resolved                     *egressauth.ResolveResponse
	ResolvedHeaders              map[string]string
	ResolvedTLSClientCertificate *resolvedTLSClientCertificate
	CacheHit                     bool
	ResolveAttempt               bool
	ResolveError                 error
	FailurePolicy                string
	BypassReason                 string
	EnforcementReason            string
}

type resolvedTLSClientCertificate struct {
	Certificate tls.Certificate
	RootCAs     *x509.CertPool
}

type egressAuthResolver interface {
	Resolve(context.Context, *egressauth.ResolveRequest) (*egressauth.ResolveResponse, error)
}

type egressAuthCache interface {
	Get(egressAuthCacheKey) (*egressauth.ResolveResponse, bool)
	Put(egressAuthCacheKey, *egressauth.ResolveResponse)
}

type egressAuthCacheKey struct {
	SandboxID       string
	AuthRef         string
	Destination     string
	DestinationPort int
	Transport       string
	Protocol        string
}

type memoryEgressAuthCache struct {
	mu      sync.RWMutex
	entries map[egressAuthCacheKey]*egressauth.ResolveResponse
	now     func() time.Time
}

type noopEgressAuthResolver struct{}

type ServerOption func(*Server)

var errEgressAuthResolverUnconfigured = errors.New("egress auth resolver is not configured")
var errEgressAuthDirectiveUnsupported = errors.New("egress auth directives unsupported by adapter")
var errEgressAuthMaterialUnavailable = errors.New("egress auth material unavailable")
var errEgressAuthDirectiveInvalid = errors.New("egress auth directive invalid")

func WithEgressAuthResolver(resolver egressAuthResolver) ServerOption {
	return func(s *Server) {
		if s != nil {
			s.authResolver = resolver
		}
	}
}

func WithEgressAuthCache(cache egressAuthCache) ServerOption {
	return func(s *Server) {
		if s != nil {
			s.authCache = cache
		}
	}
}

func WithTLSInterceptAuthority(authority tlsInterceptAuthority) ServerOption {
	return func(s *Server) {
		if s != nil {
			s.tlsAuthority = authority
		}
	}
}

func WithUpstreamTLSConfig(cfg *tls.Config) ServerOption {
	return func(s *Server) {
		if s != nil {
			s.upstreamTLSConfig = cloneTLSConfig(cfg)
		}
	}
}

func newMemoryEgressAuthCache() *memoryEgressAuthCache {
	return &memoryEgressAuthCache{
		entries: make(map[egressAuthCacheKey]*egressauth.ResolveResponse),
		now: func() time.Time {
			return time.Now().UTC()
		},
	}
}

func (c *memoryEgressAuthCache) Get(key egressAuthCacheKey) (*egressauth.ResolveResponse, bool) {
	if c == nil {
		return nil, false
	}
	c.mu.RLock()
	entry := c.entries[key]
	c.mu.RUnlock()
	if entry == nil {
		return nil, false
	}
	if entry.ExpiresAt != nil && !entry.ExpiresAt.After(c.now()) {
		c.mu.Lock()
		delete(c.entries, key)
		proxyMetrics.SetEgressAuthCacheEntries(len(c.entries))
		c.mu.Unlock()
		return nil, false
	}
	return cloneResolveResponse(entry), true
}

func (c *memoryEgressAuthCache) Put(key egressAuthCacheKey, value *egressauth.ResolveResponse) {
	if c == nil || value == nil {
		return
	}
	c.mu.Lock()
	c.entries[key] = cloneResolveResponse(value)
	proxyMetrics.SetEgressAuthCacheEntries(len(c.entries))
	c.mu.Unlock()
}

func (noopEgressAuthResolver) Resolve(_ context.Context, _ *egressauth.ResolveRequest) (*egressauth.ResolveResponse, error) {
	return nil, nil
}

func cloneResolveResponse(in *egressauth.ResolveResponse) *egressauth.ResolveResponse {
	return egressauth.CloneResolveResponse(in)
}

func cloneResolvedHeaders(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

func resolveHTTPHeadersForAdapter(ctx *egressAuthContext, allowHTTPHeaders bool) (map[string]string, error) {
	if ctx == nil || ctx.Resolved == nil {
		return nil, errEgressAuthMaterialUnavailable
	}

	if len(ctx.Resolved.Directives) == 0 && len(ctx.Resolved.Headers) > 0 {
		if !allowHTTPHeaders {
			return nil, errEgressAuthDirectiveUnsupported
		}
		return cloneResolvedHeaders(ctx.Resolved.Headers), nil
	}

	if !allowHTTPHeaders {
		if len(ctx.Resolved.Directives) > 0 || len(ctx.Resolved.Headers) > 0 {
			return nil, errEgressAuthDirectiveUnsupported
		}
		return nil, errEgressAuthMaterialUnavailable
	}

	headers := map[string]string{}
	for _, directive := range ctx.Resolved.Directives {
		switch directive.Kind {
		case egressauth.ResolveDirectiveKindHTTPHeaders:
			if directive.HTTPHeaders == nil {
				continue
			}
			for key, value := range directive.HTTPHeaders.Headers {
				headers[key] = value
			}
		default:
			return nil, errEgressAuthDirectiveUnsupported
		}
	}
	if len(headers) == 0 {
		return nil, errEgressAuthMaterialUnavailable
	}
	return headers, nil
}

func prepareHTTPHeaderDirectives(ctx *egressAuthContext, protocol string, allowHTTPHeaders bool) error {
	if ctx == nil || ctx.Rule == nil {
		return nil
	}
	if ctx.ShouldBypass() {
		return nil
	}
	if ctx.ResolveError != nil {
		return ctx.ResolveError
	}
	if ctx.Resolved == nil {
		applyEgressAuthFailurePolicy(ctx, protocol, "material_unavailable")
		return errEgressAuthMaterialUnavailable
	}

	headers, err := resolveHTTPHeadersForAdapter(ctx, allowHTTPHeaders)
	if err == nil {
		ctx.ResolvedHeaders = headers
		return nil
	}
	applyEgressAuthDirectiveError(ctx, protocol, err)
	return err
}

func resolveTLSClientCertificateForAdapter(ctx *egressAuthContext, allowTLSClientCertificate bool) (*resolvedTLSClientCertificate, error) {
	if ctx == nil || ctx.Resolved == nil {
		return nil, errEgressAuthMaterialUnavailable
	}
	if !allowTLSClientCertificate {
		if len(ctx.Resolved.Directives) > 0 || len(ctx.Resolved.Headers) > 0 {
			return nil, errEgressAuthDirectiveUnsupported
		}
		return nil, errEgressAuthMaterialUnavailable
	}
	for _, directive := range ctx.Resolved.Directives {
		switch directive.Kind {
		case egressauth.ResolveDirectiveKindTLSClientCertificate:
			if directive.TLSClientCertificate == nil {
				continue
			}
			keyPair, err := tls.X509KeyPair(
				[]byte(directive.TLSClientCertificate.CertificatePEM),
				[]byte(directive.TLSClientCertificate.PrivateKeyPEM),
			)
			if err != nil {
				return nil, errEgressAuthDirectiveInvalid
			}
			var rootCAs *x509.CertPool
			if caPEM := strings.TrimSpace(directive.TLSClientCertificate.CAPEM); caPEM != "" {
				rootCAs = x509.NewCertPool()
				if !rootCAs.AppendCertsFromPEM([]byte(caPEM)) {
					return nil, errEgressAuthDirectiveInvalid
				}
			}
			return &resolvedTLSClientCertificate{
				Certificate: keyPair,
				RootCAs:     rootCAs,
			}, nil
		default:
			return nil, errEgressAuthDirectiveUnsupported
		}
	}
	if len(ctx.Resolved.Headers) > 0 {
		return nil, errEgressAuthDirectiveUnsupported
	}
	return nil, errEgressAuthMaterialUnavailable
}

func prepareTLSClientCertificateDirectives(ctx *egressAuthContext, protocol string, allowTLSClientCertificate bool) error {
	if ctx == nil || ctx.Rule == nil {
		return nil
	}
	if ctx.ShouldBypass() {
		return nil
	}
	if ctx.ResolveError != nil {
		return ctx.ResolveError
	}
	if ctx.Resolved == nil {
		applyEgressAuthFailurePolicy(ctx, protocol, "material_unavailable")
		return errEgressAuthMaterialUnavailable
	}

	material, err := resolveTLSClientCertificateForAdapter(ctx, allowTLSClientCertificate)
	if err == nil {
		ctx.ResolvedTLSClientCertificate = material
		return nil
	}
	applyEgressAuthDirectiveError(ctx, protocol, err)
	return err
}

func applyEgressAuthDirectiveError(ctx *egressAuthContext, protocol string, err error) {
	switch {
	case errors.Is(err, errEgressAuthDirectiveUnsupported):
		applyEgressAuthFailurePolicy(ctx, protocol, "unsupported_directive")
	case errors.Is(err, errEgressAuthMaterialUnavailable):
		applyEgressAuthFailurePolicy(ctx, protocol, "material_unavailable")
	case errors.Is(err, errEgressAuthDirectiveInvalid):
		applyEgressAuthFailurePolicy(ctx, protocol, "invalid_directive")
	default:
		applyEgressAuthFailurePolicy(ctx, protocol, "resolve_error")
	}
}

func (s *Server) attachEgressAuth(req *adapterRequest, decision trafficDecision) {
	if req == nil || decision.MatchedAuthRule == nil {
		return
	}
	ctx := &egressAuthContext{
		Rule:          decision.MatchedAuthRule,
		FailurePolicy: egressAuthFailurePolicy(s.cfg, decision.MatchedAuthRule),
	}
	req.EgressAuth = ctx
	if !egressAuthEnabled(s.cfg) {
		ctx.BypassReason = "cluster_disabled"
		proxyMetrics.RecordEgressAuthDecision(decision.Protocol, "bypassed", ctx.BypassReason)
		return
	}
	if s == nil {
		return
	}

	key := buildEgressAuthCacheKey(req, decision)
	if s.authCache != nil {
		if resolved, ok := s.authCache.Get(key); ok {
			ctx.Resolved = resolved
			ctx.CacheHit = true
			ctx.EnforcementReason = "cache_hit"
			proxyMetrics.RecordEgressAuthResolve(decision.Protocol, "cache_hit", 0)
			proxyMetrics.RecordEgressAuthDecision(decision.Protocol, "enforced", ctx.EnforcementReason)
			return
		}
	}
	if s.authResolver == nil {
		ctx.ResolveError = errEgressAuthResolverUnconfigured
		applyEgressAuthFailurePolicy(ctx, decision.Protocol, "resolver_unconfigured")
		return
	}

	resolveReq := &egressauth.ResolveRequest{
		SandboxID:       compiledSandboxID(req.Compiled),
		TeamID:          compiledTeamID(req.Compiled),
		AuthRef:         decision.MatchedAuthRule.AuthRef,
		RuleName:        decision.MatchedAuthRule.Name,
		Destination:     authDestination(req),
		DestinationPort: req.DestPort,
		Transport:       decision.Transport,
		Protocol:        decision.Protocol,
	}
	ctx.ResolveAttempt = true
	start := time.Now()
	resolved, err := s.authResolver.Resolve(context.Background(), resolveReq)
	duration := time.Since(start)
	if err != nil {
		ctx.ResolveError = err
		proxyMetrics.RecordEgressAuthResolve(decision.Protocol, "error", duration)
		applyEgressAuthFailurePolicy(ctx, decision.Protocol, "resolve_error")
		return
	}
	if resolved == nil {
		proxyMetrics.RecordEgressAuthResolve(decision.Protocol, "empty", duration)
		applyEgressAuthFailurePolicy(ctx, decision.Protocol, "material_unavailable")
		return
	}
	ctx.Resolved = resolved
	ctx.EnforcementReason = "resolved"
	if s.authCache != nil {
		s.authCache.Put(key, resolved)
	}
	proxyMetrics.RecordEgressAuthResolve(decision.Protocol, "resolved", duration)
	proxyMetrics.RecordEgressAuthDecision(decision.Protocol, "enforced", ctx.EnforcementReason)
}

func (c *egressAuthContext) ShouldBypass() bool {
	return c != nil && c.BypassReason != ""
}

func (c *egressAuthContext) FailOpen() bool {
	return c != nil && c.FailurePolicy == string(policyFailureOpen())
}

func egressAuthEnabled(cfg *config.NetdConfig) bool {
	return cfg != nil && cfg.EgressAuthEnabled
}

func egressAuthFailurePolicy(cfg *config.NetdConfig, rule *policy.CompiledEgressAuthRule) string {
	if rule != nil && rule.FailurePolicy != "" {
		return string(rule.FailurePolicy)
	}
	if cfg != nil && strings.TrimSpace(cfg.EgressAuthFailurePolicy) != "" {
		return strings.TrimSpace(cfg.EgressAuthFailurePolicy)
	}
	return string(policyFailureClosed())
}

func applyEgressAuthFailurePolicy(ctx *egressAuthContext, protocol, reason string) {
	if ctx == nil {
		return
	}
	if ctx.FailOpen() {
		ctx.BypassReason = reason
		proxyMetrics.RecordEgressAuthDecision(protocol, "bypassed", reason)
		return
	}
	ctx.EnforcementReason = reason
	proxyMetrics.RecordEgressAuthDecision(protocol, "blocked", reason)
}

func policyFailureClosed() v1alpha1.EgressAuthFailurePolicy {
	return v1alpha1.EgressAuthFailurePolicyFailClosed
}

func policyFailureOpen() v1alpha1.EgressAuthFailurePolicy {
	return v1alpha1.EgressAuthFailurePolicyFailOpen
}

func buildEgressAuthCacheKey(req *adapterRequest, decision trafficDecision) egressAuthCacheKey {
	return egressAuthCacheKey{
		SandboxID:       compiledSandboxID(req.Compiled),
		AuthRef:         decision.MatchedAuthRule.AuthRef,
		Destination:     authDestination(req),
		DestinationPort: req.DestPort,
		Transport:       decision.Transport,
		Protocol:        decision.Protocol,
	}
}

func authDestination(req *adapterRequest) string {
	if req == nil {
		return ""
	}
	if host := strings.TrimSpace(req.Host); host != "" {
		return host
	}
	if req.DestIP != nil {
		return req.DestIP.String()
	}
	return ""
}

func compiledSandboxID(compiled *policy.CompiledPolicy) string {
	if compiled == nil {
		return ""
	}
	return compiled.SandboxID
}

func compiledTeamID(compiled *policy.CompiledPolicy) string {
	if compiled == nil {
		return ""
	}
	return compiled.TeamID
}

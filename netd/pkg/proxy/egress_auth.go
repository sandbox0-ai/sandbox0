package proxy

import (
	"context"
	"crypto/tls"
	"strings"
	"sync"
	"time"

	"github.com/sandbox0-ai/sandbox0/netd/pkg/policy"
	"github.com/sandbox0-ai/sandbox0/pkg/egressauth"
)

type egressAuthContext struct {
	Rule           *policy.CompiledEgressAuthRule
	Resolved       *egressauth.ResolveResponse
	CacheHit       bool
	ResolveAttempt bool
	ResolveError   error
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
	c.mu.Unlock()
}

func (noopEgressAuthResolver) Resolve(_ context.Context, _ *egressauth.ResolveRequest) (*egressauth.ResolveResponse, error) {
	return nil, nil
}

func cloneResolveResponse(in *egressauth.ResolveResponse) *egressauth.ResolveResponse {
	if in == nil {
		return nil
	}
	out := &egressauth.ResolveResponse{
		AuthRef: in.AuthRef,
	}
	if len(in.Headers) > 0 {
		out.Headers = make(map[string]string, len(in.Headers))
		for key, value := range in.Headers {
			out.Headers[key] = value
		}
	}
	if in.ExpiresAt != nil {
		expiresAt := *in.ExpiresAt
		out.ExpiresAt = &expiresAt
	}
	return out
}

func (s *Server) attachEgressAuth(req *adapterRequest, decision trafficDecision) {
	if req == nil || decision.MatchedAuthRule == nil {
		return
	}
	ctx := &egressAuthContext{
		Rule: decision.MatchedAuthRule,
	}
	req.EgressAuth = ctx
	if s == nil || s.authCache == nil {
		return
	}

	key := buildEgressAuthCacheKey(req, decision)
	if resolved, ok := s.authCache.Get(key); ok {
		ctx.Resolved = resolved
		ctx.CacheHit = true
		return
	}
	if s.authResolver == nil {
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
	resolved, err := s.authResolver.Resolve(context.Background(), resolveReq)
	if err != nil {
		ctx.ResolveError = err
		return
	}
	if resolved == nil {
		return
	}
	ctx.Resolved = resolved
	s.authCache.Put(key, resolved)
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

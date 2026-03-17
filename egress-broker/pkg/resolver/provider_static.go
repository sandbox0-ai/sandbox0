package resolver

import (
	"context"
	"strings"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/egressauth"
)

type staticProvider struct{}

func (p *staticProvider) Resolve(_ context.Context, _ *egressauth.ResolveRequest, binding *egressauth.CredentialBinding, defaultTTL time.Duration) (*ResolveResult, error) {
	if binding == nil {
		return nil, nil
	}

	ttl := defaultTTL
	if ttlOverride, ok := parseBindingTTL(binding.Config, defaultTTL); ok {
		ttl = ttlOverride
	}
	expiresAt := time.Now().UTC().Add(ttl)
	return &ResolveResult{
		Response: egressauth.NewHTTPHeadersResolveResponse(binding.Ref, binding.Headers, &expiresAt),
		TTL: ttl,
	}, nil
}

func parseBindingTTL(config map[string]string, defaultTTL time.Duration) (time.Duration, bool) {
	if len(config) == 0 {
		return defaultTTL, false
	}

	for _, key := range []string{"cacheTtl", "cache_ttl", "ttl"} {
		raw := strings.TrimSpace(config[key])
		if raw == "" {
			continue
		}
		ttl, err := time.ParseDuration(raw)
		if err != nil || ttl <= 0 {
			return defaultTTL, false
		}
		return ttl, true
	}
	return defaultTTL, false
}

package resolver

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	"github.com/sandbox0-ai/sandbox0/pkg/egressauth"
	"go.uber.org/zap"
)

var ErrAuthRefNotFound = errors.New("authRef not found")

// Service owns broker-side runtime resolution and caching.
type Service struct {
	clusterID     string
	defaultTTL    time.Duration
	logger        *zap.Logger
	bindingStore  egressauth.BindingStore
	staticAuth    map[string]config.StaticEgressAuthConfig
	providers     map[string]Provider
	resolveCache  *resultCache
}

func NewService(cfg *config.EgressBrokerConfig, bindingStore egressauth.BindingStore, logger *zap.Logger) *Service {
	if cfg == nil {
		cfg = &config.EgressBrokerConfig{}
	}
	if logger == nil {
		logger = zap.NewNop()
	}

	service := &Service{
		clusterID:    strings.TrimSpace(cfg.ClusterID),
		defaultTTL:   cfg.DefaultResolveTTL.Duration,
		logger:       logger,
		bindingStore: bindingStore,
		staticAuth:   buildStaticAuthMap(cfg),
		providers:    make(map[string]Provider),
		resolveCache: newResultCache(2048),
	}
	service.RegisterProvider("static_headers", &staticHeadersProvider{})
	return service
}

func (s *Service) RegisterProvider(name string, provider Provider) {
	if s == nil || provider == nil {
		return
	}
	name = strings.ToLower(strings.TrimSpace(name))
	if name == "" {
		return
	}
	s.providers[name] = provider
}

func (s *Service) Resolve(ctx context.Context, req *egressauth.ResolveRequest) (*egressauth.ResolveResponse, error) {
	if req == nil {
		return nil, fmt.Errorf("resolve request is required")
	}
	if strings.TrimSpace(req.AuthRef) == "" {
		return nil, fmt.Errorf("authRef is required")
	}

	if binding, updatedAt := s.lookupBinding(ctx, req); binding != nil {
		return s.resolveBinding(ctx, req, binding, updatedAt)
	}
	return s.resolveStatic(req)
}

func (s *Service) resolveBinding(ctx context.Context, req *egressauth.ResolveRequest, binding *egressauth.CredentialBinding, updatedAt time.Time) (*egressauth.ResolveResponse, error) {
	cacheKey := bindingCacheKey(s.clusterID, req, binding, updatedAt)
	now := time.Now().UTC()
	if response, ok := s.resolveCache.Get(cacheKey, now); ok {
		return response, nil
	}

	sourceVersion, err := s.bindingStore.GetSourceVersion(ctx, binding.SourceID, binding.SourceVersion)
	if err != nil {
		return nil, err
	}
	if sourceVersion == nil {
		return nil, fmt.Errorf("credential source version not found for %q", binding.Ref)
	}

	providerName := strings.ToLower(strings.TrimSpace(sourceVersion.ResolverKind))
	provider, ok := s.providers[providerName]
	if !ok {
		return nil, &UnsupportedProviderError{Provider: sourceVersion.ResolverKind}
	}

	result, err := provider.Resolve(ctx, req, binding, sourceVersion, s.defaultTTL)
	if err != nil {
		return nil, err
	}
	if result == nil || result.Response == nil {
		return nil, fmt.Errorf("provider %q returned empty response", providerName)
	}

	cacheTTL := result.TTL
	if cacheTTL <= 0 && result.Response.ExpiresAt != nil {
		cacheTTL = time.Until(*result.Response.ExpiresAt)
	}
	s.resolveCache.Set(cacheKey, result.Response, cacheTTL, now)
	return cloneResolveResponse(result.Response), nil
}

func (s *Service) resolveStatic(req *egressauth.ResolveRequest) (*egressauth.ResolveResponse, error) {
	entry, ok := s.lookupStaticAuth(req.AuthRef)
	if !ok {
		return nil, ErrAuthRefNotFound
	}

	cacheKey := staticCacheKey(req)
	now := time.Now().UTC()
	if response, ok := s.resolveCache.Get(cacheKey, now); ok {
		return response, nil
	}

	expiresAt := now.Add(entry.TTL.Duration)
	response := egressauth.NewHTTPHeadersResolveResponse(entry.AuthRef, entry.Headers, &expiresAt)
	s.resolveCache.Set(cacheKey, response, entry.TTL.Duration, now)
	return response, nil
}

func (s *Service) lookupBinding(ctx context.Context, req *egressauth.ResolveRequest) (*egressauth.CredentialBinding, time.Time) {
	if s == nil || s.bindingStore == nil || req == nil {
		return nil, time.Time{}
	}
	if s.clusterID == "" || strings.TrimSpace(req.SandboxID) == "" {
		return nil, time.Time{}
	}

	record, err := s.bindingStore.GetBindings(ctx, s.clusterID, req.SandboxID)
	if err != nil {
		s.logger.Warn("Failed to load credential bindings",
			zap.String("cluster_id", s.clusterID),
			zap.String("sandbox_id", req.SandboxID),
			zap.Error(err),
		)
		return nil, time.Time{}
	}
	if record == nil {
		return nil, time.Time{}
	}

	authRef := strings.TrimSpace(req.AuthRef)
	for idx := range record.Bindings {
		if strings.TrimSpace(record.Bindings[idx].Ref) == authRef {
			return &record.Bindings[idx], record.UpdatedAt
		}
	}
	return nil, time.Time{}
}

func (s *Service) lookupStaticAuth(authRef string) (config.StaticEgressAuthConfig, bool) {
	if s == nil || len(s.staticAuth) == 0 {
		return config.StaticEgressAuthConfig{}, false
	}
	entry, ok := s.staticAuth[strings.TrimSpace(authRef)]
	return entry, ok
}

func buildStaticAuthMap(cfg *config.EgressBrokerConfig) map[string]config.StaticEgressAuthConfig {
	if cfg == nil || len(cfg.StaticAuth) == 0 {
		return nil
	}
	out := make(map[string]config.StaticEgressAuthConfig, len(cfg.StaticAuth))
	for _, entry := range cfg.StaticAuth {
		authRef := strings.TrimSpace(entry.AuthRef)
		if authRef == "" {
			continue
		}
		out[authRef] = entry
	}
	return out
}

func bindingCacheKey(clusterID string, req *egressauth.ResolveRequest, binding *egressauth.CredentialBinding, updatedAt time.Time) string {
	return strings.Join([]string{
		"binding",
		clusterID,
		strings.TrimSpace(req.SandboxID),
		strings.TrimSpace(req.AuthRef),
		binding.SourceRef,
		fmt.Sprintf("%d", binding.SourceID),
		fmt.Sprintf("%d", binding.SourceVersion),
		updatedAt.UTC().Format(time.RFC3339Nano),
		strings.ToLower(strings.TrimSpace(req.Destination)),
		fmt.Sprintf("%d", req.DestinationPort),
		strings.ToLower(strings.TrimSpace(req.Transport)),
		strings.ToLower(strings.TrimSpace(req.Protocol)),
		strings.TrimSpace(req.RuleName),
	}, "\x00")
}

func staticCacheKey(req *egressauth.ResolveRequest) string {
	return strings.Join([]string{
		"static",
		strings.TrimSpace(req.AuthRef),
		strings.TrimSpace(req.SandboxID),
		strings.ToLower(strings.TrimSpace(req.Destination)),
		fmt.Sprintf("%d", req.DestinationPort),
		strings.ToLower(strings.TrimSpace(req.Transport)),
		strings.ToLower(strings.TrimSpace(req.Protocol)),
		strings.TrimSpace(req.RuleName),
	}, "\x00")
}

func cloneResolveResponse(in *egressauth.ResolveResponse) *egressauth.ResolveResponse {
	return egressauth.CloneResolveResponse(in)
}

func cloneHeaders(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

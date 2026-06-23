package runtime

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/egressauth"
	"go.uber.org/zap"
)

var ErrAuthRefNotFound = errors.New("authRef not found")

type StaticAuthConfig struct {
	AuthRef string
	Headers map[string]string
	TTL     time.Duration
}

type Config struct {
	DefaultResolveTTL time.Duration
	StaticAuth        []StaticAuthConfig
}

// Service owns runtime credential resolution and caching.
type Service struct {
	defaultTTL   time.Duration
	logger       *zap.Logger
	bindingStore egressauth.BindingStore
	staticAuth   map[string]StaticAuthConfig
	providers    map[string]Provider
}

func NewService(cfg Config, bindingStore egressauth.BindingStore, logger *zap.Logger) *Service {
	if logger == nil {
		logger = zap.NewNop()
	}
	if cfg.DefaultResolveTTL <= 0 {
		cfg.DefaultResolveTTL = 5 * time.Minute
	}

	service := &Service{
		defaultTTL:   cfg.DefaultResolveTTL,
		logger:       logger,
		bindingStore: bindingStore,
		staticAuth:   buildStaticAuthMap(cfg.StaticAuth),
		providers:    make(map[string]Provider),
	}
	service.RegisterProvider("static_headers", &staticHeadersProvider{})
	service.RegisterProvider("static_tls_client_certificate", &staticTLSClientCertificateProvider{})
	service.RegisterProvider("static_username_password", &staticUsernamePasswordProvider{})
	service.RegisterProvider("static_ssh_private_key", &staticSSHPrivateKeyProvider{})
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

	if binding := s.lookupBinding(ctx, req); binding != nil {
		return s.resolveBinding(ctx, req, binding)
	}
	return s.resolveStatic(req)
}

func (s *Service) resolveBinding(ctx context.Context, req *egressauth.ResolveRequest, binding *egressauth.CredentialBinding) (*egressauth.ResolveResponse, error) {
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

	return cloneResolveResponse(result.Response), nil
}

func (s *Service) resolveStatic(req *egressauth.ResolveRequest) (*egressauth.ResolveResponse, error) {
	entry, ok := s.lookupStaticAuth(req.AuthRef)
	if !ok {
		return nil, ErrAuthRefNotFound
	}

	now := time.Now().UTC()
	expiresAt := now.Add(entry.TTL)
	response := egressauth.NewHTTPHeadersResolveResponse(entry.AuthRef, entry.Headers, &expiresAt)
	return response, nil
}

func (s *Service) lookupBinding(ctx context.Context, req *egressauth.ResolveRequest) *egressauth.CredentialBinding {
	if s == nil || s.bindingStore == nil || req == nil {
		return nil
	}
	if strings.TrimSpace(req.TeamID) == "" || strings.TrimSpace(req.SandboxID) == "" {
		return nil
	}

	record, err := s.bindingStore.GetBindings(ctx, req.TeamID, req.SandboxID)
	if err != nil {
		s.logger.Warn("Failed to load credential bindings",
			zap.String("team_id", req.TeamID),
			zap.String("sandbox_id", req.SandboxID),
			zap.Error(err),
		)
		return nil
	}
	if record == nil {
		return nil
	}

	authRef := strings.TrimSpace(req.AuthRef)
	for idx := range record.Bindings {
		if strings.TrimSpace(record.Bindings[idx].Ref) == authRef {
			return &record.Bindings[idx]
		}
	}
	return nil
}

func (s *Service) lookupStaticAuth(authRef string) (StaticAuthConfig, bool) {
	if s == nil || len(s.staticAuth) == 0 {
		return StaticAuthConfig{}, false
	}
	entry, ok := s.staticAuth[strings.TrimSpace(authRef)]
	return entry, ok
}

func buildStaticAuthMap(entries []StaticAuthConfig) map[string]StaticAuthConfig {
	if len(entries) == 0 {
		return nil
	}
	out := make(map[string]StaticAuthConfig, len(entries))
	for _, entry := range entries {
		authRef := strings.TrimSpace(entry.AuthRef)
		if authRef == "" {
			continue
		}
		if entry.TTL <= 0 {
			entry.TTL = 5 * time.Minute
		}
		out[authRef] = StaticAuthConfig{
			AuthRef: authRef,
			Headers: cloneStringMap(entry.Headers),
			TTL:     entry.TTL,
		}
	}
	return out
}

func cloneResolveResponse(in *egressauth.ResolveResponse) *egressauth.ResolveResponse {
	return egressauth.CloneResolveResponse(in)
}

func cloneStringMap(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

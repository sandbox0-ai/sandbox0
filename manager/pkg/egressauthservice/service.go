package egressauthservice

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/egressauthservice/runtime"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/egressauthstore"
	"github.com/sandbox0-ai/sandbox0/pkg/egressauth"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	"go.uber.org/zap"
)

// StaticAuthConfig configures one manager-owned fallback credential mapping.
type StaticAuthConfig = runtime.StaticAuthConfig

type EgressAuthServiceConfig struct {
	DefaultResolveTTL time.Duration
	StaticAuth        []StaticAuthConfig
}

type EgressAuthService struct {
	resolver *runtime.Service
}

func NewEgressAuthService(cfg EgressAuthServiceConfig, bindingStore egressauthstore.BindingStore, logger *zap.Logger) *EgressAuthService {
	return &EgressAuthService{
		resolver: runtime.NewService(runtime.Config{
			DefaultResolveTTL: cfg.DefaultResolveTTL,
			StaticAuth:        cfg.StaticAuth,
		}, bindingStore, logger),
	}
}

func (s *EgressAuthService) Resolve(ctx context.Context, req *egressauth.ResolveRequest) (*egressauth.ResolveResponse, error) {
	if s == nil || s.resolver == nil {
		return nil, errors.New("egress auth service is not configured")
	}
	return s.resolver.Resolve(ctx, req)
}

func MapEgressAuthResolveError(err error) (int, string, string) {
	if err == nil {
		return http.StatusOK, "", ""
	}
	if errors.Is(err, runtime.ErrAuthRefNotFound) {
		return http.StatusNotFound, spec.CodeNotFound, "authRef not found"
	}

	var unsupported *runtime.UnsupportedProviderError
	if errors.As(err, &unsupported) {
		return http.StatusConflict, spec.CodeConflict, err.Error()
	}
	return http.StatusInternalServerError, spec.CodeInternal, "resolve authRef failed"
}

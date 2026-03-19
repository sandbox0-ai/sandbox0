package service

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/egressauth"
	egressauthruntime "github.com/sandbox0-ai/sandbox0/pkg/egressauth/runtime"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	"go.uber.org/zap"
)

type EgressAuthServiceConfig struct {
	DefaultResolveTTL time.Duration
	StaticAuth        []egressauthruntime.StaticAuthConfig
}

type EgressAuthService struct {
	resolver *egressauthruntime.Service
}

func NewEgressAuthService(cfg EgressAuthServiceConfig, bindingStore egressauth.BindingStore, logger *zap.Logger) *EgressAuthService {
	return &EgressAuthService{
		resolver: egressauthruntime.NewService(egressauthruntime.Config{
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
	if errors.Is(err, egressauthruntime.ErrAuthRefNotFound) {
		return http.StatusNotFound, spec.CodeNotFound, "authRef not found"
	}

	var unsupported *egressauthruntime.UnsupportedProviderError
	if errors.As(err, &unsupported) {
		return http.StatusConflict, spec.CodeConflict, err.Error()
	}
	return http.StatusInternalServerError, spec.CodeInternal, "resolve authRef failed"
}

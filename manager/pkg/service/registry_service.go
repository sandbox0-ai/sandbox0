package service

import (
	"context"
	"errors"

	"github.com/sandbox0-ai/infra/manager/pkg/registry"
	"go.uber.org/zap"
)

// RegistryService provides registry credentials for uploads.
type RegistryService struct {
	provider registry.Provider
	logger   *zap.Logger
}

// ErrRegistryProviderNotConfigured indicates no registry provider configured.
var ErrRegistryProviderNotConfigured = errors.New("registry provider is not configured")

func NewRegistryService(provider registry.Provider, logger *zap.Logger) *RegistryService {
	return &RegistryService{
		provider: provider,
		logger:   logger,
	}
}

func (s *RegistryService) GetPushCredentials(ctx context.Context, teamID string) (*registry.Credential, error) {
	if s.provider == nil {
		return nil, ErrRegistryProviderNotConfigured
	}
	return s.provider.GetPushCredentials(ctx, teamID)
}

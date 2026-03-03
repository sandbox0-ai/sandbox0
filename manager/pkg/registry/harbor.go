package registry

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/sandbox0-ai/infra/infra-operator/api/config"
)

type harborProvider struct {
	cfg     config.RegistryHarborConfig
	secrets secretReader
}

func (p *harborProvider) GetPushCredentials(ctx context.Context, teamID string) (*Credential, error) {
	registry := normalizeRegistryHost(p.cfg.Registry)
	if registry == "" {
		return nil, fmt.Errorf("harbor registry is required")
	}
	if strings.TrimSpace(p.cfg.CredentialsSecret) == "" {
		return nil, fmt.Errorf("harbor credentials secret is required")
	}
	usernameKey := strings.TrimSpace(p.cfg.UsernameKey)
	if usernameKey == "" {
		usernameKey = "username"
	}
	passwordKey := strings.TrimSpace(p.cfg.PasswordKey)
	if passwordKey == "" {
		passwordKey = "password"
	}
	username, err := p.secrets.read(ctx, p.cfg.CredentialsSecret, usernameKey)
	if err != nil {
		return nil, fmt.Errorf("read harbor username: %w", err)
	}
	password, err := p.secrets.read(ctx, p.cfg.CredentialsSecret, passwordKey)
	if err != nil {
		return nil, fmt.Errorf("read harbor password: %w", err)
	}
	// Harbor credentials are static credentials sourced from Kubernetes secret.
	return &Credential{
		Provider:  "harbor",
		Registry:  registry,
		Username:  username,
		Password:  password,
		ExpiresAt: time.Now().Add(365 * 24 * time.Hour),
	}, nil
}

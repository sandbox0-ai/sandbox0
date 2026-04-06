package registry

import (
	"context"
	"fmt"
	"strings"

	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
)

type harborProvider struct {
	cfg     config.RegistryHarborConfig
	secrets secretReader
}

func (p *harborProvider) GetPushCredentials(ctx context.Context, req PushCredentialsRequest) (*Credential, error) {
	// TODO: add team-scoped ephemeral credentials similar to AWS AssumeRole + session policy.
	registry := normalizeRegistryHost(p.cfg.Registry)
	if registry == "" {
		return nil, fmt.Errorf("harbor registry is required")
	}
	username := strings.TrimSpace(p.cfg.Username)
	password := strings.TrimSpace(p.cfg.Password)
	if username == "" || password == "" {
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
		var err error
		username, err = p.secrets.read(ctx, p.cfg.CredentialsSecret, usernameKey)
		if err != nil {
			return nil, fmt.Errorf("read harbor username: %w", err)
		}
		password, err = p.secrets.read(ctx, p.cfg.CredentialsSecret, passwordKey)
		if err != nil {
			return nil, fmt.Errorf("read harbor password: %w", err)
		}
	}
	// Harbor credentials are static credentials sourced from Kubernetes secret.
	return &Credential{
		Provider:     "harbor",
		PushRegistry: registry,
		Username:     username,
		Password:     password,
	}, nil
}

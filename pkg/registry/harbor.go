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
		var err error
		username, err = p.secrets.readRequired(ctx, p.cfg.CredentialsSecret, p.cfg.UsernameKey, "username", "harbor username")
		if err != nil {
			return nil, err
		}
		password, err = p.secrets.readRequired(ctx, p.cfg.CredentialsSecret, p.cfg.PasswordKey, "password", "harbor password")
		if err != nil {
			return nil, err
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

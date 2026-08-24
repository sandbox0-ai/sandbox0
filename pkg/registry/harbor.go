package registry

import (
	"context"
	"fmt"

	"github.com/sandbox0-ai/sandbox0/pkg/config"
)

type harborProvider struct {
	cfg config.RegistryHarborConfig
}

func (p *harborProvider) GetPushCredentials(ctx context.Context, req PushCredentialsRequest) (*Credential, error) {
	// TODO: add team-scoped ephemeral credentials similar to AWS AssumeRole + session policy.
	registry := normalizeRegistryHost(p.cfg.Registry)
	if registry == "" {
		return nil, fmt.Errorf("harbor registry is required")
	}
	username, err := credentialValue(p.cfg.Username, p.cfg.UsernameFile, "harbor username")
	if err != nil {
		return nil, err
	}
	password, err := credentialValue(p.cfg.Password, p.cfg.PasswordFile, "harbor password")
	if err != nil {
		return nil, err
	}
	if username == "" || password == "" {
		return nil, fmt.Errorf("harbor username and password are required")
	}
	return &Credential{
		Provider:     "harbor",
		PushRegistry: registry,
		Username:     username,
		Password:     password,
	}, nil
}

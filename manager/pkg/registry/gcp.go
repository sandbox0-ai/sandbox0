package registry

import (
	"context"
	"fmt"
	"strings"

	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	"golang.org/x/oauth2/google"
)

type gcpProvider struct {
	cfg     config.RegistryGCPConfig
	secrets secretReader
}

func (p *gcpProvider) GetPushCredentials(ctx context.Context, teamID string) (*Credential, error) {
	registry := normalizeRegistryHost(p.cfg.Registry)
	if registry == "" {
		return nil, fmt.Errorf("gcp registry is required")
	}
	if strings.TrimSpace(p.cfg.ServiceAccountSecret) == "" {
		return nil, fmt.Errorf("gcp service account secret is required")
	}
	secretKey := strings.TrimSpace(p.cfg.ServiceAccountKey)
	if secretKey == "" {
		secretKey = "serviceAccount.json"
	}
	serviceAccountJSON, err := p.secrets.read(ctx, p.cfg.ServiceAccountSecret, secretKey)
	if err != nil {
		return nil, fmt.Errorf("read gcp service account: %w", err)
	}
	jwtConfig, err := google.JWTConfigFromJSON([]byte(serviceAccountJSON), "https://www.googleapis.com/auth/cloud-platform")
	if err != nil {
		return nil, fmt.Errorf("parse gcp service account json: %w", err)
	}
	token, err := jwtConfig.TokenSource(ctx).Token()
	if err != nil {
		return nil, fmt.Errorf("fetch gcp access token: %w", err)
	}

	return &Credential{
		Provider:     "gcp",
		PushRegistry: registry,
		Username:     "oauth2accesstoken",
		Password:     token.AccessToken,
		ExpiresAt:    timePtr(token.Expiry),
	}, nil
}

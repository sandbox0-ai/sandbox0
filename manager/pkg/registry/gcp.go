package registry

import (
	"context"
	"fmt"
	"strings"

	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
)

var (
	gcpJWTConfigFromJSON = func(data []byte, scopes ...string) (oauth2.TokenSource, error) {
		jwtConfig, err := google.JWTConfigFromJSON(data, scopes...)
		if err != nil {
			return nil, err
		}
		return jwtConfig.TokenSource(context.Background()), nil
	}
	gcpDefaultTokenSource = func(ctx context.Context, scopes ...string) (oauth2.TokenSource, error) {
		return google.DefaultTokenSource(ctx, scopes...)
	}
)

type gcpProvider struct {
	cfg     config.RegistryGCPConfig
	secrets secretReader
}

func (p *gcpProvider) GetPushCredentials(ctx context.Context, req PushCredentialsRequest) (*Credential, error) {
	// TODO: add team-scoped ephemeral credentials similar to AWS AssumeRole + session policy.
	registry := normalizeRegistryHost(p.cfg.Registry)
	if registry == "" {
		return nil, fmt.Errorf("gcp registry is required")
	}
	serviceAccountJSON := strings.TrimSpace(p.cfg.ServiceAccountJSON)
	if serviceAccountJSON == "" {
		if strings.TrimSpace(p.cfg.ServiceAccountSecret) == "" {
			tokenSource, err := gcpDefaultTokenSource(ctx, "https://www.googleapis.com/auth/cloud-platform")
			if err != nil {
				return nil, fmt.Errorf("resolve gcp application default credentials: %w", err)
			}
			token, err := tokenSource.Token()
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
		secretKey := strings.TrimSpace(p.cfg.ServiceAccountKey)
		if secretKey == "" {
			secretKey = "serviceAccount.json"
		}
		var err error
		serviceAccountJSON, err = p.secrets.read(ctx, p.cfg.ServiceAccountSecret, secretKey)
		if err != nil {
			return nil, fmt.Errorf("read gcp service account: %w", err)
		}
	}
	tokenSource, err := gcpJWTConfigFromJSON([]byte(serviceAccountJSON), "https://www.googleapis.com/auth/cloud-platform")
	if err != nil {
		return nil, fmt.Errorf("parse gcp service account json: %w", err)
	}
	token, err := tokenSource.Token()
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

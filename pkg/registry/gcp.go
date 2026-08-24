package registry

import (
	"context"
	"fmt"

	"github.com/sandbox0-ai/sandbox0/pkg/config"
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
	cfg config.RegistryGCPConfig
}

func (p *gcpProvider) GetPushCredentials(ctx context.Context, req PushCredentialsRequest) (*Credential, error) {
	// TODO: add team-scoped ephemeral credentials similar to AWS AssumeRole + session policy.
	registry := normalizeRegistryHost(p.cfg.Registry)
	if registry == "" {
		return nil, fmt.Errorf("gcp registry is required")
	}
	serviceAccountJSON, err := credentialValue(
		p.cfg.ServiceAccountJSON,
		p.cfg.ServiceAccountJSONFile,
		"gcp service account json",
	)
	if err != nil {
		return nil, err
	}
	if serviceAccountJSON == "" {
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

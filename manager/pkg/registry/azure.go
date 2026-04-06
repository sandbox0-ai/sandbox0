package registry

import (
	"context"
	"fmt"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
)

type azureProvider struct {
	cfg     config.RegistryAzureConfig
	secrets secretReader
}

func (p *azureProvider) GetPushCredentials(ctx context.Context, req PushCredentialsRequest) (*Credential, error) {
	// TODO: add team-scoped ephemeral credentials similar to AWS AssumeRole + session policy.
	registry := normalizeRegistryHost(p.cfg.Registry)
	if registry == "" {
		return nil, fmt.Errorf("azure registry is required")
	}
	tenantID := strings.TrimSpace(p.cfg.TenantID)
	clientID := strings.TrimSpace(p.cfg.ClientID)
	clientSecret := strings.TrimSpace(p.cfg.ClientSecret)
	if tenantID == "" || clientID == "" || clientSecret == "" {
		if strings.TrimSpace(p.cfg.CredentialsSecret) == "" {
			return nil, fmt.Errorf("azure credentials secret is required")
		}
		tenantIDKey := strings.TrimSpace(p.cfg.TenantIDKey)
		if tenantIDKey == "" {
			tenantIDKey = "tenantId"
		}
		clientIDKey := strings.TrimSpace(p.cfg.ClientIDKey)
		if clientIDKey == "" {
			clientIDKey = "clientId"
		}
		clientSecretKey := strings.TrimSpace(p.cfg.ClientSecretKey)
		if clientSecretKey == "" {
			clientSecretKey = "clientSecret"
		}

		var err error
		tenantID, err = p.secrets.read(ctx, p.cfg.CredentialsSecret, tenantIDKey)
		if err != nil {
			return nil, fmt.Errorf("read azure tenant id: %w", err)
		}
		clientID, err = p.secrets.read(ctx, p.cfg.CredentialsSecret, clientIDKey)
		if err != nil {
			return nil, fmt.Errorf("read azure client id: %w", err)
		}
		clientSecret, err = p.secrets.read(ctx, p.cfg.CredentialsSecret, clientSecretKey)
		if err != nil {
			return nil, fmt.Errorf("read azure client secret: %w", err)
		}
	}

	credential, err := azidentity.NewClientSecretCredential(tenantID, clientID, clientSecret, nil)
	if err != nil {
		return nil, fmt.Errorf("create azure credential: %w", err)
	}
	scope := fmt.Sprintf("https://%s/.default", registry)
	token, err := credential.GetToken(ctx, policy.TokenRequestOptions{
		Scopes: []string{scope},
	})
	if err != nil {
		return nil, fmt.Errorf("fetch azure access token: %w", err)
	}

	return &Credential{
		Provider:     "azure",
		PushRegistry: registry,
		Username:     "00000000-0000-0000-0000-000000000000",
		Password:     token.Token,
		ExpiresAt:    timePtr(token.ExpiresOn),
	}, nil
}

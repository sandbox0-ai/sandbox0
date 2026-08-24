package registry

import (
	"context"
	"fmt"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/sandbox0-ai/sandbox0/pkg/config"
)

type azureProvider struct {
	cfg config.RegistryAzureConfig
}

func (p *azureProvider) GetPushCredentials(ctx context.Context, req PushCredentialsRequest) (*Credential, error) {
	// TODO: add team-scoped ephemeral credentials similar to AWS AssumeRole + session policy.
	registry := normalizeRegistryHost(p.cfg.Registry)
	if registry == "" {
		return nil, fmt.Errorf("azure registry is required")
	}
	tenantID, err := credentialValue(p.cfg.TenantID, p.cfg.TenantIDFile, "azure tenant id")
	if err != nil {
		return nil, err
	}
	clientID, err := credentialValue(p.cfg.ClientID, p.cfg.ClientIDFile, "azure client id")
	if err != nil {
		return nil, err
	}
	clientSecret, err := credentialValue(p.cfg.ClientSecret, p.cfg.ClientSecretFile, "azure client secret")
	if err != nil {
		return nil, err
	}

	var credential azcore.TokenCredential
	if tenantID == "" && clientID == "" && clientSecret == "" {
		credential, err = azidentity.NewDefaultAzureCredential(nil)
	} else {
		if tenantID == "" || clientID == "" || clientSecret == "" {
			return nil, fmt.Errorf("azure tenant id, client id, and client secret must be configured together")
		}
		credential, err = azidentity.NewClientSecretCredential(tenantID, clientID, clientSecret, nil)
	}
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

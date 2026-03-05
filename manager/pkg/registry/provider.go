package registry

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	"go.uber.org/zap"
	corelisters "k8s.io/client-go/listers/core/v1"
)

// Credential contains registry login credentials.
type Credential struct {
	Provider     string     `json:"provider"`
	PushRegistry string     `json:"pushRegistry"`
	PullRegistry string     `json:"pullRegistry,omitempty"`
	Username     string     `json:"username"`
	Password     string     `json:"password"`
	ExpiresAt    *time.Time `json:"expiresAt,omitempty"`
}

// Provider returns short-lived registry credentials.
type Provider interface {
	GetPushCredentials(ctx context.Context, teamID string) (*Credential, error)
}

// NewProvider creates a registry provider based on config.
func NewProvider(cfg config.RegistryConfig, secretLister corelisters.SecretLister, logger *zap.Logger) (Provider, error) {
	provider := strings.TrimSpace(strings.ToLower(cfg.Provider))
	if provider == "" {
		return nil, nil
	}
	if logger != nil {
		logger.Info("Registry provider configured", zap.String("provider", provider))
	}
	namespace := resolveNamespace(cfg.Namespace)
	secretReader := secretReader{
		secretLister: secretLister,
		namespace:    namespace,
	}
	pullRegistry := normalizeRegistryHost(cfg.PullRegistry)

	var base Provider
	switch provider {
	case "aws":
		if cfg.AWS == nil {
			return nil, fmt.Errorf("registry aws config is required")
		}
		base = &awsProvider{cfg: *cfg.AWS, secrets: secretReader}
	case "gcp":
		if cfg.GCP == nil {
			return nil, fmt.Errorf("registry gcp config is required")
		}
		base = &gcpProvider{cfg: *cfg.GCP, secrets: secretReader}
	case "azure":
		if cfg.Azure == nil {
			return nil, fmt.Errorf("registry azure config is required")
		}
		base = &azureProvider{cfg: *cfg.Azure, secrets: secretReader}
	case "aliyun":
		if cfg.Aliyun == nil {
			return nil, fmt.Errorf("registry aliyun config is required")
		}
		base = &aliyunProvider{cfg: *cfg.Aliyun, secrets: secretReader}
	case "harbor":
		if cfg.Harbor == nil {
			return nil, fmt.Errorf("registry harbor config is required")
		}
		base = &harborProvider{cfg: *cfg.Harbor, secrets: secretReader}
	case "builtin":
		if cfg.Builtin == nil {
			return nil, fmt.Errorf("registry builtin config is required")
		}
		base = &builtinProvider{
			cfg:      *cfg.Builtin,
			registry: normalizeRegistryHost(cfg.PushRegistry),
			secrets:  secretReader,
		}
	default:
		return nil, fmt.Errorf("unsupported registry provider: %s", provider)
	}

	return &providerWithPullRegistry{
		base:         base,
		pullRegistry: pullRegistry,
	}, nil
}

type providerWithPullRegistry struct {
	base         Provider
	pullRegistry string
}

func (p *providerWithPullRegistry) GetPushCredentials(ctx context.Context, teamID string) (*Credential, error) {
	creds, err := p.base.GetPushCredentials(ctx, teamID)
	if err != nil {
		return nil, err
	}
	if creds == nil {
		return nil, nil
	}

	if p.pullRegistry != "" {
		creds.PullRegistry = p.pullRegistry
	} else if creds.PullRegistry == "" {
		// Default to push endpoint when no dedicated pull endpoint is configured.
		creds.PullRegistry = creds.PushRegistry
	}
	return creds, nil
}

type secretReader struct {
	secretLister corelisters.SecretLister
	namespace    string
}

func (s secretReader) read(ctx context.Context, name, key string) (string, error) {
	if name == "" {
		return "", fmt.Errorf("secret name is required")
	}
	var secretData map[string][]byte
	if s.secretLister != nil {
		secret, err := s.secretLister.Secrets(s.namespace).Get(name)
		if err != nil {
			return "", err
		}
		secretData = secret.Data
	} else {
		return "", fmt.Errorf("secret lister is required for registry credentials")
	}
	if secretData == nil {
		return "", fmt.Errorf("secret %q has no data", name)
	}
	value, ok := secretData[key]
	if !ok {
		return "", fmt.Errorf("secret %q missing key %q", name, key)
	}
	return string(value), nil
}

func resolveNamespace(explicit string) string {
	if strings.TrimSpace(explicit) != "" {
		return explicit
	}
	data, err := os.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/namespace")
	if err == nil {
		if ns := strings.TrimSpace(string(data)); ns != "" {
			return ns
		}
	}
	return "default"
}

func normalizeRegistryHost(raw string) string {
	value := strings.TrimSpace(raw)
	value = strings.TrimPrefix(value, "https://")
	value = strings.TrimPrefix(value, "http://")
	return value
}

func timePtr(t time.Time) *time.Time {
	if t.IsZero() {
		return nil
	}
	return &t
}

// builtinProvider provides credentials for the builtin registry.
type builtinProvider struct {
	cfg      config.RegistryBuiltinConfig
	registry string
	secrets  secretReader
}

func (p *builtinProvider) GetPushCredentials(ctx context.Context, teamID string) (*Credential, error) {
	if strings.TrimSpace(p.registry) == "" {
		return nil, fmt.Errorf("builtin push registry is required")
	}
	username, err := p.secrets.read(ctx, p.cfg.AuthSecretName, p.cfg.UsernameKey)
	if err != nil {
		return nil, fmt.Errorf("read username: %w", err)
	}
	password, err := p.secrets.read(ctx, p.cfg.AuthSecretName, p.cfg.PasswordKey)
	if err != nil {
		return nil, fmt.Errorf("read password: %w", err)
	}
	return &Credential{
		Provider:     "builtin",
		PushRegistry: p.registry,
		Username:     username,
		Password:     password,
	}, nil
}

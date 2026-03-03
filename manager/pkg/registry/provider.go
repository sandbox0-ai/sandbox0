package registry

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/sandbox0-ai/infra/infra-operator/api/config"
	"go.uber.org/zap"
	corelisters "k8s.io/client-go/listers/core/v1"
)

// Credential contains registry login credentials.
type Credential struct {
	Provider  string    `json:"provider"`
	Registry  string    `json:"registry"`
	Username  string    `json:"username"`
	Password  string    `json:"password"`
	ExpiresAt time.Time `json:"expiresAt"`
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

	switch provider {
	case "aws":
		if cfg.AWS == nil {
			return nil, fmt.Errorf("registry aws config is required")
		}
		return &awsProvider{cfg: *cfg.AWS, secrets: secretReader}, nil
	case "gcp":
		if cfg.GCP == nil {
			return nil, fmt.Errorf("registry gcp config is required")
		}
		return &gcpProvider{cfg: *cfg.GCP, secrets: secretReader}, nil
	case "azure":
		if cfg.Azure == nil {
			return nil, fmt.Errorf("registry azure config is required")
		}
		return &azureProvider{cfg: *cfg.Azure, secrets: secretReader}, nil
	case "aliyun":
		if cfg.Aliyun == nil {
			return nil, fmt.Errorf("registry aliyun config is required")
		}
		return &aliyunProvider{cfg: *cfg.Aliyun, secrets: secretReader}, nil
	case "harbor":
		if cfg.Harbor == nil {
			return nil, fmt.Errorf("registry harbor config is required")
		}
		return &harborProvider{cfg: *cfg.Harbor, secrets: secretReader}, nil
	case "builtin":
		if cfg.Builtin == nil {
			return nil, fmt.Errorf("registry builtin config is required")
		}
		return &builtinProvider{
			cfg:      *cfg.Builtin,
			registry: normalizeRegistryHost(cfg.Registry),
			secrets:  secretReader,
		}, nil
	default:
		return nil, fmt.Errorf("unsupported registry provider: %s", provider)
	}
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

// builtinProvider provides credentials for the builtin registry.
type builtinProvider struct {
	cfg      config.RegistryBuiltinConfig
	registry string
	secrets  secretReader
}

func (p *builtinProvider) GetPushCredentials(ctx context.Context, teamID string) (*Credential, error) {
	username, err := p.secrets.read(ctx, p.cfg.AuthSecretName, p.cfg.UsernameKey)
	if err != nil {
		return nil, fmt.Errorf("read username: %w", err)
	}
	password, err := p.secrets.read(ctx, p.cfg.AuthSecretName, p.cfg.PasswordKey)
	if err != nil {
		return nil, fmt.Errorf("read password: %w", err)
	}
	// Builtin registry credentials don't expire, set a far future expiration
	return &Credential{
		Provider:  "builtin",
		Registry:  p.registry,
		Username:  username,
		Password:  password,
		ExpiresAt: time.Now().Add(365 * 24 * time.Hour),
	}, nil
}

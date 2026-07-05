package volume

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	obsmetrics "github.com/sandbox0-ai/sandbox0/pkg/observability/metrics"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/objectstore"
)

const (
	S3ProviderAWS = "aws"
	S3ProviderAli = "ali"
	S3ProviderR2  = "r2"
)

type S3BackendConfig struct {
	Provider     string `json:"provider,omitempty"`
	Bucket       string `json:"bucket,omitempty"`
	Prefix       string `json:"prefix,omitempty"`
	Region       string `json:"region,omitempty"`
	EndpointURL  string `json:"endpoint_url,omitempty"`
	AccessKey    string `json:"access_key,omitempty"`
	SecretKey    string `json:"secret_key,omitempty"`
	SessionToken string `json:"session_token,omitempty"`
}

func NormalizeBackend(value string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "", BackendS0FS:
		return BackendS0FS
	case BackendS3:
		return BackendS3
	default:
		return strings.ToLower(strings.TrimSpace(value))
	}
}

func IsValidBackend(value string) bool {
	switch NormalizeBackend(value) {
	case BackendS0FS, BackendS3:
		return true
	default:
		return false
	}
}

func NormalizeS3Provider(value string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "", "s3", S3ProviderAWS:
		return S3ProviderAWS
	case "aliyun", "alicloud", "oss", S3ProviderAli:
		return S3ProviderAli
	case "cloudflare", "cloudflare-r2", S3ProviderR2:
		return S3ProviderR2
	default:
		return strings.ToLower(strings.TrimSpace(value))
	}
}

func NormalizeS3BackendConfig(cfg S3BackendConfig) S3BackendConfig {
	cfg.Provider = NormalizeS3Provider(cfg.Provider)
	cfg.Bucket = strings.TrimSpace(cfg.Bucket)
	cfg.Prefix = strings.Trim(strings.TrimSpace(cfg.Prefix), "/")
	cfg.Region = strings.TrimSpace(cfg.Region)
	cfg.EndpointURL = strings.TrimRight(strings.TrimSpace(cfg.EndpointURL), "/")
	cfg.AccessKey = strings.TrimSpace(cfg.AccessKey)
	cfg.SecretKey = strings.TrimSpace(cfg.SecretKey)
	cfg.SessionToken = strings.TrimSpace(cfg.SessionToken)
	return cfg
}

func ValidateS3BackendConfig(cfg S3BackendConfig) error {
	cfg = NormalizeS3BackendConfig(cfg)
	if cfg.Bucket == "" {
		return fmt.Errorf("s3.bucket is required")
	}
	switch cfg.Provider {
	case S3ProviderAWS, S3ProviderAli, S3ProviderR2:
	default:
		return fmt.Errorf("unsupported s3.provider %q", cfg.Provider)
	}
	if cfg.Provider == S3ProviderAli && cfg.EndpointURL == "" {
		return fmt.Errorf("s3.endpoint_url is required for provider ali")
	}
	if cfg.Provider == S3ProviderR2 && cfg.EndpointURL == "" {
		return fmt.Errorf("s3.endpoint_url is required for provider r2")
	}
	if (cfg.AccessKey == "") != (cfg.SecretKey == "") {
		return fmt.Errorf("s3.access_key and s3.secret_key must be set together")
	}
	return nil
}

func MarshalS3BackendConfig(cfg S3BackendConfig) (json.RawMessage, error) {
	cfg = NormalizeS3BackendConfig(cfg)
	raw, err := json.Marshal(cfg)
	if err != nil {
		return nil, err
	}
	return json.RawMessage(raw), nil
}

func DecodeS3BackendConfig(raw json.RawMessage) (S3BackendConfig, error) {
	if len(raw) == 0 {
		return S3BackendConfig{}, fmt.Errorf("s3 backend config is required")
	}
	var cfg S3BackendConfig
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return S3BackendConfig{}, fmt.Errorf("decode s3 backend config: %w", err)
	}
	cfg = NormalizeS3BackendConfig(cfg)
	if err := ValidateS3BackendConfig(cfg); err != nil {
		return S3BackendConfig{}, err
	}
	return cfg, nil
}

func SanitizeS3BackendConfig(cfg S3BackendConfig) S3BackendConfig {
	cfg = NormalizeS3BackendConfig(cfg)
	cfg.AccessKey = ""
	cfg.SecretKey = ""
	cfg.SessionToken = ""
	return cfg
}

func S3ObjectStoreConfig(cfg S3BackendConfig, fallback *config.StorageProxyConfig, metrics *obsmetrics.StorageProxyMetrics) objectstore.Config {
	cfg = NormalizeS3BackendConfig(cfg)
	storeType := objectstore.TypeS3
	if cfg.Provider == S3ProviderAli {
		storeType = objectstore.TypeOSS
	}
	out := objectstore.Config{
		Type:         storeType,
		Bucket:       cfg.Bucket,
		Region:       cfg.Region,
		Endpoint:     cfg.EndpointURL,
		AccessKey:    cfg.AccessKey,
		SecretKey:    cfg.SecretKey,
		SessionToken: cfg.SessionToken,
		Metrics:      metrics,
	}
	if fallback != nil {
		if out.Region == "" {
			out.Region = fallback.S3Region
		}
		if out.Endpoint == "" {
			out.Endpoint = fallback.S3Endpoint
		}
		if out.AccessKey == "" && out.SecretKey == "" && out.SessionToken == "" {
			out.AccessKey = fallback.S3AccessKey
			out.SecretKey = fallback.S3SecretKey
			out.SessionToken = fallback.S3SessionToken
		}
	}
	if cfg.Provider == S3ProviderR2 && out.Region == "" {
		out.Region = "auto"
	}
	return out
}

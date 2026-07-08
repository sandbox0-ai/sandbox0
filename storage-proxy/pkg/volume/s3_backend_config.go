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
	Provider             string                         `json:"provider,omitempty"`
	Bucket               string                         `json:"bucket,omitempty"`
	Prefix               string                         `json:"prefix,omitempty"`
	Region               string                         `json:"region,omitempty"`
	EndpointURL          string                         `json:"endpoint_url,omitempty"`
	AccessKey            string                         `json:"access_key,omitempty"`
	SecretKey            string                         `json:"secret_key,omitempty"`
	SessionToken         string                         `json:"session_token,omitempty"`
	EncryptedCredentials *S3BackendEncryptedCredentials `json:"encrypted_credentials,omitempty"`
}

type S3BackendEncryptedCredentials struct {
	Version    int    `json:"version"`
	Algorithm  string `json:"algorithm"`
	KeyID      string `json:"key_id"`
	Nonce      string `json:"nonce"`
	Ciphertext string `json:"ciphertext"`
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
	if cfg.EncryptedCredentials != nil {
		cfg.EncryptedCredentials.KeyID = strings.TrimSpace(cfg.EncryptedCredentials.KeyID)
		cfg.EncryptedCredentials.Algorithm = strings.TrimSpace(cfg.EncryptedCredentials.Algorithm)
		cfg.EncryptedCredentials.Nonce = strings.TrimSpace(cfg.EncryptedCredentials.Nonce)
		cfg.EncryptedCredentials.Ciphertext = strings.TrimSpace(cfg.EncryptedCredentials.Ciphertext)
	}
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
	if cfg.Provider == S3ProviderAWS && cfg.Region == "" && cfg.EndpointURL == "" {
		return fmt.Errorf("s3.region or s3.endpoint_url is required for provider aws")
	}
	if cfg.AccessKey == "" && cfg.SecretKey == "" && !hasS3BackendEncryptedCredentials(cfg) {
		return fmt.Errorf("s3.access_key and s3.secret_key are required")
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
	return decodeS3BackendConfig(raw, true)
}

func decodeS3BackendConfig(raw json.RawMessage, validate bool) (S3BackendConfig, error) {
	if len(raw) == 0 {
		return S3BackendConfig{}, fmt.Errorf("s3 backend config is required")
	}
	var cfg S3BackendConfig
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return S3BackendConfig{}, fmt.Errorf("decode s3 backend config: %w", err)
	}
	cfg = NormalizeS3BackendConfig(cfg)
	if validate {
		if err := ValidateS3BackendConfig(cfg); err != nil {
			return S3BackendConfig{}, err
		}
	}
	return cfg, nil
}

func SanitizeS3BackendConfig(cfg S3BackendConfig) S3BackendConfig {
	cfg = NormalizeS3BackendConfig(cfg)
	cfg.AccessKey = ""
	cfg.SecretKey = ""
	cfg.SessionToken = ""
	cfg.EncryptedCredentials = nil
	return cfg
}

func hasS3BackendEncryptedCredentials(cfg S3BackendConfig) bool {
	return cfg.EncryptedCredentials != nil &&
		cfg.EncryptedCredentials.Version != 0 &&
		cfg.EncryptedCredentials.Algorithm != "" &&
		cfg.EncryptedCredentials.KeyID != "" &&
		cfg.EncryptedCredentials.Nonce != "" &&
		cfg.EncryptedCredentials.Ciphertext != ""
}

func S3ObjectStoreConfig(cfg S3BackendConfig, _ *config.StorageProxyConfig, metrics *obsmetrics.StorageProxyMetrics) objectstore.Config {
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
	if cfg.Provider == S3ProviderR2 && out.Region == "" {
		out.Region = "auto"
	}
	return out
}

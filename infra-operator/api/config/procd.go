// +kubebuilder:object:generate=true
package config

import (
	"fmt"
	"os"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	"gopkg.in/yaml.v3"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ProcdConfig holds all configuration for procd.
type ProcdConfig struct {
	// Server configuration
	// +optional
	// +kubebuilder:default=49983
	HTTPPort int `yaml:"http_port" json:"httpPort"`
	// +optional
	// +kubebuilder:default="info"
	LogLevel string `yaml:"log_level" json:"logLevel"`

	// Storage Proxy configuration
	StorageProxyBaseURL string `yaml:"storage_proxy_base_url" json:"-"`
	StorageProxyPort    int    `yaml:"storage_proxy_port" json:"-"`

	// JuiceFS Mount Defaults
	// +optional
	// +kubebuilder:default="100"
	JuiceFSCacheSize string `yaml:"juicefs_cache_size" json:"juicefsCacheSize"`
	// +optional
	// +kubebuilder:default=3
	JuiceFSPrefetch int `yaml:"juicefs_prefetch" json:"juicefsPrefetch"`
	// +optional
	// +kubebuilder:default="300"
	JuiceFSBufferSize string `yaml:"juicefs_buffer_size" json:"juicefsBufferSize"`
	// +optional
	// +kubebuilder:default=true
	JuiceFSWriteback bool `yaml:"juicefs_writeback" json:"juicefsWriteback"`

	// File manager configuration
	// +optional
	// +kubebuilder:default="/workspace"
	RootPath string `yaml:"root_path" json:"rootPath"`

	// Cache configuration
	// +optional
	// +kubebuilder:default=104857600
	CacheMaxBytes int64 `yaml:"cache_max_bytes" json:"cacheMaxBytes"`
	// +optional
	// +kubebuilder:default="30s"
	CacheTTL metav1.Duration `yaml:"cache_ttl" json:"cacheTTL"`
	// Context cleanup configuration
	// +optional
	// +kubebuilder:default="30s"
	ContextCleanupInterval metav1.Duration `yaml:"context_cleanup_interval" json:"contextCleanupInterval"`
	// +optional
	// +kubebuilder:default="0s"
	ContextIdleTimeout metav1.Duration `yaml:"context_idle_timeout" json:"contextIdleTimeout"`
	// +optional
	// +kubebuilder:default="0s"
	ContextMaxLifetime metav1.Duration `yaml:"context_max_lifetime" json:"contextMaxLifetime"`
	// +optional
	// +kubebuilder:default="0s"
	ContextFinishedTTL metav1.Duration `yaml:"context_finished_ttl" json:"contextFinishedTTL"`
	// +optional
	// +kubebuilder:default=256
	WebhookQueueSize int `yaml:"webhook_queue_size" json:"webhookQueueSize"`
	// +optional
	// +kubebuilder:default="5s"
	WebhookRequestTimeout metav1.Duration `yaml:"webhook_request_timeout" json:"webhookRequestTimeout"`
	// +optional
	// +kubebuilder:default=3
	WebhookMaxRetries int `yaml:"webhook_max_retries" json:"webhookMaxRetries"`
	// +optional
	// +kubebuilder:default="500ms"
	WebhookBaseBackoff metav1.Duration `yaml:"webhook_base_backoff" json:"webhookBaseBackoff"`

	setKeys map[string]bool `yaml:"-" json:"-"`
}

// UnmarshalYAML captures configured keys without hardcoding them.
func (c *ProcdConfig) UnmarshalYAML(value *yaml.Node) error {
	if value == nil || value.Kind == 0 {
		return nil
	}

	var raw map[string]any
	if err := value.Decode(&raw); err != nil {
		return err
	}

	setKeys := make(map[string]bool)
	for k := range raw {
		setKeys[k] = true
	}

	type alias ProcdConfig
	decoded := alias(*c)
	if err := value.Decode(&decoded); err != nil {
		return err
	}

	*c = ProcdConfig(decoded)
	c.setKeys = setKeys
	return nil
}

// EnvMap returns configured keys as environment variables.
func (c ProcdConfig) EnvMap() map[string]string {
	if len(c.setKeys) == 0 {
		return nil
	}

	env := make(map[string]string, len(c.setKeys))
	value := reflect.ValueOf(c)
	typ := value.Type()
	for i := 0; i < value.NumField(); i++ {
		field := typ.Field(i)
		if field.PkgPath != "" {
			continue
		}
		tag := field.Tag.Get("yaml")
		if tag == "" || tag == "-" {
			continue
		}
		key := strings.Split(tag, ",")[0]
		if key == "" {
			continue
		}
		if _, ok := c.setKeys[key]; !ok {
			continue
		}
		env[key] = formatProcdEnvValue(value.Field(i))
	}
	return env
}

// EnvKeys returns all yaml-tagged procd environment keys.
func (c ProcdConfig) EnvKeys() []string {
	return procdEnvKeys()
}

func procdEnvKeys() []string {
	value := reflect.ValueOf(ProcdConfig{})
	typ := value.Type()
	keys := make([]string, 0, value.NumField())
	for i := 0; i < value.NumField(); i++ {
		field := typ.Field(i)
		if field.PkgPath != "" {
			continue
		}
		tag := field.Tag.Get("yaml")
		if tag == "" || tag == "-" {
			continue
		}
		key := strings.Split(tag, ",")[0]
		if key == "" {
			continue
		}
		keys = append(keys, key)
	}
	return keys
}

var (
	procdCfg     *ProcdConfig
	procdCfgOnce sync.Once
)

// LoadProcdConfig returns the procd configuration.
func LoadProcdConfig() *ProcdConfig {
	procdCfgOnce.Do(func() {
		cfg := ProcdConfig{}
		if err := applyProcdEnvOverrides(&cfg); err != nil {
			fmt.Fprintf(os.Stderr, "Failed to apply procd env overrides: %v\n", err)
		}
		procdCfg = &cfg
	})
	return procdCfg
}

// Validate checks if the configuration is valid.
func (c *ProcdConfig) Validate() error {
	// SandboxID and TemplateID can be empty during development
	return nil
}

func applyProcdEnvOverrides(cfg *ProcdConfig) error {
	value := reflect.ValueOf(cfg).Elem()
	typ := value.Type()
	for i := 0; i < value.NumField(); i++ {
		field := typ.Field(i)
		if field.PkgPath != "" {
			continue
		}
		tag := field.Tag.Get("yaml")
		if tag == "" || tag == "-" {
			continue
		}
		key := strings.Split(tag, ",")[0]
		if key == "" {
			continue
		}
		envValue, ok := os.LookupEnv(key)
		if !ok {
			continue
		}
		if err := setProcdFieldValue(value.Field(i), envValue, key); err != nil {
			return err
		}
	}
	return nil
}

func setProcdFieldValue(field reflect.Value, value string, key string) error {
	if !field.CanSet() {
		return nil
	}

	if field.Type() == reflect.TypeOf(metav1.Duration{}) {
		parsed, err := time.ParseDuration(value)
		if err != nil {
			return fmt.Errorf("parse %s: %w", key, err)
		}
		field.Set(reflect.ValueOf(metav1.Duration{Duration: parsed}))
		return nil
	}

	switch field.Kind() {
	case reflect.String:
		field.SetString(value)
		return nil
	case reflect.Int:
		parsed, err := strconv.Atoi(value)
		if err != nil {
			return fmt.Errorf("parse %s: %w", key, err)
		}
		field.SetInt(int64(parsed))
		return nil
	case reflect.Int64:
		if field.Type() == reflect.TypeOf(time.Duration(0)) {
			parsed, err := time.ParseDuration(value)
			if err != nil {
				return fmt.Errorf("parse %s: %w", key, err)
			}
			field.SetInt(int64(parsed))
			return nil
		}
		parsed, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return fmt.Errorf("parse %s: %w", key, err)
		}
		field.SetInt(parsed)
		return nil
	case reflect.Bool:
		parsed, err := strconv.ParseBool(value)
		if err != nil {
			return fmt.Errorf("parse %s: %w", key, err)
		}
		field.SetBool(parsed)
		return nil
	default:
		return fmt.Errorf("unsupported field type for %s", key)
	}
}

func formatProcdEnvValue(value reflect.Value) string {
	if !value.IsValid() {
		return ""
	}
	if value.Type() == reflect.TypeOf(metav1.Duration{}) {
		duration := value.Interface().(metav1.Duration)
		return duration.Duration.String()
	}

	switch value.Kind() {
	case reflect.String:
		return value.String()
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		if value.Type() == reflect.TypeOf(time.Duration(0)) {
			return time.Duration(value.Int()).String()
		}
		return strconv.FormatInt(value.Int(), 10)
	case reflect.Bool:
		return strconv.FormatBool(value.Bool())
	default:
		return fmt.Sprint(value.Interface())
	}
}

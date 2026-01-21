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
	// +optional
	NodeName string `yaml:"node_name" json:"-"`

	// Server configuration
	// +optional
	// +kubebuilder:default=49983
	HTTPPort int `yaml:"http_port" json:"httpPort"`
	// +optional
	// +kubebuilder:default="info"
	LogLevel string `yaml:"log_level" json:"logLevel"`

	// Storage Proxy configuration
	// +optional
	// +kubebuilder:default="storage-proxy"
	StorageProxyBaseURL string `yaml:"storage_proxy_base_url" json:"-"`
	// +optional
	// +kubebuilder:default=8080
	StorageProxyPort int `yaml:"storage_proxy_port" json:"-"`
	// +optional
	// +kubebuilder:default=3
	StorageProxyReplicas int `yaml:"storage_proxy_replicas" json:"storageProxyReplicas"`

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

	setKeys map[string]string `yaml:"-" json:"-"`
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

	setKeys := make(map[string]string)
	for k, v := range raw {
		setKeys[k] = fmt.Sprint(v)
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
	return c.setKeys
}

var (
	procdCfg     *ProcdConfig
	procdCfgOnce sync.Once
)

// LoadProcdConfig returns the procd configuration.
func LoadProcdConfig() *ProcdConfig {
	procdCfgOnce.Do(func() {
		procdCfg = loadProcdConfig()
	})
	return procdCfg
}

// Validate checks if the configuration is valid.
func (c *ProcdConfig) Validate() error {
	// SandboxID and TemplateID can be empty during development
	return nil
}

func loadProcdConfig() *ProcdConfig {
	cfg := ProcdConfig{}
	path := os.Getenv("CONFIG_PATH")
	if path == "" {
		path = "/config/config.yaml"
	}

	if path != "" {
		data, err := os.ReadFile(path)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to read procd config from %s: %v, using empty config\n", path, err)
		} else {
			data = []byte(os.ExpandEnv(string(data)))
			if err := yaml.Unmarshal(data, &cfg); err != nil {
				fmt.Fprintf(os.Stderr, "Failed to unmarshal procd config from %s: %v, using empty config\n", path, err)
			}
		}
	}

	if err := applyProcdEnvOverrides(&cfg); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to apply procd env overrides: %v\n", err)
	}

	return &cfg
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
	default:
		return fmt.Errorf("unsupported field type for %s", key)
	}
}

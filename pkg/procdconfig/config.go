// Package procdconfig defines the lightweight runtime configuration shared by
// procd and the components that inject its environment variables.
package procdconfig

import (
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	"gopkg.in/yaml.v3"
)

const (
	DefaultHTTPPort               = 49983
	DefaultLogLevel               = "info"
	DefaultRootPath               = "/workspace"
	DefaultContextCleanupInterval = 30 * time.Second
	DefaultWebhookQueueSize       = 256
	DefaultWebhookRequestTimeout  = 5 * time.Second
	DefaultWebhookMaxRetries      = 3
	DefaultWebhookBaseBackoff     = 500 * time.Millisecond
	DefaultWebhookOutboxDir       = "/var/lib/sandbox0/procd/webhook-outbox"
	DefaultSessionStateDir        = "/var/lib/sandbox0/procd/sessions"
)

// Duration wraps time.Duration with string-based JSON and YAML encoding.
type Duration struct {
	time.Duration
}

// UnmarshalJSON decodes a duration string.
func (d *Duration) UnmarshalJSON(data []byte) error {
	var value string
	if err := json.Unmarshal(data, &value); err != nil {
		return err
	}
	parsed, err := time.ParseDuration(value)
	if err != nil {
		return err
	}
	d.Duration = parsed
	return nil
}

// MarshalJSON encodes the duration as a string.
func (d Duration) MarshalJSON() ([]byte, error) {
	return json.Marshal(d.String())
}

// UnmarshalYAML decodes a duration string.
func (d *Duration) UnmarshalYAML(value *yaml.Node) error {
	var text string
	if err := value.Decode(&text); err != nil {
		return err
	}
	parsed, err := time.ParseDuration(text)
	if err != nil {
		return err
	}
	d.Duration = parsed
	return nil
}

// MarshalYAML encodes the duration as a string.
func (d Duration) MarshalYAML() (any, error) {
	return d.String(), nil
}

// Config holds all runtime configuration for procd.
type Config struct {
	HTTPPort int    `yaml:"http_port" json:"httpPort"`
	LogLevel string `yaml:"log_level" json:"logLevel"`

	RootPath string `yaml:"root_path" json:"rootPath"`

	ContextCleanupInterval Duration `yaml:"context_cleanup_interval" json:"contextCleanupInterval"`
	ContextIdleTimeout     Duration `yaml:"context_idle_timeout" json:"contextIdleTimeout"`
	ContextMaxLifetime     Duration `yaml:"context_max_lifetime" json:"contextMaxLifetime"`
	ContextFinishedTTL     Duration `yaml:"context_finished_ttl" json:"contextFinishedTTL"`
	WebhookQueueSize       int      `yaml:"webhook_queue_size" json:"webhookQueueSize"`
	WebhookRequestTimeout  Duration `yaml:"webhook_request_timeout" json:"webhookRequestTimeout"`
	WebhookMaxRetries      int      `yaml:"webhook_max_retries" json:"webhookMaxRetries"`
	WebhookBaseBackoff     Duration `yaml:"webhook_base_backoff" json:"webhookBaseBackoff"`
	WebhookOutboxDir       string   `yaml:"webhook_outbox_dir" json:"webhookOutboxDir"`
	SessionStateDir        string   `yaml:"session_state_dir" json:"sessionStateDir"`

	setKeys map[string]bool
}

// MarshalYAML preserves the fields when Config is embedded inline by manager.
func (c Config) MarshalYAML() (any, error) {
	type plain Config
	return plain(c), nil
}

// UnmarshalYAML captures configured keys without hardcoding them.
func (c *Config) UnmarshalYAML(value *yaml.Node) error {
	if value == nil || value.Kind == 0 {
		return nil
	}

	var raw map[string]any
	if err := value.Decode(&raw); err != nil {
		return err
	}
	setKeys := make(map[string]bool, len(raw))
	for key := range raw {
		setKeys[key] = true
	}

	type alias Config
	decoded := alias(*c)
	if err := value.Decode(&decoded); err != nil {
		return err
	}
	*c = Config(decoded)
	c.setKeys = setKeys
	return nil
}

// EnvMap returns explicitly configured keys as environment variables.
func (c Config) EnvMap() map[string]string {
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
		if !c.setKeys[key] {
			continue
		}
		env[key] = formatEnvValue(value.Field(i))
	}
	return env
}

var (
	loadedConfig *Config
	loadOnce     sync.Once
)

// Load returns the process-wide procd configuration.
func Load() *Config {
	loadOnce.Do(func() {
		cfg := Config{
			WebhookOutboxDir: DefaultWebhookOutboxDir,
			SessionStateDir:  DefaultSessionStateDir,
		}
		if err := applyEnvOverrides(&cfg); err != nil {
			fmt.Fprintf(os.Stderr, "Failed to apply procd env overrides: %v\n", err)
		}
		cfg.ApplyDefaults()
		loadedConfig = &cfg
	})
	return loadedConfig
}

// ApplyDefaults fills optional configuration values.
func (c *Config) ApplyDefaults() {
	if c.HTTPPort <= 0 {
		c.HTTPPort = DefaultHTTPPort
	}
	if strings.TrimSpace(c.LogLevel) == "" {
		c.LogLevel = DefaultLogLevel
	}
	if strings.TrimSpace(c.RootPath) == "" {
		c.RootPath = DefaultRootPath
	}
	if c.ContextCleanupInterval.Duration <= 0 {
		c.ContextCleanupInterval = Duration{Duration: DefaultContextCleanupInterval}
	}
	if c.WebhookQueueSize <= 0 {
		c.WebhookQueueSize = DefaultWebhookQueueSize
	}
	if c.WebhookRequestTimeout.Duration <= 0 {
		c.WebhookRequestTimeout = Duration{Duration: DefaultWebhookRequestTimeout}
	}
	if c.WebhookMaxRetries <= 0 {
		c.WebhookMaxRetries = DefaultWebhookMaxRetries
	}
	if c.WebhookBaseBackoff.Duration <= 0 {
		c.WebhookBaseBackoff = Duration{Duration: DefaultWebhookBaseBackoff}
	}
	if strings.TrimSpace(c.WebhookOutboxDir) == "" {
		c.WebhookOutboxDir = DefaultWebhookOutboxDir
	}
	if strings.TrimSpace(c.SessionStateDir) == "" {
		c.SessionStateDir = DefaultSessionStateDir
	}
}

// Validate checks if the configuration is valid.
func (c *Config) Validate() error {
	return nil
}

// DeepCopyInto copies the receiver into out.
func (c *Config) DeepCopyInto(out *Config) {
	*out = *c
	if c.setKeys != nil {
		out.setKeys = make(map[string]bool, len(c.setKeys))
		for key, value := range c.setKeys {
			out.setKeys[key] = value
		}
	}
}

// DeepCopy returns an independent copy of the configuration.
func (c *Config) DeepCopy() *Config {
	if c == nil {
		return nil
	}
	out := new(Config)
	c.DeepCopyInto(out)
	return out
}

func applyEnvOverrides(cfg *Config) error {
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
		envValue, ok := os.LookupEnv(key)
		if !ok {
			continue
		}
		if err := setFieldValue(value.Field(i), envValue, key); err != nil {
			return err
		}
	}
	return nil
}

func setFieldValue(field reflect.Value, value string, key string) error {
	if !field.CanSet() {
		return nil
	}
	if field.Type() == reflect.TypeOf(Duration{}) {
		parsed, err := time.ParseDuration(value)
		if err != nil {
			return fmt.Errorf("parse %s: %w", key, err)
		}
		field.Set(reflect.ValueOf(Duration{Duration: parsed}))
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

func formatEnvValue(value reflect.Value) string {
	if value.Type() == reflect.TypeOf(Duration{}) {
		return value.Interface().(Duration).String()
	}
	switch value.Kind() {
	case reflect.String:
		return value.String()
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return strconv.FormatInt(value.Int(), 10)
	case reflect.Bool:
		return strconv.FormatBool(value.Bool())
	default:
		return fmt.Sprint(value.Interface())
	}
}

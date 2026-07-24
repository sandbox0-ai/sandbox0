package clickhouse

import (
	"fmt"
	"regexp"
	"time"
)

const (
	LegacyEventsTable                  = "sandbox_events"
	DefaultDatabase                    = "sandbox0_observability"
	DefaultEventsTable                 = "sandbox_audit_events"
	DefaultLogsTable                   = "sandbox_logs"
	DefaultRuntimeSamplesTable         = "sandbox_runtime_samples"
	DefaultRetentionDays               = 90
	DefaultLogsRetentionDays           = 7
	DefaultRuntimeSamplesRetentionDays = 30
	DefaultRuntimeQueryConcurrency     = 4
	DefaultRuntimeQueryTimeout         = time.Minute
	MaxRuntimeQueryConcurrency         = 16
	DefaultQueryLimit                  = 100
	MaxQueryLimit                      = 1000
)

var identifierPattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

type Config struct {
	Database                    string
	EventsTable                 string
	EventsStoragePolicy         string
	LogsTable                   string
	RuntimeSamplesTable         string
	RetentionDays               int
	LogsRetentionDays           int
	RuntimeSamplesRetentionDays int
	RuntimeQueryConcurrency     int
	RuntimeQueryTimeout         time.Duration
}

func normalizeConfig(cfg Config) (Config, error) {
	if cfg.Database == "" {
		cfg.Database = DefaultDatabase
	}
	if cfg.EventsTable == "" {
		cfg.EventsTable = DefaultEventsTable
	}
	if cfg.LogsTable == "" {
		cfg.LogsTable = DefaultLogsTable
	}
	if cfg.RuntimeSamplesTable == "" {
		cfg.RuntimeSamplesTable = DefaultRuntimeSamplesTable
	}
	if cfg.RetentionDays == 0 {
		cfg.RetentionDays = DefaultRetentionDays
	}
	if cfg.LogsRetentionDays == 0 {
		cfg.LogsRetentionDays = DefaultLogsRetentionDays
	}
	if cfg.RuntimeSamplesRetentionDays == 0 {
		cfg.RuntimeSamplesRetentionDays = DefaultRuntimeSamplesRetentionDays
	}
	if cfg.RuntimeQueryConcurrency == 0 {
		cfg.RuntimeQueryConcurrency = DefaultRuntimeQueryConcurrency
	}
	if cfg.RuntimeQueryTimeout == 0 {
		cfg.RuntimeQueryTimeout = DefaultRuntimeQueryTimeout
	}
	if cfg.RetentionDays < 0 {
		return Config{}, fmt.Errorf("retention_days must be non-negative")
	}
	if cfg.LogsRetentionDays < 0 {
		return Config{}, fmt.Errorf("logs_retention_days must be non-negative")
	}
	if cfg.RuntimeSamplesRetentionDays < 0 {
		return Config{}, fmt.Errorf("runtime_samples_retention_days must be non-negative")
	}
	if cfg.RuntimeQueryConcurrency < 0 {
		return Config{}, fmt.Errorf("runtime_query_concurrency must be non-negative")
	}
	if cfg.RuntimeQueryConcurrency > MaxRuntimeQueryConcurrency {
		return Config{}, fmt.Errorf("runtime_query_concurrency cannot exceed %d", MaxRuntimeQueryConcurrency)
	}
	if cfg.RuntimeQueryTimeout < 0 {
		return Config{}, fmt.Errorf("runtime_query_timeout must be non-negative")
	}
	if err := validateIdentifier("database", cfg.Database); err != nil {
		return Config{}, err
	}
	if err := validateIdentifier("events_table", cfg.EventsTable); err != nil {
		return Config{}, err
	}
	if cfg.EventsStoragePolicy != "" {
		if err := validateIdentifier("events_storage_policy", cfg.EventsStoragePolicy); err != nil {
			return Config{}, err
		}
	}
	if err := validateIdentifier("logs_table", cfg.LogsTable); err != nil {
		return Config{}, err
	}
	if err := validateIdentifier("runtime_samples_table", cfg.RuntimeSamplesTable); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func validateIdentifier(name, value string) error {
	if !identifierPattern.MatchString(value) {
		return fmt.Errorf("%s must be a ClickHouse identifier", name)
	}
	return nil
}

func quoteIdentifier(value string) string {
	return "`" + value + "`"
}

func qualifiedEventsTable(cfg Config) string {
	return quoteIdentifier(cfg.Database) + "." + quoteIdentifier(cfg.EventsTable)
}

func qualifiedLogsTable(cfg Config) string {
	return quoteIdentifier(cfg.Database) + "." + quoteIdentifier(cfg.LogsTable)
}

func qualifiedRuntimeSamplesTable(cfg Config) string {
	return quoteIdentifier(cfg.Database) + "." + quoteIdentifier(cfg.RuntimeSamplesTable)
}

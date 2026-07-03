package clickhouse

import (
	"fmt"
	"regexp"
)

const (
	DefaultDatabase             = "sandbox0_observability"
	DefaultEventsTable          = "sandbox_events"
	DefaultLogsTable            = "sandbox_logs"
	DefaultMetricsTable         = "sandbox_metric_samples"
	DefaultRetentionDays        = 90
	DefaultLogsRetentionDays    = 7
	DefaultMetricsRetentionDays = 30
	DefaultQueryLimit           = 100
	MaxQueryLimit               = 1000
)

var identifierPattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

type Config struct {
	Database             string
	EventsTable          string
	LogsTable            string
	MetricsTable         string
	RetentionDays        int
	LogsRetentionDays    int
	MetricsRetentionDays int
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
	if cfg.MetricsTable == "" {
		cfg.MetricsTable = DefaultMetricsTable
	}
	if cfg.RetentionDays == 0 {
		cfg.RetentionDays = DefaultRetentionDays
	}
	if cfg.LogsRetentionDays == 0 {
		cfg.LogsRetentionDays = DefaultLogsRetentionDays
	}
	if cfg.MetricsRetentionDays == 0 {
		cfg.MetricsRetentionDays = DefaultMetricsRetentionDays
	}
	if cfg.RetentionDays < 0 {
		return Config{}, fmt.Errorf("retention_days must be non-negative")
	}
	if cfg.LogsRetentionDays < 0 {
		return Config{}, fmt.Errorf("logs_retention_days must be non-negative")
	}
	if cfg.MetricsRetentionDays < 0 {
		return Config{}, fmt.Errorf("metrics_retention_days must be non-negative")
	}
	if err := validateIdentifier("database", cfg.Database); err != nil {
		return Config{}, err
	}
	if err := validateIdentifier("events_table", cfg.EventsTable); err != nil {
		return Config{}, err
	}
	if err := validateIdentifier("logs_table", cfg.LogsTable); err != nil {
		return Config{}, err
	}
	if err := validateIdentifier("metrics_table", cfg.MetricsTable); err != nil {
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

func qualifiedMetricsTable(cfg Config) string {
	return quoteIdentifier(cfg.Database) + "." + quoteIdentifier(cfg.MetricsTable)
}

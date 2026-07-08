package clickhouse

import (
	"fmt"
	"regexp"
)

const (
	DefaultDatabase          = "sandbox0_metering"
	DefaultEventsTable       = "usage_events"
	DefaultWindowsTable      = "usage_windows"
	DefaultWatermarksTable   = "producer_watermarks"
	DefaultSandboxStateTable = "sandbox_projection_state"
	DefaultStorageStateTable = "storage_projection_state"
)

var identifierPattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

type Config struct {
	Database          string
	EventsTable       string
	WindowsTable      string
	WatermarksTable   string
	SandboxStateTable string
	StorageStateTable string
}

func normalizeConfig(cfg Config) (Config, error) {
	if cfg.Database == "" {
		cfg.Database = DefaultDatabase
	}
	if cfg.EventsTable == "" {
		cfg.EventsTable = DefaultEventsTable
	}
	if cfg.WindowsTable == "" {
		cfg.WindowsTable = DefaultWindowsTable
	}
	if cfg.WatermarksTable == "" {
		cfg.WatermarksTable = DefaultWatermarksTable
	}
	if cfg.SandboxStateTable == "" {
		cfg.SandboxStateTable = DefaultSandboxStateTable
	}
	if cfg.StorageStateTable == "" {
		cfg.StorageStateTable = DefaultStorageStateTable
	}
	for name, value := range map[string]string{
		"database":            cfg.Database,
		"events_table":        cfg.EventsTable,
		"windows_table":       cfg.WindowsTable,
		"watermarks_table":    cfg.WatermarksTable,
		"sandbox_state_table": cfg.SandboxStateTable,
		"storage_state_table": cfg.StorageStateTable,
	} {
		if !identifierPattern.MatchString(value) {
			return Config{}, fmt.Errorf("%s must be a ClickHouse identifier", name)
		}
	}
	return cfg, nil
}

func quoteIdentifier(value string) string {
	return "`" + value + "`"
}

func qualified(database, table string) string {
	return quoteIdentifier(database) + "." + quoteIdentifier(table)
}

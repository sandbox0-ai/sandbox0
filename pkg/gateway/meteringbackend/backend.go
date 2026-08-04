// Package meteringbackend initializes the shared gateway metering read model.
package meteringbackend

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	meteringclickhouse "github.com/sandbox0-ai/sandbox0/pkg/metering/clickhouse"
	"go.uber.org/zap"
)

// Open initializes the optional ClickHouse metering read model used by region
// and cluster gateway deployments.
func Open(ctx context.Context, cfg *config.MeteringConfig, logger *zap.Logger) (*sql.DB, *meteringclickhouse.Repository, error) {
	if cfg == nil || !cfg.Enabled {
		return nil, nil, nil
	}

	ch := cfg.ClickHouse
	timeout := connectTimeout(ch)
	connectCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	db, repo, err := meteringclickhouse.Open(connectCtx, openConfig(ch))
	if err != nil {
		return nil, nil, fmt.Errorf("initialize clickhouse metering backend: %w", err)
	}
	logger.Info("Metering ClickHouse backend initialized",
		zap.String("database", ch.Database),
		zap.String("events_table", ch.EventsTable),
		zap.String("windows_table", ch.WindowsTable),
		zap.Bool("schema_migration", !ch.SkipSchemaMigration),
	)
	return db, repo, nil
}

func connectTimeout(cfg config.MeteringClickHouseConfig) time.Duration {
	timeout := cfg.ConnectTimeout.Duration
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	return timeout
}

func openConfig(cfg config.MeteringClickHouseConfig) meteringclickhouse.OpenConfig {
	return meteringclickhouse.OpenConfig{
		DSN: strings.TrimSpace(cfg.DSN),
		Schema: meteringclickhouse.Config{
			Database:          cfg.Database,
			EventsTable:       cfg.EventsTable,
			WindowsTable:      cfg.WindowsTable,
			WatermarksTable:   cfg.WatermarksTable,
			SandboxStateTable: cfg.SandboxStateTable,
			StorageStateTable: cfg.StorageStateTable,
		},
		Migrate: !cfg.SkipSchemaMigration,
	}
}

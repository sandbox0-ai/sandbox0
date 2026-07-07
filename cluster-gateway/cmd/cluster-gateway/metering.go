package main

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

func initMetering(ctx context.Context, cfg *config.ClusterGatewayConfig, logger *zap.Logger) (*sql.DB, *meteringclickhouse.Repository, error) {
	if cfg == nil || !cfg.Metering.Enabled {
		return nil, nil, nil
	}
	ch := cfg.Metering.ClickHouse
	timeout := ch.ConnectTimeout.Duration
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	connectCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	db, repo, err := meteringclickhouse.Open(connectCtx, meteringclickhouse.OpenConfig{
		DSN: strings.TrimSpace(ch.DSN),
		Schema: meteringclickhouse.Config{
			Database:          ch.Database,
			EventsTable:       ch.EventsTable,
			WindowsTable:      ch.WindowsTable,
			WatermarksTable:   ch.WatermarksTable,
			SandboxStateTable: ch.SandboxStateTable,
			StorageStateTable: ch.StorageStateTable,
		},
		Migrate: !ch.SkipSchemaMigration,
	})
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

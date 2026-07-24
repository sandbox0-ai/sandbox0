package main

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	obsclickhouse "github.com/sandbox0-ai/sandbox0/pkg/sandboxobservability/clickhouse"
	"go.uber.org/zap"
)

func initSandboxObservability(ctx context.Context, cfg *config.ClusterGatewayConfig, logger *zap.Logger) (*sql.DB, *obsclickhouse.Repository, error) {
	if cfg == nil {
		return nil, nil, nil
	}

	sandboxObs := cfg.SandboxObservability
	switch sandboxObs.BackendType() {
	case config.SandboxObservabilityBackendDisabled:
		logger.Info("Sandbox observability backend disabled")
		return nil, nil, nil
	case config.SandboxObservabilityBackendClickHouse:
		clickHouseCfg := sandboxObs.ClickHouse
		timeout := clickHouseCfg.ConnectTimeout.Duration
		if timeout == 0 {
			timeout = 10 * time.Second
		}
		connectCtx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()

		db, repo, err := obsclickhouse.Open(connectCtx, obsclickhouse.OpenConfig{
			DSN: strings.TrimSpace(clickHouseCfg.DSN),
			Schema: obsclickhouse.Config{
				Database:                    clickHouseCfg.Database,
				EventsTable:                 clickHouseCfg.EventsTable,
				EventsStoragePolicy:         clickHouseCfg.EventsStoragePolicy,
				LogsTable:                   clickHouseCfg.LogsTable,
				RuntimeSamplesTable:         clickHouseCfg.RuntimeSamplesTable,
				RetentionDays:               clickHouseCfg.RetentionDays,
				LogsRetentionDays:           clickHouseCfg.LogsRetentionDays,
				RuntimeSamplesRetentionDays: clickHouseCfg.RuntimeSamplesRetentionDays,
			},
			Migrate:            !clickHouseCfg.SkipSchemaMigration,
			RequireAuditSchema: sandboxObs.AuditEnabled,
		})
		if err != nil {
			return nil, nil, fmt.Errorf("initialize clickhouse sandbox observability backend: %w", err)
		}
		logger.Info("Sandbox observability ClickHouse backend initialized",
			zap.String("database", repo.Database()),
			zap.String("events_table", repo.EventsTable()),
			zap.String("events_storage_policy", strings.TrimSpace(clickHouseCfg.EventsStoragePolicy)),
			zap.String("logs_table", repo.LogsTable()),
			zap.String("runtime_samples_table", repo.RuntimeSamplesTable()),
			zap.Int("retention_days", repo.RetentionDays()),
			zap.Int("logs_retention_days", repo.LogsRetentionDays()),
			zap.Int("runtime_samples_retention_days", repo.RuntimeSamplesRetentionDays()),
			zap.Bool("schema_migration", !clickHouseCfg.SkipSchemaMigration),
		)
		return db, repo, nil
	default:
		return nil, nil, fmt.Errorf("unsupported sandbox observability backend %q", sandboxObs.Backend)
	}
}

package main

import (
	"fmt"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxclaimreconciler"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	"github.com/sandbox0-ai/sandbox0/pkg/config"
	"go.uber.org/zap"
)

func configureSandboxClaimReconciler(
	cfg *config.ManagerConfig,
	store *sandboxstore.PGSandboxStore,
) (*sandboxclaimreconciler.Worker, error) {
	if cfg == nil {
		return nil, fmt.Errorf("manager config is required")
	}
	if store == nil {
		return nil, fmt.Errorf("Nomad sandbox claim cleanup requires the PostgreSQL sandbox store")
	}
	worker, err := sandboxclaimreconciler.New(sandboxclaimreconciler.Config{Store: store})
	if err != nil {
		return nil, fmt.Errorf("configure sandbox claim cleanup reconciler: %w", err)
	}
	return worker, nil
}

func logSandboxClaimReconcilePass(logger *zap.Logger, result sandboxclaimreconciler.Result, err error) {
	if logger == nil {
		return
	}
	fields := []zap.Field{
		zap.Int("scanned", result.Scanned), zap.Int("fenced", result.Fenced),
		zap.Int("pending", result.Pending), zap.Int("cleaned", result.Cleaned),
		zap.Int("skipped", result.Skipped), zap.Int("failed", result.Failed),
	}
	if err != nil {
		logger.Warn("Sandbox claim reconcile pass failed", append(fields, zap.Error(err))...)
		return
	}
	if result.Cleaned > 0 {
		logger.Info("Sandbox claim reconcile pass completed", fields...)
	}
}

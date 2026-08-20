package main

import (
	"fmt"

	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/rootfsmaterializer"
	"github.com/sandbox0-ai/sandbox0/pkg/objectstore"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsblock"
	"go.uber.org/zap"
)

func buildRootFSCompositeMaterializer(
	cfg *config.ManagerConfig,
	store rootfsmaterializer.GenerationStore,
	objects objectstore.Store,
) (*rootfsmaterializer.Worker, error) {
	if cfg == nil {
		return nil, fmt.Errorf("manager config is required")
	}
	if cfg.RootFSMaintenance.MaterializerDisabled || objects == nil {
		return nil, nil
	}
	conditional, ok := objects.(objectstore.ConditionalStore)
	if !ok || !objectstore.SupportsConditionalCreate(objects) {
		return nil, fmt.Errorf("RootFS object store must support conditional create for composite materialization")
	}
	worker, err := rootfsmaterializer.New(rootfsmaterializer.Config{
		Store: store, Source: conditional,
		Publisher: rootfsblock.ObjectStorePublisher{Store: conditional},
		ScanLimit: cfg.RootFSMaintenance.MaterializerScanLimit,
		Interval:  cfg.RootFSMaintenance.MaterializerInterval.Duration,
	})
	if err != nil {
		return nil, fmt.Errorf("create RootFS composite materializer: %w", err)
	}
	return worker, nil
}

func configureRootFSCompositeMaterializer(
	cfg *config.ManagerConfig,
	store rootfsmaterializer.GenerationStore,
	objects objectstore.Store,
) (*rootfsmaterializer.Worker, error) {
	worker, err := buildRootFSCompositeMaterializer(cfg, store, objects)
	if err != nil {
		return nil, err
	}
	if cfg != nil && cfg.SandboxRuntimeBackend == config.SandboxRuntimeBackendNomad && worker == nil {
		return nil, fmt.Errorf("Nomad sandbox runtime requires the RootFS composite materializer and conditional object storage")
	}
	return worker, nil
}

func logRootFSCompositeMaterializerPass(logger *zap.Logger, result rootfsmaterializer.Result, err error) {
	if logger == nil {
		return
	}
	fields := []zap.Field{
		zap.Int("scanned", result.Scanned), zap.Int("materialized", result.Materialized), zap.Int("failed", result.Failed),
	}
	if err != nil {
		logger.Warn("Rootfs composite materializer pass failed", append(fields, zap.Error(err))...)
		return
	}
	if result.Materialized > 0 {
		logger.Info("Rootfs composite materializer pass completed", fields...)
	}
}

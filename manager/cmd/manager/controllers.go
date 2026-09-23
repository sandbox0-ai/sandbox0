package main

import (
	"context"
	"errors"

	obsmetrics "github.com/sandbox0-ai/sandbox0/manager/pkg/metrics"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/nomadclaim"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/nomadmigration"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/rootfsmaintenance"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/service"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/templatebuild"
	"github.com/sandbox0-ai/sandbox0/pkg/clock"
	"github.com/sandbox0-ai/sandbox0/pkg/config"
	meteringoutbox "github.com/sandbox0-ai/sandbox0/pkg/metering/outbox"
	"github.com/sandbox0-ai/sandbox0/pkg/naming"
	"github.com/sandbox0-ai/sandbox0/pkg/objectstore"
	"github.com/sandbox0-ai/sandbox0/pkg/runtimecheckpoint"
	"go.uber.org/zap"
)

// managerControllerSet groups active-active background components. Their
// distinct triggers and invariants remain separate; this type only owns their
// common process lifecycle.
type managerControllerSet struct {
	cfg                              *config.ManagerConfig
	clock                            *clock.Clock
	logger                           *zap.Logger
	memoryResumeWorker               *nomadclaim.CheckpointResumeWorker
	memoryRestoreCancellationWorker  *nomadclaim.CheckpointRestoreCancellationWorker
	sandboxPauseController           *service.SandboxPauseController
	sandboxTTLController             *service.SandboxTTLController
	sandboxRootFSController          *service.SandboxRootFSController
	sandboxNetworkMutationController *service.SandboxNetworkMutationController
	templateBuildWorker              *templatebuild.TemplateBuildWorker
	sandboxStore                     *sandboxstore.PGSandboxStore
	rootFSObjectStore                objectstore.Store
	rootFSObjectStoreErr             error
	meteringRepo                     *meteringoutbox.Repository
	managerMetrics                   *obsmetrics.ManagerMetrics
}

func (s *managerControllerSet) Start(ctx context.Context) {
	if s.memoryResumeWorker != nil {
		go logControllerError(ctx, s.logger, "Memory resume recovery stopped", func() error {
			return s.memoryResumeWorker.Run(ctx)
		})
	}
	if s.memoryRestoreCancellationWorker != nil {
		go logControllerError(ctx, s.logger, "Memory restore cancellation stopped", func() error {
			return s.memoryRestoreCancellationWorker.Run(ctx)
		})
	}
	if s.templateBuildWorker != nil {
		go logControllerError(ctx, s.logger, "Template RootFS build worker stopped", func() error {
			return s.templateBuildWorker.Run(ctx)
		})
		s.logger.Info("Template RootFS build worker started",
			zap.String("clusterID", naming.ClusterIDOrDefault(&s.cfg.DefaultClusterId)),
		)
	}

	if s.sandboxTTLController != nil {
		go logControllerErrorExact(ctx, s.logger, "Sandbox TTL controller failed", func() error {
			return s.sandboxTTLController.Run(ctx)
		})
	}
	go logControllerErrorExact(ctx, s.logger, "Sandbox pause controller failed", func() error {
		return s.sandboxPauseController.Run(ctx, 2)
	})
	if s.sandboxRootFSController != nil {
		go logControllerErrorExact(ctx, s.logger, "Sandbox RootFS operation controller failed", func() error {
			return s.sandboxRootFSController.Run(ctx, 2)
		})
	}
	if s.sandboxNetworkMutationController != nil {
		go logControllerErrorExact(ctx, s.logger, "Sandbox network mutation controller failed", func() error {
			return s.sandboxNetworkMutationController.Run(ctx, 2)
		})
	}

	s.startRootFSMaintenance(ctx)
	s.startMigrationImageGC(ctx)
}

func (s *managerControllerSet) startMigrationImageGC(ctx context.Context) {
	if s.rootFSObjectStore == nil || s.sandboxStore == nil {
		return
	}
	collector, err := runtimecheckpoint.NewCollector(s.rootFSObjectStore)
	if err != nil {
		s.logger.Error("Migration image GC unavailable", zap.Error(err))
		return
	}
	worker, err := nomadmigration.NewImageGC(s.sandboxStore, collector)
	if err != nil {
		s.logger.Error("Migration image GC unavailable", zap.Error(err))
		return
	}
	go logControllerError(ctx, s.logger, "Migration image GC stopped", func() error {
		return worker.Run(ctx, func(report nomadmigration.Report) {
			if report.Error != nil {
				s.logger.Warn("Migration image GC pass failed", zap.Error(report.Error))
			}
		})
	})
	checkpointWorker, err := nomadmigration.NewCheckpointImageGC(s.sandboxStore, collector)
	if err != nil {
		s.logger.Error("Checkpoint image GC unavailable", zap.Error(err))
		return
	}
	go logControllerError(ctx, s.logger, "Checkpoint image GC stopped", func() error {
		return checkpointWorker.Run(ctx, func(report nomadmigration.Report) {
			if report.Error != nil {
				s.logger.Warn("Checkpoint image GC pass failed", zap.Error(report.Error))
			}
		})
	})
	captureWorker, err := nomadmigration.NewCaptureUploadGC(s.sandboxStore, collector)
	if err != nil {
		s.logger.Error("Capture upload GC unavailable", zap.Error(err))
		return
	}
	go logControllerError(ctx, s.logger, "Capture upload GC stopped", func() error {
		return captureWorker.Run(ctx, func(report nomadmigration.Report) {
			if report.Error != nil {
				s.logger.Warn("Capture upload GC pass failed", zap.Error(report.Error))
			}
		})
	})
	checkpointCaptureWorker, err := nomadmigration.NewCheckpointCaptureUploadGC(s.sandboxStore, collector)
	if err != nil {
		s.logger.Error("Checkpoint capture upload GC unavailable", zap.Error(err))
		return
	}
	go logControllerError(ctx, s.logger, "Checkpoint capture upload GC stopped", func() error {
		return checkpointCaptureWorker.Run(ctx, func(report nomadmigration.Report) {
			if report.Error != nil {
				s.logger.Warn("Checkpoint capture upload GC pass failed", zap.Error(report.Error))
			}
		})
	})
}

func (s *managerControllerSet) startRootFSMaintenance(ctx context.Context) {
	if s.cfg.RootFSMaintenance.Disabled {
		s.logger.Info("Rootfs maintenance controller disabled by config")
		return
	}
	if s.rootFSObjectStoreErr != nil {
		s.logger.Warn("Rootfs maintenance disabled; object store is not configured", zap.Error(s.rootFSObjectStoreErr))
		return
	}
	if s.rootFSObjectStore == nil {
		s.logger.Warn("Rootfs maintenance disabled; object store is not configured")
		return
	}

	maintenance := rootfsmaintenance.New(
		s.sandboxStore,
		s.rootFSObjectStore,
		rootFSMaintenanceControllerConfig(s.cfg),
		s.logger,
		s.managerMetrics,
	)
	maintenance.SetObjectInspector(rootFSObjectStoreInspector{store: s.rootFSObjectStore})
	if s.meteringRepo != nil {
		maintenance.SetStorageMeteringRecorder(s.meteringRepo)
	}
	go logControllerErrorExact(ctx, s.logger, "Rootfs maintenance controller failed", func() error {
		return maintenance.Run(ctx)
	})
}

func logControllerError(ctx context.Context, logger *zap.Logger, message string, run func() error) {
	if err := run(); err != nil && !errors.Is(err, context.Canceled) {
		logger.Error(message, zap.Error(err))
	}
}

func logControllerErrorExact(ctx context.Context, logger *zap.Logger, message string, run func() error) {
	if err := run(); err != nil && err != context.Canceled {
		logger.Error(message, zap.Error(err))
	}
}

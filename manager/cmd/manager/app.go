package main

import (
	"context"
	"errors"
	"net/http"
	"time"

	httpserver "github.com/sandbox0-ai/sandbox0/manager/pkg/http"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/nodeauthority"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/nodeenrollment"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/nodepoolautoscaler"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/nodepoollifecycle"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/rootfsimportdiscovery"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/rootfsimportworker"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/rootfsmaterializer"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/runtimeslotreconciler"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxclaimreconciler"
	"go.uber.org/zap"
)

// managerApp owns process-level startup and shutdown. Feature
// construction stays outside this lifecycle object so dependencies are fully
// assembled before any server or controller starts.
type managerApp struct {
	ctx                    context.Context
	cancel                 context.CancelFunc
	logger                 *zap.Logger
	httpServer             *httpserver.Server
	nodeAuthority          *nodeauthority.Component
	nodeEnrollment         *nodeenrollment.Server
	nodePoolAutoscaler     *nodepoolautoscaler.Worker
	nodePoolLifecycle      *nodepoollifecycle.Worker
	rootFSMaterializer     *rootfsmaterializer.Worker
	rootFSImportDiscovery  *rootfsimportdiscovery.Worker
	rootFSImporter         *rootfsimportworker.Worker
	sandboxClaimReconciler *sandboxclaimreconciler.Worker
	metricsPort            int
	startControllers       func(context.Context)
}

func (a *managerApp) Run() {
	go startMetricsServer(a.metricsPort, a.logger)
	if !a.startNodeAuthority() {
		return
	}
	if !a.startNodeEnrollment() {
		return
	}
	if a.nodePoolAutoscaler != nil {
		go a.nodePoolAutoscaler.Run(a.ctx, func(decision nodepoolautoscaler.Decision, err error) {
			fields := []zap.Field{
				zap.String("action", decision.Action),
				zap.Int("current_elastic", decision.CurrentElastic),
				zap.Int("target_elastic", decision.TargetElastic),
			}
			if err != nil {
				a.logger.Warn("Sandbox node pool reconcile failed", append(fields, zap.Error(err))...)
				return
			}
			if decision.Action == "scale_out" || decision.Action == "scale_in" {
				a.logger.Info("Sandbox node pool desired capacity changed", fields...)
			}
		})
		a.logger.Info("Sandbox node pool autoscaler started")
	}
	if a.nodePoolLifecycle != nil {
		go a.nodePoolLifecycle.Run(a.ctx, func(result nodepoollifecycle.Result, err error) {
			fields := []zap.Field{
				zap.Int("observed", result.Observed),
				zap.Int("completed", result.Completed),
				zap.Int("rolled_back", result.RolledBack),
			}
			if err != nil {
				a.logger.Warn("Sandbox node lifecycle reconcile failed", append(fields, zap.Error(err))...)
			}
		})
		a.logger.Info("Sandbox node lifecycle controller started")
	}
	if a.rootFSMaterializer != nil {
		go a.rootFSMaterializer.Run(a.ctx, func(result rootfsmaterializer.Result, err error) {
			logRootFSCompositeMaterializerPass(a.logger, result, err)
		})
		a.logger.Info("Active-active Rootfs composite materializer started")
	}
	if a.rootFSImportDiscovery != nil {
		go a.rootFSImportDiscovery.Run(a.ctx, func(result rootfsimportdiscovery.Result, err error) {
			logRootFSImportDiscoveryPass(a.logger, result, err)
		})
		a.logger.Info("Active-active template Rootfs import discovery started")
	}
	if a.rootFSImporter != nil {
		go a.rootFSImporter.Run(a.ctx, func(result rootfsimportworker.Result, err error) {
			logRootFSImportWorkerPass(a.logger, result, err)
		})
		a.logger.Info("Active-active durable Rootfs importer started")
	}
	if a.sandboxClaimReconciler != nil {
		go a.sandboxClaimReconciler.Run(a.ctx, func(result sandboxclaimreconciler.Result, err error) {
			logSandboxClaimReconcilePass(a.logger, result, err)
		})
		a.logger.Info("Active-active abandoned sandbox claim reconciler started")
	}

	go func() {
		if err := a.httpServer.Start(a.ctx); err != nil && err != http.ErrServerClosed {
			a.logger.Fatal("HTTP server failed", zap.Error(err))
		}
	}()

	if a.startControllers != nil {
		a.startControllers(a.ctx)
	}

	a.logger.Info("Manager is running")
	<-a.ctx.Done()
	a.logger.Info("Shutting down gracefully")
	// Give components time to finish their context-driven shutdown paths.
	time.Sleep(2 * time.Second)
	a.logger.Info("Manager stopped")
}

func (a *managerApp) startNodeEnrollment() bool {
	if a.nodeEnrollment == nil {
		return true
	}
	errorsCh := make(chan error, 1)
	go func() { errorsCh <- a.nodeEnrollment.Run(a.ctx) }()
	select {
	case <-a.ctx.Done():
		return false
	case err := <-errorsCh:
		if err != nil {
			a.logger.Error("Manager node enrollment failed before becoming ready", zap.Error(err))
		}
		a.cancel()
		return false
	case <-a.nodeEnrollment.Ready():
		a.logger.Info("Manager node enrollment listener is ready")
	}
	go func() {
		if err := <-errorsCh; err != nil && !errors.Is(err, context.Canceled) {
			a.logger.Error("Manager node enrollment stopped", zap.Error(err))
			a.cancel()
		}
	}()
	return true
}

func (a *managerApp) startNodeAuthority() bool {
	if a.nodeAuthority == nil {
		return true
	}
	errorsCh := make(chan error, 1)
	go func() { errorsCh <- a.nodeAuthority.RunServer(a.ctx) }()
	select {
	case <-a.ctx.Done():
		return false
	case err := <-errorsCh:
		if err != nil {
			a.logger.Error("Manager node authority failed before becoming ready", zap.Error(err))
		}
		a.cancel()
		return false
	case <-a.nodeAuthority.Ready():
		a.logger.Info("Manager node authority listener is ready")
	}
	go func() {
		if err := <-errorsCh; err != nil && !errors.Is(err, context.Canceled) {
			a.logger.Error("Manager node authority stopped", zap.Error(err))
			a.cancel()
		}
	}()
	if a.nodeAuthority.TerminalEnabled() {
		go func() {
			err := a.nodeAuthority.RunTerminal(a.ctx, func(report runtimeslotreconciler.WorkerReport) {
				logManagerRuntimeSlotTerminalPass(a.logger, report)
			})
			if err != nil && !errors.Is(err, context.Canceled) {
				a.logger.Error("Runtime slot terminal worker stopped", zap.Error(err))
				a.cancel()
			}
		}()
		a.logger.Info("Active-active runtime slot terminal reconciler started")
	}
	return true
}

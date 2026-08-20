package main

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/generated/informers/externalversions"
	httpserver "github.com/sandbox0-ai/sandbox0/manager/pkg/http"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/nodeauthority"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/rootfsmaterializer"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/runtimeslotreconciler"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxclaimreconciler"
	"go.uber.org/zap"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
)

// managerApp owns process-level startup, leader election, and shutdown. Feature
// construction stays outside this lifecycle object so dependencies are fully
// assembled before any server or controller starts.
type managerApp struct {
	ctx                    context.Context
	cancel                 context.CancelFunc
	logger                 *zap.Logger
	k8sClient              kubernetes.Interface
	httpServer             *httpserver.Server
	nodeAuthority          *nodeauthority.Component
	rootFSMaterializer     *rootfsmaterializer.Worker
	sandboxClaimReconciler *sandboxclaimreconciler.Worker
	informerFactory        informers.SharedInformerFactory
	crdInformerFactory     externalversions.SharedInformerFactory
	cacheSyncs             []cache.InformerSynced
	metricsPort            int
	leaderElectionEnabled  bool
	startControllers       func(context.Context)
}

func (a *managerApp) Run() {
	go startMetricsServer(a.metricsPort, a.logger)
	if !a.startNodeAuthority() {
		return
	}
	if a.rootFSMaterializer != nil {
		go a.rootFSMaterializer.Run(a.ctx, func(result rootfsmaterializer.Result, err error) {
			logRootFSCompositeMaterializerPass(a.logger, result, err)
		})
		a.logger.Info("Active-active Rootfs composite materializer started")
	}
	if a.sandboxClaimReconciler != nil {
		go a.sandboxClaimReconciler.Run(a.ctx, func(result sandboxclaimreconciler.Result, err error) {
			logSandboxClaimReconcilePass(a.logger, result, err)
		})
		a.logger.Info("Active-active abandoned sandbox claim reconciler started")
	}

	a.logger.Info("Starting informers")
	a.informerFactory.Start(a.ctx.Done())
	a.crdInformerFactory.Start(a.ctx.Done())

	a.logger.Info("Waiting for informer caches to sync")
	if !cache.WaitForCacheSync(a.ctx.Done(), a.cacheSyncs...) {
		a.logger.Fatal("Failed to sync informer caches")
	}
	for typ, synced := range a.crdInformerFactory.WaitForCacheSync(a.ctx.Done()) {
		if !synced {
			a.logger.Warn("CRD informer cache not synced", zap.String("type", typ.String()))
		} else {
			a.logger.Info("CRD informer cache synced", zap.String("type", typ.String()))
		}
	}

	go func() {
		if err := a.httpServer.Start(a.ctx); err != nil && err != http.ErrServerClosed {
			a.logger.Fatal("HTTP server failed", zap.Error(err))
		}
	}()

	if a.leaderElectionEnabled {
		go func() {
			if err := runManagerLeaderElection(a.ctx, a.k8sClient, a.logger, a.startControllers, a.cancel); err != nil {
				a.logger.Error("Manager controller leader election stopped", zap.Error(err))
				a.cancel()
			}
		}()
	} else {
		a.logger.Warn("Manager controller leader election is disabled")
		a.startControllers(a.ctx)
	}

	a.logger.Info("Manager is running", zap.Bool("leaderElection", a.leaderElectionEnabled))
	<-a.ctx.Done()
	a.logger.Info("Shutting down gracefully")
	// Give components time to finish their context-driven shutdown paths.
	time.Sleep(2 * time.Second)
	a.logger.Info("Manager stopped")
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

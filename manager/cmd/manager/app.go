package main

import (
	"context"
	"net/http"
	"time"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/generated/informers/externalversions"
	httpserver "github.com/sandbox0-ai/sandbox0/manager/pkg/http"
	storageproxyruntime "github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/runtime"
	"go.uber.org/zap"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
)

// managerApp owns process-level startup, leader election, and shutdown. Feature
// construction stays outside this lifecycle object so dependencies are fully
// assembled before any server or controller starts.
type managerApp struct {
	ctx                   context.Context
	cancel                context.CancelFunc
	logger                *zap.Logger
	k8sClient             kubernetes.Interface
	httpServer            *httpserver.Server
	storageRuntime        *storageproxyruntime.Runtime
	informerFactory       informers.SharedInformerFactory
	crdInformerFactory    externalversions.SharedInformerFactory
	cacheSyncs            []cache.InformerSynced
	metricsPort           int
	leaderElectionEnabled bool
	startControllers      func(context.Context)
}

func (a *managerApp) Run() {
	go startMetricsServer(a.metricsPort, a.logger)

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
	if a.storageRuntime != nil {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
		if err := a.storageRuntime.Shutdown(shutdownCtx); err != nil {
			a.logger.Error("Manager storage shutdown reported errors", zap.Error(err))
		}
		shutdownCancel()
	}

	// Give components time to finish their context-driven shutdown paths.
	time.Sleep(2 * time.Second)
	a.logger.Info("Manager stopped")
}

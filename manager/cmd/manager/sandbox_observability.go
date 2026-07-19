package main

import (
	"context"
	"net/http"
	"strings"

	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/service"
	gatewayauthn "github.com/sandbox0-ai/sandbox0/pkg/gateway/authn"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"github.com/sandbox0-ai/sandbox0/pkg/observability"
	httpobs "github.com/sandbox0-ai/sandbox0/pkg/observability/http"
	"github.com/sandbox0-ai/sandbox0/pkg/sandboxobservability"
	sandboxobsingest "github.com/sandbox0-ai/sandbox0/pkg/sandboxobservability/ingest"
	"go.uber.org/zap"
	"k8s.io/client-go/kubernetes"
	corelisters "k8s.io/client-go/listers/core/v1"
)

func buildSandboxObservabilityLogWorker(cfg *config.ManagerConfig, internalAuthGen *internalauth.Generator, obsProvider *observability.Provider, logger *zap.Logger) *sandboxobsingest.LogWorker {
	if cfg == nil {
		return nil
	}
	if logger == nil {
		logger = zap.NewNop()
	}
	if strings.TrimSpace(cfg.SandboxObservabilityLogsIngestURL) == "" {
		logger.Info("Sandbox log observability producer disabled")
		return nil
	}
	if internalAuthGen == nil {
		logger.Warn("Sandbox observability producers disabled; internal auth generator is not configured")
		return nil
	}
	httpClient := &http.Client{Timeout: cfg.SandboxObservabilityIngestRequestTimeout.Duration}
	if obsProvider != nil {
		httpClient = obsProvider.HTTP.NewClient(httpobs.Config{Timeout: cfg.SandboxObservabilityIngestRequestTimeout.Duration})
	}
	writer := sandboxobservability.NewHTTPWriter(sandboxobservability.HTTPWriterOptions{
		LogsURL:        cfg.SandboxObservabilityLogsIngestURL,
		Client:         httpClient,
		RequestTimeout: cfg.SandboxObservabilityIngestRequestTimeout.Duration,
		TeamTokenProvider: func(_ context.Context, teamID string) (string, error) {
			return internalAuthGen.Generate(internalauth.ServiceClusterGateway, teamID, "", internalauth.GenerateOptions{
				Permissions: []string{gatewayauthn.PermSandboxObservabilityWrite},
			})
		},
	})
	ingestCfg := sandboxobsingest.Config{
		QueueSize:     cfg.SandboxObservabilityIngestQueueSize,
		BatchSize:     cfg.SandboxObservabilityIngestBatchSize,
		FlushInterval: cfg.SandboxObservabilityIngestFlushInterval.Duration,
		MaxRetries:    cfg.SandboxObservabilityIngestMaxRetries,
		RetryBackoff:  cfg.SandboxObservabilityIngestRetryBackoff.Duration,
	}
	worker, err := sandboxobsingest.NewLogWorker(writer, ingestCfg)
	if err != nil {
		logger.Warn("Sandbox log observability producer disabled", zap.Error(err))
		return nil
	}
	return worker
}

func startSandboxObservabilityLogProducer(ctx context.Context, cfg *config.ManagerConfig, k8sClient kubernetes.Interface, podLister corelisters.PodLister, logWorker *sandboxobsingest.LogWorker, logger *zap.Logger, clock service.TimeProvider) {
	if cfg == nil {
		return
	}
	if logWorker == nil {
		return
	}
	go logWorker.Run(ctx)
	producer := service.NewSandboxLogProducer(k8sClient, podLister, logWorker, service.SandboxLogProducerConfig{
		RegionID:     cfg.RegionID,
		ClusterID:    cfg.DefaultClusterId,
		PollInterval: cfg.SandboxObservabilityLogPollInterval.Duration,
	}, logger, clock)
	go producer.Run(ctx)
	logger.Info("Sandbox log observability producer started",
		zap.Duration("poll_interval", cfg.SandboxObservabilityLogPollInterval.Duration),
	)
}

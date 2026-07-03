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

func buildSandboxObservabilityProducerWorkers(cfg *config.ManagerConfig, internalAuthGen *internalauth.Generator, obsProvider *observability.Provider, logger *zap.Logger) (*sandboxobsingest.LogWorker, *sandboxobsingest.MetricWorker) {
	if cfg == nil {
		return nil, nil
	}
	if logger == nil {
		logger = zap.NewNop()
	}
	if strings.TrimSpace(cfg.SandboxObservabilityLogsIngestURL) == "" && strings.TrimSpace(cfg.SandboxObservabilityMetricsIngestURL) == "" {
		logger.Info("Sandbox observability producers disabled")
		return nil, nil
	}
	if internalAuthGen == nil {
		logger.Warn("Sandbox observability producers disabled; internal auth generator is not configured")
		return nil, nil
	}
	httpClient := &http.Client{Timeout: cfg.SandboxObservabilityIngestRequestTimeout.Duration}
	if obsProvider != nil {
		httpClient = obsProvider.HTTP.NewClient(httpobs.Config{Timeout: cfg.SandboxObservabilityIngestRequestTimeout.Duration})
	}
	writer := sandboxobservability.NewHTTPWriter(sandboxobservability.HTTPWriterOptions{
		LogsURL:        cfg.SandboxObservabilityLogsIngestURL,
		MetricsURL:     cfg.SandboxObservabilityMetricsIngestURL,
		Client:         httpClient,
		RequestTimeout: cfg.SandboxObservabilityIngestRequestTimeout.Duration,
		TokenProvider: func(context.Context) (string, error) {
			return internalAuthGen.GenerateSystem("cluster-gateway", internalauth.GenerateOptions{
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
	var logWorker *sandboxobsingest.LogWorker
	if strings.TrimSpace(cfg.SandboxObservabilityLogsIngestURL) != "" {
		worker, err := sandboxobsingest.NewLogWorker(writer, ingestCfg)
		if err != nil {
			logger.Warn("Sandbox log observability producer disabled", zap.Error(err))
		} else {
			logWorker = worker
		}
	}
	var metricWorker *sandboxobsingest.MetricWorker
	if strings.TrimSpace(cfg.SandboxObservabilityMetricsIngestURL) != "" {
		worker, err := sandboxobsingest.NewMetricWorker(writer, ingestCfg)
		if err != nil {
			logger.Warn("Sandbox runtime metric observability producer disabled", zap.Error(err))
		} else {
			metricWorker = worker
		}
	}
	return logWorker, metricWorker
}

func startSandboxObservabilityProducers(ctx context.Context, cfg *config.ManagerConfig, k8sClient kubernetes.Interface, sandboxService *service.SandboxService, podLister corelisters.PodLister, logWorker *sandboxobsingest.LogWorker, metricWorker *sandboxobsingest.MetricWorker, logger *zap.Logger, clock service.TimeProvider) {
	if cfg == nil {
		return
	}
	if logWorker != nil {
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
	if metricWorker != nil {
		go metricWorker.Run(ctx)
		sampler := service.NewSandboxRuntimeMetricSampler(sandboxService, podLister, metricWorker, service.SandboxRuntimeMetricSamplerConfig{
			RegionID:  cfg.RegionID,
			ClusterID: cfg.DefaultClusterId,
			Interval:  cfg.SandboxObservabilityMetricSampleInterval.Duration,
		}, logger, clock)
		go sampler.Run(ctx)
		logger.Info("Sandbox runtime metric observability sampler started",
			zap.Duration("sample_interval", cfg.SandboxObservabilityMetricSampleInterval.Duration),
		)
	}
}

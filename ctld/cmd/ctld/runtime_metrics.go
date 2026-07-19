package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	ctldpower "github.com/sandbox0-ai/sandbox0/ctld/internal/ctld/power"
	ctldruntimemetrics "github.com/sandbox0-ai/sandbox0/ctld/internal/ctld/runtimemetrics"
	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	gatewayauthn "github.com/sandbox0-ai/sandbox0/pkg/gateway/authn"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"github.com/sandbox0-ai/sandbox0/pkg/observability"
	httpobs "github.com/sandbox0-ai/sandbox0/pkg/observability/http"
	"github.com/sandbox0-ai/sandbox0/pkg/sandboxobservability"
	sandboxobsingest "github.com/sandbox0-ai/sandbox0/pkg/sandboxobservability/ingest"
	"go.uber.org/zap"
	corelisters "k8s.io/client-go/listers/core/v1"
)

type ctldRuntimeMetricsProducer struct {
	worker    *sandboxobsingest.RuntimeSampleWorker
	collector *ctldruntimemetrics.Collector
}

type ctldRuntimeMetricsHandle struct {
	collectorCancel context.CancelFunc
	collectorDone   <-chan struct{}
	workerCancel    context.CancelFunc
	workerDone      <-chan struct{}
	statsClose      func() error
	shutdownOnce    sync.Once
	shutdownErr     error
}

func startCtldRuntimeMetrics(ctx context.Context, cfg *config.CtldConfig, statsClient ctldruntimemetrics.StatsClient, podCache *ctldpower.PodCache, obsProvider *observability.Provider, logger *zap.Logger) *ctldRuntimeMetricsHandle {
	if cfg == nil || strings.TrimSpace(cfg.SandboxObservabilityRuntimeSamplesIngestURL) == "" {
		return nil
	}
	if statsClient == nil || podCache == nil || podCache.PodLister() == nil {
		log.Printf("ctld runtime metric producer disabled: CRI client or pod cache unavailable")
		return nil
	}
	privateKey, err := internalauth.LoadEd25519PrivateKeyFromFile(internalauth.DefaultInternalJWTPrivateKeyPath)
	if err != nil {
		log.Printf("ctld runtime metric producer disabled: load internal jwt private key: %v", err)
		return nil
	}
	generator := internalauth.NewGenerator(internalauth.GeneratorConfig{
		Caller:     "ctld",
		PrivateKey: privateKey,
		TTL:        10 * time.Second,
	})
	producer, err := newCtldRuntimeMetricsProducer(cfg, statsClient, podCache.PodLister(), generator, obsProvider, logger)
	if err != nil {
		log.Printf("ctld runtime metric producer disabled: %v", err)
		return nil
	}
	handle := startCtldRuntimeMetricLoops(ctx, producer.worker.Run, func(collectorCtx context.Context) {
		syncCtx, cancel := context.WithTimeout(collectorCtx, 10*time.Second)
		synced := podCache.WaitForSync(syncCtx)
		cancel()
		if collectorCtx.Err() != nil {
			return
		}
		if !synced {
			log.Printf("ctld runtime metric producer starting before pod cache sync completed")
		}
		producer.collector.Run(collectorCtx)
	})
	if closer, ok := statsClient.(interface{ Close() error }); ok {
		handle.statsClose = closer.Close
	}
	log.Printf("ctld runtime metric producer started: sample_interval=%s sample_jitter=%s", cfg.SandboxObservabilityRuntimeSampleInterval.Duration, cfg.SandboxObservabilityRuntimeSampleJitter.Duration)
	return handle
}

func startCtldRuntimeMetricLoops(parent context.Context, workerRun, collectorRun func(context.Context)) *ctldRuntimeMetricsHandle {
	collectorCtx, collectorCancel := context.WithCancel(parent)
	workerCtx, workerCancel := context.WithCancel(context.WithoutCancel(parent))
	collectorDone := make(chan struct{})
	workerDone := make(chan struct{})
	go func() {
		defer close(workerDone)
		workerRun(workerCtx)
	}()
	go func() {
		defer close(collectorDone)
		collectorRun(collectorCtx)
	}()
	return &ctldRuntimeMetricsHandle{
		collectorCancel: collectorCancel,
		collectorDone:   collectorDone,
		workerCancel:    workerCancel,
		workerDone:      workerDone,
	}
}

func (h *ctldRuntimeMetricsHandle) Shutdown(ctx context.Context) error {
	if h == nil {
		return nil
	}
	h.shutdownOnce.Do(func() {
		h.collectorCancel()
		if err := waitRuntimeMetricLoop(ctx, h.collectorDone, "collector"); err != nil {
			h.workerCancel()
			h.shutdownErr = errors.Join(err, h.closeStats())
			return
		}
		h.workerCancel()
		h.shutdownErr = errors.Join(waitRuntimeMetricLoop(ctx, h.workerDone, "worker"), h.closeStats())
	})
	return h.shutdownErr
}

func (h *ctldRuntimeMetricsHandle) closeStats() error {
	if h == nil || h.statsClose == nil {
		return nil
	}
	return h.statsClose()
}

func waitRuntimeMetricLoop(ctx context.Context, done <-chan struct{}, name string) error {
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("wait for runtime metric %s shutdown: %w", name, ctx.Err())
	}
}

func newCtldRuntimeMetricsProducer(cfg *config.CtldConfig, statsClient ctldruntimemetrics.StatsClient, podLister corelisters.PodLister, generator *internalauth.Generator, obsProvider *observability.Provider, logger *zap.Logger) (*ctldRuntimeMetricsProducer, error) {
	if cfg == nil {
		return nil, fmt.Errorf("ctld config is nil")
	}
	if strings.TrimSpace(cfg.SandboxObservabilityRuntimeSamplesIngestURL) == "" {
		return nil, fmt.Errorf("runtime samples ingest URL is empty")
	}
	if generator == nil {
		return nil, fmt.Errorf("internal auth generator is nil")
	}
	httpClient := &http.Client{}
	if obsProvider != nil {
		httpClient = obsProvider.HTTP.NewClient(httpobs.Config{Timeout: cfg.SandboxObservabilityIngestRequestTimeout.Duration})
	}
	writer := sandboxobservability.NewHTTPWriter(sandboxobservability.HTTPWriterOptions{
		RuntimeSamplesURL: cfg.SandboxObservabilityRuntimeSamplesIngestURL,
		Client:            httpClient,
		RequestTimeout:    cfg.SandboxObservabilityIngestRequestTimeout.Duration,
		TeamTokenProvider: func(_ context.Context, teamID string) (string, error) {
			return generator.Generate(internalauth.ServiceClusterGateway, teamID, "", internalauth.GenerateOptions{
				Permissions: []string{gatewayauthn.PermSandboxObservabilityWrite},
			})
		},
	})
	worker, err := sandboxobsingest.NewRuntimeSampleWorker(writer, sandboxobsingest.Config{
		QueueSize:     cfg.SandboxObservabilityIngestQueueSize,
		BatchSize:     cfg.SandboxObservabilityIngestBatchSize,
		FlushInterval: cfg.SandboxObservabilityIngestFlushInterval.Duration,
		MaxRetries:    cfg.SandboxObservabilityIngestMaxRetries,
		RetryBackoff:  cfg.SandboxObservabilityIngestRetryBackoff.Duration,
	})
	if err != nil {
		return nil, fmt.Errorf("create runtime sample worker: %w", err)
	}
	collector, err := ctldruntimemetrics.NewCollector(ctldruntimemetrics.CollectorConfig{
		RegionID:    cfg.RegionID,
		ClusterID:   cfg.DefaultClusterId,
		NodeName:    nodeName,
		Interval:    cfg.SandboxObservabilityRuntimeSampleInterval.Duration,
		Jitter:      cfg.SandboxObservabilityRuntimeSampleJitter.Duration,
		Logger:      logger,
		StatsClient: statsClient,
		PodLister:   podLister,
		Sink:        worker,
	})
	if err != nil {
		return nil, fmt.Errorf("create runtime metric collector: %w", err)
	}
	return &ctldRuntimeMetricsProducer{worker: worker, collector: collector}, nil
}

package main

import (
	"context"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	apiconfig "github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	meteringoutbox "github.com/sandbox0-ai/sandbox0/pkg/metering/outbox"
	"github.com/sandbox0-ai/sandbox0/pkg/naming"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/objectstore/requestmetering"
	"go.uber.org/zap"
)

const ctldObjectStoreRequestFinalFlushTimeout = 10 * time.Second

func startCtldObjectStoreRequestMetering(
	ctx context.Context,
	cfg *apiconfig.StorageProxyConfig,
	pool *pgxpool.Pool,
	nodeName string,
	logger *zap.Logger,
) *requestmetering.Aggregator {
	if cfg == nil || !cfg.Metering.Enabled || pool == nil {
		return nil
	}
	instance := strings.TrimSpace(nodeName)
	if instance == "" {
		instance = strings.TrimSpace(podName)
	}
	aggregator := requestmetering.NewAggregator(
		requestmetering.NewRecorder(meteringoutbox.NewRepository(pool)),
		cfg.RegionID,
		naming.ClusterIDOrDefault(&cfg.DefaultClusterId),
		requestmetering.ProducerName(requestmetering.ProducerCtld, instance),
		logger,
	)
	go aggregator.Run(ctx, requestmetering.DefaultFlushInterval)
	return aggregator
}

func flushCtldObjectStoreRequestMetering(aggregator *requestmetering.Aggregator, logger *zap.Logger) {
	if aggregator == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), ctldObjectStoreRequestFinalFlushTimeout)
	defer cancel()
	if err := aggregator.Flush(ctx); err != nil && logger != nil {
		logger.Warn("Final object store request metering flush failed", zap.Error(err))
	}
}

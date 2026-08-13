package main

import (
	"context"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
	ctldobjectmetering "github.com/sandbox0-ai/sandbox0/ctld/internal/ctld/objectrequestmetering"
	apiconfig "github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/objectstore/requestmetering"
	"go.uber.org/zap"
)

func startCtldObjectStoreRequestMetering(
	ctx context.Context,
	cfg *apiconfig.StorageProxyConfig,
	pool *pgxpool.Pool,
	nodeName string,
	logger *zap.Logger,
) *requestmetering.Aggregator {
	instance := strings.TrimSpace(nodeName)
	if instance == "" {
		instance = strings.TrimSpace(podName)
	}
	return ctldobjectmetering.Start(ctx, cfg, pool, instance, logger)
}

func flushCtldObjectStoreRequestMetering(aggregator *requestmetering.Aggregator, logger *zap.Logger) {
	ctldobjectmetering.Flush(aggregator, logger)
}

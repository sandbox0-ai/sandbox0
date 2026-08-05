package objectrequestmetering

import (
	"context"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	apiconfig "github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	meteringoutbox "github.com/sandbox0-ai/sandbox0/pkg/metering/outbox"
	"github.com/sandbox0-ai/sandbox0/pkg/naming"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfslease"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/objectstore/requestmetering"
	"go.uber.org/zap"
)

const finalFlushTimeout = 10 * time.Second

func Start(
	ctx context.Context,
	cfg *apiconfig.StorageProxyConfig,
	pool *pgxpool.Pool,
	instance string,
	logger *zap.Logger,
) *requestmetering.Aggregator {
	if cfg == nil || !cfg.Metering.Enabled || pool == nil {
		return nil
	}
	aggregator := requestmetering.NewAggregator(
		requestmetering.NewRecorder(meteringoutbox.NewRepository(pool)),
		cfg.RegionID,
		naming.ClusterIDOrDefault(&cfg.DefaultClusterId),
		requestmetering.ProducerName(requestmetering.ProducerCtld, strings.TrimSpace(instance)),
		logger,
	)
	aggregator.SetRootFSTeamResolver(rootfslease.NewRepository(pool))
	go aggregator.Run(ctx, requestmetering.DefaultFlushInterval)
	return aggregator
}

func Flush(aggregator *requestmetering.Aggregator, logger *zap.Logger) {
	if aggregator == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), finalFlushTimeout)
	defer cancel()
	if err := aggregator.Flush(ctx); err != nil && logger != nil {
		logger.Warn("Final object store request metering flush failed", zap.Error(err))
	}
}

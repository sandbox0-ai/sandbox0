package objectrequestmetering

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

const finalFlushTimeout = 10 * time.Second

// Start creates the shared object-request metering producer used by ctld and
// the rootfs snapshotter. PostgreSQL remains the durable usage-truth boundary.
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
	go aggregator.Run(ctx, requestmetering.DefaultFlushInterval)
	return aggregator
}

// Flush persists the producer's final in-memory request counts during a clean
// shutdown.
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

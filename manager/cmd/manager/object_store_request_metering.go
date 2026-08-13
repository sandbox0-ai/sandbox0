package main

import (
	"context"
	"os"
	"strings"
	"time"

	meteringoutbox "github.com/sandbox0-ai/sandbox0/pkg/metering/outbox"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfslease"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/objectstore/requestmetering"
	"go.uber.org/zap"
)

const objectStoreRequestFinalFlushTimeout = 10 * time.Second

func startManagerObjectStoreRequestMetering(
	ctx context.Context,
	repo *meteringoutbox.Repository,
	regionID,
	clusterID string,
	logger *zap.Logger,
) *requestmetering.Aggregator {
	if repo == nil {
		return nil
	}
	instance := strings.TrimSpace(os.Getenv("POD_NAME"))
	if instance == "" {
		instance = strings.TrimSpace(clusterID)
	}
	aggregator := requestmetering.NewAggregator(
		requestmetering.NewRecorder(repo),
		regionID,
		clusterID,
		requestmetering.ProducerName(requestmetering.ProducerManager, instance),
		logger,
	)
	aggregator.SetRootFSTeamResolver(rootfslease.NewRepository(repo.Pool()))
	go aggregator.Run(ctx, requestmetering.DefaultFlushInterval)
	return aggregator
}

func flushObjectStoreRequestMetering(aggregator *requestmetering.Aggregator, logger *zap.Logger) {
	if aggregator == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), objectStoreRequestFinalFlushTimeout)
	defer cancel()
	if err := aggregator.Flush(ctx); err != nil && logger != nil {
		logger.Warn("Final object store request metering flush failed", zap.Error(err))
	}
}

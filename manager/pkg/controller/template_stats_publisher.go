package controller

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/pkg/clock"
	"github.com/sandbox0-ai/sandbox0/pkg/pubsub"
	"go.uber.org/zap"
)

// TemplateCounts holds idle and active counts.
type TemplateCounts struct {
	IdleCount   int32
	ActiveCount int32
}

// TemplateStatsPublisher publishes template stats updates.
type TemplateStatsPublisher interface {
	PublishTemplateStats(ctx context.Context, template *v1alpha1.SandboxTemplate, idleCount, activeCount int32) error
}

// PGTemplateStatsPublisher publishes stats through PostgreSQL NOTIFY.
type PGTemplateStatsPublisher struct {
	pool      *pgxpool.Pool
	clusterID string
	clk       *clock.Clock
	logger    *zap.Logger
}

// NewPGTemplateStatsPublisher creates a new publisher.
func NewPGTemplateStatsPublisher(pool *pgxpool.Pool, clusterID string, clk *clock.Clock, logger *zap.Logger) *PGTemplateStatsPublisher {
	return &PGTemplateStatsPublisher{
		pool:      pool,
		clusterID: clusterID,
		clk:       clk,
		logger:    logger,
	}
}

// PublishTemplateStats sends an idle/active stats event for a template.
func (p *PGTemplateStatsPublisher) PublishTemplateStats(ctx context.Context, template *v1alpha1.SandboxTemplate, idleCount, activeCount int32) error {
	if p == nil || p.pool == nil {
		return fmt.Errorf("publisher not configured")
	}
	if template == nil {
		return fmt.Errorf("template is nil")
	}
	if p.clusterID == "" {
		return fmt.Errorf("cluster id is empty")
	}

	event := pubsub.TemplateIdleEvent{
		EventBase:   pubsub.NewEventBase(p.clk),
		ClusterID:   p.clusterID,
		TemplateID:  template.Name,
		IdleCount:   idleCount,
		ActiveCount: activeCount,
	}

	payload, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal event: %w", err)
	}

	_, err = p.pool.Exec(ctx, "SELECT pg_notify($1, $2)", pubsub.TemplateIdleChannel, string(payload))
	if err != nil {
		return fmt.Errorf("notify: %w", err)
	}

	p.logger.Debug("Published template stats",
		zap.String("cluster_id", event.ClusterID),
		zap.String("template_id", event.TemplateID),
		zap.Int32("idle_count", event.IdleCount),
		zap.Int32("active_count", event.ActiveCount),
	)

	return nil
}

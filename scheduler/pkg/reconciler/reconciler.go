package reconciler

import (
	"context"
	"time"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/pkg/clock"
	obsmetrics "github.com/sandbox0-ai/sandbox0/pkg/observability/metrics"
	templreconciler "github.com/sandbox0-ai/sandbox0/pkg/template/reconciler"
	"github.com/sandbox0-ai/sandbox0/pkg/template/store"
	"github.com/sandbox0-ai/sandbox0/scheduler/pkg/client"
	"github.com/sandbox0-ai/sandbox0/scheduler/pkg/db"
	"go.uber.org/zap"
)

// Reconciler wraps the shared multi-cluster reconciler.
type Reconciler struct {
	inner *templreconciler.MultiClusterReconciler
}

// NewReconciler creates a new Reconciler.
func NewReconciler(
	templateStore store.TemplateStore,
	allocationStore store.AllocationStore,
	clusterStore *db.Repository,
	clusterGatewayClient *client.ClusterGatewayClient,
	interval time.Duration,
	clk *clock.Clock,
	podsPerNode int,
	logger *zap.Logger,
	metrics *obsmetrics.SchedulerMetrics,
) *Reconciler {
	adapter := &clusterGatewayAdapter{client: clusterGatewayClient}
	return &Reconciler{
		inner: templreconciler.NewMultiClusterReconciler(
			templateStore,
			allocationStore,
			clusterStore,
			adapter,
			interval,
			clk,
			podsPerNode,
			logger,
			metrics,
		),
	}
}

// Start starts the reconciliation loop.
func (r *Reconciler) Start(ctx context.Context) {
	r.inner.Start(ctx)
}

// TriggerReconcile triggers an immediate reconciliation.
func (r *Reconciler) TriggerReconcile(ctx context.Context) {
	r.inner.TriggerReconcile(ctx)
}

// GetStatus returns the current reconciler status.
func (r *Reconciler) GetStatus() (time.Time, error) {
	return r.inner.GetStatus()
}

// GetTemplateIdleCount returns idle count for a template in a cluster.
func (r *Reconciler) GetTemplateIdleCount(clusterID, templateID string) (int32, bool) {
	return r.inner.GetTemplateIdleCount(clusterID, templateID)
}

// GetTemplateStatsAge returns age since last stats update for a cluster.
func (r *Reconciler) GetTemplateStatsAge(clusterID string) (time.Duration, bool) {
	return r.inner.GetTemplateStatsAge(clusterID)
}

// GetTemplateStatsUpdatedAt returns when the latest template stats snapshot was collected.
func (r *Reconciler) GetTemplateStatsUpdatedAt(clusterID string) (time.Time, bool) {
	return r.inner.GetTemplateStatsUpdatedAt(clusterID)
}

// GetClusterSummary returns the cached cluster summary for a shard.
func (r *Reconciler) GetClusterSummary(clusterID string) (*templreconciler.ClusterSummary, bool) {
	return r.inner.GetClusterSummary(clusterID)
}

// GetClusterSummaryAge returns age since the cluster summary cache was updated.
func (r *Reconciler) GetClusterSummaryAge(clusterID string) (time.Duration, bool) {
	return r.inner.GetClusterSummaryAge(clusterID)
}

// UpdateTemplateStats updates stats cache for a template in a cluster.
func (r *Reconciler) UpdateTemplateStats(clusterID, templateID string, idleCount, activeCount int32, updatedAt time.Time) {
	r.inner.UpdateTemplateStats(clusterID, templateID, idleCount, activeCount, updatedAt)
}

type clusterGatewayAdapter struct {
	client *client.ClusterGatewayClient
}

func (a *clusterGatewayAdapter) GetClusterSummary(ctx context.Context, baseURL string) (*templreconciler.ClusterSummary, error) {
	summary, err := a.client.GetClusterSummary(ctx, baseURL)
	if err != nil {
		return nil, err
	}
	return &templreconciler.ClusterSummary{
		ClusterID:             summary.ClusterID,
		NodeCount:             summary.NodeCount,
		TotalNodeCount:        summary.TotalNodeCount,
		SandboxNodeCount:      summary.SandboxNodeCount,
		IdlePodCount:          summary.IdlePodCount,
		ActivePodCount:        summary.ActivePodCount,
		PendingActivePodCount: summary.PendingActivePodCount,
		TotalPodCount:         summary.TotalPodCount,
	}, nil
}

func (a *clusterGatewayAdapter) GetTemplateStats(ctx context.Context, baseURL string) (*templreconciler.TemplateStats, error) {
	stats, err := a.client.GetTemplateStats(ctx, baseURL)
	if err != nil {
		return nil, err
	}
	out := &templreconciler.TemplateStats{
		Templates: make([]templreconciler.TemplateStat, 0, len(stats.Templates)),
	}
	for _, stat := range stats.Templates {
		out.Templates = append(out.Templates, templreconciler.TemplateStat{
			TemplateID:         stat.TemplateID,
			IdleCount:          stat.IdleCount,
			ActiveCount:        stat.ActiveCount,
			PendingActiveCount: stat.PendingActiveCount,
			MinIdle:            stat.MinIdle,
			MaxIdle:            stat.MaxIdle,
		})
	}
	return out, nil
}

func (a *clusterGatewayAdapter) CreateOrUpdateTemplate(ctx context.Context, baseURL string, template *v1alpha1.SandboxTemplate) error {
	return a.client.CreateOrUpdateTemplate(ctx, baseURL, template)
}

func (a *clusterGatewayAdapter) DeleteTemplate(ctx context.Context, baseURL string, templateID string) error {
	return a.client.DeleteTemplate(ctx, baseURL, templateID)
}

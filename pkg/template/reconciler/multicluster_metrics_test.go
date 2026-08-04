package reconciler

import (
	"context"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/pkg/template"
	"go.uber.org/zap"
)

func TestFetchClusterSummariesPublishesCapacityMetrics(t *testing.T) {
	metrics := &metricRecorder{capacity: make(map[string]float64), summaryAge: make(map[string]float64)}

	rec := NewMultiClusterReconciler(
		nil,
		nil,
		fakeMetricClusterStore{},
		&fakeMetricClusterClient{
			summaries: map[string]*ClusterSummary{
				"http://cluster-a": {
					ClusterID:             "cluster-a",
					NodeCount:             4,
					TotalNodeCount:        4,
					SandboxNodeCount:      3,
					IdlePodCount:          2,
					ActivePodCount:        5,
					PendingActivePodCount: 2,
					TotalPodCount:         7,
				},
			},
		},
		time.Second,
		nil,
		10,
		zap.NewNop(),
		metrics,
	)

	rec.fetchClusterSummaries(context.Background(), []*template.Cluster{
		{
			ClusterID:         "cluster-a",
			ClusterGatewayURL: "http://cluster-a",
			Enabled:           true,
		},
	})

	if got := metrics.capacity["cluster-a/available_headroom"]; got != 23 {
		t.Fatalf("available_headroom = %v, want 23", got)
	}
	if got := metrics.capacity["cluster-a/pending_active_pods"]; got != 2 {
		t.Fatalf("pending_active_pods = %v, want 2", got)
	}
	if got := metrics.summaryAge["cluster-a"]; got != 0 {
		t.Fatalf("cluster_summary_age = %v, want 0", got)
	}
}

type metricRecorder struct {
	capacity   map[string]float64
	summaryAge map[string]float64
}

func (*metricRecorder) ObserveCapacityClamp(string, string) {}

func (*metricRecorder) ObserveTemplateAllocation(string, string, string, string, int32) {}

func (*metricRecorder) ObserveReconcileDuration(time.Duration) {}

func (*metricRecorder) ObserveReconcileResult(string) {}

func (*metricRecorder) ObserveLastReconcileTimestamp() {}

func (m *metricRecorder) ObserveClusterCapacity(clusterID, metric string, value float64) {
	m.capacity[clusterID+"/"+metric] = value
}

func (m *metricRecorder) ObserveClusterSummaryAge(clusterID string, ageSeconds float64) {
	m.summaryAge[clusterID] = ageSeconds
}

func (*metricRecorder) ObserveTemplateSyncStatus(string, string, string, float64) {}

func (*metricRecorder) ObserveOrphanRemoved(string) {}

type fakeMetricClusterStore struct{}

func (fakeMetricClusterStore) ListEnabledClusters(ctx context.Context) ([]*template.Cluster, error) {
	return nil, nil
}

func (fakeMetricClusterStore) UpdateClusterLastSeen(ctx context.Context, clusterID string) error {
	return nil
}

type fakeMetricClusterClient struct {
	summaries map[string]*ClusterSummary
}

func (c *fakeMetricClusterClient) GetClusterSummary(ctx context.Context, baseURL string) (*ClusterSummary, error) {
	return c.summaries[baseURL], nil
}

func (c *fakeMetricClusterClient) GetTemplateStats(ctx context.Context, baseURL string) (*TemplateStats, error) {
	return &TemplateStats{}, nil
}

func (c *fakeMetricClusterClient) CreateOrUpdateTemplate(ctx context.Context, baseURL string, tpl *v1alpha1.SandboxTemplate) error {
	return nil
}

func (c *fakeMetricClusterClient) DeleteTemplate(ctx context.Context, baseURL string, templateID string) error {
	return nil
}

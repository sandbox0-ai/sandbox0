package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// SchedulerMetrics contains resource-capacity routing metrics.
type SchedulerMetrics struct {
	ClusterCapacity  *prometheus.GaugeVec
	RoutingDecisions *prometheus.CounterVec
}

// NewScheduler registers scheduler metrics. It returns nil when registry is nil.
func NewScheduler(registry prometheus.Registerer) *SchedulerMetrics {
	if registry == nil {
		return nil
	}
	factory := promauto.With(registry)
	return &SchedulerMetrics{
		ClusterCapacity: factory.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "scheduler_cluster_resource_capacity",
				Help: "Live resource lease and resource-neutral warm-slot capacity by cluster.",
			},
			[]string{"cluster_id", "metric"},
		),
		RoutingDecisions: factory.NewCounterVec(
			prometheus.CounterOpts{
				Name: "scheduler_routing_decisions_total",
				Help: "Total number of regional sandbox routing decisions by cluster and reason.",
			},
			[]string{"cluster_id", "reason"},
		),
	}
}

// ObserveClusterCapacity records one live capacity value.
func (m *SchedulerMetrics) ObserveClusterCapacity(clusterID, metric string, value float64) {
	if m == nil || m.ClusterCapacity == nil {
		return
	}
	m.ClusterCapacity.WithLabelValues(clusterID, metric).Set(value)
}

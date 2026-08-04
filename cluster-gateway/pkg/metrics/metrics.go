package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// ClusterGatewayMetrics holds Prometheus metrics for cluster-gateway-specific
// control paths.
type ClusterGatewayMetrics struct {
	AuditDeliveryStageDuration *prometheus.HistogramVec
	AuditCanonicalQueueDepth   prometheus.Gauge
	AuditCanonicalInFlight     prometheus.Gauge
	AuditCanonicalBatchSize    *prometheus.HistogramVec
}

// NewClusterGateway registers and returns cluster-gateway metrics.
// It returns nil when registry is nil.
func NewClusterGateway(registry prometheus.Registerer) *ClusterGatewayMetrics {
	if registry == nil {
		return nil
	}

	factory := promauto.With(registry)
	return &ClusterGatewayMetrics{
		AuditDeliveryStageDuration: factory.NewHistogramVec(prometheus.HistogramOpts{
			Name: "cluster_gateway_audit_delivery_stage_duration_seconds",
			Help: "Duration of audit delivery stages by mode and result",
			Buckets: []float64{
				.0001, .00025, .0005, .001, .0025, .005, .01, .025,
				.05, .1, .25, .5, 1, 2.5, 5, 10,
			},
		}, []string{"mode", "stage", "result"}),
		AuditCanonicalQueueDepth: factory.NewGauge(prometheus.GaugeOpts{
			Name: "cluster_gateway_audit_canonical_queue_depth",
			Help: "Current number of foreground canonical audit batches waiting to be formed",
		}),
		AuditCanonicalInFlight: factory.NewGauge(prometheus.GaugeOpts{
			Name: "cluster_gateway_audit_canonical_in_flight",
			Help: "Current number of canonical audit batches writing to the backend",
		}),
		AuditCanonicalBatchSize: factory.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "cluster_gateway_audit_canonical_batch_size",
			Help:    "Number of audit events in each canonical backend insert",
			Buckets: []float64{1, 2, 4, 8, 16, 32, 64, 128, 256, 500},
		}, []string{"source", "result"}),
	}
}

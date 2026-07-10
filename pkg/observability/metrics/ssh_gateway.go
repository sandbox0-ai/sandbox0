package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// SSHGatewayMetrics holds Prometheus metrics for the ssh-gateway service.
type SSHGatewayMetrics struct {
	ConnectionsActive prometheus.Gauge
}

// NewSSHGateway registers and returns ssh-gateway metrics.
// Returns nil when registry is nil.
func NewSSHGateway(registry prometheus.Registerer) *SSHGatewayMetrics {
	if registry == nil {
		return nil
	}

	factory := promauto.With(registry)
	return &SSHGatewayMetrics{
		ConnectionsActive: factory.NewGauge(prometheus.GaugeOpts{
			Name: "ssh_gateway_connections_active",
			Help: "Current number of accepted downstream SSH TCP connections being handled",
		}),
	}
}

// Package metrics provides Prometheus metrics for netd.
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// SandboxConnectionsTotal tracks total connections per sandbox
	SandboxConnectionsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "netd",
			Name:      "sandbox_connections_total",
			Help:      "Total number of connections per sandbox",
		},
		[]string{"sandbox_id", "team_id", "direction", "decision"},
	)

	// SandboxBytesTotal tracks total bytes per sandbox
	SandboxBytesTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "netd",
			Name:      "sandbox_bytes_total",
			Help:      "Total bytes transferred per sandbox",
		},
		[]string{"sandbox_id", "team_id", "direction"},
	)

	// SandboxPacketsTotal tracks total packets per sandbox
	SandboxPacketsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "netd",
			Name:      "sandbox_packets_total",
			Help:      "Total packets per sandbox",
		},
		[]string{"sandbox_id", "team_id", "direction"},
	)

	// ProxyRequestDuration tracks proxy request duration
	ProxyRequestDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "netd",
			Name:      "proxy_request_duration_seconds",
			Help:      "Duration of proxy requests in seconds",
			Buckets:   prometheus.ExponentialBuckets(0.001, 2, 15), // 1ms to ~16s
		},
		[]string{"sandbox_id", "protocol", "decision"},
	)

	// PolicyEvaluationDuration tracks policy evaluation duration
	PolicyEvaluationDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "netd",
			Name:      "policy_evaluation_duration_seconds",
			Help:      "Duration of policy evaluation in seconds",
			Buckets:   prometheus.ExponentialBuckets(0.0001, 2, 10), // 0.1ms to ~100ms
		},
		[]string{"sandbox_id"},
	)

	// DNSResolutionDuration tracks DNS resolution duration
	DNSResolutionDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "netd",
			Name:      "dns_resolution_duration_seconds",
			Help:      "Duration of DNS resolution in seconds",
			Buckets:   prometheus.ExponentialBuckets(0.001, 2, 10), // 1ms to ~1s
		},
		[]string{"status"}, // success, error, rebinding_blocked
	)

	// ActiveSandboxes tracks number of active sandboxes on this node
	ActiveSandboxes = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "netd",
			Name:      "active_sandboxes",
			Help:      "Number of active sandboxes on this node",
		},
	)

	// IPTablesRulesTotal tracks number of iptables rules
	IPTablesRulesTotal = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "netd",
			Name:      "iptables_rules_total",
			Help:      "Total number of iptables rules",
		},
		[]string{"chain"},
	)

	// TCClassesTotal tracks number of tc classes
	TCClassesTotal = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "netd",
			Name:      "tc_classes_total",
			Help:      "Total number of tc classes",
		},
	)

	// WatcherCacheSize tracks size of watcher caches
	WatcherCacheSize = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "netd",
			Name:      "watcher_cache_size",
			Help:      "Size of watcher caches",
		},
		[]string{"cache_type"}, // pods, network_policies, bandwidth_policies
	)

	// RuleApplicationErrors tracks errors when applying rules
	RuleApplicationErrors = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "netd",
			Name:      "rule_application_errors_total",
			Help:      "Total number of errors when applying rules",
		},
		[]string{"sandbox_id", "rule_type"},
	)
)

// RecordConnection records a connection metric
func RecordConnection(sandboxID, teamID, direction, decision string) {
	SandboxConnectionsTotal.WithLabelValues(sandboxID, teamID, direction, decision).Inc()
}

// RecordBytes records bytes transferred
func RecordBytes(sandboxID, teamID, direction string, bytes int64) {
	SandboxBytesTotal.WithLabelValues(sandboxID, teamID, direction).Add(float64(bytes))
}

// RecordPackets records packets transferred
func RecordPackets(sandboxID, teamID, direction string, packets int64) {
	SandboxPacketsTotal.WithLabelValues(sandboxID, teamID, direction).Add(float64(packets))
}

// RecordProxyRequest records a proxy request
func RecordProxyRequest(sandboxID, protocol, decision string, durationSeconds float64) {
	ProxyRequestDuration.WithLabelValues(sandboxID, protocol, decision).Observe(durationSeconds)
}

// RecordPolicyEvaluation records policy evaluation time
func RecordPolicyEvaluation(sandboxID string, durationSeconds float64) {
	PolicyEvaluationDuration.WithLabelValues(sandboxID).Observe(durationSeconds)
}

// RecordDNSResolution records DNS resolution
func RecordDNSResolution(status string, durationSeconds float64) {
	DNSResolutionDuration.WithLabelValues(status).Observe(durationSeconds)
}

// SetActiveSandboxes sets the number of active sandboxes
func SetActiveSandboxes(count int) {
	ActiveSandboxes.Set(float64(count))
}

// SetIPTablesRules sets the number of iptables rules
func SetIPTablesRules(chain string, count int) {
	IPTablesRulesTotal.WithLabelValues(chain).Set(float64(count))
}

// SetTCClasses sets the number of tc classes
func SetTCClasses(count int) {
	TCClassesTotal.Set(float64(count))
}

// SetWatcherCacheSize sets the watcher cache size
func SetWatcherCacheSize(cacheType string, size int) {
	WatcherCacheSize.WithLabelValues(cacheType).Set(float64(size))
}

// RecordRuleError records a rule application error
func RecordRuleError(sandboxID, ruleType string) {
	RuleApplicationErrors.WithLabelValues(sandboxID, ruleType).Inc()
}

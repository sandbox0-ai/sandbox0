package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// ManagerMetrics holds Prometheus metrics for the manager service.
type ManagerMetrics struct {
	TemplatesTotal                  prometheus.Gauge
	IdlePodsTotal                   *prometheus.GaugeVec
	ActivePodsTotal                 *prometheus.GaugeVec
	SandboxClaimsTotal              *prometheus.CounterVec
	SandboxClaimDuration            *prometheus.HistogramVec
	SandboxClaimPhaseDuration       *prometheus.HistogramVec
	SandboxDeleteCleanupPhase       *prometheus.HistogramVec
	SandboxIdleClaimsTotal          *prometheus.CounterVec
	PodNetworkIdentityChecksTotal   *prometheus.CounterVec
	PodNetworkIdentityStageDuration *prometheus.HistogramVec
	NetworkPolicyApplyTotal         *prometheus.CounterVec
	NetworkPolicyApplyDuration      *prometheus.HistogramVec
	K8sClientRateLimit              *prometheus.GaugeVec
	AutoscalerDecisionsTotal        *prometheus.CounterVec
	AutoscalerPoolReplicas          *prometheus.GaugeVec
	AutoscalerPoolPods              *prometheus.GaugeVec
	AutoscalerColdClaimsInFlight    *prometheus.GaugeVec
	AutoscalerScaleDelta            *prometheus.HistogramVec
	PodsCleanedTotal                *prometheus.CounterVec
	ReconcileTotal                  *prometheus.CounterVec
	ReconcileDuration               *prometheus.HistogramVec
	MeteringEventsTotal             *prometheus.CounterVec
	MeteringWindowsTotal            *prometheus.CounterVec
	MeteringErrorsTotal             *prometheus.CounterVec
	RootFSMaintenanceRunsTotal      *prometheus.CounterVec
	RootFSMaintenanceDuration       *prometheus.HistogramVec
	RootFSGCLayersTotal             prometheus.Counter
	RootFSObjectDeletesTotal        *prometheus.CounterVec
	RootFSObjectDeletionQueueDepth  *prometheus.GaugeVec
	RootFSStorageBytes              prometheus.Gauge
	RootFSStorageObjects            prometheus.Gauge
}

// NewManager registers and returns manager metrics.
// Returns nil when registry is nil.
func NewManager(registry prometheus.Registerer) *ManagerMetrics {
	if registry == nil {
		return nil
	}

	factory := promauto.With(registry)

	return &ManagerMetrics{
		TemplatesTotal: factory.NewGauge(prometheus.GaugeOpts{
			Name: "manager_templates_total",
			Help: "Total number of sandbox templates",
		}),
		IdlePodsTotal: factory.NewGaugeVec(prometheus.GaugeOpts{
			Name: "manager_idle_pods_total",
			Help: "Total number of idle pods per template",
		}, []string{"template"}),
		ActivePodsTotal: factory.NewGaugeVec(prometheus.GaugeOpts{
			Name: "manager_active_pods_total",
			Help: "Total number of active pods per template",
		}, []string{"template"}),
		SandboxClaimsTotal: factory.NewCounterVec(prometheus.CounterOpts{
			Name: "manager_sandbox_claims_total",
			Help: "Total number of sandbox claims",
		}, []string{"template", "status"}),
		SandboxClaimDuration: factory.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "manager_sandbox_claim_duration_seconds",
			Help:    "Duration of sandbox claim operations",
			Buckets: prometheus.DefBuckets,
		}, []string{"template", "type"}), // type: "hot" (from pool) or "cold" (new pod)
		SandboxClaimPhaseDuration: factory.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "manager_sandbox_claim_phase_duration_seconds",
			Help:    "Duration of sandbox claim phases",
			Buckets: []float64{.001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30, 60, 120},
		}, []string{"template", "type", "phase", "status"}), // type: "hot", "cold", or "unknown"
		SandboxDeleteCleanupPhase: factory.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "manager_sandbox_delete_cleanup_phase_duration_seconds",
			Help:    "Duration of sandbox deletion cleanup phases",
			Buckets: []float64{.001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30, 60, 120, 300},
		}, []string{"phase", "status", "scope"}), // scope: "sandbox_delete", "runtime_only", or "unknown"
		SandboxIdleClaimsTotal: factory.NewCounterVec(prometheus.CounterOpts{
			Name: "manager_sandbox_idle_claims_total",
			Help: "Total number of idle-pool claim attempts by result",
		}, []string{"template", "result"}),
		PodNetworkIdentityChecksTotal: factory.NewCounterVec(prometheus.CounterOpts{
			Name: "manager_pod_network_identity_checks_total",
			Help: "Total number of pod network identity readiness checks by source, result, and reason",
		}, []string{"source", "result", "reason"}),
		PodNetworkIdentityStageDuration: factory.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "manager_pod_network_identity_stage_duration_seconds",
			Help:    "Duration of pod network identity wait stages",
			Buckets: []float64{.001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30, 60, 120},
		}, []string{"template", "stage", "status", "reason"}),
		NetworkPolicyApplyTotal: factory.NewCounterVec(prometheus.CounterOpts{
			Name: "manager_network_policy_apply_total",
			Help: "Total number of network policy apply attempts by provider and result",
		}, []string{"provider", "result"}),
		NetworkPolicyApplyDuration: factory.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "manager_network_policy_apply_duration_seconds",
			Help:    "Duration of network policy apply attempts by provider and result",
			Buckets: []float64{.001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30},
		}, []string{"provider", "result"}),
		K8sClientRateLimit: factory.NewGaugeVec(prometheus.GaugeOpts{
			Name: "manager_k8s_client_rate_limit",
			Help: "Effective Kubernetes client rate limit configuration values",
		}, []string{"setting"}),
		AutoscalerDecisionsTotal: factory.NewCounterVec(prometheus.CounterOpts{
			Name: "manager_autoscaler_decisions_total",
			Help: "Total number of autoscaler decisions by action, reason, and result",
		}, []string{"template", "action", "reason", "result"}),
		AutoscalerPoolReplicas: factory.NewGaugeVec(prometheus.GaugeOpts{
			Name: "manager_autoscaler_pool_replicas",
			Help: "Autoscaler observed and desired idle pool ReplicaSet replicas",
		}, []string{"template", "state"}), // state: current or desired
		AutoscalerPoolPods: factory.NewGaugeVec(prometheus.GaugeOpts{
			Name: "manager_autoscaler_pool_pods",
			Help: "Autoscaler observed pod counts by template and state",
		}, []string{"template", "state"}), // state: ready_idle, pending_idle, active, active_without_ip
		AutoscalerColdClaimsInFlight: factory.NewGaugeVec(prometheus.GaugeOpts{
			Name: "manager_autoscaler_cold_claims_in_flight",
			Help: "Current admitted cold claims waiting for pod network identity",
		}, []string{"template"}),
		AutoscalerScaleDelta: factory.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "manager_autoscaler_scale_delta",
			Help:    "Absolute ReplicaSet replica delta applied by the autoscaler",
			Buckets: []float64{1, 2, 5, 10, 20, 50, 100},
		}, []string{"template", "direction"}),
		PodsCleanedTotal: factory.NewCounterVec(prometheus.CounterOpts{
			Name: "manager_pods_cleaned_total",
			Help: "Total number of pods cleaned up",
		}, []string{"template", "reason"}), // reason: "excess" or "expired"
		ReconcileTotal: factory.NewCounterVec(prometheus.CounterOpts{
			Name: "manager_reconcile_total",
			Help: "Total number of reconciliation operations",
		}, []string{"template", "result"}), // result: "success" or "error"
		ReconcileDuration: factory.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "manager_reconcile_duration_seconds",
			Help:    "Duration of reconciliation operations",
			Buckets: prometheus.DefBuckets,
		}, []string{"template"}),
		MeteringEventsTotal: factory.NewCounterVec(prometheus.CounterOpts{
			Name: "manager_metering_events_total",
			Help: "Total number of manager metering lifecycle events attempted",
		}, []string{"event_type", "result"}),
		MeteringWindowsTotal: factory.NewCounterVec(prometheus.CounterOpts{
			Name: "manager_metering_windows_total",
			Help: "Total number of manager metering usage windows attempted",
		}, []string{"window_type", "result"}),
		MeteringErrorsTotal: factory.NewCounterVec(prometheus.CounterOpts{
			Name: "manager_metering_errors_total",
			Help: "Total number of manager metering projector errors",
		}, []string{"operation"}),
		RootFSMaintenanceRunsTotal: factory.NewCounterVec(prometheus.CounterOpts{
			Name: "manager_rootfs_maintenance_runs_total",
			Help: "Total number of rootfs maintenance cycles",
		}, []string{"status"}),
		RootFSMaintenanceDuration: factory.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "manager_rootfs_maintenance_duration_seconds",
			Help:    "Duration of rootfs maintenance cycles",
			Buckets: prometheus.DefBuckets,
		}, []string{"status"}),
		RootFSGCLayersTotal: factory.NewCounter(prometheus.CounterOpts{
			Name: "manager_rootfs_gc_layers_total",
			Help: "Total number of rootfs layer metadata records garbage-collected",
		}),
		RootFSObjectDeletesTotal: factory.NewCounterVec(prometheus.CounterOpts{
			Name: "manager_rootfs_object_deletes_total",
			Help: "Total number of rootfs object deletion attempts by status",
		}, []string{"status"}),
		RootFSObjectDeletionQueueDepth: factory.NewGaugeVec(prometheus.GaugeOpts{
			Name: "manager_rootfs_object_deletion_queue_depth",
			Help: "Rootfs object deletion queue depth by state",
		}, []string{"state"}),
		RootFSStorageBytes: factory.NewGauge(prometheus.GaugeOpts{
			Name: "manager_rootfs_storage_bytes",
			Help: "Current reachable persistent rootfs COW object bytes",
		}),
		RootFSStorageObjects: factory.NewGauge(prometheus.GaugeOpts{
			Name: "manager_rootfs_storage_objects",
			Help: "Current reachable persistent rootfs COW object count",
		}),
	}
}

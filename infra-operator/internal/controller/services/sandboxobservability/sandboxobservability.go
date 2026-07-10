package sandboxobservability

import (
	"context"
	"fmt"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	apiconfig "github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	infrav1alpha1 "github.com/sandbox0-ai/sandbox0/infra-operator/api/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/infra-operator/internal/controller/pkg/common"
	clickhousesvc "github.com/sandbox0-ai/sandbox0/infra-operator/internal/controller/services/clickhouse"
	obsclickhouse "github.com/sandbox0-ai/sandbox0/pkg/sandboxobservability/clickhouse"
)

type Reconciler struct {
	Resources *common.ResourceManager
}

type RuntimeConfig struct {
	DSN                         string
	Database                    string
	EventsTable                 string
	LogsTable                   string
	RuntimeSamplesTable         string
	RetentionDays               int
	LogsRetentionDays           int
	RuntimeSamplesRetentionDays int
	ConnectTimeout              metav1.Duration
	SkipSchemaMigration         bool
	Ingest                      infrav1alpha1.SandboxObservabilityIngestConfig
}

func NewReconciler(resources *common.ResourceManager) *Reconciler {
	return &Reconciler{Resources: resources}
}

func (r *Reconciler) CleanupBuiltinResources(ctx context.Context, infra *infrav1alpha1.Sandbox0Infra) error {
	return nil
}

func (r *Reconciler) Reconcile(ctx context.Context, infra *infrav1alpha1.Sandbox0Infra) error {
	if infra == nil || !infrav1alpha1.IsSandboxObservabilityEnabled(infra) {
		return nil
	}
	_, ok, err := GetRuntimeConfig(ctx, r.Resources.Client, infra)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("sandbox observability requires an enabled clickhouse component")
	}
	return nil
}

func ApplyClusterGatewayConfig(ctx context.Context, c client.Client, infra *infrav1alpha1.Sandbox0Infra, cfg *apiconfig.ClusterGatewayConfig) error {
	if cfg == nil {
		return nil
	}
	runtimeCfg, ok, err := GetRuntimeConfig(ctx, c, infra)
	if err != nil {
		return err
	}
	if !ok {
		cfg.SandboxObservability = apiconfig.SandboxObservabilityConfig{Backend: apiconfig.SandboxObservabilityBackendDisabled}
		return nil
	}
	cfg.SandboxObservability = apiconfig.SandboxObservabilityConfig{
		Backend: apiconfig.SandboxObservabilityBackendClickHouse,
		ClickHouse: apiconfig.SandboxObservabilityClickHouseConfig{
			DSN:                         runtimeCfg.DSN,
			Database:                    runtimeCfg.Database,
			EventsTable:                 runtimeCfg.EventsTable,
			LogsTable:                   runtimeCfg.LogsTable,
			RuntimeSamplesTable:         runtimeCfg.RuntimeSamplesTable,
			RetentionDays:               runtimeCfg.RetentionDays,
			LogsRetentionDays:           runtimeCfg.LogsRetentionDays,
			RuntimeSamplesRetentionDays: runtimeCfg.RuntimeSamplesRetentionDays,
			ConnectTimeout:              runtimeCfg.ConnectTimeout,
			SkipSchemaMigration:         runtimeCfg.SkipSchemaMigration,
		},
	}
	return nil
}

func ApplyNetdConfig(ctx context.Context, c client.Client, infra *infrav1alpha1.Sandbox0Infra, clusterGatewayURL string, cfg *apiconfig.NetdConfig) error {
	if cfg == nil {
		return nil
	}
	runtimeCfg, ok, err := GetRuntimeConfig(ctx, c, infra)
	if err != nil {
		return err
	}
	if !ok || strings.TrimSpace(clusterGatewayURL) == "" {
		cfg.SandboxObservabilityIngestURL = ""
		return nil
	}
	cfg.SandboxObservabilityIngestURL = strings.TrimRight(clusterGatewayURL, "/") + "/internal/v1/sandbox-observability/events"
	applyIngestConfig(runtimeCfg.Ingest, cfg)
	return nil
}

func ApplyManagerConfig(ctx context.Context, c client.Client, infra *infrav1alpha1.Sandbox0Infra, clusterGatewayURL string, cfg *apiconfig.ManagerConfig) error {
	if cfg == nil {
		return nil
	}
	runtimeCfg, ok, err := GetRuntimeConfig(ctx, c, infra)
	if err != nil {
		return err
	}
	if !ok || strings.TrimSpace(clusterGatewayURL) == "" {
		cfg.SandboxObservabilityLogsIngestURL = ""
		return nil
	}
	base := strings.TrimRight(clusterGatewayURL, "/") + "/internal/v1/sandbox-observability"
	cfg.SandboxObservabilityLogsIngestURL = base + "/logs"
	applyManagerIngestConfig(runtimeCfg.Ingest, cfg)
	return nil
}

// ApplyCtldConfig injects the node-local runtime sample producer endpoint and
// bounded ingest settings into ctld's runtime configuration.
func ApplyCtldConfig(ctx context.Context, c client.Client, infra *infrav1alpha1.Sandbox0Infra, clusterGatewayURL string, cfg *apiconfig.CtldConfig) error {
	if cfg == nil {
		return nil
	}
	runtimeCfg, ok, err := GetRuntimeConfig(ctx, c, infra)
	if err != nil {
		return err
	}
	if !ok || strings.TrimSpace(clusterGatewayURL) == "" {
		cfg.SandboxObservabilityRuntimeSamplesIngestURL = ""
		return nil
	}
	cfg.SandboxObservabilityRuntimeSamplesIngestURL = strings.TrimRight(clusterGatewayURL, "/") + "/internal/v1/sandbox-observability/runtime-samples"
	applyCtldIngestConfig(runtimeCfg.Ingest, cfg)
	return nil
}

func GetRuntimeConfig(ctx context.Context, c client.Client, infra *infrav1alpha1.Sandbox0Infra) (RuntimeConfig, bool, error) {
	if infra == nil || !infrav1alpha1.IsSandboxObservabilityEnabled(infra) {
		return RuntimeConfig{}, false, nil
	}
	clickHouseCfg, ok, err := clickhousesvc.GetRuntimeConfig(ctx, c, infra)
	if err != nil || !ok {
		return RuntimeConfig{}, ok, err
	}
	cfg := RuntimeConfig{
		DSN:                         clickHouseCfg.DSN,
		Database:                    firstNonEmpty(clickHouseCfg.Databases.Observability, obsclickhouse.DefaultDatabase),
		EventsTable:                 obsclickhouse.DefaultEventsTable,
		LogsTable:                   obsclickhouse.DefaultLogsTable,
		RuntimeSamplesTable:         obsclickhouse.DefaultRuntimeSamplesTable,
		RetentionDays:               obsclickhouse.DefaultRetentionDays,
		LogsRetentionDays:           obsclickhouse.DefaultLogsRetentionDays,
		RuntimeSamplesRetentionDays: obsclickhouse.DefaultRuntimeSamplesRetentionDays,
		ConnectTimeout:              clickHouseCfg.ConnectTimeout,
		SkipSchemaMigration:         !clickHouseCfg.SchemaMigrationEnabled,
		Ingest:                      resolveIngestConfig(infra),
	}
	applyRetentionConfig(infra, &cfg)
	applyTableOverrides(infra, &cfg)
	return cfg, true, nil
}

func applyRetentionConfig(infra *infrav1alpha1.Sandbox0Infra, cfg *RuntimeConfig) {
	retention := infra.Spec.SandboxObservability.Retention
	if retention.AuditDays > 0 {
		cfg.RetentionDays = retention.AuditDays
	}
	if retention.LogDays > 0 {
		cfg.LogsRetentionDays = retention.LogDays
	}
	if retention.RuntimeSampleDays > 0 {
		cfg.RuntimeSamplesRetentionDays = retention.RuntimeSampleDays
	}
}

func applyTableOverrides(infra *infrav1alpha1.Sandbox0Infra, cfg *RuntimeConfig) {
	if infra == nil || infra.Spec.SandboxObservability == nil || cfg == nil {
		return
	}
	switch infra.Spec.SandboxObservability.Type {
	case infrav1alpha1.SandboxObservabilityTypeBuiltin:
		if infra.Spec.SandboxObservability.Builtin == nil {
			return
		}
		ch := infra.Spec.SandboxObservability.Builtin.ClickHouse
		cfg.Database = firstNonEmpty(ch.Database, cfg.Database)
		cfg.EventsTable = firstNonEmpty(ch.EventsTable, cfg.EventsTable)
		cfg.LogsTable = firstNonEmpty(ch.LogsTable, cfg.LogsTable)
		cfg.RuntimeSamplesTable = firstNonEmpty(ch.RuntimeSamplesTable, cfg.RuntimeSamplesTable)
	case infrav1alpha1.SandboxObservabilityTypeExternal:
		if infra.Spec.SandboxObservability.External == nil {
			return
		}
		ch := infra.Spec.SandboxObservability.External.ClickHouse
		cfg.Database = firstNonEmpty(ch.Database, cfg.Database)
		cfg.EventsTable = firstNonEmpty(ch.EventsTable, cfg.EventsTable)
		cfg.LogsTable = firstNonEmpty(ch.LogsTable, cfg.LogsTable)
		cfg.RuntimeSamplesTable = firstNonEmpty(ch.RuntimeSamplesTable, cfg.RuntimeSamplesTable)
		cfg.ConnectTimeout = ch.ConnectTimeout
		cfg.SkipSchemaMigration = ch.SkipSchemaMigration
	}
}

func applyIngestConfig(ingest infrav1alpha1.SandboxObservabilityIngestConfig, cfg *apiconfig.NetdConfig) {
	cfg.SandboxObservabilityIngestQueueSize = ingest.QueueSize
	cfg.SandboxObservabilityIngestBatchSize = ingest.BatchSize
	cfg.SandboxObservabilityIngestFlushInterval = ingest.FlushInterval
	cfg.SandboxObservabilityIngestRequestTimeout = ingest.RequestTimeout
	cfg.SandboxObservabilityIngestMaxRetries = ingest.MaxRetries
	cfg.SandboxObservabilityIngestRetryBackoff = ingest.RetryBackoff
}

func applyManagerIngestConfig(ingest infrav1alpha1.SandboxObservabilityIngestConfig, cfg *apiconfig.ManagerConfig) {
	cfg.SandboxObservabilityIngestQueueSize = ingest.QueueSize
	cfg.SandboxObservabilityIngestBatchSize = ingest.BatchSize
	cfg.SandboxObservabilityIngestFlushInterval = ingest.FlushInterval
	cfg.SandboxObservabilityIngestRequestTimeout = ingest.RequestTimeout
	cfg.SandboxObservabilityIngestMaxRetries = ingest.MaxRetries
	cfg.SandboxObservabilityIngestRetryBackoff = ingest.RetryBackoff
}

func applyCtldIngestConfig(ingest infrav1alpha1.SandboxObservabilityIngestConfig, cfg *apiconfig.CtldConfig) {
	cfg.SandboxObservabilityIngestQueueSize = ingest.QueueSize
	cfg.SandboxObservabilityIngestBatchSize = ingest.BatchSize
	cfg.SandboxObservabilityIngestFlushInterval = ingest.FlushInterval
	cfg.SandboxObservabilityIngestRequestTimeout = ingest.RequestTimeout
	cfg.SandboxObservabilityIngestMaxRetries = ingest.MaxRetries
	cfg.SandboxObservabilityIngestRetryBackoff = ingest.RetryBackoff
}

func resolveIngestConfig(infra *infrav1alpha1.Sandbox0Infra) infrav1alpha1.SandboxObservabilityIngestConfig {
	cfg := infrav1alpha1.SandboxObservabilityIngestConfig{}
	if infra != nil && infra.Spec.SandboxObservability != nil {
		cfg = infra.Spec.SandboxObservability.Ingest
	}
	return cfg
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

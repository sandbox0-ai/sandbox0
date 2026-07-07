package metering

import (
	"context"
	"fmt"

	apiconfig "github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	infrav1alpha1 "github.com/sandbox0-ai/sandbox0/infra-operator/api/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/infra-operator/internal/controller/pkg/common"
	clickhousesvc "github.com/sandbox0-ai/sandbox0/infra-operator/internal/controller/services/clickhouse"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	DefaultEventsTable       = "usage_events"
	DefaultWindowsTable      = "usage_windows"
	DefaultWatermarksTable   = "producer_watermarks"
	DefaultSandboxStateTable = "sandbox_projection_state"
	DefaultStorageStateTable = "storage_projection_state"
)

type Reconciler struct {
	Resources *common.ResourceManager
}

func NewReconciler(resources *common.ResourceManager) *Reconciler {
	return &Reconciler{Resources: resources}
}

func (r *Reconciler) Reconcile(ctx context.Context, infra *infrav1alpha1.Sandbox0Infra) error {
	cfg, ok, err := RuntimeConfig(ctx, r.Resources.Client, infra)
	if err != nil {
		return err
	}
	if !ok || !cfg.Enabled {
		return fmt.Errorf("metering requires an enabled clickhouse component")
	}
	return nil
}

func ApplyManagerConfig(ctx context.Context, c ctrlclient.Client, infra *infrav1alpha1.Sandbox0Infra, cfg *apiconfig.ManagerConfig) error {
	if cfg == nil {
		return nil
	}
	return apply(ctx, c, infra, &cfg.Metering)
}

func ApplyStorageProxyConfig(ctx context.Context, c ctrlclient.Client, infra *infrav1alpha1.Sandbox0Infra, cfg *apiconfig.StorageProxyConfig) error {
	if cfg == nil {
		return nil
	}
	return apply(ctx, c, infra, &cfg.Metering)
}

func ApplyNetdConfig(ctx context.Context, c ctrlclient.Client, infra *infrav1alpha1.Sandbox0Infra, cfg *apiconfig.NetdConfig) error {
	if cfg == nil {
		return nil
	}
	return apply(ctx, c, infra, &cfg.Metering)
}

func ApplyClusterGatewayConfig(ctx context.Context, c ctrlclient.Client, infra *infrav1alpha1.Sandbox0Infra, cfg *apiconfig.ClusterGatewayConfig) error {
	if cfg == nil {
		return nil
	}
	return apply(ctx, c, infra, &cfg.Metering)
}

func ApplyRegionalGatewayConfig(ctx context.Context, c ctrlclient.Client, infra *infrav1alpha1.Sandbox0Infra, cfg *apiconfig.RegionalGatewayConfig) error {
	if cfg == nil {
		return nil
	}
	return apply(ctx, c, infra, &cfg.Metering)
}

func apply(ctx context.Context, c ctrlclient.Client, infra *infrav1alpha1.Sandbox0Infra, dst *apiconfig.MeteringConfig) error {
	cfg, _, err := RuntimeConfig(ctx, c, infra)
	if err != nil {
		return err
	}
	*dst = cfg
	return nil
}

func RuntimeConfig(ctx context.Context, c ctrlclient.Client, infra *infrav1alpha1.Sandbox0Infra) (apiconfig.MeteringConfig, bool, error) {
	if infra == nil || !infrav1alpha1.IsMeteringEnabled(infra) {
		return apiconfig.MeteringConfig{}, false, nil
	}
	clickHouseCfg, ok, err := clickhousesvc.GetRuntimeConfig(ctx, c, infra)
	if err != nil || !ok {
		return apiconfig.MeteringConfig{}, ok, err
	}
	spec := infra.Spec.Metering.ClickHouse
	return apiconfig.MeteringConfig{
		Enabled: true,
		ClickHouse: apiconfig.MeteringClickHouseConfig{
			DSN:                 clickHouseCfg.DSN,
			Database:            firstNonEmpty(clickHouseCfg.Databases.Metering, clickhousesvc.DefaultMeteringDB),
			EventsTable:         firstNonEmpty(spec.EventsTable, DefaultEventsTable),
			WindowsTable:        firstNonEmpty(spec.WindowsTable, DefaultWindowsTable),
			WatermarksTable:     firstNonEmpty(spec.WatermarksTable, DefaultWatermarksTable),
			SandboxStateTable:   firstNonEmpty(spec.SandboxStateTable, DefaultSandboxStateTable),
			StorageStateTable:   firstNonEmpty(spec.StorageStateTable, DefaultStorageStateTable),
			ConnectTimeout:      clickHouseCfg.ConnectTimeout,
			SkipSchemaMigration: !clickHouseCfg.SchemaMigrationEnabled,
		},
	}, true, nil
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

package sandboxobservability

import (
	"context"
	"testing"
	"time"

	apiconfig "github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	infrav1alpha1 "github.com/sandbox0-ai/sandbox0/infra-operator/api/v1alpha1"
	sandboxobstypes "github.com/sandbox0-ai/sandbox0/pkg/sandboxobservability"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

type sandboxObservabilityIngestSettings struct {
	url            string
	queueSize      int
	batchSize      int
	flushInterval  time.Duration
	requestTimeout time.Duration
	maxRetries     int
	retryBackoff   time.Duration
}

func newExternalSandboxObservabilityInfra(settings sandboxObservabilityIngestSettings) *infrav1alpha1.Sandbox0Infra {
	return &infrav1alpha1.Sandbox0Infra{
		ObjectMeta: metav1.ObjectMeta{Name: "sandbox0", Namespace: "sandbox0-system"},
		Spec: infrav1alpha1.Sandbox0InfraSpec{
			SandboxObservability: &infrav1alpha1.SandboxObservabilityConfig{
				Type: infrav1alpha1.SandboxObservabilityTypeExternal,
				External: &infrav1alpha1.ExternalSandboxObservabilityConfig{
					ClickHouse: infrav1alpha1.ExternalSandboxObservabilityClickHouseConfig{
						DSNSecret: infrav1alpha1.SandboxObservabilityClickHouseDSNSecretRef{Name: "sandbox-observability-dsn"},
					},
				},
				Ingest: infrav1alpha1.SandboxObservabilityIngestConfig{
					QueueSize:      settings.queueSize,
					BatchSize:      settings.batchSize,
					FlushInterval:  metav1.Duration{Duration: settings.flushInterval},
					RequestTimeout: metav1.Duration{Duration: settings.requestTimeout},
					MaxRetries:     settings.maxRetries,
					RetryBackoff:   metav1.Duration{Duration: settings.retryBackoff},
				},
			},
		},
	}
}

func newExternalSandboxObservabilityTestClient(t *testing.T, infra *infrav1alpha1.Sandbox0Infra) ctrlclient.Client {
	t.Helper()
	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))
	require.NoError(t, infrav1alpha1.AddToScheme(scheme))
	return fake.NewClientBuilder().WithScheme(scheme).WithObjects(
		infra,
		&corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: "sandbox-observability-dsn", Namespace: "sandbox0-system"},
			Data:       map[string][]byte{"dsn": []byte("clickhouse://sandbox0:password@clickhouse:9000/sandbox0_observability")},
		},
	).Build()
}

func TestApplyRuntimeConfigInjectsIngestURL(t *testing.T) {
	for _, tt := range []struct {
		name     string
		settings sandboxObservabilityIngestSettings
		apply    func(context.Context, ctrlclient.Client, *infrav1alpha1.Sandbox0Infra) (sandboxObservabilityIngestSettings, error)
	}{
		{
			name: "manager logs",
			settings: sandboxObservabilityIngestSettings{
				url: "http://cluster-gateway.svc/internal/v1/sandbox-observability/logs", queueSize: 11, batchSize: 7,
				flushInterval: 2 * time.Second, requestTimeout: 3 * time.Second, maxRetries: 5, retryBackoff: 250 * time.Millisecond,
			},
			apply: func(ctx context.Context, client ctrlclient.Client, infra *infrav1alpha1.Sandbox0Infra) (sandboxObservabilityIngestSettings, error) {
				cfg := &apiconfig.ManagerConfig{}
				err := ApplyManagerConfig(ctx, client, infra, "http://cluster-gateway.svc/", cfg)
				return sandboxObservabilityIngestSettings{
					url: cfg.SandboxObservabilityLogsIngestURL, queueSize: cfg.SandboxObservabilityIngestQueueSize, batchSize: cfg.SandboxObservabilityIngestBatchSize,
					flushInterval: cfg.SandboxObservabilityIngestFlushInterval.Duration, requestTimeout: cfg.SandboxObservabilityIngestRequestTimeout.Duration,
					maxRetries: cfg.SandboxObservabilityIngestMaxRetries, retryBackoff: cfg.SandboxObservabilityIngestRetryBackoff.Duration,
				}, err
			},
		},
		{
			name: "ctld runtime samples",
			settings: sandboxObservabilityIngestSettings{
				url: "http://cluster-gateway.svc/internal/v1/sandbox-observability/runtime-samples", queueSize: 23, batchSize: 9,
				flushInterval: 2 * time.Second, requestTimeout: 3 * time.Second, maxRetries: 4, retryBackoff: 250 * time.Millisecond,
			},
			apply: func(ctx context.Context, client ctrlclient.Client, infra *infrav1alpha1.Sandbox0Infra) (sandboxObservabilityIngestSettings, error) {
				cfg := &apiconfig.CtldConfig{}
				err := ApplyCtldConfig(ctx, client, infra, "http://cluster-gateway.svc/", cfg)
				return sandboxObservabilityIngestSettings{
					url: cfg.SandboxObservabilityRuntimeSamplesIngestURL, queueSize: cfg.SandboxObservabilityIngestQueueSize, batchSize: cfg.SandboxObservabilityIngestBatchSize,
					flushInterval: cfg.SandboxObservabilityIngestFlushInterval.Duration, requestTimeout: cfg.SandboxObservabilityIngestRequestTimeout.Duration,
					maxRetries: cfg.SandboxObservabilityIngestMaxRetries, retryBackoff: cfg.SandboxObservabilityIngestRetryBackoff.Duration,
				}, err
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			infra := newExternalSandboxObservabilityInfra(tt.settings)
			got, err := tt.apply(context.Background(), newExternalSandboxObservabilityTestClient(t, infra), infra)

			require.NoError(t, err)
			assert.Equal(t, tt.settings, got)
		})
	}
}

func TestApplyNetworkRuntimeConfigInjectsAuditIngestURLOnlyWhenLicensedAuditIsEnabled(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))
	require.NoError(t, infrav1alpha1.AddToScheme(scheme))

	enabled := true
	infra := &infrav1alpha1.Sandbox0Infra{
		ObjectMeta: metav1.ObjectMeta{Name: "sandbox0", Namespace: "sandbox0-system"},
		Spec: infrav1alpha1.Sandbox0InfraSpec{
			ClickHouse: &infrav1alpha1.ClickHouseConfig{
				Type: infrav1alpha1.ClickHouseTypeExternal,
				External: &infrav1alpha1.ExternalClickHouseConfig{
					DSNSecret: infrav1alpha1.ClickHouseDSNSecretRef{Name: "clickhouse-dsn"},
				},
			},
			SandboxObservability: &infrav1alpha1.SandboxObservabilityConfig{
				Enabled: &enabled,
				Backend: infrav1alpha1.SandboxObservabilityBackendClickHouse,
				Type:    infrav1alpha1.SandboxObservabilityTypeExternal,
				Audit: &infrav1alpha1.SandboxObservabilityAuditConfig{
					Enabled:      true,
					DeliveryMode: sandboxobstypes.AuditDeliveryModeCanonicalSync,
				},
			},
		},
	}
	client := fake.NewClientBuilder().WithScheme(scheme).WithObjects(
		infra,
		&corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: "clickhouse-dsn", Namespace: infra.Namespace},
			Data:       map[string][]byte{"dsn": []byte("clickhouse://sandbox0:password@clickhouse:9000/default")},
		},
	).Build()
	cfg := &apiconfig.NetworkRuntimeConfig{}

	err := ApplyNetworkRuntimeConfig(context.Background(), client, infra, "http://cluster-gateway.svc/", cfg)

	require.NoError(t, err)
	assert.Equal(t, "http://cluster-gateway.svc/internal/v1/sandbox-observability/events", cfg.SandboxObservabilityIngestURL)
	assert.Equal(t, sandboxobstypes.AuditDeliveryModeCanonicalSync, cfg.SandboxObservabilityAuditDeliveryMode)

	infra.Spec.SandboxObservability.Audit.Enabled = false
	err = ApplyNetworkRuntimeConfig(context.Background(), client, infra, "http://cluster-gateway.svc/", cfg)
	require.NoError(t, err)
	assert.Empty(t, cfg.SandboxObservabilityIngestURL)
	assert.Empty(t, cfg.SandboxObservabilityAuditSpoolDir)
	assert.Empty(t, cfg.SandboxObservabilityAuditDeliveryMode)
}

func TestApplyManagerConfigClearsIngestURLsWhenDisabled(t *testing.T) {
	cfg := &apiconfig.ManagerConfig{
		SandboxObservabilityLogsIngestURL: "http://old/logs",
	}
	infra := &infrav1alpha1.Sandbox0Infra{
		Spec: infrav1alpha1.Sandbox0InfraSpec{
			SandboxObservability: &infrav1alpha1.SandboxObservabilityConfig{Type: infrav1alpha1.SandboxObservabilityTypeDisabled},
		},
	}

	err := ApplyManagerConfig(context.Background(), nil, infra, "http://cluster-gateway.svc", cfg)

	require.NoError(t, err)
	assert.Empty(t, cfg.SandboxObservabilityLogsIngestURL)
}

func TestApplyCtldConfigClearsIngestURLWhenDisabled(t *testing.T) {
	cfg := &apiconfig.CtldConfig{SandboxObservabilityRuntimeSamplesIngestURL: "http://old/runtime-samples"}
	infra := &infrav1alpha1.Sandbox0Infra{
		Spec: infrav1alpha1.Sandbox0InfraSpec{
			SandboxObservability: &infrav1alpha1.SandboxObservabilityConfig{Type: infrav1alpha1.SandboxObservabilityTypeDisabled},
		},
	}

	err := ApplyCtldConfig(context.Background(), nil, infra, "http://cluster-gateway.svc", cfg)

	require.NoError(t, err)
	assert.Empty(t, cfg.SandboxObservabilityRuntimeSamplesIngestURL)
}

func TestGetRuntimeConfigUsesRegionClickHouse(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))
	require.NoError(t, infrav1alpha1.AddToScheme(scheme))

	enabled := true
	infra := &infrav1alpha1.Sandbox0Infra{
		ObjectMeta: metav1.ObjectMeta{Name: "sandbox0", Namespace: "sandbox0-system"},
		Spec: infrav1alpha1.Sandbox0InfraSpec{
			ClickHouse: &infrav1alpha1.ClickHouseConfig{
				Type: infrav1alpha1.ClickHouseTypeExternal,
				External: &infrav1alpha1.ExternalClickHouseConfig{
					DSNSecret: infrav1alpha1.ClickHouseDSNSecretRef{Name: "clickhouse-dsn"},
				},
				Databases: infrav1alpha1.ClickHouseDatabaseConfig{
					Observability: "sandbox0_obs",
				},
			},
			SandboxObservability: &infrav1alpha1.SandboxObservabilityConfig{
				Enabled: &enabled,
				Backend: infrav1alpha1.SandboxObservabilityBackendClickHouse,
			},
		},
	}
	client := fake.NewClientBuilder().WithScheme(scheme).WithObjects(
		infra,
		&corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: "clickhouse-dsn", Namespace: "sandbox0-system"},
			Data:       map[string][]byte{"dsn": []byte("clickhouse://sandbox0:password@clickhouse:9000/sandbox0_obs")},
		},
	).Build()

	cfg, ok, err := GetRuntimeConfig(context.Background(), client, infra)

	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, "clickhouse://sandbox0:password@clickhouse:9000/sandbox0_obs", cfg.DSN)
	assert.Equal(t, "sandbox0_obs", cfg.Database)
	assert.Equal(t, "sandbox_audit_events", cfg.EventsTable)
	assert.Equal(t, sandboxobstypes.AuditDeliveryModeDurableAsync, cfg.AuditDeliveryMode)
}

func TestGetRuntimeConfigMovesPersistedLegacyAuditDefaultToCanonicalTable(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))
	require.NoError(t, infrav1alpha1.AddToScheme(scheme))
	enabled := true
	infra := &infrav1alpha1.Sandbox0Infra{
		ObjectMeta: metav1.ObjectMeta{Name: "sandbox0", Namespace: "sandbox0-system"},
		Spec: infrav1alpha1.Sandbox0InfraSpec{
			ClickHouse: &infrav1alpha1.ClickHouseConfig{
				Type: infrav1alpha1.ClickHouseTypeExternal,
				External: &infrav1alpha1.ExternalClickHouseConfig{
					DSNSecret: infrav1alpha1.ClickHouseDSNSecretRef{Name: "clickhouse-dsn"},
				},
			},
			SandboxObservability: &infrav1alpha1.SandboxObservabilityConfig{
				Enabled: &enabled,
				Backend: infrav1alpha1.SandboxObservabilityBackendClickHouse,
				Type:    infrav1alpha1.SandboxObservabilityTypeExternal,
				Audit:   &infrav1alpha1.SandboxObservabilityAuditConfig{Enabled: true},
				External: &infrav1alpha1.ExternalSandboxObservabilityConfig{
					ClickHouse: infrav1alpha1.ExternalSandboxObservabilityClickHouseConfig{
						DSNSecret:   infrav1alpha1.SandboxObservabilityClickHouseDSNSecretRef{Name: "clickhouse-dsn"},
						EventsTable: "sandbox_events",
					},
				},
			},
		},
	}
	client := fake.NewClientBuilder().WithScheme(scheme).WithObjects(
		infra,
		&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "clickhouse-dsn", Namespace: infra.Namespace}, Data: map[string][]byte{"dsn": []byte("clickhouse://sandbox0@clickhouse:9000/default")}},
	).Build()

	cfg, ok, err := GetRuntimeConfig(context.Background(), client, infra)
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, "sandbox_audit_events", cfg.EventsTable)
}

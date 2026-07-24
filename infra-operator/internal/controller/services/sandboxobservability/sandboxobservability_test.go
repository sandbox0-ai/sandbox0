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
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestApplyManagerConfigInjectsLogsIngestURL(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))
	require.NoError(t, infrav1alpha1.AddToScheme(scheme))

	infra := &infrav1alpha1.Sandbox0Infra{
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
					QueueSize:      11,
					BatchSize:      7,
					FlushInterval:  metav1.Duration{Duration: 2 * time.Second},
					RequestTimeout: metav1.Duration{Duration: 3 * time.Second},
					MaxRetries:     5,
					RetryBackoff:   metav1.Duration{Duration: 250 * time.Millisecond},
				},
			},
		},
	}
	client := fake.NewClientBuilder().WithScheme(scheme).WithObjects(
		infra,
		&corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: "sandbox-observability-dsn", Namespace: "sandbox0-system"},
			Data:       map[string][]byte{"dsn": []byte("clickhouse://sandbox0:password@clickhouse:9000/sandbox0_observability")},
		},
	).Build()
	cfg := &apiconfig.ManagerConfig{}

	err := ApplyManagerConfig(context.Background(), client, infra, "http://cluster-gateway.svc/", cfg)

	require.NoError(t, err)
	assert.Equal(t, "http://cluster-gateway.svc/internal/v1/sandbox-observability/logs", cfg.SandboxObservabilityLogsIngestURL)
	assert.Equal(t, 11, cfg.SandboxObservabilityIngestQueueSize)
	assert.Equal(t, 7, cfg.SandboxObservabilityIngestBatchSize)
	assert.Equal(t, 2*time.Second, cfg.SandboxObservabilityIngestFlushInterval.Duration)
	assert.Equal(t, 3*time.Second, cfg.SandboxObservabilityIngestRequestTimeout.Duration)
	assert.Equal(t, 5, cfg.SandboxObservabilityIngestMaxRetries)
	assert.Equal(t, 250*time.Millisecond, cfg.SandboxObservabilityIngestRetryBackoff.Duration)
}

func TestApplyNetdConfigInjectsAuditIngestURLOnlyWhenLicensedAuditIsEnabled(t *testing.T) {
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
	cfg := &apiconfig.NetdConfig{}

	err := ApplyNetdConfig(context.Background(), client, infra, "http://cluster-gateway.svc/", cfg)

	require.NoError(t, err)
	assert.Equal(t, "http://cluster-gateway.svc/internal/v1/sandbox-observability/events", cfg.SandboxObservabilityIngestURL)
	assert.Equal(t, sandboxobstypes.AuditDeliveryModeCanonicalSync, cfg.SandboxObservabilityAuditDeliveryMode)

	infra.Spec.SandboxObservability.Audit.Enabled = false
	err = ApplyNetdConfig(context.Background(), client, infra, "http://cluster-gateway.svc/", cfg)
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

func TestApplyCtldConfigInjectsRuntimeSamplesIngestURL(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))
	require.NoError(t, infrav1alpha1.AddToScheme(scheme))

	infra := &infrav1alpha1.Sandbox0Infra{
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
					QueueSize:      23,
					BatchSize:      9,
					FlushInterval:  metav1.Duration{Duration: 2 * time.Second},
					RequestTimeout: metav1.Duration{Duration: 3 * time.Second},
					MaxRetries:     4,
					RetryBackoff:   metav1.Duration{Duration: 250 * time.Millisecond},
				},
			},
		},
	}
	client := fake.NewClientBuilder().WithScheme(scheme).WithObjects(
		infra,
		&corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: "sandbox-observability-dsn", Namespace: "sandbox0-system"},
			Data:       map[string][]byte{"dsn": []byte("clickhouse://sandbox0:password@clickhouse:9000/sandbox0_observability")},
		},
	).Build()
	cfg := &apiconfig.CtldConfig{}

	err := ApplyCtldConfig(context.Background(), client, infra, "http://cluster-gateway.svc/", cfg)

	require.NoError(t, err)
	assert.Equal(t, "http://cluster-gateway.svc/internal/v1/sandbox-observability/runtime-samples", cfg.SandboxObservabilityRuntimeSamplesIngestURL)
	assert.Equal(t, 23, cfg.SandboxObservabilityIngestQueueSize)
	assert.Equal(t, 9, cfg.SandboxObservabilityIngestBatchSize)
	assert.Equal(t, 2*time.Second, cfg.SandboxObservabilityIngestFlushInterval.Duration)
	assert.Equal(t, 3*time.Second, cfg.SandboxObservabilityIngestRequestTimeout.Duration)
	assert.Equal(t, 4, cfg.SandboxObservabilityIngestMaxRetries)
	assert.Equal(t, 250*time.Millisecond, cfg.SandboxObservabilityIngestRetryBackoff.Duration)
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
				Audit: &infrav1alpha1.SandboxObservabilityAuditConfig{
					Enabled:       true,
					StoragePolicy: "audit_hot_s3",
				},
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
	assert.Equal(t, "audit_hot_s3", cfg.EventsStoragePolicy)
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

package clickhouse

import (
	"context"
	"testing"
	"time"

	infrav1alpha1 "github.com/sandbox0-ai/sandbox0/infra-operator/api/v1alpha1"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestGetRuntimeConfigUsesTopLevelExternalClickHouse(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))
	require.NoError(t, infrav1alpha1.AddToScheme(scheme))

	infra := &infrav1alpha1.Sandbox0Infra{
		ObjectMeta: metav1.ObjectMeta{Name: "sandbox0", Namespace: "sandbox0-system"},
		Spec: infrav1alpha1.Sandbox0InfraSpec{
			ClickHouse: &infrav1alpha1.ClickHouseConfig{
				Type: infrav1alpha1.ClickHouseTypeExternal,
				External: &infrav1alpha1.ExternalClickHouseConfig{
					DSNSecret:      infrav1alpha1.ClickHouseDSNSecretRef{Name: "clickhouse-dsn"},
					ConnectTimeout: metav1.Duration{Duration: 3 * time.Second},
				},
				Databases: infrav1alpha1.ClickHouseDatabaseConfig{
					Observability: "sandbox0_obs",
					Metering:      "sandbox0_usage",
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
	require.Equal(t, "clickhouse://sandbox0:password@clickhouse:9000/sandbox0_obs", cfg.DSN)
	require.Equal(t, "sandbox0_obs", cfg.Databases.Observability)
	require.Equal(t, "sandbox0_usage", cfg.Databases.Metering)
	require.Equal(t, 3*time.Second, cfg.ConnectTimeout.Duration)
	require.True(t, cfg.SchemaMigrationEnabled)
}

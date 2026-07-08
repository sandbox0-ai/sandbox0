package clickhouse

import (
	"context"
	"strings"
	"testing"
	"time"

	infrav1alpha1 "github.com/sandbox0-ai/sandbox0/infra-operator/api/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/infra-operator/internal/controller/pkg/common"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
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

func TestReconcileBuiltinSecretUsesDefaultConnectionDatabase(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))
	require.NoError(t, infrav1alpha1.AddToScheme(scheme))

	infra := &infrav1alpha1.Sandbox0Infra{
		ObjectMeta: metav1.ObjectMeta{Name: "sandbox0", Namespace: "sandbox0-system"},
		Spec: infrav1alpha1.Sandbox0InfraSpec{
			ClickHouse: &infrav1alpha1.ClickHouseConfig{
				Type: infrav1alpha1.ClickHouseTypeBuiltin,
				Databases: infrav1alpha1.ClickHouseDatabaseConfig{
					Observability: "sandbox0_obs",
					Metering:      "sandbox0_usage",
				},
			},
		},
	}
	client := fake.NewClientBuilder().WithScheme(scheme).WithObjects(infra).Build()
	reconciler := NewReconciler(common.NewResourceManager(client, scheme, nil, common.LocalDevConfig{}))

	require.NoError(t, reconciler.reconcileBuiltinSecret(context.Background(), infra, resolvedConfig{
		Type: infrav1alpha1.ClickHouseTypeBuiltin,
		Databases: infrav1alpha1.ClickHouseDatabaseConfig{
			Observability: "sandbox0_obs",
			Metering:      "sandbox0_usage",
		},
	}))

	secret := &corev1.Secret{}
	require.NoError(t, client.Get(context.Background(), types.NamespacedName{Name: BuiltinSecretName(infra), Namespace: infra.Namespace}, secret))
	require.Equal(t, "default", string(secret.Data["database"]))
	require.Equal(t, "sandbox0_obs", string(secret.Data["observability_database"]))
	require.Equal(t, "sandbox0_usage", string(secret.Data["metering_database"]))
	require.True(t, strings.HasSuffix(string(secret.Data["dsn"]), "/default"), string(secret.Data["dsn"]))
}

func TestReconcileBuiltinSecretUpdatesLegacyObservabilityDSN(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))
	require.NoError(t, infrav1alpha1.AddToScheme(scheme))

	infra := &infrav1alpha1.Sandbox0Infra{
		ObjectMeta: metav1.ObjectMeta{Name: "sandbox0", Namespace: "sandbox0-system"},
	}
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: BuiltinSecretName(infra), Namespace: infra.Namespace},
		Data: map[string][]byte{
			"username":               []byte("sandbox0"),
			"password":               []byte("existing-password"),
			"database":               []byte("sandbox0_observability"),
			"observability_database": []byte("sandbox0_observability"),
			"metering_database":      []byte("sandbox0_metering"),
			"host":                   []byte(builtinHost(infra)),
			"port":                   []byte("9000"),
			"dsn":                    []byte("clickhouse://sandbox0:existing-password@" + builtinHost(infra) + ":9000/sandbox0_observability"),
		},
	}
	client := fake.NewClientBuilder().WithScheme(scheme).WithObjects(infra, secret).Build()
	reconciler := NewReconciler(common.NewResourceManager(client, scheme, nil, common.LocalDevConfig{}))

	require.NoError(t, reconciler.reconcileBuiltinSecret(context.Background(), infra, resolvedConfig{
		Type: infrav1alpha1.ClickHouseTypeBuiltin,
	}))

	updated := &corev1.Secret{}
	require.NoError(t, client.Get(context.Background(), types.NamespacedName{Name: BuiltinSecretName(infra), Namespace: infra.Namespace}, updated))
	require.Equal(t, "existing-password", string(updated.Data["password"]))
	require.Equal(t, "default", string(updated.Data["database"]))
	require.True(t, strings.HasSuffix(string(updated.Data["dsn"]), "/default"), string(updated.Data["dsn"]))
}

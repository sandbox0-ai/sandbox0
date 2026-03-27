package controller

import (
	"context"
	"reflect"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	infrav1alpha1 "github.com/sandbox0-ai/sandbox0/infra-operator/api/v1alpha1"
)

func TestServiceConfigCapabilitiesExposeOnlySupportedFields(t *testing.T) {
	t.Run("netd schema omits unsupported workload fields", func(t *testing.T) {
		typ := reflect.TypeOf(infrav1alpha1.NetdServiceConfig{})
		for _, fieldName := range []string{"Replicas", "Resources", "Service", "Ingress"} {
			if _, ok := typ.FieldByName(fieldName); ok {
				t.Fatalf("expected NetdServiceConfig to omit field %q", fieldName)
			}
		}
		if _, ok := typ.FieldByName("Enabled"); !ok {
			t.Fatal("expected NetdServiceConfig to expose Enabled")
		}
	})

	t.Run("scheduler and dataplane services omit ingress", func(t *testing.T) {
		for name, typ := range map[string]reflect.Type{
			"scheduler":      reflect.TypeOf(infrav1alpha1.SchedulerServiceConfig{}),
			"clusterGateway": reflect.TypeOf(infrav1alpha1.ClusterGatewayServiceConfig{}),
			"manager":        reflect.TypeOf(infrav1alpha1.ManagerServiceConfig{}),
			"storageProxy":   reflect.TypeOf(infrav1alpha1.StorageProxyServiceConfig{}),
		} {
			if _, ok := typ.FieldByName("Ingress"); ok {
				t.Fatalf("expected %s service config to omit Ingress", name)
			}
			for _, supported := range []string{"Enabled", "Replicas", "Resources", "Service"} {
				if _, ok := typ.FieldByName(supported); !ok {
					t.Fatalf("expected %s service config to expose %s", name, supported)
				}
			}
		}
	})

	t.Run("gateway services retain ingress", func(t *testing.T) {
		for name, typ := range map[string]reflect.Type{
			"globalGateway":   reflect.TypeOf(infrav1alpha1.GlobalGatewayServiceConfig{}),
			"regionalGateway": reflect.TypeOf(infrav1alpha1.RegionalGatewayServiceConfig{}),
		} {
			if _, ok := typ.FieldByName("Ingress"); !ok {
				t.Fatalf("expected %s service config to expose Ingress", name)
			}
		}
	})
}

func TestValidateSpecSemanticsRejectsDisabledBuiltinPersistence(t *testing.T) {
	infra := &infrav1alpha1.Sandbox0Infra{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "sandbox0-system"},
		Spec: infrav1alpha1.Sandbox0InfraSpec{
			Database: &infrav1alpha1.DatabaseConfig{
				Type: infrav1alpha1.DatabaseTypeBuiltin,
				Builtin: &infrav1alpha1.BuiltinDatabaseConfig{
					Persistence: &infrav1alpha1.PersistenceConfig{Enabled: false},
				},
			},
			Storage: &infrav1alpha1.StorageConfig{
				Type: infrav1alpha1.StorageTypeBuiltin,
				Builtin: &infrav1alpha1.BuiltinStorageConfig{
					Persistence: &infrav1alpha1.PersistenceConfig{Enabled: false},
				},
			},
			Registry: &infrav1alpha1.RegistryConfig{
				Provider: infrav1alpha1.RegistryProviderBuiltin,
				Builtin: &infrav1alpha1.BuiltinRegistryConfig{
					Persistence: &infrav1alpha1.PersistenceConfig{Enabled: false},
				},
			},
		},
	}

	err := validateSpecSemantics(context.Background(), newValidationTestClient(t), infra)
	if err == nil {
		t.Fatal("expected validation error")
	}

	message := err.Error()
	for _, want := range []string{
		"spec.database.builtin.persistence.enabled=false is not supported",
		"spec.storage.builtin.persistence.enabled=false is not supported",
		"spec.registry.builtin.persistence.enabled=false is not supported",
	} {
		if !strings.Contains(message, want) {
			t.Fatalf("expected validation message %q in %q", want, message)
		}
	}
}

func TestValidateSpecSemanticsRejectsBuiltinDatabaseCreateOnceChanges(t *testing.T) {
	infra := &infrav1alpha1.Sandbox0Infra{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "sandbox0-system"},
		Spec: infrav1alpha1.Sandbox0InfraSpec{
			Database: &infrav1alpha1.DatabaseConfig{
				Type: infrav1alpha1.DatabaseTypeBuiltin,
				Builtin: &infrav1alpha1.BuiltinDatabaseConfig{
					Username: "new-user",
					Database: "new-db",
					Port:     5433,
					Persistence: &infrav1alpha1.PersistenceConfig{
						Enabled:      true,
						Size:         resource.MustParse("20Gi"),
						StorageClass: "slow",
					},
				},
			},
		},
	}

	client := newValidationTestClient(t,
		&corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "demo-sandbox0-database-credentials",
				Namespace: "sandbox0-system",
			},
			Data: map[string][]byte{
				"username": []byte("old-user"),
				"database": []byte("old-db"),
				"port":     []byte("5432"),
			},
		},
		&corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "demo-postgres-data",
				Namespace: "sandbox0-system",
			},
			Spec: corev1.PersistentVolumeClaimSpec{
				Resources: corev1.VolumeResourceRequirements{
					Requests: corev1.ResourceList{
						corev1.ResourceStorage: resource.MustParse("10Gi"),
					},
				},
				StorageClassName: stringPtr("fast"),
			},
		},
	)

	err := validateSpecSemantics(context.Background(), client, infra)
	if err == nil {
		t.Fatal("expected validation error")
	}

	message := err.Error()
	for _, want := range []string{
		"spec.database.builtin.username cannot be changed",
		"spec.database.builtin.database cannot be changed",
		"spec.database.builtin.port cannot be changed",
		"spec.database.builtin.persistence.size cannot be changed",
		"spec.database.builtin.persistence.storageClass cannot be changed",
	} {
		if !strings.Contains(message, want) {
			t.Fatalf("expected validation message %q in %q", want, message)
		}
	}
}

func TestValidateSpecSemanticsRejectsBuiltinStorageCreateOnceChanges(t *testing.T) {
	infra := &infrav1alpha1.Sandbox0Infra{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "sandbox0-system"},
		Spec: infrav1alpha1.Sandbox0InfraSpec{
			Storage: &infrav1alpha1.StorageConfig{
				Type: infrav1alpha1.StorageTypeBuiltin,
				Builtin: &infrav1alpha1.BuiltinStorageConfig{
					Persistence: &infrav1alpha1.PersistenceConfig{
						Enabled:      true,
						Size:         resource.MustParse("100Gi"),
						StorageClass: "slow",
					},
					Credentials: &infrav1alpha1.StorageCredentials{
						AccessKey: "new-access",
						SecretKey: "new-secret",
					},
				},
			},
		},
	}

	client := newValidationTestClient(t,
		&corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "demo-sandbox0-rustfs-credentials",
				Namespace: "sandbox0-system",
			},
			Data: map[string][]byte{
				"RUSTFS_ACCESS_KEY": []byte("old-access"),
				"RUSTFS_SECRET_KEY": []byte("old-secret"),
			},
		},
		&corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "demo-rustfs-data",
				Namespace: "sandbox0-system",
			},
			Spec: corev1.PersistentVolumeClaimSpec{
				Resources: corev1.VolumeResourceRequirements{
					Requests: corev1.ResourceList{
						corev1.ResourceStorage: resource.MustParse("50Gi"),
					},
				},
				StorageClassName: stringPtr("fast"),
			},
		},
	)

	err := validateSpecSemantics(context.Background(), client, infra)
	if err == nil {
		t.Fatal("expected validation error")
	}

	message := err.Error()
	for _, want := range []string{
		"spec.storage.builtin.credentials.accessKey cannot be changed",
		"spec.storage.builtin.credentials.secretKey cannot be changed",
		"spec.storage.builtin.persistence.size cannot be changed",
		"spec.storage.builtin.persistence.storageClass cannot be changed",
	} {
		if !strings.Contains(message, want) {
			t.Fatalf("expected validation message %q in %q", want, message)
		}
	}
}

func TestValidateSpecSemanticsRejectsBuiltinRegistryPVCChanges(t *testing.T) {
	infra := &infrav1alpha1.Sandbox0Infra{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "sandbox0-system"},
		Spec: infrav1alpha1.Sandbox0InfraSpec{
			Registry: &infrav1alpha1.RegistryConfig{
				Provider: infrav1alpha1.RegistryProviderBuiltin,
				Builtin: &infrav1alpha1.BuiltinRegistryConfig{
					Persistence: &infrav1alpha1.PersistenceConfig{
						Enabled:      true,
						Size:         resource.MustParse("40Gi"),
						StorageClass: "slow",
					},
				},
			},
		},
	}

	client := newValidationTestClient(t,
		&corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "demo-registry-data",
				Namespace: "sandbox0-system",
			},
			Spec: corev1.PersistentVolumeClaimSpec{
				Resources: corev1.VolumeResourceRequirements{
					Requests: corev1.ResourceList{
						corev1.ResourceStorage: resource.MustParse("20Gi"),
					},
				},
				StorageClassName: stringPtr("fast"),
			},
		},
	)

	err := validateSpecSemantics(context.Background(), client, infra)
	if err == nil {
		t.Fatal("expected validation error")
	}

	message := err.Error()
	for _, want := range []string{
		"spec.registry.builtin.persistence.size cannot be changed",
		"spec.registry.builtin.persistence.storageClass cannot be changed",
	} {
		if !strings.Contains(message, want) {
			t.Fatalf("expected validation message %q in %q", want, message)
		}
	}
}

func newValidationTestClient(t *testing.T, objects ...ctrlclient.Object) ctrlclient.Client {
	t.Helper()

	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("add corev1 scheme: %v", err)
	}

	return fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(objects...).
		Build()
}

func stringPtr(value string) *string {
	return &value
}

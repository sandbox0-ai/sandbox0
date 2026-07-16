package controller

import (
	"context"
	"os"
	"path/filepath"
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
	t.Run("scheduler and dataplane services omit ingress", func(t *testing.T) {
		for name, typ := range map[string]reflect.Type{
			"scheduler":      reflect.TypeOf(infrav1alpha1.SchedulerServiceConfig{}),
			"clusterGateway": reflect.TypeOf(infrav1alpha1.ClusterGatewayServiceConfig{}),
			"manager":        reflect.TypeOf(infrav1alpha1.ManagerServiceConfig{}),
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

	t.Run("persistence schema omits enabled flag", func(t *testing.T) {
		typ := reflect.TypeOf(infrav1alpha1.PersistenceConfig{})
		if _, ok := typ.FieldByName("Enabled"); ok {
			t.Fatal("expected PersistenceConfig to omit Enabled")
		}
		for _, supported := range []string{"Size", "StorageClass"} {
			if _, ok := typ.FieldByName(supported); !ok {
				t.Fatalf("expected PersistenceConfig to expose %s", supported)
			}
		}
	})
}

func TestGeneratedCRDIncludesCreateOncePresenceValidations(t *testing.T) {
	crdPath := filepath.Join("..", "..", "chart", "crds", "infra.sandbox0.ai_sandbox0infras.yaml")
	content, err := os.ReadFile(crdPath)
	if err != nil {
		t.Fatalf("read generated CRD: %v", err)
	}

	text := string(content)
	for _, want := range []string{
		"credentials presence is immutable after creation",
		"persistence presence is immutable after creation",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("expected generated CRD to contain %q", want)
		}
	}
}

func TestGeneratedCRDDefaultsObjectEncryptionEnabled(t *testing.T) {
	crdPath := filepath.Join("..", "..", "chart", "crds", "infra.sandbox0.ai_sandbox0infras.yaml")
	content, err := os.ReadFile(crdPath)
	if err != nil {
		t.Fatalf("read generated CRD: %v", err)
	}

	text := string(content)
	idx := strings.Index(text, "objectEncryptionEnabled:")
	if idx < 0 {
		t.Fatal("expected generated CRD to include objectEncryptionEnabled")
	}
	end := idx + 200
	if end > len(text) {
		end = len(text)
	}
	if !strings.Contains(text[idx:end], "default: true") {
		t.Fatalf("expected objectEncryptionEnabled default true near CRD field, got:\n%s", text[idx:end])
	}
}

func TestValidateSpecSemanticsRejectsAuditWithoutSandboxObservability(t *testing.T) {
	disabled := false
	infra := &infrav1alpha1.Sandbox0Infra{
		Spec: infrav1alpha1.Sandbox0InfraSpec{
			SandboxObservability: &infrav1alpha1.SandboxObservabilityConfig{
				Enabled: &disabled,
				Audit:   &infrav1alpha1.SandboxObservabilityAuditConfig{Enabled: true},
			},
		},
	}

	err := validateSpecSemantics(context.Background(), nil, infra)
	if err == nil || !strings.Contains(err.Error(), "sandboxObservability.audit requires sandboxObservability to be enabled") {
		t.Fatalf("error = %v, want audit enablement validation", err)
	}
}

func TestValidateSpecSemanticsRejectsAuditWithMultipleClusterGateways(t *testing.T) {
	enabled := true
	infra := &infrav1alpha1.Sandbox0Infra{
		Spec: infrav1alpha1.Sandbox0InfraSpec{
			ClickHouse: &infrav1alpha1.ClickHouseConfig{Type: infrav1alpha1.ClickHouseTypeBuiltin},
			SandboxObservability: &infrav1alpha1.SandboxObservabilityConfig{
				Enabled: &enabled,
				Backend: infrav1alpha1.SandboxObservabilityBackendClickHouse,
				Audit:   &infrav1alpha1.SandboxObservabilityAuditConfig{Enabled: true},
			},
			Services: &infrav1alpha1.ServicesConfig{
				ClusterGateway: &infrav1alpha1.ClusterGatewayServiceConfig{
					WorkloadServiceConfig: infrav1alpha1.WorkloadServiceConfig{Replicas: 2},
				},
			},
		},
	}

	err := validateSpecSemantics(context.Background(), nil, infra)
	if err == nil || !strings.Contains(err.Error(), "sandboxObservability.audit requires spec.services.clusterGateway.replicas to be 1") {
		t.Fatalf("error = %v, want cluster-gateway replica validation", err)
	}
}

func TestValidateSpecSemanticsRejectsUnknownAuditDeliveryMode(t *testing.T) {
	enabled := true
	infra := &infrav1alpha1.Sandbox0Infra{
		Spec: infrav1alpha1.Sandbox0InfraSpec{
			ClickHouse: &infrav1alpha1.ClickHouseConfig{Type: infrav1alpha1.ClickHouseTypeBuiltin},
			SandboxObservability: &infrav1alpha1.SandboxObservabilityConfig{
				Enabled: &enabled,
				Backend: infrav1alpha1.SandboxObservabilityBackendClickHouse,
				Audit: &infrav1alpha1.SandboxObservabilityAuditConfig{
					Enabled:      true,
					DeliveryMode: "best_effort",
				},
			},
		},
	}

	err := validateSpecSemantics(context.Background(), nil, infra)
	if err == nil || !strings.Contains(err.Error(), "spec.sandboxObservability.audit.deliveryMode must be durable_async or canonical_sync") {
		t.Fatalf("error = %v, want audit delivery mode validation", err)
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

func TestValidateSpecSemanticsRejectsInvalidNodePortServicePort(t *testing.T) {
	infra := &infrav1alpha1.Sandbox0Infra{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "sandbox0-system"},
		Spec: infrav1alpha1.Sandbox0InfraSpec{
			Services: &infrav1alpha1.ServicesConfig{
				ClusterGateway: &infrav1alpha1.ClusterGatewayServiceConfig{
					ServiceExposureConfig: infrav1alpha1.ServiceExposureConfig{
						Service: &infrav1alpha1.ServiceNetworkConfig{
							Type: corev1.ServiceTypeNodePort,
							Port: 19443,
						},
					},
				},
			},
		},
	}

	err := validateSpecSemantics(context.Background(), newValidationTestClient(t), infra)
	if err == nil {
		t.Fatal("expected validation error")
	}
	if !strings.Contains(err.Error(), "spec.services.clusterGateway.service.port must be within 30000-32767 when service.type is NodePort") {
		t.Fatalf("unexpected validation error: %v", err)
	}
}

func TestValidateSpecSemanticsRejectsInvalidClusterID(t *testing.T) {
	infra := &infrav1alpha1.Sandbox0Infra{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "sandbox0-system"},
		Spec: infrav1alpha1.Sandbox0InfraSpec{
			Cluster: &infrav1alpha1.ClusterConfig{
				ID: "sandbox0-gcp-use4-gke",
			},
		},
	}

	err := validateSpecSemantics(context.Background(), newValidationTestClient(t), infra)
	if err == nil {
		t.Fatal("expected validation error")
	}
	if !strings.Contains(err.Error(), "spec.cluster.id is invalid") {
		t.Fatalf("unexpected validation error: %v", err)
	}
}

func TestValidateSpecSemanticsRejectsSandboxObservabilityWithoutClickHouse(t *testing.T) {
	enabled := true
	infra := &infrav1alpha1.Sandbox0Infra{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "sandbox0-system"},
		Spec: infrav1alpha1.Sandbox0InfraSpec{
			ClickHouse: &infrav1alpha1.ClickHouseConfig{
				Type: infrav1alpha1.ClickHouseTypeDisabled,
			},
			SandboxObservability: &infrav1alpha1.SandboxObservabilityConfig{
				Enabled: &enabled,
				Backend: infrav1alpha1.SandboxObservabilityBackendClickHouse,
			},
		},
	}

	err := validateSpecSemantics(context.Background(), newValidationTestClient(t), infra)
	if err == nil {
		t.Fatal("expected validation error")
	}
	if !strings.Contains(err.Error(), "sandboxObservability backend clickhouse requires spec.clickHouse type builtin or external") {
		t.Fatalf("unexpected validation error: %v", err)
	}
}

func TestValidateSpecSemanticsRejectsMeteringWithoutClickHouse(t *testing.T) {
	enabled := true
	infra := &infrav1alpha1.Sandbox0Infra{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "sandbox0-system"},
		Spec: infrav1alpha1.Sandbox0InfraSpec{
			ClickHouse: &infrav1alpha1.ClickHouseConfig{
				Type: infrav1alpha1.ClickHouseTypeDisabled,
			},
			Metering: &infrav1alpha1.MeteringConfig{
				Enabled: &enabled,
			},
		},
	}

	err := validateSpecSemantics(context.Background(), newValidationTestClient(t), infra)
	if err == nil {
		t.Fatal("expected validation error")
	}
	if !strings.Contains(err.Error(), "metering requires spec.clickHouse type builtin or external") {
		t.Fatalf("unexpected validation error: %v", err)
	}
}

func TestValidateSpecSemanticsAcceptsMeteringWithClickHouse(t *testing.T) {
	enabled := true
	infra := &infrav1alpha1.Sandbox0Infra{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "sandbox0-system"},
		Spec: infrav1alpha1.Sandbox0InfraSpec{
			ClickHouse: &infrav1alpha1.ClickHouseConfig{
				Type: infrav1alpha1.ClickHouseTypeBuiltin,
			},
			Metering: &infrav1alpha1.MeteringConfig{
				Enabled: &enabled,
			},
		},
	}

	if err := validateSpecSemantics(context.Background(), newValidationTestClient(t), infra); err != nil {
		t.Fatalf("expected metering with clickhouse to be valid, got: %v", err)
	}
}

func TestValidateSpecSemanticsAcceptsValidNodePortServicePort(t *testing.T) {
	infra := &infrav1alpha1.Sandbox0Infra{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "sandbox0-system"},
		Spec: infrav1alpha1.Sandbox0InfraSpec{
			Services: &infrav1alpha1.ServicesConfig{
				ClusterGateway: &infrav1alpha1.ClusterGatewayServiceConfig{
					ServiceExposureConfig: infrav1alpha1.ServiceExposureConfig{
						Service: &infrav1alpha1.ServiceNetworkConfig{
							Type: corev1.ServiceTypeNodePort,
							Port: 30443,
						},
					},
				},
			},
		},
	}

	if err := validateSpecSemantics(context.Background(), newValidationTestClient(t), infra); err != nil {
		t.Fatalf("expected valid nodePort configuration, got: %v", err)
	}
}

func TestValidateSpecSemanticsAcceptsSSHEndpointPortBelowNodePortRange(t *testing.T) {
	infra := &infrav1alpha1.Sandbox0Infra{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "sandbox0-system"},
		Spec: infrav1alpha1.Sandbox0InfraSpec{
			Services: &infrav1alpha1.ServicesConfig{
				SSHGateway: &infrav1alpha1.SSHGatewayServiceConfig{
					ServiceExposureConfig: infrav1alpha1.ServiceExposureConfig{
						Service: &infrav1alpha1.ServiceNetworkConfig{
							Type: corev1.ServiceTypeNodePort,
							Port: 30222,
						},
					},
					EndpointPort: 22,
				},
			},
		},
	}

	if err := validateSpecSemantics(context.Background(), newValidationTestClient(t), infra); err != nil {
		t.Fatalf("expected valid SSH endpoint port configuration, got: %v", err)
	}
}

func TestValidateSpecSemanticsRejectsInvalidSSHEndpointPort(t *testing.T) {
	infra := &infrav1alpha1.Sandbox0Infra{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "sandbox0-system"},
		Spec: infrav1alpha1.Sandbox0InfraSpec{
			Services: &infrav1alpha1.ServicesConfig{
				SSHGateway: &infrav1alpha1.SSHGatewayServiceConfig{
					EndpointPort: 70000,
				},
			},
		},
	}

	err := validateSpecSemantics(context.Background(), newValidationTestClient(t), infra)
	if err == nil {
		t.Fatal("expected validation error")
	}
	if !strings.Contains(err.Error(), "spec.services.sshGateway.endpointPort must be within 1-65535 when set") {
		t.Fatalf("unexpected validation error: %v", err)
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

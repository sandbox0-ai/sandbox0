package storageproxy

import (
	"context"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	infrav1alpha1 "github.com/sandbox0-ai/sandbox0/infra-operator/api/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/infra-operator/internal/controller/pkg/common"
)

func TestReconcileUsesServicePortForHTTPServiceExposure(t *testing.T) {
	infra := &infrav1alpha1.Sandbox0Infra{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "demo",
			Namespace: "sandbox0-system",
		},
		Spec: infrav1alpha1.Sandbox0InfraSpec{
			Database: &infrav1alpha1.DatabaseConfig{
				Type: infrav1alpha1.DatabaseTypeBuiltin,
				Builtin: &infrav1alpha1.BuiltinDatabaseConfig{
					Enabled:  true,
					Port:     5432,
					Username: "sandbox0",
					Database: "sandbox0",
					SSLMode:  "disable",
				},
			},
			Storage: &infrav1alpha1.StorageConfig{
				Type: infrav1alpha1.StorageTypeBuiltin,
				Builtin: &infrav1alpha1.BuiltinStorageConfig{
					Enabled: true,
					Bucket:  "sandbox0",
					Region:  "us-east-1",
				},
			},
			Services: &infrav1alpha1.ServicesConfig{
				StorageProxy: &infrav1alpha1.StorageProxyServiceConfig{
					WorkloadServiceConfig: infrav1alpha1.WorkloadServiceConfig{
						EnabledServiceConfig: infrav1alpha1.EnabledServiceConfig{Enabled: true},
						Replicas:             1,
					},
					ServiceExposureConfig: infrav1alpha1.ServiceExposureConfig{
						Service: &infrav1alpha1.ServiceNetworkConfig{
							Port: 18083,
						},
					},
					Config: &infrav1alpha1.StorageProxyConfig{
						GRPCPort: 8080,
						HTTPPort: 8081,
					},
				},
			},
		},
	}

	reconciler, client := newStorageProxyTestReconciler(t,
		infra.DeepCopy(),
		&corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "demo-sandbox0-database-credentials",
				Namespace: infra.Namespace,
			},
			Data: map[string][]byte{
				"username": []byte("sandbox0"),
				"password": []byte("db-password"),
				"database": []byte("sandbox0"),
				"port":     []byte("5432"),
			},
		},
		&corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "demo-sandbox0-rustfs-credentials",
				Namespace: infra.Namespace,
			},
			Data: map[string][]byte{
				"endpoint":          []byte("http://demo-rustfs.sandbox0-system.svc:9000"),
				"RUSTFS_ACCESS_KEY": []byte("access-key"),
				"RUSTFS_SECRET_KEY": []byte("secret-key"),
			},
		},
	)

	if err := reconciler.Reconcile(context.Background(), infra, "sandbox0ai/infra", "latest"); err != nil && !strings.Contains(err.Error(), "not ready") {
		t.Fatalf("reconcile returned unexpected error: %v", err)
	}

	service := &corev1.Service{}
	if err := client.Get(context.Background(), types.NamespacedName{
		Name:      "demo-storage-proxy",
		Namespace: infra.Namespace,
	}, service); err != nil {
		t.Fatalf("get service: %v", err)
	}

	httpPort := findServicePort(t, service, "http")
	if httpPort.Port != 18083 {
		t.Fatalf("expected http service port 18083, got %d", httpPort.Port)
	}
	if httpPort.TargetPort.IntValue() != 8081 {
		t.Fatalf("expected http target port 8081, got %d", httpPort.TargetPort.IntValue())
	}

	grpcPort := findServicePort(t, service, "grpc")
	if grpcPort.Port != 8080 {
		t.Fatalf("expected grpc service port 8080, got %d", grpcPort.Port)
	}
}

func newStorageProxyTestReconciler(t *testing.T, objects ...runtime.Object) (*Reconciler, ctrlclient.Client) {
	t.Helper()

	scheme := runtime.NewScheme()
	if err := infrav1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add infra scheme: %v", err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("add core scheme: %v", err)
	}
	if err := appsv1.AddToScheme(scheme); err != nil {
		t.Fatalf("add apps scheme: %v", err)
	}

	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithRuntimeObjects(objects...).
		Build()

	return NewReconciler(common.NewResourceManager(client, scheme, nil, common.LocalDevConfig{})), client
}

func findServicePort(t *testing.T, service *corev1.Service, name string) corev1.ServicePort {
	t.Helper()
	for _, port := range service.Spec.Ports {
		if port.Name == name {
			return port
		}
	}
	t.Fatalf("expected service port %q to exist", name)
	return corev1.ServicePort{}
}

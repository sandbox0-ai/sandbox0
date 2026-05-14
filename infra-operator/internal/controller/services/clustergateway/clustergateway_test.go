package clustergateway

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
	infraplan "github.com/sandbox0-ai/sandbox0/infra-operator/internal/plan"
)

func TestReconcileKeepsHTTPBackendForHTTPSBaseURL(t *testing.T) {
	infra := &infrav1alpha1.Sandbox0Infra{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "demo",
			Namespace: "sandbox0-system",
		},
		Spec: infrav1alpha1.Sandbox0InfraSpec{
			Region: "gcp-ue4",
			PublicExposure: &infrav1alpha1.PublicExposureConfig{
				Enabled:    true,
				RootDomain: "sandbox0.app",
				RegionID:   "gcp-ue4",
			},
			InitUser: &infrav1alpha1.InitUserConfig{
				Email: "admin@example.com",
			},
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
			Services: &infrav1alpha1.ServicesConfig{
				ClusterGateway: &infrav1alpha1.ClusterGatewayServiceConfig{
					WorkloadServiceConfig: infrav1alpha1.WorkloadServiceConfig{
						EnabledServiceConfig: infrav1alpha1.EnabledServiceConfig{Enabled: true},
						Replicas:             1,
					},
					Config: &infrav1alpha1.ClusterGatewayConfig{
						AuthMode: "public",
						GatewayConfig: infrav1alpha1.GatewayConfig{
							BaseURL: "https://gcp-ue4.sandbox0.ai",
						},
					},
				},
			},
		},
	}

	reconciler, client := newClusterGatewayTestReconciler(t,
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
				Name:      "admin-password",
				Namespace: infra.Namespace,
			},
			Data: map[string][]byte{
				"password": []byte("admin-password"),
			},
		},
	)

	if err := reconciler.Reconcile(context.Background(), "sandbox0ai/infra", "latest", infraplan.Compile(infra)); err != nil && !strings.Contains(err.Error(), "not ready") {
		t.Fatalf("reconcile returned unexpected error: %v", err)
	}

	service := &corev1.Service{}
	if err := client.Get(context.Background(), types.NamespacedName{Name: "demo-cluster-gateway", Namespace: infra.Namespace}, service); err != nil {
		t.Fatalf("get cluster gateway service: %v", err)
	}
	if service.Spec.Ports[0].Port != 8443 {
		t.Fatalf("expected http service port 8443, got %d", service.Spec.Ports[0].Port)
	}
	if service.Spec.Ports[0].Name != "http" {
		t.Fatalf("expected http service port name, got %q", service.Spec.Ports[0].Name)
	}
	if service.Spec.Ports[0].TargetPort.IntVal != 8443 {
		t.Fatalf("expected http target port, got %#v", service.Spec.Ports[0].TargetPort)
	}

	deployment := &appsv1.Deployment{}
	if err := client.Get(context.Background(), types.NamespacedName{Name: "demo-cluster-gateway", Namespace: infra.Namespace}, deployment); err != nil {
		t.Fatalf("get cluster gateway deployment: %v", err)
	}
	if !hasContainerPort(deployment.Spec.Template.Spec.Containers[0].Ports, "http", 8443) {
		t.Fatalf("expected plain http container port, got %#v", deployment.Spec.Template.Spec.Containers[0].Ports)
	}
	if deployment.Spec.Template.Spec.Containers[0].ReadinessProbe.HTTPGet.Scheme != corev1.URISchemeHTTP {
		t.Fatalf("expected http readiness probe, got %s", deployment.Spec.Template.Spec.Containers[0].ReadinessProbe.HTTPGet.Scheme)
	}
	if deployment.Spec.Template.Spec.Containers[0].ReadinessProbe.HTTPGet.Port.StrVal != "http" {
		t.Fatalf("expected readiness probe to use http port, got %#v", deployment.Spec.Template.Spec.Containers[0].ReadinessProbe.HTTPGet.Port)
	}
	if hasVolume(deployment.Spec.Template.Spec.Volumes, "gateway-tls") {
		t.Fatal("did not expect gateway-tls volume for ingress-terminated TLS")
	}
}

func TestReconcilePublicModeSkipsControlPlanePublicKeyMount(t *testing.T) {
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
			Services: &infrav1alpha1.ServicesConfig{
				ClusterGateway: &infrav1alpha1.ClusterGatewayServiceConfig{
					WorkloadServiceConfig: infrav1alpha1.WorkloadServiceConfig{
						EnabledServiceConfig: infrav1alpha1.EnabledServiceConfig{Enabled: true},
					},
					Config: &infrav1alpha1.ClusterGatewayConfig{
						AuthMode: "public",
					},
				},
			},
		},
	}

	reconciler, client := newClusterGatewayTestReconciler(t,
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
	)

	if err := reconciler.Reconcile(context.Background(), "sandbox0ai/infra", "latest", infraplan.Compile(infra)); err != nil && !strings.Contains(err.Error(), "not ready") {
		t.Fatalf("reconcile returned unexpected error: %v", err)
	}

	deployment := &appsv1.Deployment{}
	if err := client.Get(context.Background(), types.NamespacedName{Name: "demo-cluster-gateway", Namespace: infra.Namespace}, deployment); err != nil {
		t.Fatalf("get cluster gateway deployment: %v", err)
	}
	if hasVolume(deployment.Spec.Template.Spec.Volumes, "internal-jwt-public-key") {
		t.Fatal("expected public auth mode to skip internal-jwt-public-key volume")
	}
}

func TestReconcileInternalModeMountsControlPlanePublicKey(t *testing.T) {
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
			Services: &infrav1alpha1.ServicesConfig{
				ClusterGateway: &infrav1alpha1.ClusterGatewayServiceConfig{
					WorkloadServiceConfig: infrav1alpha1.WorkloadServiceConfig{
						EnabledServiceConfig: infrav1alpha1.EnabledServiceConfig{Enabled: true},
					},
					Config: &infrav1alpha1.ClusterGatewayConfig{
						AuthMode: "internal",
					},
				},
			},
		},
	}

	reconciler, client := newClusterGatewayTestReconciler(t,
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
	)

	if err := reconciler.Reconcile(context.Background(), "sandbox0ai/infra", "latest", infraplan.Compile(infra)); err != nil && !strings.Contains(err.Error(), "not ready") {
		t.Fatalf("reconcile returned unexpected error: %v", err)
	}

	deployment := &appsv1.Deployment{}
	if err := client.Get(context.Background(), types.NamespacedName{Name: "demo-cluster-gateway", Namespace: infra.Namespace}, deployment); err != nil {
		t.Fatalf("get cluster gateway deployment: %v", err)
	}
	if !hasVolume(deployment.Spec.Template.Spec.Volumes, "internal-jwt-public-key") {
		t.Fatal("expected internal auth mode to mount internal-jwt-public-key volume")
	}
}

func TestReconcileRegionalGatewayPublicModeUpgradesToBoth(t *testing.T) {
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
			Services: &infrav1alpha1.ServicesConfig{
				RegionalGateway: &infrav1alpha1.RegionalGatewayServiceConfig{
					WorkloadServiceConfig: infrav1alpha1.WorkloadServiceConfig{
						EnabledServiceConfig: infrav1alpha1.EnabledServiceConfig{Enabled: true},
					},
				},
				ClusterGateway: &infrav1alpha1.ClusterGatewayServiceConfig{
					WorkloadServiceConfig: infrav1alpha1.WorkloadServiceConfig{
						EnabledServiceConfig: infrav1alpha1.EnabledServiceConfig{Enabled: true},
					},
					Config: &infrav1alpha1.ClusterGatewayConfig{
						AuthMode: "public",
					},
				},
			},
		},
	}

	reconciler, client := newClusterGatewayTestReconciler(t,
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
	)

	if err := reconciler.Reconcile(context.Background(), "sandbox0ai/infra", "latest", infraplan.Compile(infra)); err != nil && !strings.Contains(err.Error(), "not ready") {
		t.Fatalf("reconcile returned unexpected error: %v", err)
	}

	deployment := &appsv1.Deployment{}
	if err := client.Get(context.Background(), types.NamespacedName{Name: "demo-cluster-gateway", Namespace: infra.Namespace}, deployment); err != nil {
		t.Fatalf("get cluster gateway deployment: %v", err)
	}
	if !hasVolume(deployment.Spec.Template.Spec.Volumes, "internal-jwt-public-key") {
		t.Fatal("expected regional-gateway mode to mount internal-jwt-public-key volume")
	}

	configMap := &corev1.ConfigMap{}
	if err := client.Get(context.Background(), types.NamespacedName{Name: "demo-cluster-gateway", Namespace: infra.Namespace}, configMap); err != nil {
		t.Fatalf("get cluster gateway configmap: %v", err)
	}
	if !strings.Contains(configMap.Data["config.yaml"], "auth_mode: both") {
		t.Fatalf("expected cluster-gateway auth_mode to be promoted to both, got config %q", configMap.Data["config.yaml"])
	}
}

func TestReconcileFunctionGatewayPublicModeUpgradesToBoth(t *testing.T) {
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
			Services: &infrav1alpha1.ServicesConfig{
				FunctionGateway: &infrav1alpha1.FunctionGatewayServiceConfig{
					WorkloadServiceConfig: infrav1alpha1.WorkloadServiceConfig{
						EnabledServiceConfig: infrav1alpha1.EnabledServiceConfig{Enabled: true},
					},
				},
				ClusterGateway: &infrav1alpha1.ClusterGatewayServiceConfig{
					WorkloadServiceConfig: infrav1alpha1.WorkloadServiceConfig{
						EnabledServiceConfig: infrav1alpha1.EnabledServiceConfig{Enabled: true},
					},
					Config: &infrav1alpha1.ClusterGatewayConfig{
						AuthMode: "public",
					},
				},
			},
		},
	}

	reconciler, client := newClusterGatewayTestReconciler(t,
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
	)

	if err := reconciler.Reconcile(context.Background(), "sandbox0ai/infra", "latest", infraplan.Compile(infra)); err != nil && !strings.Contains(err.Error(), "not ready") {
		t.Fatalf("reconcile returned unexpected error: %v", err)
	}

	deployment := &appsv1.Deployment{}
	if err := client.Get(context.Background(), types.NamespacedName{Name: "demo-cluster-gateway", Namespace: infra.Namespace}, deployment); err != nil {
		t.Fatalf("get cluster gateway deployment: %v", err)
	}
	if !hasVolume(deployment.Spec.Template.Spec.Volumes, "internal-jwt-public-key") {
		t.Fatal("expected function-gateway mode to mount internal-jwt-public-key volume")
	}

	configMap := &corev1.ConfigMap{}
	if err := client.Get(context.Background(), types.NamespacedName{Name: "demo-cluster-gateway", Namespace: infra.Namespace}, configMap); err != nil {
		t.Fatalf("get cluster gateway configmap: %v", err)
	}
	if !strings.Contains(configMap.Data["config.yaml"], "auth_mode: both") {
		t.Fatalf("expected cluster-gateway auth_mode to be promoted to both, got config %q", configMap.Data["config.yaml"])
	}
}

func newClusterGatewayTestReconciler(t *testing.T, objects ...runtime.Object) (*Reconciler, ctrlclient.Client) {
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

func hasVolume(volumes []corev1.Volume, name string) bool {
	for _, volume := range volumes {
		if volume.Name == name {
			return true
		}
	}
	return false
}

func hasContainerPort(ports []corev1.ContainerPort, name string, port int32) bool {
	for _, candidate := range ports {
		if candidate.Name == name && candidate.ContainerPort == port {
			return true
		}
	}
	return false
}

func TestBuildConfigUsesStorageProxyServicePortForDerivedURL(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("add core scheme: %v", err)
	}
	if err := infrav1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add infra scheme: %v", err)
	}

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
			Services: &infrav1alpha1.ServicesConfig{
				StorageProxy: &infrav1alpha1.StorageProxyServiceConfig{
					WorkloadServiceConfig: infrav1alpha1.WorkloadServiceConfig{
						EnabledServiceConfig: infrav1alpha1.EnabledServiceConfig{Enabled: true},
					},
					ServiceExposureConfig: infrav1alpha1.ServiceExposureConfig{
						Service: &infrav1alpha1.ServiceNetworkConfig{
							Port: 18083,
						},
					},
					Config: &infrav1alpha1.StorageProxyConfig{
						HTTPPort: 8081,
					},
				},
			},
		},
	}

	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(
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
		).
		Build()

	reconciler := NewReconciler(common.NewResourceManager(client, scheme, nil, common.LocalDevConfig{}))
	cfg, err := reconciler.buildConfig(context.Background(), infraplan.Compile(infra))
	if err != nil {
		t.Fatalf("buildConfig returned error: %v", err)
	}

	if cfg.StorageProxyURL != "http://demo-storage-proxy:18083" {
		t.Fatalf("expected storage proxy url to use service port, got %q", cfg.StorageProxyURL)
	}
}

func TestBuildConfigPublishesSSHEndpoint(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("add core scheme: %v", err)
	}
	if err := infrav1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add infra scheme: %v", err)
	}

	infra := &infrav1alpha1.Sandbox0Infra{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "demo",
			Namespace: "sandbox0-system",
		},
		Spec: infrav1alpha1.Sandbox0InfraSpec{
			PublicExposure: &infrav1alpha1.PublicExposureConfig{
				RootDomain: "sandbox0.app",
				RegionID:   "aws-us-east-1",
			},
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
			Services: &infrav1alpha1.ServicesConfig{
				SSHGateway: &infrav1alpha1.SSHGatewayServiceConfig{
					WorkloadServiceConfig: infrav1alpha1.WorkloadServiceConfig{
						EnabledServiceConfig: infrav1alpha1.EnabledServiceConfig{Enabled: true},
					},
					ServiceExposureConfig: infrav1alpha1.ServiceExposureConfig{
						Service: &infrav1alpha1.ServiceNetworkConfig{
							Type: corev1.ServiceTypeNodePort,
							Port: 30222,
						},
					},
					Config: &infrav1alpha1.SSHGatewayConfig{
						SSHPort: 2222,
					},
				},
			},
		},
	}

	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(
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
		).
		Build()

	reconciler := NewReconciler(common.NewResourceManager(client, scheme, nil, common.LocalDevConfig{}))
	cfg, err := reconciler.buildConfig(context.Background(), infraplan.Compile(infra))
	if err != nil {
		t.Fatalf("buildConfig returned error: %v", err)
	}
	if cfg.SSHEndpointHost != "aws-us-east-1.ssh.sandbox0.app" {
		t.Fatalf("ssh endpoint host = %q, want %q", cfg.SSHEndpointHost, "aws-us-east-1.ssh.sandbox0.app")
	}
	if cfg.SSHEndpointPort != 30222 {
		t.Fatalf("ssh endpoint port = %d, want %d", cfg.SSHEndpointPort, 30222)
	}
}

func TestBuildConfigSkipsInitUserForInternalOnlyClusterGateway(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("add core scheme: %v", err)
	}
	if err := infrav1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add infra scheme: %v", err)
	}

	infra := &infrav1alpha1.Sandbox0Infra{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "demo",
			Namespace: "sandbox0-system",
		},
		Spec: infrav1alpha1.Sandbox0InfraSpec{
			InitUser: &infrav1alpha1.InitUserConfig{
				Email: "admin@example.com",
			},
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
			Services: &infrav1alpha1.ServicesConfig{
				ClusterGateway: &infrav1alpha1.ClusterGatewayServiceConfig{
					WorkloadServiceConfig: infrav1alpha1.WorkloadServiceConfig{
						EnabledServiceConfig: infrav1alpha1.EnabledServiceConfig{Enabled: true},
					},
					Config: &infrav1alpha1.ClusterGatewayConfig{
						AuthMode: "internal",
					},
				},
			},
		},
	}

	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(
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
		).
		Build()

	reconciler := NewReconciler(common.NewResourceManager(client, scheme, nil, common.LocalDevConfig{}))
	cfg, err := reconciler.buildConfig(context.Background(), infraplan.Compile(infra))
	if err != nil {
		t.Fatalf("buildConfig returned error: %v", err)
	}
	if cfg.BuiltInAuth.InitUser != nil {
		t.Fatalf("expected init user to be omitted for internal-only cluster gateway, got %#v", cfg.BuiltInAuth.InitUser)
	}
}

func TestBuildConfigLeavesInitUserPasswordEmptyForOIDCOnlyBootstrap(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("add core scheme: %v", err)
	}
	if err := infrav1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add infra scheme: %v", err)
	}

	infra := &infrav1alpha1.Sandbox0Infra{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "demo",
			Namespace: "sandbox0-system",
		},
		Spec: infrav1alpha1.Sandbox0InfraSpec{
			InitUser: &infrav1alpha1.InitUserConfig{
				Email: "admin@example.com",
			},
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
			Services: &infrav1alpha1.ServicesConfig{
				ClusterGateway: &infrav1alpha1.ClusterGatewayServiceConfig{
					WorkloadServiceConfig: infrav1alpha1.WorkloadServiceConfig{
						EnabledServiceConfig: infrav1alpha1.EnabledServiceConfig{Enabled: true},
					},
					Config: &infrav1alpha1.ClusterGatewayConfig{
						AuthMode: "public",
						GatewayConfig: infrav1alpha1.GatewayConfig{
							BuiltInAuth: infrav1alpha1.BuiltInAuthConfig{
								Enabled: false,
							},
							OIDCProviders: []infrav1alpha1.OIDCProviderConfig{
								{Enabled: true},
							},
						},
					},
				},
			},
		},
	}

	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(
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
		).
		Build()

	reconciler := NewReconciler(common.NewResourceManager(client, scheme, nil, common.LocalDevConfig{}))
	cfg, err := reconciler.buildConfig(context.Background(), infraplan.Compile(infra))
	if err != nil {
		t.Fatalf("buildConfig returned error: %v", err)
	}
	if cfg.BuiltInAuth.InitUser == nil {
		t.Fatal("expected init user config")
	}
	if cfg.BuiltInAuth.InitUser.Password != "" {
		t.Fatalf("expected oidc-only init user password to be empty, got %q", cfg.BuiltInAuth.InitUser.Password)
	}
}

func TestBuildConfigDefaultsRegionIDAndInitUserHomeRegionFromPublicExposure(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("add core scheme: %v", err)
	}
	if err := infrav1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add infra scheme: %v", err)
	}

	infra := &infrav1alpha1.Sandbox0Infra{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "demo",
			Namespace: "sandbox0-system",
		},
		Spec: infrav1alpha1.Sandbox0InfraSpec{
			InitUser: &infrav1alpha1.InitUserConfig{
				Email: "admin@example.com",
				Name:  "Admin",
				PasswordSecret: infrav1alpha1.SecretKeyRef{
					Name: "demo-init-user-password",
					Key:  "password",
				},
			},
			PublicExposure: &infrav1alpha1.PublicExposureConfig{
				Enabled:  true,
				RegionID: "aws-us-east-1",
			},
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
			Services: &infrav1alpha1.ServicesConfig{
				ClusterGateway: &infrav1alpha1.ClusterGatewayServiceConfig{
					WorkloadServiceConfig: infrav1alpha1.WorkloadServiceConfig{
						EnabledServiceConfig: infrav1alpha1.EnabledServiceConfig{Enabled: true},
					},
					Config: &infrav1alpha1.ClusterGatewayConfig{
						AuthMode: "public",
					},
				},
			},
		},
	}

	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(
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
					Name:      "demo-init-user-password",
					Namespace: infra.Namespace,
				},
				Data: map[string][]byte{
					"password": []byte("admin-password"),
				},
			},
		).
		Build()

	reconciler := NewReconciler(common.NewResourceManager(client, scheme, nil, common.LocalDevConfig{}))
	cfg, err := reconciler.buildConfig(context.Background(), infraplan.Compile(infra))
	if err != nil {
		t.Fatalf("buildConfig returned error: %v", err)
	}
	if cfg.RegionID != "aws-us-east-1" {
		t.Fatalf("expected region id to default from public exposure, got %q", cfg.RegionID)
	}
	if cfg.BuiltInAuth.InitUser == nil {
		t.Fatal("expected init user config")
	}
	if cfg.BuiltInAuth.InitUser.HomeRegionID != "aws-us-east-1" {
		t.Fatalf("expected init user home region to default from resolved region, got %#v", cfg.BuiltInAuth.InitUser)
	}
	if cfg.BuiltInAuth.InitUser.Password != "admin-password" {
		t.Fatalf("unexpected init user password: %q", cfg.BuiltInAuth.InitUser.Password)
	}
}

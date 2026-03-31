package globalgateway

import (
	"context"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	infrav1alpha1 "github.com/sandbox0-ai/sandbox0/infra-operator/api/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/infra-operator/internal/controller/pkg/common"
)

func TestBuildConfigPopulatesDatabaseAndJWTSecret(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("add corev1 scheme: %v", err)
	}
	if err := networkingv1.AddToScheme(scheme); err != nil {
		t.Fatalf("add networkingv1 scheme: %v", err)
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
				GlobalGateway: &infrav1alpha1.GlobalGatewayServiceConfig{
					WorkloadServiceConfig: infrav1alpha1.WorkloadServiceConfig{
						EnabledServiceConfig: infrav1alpha1.EnabledServiceConfig{Enabled: true},
						Replicas:             1,
					},
				},
			},
			InitUser: &infrav1alpha1.InitUserConfig{
				Email: "admin@example.com",
				Name:  "Admin",
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
					Name:      "admin-password",
					Namespace: infra.Namespace,
				},
				Data: map[string][]byte{
					"password": []byte("admin-password"),
				},
			},
		).
		Build()

	reconciler := NewReconciler(common.NewResourceManager(client, scheme, nil, common.LocalDevConfig{}))
	cfg, err := reconciler.buildConfig(context.Background(), infra)
	if err != nil {
		t.Fatalf("buildConfig returned error: %v", err)
	}

	wantDSN := "postgres://sandbox0:db-password@demo-postgres.sandbox0-system.svc:5432/sandbox0?sslmode=disable"
	if cfg.DatabaseURL != wantDSN {
		t.Fatalf("unexpected database url: %q", cfg.DatabaseURL)
	}
	if cfg.HTTPPort != 8080 {
		t.Fatalf("expected default http port 8080, got %d", cfg.HTTPPort)
	}
	if cfg.LogLevel != "info" {
		t.Fatalf("expected default log level info, got %q", cfg.LogLevel)
	}
	if cfg.DatabaseMaxConns != 30 {
		t.Fatalf("expected default database max conns 30, got %d", cfg.DatabaseMaxConns)
	}
	if cfg.DatabaseMinConns != 8 {
		t.Fatalf("expected default database min conns 8, got %d", cfg.DatabaseMinConns)
	}
	if cfg.DatabaseSchema != "global_gateway" {
		t.Fatalf("expected default database schema global_gateway, got %q", cfg.DatabaseSchema)
	}
	if cfg.RegionTokenTTL.Duration != 5*time.Minute {
		t.Fatalf("expected default region token ttl 5m, got %s", cfg.RegionTokenTTL.Duration)
	}
	if cfg.JWTAccessTokenTTL.Duration != 15*time.Minute {
		t.Fatalf("expected default access token ttl 15m, got %s", cfg.JWTAccessTokenTTL.Duration)
	}
	if cfg.JWTRefreshTokenTTL.Duration != 168*time.Hour {
		t.Fatalf("expected default refresh token ttl 168h, got %s", cfg.JWTRefreshTokenTTL.Duration)
	}
	if cfg.BaseURL != "http://localhost:8080" {
		t.Fatalf("expected default base url, got %q", cfg.BaseURL)
	}
	if cfg.JWTIssuer != "global-gateway" {
		t.Fatalf("unexpected jwt issuer: %q", cfg.JWTIssuer)
	}
	if cfg.JWTSecret == "" {
		t.Fatal("expected jwt secret to be generated")
	}
	if cfg.BuiltInAuth.InitUser == nil {
		t.Fatal("expected init user config")
	}
	if cfg.BuiltInAuth.InitUser.Password != "admin-password" {
		t.Fatalf("unexpected init user password: %q", cfg.BuiltInAuth.InitUser.Password)
	}

	jwtSecret := &corev1.Secret{}
	if err := client.Get(context.Background(), types.NamespacedName{
		Name:      "demo-global-gateway-jwt",
		Namespace: infra.Namespace,
	}, jwtSecret); err != nil {
		t.Fatalf("expected jwt secret to be created: %v", err)
	}
}

func TestBuildConfigPreservesInitUserHomeRegionID(t *testing.T) {
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
				GlobalGateway: &infrav1alpha1.GlobalGatewayServiceConfig{
					WorkloadServiceConfig: infrav1alpha1.WorkloadServiceConfig{
						EnabledServiceConfig: infrav1alpha1.EnabledServiceConfig{Enabled: true},
						Replicas:             1,
					},
					Config: &infrav1alpha1.GlobalGatewayConfig{},
				},
			},
			InitUser: &infrav1alpha1.InitUserConfig{
				Email:        "admin@example.com",
				Name:         "Admin",
				HomeRegionID: "aws/us-east-1",
			},
		},
	}

	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("add corev1 scheme: %v", err)
	}
	if err := infrav1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add infra scheme: %v", err)
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
					Name:      "admin-password",
					Namespace: infra.Namespace,
				},
				Data: map[string][]byte{
					"password": []byte("admin-password"),
				},
			},
		).
		Build()

	reconciler := NewReconciler(common.NewResourceManager(client, scheme, nil, common.LocalDevConfig{}))
	cfg, err := reconciler.buildConfig(context.Background(), infra)
	if err != nil {
		t.Fatalf("buildConfig returned error: %v", err)
	}

	if cfg.BuiltInAuth.InitUser == nil || cfg.BuiltInAuth.InitUser.HomeRegionID != "aws/us-east-1" {
		t.Fatalf("expected init user home region id to be preserved, got %#v", cfg.BuiltInAuth.InitUser)
	}
}

func TestBuildConfigLeavesInitUserPasswordEmptyForOIDCOnlyBootstrap(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("add corev1 scheme: %v", err)
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
				GlobalGateway: &infrav1alpha1.GlobalGatewayServiceConfig{
					WorkloadServiceConfig: infrav1alpha1.WorkloadServiceConfig{
						EnabledServiceConfig: infrav1alpha1.EnabledServiceConfig{Enabled: true},
						Replicas:             1,
					},
					Config: &infrav1alpha1.GlobalGatewayConfig{
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
			InitUser: &infrav1alpha1.InitUserConfig{
				Email: "admin@example.com",
				Name:  "Admin",
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
	cfg, err := reconciler.buildConfig(context.Background(), infra)
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

func TestBuildConfigDerivesRegionFromPublicExposure(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("add corev1 scheme: %v", err)
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
			PublicExposure: &infrav1alpha1.PublicExposureConfig{
				Enabled:    true,
				RootDomain: "sandbox0.app",
				RegionID:   "aws-us-east-1",
			},
			Services: &infrav1alpha1.ServicesConfig{
				GlobalGateway: &infrav1alpha1.GlobalGatewayServiceConfig{
					WorkloadServiceConfig: infrav1alpha1.WorkloadServiceConfig{
						EnabledServiceConfig: infrav1alpha1.EnabledServiceConfig{Enabled: true},
						Replicas:             1,
					},
				},
				RegionalGateway: &infrav1alpha1.RegionalGatewayServiceConfig{
					WorkloadServiceConfig: infrav1alpha1.WorkloadServiceConfig{
						EnabledServiceConfig: infrav1alpha1.EnabledServiceConfig{Enabled: true},
						Replicas:             1,
					},
				},
			},
			InitUser: &infrav1alpha1.InitUserConfig{
				Email: "admin@example.com",
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
					Name:      "admin-password",
					Namespace: infra.Namespace,
				},
				Data: map[string][]byte{
					"password": []byte("admin-password"),
				},
			},
		).
		Build()

	reconciler := NewReconciler(common.NewResourceManager(client, scheme, nil, common.LocalDevConfig{}))
	cfg, err := reconciler.buildConfig(context.Background(), infra)
	if err != nil {
		t.Fatalf("buildConfig returned error: %v", err)
	}

	if cfg.RegionID != "aws/us-east-1" {
		t.Fatalf("expected canonical region id, got %q", cfg.RegionID)
	}
	if !cfg.PublicExposureEnabled {
		t.Fatal("expected public exposure to be enabled")
	}
	if cfg.PublicRootDomain != "sandbox0.app" {
		t.Fatalf("expected public root domain to be preserved, got %q", cfg.PublicRootDomain)
	}
	if cfg.BuiltInAuth.InitUser == nil || cfg.BuiltInAuth.InitUser.HomeRegionID != "aws/us-east-1" {
		t.Fatalf("expected init user home region to default from canonical region, got %#v", cfg.BuiltInAuth.InitUser)
	}
}

func TestDesiredBootstrapRegionUsesRegionalGatewayServiceAddress(t *testing.T) {
	infra := &infrav1alpha1.Sandbox0Infra{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "demo",
			Namespace: "sandbox0-system",
		},
		Spec: infrav1alpha1.Sandbox0InfraSpec{
			Services: &infrav1alpha1.ServicesConfig{
				RegionalGateway: &infrav1alpha1.RegionalGatewayServiceConfig{
					WorkloadServiceConfig: infrav1alpha1.WorkloadServiceConfig{
						EnabledServiceConfig: infrav1alpha1.EnabledServiceConfig{Enabled: true},
					},
					ServiceExposureConfig: infrav1alpha1.ServiceExposureConfig{
						Service: &infrav1alpha1.ServiceNetworkConfig{
							Port: 18080,
						},
					},
				},
			},
		},
	}

	region := desiredBootstrapRegion(infra, "aws/us-east-1")
	if region == nil {
		t.Fatal("expected bootstrap region")
	}
	if region.ID != "aws/us-east-1" {
		t.Fatalf("unexpected region id: %q", region.ID)
	}
	if region.RegionalGatewayURL != "http://demo-regional-gateway:18080" {
		t.Fatalf("unexpected regional gateway url: %q", region.RegionalGatewayURL)
	}
}

func TestReconcileCreatesGlobalGatewayResources(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := appsv1.AddToScheme(scheme); err != nil {
		t.Fatalf("add appsv1 scheme: %v", err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("add corev1 scheme: %v", err)
	}
	if err := networkingv1.AddToScheme(scheme); err != nil {
		t.Fatalf("add networkingv1 scheme: %v", err)
	}
	if err := infrav1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add infra scheme: %v", err)
	}

	replicas := int32(1)
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
				GlobalGateway: &infrav1alpha1.GlobalGatewayServiceConfig{
					WorkloadServiceConfig: infrav1alpha1.WorkloadServiceConfig{
						EnabledServiceConfig: infrav1alpha1.EnabledServiceConfig{Enabled: true},
						Replicas:             replicas,
					},
				},
			},
		},
	}

	readyDeployment := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "demo-global-gateway",
			Namespace: infra.Namespace,
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
		},
		Status: appsv1.DeploymentStatus{
			ReadyReplicas: replicas,
		},
	}

	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(readyDeployment).
		WithObjects(
			infra.DeepCopy(),
			readyDeployment,
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
	if err := reconciler.Reconcile(context.Background(), infra, "ghcr.io/sandbox0-ai/sandbox0", "latest", nil); err != nil {
		t.Fatalf("reconcile returned error: %v", err)
	}

	configMap := &corev1.ConfigMap{}
	if err := client.Get(context.Background(), types.NamespacedName{
		Name:      "demo-global-gateway",
		Namespace: infra.Namespace,
	}, configMap); err != nil {
		t.Fatalf("expected configmap to be created: %v", err)
	}
	if !strings.Contains(configMap.Data["config.yaml"], "database_url: postgres://sandbox0:db-password@demo-postgres.sandbox0-system.svc:5432/sandbox0?sslmode=disable") {
		t.Fatalf("expected configmap to contain database url, got %q", configMap.Data["config.yaml"])
	}

	service := &corev1.Service{}
	if err := client.Get(context.Background(), types.NamespacedName{
		Name:      "demo-global-gateway",
		Namespace: infra.Namespace,
	}, service); err != nil {
		t.Fatalf("expected service to be created: %v", err)
	}
	if len(service.Spec.Ports) != 1 || service.Spec.Ports[0].Port != 8080 {
		t.Fatalf("expected service port 8080, got %#v", service.Spec.Ports)
	}

	deployment := &appsv1.Deployment{}
	if err := client.Get(context.Background(), types.NamespacedName{
		Name:      "demo-global-gateway",
		Namespace: infra.Namespace,
	}, deployment); err != nil {
		t.Fatalf("expected deployment to be updated: %v", err)
	}
	if len(deployment.Spec.Template.Spec.Containers) != 1 {
		t.Fatalf("expected one container, got %d", len(deployment.Spec.Template.Spec.Containers))
	}
	container := deployment.Spec.Template.Spec.Containers[0]
	if container.Name != "global-gateway" {
		t.Fatalf("unexpected container name %q", container.Name)
	}
	if container.Image != "ghcr.io/sandbox0-ai/sandbox0:latest" {
		t.Fatalf("unexpected image %q", container.Image)
	}
	if len(container.Ports) != 1 || container.Ports[0].ContainerPort != 8080 {
		t.Fatalf("expected container port 8080, got %#v", container.Ports)
	}

}

package globaldirectory

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
				GlobalDirectory: &infrav1alpha1.GlobalDirectoryServiceConfig{
					BaseServiceConfig: infrav1alpha1.BaseServiceConfig{
						Enabled:  true,
						Replicas: 1,
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
	if cfg.DatabaseSchema != "gd" {
		t.Fatalf("expected default database schema gd, got %q", cfg.DatabaseSchema)
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
	if cfg.JWTIssuer != "global-directory" {
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
		Name:      "demo-global-directory-jwt",
		Namespace: infra.Namespace,
	}, jwtSecret); err != nil {
		t.Fatalf("expected jwt secret to be created: %v", err)
	}
}

func TestReconcileCreatesGlobalDirectoryResourcesAndEndpoint(t *testing.T) {
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
				GlobalDirectory: &infrav1alpha1.GlobalDirectoryServiceConfig{
					BaseServiceConfig: infrav1alpha1.BaseServiceConfig{
						Enabled:  true,
						Replicas: replicas,
					},
				},
			},
		},
	}

	readyDeployment := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "demo-global-directory",
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
	if err := reconciler.Reconcile(context.Background(), infra, "ghcr.io/sandbox0-ai/sandbox0", "latest"); err != nil {
		t.Fatalf("reconcile returned error: %v", err)
	}

	configMap := &corev1.ConfigMap{}
	if err := client.Get(context.Background(), types.NamespacedName{
		Name:      "demo-global-directory",
		Namespace: infra.Namespace,
	}, configMap); err != nil {
		t.Fatalf("expected configmap to be created: %v", err)
	}
	if !strings.Contains(configMap.Data["config.yaml"], "database_url: postgres://sandbox0:db-password@demo-postgres.sandbox0-system.svc:5432/sandbox0?sslmode=disable") {
		t.Fatalf("expected configmap to contain database url, got %q", configMap.Data["config.yaml"])
	}

	service := &corev1.Service{}
	if err := client.Get(context.Background(), types.NamespacedName{
		Name:      "demo-global-directory",
		Namespace: infra.Namespace,
	}, service); err != nil {
		t.Fatalf("expected service to be created: %v", err)
	}
	if len(service.Spec.Ports) != 1 || service.Spec.Ports[0].Port != 8080 {
		t.Fatalf("expected service port 8080, got %#v", service.Spec.Ports)
	}

	deployment := &appsv1.Deployment{}
	if err := client.Get(context.Background(), types.NamespacedName{
		Name:      "demo-global-directory",
		Namespace: infra.Namespace,
	}, deployment); err != nil {
		t.Fatalf("expected deployment to be updated: %v", err)
	}
	if len(deployment.Spec.Template.Spec.Containers) != 1 {
		t.Fatalf("expected one container, got %d", len(deployment.Spec.Template.Spec.Containers))
	}
	container := deployment.Spec.Template.Spec.Containers[0]
	if container.Name != "global-directory" {
		t.Fatalf("unexpected container name %q", container.Name)
	}
	if container.Image != "ghcr.io/sandbox0-ai/sandbox0:latest" {
		t.Fatalf("unexpected image %q", container.Image)
	}
	if len(container.Ports) != 1 || container.Ports[0].ContainerPort != 8080 {
		t.Fatalf("expected container port 8080, got %#v", container.Ports)
	}

	if infra.Status.Endpoints == nil || infra.Status.Endpoints.GlobalDirectory == "" {
		t.Fatal("expected global-directory endpoint to be recorded")
	}
	if infra.Status.Endpoints.GlobalDirectory != "http://demo-global-directory:8080" {
		t.Fatalf("unexpected global-directory endpoint %q", infra.Status.Endpoints.GlobalDirectory)
	}
}

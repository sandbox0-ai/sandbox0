package clustergateway

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	infrav1alpha1 "github.com/sandbox0-ai/sandbox0/infra-operator/api/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/infra-operator/internal/controller/pkg/common"
)

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
	cfg, err := reconciler.buildConfig(context.Background(), infra)
	if err != nil {
		t.Fatalf("buildConfig returned error: %v", err)
	}

	if cfg.StorageProxyURL != "http://demo-storage-proxy:18083" {
		t.Fatalf("expected storage proxy url to use service port, got %q", cfg.StorageProxyURL)
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
	cfg, err := reconciler.buildConfig(context.Background(), infra)
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
	cfg, err := reconciler.buildConfig(context.Background(), infra)
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

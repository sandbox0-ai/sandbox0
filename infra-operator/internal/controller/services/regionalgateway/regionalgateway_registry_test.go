package regionalgateway

import (
	"context"
	"testing"

	apiconfig "github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	infrav1alpha1 "github.com/sandbox0-ai/sandbox0/infra-operator/api/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/infra-operator/internal/controller/pkg/common"
	infraplan "github.com/sandbox0-ai/sandbox0/infra-operator/internal/plan"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestApplyRegistryConfigBuiltin(t *testing.T) {
	r := &Reconciler{}
	infra := &infrav1alpha1.Sandbox0Infra{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "s0cp",
			Namespace: "sandbox0-system",
		},
		Spec: infrav1alpha1.Sandbox0InfraSpec{
			Registry: &infrav1alpha1.RegistryConfig{
				Provider: infrav1alpha1.RegistryProviderBuiltin,
				Builtin: &infrav1alpha1.BuiltinRegistryConfig{
					Enabled: true,
					Port:    5000,
				},
			},
		},
	}
	cfg := &apiconfig.RegionalGatewayConfig{}

	envVars, err := r.applyRegistryConfig(infra, cfg)
	if err != nil {
		t.Fatalf("applyRegistryConfig returned error: %v", err)
	}

	if cfg.Registry.Provider != "builtin" {
		t.Fatalf("unexpected provider: %s", cfg.Registry.Provider)
	}
	if cfg.Registry.PushRegistry != "s0cp-registry.sandbox0-system.svc:5000" {
		t.Fatalf("unexpected push registry: %s", cfg.Registry.PushRegistry)
	}
	if cfg.Registry.Builtin == nil {
		t.Fatal("builtin registry config is nil")
	}
	if cfg.Registry.Builtin.Username != "${S0_REGISTRY_BUILTIN_USERNAME}" {
		t.Fatalf("unexpected username placeholder: %s", cfg.Registry.Builtin.Username)
	}
	if len(envVars) != 2 {
		t.Fatalf("unexpected env vars count: %d", len(envVars))
	}
	if envVars[0].Name != "S0_REGISTRY_BUILTIN_USERNAME" || envVars[0].ValueFrom == nil || envVars[0].ValueFrom.SecretKeyRef == nil {
		t.Fatalf("unexpected first env var: %+v", envVars[0])
	}
	if envVars[0].ValueFrom.SecretKeyRef.Name != "s0cp-registry-auth" {
		t.Fatalf("unexpected secret name: %s", envVars[0].ValueFrom.SecretKeyRef.Name)
	}
}

func TestApplyRegistryConfigAWS(t *testing.T) {
	r := &Reconciler{}
	infra := &infrav1alpha1.Sandbox0Infra{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "s0cp",
			Namespace: "sandbox0-system",
		},
		Spec: infrav1alpha1.Sandbox0InfraSpec{
			Registry: &infrav1alpha1.RegistryConfig{
				Provider: infrav1alpha1.RegistryProviderAWS,
				AWS: &infrav1alpha1.AWSRegistryConfig{
					Registry:   "123456789012.dkr.ecr.us-east-1.amazonaws.com",
					Region:     "us-east-1",
					RegistryID: "123456789012",
					PullSecret: infrav1alpha1.DockerConfigSecretRef{Name: "ecr-pull"},
					CredentialsSecret: infrav1alpha1.AWSRegistryCredentialsSecret{
						Name:         "aws-credentials",
						AccessKeyKey: "accessKeyId",
						SecretKeyKey: "secretAccessKey",
					},
				},
			},
		},
	}
	cfg := &apiconfig.RegionalGatewayConfig{}

	envVars, err := r.applyRegistryConfig(infra, cfg)
	if err != nil {
		t.Fatalf("applyRegistryConfig returned error: %v", err)
	}

	if cfg.Registry.Provider != "aws" {
		t.Fatalf("unexpected provider: %s", cfg.Registry.Provider)
	}
	if cfg.Registry.AWS == nil {
		t.Fatal("aws registry config is nil")
	}
	if cfg.Registry.AWS.AccessKeyID != "${S0_REGISTRY_AWS_ACCESS_KEY_ID}" {
		t.Fatalf("unexpected access key placeholder: %s", cfg.Registry.AWS.AccessKeyID)
	}
	if len(envVars) != 2 {
		t.Fatalf("unexpected env vars count: %d", len(envVars))
	}
	if envVars[0].ValueFrom == nil || envVars[0].ValueFrom.SecretKeyRef == nil {
		t.Fatalf("unexpected env var[0]: %+v", envVars[0])
	}
	if envVars[0].ValueFrom.SecretKeyRef.Name != "aws-credentials" {
		t.Fatalf("unexpected secret name: %s", envVars[0].ValueFrom.SecretKeyRef.Name)
	}
}

func TestApplyRegistryConfigSkipsWhenRegistryIsNotDeclared(t *testing.T) {
	r := &Reconciler{}
	infra := &infrav1alpha1.Sandbox0Infra{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "s0cp",
			Namespace: "sandbox0-system",
		},
	}
	cfg := &apiconfig.RegionalGatewayConfig{}

	envVars, err := r.applyRegistryConfig(infra, cfg)
	if err != nil {
		t.Fatalf("applyRegistryConfig returned error: %v", err)
	}
	if len(envVars) != 0 {
		t.Fatalf("expected no env vars, got %d", len(envVars))
	}
	if cfg.Registry.Provider != "" || cfg.Registry.PushRegistry != "" || cfg.Registry.PullRegistry != "" {
		t.Fatalf("expected empty registry config, got %#v", cfg.Registry)
	}
}

func TestBuildConfigUsesCompiledPlanForDefaultClusterGatewayURL(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("failed to add corev1 scheme: %v", err)
	}
	if err := infrav1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("failed to add infra scheme: %v", err)
	}

	infra := &infrav1alpha1.Sandbox0Infra{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "s0cp",
			Namespace: "sandbox0-system",
		},
		Spec: infrav1alpha1.Sandbox0InfraSpec{
			Database: &infrav1alpha1.DatabaseConfig{
				Type: infrav1alpha1.DatabaseTypeExternal,
				External: &infrav1alpha1.ExternalDatabaseConfig{
					Host:     "postgres.example.internal",
					Port:     5432,
					Database: "sandbox0",
					Username: "sandbox0",
					PasswordSecret: infrav1alpha1.SecretKeyRef{
						Name: "regional-db",
						Key:  "password",
					},
				},
			},
			Services: &infrav1alpha1.ServicesConfig{
				RegionalGateway: &infrav1alpha1.RegionalGatewayServiceConfig{
					BaseServiceConfig: infrav1alpha1.BaseServiceConfig{
						Enabled: true,
					},
				},
			},
		},
	}
	dbSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "regional-db",
			Namespace: "sandbox0-system",
		},
		Data: map[string][]byte{
			"password": []byte("secret"),
		},
	}
	client := fake.NewClientBuilder().WithScheme(scheme).WithObjects(dbSecret).Build()
	reconciler := NewReconciler(common.NewResourceManager(client, scheme, nil, common.LocalDevConfig{}))
	compiled := infraplan.Compile(infra)
	compiled.RegionalGateway.DefaultClusterGatewayURL = "http://planned-cluster-gateway:9443"

	cfg, _, err := reconciler.buildConfig(context.Background(), infra, compiled)
	if err != nil {
		t.Fatalf("buildConfig returned error: %v", err)
	}
	if got := cfg.DefaultClusterGatewayURL; got != compiled.RegionalGateway.DefaultClusterGatewayURL {
		t.Fatalf("expected cluster gateway URL %q, got %q", compiled.RegionalGateway.DefaultClusterGatewayURL, got)
	}
}

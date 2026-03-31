package regionalgateway

import (
	"context"
	"strings"
	"testing"

	apiconfig "github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	infrav1alpha1 "github.com/sandbox0-ai/sandbox0/infra-operator/api/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/infra-operator/internal/controller/pkg/common"
	infraplan "github.com/sandbox0-ai/sandbox0/infra-operator/internal/plan"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
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
					WorkloadServiceConfig: infrav1alpha1.WorkloadServiceConfig{
						EnabledServiceConfig: infrav1alpha1.EnabledServiceConfig{Enabled: true},
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

func TestBuildConfigSkipsInitUserForFederatedGlobalAuth(t *testing.T) {
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
			InitUser: &infrav1alpha1.InitUserConfig{
				Email: "admin@example.com",
			},
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
					WorkloadServiceConfig: infrav1alpha1.WorkloadServiceConfig{
						EnabledServiceConfig: infrav1alpha1.EnabledServiceConfig{Enabled: true},
					},
					Config: &infrav1alpha1.RegionalGatewayConfig{
						AuthMode: "federated_global",
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

	cfg, _, err := reconciler.buildConfig(context.Background(), infra, infraplan.Compile(infra))
	if err != nil {
		t.Fatalf("buildConfig returned error: %v", err)
	}
	if cfg.BuiltInAuth.InitUser != nil {
		t.Fatalf("expected init user to be omitted for federated_global mode, got %#v", cfg.BuiltInAuth.InitUser)
	}
}

func TestBuildConfigLeavesInitUserPasswordEmptyForOIDCOnlyBootstrap(t *testing.T) {
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
			InitUser: &infrav1alpha1.InitUserConfig{
				Email: "admin@example.com",
			},
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
					WorkloadServiceConfig: infrav1alpha1.WorkloadServiceConfig{
						EnabledServiceConfig: infrav1alpha1.EnabledServiceConfig{Enabled: true},
					},
					Config: &infrav1alpha1.RegionalGatewayConfig{
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

	cfg, _, err := reconciler.buildConfig(context.Background(), infra, infraplan.Compile(infra))
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

func TestReconcileAppliesServiceAnnotations(t *testing.T) {
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
					WorkloadServiceConfig: infrav1alpha1.WorkloadServiceConfig{
						EnabledServiceConfig: infrav1alpha1.EnabledServiceConfig{Enabled: true},
						Replicas:             1,
					},
					ServiceExposureConfig: infrav1alpha1.ServiceExposureConfig{
						Service: &infrav1alpha1.ServiceNetworkConfig{
							Type: corev1.ServiceTypeLoadBalancer,
							Port: 443,
							Annotations: map[string]string{
								"service.beta.kubernetes.io/aws-load-balancer-ssl-cert":  "arn:aws:acm:us-east-1:123456789012:certificate/example",
								"service.beta.kubernetes.io/aws-load-balancer-ssl-ports": "443",
							},
						},
					},
					Config: &infrav1alpha1.RegionalGatewayConfig{},
				},
			},
		},
	}

	reconciler, client := newRegionalGatewayTestReconciler(t,
		infra.DeepCopy(),
		&corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "regional-db",
				Namespace: infra.Namespace,
			},
			Data: map[string][]byte{
				"password": []byte("secret"),
			},
		},
		&corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "s0cp-sandbox0-internal-jwt-control-plane",
				Namespace: infra.Namespace,
			},
			Data: map[string][]byte{
				"private.key": []byte("private-key"),
				"public.key":  []byte("public-key"),
			},
		},
	)

	if err := reconciler.Reconcile(context.Background(), infra, "sandbox0ai/infra", "latest", nil); err != nil && !strings.Contains(err.Error(), "not ready") {
		t.Fatalf("reconcile returned unexpected error: %v", err)
	}

	service := &corev1.Service{}
	if err := client.Get(context.Background(), types.NamespacedName{
		Name:      "s0cp-regional-gateway",
		Namespace: infra.Namespace,
	}, service); err != nil {
		t.Fatalf("get service: %v", err)
	}

	if service.Spec.Type != corev1.ServiceTypeLoadBalancer {
		t.Fatalf("expected load balancer service, got %s", service.Spec.Type)
	}
	if got := service.Annotations["service.beta.kubernetes.io/aws-load-balancer-ssl-cert"]; got != "arn:aws:acm:us-east-1:123456789012:certificate/example" {
		t.Fatalf("unexpected ssl-cert annotation %q", got)
	}
	if got := service.Annotations["service.beta.kubernetes.io/aws-load-balancer-ssl-ports"]; got != "443" {
		t.Fatalf("unexpected ssl-ports annotation %q", got)
	}
	if len(service.Spec.Ports) != 1 || service.Spec.Ports[0].Port != 443 {
		t.Fatalf("unexpected service ports: %#v", service.Spec.Ports)
	}
}

func newRegionalGatewayTestReconciler(t *testing.T, objects ...runtime.Object) (*Reconciler, ctrlclient.Client) {
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
	if err := networkingv1.AddToScheme(scheme); err != nil {
		t.Fatalf("add networking scheme: %v", err)
	}

	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithRuntimeObjects(objects...).
		Build()

	return NewReconciler(common.NewResourceManager(client, scheme, nil, common.LocalDevConfig{})), client
}

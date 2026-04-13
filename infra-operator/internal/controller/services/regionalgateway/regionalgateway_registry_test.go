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

func TestReconcileKeepsHTTPBackendForHTTPSIngress(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("failed to add core scheme: %v", err)
	}
	if err := appsv1.AddToScheme(scheme); err != nil {
		t.Fatalf("failed to add apps scheme: %v", err)
	}
	if err := infrav1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("failed to add infra scheme: %v", err)
	}

	infra := &infrav1alpha1.Sandbox0Infra{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "demo",
			Namespace: "sandbox0-system",
		},
		Spec: infrav1alpha1.Sandbox0InfraSpec{
			Region: "gcp-ue4",
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
							Type: "ClusterIP",
							Port: 80,
						},
					},
					IngressExposureConfig: infrav1alpha1.IngressExposureConfig{
						Ingress: &infrav1alpha1.IngressConfig{
							Enabled:   true,
							ClassName: "nginx",
							Host:      "gcp-ue4.sandbox0.ai",
							TLSSecret: "gcp-ue4-sandbox0-ai-tls",
						},
					},
					Config: &infrav1alpha1.RegionalGatewayConfig{
						AuthMode: "federated_global",
						GatewayConfig: infrav1alpha1.GatewayConfig{
							BaseURL:         "https://gcp-ue4.sandbox0.ai",
							JWTIssuer:       "https://api.sandbox0.ai",
							JWTPublicKeyPEM: "-----BEGIN PUBLIC KEY-----\nMCowBQYDK2VwAyEA2LS/8P8G8l6hFGxqfQ4WdSx7sY9qsK0kGugHgdGX6lY=\n-----END PUBLIC KEY-----",
						},
					},
				},
				ClusterGateway: &infrav1alpha1.ClusterGatewayServiceConfig{
					WorkloadServiceConfig: infrav1alpha1.WorkloadServiceConfig{
						EnabledServiceConfig: infrav1alpha1.EnabledServiceConfig{Enabled: true},
					},
				},
			},
			PublicExposure: &infrav1alpha1.PublicExposureConfig{
				Enabled:    true,
				RootDomain: "sandbox0.app",
				RegionID:   "gcp-ue4",
			},
		},
	}

	dbSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "regional-db",
			Namespace: infra.Namespace,
		},
		Data: map[string][]byte{
			"password": []byte("db-password"),
		},
	}

	reconciler, client := newRegionalGatewayTestReconciler(t, infra.DeepCopy(), dbSecret)

	if err := reconciler.Reconcile(context.Background(), "sandbox0ai/infra", "latest", infraplan.Compile(infra)); err != nil && !strings.Contains(err.Error(), "not ready") {
		t.Fatalf("reconcile returned unexpected error: %v", err)
	}

	service := &corev1.Service{}
	if err := client.Get(context.Background(), types.NamespacedName{Name: "demo-regional-gateway", Namespace: infra.Namespace}, service); err != nil {
		t.Fatalf("get regional gateway service: %v", err)
	}
	if service.Spec.Type != corev1.ServiceTypeClusterIP {
		t.Fatalf("expected ClusterIP service for ingress backend, got %q", service.Spec.Type)
	}
	if service.Spec.Ports[0].Port != 80 {
		t.Fatalf("expected http service port 80, got %d", service.Spec.Ports[0].Port)
	}
	if service.Spec.Ports[0].Name != "http" {
		t.Fatalf("expected http service port name, got %q", service.Spec.Ports[0].Name)
	}
	if service.Spec.Ports[0].TargetPort.IntVal != 8080 {
		t.Fatalf("expected http target port 8080, got %#v", service.Spec.Ports[0].TargetPort)
	}

	deployment := &appsv1.Deployment{}
	if err := client.Get(context.Background(), types.NamespacedName{Name: "demo-regional-gateway", Namespace: infra.Namespace}, deployment); err != nil {
		t.Fatalf("get regional gateway deployment: %v", err)
	}
	if deployment.Spec.Template.Spec.Containers[0].Ports[0].Name != "http" {
		t.Fatalf("expected http container port name, got %q", deployment.Spec.Template.Spec.Containers[0].Ports[0].Name)
	}
	if deployment.Spec.Template.Spec.Containers[0].ReadinessProbe.HTTPGet.Scheme != corev1.URISchemeHTTP {
		t.Fatalf("expected http readiness probe, got %s", deployment.Spec.Template.Spec.Containers[0].ReadinessProbe.HTTPGet.Scheme)
	}
	if hasVolume(deployment.Spec.Template.Spec.Volumes, "gateway-tls") {
		t.Fatal("did not expect gateway-tls volume for ingress-terminated TLS")
	}

	ingress := &networkingv1.Ingress{}
	if err := client.Get(context.Background(), types.NamespacedName{Name: "demo-regional-gateway", Namespace: infra.Namespace}, ingress); err != nil {
		t.Fatalf("get regional gateway ingress: %v", err)
	}
	if ingress.Spec.IngressClassName == nil || *ingress.Spec.IngressClassName != "nginx" {
		t.Fatalf("expected nginx ingress class, got %#v", ingress.Spec.IngressClassName)
	}
	if len(ingress.Spec.TLS) != 1 || ingress.Spec.TLS[0].SecretName != "gcp-ue4-sandbox0-ai-tls" {
		t.Fatalf("expected ingress TLS secret, got %#v", ingress.Spec.TLS)
	}
	backend := ingress.Spec.Rules[0].HTTP.Paths[0].Backend.Service
	if backend == nil || backend.Name != "demo-regional-gateway" || backend.Port.Number != 80 {
		t.Fatalf("unexpected ingress backend: %#v", backend)
	}
}

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
					Registry:      "123456789012.dkr.ecr.us-east-1.amazonaws.com",
					Region:        "us-east-1",
					RegistryID:    "123456789012",
					AssumeRoleARN: "arn:aws:iam::123456789012:role/sandbox0-ecr-broker",
					ExternalID:    "sandbox0-test",
					PullSecret:    infrav1alpha1.DockerConfigSecretRef{Name: "ecr-pull"},
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
	if cfg.Registry.AWS.AssumeRoleARN != "arn:aws:iam::123456789012:role/sandbox0-ecr-broker" {
		t.Fatalf("unexpected assume role arn: %s", cfg.Registry.AWS.AssumeRoleARN)
	}
	if cfg.Registry.AWS.ExternalID != "sandbox0-test" {
		t.Fatalf("unexpected external id: %s", cfg.Registry.AWS.ExternalID)
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

func TestApplyRegistryConfigGCPWithoutServiceAccountSecret(t *testing.T) {
	r := &Reconciler{}
	infra := &infrav1alpha1.Sandbox0Infra{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "s0cp",
			Namespace: "sandbox0-system",
		},
		Spec: infrav1alpha1.Sandbox0InfraSpec{
			Registry: &infrav1alpha1.RegistryConfig{
				Provider: infrav1alpha1.RegistryProviderGCP,
				GCP: &infrav1alpha1.GCPRegistryConfig{
					Registry: "us-east4-docker.pkg.dev",
				},
			},
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
	if cfg.Registry.GCP == nil {
		t.Fatal("expected gcp registry config")
	}
	if cfg.Registry.GCP.ServiceAccountJSON != "" {
		t.Fatalf("expected empty service account json placeholder, got %q", cfg.Registry.GCP.ServiceAccountJSON)
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

	cfg, _, err := reconciler.buildConfig(context.Background(), compiled)
	if err != nil {
		t.Fatalf("buildConfig returned error: %v", err)
	}
	if got := cfg.DefaultClusterGatewayURL; got != compiled.RegionalGateway.DefaultClusterGatewayURL {
		t.Fatalf("expected cluster gateway URL %q, got %q", compiled.RegionalGateway.DefaultClusterGatewayURL, got)
	}
}

func TestBuildConfigPublishesSSHEndpoint(t *testing.T) {
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
			PublicExposure: &infrav1alpha1.PublicExposureConfig{
				RootDomain: "sandbox0.app",
				RegionID:   "aws-us-east-1",
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
				},
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
	cfg, _, err := reconciler.buildConfig(context.Background(), infraplan.Compile(infra))
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

func TestBuildConfigUsesCompiledPlanForSchedulerRouting(t *testing.T) {
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
				Scheduler: &infrav1alpha1.SchedulerServiceConfig{
					WorkloadServiceConfig: infrav1alpha1.WorkloadServiceConfig{
						EnabledServiceConfig: infrav1alpha1.EnabledServiceConfig{Enabled: true},
					},
					ServiceExposureConfig: infrav1alpha1.ServiceExposureConfig{
						Service: &infrav1alpha1.ServiceNetworkConfig{Port: 8080},
					},
					Config: &infrav1alpha1.SchedulerConfig{HTTPPort: 8080},
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
	ctx := context.Background()
	compiled := infraplan.Compile(infra)
	compiled.Services.Scheduler = infraplan.ServiceReference{URL: "http://planned-scheduler:8080"}

	cfg, _, err := reconciler.buildConfig(ctx, compiled)
	if err != nil {
		t.Fatalf("buildConfig returned error: %v", err)
	}
	if !cfg.SchedulerEnabled {
		t.Fatalf("expected scheduler to be enabled in regional gateway config")
	}
	if cfg.SchedulerURL != compiled.Services.Scheduler.URL {
		t.Fatalf("expected scheduler URL %q, got %q", compiled.Services.Scheduler.URL, cfg.SchedulerURL)
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

	cfg, _, err := reconciler.buildConfig(context.Background(), infraplan.Compile(infra))
	if err != nil {
		t.Fatalf("buildConfig returned error: %v", err)
	}
	if cfg.BuiltInAuth.InitUser != nil {
		t.Fatalf("expected init user to be omitted for federated_global mode, got %#v", cfg.BuiltInAuth.InitUser)
	}
	if cfg.JWTSecret != "" {
		t.Fatalf("expected jwt secret to be empty for federated_global mode, got %q", cfg.JWTSecret)
	}
	if cfg.JWTPublicKeyPEM == "" {
		t.Fatal("expected federated_global mode to populate jwt public key")
	}
	if cfg.JWTIssuer != "global-gateway" {
		t.Fatalf("expected default federated issuer global-gateway, got %q", cfg.JWTIssuer)
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

	cfg, _, err := reconciler.buildConfig(context.Background(), infraplan.Compile(infra))
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

	if err := reconciler.Reconcile(context.Background(), "sandbox0ai/infra", "latest", infraplan.Compile(infra)); err != nil && !strings.Contains(err.Error(), "not ready") {
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

func hasVolume(volumes []corev1.Volume, name string) bool {
	for _, volume := range volumes {
		if volume.Name == name {
			return true
		}
	}
	return false
}

package manager

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	infrav1alpha1 "github.com/sandbox0-ai/sandbox0/infra-operator/api/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/infra-operator/internal/controller/pkg/common"
	infraplan "github.com/sandbox0-ai/sandbox0/infra-operator/internal/plan"
)

func TestCompilePlanSelectsNetworkPolicyProvider(t *testing.T) {
	t.Run("defaults to noop when network is disabled", func(t *testing.T) {
		infra := &infrav1alpha1.Sandbox0Infra{}
		if got := infraplan.Compile(infra).Manager.NetworkPolicyProvider; got != "noop" {
			t.Fatalf("expected noop provider, got %q", got)
		}
	})

	t.Run("uses netd when network is enabled", func(t *testing.T) {
		infra := &infrav1alpha1.Sandbox0Infra{
			Spec: infrav1alpha1.Sandbox0InfraSpec{
				Network: &infrav1alpha1.NetworkConfig{Config: &infrav1alpha1.NetdConfig{}},
			},
		}
		if got := infraplan.Compile(infra).Manager.NetworkPolicyProvider; got != "netd" {
			t.Fatalf("expected ctld network runtime provider, got %q", got)
		}
	})
}

func TestCompilePlanSandboxPodPlacementPrefersSharedPlacement(t *testing.T) {
	infra := &infrav1alpha1.Sandbox0Infra{
		Spec: infrav1alpha1.Sandbox0InfraSpec{
			SandboxNodePlacement: &infrav1alpha1.SandboxNodePlacementConfig{
				NodeSelector: map[string]string{
					"sandbox0.ai/node-role": "shared",
				},
				Tolerations: []corev1.Toleration{
					{
						Key:      "sandbox0.ai/sandbox",
						Operator: corev1.TolerationOpEqual,
						Value:    "true",
						Effect:   corev1.TaintEffectNoSchedule,
					},
				},
			},
		},
	}

	placement := infraplan.Compile(infra).Manager.SandboxPodPlacement
	if got := placement.NodeSelector["sandbox0.ai/node-role"]; got != "shared" {
		t.Fatalf("expected shared placement to win, got %q", got)
	}
	if len(placement.Tolerations) != 1 || placement.Tolerations[0].Key != "sandbox0.ai/sandbox" {
		t.Fatalf("expected shared tolerations, got %#v", placement.Tolerations)
	}
}

func TestBuildConfigPropagatesNetworkMITMCASecretName(t *testing.T) {
	t.Run("uses explicit secret name", func(t *testing.T) {
		reconciler := newManagerTestReconciler(t)
		if err := reconciler.Resources.Client.Create(context.Background(), newValidMITMCASecret(t, "sandbox0-system", "custom-netd-ca")); err != nil {
			t.Fatalf("seed explicit network-runtime MITM CA secret: %v", err)
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
				Network: &infrav1alpha1.NetworkConfig{
					MITMCASecretName: "custom-netd-ca",
					Config:           &infrav1alpha1.NetdConfig{},
				},
			},
		}

		cfg, err := reconciler.buildConfig(context.Background(), "sandbox0/manager", "test", infraplan.Compile(infra))
		if err != nil {
			t.Fatalf("buildConfig returned error: %v", err)
		}
		if cfg.NetdMITMCASecretName != "custom-netd-ca" {
			t.Fatalf("network-runtime MITM CA secret = %q, want custom-netd-ca", cfg.NetdMITMCASecretName)
		}
		if cfg.NetdMITMCASecretNamespace != "sandbox0-system" {
			t.Fatalf("network-runtime MITM CA secret namespace = %q, want sandbox0-system", cfg.NetdMITMCASecretNamespace)
		}
	})

	t.Run("derives managed secret name when network is enabled", func(t *testing.T) {
		reconciler := newManagerTestReconciler(t)
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
				Network: &infrav1alpha1.NetworkConfig{Config: &infrav1alpha1.NetdConfig{}},
			},
		}

		cfg, err := reconciler.buildConfig(context.Background(), "sandbox0/manager", "test", infraplan.Compile(infra))
		if err != nil {
			t.Fatalf("buildConfig returned error: %v", err)
		}
		if cfg.NetdMITMCASecretName != "demo-netd-mitm-ca" {
			t.Fatalf("network-runtime MITM CA secret = %q, want demo-netd-mitm-ca", cfg.NetdMITMCASecretName)
		}
		if cfg.NetdMITMCASecretNamespace != "sandbox0-system" {
			t.Fatalf("network-runtime MITM CA secret namespace = %q, want sandbox0-system", cfg.NetdMITMCASecretNamespace)
		}

		secret := &corev1.Secret{}
		if err := reconciler.Resources.Client.Get(context.Background(), types.NamespacedName{
			Namespace: "sandbox0-system",
			Name:      "demo-netd-mitm-ca",
		}, secret); err != nil {
			t.Fatalf("expected managed network-runtime MITM CA secret to be created: %v", err)
		}
	})
}

func TestBuildConfigPreservesSandboxRuntimeClassName(t *testing.T) {
	reconciler := newManagerTestReconciler(t)
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
				Manager: &infrav1alpha1.ManagerServiceConfig{
					Config: &infrav1alpha1.ManagerConfig{
						SandboxRuntimeClassName: "kata-shared",
					},
				},
			},
		},
	}

	cfg, err := reconciler.buildConfig(context.Background(), "sandbox0/manager", "test", infraplan.Compile(infra))
	if err != nil {
		t.Fatalf("buildConfig returned error: %v", err)
	}
	if cfg.SandboxRuntimeClassName != "kata-shared" {
		t.Fatalf("sandbox runtime class = %q, want kata-shared", cfg.SandboxRuntimeClassName)
	}
}

func TestBuildConfigDerivesProcdBinImageRef(t *testing.T) {
	reconciler := newManagerTestReconciler(t)
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
				Manager: &infrav1alpha1.ManagerServiceConfig{
					Config: &infrav1alpha1.ManagerConfig{},
				},
			},
		},
	}

	cfg, err := reconciler.buildConfig(context.Background(), "sandbox0ai/infra", "test", infraplan.Compile(infra))
	if err != nil {
		t.Fatalf("buildConfig returned error: %v", err)
	}
	if cfg.ManagerImage != "sandbox0ai/infra:test" {
		t.Fatalf("manager image = %q, want sandbox0ai/infra:test", cfg.ManagerImage)
	}
	if cfg.ProcdBinImageRef != "sandbox0ai/infra:test-procd-bin" {
		t.Fatalf("procd bin image ref = %q, want sandbox0ai/infra:test-procd-bin", cfg.ProcdBinImageRef)
	}
}

func TestBuildConfigPreservesExplicitProcdBinImageRef(t *testing.T) {
	reconciler := newManagerTestReconciler(t)
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
				Manager: &infrav1alpha1.ManagerServiceConfig{
					Config: &infrav1alpha1.ManagerConfig{
						ProcdBinImageRef: "registry.example.com/procd-bin:v1",
					},
				},
			},
		},
	}

	cfg, err := reconciler.buildConfig(context.Background(), "sandbox0ai/infra", "test", infraplan.Compile(infra))
	if err != nil {
		t.Fatalf("buildConfig returned error: %v", err)
	}
	if cfg.ProcdBinImageRef != "registry.example.com/procd-bin:v1" {
		t.Fatalf("procd bin image ref = %q, want registry.example.com/procd-bin:v1", cfg.ProcdBinImageRef)
	}
}

func TestBuildConfigEnablesCtldWhenManagerIsEnabled(t *testing.T) {
	reconciler := newManagerTestReconciler(t)
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
				Manager: &infrav1alpha1.ManagerServiceConfig{
					WorkloadServiceConfig: infrav1alpha1.WorkloadServiceConfig{
						EnabledServiceConfig: infrav1alpha1.EnabledServiceConfig{Enabled: true},
					},
				},
			},
		},
	}

	cfg, err := reconciler.buildConfig(context.Background(), "sandbox0/manager", "test", infraplan.Compile(infra))
	if err != nil {
		t.Fatalf("buildConfig returned error: %v", err)
	}
	if !cfg.CtldEnabled {
		t.Fatal("expected ctld to be enabled when manager data-plane services are enabled")
	}
	if cfg.CtldPort != 8095 {
		t.Fatalf("ctld port = %d, want 8095", cfg.CtldPort)
	}
}

func TestBuildConfigInjectsRootFSObjectStorage(t *testing.T) {
	reconciler := newManagerTestReconciler(t)
	requireSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "s3-credentials",
			Namespace: "sandbox0-system",
		},
		Data: map[string][]byte{
			"accessKeyId":     []byte("access-key"),
			"secretAccessKey": []byte("secret-key"),
			"sessionToken":    []byte("session-token"),
		},
	}
	if err := reconciler.Resources.Client.Create(context.Background(), requireSecret); err != nil {
		t.Fatalf("seed s3 credentials: %v", err)
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
			Storage: &infrav1alpha1.StorageConfig{
				Type: infrav1alpha1.StorageTypeS3,
				S3: &infrav1alpha1.S3StorageConfig{
					Bucket:          "rootfs-bucket",
					Region:          "us-east-1",
					Endpoint:        "https://s3.example.com",
					SessionTokenKey: "sessionToken",
					CredentialsSecret: infrav1alpha1.S3CredentialsSecret{
						Name: "s3-credentials",
					},
				},
			},
		},
	}

	cfg, err := reconciler.buildConfig(context.Background(), "sandbox0/manager", "test", infraplan.Compile(infra))
	if err != nil {
		t.Fatalf("buildConfig returned error: %v", err)
	}
	if cfg.RootFSObjectStorage.Type != string(infrav1alpha1.StorageTypeS3) {
		t.Fatalf("rootfs object storage type = %q, want s3", cfg.RootFSObjectStorage.Type)
	}
	if cfg.RootFSObjectStorage.Bucket != "rootfs-bucket" || cfg.RootFSObjectStorage.Endpoint != "https://s3.example.com" {
		t.Fatalf("unexpected rootfs object storage: %#v", cfg.RootFSObjectStorage)
	}
	if cfg.RootFSObjectStorage.AccessKey != "access-key" || cfg.RootFSObjectStorage.SecretKey != "secret-key" || cfg.RootFSObjectStorage.SessionToken != "session-token" {
		t.Fatalf("unexpected rootfs object storage credentials: %#v", cfg.RootFSObjectStorage)
	}
}

func TestReconcileStorageRuntimeUsesManagerBudgetAndService(t *testing.T) {
	reconciler := newManagerTestReconciler(t)
	ctx := context.Background()
	infra := &infrav1alpha1.Sandbox0Infra{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "sandbox0-system", UID: types.UID("demo-uid")},
		Spec: infrav1alpha1.Sandbox0InfraSpec{
			Database: &infrav1alpha1.DatabaseConfig{
				Type: infrav1alpha1.DatabaseTypeBuiltin,
				Builtin: &infrav1alpha1.BuiltinDatabaseConfig{
					Enabled: true, Port: 5432, Username: "sandbox0", Database: "sandbox0", SSLMode: "disable",
				},
			},
			Storage: &infrav1alpha1.StorageConfig{
				Type: infrav1alpha1.StorageTypeBuiltin,
				Builtin: &infrav1alpha1.BuiltinStorageConfig{
					Enabled: true, Bucket: "sandbox0", Region: "us-east-1",
				},
				Runtime: &infrav1alpha1.StorageProxyConfig{
					HTTPPort: 8081, CacheSizeLimit: "512Mi", LogSizeLimit: "64Mi",
				},
			},
			Services: &infrav1alpha1.ServicesConfig{
				Manager: &infrav1alpha1.ManagerServiceConfig{
					WorkloadServiceConfig: infrav1alpha1.WorkloadServiceConfig{
						EnabledServiceConfig: infrav1alpha1.EnabledServiceConfig{Enabled: true},
						Replicas:             1,
						Resources: &corev1.ResourceRequirements{
							Requests: corev1.ResourceList{
								corev1.ResourceCPU:    resource.MustParse("75m"),
								corev1.ResourceMemory: resource.MustParse("192Mi"),
							},
							Limits: corev1.ResourceList{
								corev1.ResourceCPU:    resource.MustParse("1"),
								corev1.ResourceMemory: resource.MustParse("1Gi"),
							},
						},
					},
					Config: &infrav1alpha1.ManagerConfig{HTTPPort: 8080, MetricsPort: 9090},
				},
			},
		},
	}
	if err := reconciler.Resources.Client.Create(ctx, &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "demo-sandbox0-rustfs-credentials", Namespace: infra.Namespace},
		Data: map[string][]byte{
			"endpoint":          []byte("http://demo-rustfs.sandbox0-system.svc:9000"),
			"RUSTFS_ACCESS_KEY": []byte("access-key"),
			"RUSTFS_SECRET_KEY": []byte("secret-key"),
		},
	}); err != nil {
		t.Fatal(err)
	}

	compiled := infraplan.Compile(infra)
	if err := reconciler.Reconcile(ctx, "sandbox0ai/infra", "test", compiled); err == nil || !strings.Contains(err.Error(), "not ready") {
		t.Fatalf("first Reconcile() error = %v, want rollout not ready", err)
	}

	deployment := &appsv1.Deployment{}
	if err := reconciler.Resources.Client.Get(ctx, types.NamespacedName{Name: "demo-manager", Namespace: infra.Namespace}, deployment); err != nil {
		t.Fatal(err)
	}
	if deployment.Spec.Replicas == nil || *deployment.Spec.Replicas != 1 {
		t.Fatalf("manager replicas = %v, want 1", deployment.Spec.Replicas)
	}
	resources := deployment.Spec.Template.Spec.Containers[0].Resources
	if got := resources.Requests.Cpu().String(); got != "75m" {
		t.Fatalf("manager CPU request = %q, want 75m", got)
	}
	if got := resources.Requests.Memory().String(); got != "192Mi" {
		t.Fatalf("manager memory request = %q, want 192Mi", got)
	}
	if got := resources.Limits.Cpu().String(); got != "1" {
		t.Fatalf("manager CPU limit = %q, want 1", got)
	}
	if got := resources.Limits.Memory().String(); got != "1Gi" {
		t.Fatalf("manager memory limit = %q, want 1Gi", got)
	}

	service := &corev1.Service{}
	if err := reconciler.Resources.Client.Get(ctx, types.NamespacedName{Name: "demo-manager", Namespace: infra.Namespace}, service); err != nil {
		t.Fatal(err)
	}
	assertManagerServicePort(t, service, "storage-http", 8081, 8081)
}

func TestResolveStorageRuntimeHTTPPortRemapsManagerListeners(t *testing.T) {
	for _, tc := range []struct {
		name string
		port int32
	}{
		{name: "http", port: 8080},
		{name: "metrics", port: 9090},
		{name: "webhook", port: 9443},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := resolveStorageRuntimeHTTPPort(tc.port, 8080, 9090, 9443)
			if err != nil {
				t.Fatalf("resolve port %d: %v", tc.port, err)
			}
			if got != int(storageRuntimeFallbackPort) {
				t.Fatalf("resolve port %d = %d, want %d", tc.port, got, storageRuntimeFallbackPort)
			}
		})
	}
	got, err := resolveStorageRuntimeHTTPPort(8081, 8080, 9090, 9443)
	if err != nil || got != 8081 {
		t.Fatalf("non-conflicting storage port = %d, %v; want 8081, nil", got, err)
	}
	got, err = resolveStorageRuntimeHTTPPort(8080, 8080, 9090, 9443, storageRuntimeFallbackPort)
	if err != nil || got != int(storageRuntimeFallbackPort+1) {
		t.Fatalf("occupied fallback port = %d, %v; want %d, nil", got, err, storageRuntimeFallbackPort+1)
	}
}

func TestValidateManagerServicePortsRejectsDuplicatePorts(t *testing.T) {
	err := validateManagerServicePorts([]corev1.ServicePort{
		{Name: "metrics", Port: 9090},
		{Name: "storage-http", Port: 9090},
	})
	if err == nil || !strings.Contains(err.Error(), "manager Service port 9090 is used by both metrics and storage-http") {
		t.Fatalf("validateManagerServicePorts() error = %v", err)
	}
}

func newManagerTestReconciler(t *testing.T) *Reconciler {
	t.Helper()

	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("add core scheme: %v", err)
	}
	if err := infrav1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add infra scheme: %v", err)
	}
	if err := appsv1.AddToScheme(scheme); err != nil {
		t.Fatalf("add apps scheme: %v", err)
	}

	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(&corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "demo-sandbox0-database-credentials",
				Namespace: "sandbox0-system",
			},
			Data: map[string][]byte{
				"username": []byte("sandbox0"),
				"password": []byte("db-password"),
				"database": []byte("sandbox0"),
				"port":     []byte("5432"),
			},
		}).
		Build()
	return NewReconciler(common.NewResourceManager(client, scheme, nil, common.LocalDevConfig{}))
}

func assertManagerServicePort(t *testing.T, service *corev1.Service, name string, wantPort, wantTarget int32) {
	t.Helper()
	for _, port := range service.Spec.Ports {
		if port.Name == name {
			if port.Port != wantPort || int32(port.TargetPort.IntValue()) != wantTarget {
				t.Fatalf("service port %s = %d->%d, want %d->%d", name, port.Port, port.TargetPort.IntValue(), wantPort, wantTarget)
			}
			return
		}
	}
	t.Fatalf("service port %s not found", name)
}

func newValidMITMCASecret(t *testing.T, namespace, name string) *corev1.Secret {
	t.Helper()

	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate RSA key: %v", err)
	}
	serialNumber, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		t.Fatalf("generate serial: %v", err)
	}

	now := time.Now().UTC()
	template := &x509.Certificate{
		SerialNumber: serialNumber,
		Subject: pkix.Name{
			CommonName:   "test-netd-mitm-ca",
			Organization: []string{"sandbox0"},
		},
		NotBefore:             now.Add(-time.Hour),
		NotAfter:              now.Add(time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
		IsCA:                  true,
		MaxPathLenZero:        true,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &privateKey.PublicKey, privateKey)
	if err != nil {
		t.Fatalf("create certificate: %v", err)
	}

	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Type: corev1.SecretTypeOpaque,
		Data: map[string][]byte{
			"ca.crt": pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
			"ca.key": pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(privateKey)}),
		},
	}
}

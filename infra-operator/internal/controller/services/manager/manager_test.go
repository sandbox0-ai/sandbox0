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
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	infrav1alpha1 "github.com/sandbox0-ai/sandbox0/infra-operator/api/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/infra-operator/internal/controller/pkg/common"
	infraplan "github.com/sandbox0-ai/sandbox0/infra-operator/internal/plan"
)

func TestCompilePlanDefaultsToNoopNetworkPolicyProviderWhenNetdIsDisabled(t *testing.T) {
	t.Run("defaults to noop when netd is disabled", func(t *testing.T) {
		infra := &infrav1alpha1.Sandbox0Infra{}
		if got := infraplan.Compile(infra).Manager.NetworkPolicyProvider; got != "noop" {
			t.Fatalf("expected noop provider, got %q", got)
		}
	})

	t.Run("uses netd when netd is enabled", func(t *testing.T) {
		infra := &infrav1alpha1.Sandbox0Infra{
			Spec: infrav1alpha1.Sandbox0InfraSpec{
				Services: &infrav1alpha1.ServicesConfig{
					Netd: &infrav1alpha1.NetdServiceConfig{
						EnabledServiceConfig: infrav1alpha1.EnabledServiceConfig{
							Enabled: true,
						},
					},
				},
			},
		}
		if got := infraplan.Compile(infra).Manager.NetworkPolicyProvider; got != "netd" {
			t.Fatalf("expected netd provider, got %q", got)
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
			Services: &infrav1alpha1.ServicesConfig{
				Netd: &infrav1alpha1.NetdServiceConfig{
					NodeSelector: map[string]string{
						"sandbox0.ai/node-role": "legacy",
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

func TestBuildConfigPropagatesNetdMITMCASecretName(t *testing.T) {
	t.Run("uses explicit secret name", func(t *testing.T) {
		reconciler := newManagerTestReconciler(t)
		if err := reconciler.Resources.Client.Create(context.Background(), newValidMITMCASecret(t, "sandbox0-system", "custom-netd-ca")); err != nil {
			t.Fatalf("seed explicit netd mitm ca secret: %v", err)
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
					Netd: &infrav1alpha1.NetdServiceConfig{
						EnabledServiceConfig: infrav1alpha1.EnabledServiceConfig{Enabled: true},
						MITMCASecretName:     "custom-netd-ca",
					},
				},
			},
		}

		cfg, err := reconciler.buildConfig(context.Background(), "sandbox0/manager", "test", infraplan.Compile(infra))
		if err != nil {
			t.Fatalf("buildConfig returned error: %v", err)
		}
		if cfg.NetdMITMCASecretName != "custom-netd-ca" {
			t.Fatalf("netd mitm ca secret = %q, want custom-netd-ca", cfg.NetdMITMCASecretName)
		}
		if cfg.NetdMITMCASecretNamespace != "sandbox0-system" {
			t.Fatalf("netd mitm ca secret namespace = %q, want sandbox0-system", cfg.NetdMITMCASecretNamespace)
		}
	})

	t.Run("derives managed secret name when netd is enabled", func(t *testing.T) {
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
					Netd: &infrav1alpha1.NetdServiceConfig{
						EnabledServiceConfig: infrav1alpha1.EnabledServiceConfig{Enabled: true},
					},
				},
			},
		}

		cfg, err := reconciler.buildConfig(context.Background(), "sandbox0/manager", "test", infraplan.Compile(infra))
		if err != nil {
			t.Fatalf("buildConfig returned error: %v", err)
		}
		if cfg.NetdMITMCASecretName != "demo-netd-mitm-ca" {
			t.Fatalf("netd mitm ca secret = %q, want demo-netd-mitm-ca", cfg.NetdMITMCASecretName)
		}
		if cfg.NetdMITMCASecretNamespace != "sandbox0-system" {
			t.Fatalf("netd mitm ca secret namespace = %q, want sandbox0-system", cfg.NetdMITMCASecretNamespace)
		}

		secret := &corev1.Secret{}
		if err := reconciler.Resources.Client.Get(context.Background(), types.NamespacedName{
			Namespace: "sandbox0-system",
			Name:      "demo-netd-mitm-ca",
		}, secret); err != nil {
			t.Fatalf("expected managed netd mitm ca secret to be created: %v", err)
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

func TestReconcileStorageOnlyUsesManagerHostAndSwitchesAliasAfterFullRollout(t *testing.T) {
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
			},
			Services: &infrav1alpha1.ServicesConfig{
				Manager: &infrav1alpha1.ManagerServiceConfig{
					WorkloadServiceConfig: infrav1alpha1.WorkloadServiceConfig{
						EnabledServiceConfig: infrav1alpha1.EnabledServiceConfig{Enabled: false},
						Replicas:             9,
					},
					Config: &infrav1alpha1.ManagerConfig{HTTPPort: 8080, MetricsPort: 9090},
				},
				StorageProxy: &infrav1alpha1.StorageProxyServiceConfig{
					WorkloadServiceConfig: infrav1alpha1.WorkloadServiceConfig{
						EnabledServiceConfig: infrav1alpha1.EnabledServiceConfig{Enabled: true},
						Replicas:             1,
					},
					ServiceExposureConfig: infrav1alpha1.ServiceExposureConfig{Service: &infrav1alpha1.ServiceNetworkConfig{
						Port:        18083,
						Annotations: map[string]string{"service.test/alias": "storage"},
					}},
					Config: &infrav1alpha1.StorageProxyConfig{
						// A standalone storage-proxy could share manager's port because it
						// ran in a different Pod. Embedding must remap only the target.
						HTTPPort: 8080, MetricsPort: 19090,
						CacheSizeLimit: "512Mi", LogSizeLimit: "64Mi", ObjectEncryptionEnabled: true,
					},
				},
			},
		},
	}
	for _, object := range []ctrlclient.Object{
		&corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: "demo-sandbox0-rustfs-credentials", Namespace: infra.Namespace},
			Data: map[string][]byte{
				"endpoint": []byte("http://demo-rustfs.sandbox0-system.svc:9000"), "RUSTFS_ACCESS_KEY": []byte("access-key"), "RUSTFS_SECRET_KEY": []byte("secret-key"),
			},
		},
		&corev1.Service{
			ObjectMeta: metav1.ObjectMeta{Name: "demo-storage-proxy", Namespace: infra.Namespace},
			Spec: corev1.ServiceSpec{
				Selector: common.GetServiceLabels(infra.Name, "storage-proxy"),
				Ports:    []corev1.ServicePort{common.BuildServicePort("http", 18083, 8080, corev1.ServiceTypeClusterIP)},
			},
		},
	} {
		if err := reconciler.Resources.Client.Create(ctx, object); err != nil {
			t.Fatal(err)
		}
	}

	compiled := infraplan.Compile(infra)
	err := reconciler.Reconcile(ctx, "sandbox0ai/infra", "test", compiled)
	if err == nil || !strings.Contains(err.Error(), "not ready") {
		t.Fatalf("first Reconcile() error = %v, want rollout not ready", err)
	}

	deployment := &appsv1.Deployment{}
	if err := reconciler.Resources.Client.Get(ctx, types.NamespacedName{Name: "demo-manager", Namespace: infra.Namespace}, deployment); err != nil {
		t.Fatal(err)
	}
	if deployment.Spec.Replicas == nil || *deployment.Spec.Replicas != 1 {
		t.Fatalf("storage-only manager host replicas = %v, want 1", deployment.Spec.Replicas)
	}
	container := deployment.Spec.Template.Spec.Containers[0]
	assertUniqueManagerPodNames(t, container.Ports, container.VolumeMounts, deployment.Spec.Template.Spec.Volumes)
	assertManagerEnv(t, container.Env, "STORAGE_PROXY_CONFIG_PATH", embeddedStorageConfigPath)
	assertManagerPort(t, container.Ports, "storage-http", 18081)
	assertManagerVolumeMount(t, container.VolumeMounts, "storage-config", embeddedStorageConfigPath)
	assertManagerVolumeMount(t, container.VolumeMounts, "storage-cache", "/var/lib/storage-proxy/cache")
	assertManagerVolumeMount(t, container.VolumeMounts, "storage-logs", "/var/log/storage-proxy")
	assertManagerVolumeMount(t, container.VolumeMounts, "object-encryption-key", common.ObjectEncryptionMountDir)
	if deployment.Spec.Template.Annotations[managerConfigHashAnnotation] == "" || deployment.Spec.Template.Annotations[storageConfigHashAnnotation] == "" {
		t.Fatalf("pod annotations do not contain both config hashes: %#v", deployment.Spec.Template.Annotations)
	}
	assertManagerEmptyDirSizeLimit(t, deployment.Spec.Template.Spec.Volumes, "storage-cache", "512Mi")
	assertManagerEmptyDirSizeLimit(t, deployment.Spec.Template.Spec.Volumes, "storage-logs", "64Mi")
	if got := container.Resources.Requests.Cpu().String(); got != "200m" {
		t.Fatalf("combined CPU request = %q, want 200m", got)
	}
	if got := container.Resources.Requests.Memory().String(); got != "512Mi" {
		t.Fatalf("combined memory request = %q, want 512Mi", got)
	}
	if got := container.Resources.Limits.Cpu().String(); got != "1" {
		t.Fatalf("combined CPU limit = %q, want 1", got)
	}
	if got := container.Resources.Limits.Memory().String(); got != "1Gi" {
		t.Fatalf("combined memory limit = %q, want 1Gi", got)
	}

	alias := &corev1.Service{}
	if err := reconciler.Resources.Client.Get(ctx, types.NamespacedName{Name: "demo-storage-proxy", Namespace: infra.Namespace}, alias); err != nil {
		t.Fatal(err)
	}
	if got := alias.Spec.Selector["app.kubernetes.io/component"]; got != "storage-proxy" {
		t.Fatalf("alias switched before rollout: component selector = %q", got)
	}
	initialPodConfigHash := deployment.Spec.Template.Annotations[common.PodTemplateConfigHashAnnotation]
	infra.Spec.Services.StorageProxy.Config.CacheSizeLimit = "768Mi"
	compiled = infraplan.Compile(infra)
	if err := reconciler.Reconcile(ctx, "sandbox0ai/infra", "test", compiled); err == nil || !strings.Contains(err.Error(), "not ready") {
		t.Fatalf("storage config update Reconcile() error = %v, want rollout not ready", err)
	}
	if err := reconciler.Resources.Client.Get(ctx, types.NamespacedName{Name: "demo-manager", Namespace: infra.Namespace}, deployment); err != nil {
		t.Fatal(err)
	}
	if got := deployment.Spec.Template.Annotations[common.PodTemplateConfigHashAnnotation]; got == initialPodConfigHash {
		t.Fatalf("storage-only config change did not update manager pod hash %q", got)
	}

	deployment.Status.ObservedGeneration = deployment.Generation
	deployment.Status.Replicas = 2
	deployment.Status.UpdatedReplicas = 1
	deployment.Status.ReadyReplicas = 1
	deployment.Status.AvailableReplicas = 1
	if err := reconciler.Resources.Client.Status().Update(ctx, deployment); err != nil {
		t.Fatal(err)
	}
	if err := reconciler.Reconcile(ctx, "sandbox0ai/infra", "test", compiled); err == nil || !strings.Contains(err.Error(), "rollout pending") {
		t.Fatalf("surge Reconcile() error = %v, want rollout pending", err)
	}
	if err := reconciler.Resources.Client.Get(ctx, types.NamespacedName{Name: "demo-storage-proxy", Namespace: infra.Namespace}, alias); err != nil {
		t.Fatal(err)
	}
	if got := alias.Spec.Selector["app.kubernetes.io/component"]; got != "storage-proxy" {
		t.Fatalf("alias switched while old pod remained: component selector = %q", got)
	}

	if err := reconciler.Resources.Client.Get(ctx, types.NamespacedName{Name: "demo-manager", Namespace: infra.Namespace}, deployment); err != nil {
		t.Fatal(err)
	}
	deployment.Status.ObservedGeneration = deployment.Generation
	deployment.Status.Replicas = 1
	deployment.Status.UpdatedReplicas = 1
	deployment.Status.ReadyReplicas = 1
	deployment.Status.AvailableReplicas = 1
	deployment.Status.UnavailableReplicas = 0
	if err := reconciler.Resources.Client.Status().Update(ctx, deployment); err != nil {
		t.Fatal(err)
	}
	if err := reconciler.Reconcile(ctx, "sandbox0ai/infra", "test", compiled); err != nil {
		t.Fatalf("complete Reconcile() error = %v", err)
	}
	if err := reconciler.Resources.Client.Get(ctx, types.NamespacedName{Name: "demo-storage-proxy", Namespace: infra.Namespace}, alias); err != nil {
		t.Fatal(err)
	}
	if got := alias.Spec.Selector["app.kubernetes.io/component"]; got != "manager" {
		t.Fatalf("alias component selector = %q, want manager", got)
	}
	if got := alias.Annotations["service.test/alias"]; got != "storage" {
		t.Fatalf("alias annotation = %q, want storage", got)
	}
	assertManagerServicePort(t, alias, "http", 18083, 18081)
	assertManagerServicePort(t, alias, "metrics", 19090, 9090)
}

func TestReconcileCanonicalStorageUsesManagerBudgetAndService(t *testing.T) {
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
				StorageProxy: &infrav1alpha1.StorageProxyServiceConfig{
					WorkloadServiceConfig: infrav1alpha1.WorkloadServiceConfig{
						EnabledServiceConfig: infrav1alpha1.EnabledServiceConfig{Enabled: true},
						Replicas:             4,
						Resources: &corev1.ResourceRequirements{Requests: corev1.ResourceList{
							corev1.ResourceCPU: resource.MustParse("900m"),
						}},
					},
					Config: &infrav1alpha1.StorageProxyConfig{HTTPPort: 18081},
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
	if err := reconciler.Resources.Client.Get(ctx, types.NamespacedName{Name: "demo-storage-proxy", Namespace: infra.Namespace}, &corev1.Service{}); err == nil {
		t.Fatal("canonical storage runtime unexpectedly created the deprecated alias Service")
	}
}

func TestResolveEmbeddedStorageHTTPPortRemapsManagerListeners(t *testing.T) {
	for _, tc := range []struct {
		name string
		port int32
	}{
		{name: "http", port: 8080},
		{name: "metrics", port: 9090},
		{name: "webhook", port: 9443},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := resolveEmbeddedStorageHTTPPort(tc.port, 8080, 9090, 9443)
			if err != nil {
				t.Fatalf("resolve port %d: %v", tc.port, err)
			}
			if got != int(embeddedStorageFallbackPort) {
				t.Fatalf("resolve port %d = %d, want %d", tc.port, got, embeddedStorageFallbackPort)
			}
		})
	}
	got, err := resolveEmbeddedStorageHTTPPort(8081, 8080, 9090, 9443)
	if err != nil || got != 8081 {
		t.Fatalf("non-conflicting storage port = %d, %v; want 8081, nil", got, err)
	}
	got, err = resolveEmbeddedStorageHTTPPort(8080, 8080, 9090, 9443, embeddedStorageFallbackPort)
	if err != nil || got != int(embeddedStorageFallbackPort+1) {
		t.Fatalf("occupied fallback port = %d, %v; want %d, nil", got, err, embeddedStorageFallbackPort+1)
	}
}

func TestResolveManagerResourcesPreservesCanonicalProcessBudget(t *testing.T) {
	explicit := &corev1.ResourceRequirements{
		Requests: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("75m"),
			corev1.ResourceMemory: resource.MustParse("192Mi"),
		},
		Limits: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("1"),
			corev1.ResourceMemory: resource.MustParse("1Gi"),
		},
	}
	resolved := resolveManagerResources(explicit, nil, true, false)
	if got := resolved.Requests.Cpu().String(); got != "75m" {
		t.Fatalf("explicit canonical CPU request = %q, want 75m", got)
	}
	if got := resolved.Requests.Memory().String(); got != "192Mi" {
		t.Fatalf("explicit canonical memory request = %q, want 192Mi", got)
	}

	resolved = resolveManagerResources(nil, nil, true, false)
	if got := resolved.Requests.Cpu().String(); got != "200m" {
		t.Fatalf("default canonical CPU request = %q, want legacy-equivalent 200m", got)
	}
	if got := resolved.Requests.Memory().String(); got != "512Mi" {
		t.Fatalf("default canonical memory request = %q, want legacy-equivalent 512Mi", got)
	}
	if got := resolved.Limits.Cpu().String(); got != "1" {
		t.Fatalf("default canonical CPU limit = %q, want legacy-equivalent 1", got)
	}
	if got := resolved.Limits.Memory().String(); got != "1Gi" {
		t.Fatalf("default canonical memory limit = %q, want legacy-equivalent 1Gi", got)
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

func assertManagerEnv(t *testing.T, env []corev1.EnvVar, name, want string) {
	t.Helper()
	for _, item := range env {
		if item.Name == name {
			if item.Value != want {
				t.Fatalf("env %s = %q, want %q", name, item.Value, want)
			}
			return
		}
	}
	t.Fatalf("env %s not found", name)
}

func assertManagerPort(t *testing.T, ports []corev1.ContainerPort, name string, want int32) {
	t.Helper()
	seenNames := make(map[string]struct{}, len(ports))
	for _, port := range ports {
		if _, exists := seenNames[port.Name]; exists {
			t.Fatalf("duplicate container port name %q", port.Name)
		}
		seenNames[port.Name] = struct{}{}
		if port.Name == name {
			if port.ContainerPort != want {
				t.Fatalf("container port %s = %d, want %d", name, port.ContainerPort, want)
			}
			return
		}
	}
	t.Fatalf("container port %s not found", name)
}

func assertManagerVolumeMount(t *testing.T, mounts []corev1.VolumeMount, name, wantPath string) {
	t.Helper()
	for _, mount := range mounts {
		if mount.Name == name {
			if mount.MountPath != wantPath {
				t.Fatalf("mount %s path = %q, want %q", name, mount.MountPath, wantPath)
			}
			return
		}
	}
	t.Fatalf("mount %s not found", name)
}

func assertManagerEmptyDirSizeLimit(t *testing.T, volumes []corev1.Volume, name, want string) {
	t.Helper()
	for _, volume := range volumes {
		if volume.Name == name {
			if volume.EmptyDir == nil || volume.EmptyDir.SizeLimit == nil {
				t.Fatalf("volume %s has no emptyDir size limit", name)
			}
			if got := volume.EmptyDir.SizeLimit.String(); got != want {
				t.Fatalf("volume %s size limit = %q, want %q", name, got, want)
			}
			return
		}
	}
	t.Fatalf("volume %s not found", name)
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

func assertUniqueManagerPodNames(t *testing.T, ports []corev1.ContainerPort, mounts []corev1.VolumeMount, volumes []corev1.Volume) {
	t.Helper()
	portNames := make(map[string]struct{}, len(ports))
	for _, port := range ports {
		if _, exists := portNames[port.Name]; exists {
			t.Fatalf("duplicate container port name %q", port.Name)
		}
		portNames[port.Name] = struct{}{}
	}
	mountNames := make(map[string]struct{}, len(mounts))
	for _, mount := range mounts {
		if _, exists := mountNames[mount.Name]; exists {
			t.Fatalf("duplicate volume mount name %q", mount.Name)
		}
		mountNames[mount.Name] = struct{}{}
	}
	volumeNames := make(map[string]struct{}, len(volumes))
	for _, volume := range volumes {
		if _, exists := volumeNames[volume.Name]; exists {
			t.Fatalf("duplicate volume name %q", volume.Name)
		}
		volumeNames[volume.Name] = struct{}{}
	}
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

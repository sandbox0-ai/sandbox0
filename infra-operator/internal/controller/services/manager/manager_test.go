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
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	config "github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	infrav1alpha1 "github.com/sandbox0-ai/sandbox0/infra-operator/api/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/infra-operator/internal/controller/pkg/common"
	infraplan "github.com/sandbox0-ai/sandbox0/infra-operator/internal/plan"
	"github.com/sandbox0-ai/sandbox0/pkg/runtimecontrol"
)

func TestCompilePlanSelectsNetworkPolicyProvider(t *testing.T) {
	t.Run("defaults to noop when network is disabled", func(t *testing.T) {
		infra := &infrav1alpha1.Sandbox0Infra{}
		if got := infraplan.Compile(infra).Manager.NetworkPolicyProvider; got != "noop" {
			t.Fatalf("expected noop provider, got %q", got)
		}
	})

	t.Run("uses ctld when network is enabled", func(t *testing.T) {
		infra := &infrav1alpha1.Sandbox0Infra{
			Spec: infrav1alpha1.Sandbox0InfraSpec{
				Network: &infrav1alpha1.NetworkConfig{Config: &infrav1alpha1.NetworkRuntimeConfig{}},
			},
		}
		if got := infraplan.Compile(infra).Manager.NetworkPolicyProvider; got != "ctld" {
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
				PreferredNodeSelector: map[string]string{
					"sandbox0.ai/capacity-type": "fixed",
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
	if got := placement.PreferredNodeSelector["sandbox0.ai/capacity-type"]; got != "fixed" {
		t.Fatalf("expected fixed preferred placement, got %q", got)
	}
}

func TestBuildConfigPropagatesNetworkMITMCASecretName(t *testing.T) {
	t.Run("uses explicit secret name", func(t *testing.T) {
		reconciler := newManagerTestReconciler(t)
		if err := reconciler.Resources.Client.Create(context.Background(), newValidMITMCASecret(t, "sandbox0-system", "custom-ctld-network-ca")); err != nil {
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
					MITMCASecretName: "custom-ctld-network-ca",
					Config:           &infrav1alpha1.NetworkRuntimeConfig{},
				},
			},
		}

		cfg, err := reconciler.buildConfig(context.Background(), "sandbox0/manager", "test", infraplan.Compile(infra))
		if err != nil {
			t.Fatalf("buildConfig returned error: %v", err)
		}
		if cfg.NetworkMITMCASecretName != "custom-ctld-network-ca" {
			t.Fatalf("network-runtime MITM CA secret = %q, want custom-ctld-network-ca", cfg.NetworkMITMCASecretName)
		}
		if cfg.NetworkMITMCASecretNamespace != "sandbox0-system" {
			t.Fatalf("network-runtime MITM CA secret namespace = %q, want sandbox0-system", cfg.NetworkMITMCASecretNamespace)
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
				Network: &infrav1alpha1.NetworkConfig{Config: &infrav1alpha1.NetworkRuntimeConfig{}},
			},
		}

		cfg, err := reconciler.buildConfig(context.Background(), "sandbox0/manager", "test", infraplan.Compile(infra))
		if err != nil {
			t.Fatalf("buildConfig returned error: %v", err)
		}
		if cfg.NetworkMITMCASecretName != "demo-ctld-network-mitm-ca" {
			t.Fatalf("network-runtime MITM CA secret = %q, want demo-ctld-network-mitm-ca", cfg.NetworkMITMCASecretName)
		}
		if cfg.NetworkMITMCASecretNamespace != "sandbox0-system" {
			t.Fatalf("network-runtime MITM CA secret namespace = %q, want sandbox0-system", cfg.NetworkMITMCASecretNamespace)
		}

		secret := &corev1.Secret{}
		if err := reconciler.Resources.Client.Get(context.Background(), types.NamespacedName{
			Namespace: "sandbox0-system",
			Name:      "demo-ctld-network-mitm-ca",
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
	if cfg.CtldRuntimeWatchPort != runtimecontrol.DefaultCtldWatchPort {
		t.Fatalf("ctld runtime watch port = %d, want %d", cfg.CtldRuntimeWatchPort, runtimecontrol.DefaultCtldWatchPort)
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

func TestReconcileUsesManagerBudgetAndRootFSObjectStorage(t *testing.T) {
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
						EnabledServiceConfig: infrav1alpha1.EnabledServiceConfig{Enabled: true},
						Replicas:             2,
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
					Config: &infrav1alpha1.ManagerConfig{
						HTTPPort: 8080, MetricsPort: 9090, SandboxRuntimeBackend: "nomad",
						NodeAuthority: infrav1alpha1.NodeAuthorityConfig{
							Enabled: true, Port: 8421, TLSSecretName: "manager-node-tls",
							Identities: []infrav1alpha1.NodeAuthorityIdentityConfig{{
								CommonName: "node-agent", ClusterID: "cluster-1", NodeID: "node-1",
								NodeUID: "node-uid-1", PodUID: "agent-1",
							}},
							Claim: infrav1alpha1.RuntimeSlotClaimConfig{SecretName: "manager-nomad-claim"},
							Terminal: infrav1alpha1.RuntimeSlotTerminalConfig{
								Enabled: true, ControlSecretName: "manager-nomad-control",
							},
						},
					},
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
	if deployment.Spec.Replicas == nil || *deployment.Spec.Replicas != 2 {
		t.Fatalf("manager replicas = %v, want 2", deployment.Spec.Replicas)
	}
	leaderElectionName := ""
	for _, env := range deployment.Spec.Template.Spec.Containers[0].Env {
		if env.Name == "MANAGER_LEADER_ELECTION_NAME" {
			leaderElectionName = env.Value
			break
		}
	}
	if leaderElectionName != "demo-manager" {
		t.Fatalf("manager leader election name = %q, want demo-manager", leaderElectionName)
	}
	if deployment.Spec.Strategy.Type != appsv1.RollingUpdateDeploymentStrategyType ||
		deployment.Spec.Strategy.RollingUpdate == nil ||
		deployment.Spec.Strategy.RollingUpdate.MaxSurge == nil ||
		deployment.Spec.Strategy.RollingUpdate.MaxUnavailable == nil {
		t.Fatalf("manager deployment strategy = %#v, want no-surge rolling update", deployment.Spec.Strategy)
	}
	if got := deployment.Spec.Strategy.RollingUpdate.MaxSurge.IntValue(); got != 0 {
		t.Fatalf("manager maxSurge = %d, want 0", got)
	}
	if got := deployment.Spec.Strategy.RollingUpdate.MaxUnavailable.IntValue(); got != 1 {
		t.Fatalf("manager maxUnavailable = %d, want 1", got)
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
	assertManagerObjectEncryptionMount(t, deployment, infra.Name)
	assertManagerNodeAuthorityDeployment(t, deployment)

	service := &corev1.Service{}
	if err := reconciler.Resources.Client.Get(ctx, types.NamespacedName{Name: "demo-manager", Namespace: infra.Namespace}, service); err != nil {
		t.Fatal(err)
	}
	for _, port := range service.Spec.Ports {
		if port.Name == "storage-http" {
			t.Fatalf("retired storage-http port is still exposed: %#v", service.Spec.Ports)
		}
	}
	for _, port := range service.Spec.Ports {
		if port.Name == "node-authority" {
			t.Fatalf("load-balanced manager Service exposes node authority: %#v", service.Spec.Ports)
		}
	}
	nodeAuthorityService := &corev1.Service{}
	if err := reconciler.Resources.Client.Get(ctx, types.NamespacedName{
		Name: "demo-manager-nodes", Namespace: infra.Namespace,
	}, nodeAuthorityService); err != nil {
		t.Fatal(err)
	}
	if nodeAuthorityService.Spec.ClusterIP != corev1.ClusterIPNone ||
		!nodeAuthorityService.Spec.PublishNotReadyAddresses ||
		len(nodeAuthorityService.Spec.Ports) != 1 ||
		nodeAuthorityService.Spec.Ports[0].Name != "node-authority" ||
		nodeAuthorityService.Spec.Ports[0].Port != 8421 ||
		nodeAuthorityService.Spec.Ports[0].TargetPort.IntVal != 8421 {
		t.Fatalf("manager headless node authority Service = %#v", nodeAuthorityService.Spec)
	}

	infra.Spec.Services.Manager.Config.SandboxRuntimeBackend = config.SandboxRuntimeBackendKubernetes
	infra.Spec.Services.Manager.Config.NodeAuthority = infrav1alpha1.NodeAuthorityConfig{}
	if err := reconciler.Reconcile(ctx, "sandbox0ai/infra", "test", infraplan.Compile(infra)); err == nil ||
		!strings.Contains(err.Error(), "not ready") {
		t.Fatalf("disabled authority Reconcile() error = %v, want rollout not ready", err)
	}
	if err := reconciler.Resources.Client.Get(ctx, types.NamespacedName{
		Name: "demo-manager-nodes", Namespace: infra.Namespace,
	}, &corev1.Service{}); !apierrors.IsNotFound(err) {
		t.Fatalf("disabled manager node authority Service still exists: %v", err)
	}
}

func assertManagerNodeAuthorityDeployment(t *testing.T, deployment *appsv1.Deployment) {
	t.Helper()
	container := deployment.Spec.Template.Spec.Containers[0]
	foundPort := false
	foundTLSMount := false
	foundControlMount := false
	foundClaimMount := false
	for _, port := range container.Ports {
		foundPort = foundPort || (port.Name == "node-authority" && port.ContainerPort == 8421)
	}
	for _, mount := range container.VolumeMounts {
		foundTLSMount = foundTLSMount || (mount.Name == nodeAuthorityTLSVolumeName &&
			mount.MountPath == "/etc/sandbox0/node-authority/tls" && mount.ReadOnly)
		foundControlMount = foundControlMount || (mount.Name == nodeAuthorityControlVolumeName &&
			mount.MountPath == "/etc/sandbox0/node-authority/control" && mount.ReadOnly)
		foundClaimMount = foundClaimMount || (mount.Name == nodeAuthorityClaimVolumeName &&
			mount.MountPath == "/etc/sandbox0/node-authority/claim" && mount.ReadOnly)
	}
	if !foundPort || !foundTLSMount || !foundControlMount || !foundClaimMount {
		t.Fatalf("manager node authority container wiring is incomplete: ports=%#v mounts=%#v", container.Ports, container.VolumeMounts)
	}
	foundTLSVolume := false
	foundControlVolume := false
	foundClaimVolume := false
	for _, volume := range deployment.Spec.Template.Spec.Volumes {
		foundTLSVolume = foundTLSVolume || (volume.Name == nodeAuthorityTLSVolumeName &&
			volume.Secret != nil && volume.Secret.SecretName == "manager-node-tls")
		foundControlVolume = foundControlVolume || (volume.Name == nodeAuthorityControlVolumeName &&
			volume.Secret != nil && volume.Secret.SecretName == "manager-nomad-control")
		foundClaimVolume = foundClaimVolume || (volume.Name == nodeAuthorityClaimVolumeName &&
			volume.Secret != nil && volume.Secret.SecretName == "manager-nomad-claim" &&
			len(volume.Secret.Items) == 2)
	}
	if !foundTLSVolume || !foundControlVolume || !foundClaimVolume {
		t.Fatalf("manager node authority volumes are incomplete: %#v", deployment.Spec.Template.Spec.Volumes)
	}
}

func TestApplyManagerRuntimeDeploymentConfigFailsClosed(t *testing.T) {
	base := func() *config.ManagerConfig {
		return &config.ManagerConfig{
			SandboxRuntimeBackend: config.SandboxRuntimeBackendNomad,
			NodeAuthority: config.NodeAuthorityConfig{
				Enabled: true, TLSSecretName: "manager-node-tls",
				Identities: []config.NodeAuthorityIdentityConfig{{CommonName: "node", NodeUID: "uid", PodUID: "agent"}},
				Claim:      config.RuntimeSlotClaimConfig{SecretName: "nomad-claim"},
				Terminal:   config.RuntimeSlotTerminalConfig{Enabled: true, ControlSecretName: "nomad-control"},
			},
		}
	}
	tests := []struct {
		name   string
		mutate func(*config.ManagerConfig)
	}{
		{name: "missing authority", mutate: func(cfg *config.ManagerConfig) { cfg.NodeAuthority.Enabled = false }},
		{name: "missing terminal", mutate: func(cfg *config.ManagerConfig) { cfg.NodeAuthority.Terminal = config.RuntimeSlotTerminalConfig{} }},
		{name: "missing claim secret", mutate: func(cfg *config.ManagerConfig) { cfg.NodeAuthority.Claim.SecretName = "" }},
		{name: "short claim ttl", mutate: func(cfg *config.ManagerConfig) {
			cfg.NodeAuthority.Claim.ClaimTTL = metav1.Duration{Duration: 500 * time.Millisecond}
		}},
		{name: "nonpositive slo", mutate: func(cfg *config.ManagerConfig) {
			cfg.NodeAuthority.Claim.SLO = metav1.Duration{Duration: -time.Second}
		}},
		{name: "unknown backend", mutate: func(cfg *config.ManagerConfig) { cfg.SandboxRuntimeBackend = "containerd" }},
		{name: "claim config on kubernetes", mutate: func(cfg *config.ManagerConfig) {
			cfg.SandboxRuntimeBackend = config.SandboxRuntimeBackendKubernetes
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cfg := base()
			if test.mutate != nil {
				test.mutate(cfg)
			}
			if err := applyManagerRuntimeDeploymentConfig(cfg); err == nil {
				t.Fatalf("invalid config was accepted: %#v", cfg)
			}
		})
	}
}

func TestApplyManagerRuntimeDeploymentConfigPinsNomadClaimAssets(t *testing.T) {
	cfg := &config.ManagerConfig{
		SandboxRuntimeBackend: config.SandboxRuntimeBackendNomad,
		NodeAuthority: config.NodeAuthorityConfig{
			Enabled: true, TLSSecretName: "manager-node-tls",
			Identities: []config.NodeAuthorityIdentityConfig{{CommonName: "node", NodeUID: "uid", PodUID: "agent"}},
			Claim: config.RuntimeSlotClaimConfig{
				SecretName: "nomad-claim", ProfileCatalogFile: "/ignored/profiles.json", WriterTokenKeyFile: "/ignored/key",
			},
			Terminal: config.RuntimeSlotTerminalConfig{Enabled: true, ControlSecretName: "nomad-control"},
		},
	}
	if err := applyManagerRuntimeDeploymentConfig(cfg); err != nil {
		t.Fatal(err)
	}
	claim := cfg.NodeAuthority.Claim
	if claim.ProfileCatalogFile != config.NodeAuthorityRuntimeProfilesPath ||
		claim.WriterTokenKeyFile != config.NodeAuthorityWriterTokenKeyPath ||
		claim.ClaimTTL.Duration != 15*time.Second || claim.SLO.Duration != time.Second {
		t.Fatalf("Nomad claim deployment config = %#v", claim)
	}
}

func TestApplyNodeAuthorityDeploymentConfigFailsClosed(t *testing.T) {
	tests := []struct {
		name string
		cfg  config.NodeAuthorityConfig
	}{
		{name: "terminal without authority", cfg: config.NodeAuthorityConfig{
			Terminal: config.RuntimeSlotTerminalConfig{Enabled: true},
		}},
		{name: "missing TLS secret", cfg: config.NodeAuthorityConfig{Enabled: true}},
		{name: "missing identity", cfg: config.NodeAuthorityConfig{
			Enabled: true, TLSSecretName: "manager-node-tls",
		}},
		{name: "noncanonical TLS secret", cfg: config.NodeAuthorityConfig{
			Enabled: true, TLSSecretName: " Manager TLS ",
		}},
		{name: "missing terminal control secret", cfg: config.NodeAuthorityConfig{
			Enabled: true, TLSSecretName: "manager-node-tls",
			Identities: []config.NodeAuthorityIdentityConfig{{CommonName: "node", NodeUID: "uid", PodUID: "agent"}},
			Terminal:   config.RuntimeSlotTerminalConfig{Enabled: true},
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cfg := &config.ManagerConfig{NodeAuthority: test.cfg}
			if err := applyNodeAuthorityDeploymentConfig(cfg); err == nil {
				t.Fatalf("invalid config was accepted: %#v", cfg.NodeAuthority)
			}
		})
	}
}

func TestApplyNodeAuthorityDeploymentConfigPinsMountedPaths(t *testing.T) {
	cfg := &config.ManagerConfig{NodeAuthority: config.NodeAuthorityConfig{
		Enabled: true, TLSSecretName: "manager-node-tls",
		Identities: []config.NodeAuthorityIdentityConfig{{CommonName: "node", NodeUID: "uid", PodUID: "agent"}},
		Terminal:   config.RuntimeSlotTerminalConfig{Enabled: true, ControlSecretName: "nomad-control"},
	}}
	if err := applyNodeAuthorityDeploymentConfig(cfg); err != nil {
		t.Fatal(err)
	}
	node := cfg.NodeAuthority
	if node.Port != config.DefaultNodeAuthorityPort || node.CertFile != config.NodeAuthorityServerCertPath ||
		node.KeyFile != config.NodeAuthorityServerKeyPath || node.ClientCAFile != config.NodeAuthorityClientCAPath ||
		node.Terminal.NomadEndpointsFile != config.NodeAuthorityNomadEndpointsPath ||
		node.Terminal.Interval.Duration != time.Second || node.Terminal.PassTimeout.Duration != 2*time.Minute ||
		node.Terminal.ScanLimit != 100 {
		t.Fatalf("node authority deployment config = %#v", node)
	}
}

func assertManagerObjectEncryptionMount(t *testing.T, deployment *appsv1.Deployment, infraName string) {
	t.Helper()
	if deployment == nil || len(deployment.Spec.Template.Spec.Containers) == 0 {
		t.Fatal("manager deployment has no container")
	}
	foundMount := false
	for _, mount := range deployment.Spec.Template.Spec.Containers[0].VolumeMounts {
		if mount.Name == "object-encryption-key" && mount.MountPath == common.ObjectEncryptionMountDir && mount.ReadOnly {
			foundMount = true
			break
		}
	}
	if !foundMount {
		t.Fatalf("manager object encryption mount is missing: %#v", deployment.Spec.Template.Spec.Containers[0].VolumeMounts)
	}
	for _, volume := range deployment.Spec.Template.Spec.Volumes {
		if volume.Name == "object-encryption-key" && volume.Secret != nil &&
			volume.Secret.SecretName == common.ObjectEncryptionSecretName(infraName) {
			return
		}
	}
	t.Fatalf("manager object encryption volume is missing: %#v", deployment.Spec.Template.Spec.Volumes)
}

func TestValidateManagerServicePortsRejectsDuplicatePorts(t *testing.T) {
	err := validateManagerServicePorts([]corev1.ServicePort{
		{Name: "metrics", Port: 9090},
		{Name: "webhook", Port: 9090},
	})
	if err == nil || !strings.Contains(err.Error(), "manager Service port 9090 is used by both metrics and webhook") {
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
			CommonName:   "test-ctld-network-mitm-ca",
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

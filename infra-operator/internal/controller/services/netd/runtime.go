package netd

import (
	"context"
	"fmt"
	"path/filepath"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/log"

	apiconfig "github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	"github.com/sandbox0-ai/sandbox0/infra-operator/internal/controller/pkg/common"
	meteringsvc "github.com/sandbox0-ai/sandbox0/infra-operator/internal/controller/services/metering"
	redissvc "github.com/sandbox0-ai/sandbox0/infra-operator/internal/controller/services/redis"
	sandboxobssvc "github.com/sandbox0-ai/sandbox0/infra-operator/internal/controller/services/sandboxobservability"
	infraplan "github.com/sandbox0-ai/sandbox0/infra-operator/internal/plan"
	"github.com/sandbox0-ai/sandbox0/netd/pkg/activeguard"
	pkginternalauth "github.com/sandbox0-ai/sandbox0/pkg/internalauth"
)

const (
	ConfigPath               = "/config/netd.yaml"
	ConfigVolumeName         = "netd-config"
	ActiveLockEnv            = activeguard.EnvPath
	ActiveLockVolumeName     = "netd-active-lock"
	ActiveLockHostDirectory  = "/var/lib/sandbox0/netd-locks"
	ActiveLockMountDirectory = "/var/lib/sandbox0/netd-locks"
	RunVolumeName            = "netd-run"
	RunMountDirectory        = "/run"
	ConfigHashAnnotation     = "infra.sandbox0.ai/netd-config-hash"
)

// RuntimeAssets are the config, host access, secrets, and placement required
// to run netd either as a legacy DaemonSet or inside the ctld process.
type RuntimeAssets struct {
	Config           *apiconfig.NetdConfig
	ConfigRef        common.ServiceConfigRef
	PodAnnotations   map[string]string
	Volumes          []corev1.Volume
	VolumeMounts     []corev1.VolumeMount
	Ports            []corev1.ContainerPort
	RuntimeClassName *string
	NodeSelector     map[string]string
	Tolerations      []corev1.Toleration
	ActiveLockPath   string
}

// ScopedActiveLockPath isolates netd fencing between Sandbox0Infra instances
// that share a Kubernetes node.
func ScopedActiveLockPath(namespace, name string) string {
	return filepath.Join(ActiveLockMountDirectory, namespace, name, "netd.lock")
}

// BuildRuntimeAssets materializes the single netd config and workload assets
// shared by legacy handoff and embedded ctld runtimes.
func (r *Reconciler) BuildRuntimeAssets(ctx context.Context, compiledPlan *infraplan.InfraPlan) (*RuntimeAssets, error) {
	if compiledPlan == nil {
		return nil, fmt.Errorf("compiled plan is required")
	}
	if r == nil || r.Resources == nil || r.Resources.Client == nil {
		return nil, fmt.Errorf("netd resource manager is required")
	}
	scope := compiledPlan.Scope
	name := fmt.Sprintf("%s-netd", scope.Name)
	labels := common.GetServiceLabels(scope.Name, "netd")
	config := &apiconfig.NetdConfig{}
	if compiledPlan.Netd.Config != nil {
		config = compiledPlan.Netd.Config.DeepCopy()
	}
	if config.NodeName == "" {
		config.NodeName = "${NODE_NAME}"
	}
	if config.MetricsPort == 0 {
		config.MetricsPort = 9091
	}
	if config.HealthPort == 0 {
		config.HealthPort = 8081
	}
	if config.ProxyHTTPPort == 0 {
		config.ProxyHTTPPort = 18080
	}
	if config.ProxyHTTPSPort == 0 {
		config.ProxyHTTPSPort = 18443
	}
	if err := config.ValidateListenerPorts(map[int]string{8095: "ctld HTTP port"}); err != nil {
		return nil, err
	}
	if config.ClusterDNSCIDR == "" {
		cidr, err := resolveClusterDNSCIDR(ctx, r.Resources.Client, log.FromContext(ctx))
		if err != nil {
			return nil, err
		}
		config.ClusterDNSCIDR = cidr
	}
	if err := redissvc.ApplyNetdRedisConfig(ctx, r.Resources.Client, scope.Owner(), config); err != nil {
		return nil, err
	}
	if dsn, err := compiledPlan.DatabaseDSN(ctx, r.Resources.Client); err == nil {
		config.DatabaseURL = dsn
	}
	config.RegionID = compiledPlan.Netd.RegionID
	config.ClusterID = compiledPlan.Netd.ClusterID
	if err := meteringsvc.ApplyNetdConfig(ctx, r.Resources.Client, scope.Owner(), config); err != nil {
		return nil, err
	}
	if config.EgressAuthResolverURL == "" {
		config.EgressAuthResolverURL = compiledPlan.Netd.EgressAuthResolverURL
	}
	if err := sandboxobssvc.ApplyNetdConfig(ctx, r.Resources.Client, scope.Owner(), compiledPlan.Services.ClusterGateway.URL, config); err != nil {
		return nil, err
	}
	mitmCASecretName, err := r.resolveMITMCASecretName(ctx, compiledPlan, labels)
	if err != nil {
		return nil, err
	}
	keySecretName, privateKeyKey, _ := compiledPlan.DataPlaneKeyRefs()
	auditKeySecretName, auditPrivateKeyKey, _ := compiledPlan.AuditNetdKeyRefs()
	if mitmCASecretName != "" {
		if config.MITMCACertPath == "" {
			config.MITMCACertPath = "/tls/ca.crt"
		}
		if config.MITMCAKeyPath == "" {
			config.MITMCAKeyPath = "/tls/ca.key"
		}
	}
	configRef, err := r.Resources.ReconcileHashedServiceConfigMapWithScope(ctx, scope, name, labels, config)
	if err != nil {
		return nil, err
	}
	directoryOrCreate := corev1.HostPathDirectoryOrCreate
	volumes := []corev1.Volume{
		{
			Name: ConfigVolumeName,
			VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{
				LocalObjectReference: corev1.LocalObjectReference{Name: configRef.ConfigMapName},
			}},
		},
		{
			Name: "bpf-fs",
			VolumeSource: corev1.VolumeSource{HostPath: &corev1.HostPathVolumeSource{
				Path: "/sys/fs/bpf", Type: &directoryOrCreate,
			}},
		},
		{
			Name: "modules",
			VolumeSource: corev1.VolumeSource{HostPath: &corev1.HostPathVolumeSource{
				Path: "/lib/modules", Type: &directoryOrCreate,
			}},
		},
		{Name: RunVolumeName, VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}},
		{
			Name: ActiveLockVolumeName,
			VolumeSource: corev1.VolumeSource{HostPath: &corev1.HostPathVolumeSource{
				Path: ActiveLockHostDirectory, Type: &directoryOrCreate,
			}},
		},
		{
			Name: "internal-jwt-private-key",
			VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{
				SecretName: keySecretName,
				Items:      []corev1.KeyToPath{{Key: privateKeyKey, Path: "internal_jwt_private.key"}},
			}},
		},
	}
	volumeMounts := []corev1.VolumeMount{
		{Name: ConfigVolumeName, MountPath: ConfigPath, SubPath: "config.yaml", ReadOnly: true},
		{Name: "bpf-fs", MountPath: "/sys/fs/bpf"},
		{Name: "modules", MountPath: "/lib/modules", ReadOnly: true},
		{Name: RunVolumeName, MountPath: RunMountDirectory},
		{Name: ActiveLockVolumeName, MountPath: ActiveLockMountDirectory},
		{Name: "internal-jwt-private-key", MountPath: pkginternalauth.DefaultInternalJWTPrivateKeyPath, SubPath: "internal_jwt_private.key", ReadOnly: true},
	}
	if config.SandboxObservabilityIngestURL != "" {
		volumes = append(volumes,
			corev1.Volume{
				Name: "audit-spool",
				VolumeSource: corev1.VolumeSource{HostPath: &corev1.HostPathVolumeSource{
					Path: filepath.Join("/var/lib/sandbox0/netd", scope.Namespace, scope.Name), Type: &directoryOrCreate,
				}},
			},
			corev1.Volume{
				Name: "audit-jwt-private-key",
				VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{
					SecretName: auditKeySecretName,
					Items:      []corev1.KeyToPath{{Key: auditPrivateKeyKey, Path: "audit_jwt_private.key"}},
				}},
			},
		)
		volumeMounts = append(volumeMounts,
			corev1.VolumeMount{Name: "audit-spool", MountPath: "/var/lib/sandbox0/netd"},
			corev1.VolumeMount{Name: "audit-jwt-private-key", MountPath: pkginternalauth.DefaultAuditJWTPrivateKeyPath, SubPath: "audit_jwt_private.key", ReadOnly: true},
		)
	}
	if mitmCASecretName != "" {
		volumes = append(volumes, corev1.Volume{
			Name:         "mitm-ca",
			VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: mitmCASecretName}},
		})
		volumeMounts = append(volumeMounts, corev1.VolumeMount{Name: "mitm-ca", MountPath: "/tls", ReadOnly: true})
	}
	return &RuntimeAssets{
		Config:         config,
		ConfigRef:      configRef,
		PodAnnotations: configRef.PodAnnotations(),
		Volumes:        volumes,
		VolumeMounts:   volumeMounts,
		Ports: []corev1.ContainerPort{
			{Name: "metrics", ContainerPort: int32(config.MetricsPort)},
			{Name: "health", ContainerPort: int32(config.HealthPort)},
		},
		RuntimeClassName: compiledPlan.Netd.RuntimeClassName,
		NodeSelector:     compiledPlan.Netd.NodeSelector,
		Tolerations:      compiledPlan.Netd.Tolerations,
		ActiveLockPath:   ScopedActiveLockPath(scope.Namespace, scope.Name),
	}, nil
}

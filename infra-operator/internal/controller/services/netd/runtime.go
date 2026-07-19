package netd

import (
	"context"
	"fmt"
	"net"
	"path/filepath"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	apiconfig "github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	"github.com/sandbox0-ai/sandbox0/infra-operator/internal/controller/pkg/common"
	meteringsvc "github.com/sandbox0-ai/sandbox0/infra-operator/internal/controller/services/metering"
	redissvc "github.com/sandbox0-ai/sandbox0/infra-operator/internal/controller/services/redis"
	sandboxobssvc "github.com/sandbox0-ai/sandbox0/infra-operator/internal/controller/services/sandboxobservability"
	infraplan "github.com/sandbox0-ai/sandbox0/infra-operator/internal/plan"
	pkginternalauth "github.com/sandbox0-ai/sandbox0/pkg/internalauth"
)

const (
	ConfigPath           = "/config/netd.yaml"
	ConfigVolumeName     = "netd-config"
	RunVolumeName        = "netd-run"
	RunMountDirectory    = "/run"
	ConfigHashAnnotation = "infra.sandbox0.ai/netd-config-hash"
)

// Reconciler builds the network runtime assets consumed by ctld.
type Reconciler struct {
	Resources *common.ResourceManager
}

func NewReconciler(resources *common.ResourceManager) *Reconciler {
	return &Reconciler{Resources: resources}
}

func (r *Reconciler) resolveMITMCASecretName(ctx context.Context, compiledPlan *infraplan.InfraPlan, labels map[string]string) (string, error) {
	return EnsureMITMCASecretWithScope(ctx, r.Resources, compiledPlan.Scope, compiledPlan, labels)
}

func resolveClusterDNSCIDR(ctx context.Context, client ctrlclient.Client, _ logr.Logger) (string, error) {
	if client == nil {
		return "", fmt.Errorf("client is nil")
	}
	for _, name := range []string{"kube-dns", "coredns"} {
		svc := &corev1.Service{}
		if err := client.Get(ctx, types.NamespacedName{Name: name, Namespace: "kube-system"}, svc); err != nil {
			continue
		}
		if svc.Spec.ClusterIP == "" || svc.Spec.ClusterIP == "None" {
			continue
		}
		ip := net.ParseIP(svc.Spec.ClusterIP)
		if ip == nil {
			continue
		}
		if ip.To4() != nil {
			return ip.String() + "/32", nil
		}
		return ip.String() + "/128", nil
	}
	return "", fmt.Errorf("failed to resolve cluster DNS CIDR; set network.config.clusterDnsCidr explicitly")
}

// RuntimeAssets are the config, host access, and secrets required by ctld's
// network runtime.
type RuntimeAssets struct {
	Config         *apiconfig.NetdConfig
	ConfigRef      common.ServiceConfigRef
	PodAnnotations map[string]string
	Volumes        []corev1.Volume
	VolumeMounts   []corev1.VolumeMount
	Ports          []corev1.ContainerPort
}

// BuildRuntimeAssets materializes ctld's network config and workload assets.
func (r *Reconciler) BuildRuntimeAssets(ctx context.Context, compiledPlan *infraplan.InfraPlan) (*RuntimeAssets, error) {
	if compiledPlan == nil {
		return nil, fmt.Errorf("compiled plan is required")
	}
	if !compiledPlan.Network.Enabled {
		return nil, nil
	}
	if r == nil || r.Resources == nil || r.Resources.Client == nil {
		return nil, fmt.Errorf("ctld network runtime resource manager is required")
	}
	scope := compiledPlan.Scope
	name := fmt.Sprintf("%s-netd", scope.Name)
	labels := common.GetServiceLabels(scope.Name, "netd")
	config := &apiconfig.NetdConfig{}
	if compiledPlan.Network.Config != nil {
		config = compiledPlan.Network.Config.DeepCopy()
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
	if _, _, _, err := config.ProxyAdmissionLimits(); err != nil {
		return nil, err
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
	if err := redissvc.ApplyTeamQuotaDistributedEnforcementConfig(ctx, r.Resources.Client, scope.Owner(), &config.TeamQuotaDistributedEnforcement); err != nil {
		return nil, err
	}
	if dsn, err := compiledPlan.DatabaseDSN(ctx, r.Resources.Client); err == nil {
		config.DatabaseURL = dsn
	}
	config.RegionID = compiledPlan.Network.RegionID
	config.ClusterID = compiledPlan.Network.ClusterID
	if err := meteringsvc.ApplyNetdConfig(ctx, r.Resources.Client, scope.Owner(), config); err != nil {
		return nil, err
	}
	if config.EgressAuthResolverURL == "" {
		config.EgressAuthResolverURL = compiledPlan.Network.EgressAuthResolverURL
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
	}, nil
}

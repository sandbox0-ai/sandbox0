package ctld

import (
	"context"
	"fmt"
	"strings"

	apiconfig "github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/log"

	infrav1alpha1 "github.com/sandbox0-ai/sandbox0/infra-operator/api/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/infra-operator/internal/controller/pkg/common"
	"github.com/sandbox0-ai/sandbox0/infra-operator/internal/controller/services/database"
	"github.com/sandbox0-ai/sandbox0/infra-operator/internal/controller/services/storage"
	"github.com/sandbox0-ai/sandbox0/infra-operator/internal/runtimeconfig"
	"github.com/sandbox0-ai/sandbox0/pkg/volumeportal"
)

type Reconciler struct {
	Resources *common.ResourceManager
}

const (
	containerdDataMountPath        = "/host-var-lib/containerd"
	defaultContainerdHostDataRoot  = "/var/lib/containerd"
	defaultContainerdHostStateRoot = "/run/containerd"
	ctldProbeTimeoutSeconds        = 10
	ctldProbeFailureThreshold      = 6
)

func NewReconciler(resources *common.ResourceManager) *Reconciler {
	return &Reconciler{Resources: resources}
}

func (r *Reconciler) Reconcile(ctx context.Context, infra *infrav1alpha1.Sandbox0Infra, imageRepo, imageTag string) error {
	logger := log.FromContext(ctx)
	if !infrav1alpha1.HasDataPlaneServices(infra) {
		logger.Info("Data-plane services are disabled, skipping ctld")
		return nil
	}

	const ctldHTTPPort = 8095

	name := fmt.Sprintf("%s-ctld", infra.Name)
	labels := common.GetServiceLabels(infra.Name, "ctld")
	storageConfig, err := r.buildStorageConfig(ctx, infra)
	if err != nil {
		return err
	}
	if storageConfig.ObjectEncryptionEnabled {
		if err := common.EnsureObjectEncryptionKeySecret(ctx, r.Resources, infra); err != nil {
			return err
		}
		storageConfig.ObjectEncryptionKeyPath = common.ObjectEncryptionKeyPath
	}
	configRef, err := r.Resources.ReconcileHashedServiceConfigMap(ctx, infra, name, labels, storageConfig)
	if err != nil {
		return err
	}
	podAnnotations := configRef.PodAnnotations()
	if err := r.ensureCSIDriver(ctx, labels); err != nil {
		return err
	}

	image := fmt.Sprintf("%s:%s", imageRepo, imageTag)
	pullPolicy := corev1.PullIfNotPresent
	if r.Resources.ImagePullPolicy != nil {
		pullPolicy = *r.Resources.ImagePullPolicy
	}

	nodeSelector, tolerations := common.ResolveSandboxNodePlacement(infra)
	containerdHostDataRoot := ctldContainerdHostDataRoot(infra)
	args := ctldArgs(containerdHostDataRoot)
	bidirectional := corev1.MountPropagationBidirectional
	hostPathDirectoryOrCreate := corev1.HostPathDirectoryOrCreate
	volumeMounts := []corev1.VolumeMount{
		{Name: "config", MountPath: "/config/config.yaml", SubPath: "config.yaml", ReadOnly: true},
		{Name: "csi-plugin", MountPath: "/csi"},
		{Name: "kubelet", MountPath: "/var/lib/kubelet", MountPropagation: &bidirectional},
		{Name: "ctld-data", MountPath: "/var/lib/sandbox0/ctld"},
		{Name: "containerd-sock", MountPath: "/host-run/containerd"},
		{Name: "containerd-data", MountPath: containerdDataMountPath, ReadOnly: true},
	}
	volumes := []corev1.Volume{
		{
			Name: "config",
			VolumeSource: corev1.VolumeSource{
				ConfigMap: &corev1.ConfigMapVolumeSource{
					LocalObjectReference: corev1.LocalObjectReference{Name: configRef.ConfigMapName},
				},
			},
		},
		{
			Name: "csi-plugin",
			VolumeSource: corev1.VolumeSource{
				HostPath: &corev1.HostPathVolumeSource{
					Path: "/var/lib/kubelet/plugins/volume.sandbox0.ai",
					Type: &hostPathDirectoryOrCreate,
				},
			},
		},
		{
			Name: "plugin-registration",
			VolumeSource: corev1.VolumeSource{
				HostPath: &corev1.HostPathVolumeSource{
					Path: "/var/lib/kubelet/plugins_registry",
					Type: &hostPathDirectoryOrCreate,
				},
			},
		},
		{
			Name: "kubelet",
			VolumeSource: corev1.VolumeSource{
				HostPath: &corev1.HostPathVolumeSource{Path: "/var/lib/kubelet"},
			},
		},
		{
			Name: "ctld-data",
			VolumeSource: corev1.VolumeSource{
				HostPath: &corev1.HostPathVolumeSource{
					Path: "/var/lib/sandbox0/ctld",
					Type: &hostPathDirectoryOrCreate,
				},
			},
		},
		{
			Name: "containerd-sock",
			VolumeSource: corev1.VolumeSource{
				HostPath: &corev1.HostPathVolumeSource{Path: defaultContainerdHostStateRoot},
			},
		},
		{
			Name: "containerd-data",
			VolumeSource: corev1.VolumeSource{
				HostPath: &corev1.HostPathVolumeSource{Path: containerdHostDataRoot},
			},
		},
	}
	if storageConfig.ObjectEncryptionEnabled {
		volumeMounts = append(volumeMounts, corev1.VolumeMount{
			Name:      "object-encryption-key",
			MountPath: common.ObjectEncryptionMountDir,
			ReadOnly:  true,
		})
		volumes = append(volumes, corev1.Volume{
			Name: "object-encryption-key",
			VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{
					SecretName: common.ObjectEncryptionSecretName(infra.Name),
					Items: []corev1.KeyToPath{{
						Key:  common.ObjectEncryptionSecretKey,
						Path: common.ObjectEncryptionKeyFilename,
					}},
				},
			},
		})
	}

	desired := &appsv1.DaemonSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: infra.Namespace,
		},
		Spec: appsv1.DaemonSetSpec{
			Selector: &metav1.LabelSelector{
				MatchLabels: labels,
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels:      labels,
					Annotations: common.EnsurePodTemplateAnnotations(podAnnotations),
				},
				Spec: corev1.PodSpec{
					HostPID:            true,
					ServiceAccountName: name,
					NodeSelector:       nodeSelector,
					Tolerations:        tolerations,
					HostNetwork:        true,
					DNSPolicy:          corev1.DNSClusterFirstWithHostNet,
					Containers: []corev1.Container{
						{
							Name:            "ctld",
							Image:           image,
							ImagePullPolicy: pullPolicy,
							Args:            args,
							Env: common.AppendObservabilityEnvVars([]corev1.EnvVar{
								{
									Name:  "SERVICE",
									Value: "ctld",
								},
								{
									Name:  "CONFIG_PATH",
									Value: "/config/config.yaml",
								},
								{
									Name: "NODE_NAME",
									ValueFrom: &corev1.EnvVarSource{
										FieldRef: &corev1.ObjectFieldSelector{FieldPath: "spec.nodeName"},
									},
								},
								{
									Name: "POD_NAME",
									ValueFrom: &corev1.EnvVarSource{
										FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.name"},
									},
								},
								{
									Name: "POD_NAMESPACE",
									ValueFrom: &corev1.EnvVarSource{
										FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.namespace"},
									},
								},
							}, infra, common.ObservabilityEnvConfig{
								ServiceName: "ctld",
								RegionID:    common.ResolveRegionID(infra),
								ClusterID:   common.ResolveClusterID(infra),
							}),
							Ports: []corev1.ContainerPort{{
								Name:          "http",
								ContainerPort: ctldHTTPPort,
							}},
							LivenessProbe: &corev1.Probe{
								ProbeHandler: corev1.ProbeHandler{
									HTTPGet: &corev1.HTTPGetAction{Path: "/healthz", Port: intstr.FromString("http")},
								},
								InitialDelaySeconds: 5,
								PeriodSeconds:       10,
								TimeoutSeconds:      ctldProbeTimeoutSeconds,
								FailureThreshold:    ctldProbeFailureThreshold,
							},
							ReadinessProbe: &corev1.Probe{
								ProbeHandler: corev1.ProbeHandler{
									HTTPGet: &corev1.HTTPGetAction{Path: "/readyz", Port: intstr.FromString("http")},
								},
								InitialDelaySeconds: 2,
								PeriodSeconds:       5,
								TimeoutSeconds:      ctldProbeTimeoutSeconds,
								FailureThreshold:    ctldProbeFailureThreshold,
							},
							SecurityContext: &corev1.SecurityContext{
								Privileged: common.BoolPtr(true),
							},
							VolumeMounts: volumeMounts,
						},
						{
							Name:            "csi-node-driver-registrar",
							Image:           "registry.k8s.io/sig-storage/csi-node-driver-registrar:v2.13.0",
							ImagePullPolicy: corev1.PullIfNotPresent,
							Args: []string{
								"--csi-address=/csi/csi.sock",
								"--kubelet-registration-path=/var/lib/kubelet/plugins/volume.sandbox0.ai/csi.sock",
							},
							VolumeMounts: []corev1.VolumeMount{
								{
									Name:      "csi-plugin",
									MountPath: "/csi",
								},
								{
									Name:      "plugin-registration",
									MountPath: "/registration",
								},
							},
						},
					},
					Volumes: volumes,
				},
			},
		},
	}

	return r.Resources.ApplyDaemonSet(ctx, infra, desired)
}

func ctldArgs(containerdHostDataRoot string) []string {
	if strings.TrimSpace(containerdHostDataRoot) == "" {
		containerdHostDataRoot = defaultContainerdHostDataRoot
	}
	args := []string{
		"-http-addr=:8095",
		"-cri-endpoint=/host-run/containerd/containerd.sock",
		"-containerd-endpoint=/host-run/containerd/containerd.sock",
		"-containerd-root=/host-run/containerd",
		"-containerd-host-root=" + defaultContainerdHostStateRoot,
		"-containerd-data-root=" + containerdDataMountPath,
		"-containerd-host-data-root=" + containerdHostDataRoot,
		"-volume-portal-root=/var/lib/sandbox0/ctld",
		"-csi-socket=/csi/csi.sock",
	}
	return args
}

func ctldContainerdHostDataRoot(infra *infrav1alpha1.Sandbox0Infra) string {
	if infra == nil || infra.Spec.Services == nil || infra.Spec.Services.Ctld == nil {
		return defaultContainerdHostDataRoot
	}
	if root := strings.TrimSpace(infra.Spec.Services.Ctld.ContainerdHostDataRoot); root != "" {
		return root
	}
	return defaultContainerdHostDataRoot
}

func (r *Reconciler) ensureCSIDriver(ctx context.Context, labels map[string]string) error {
	attachRequired := false
	podInfoOnMount := true
	lifecycleModes := []storagev1.VolumeLifecycleMode{storagev1.VolumeLifecycleEphemeral}
	desired := &storagev1.CSIDriver{
		ObjectMeta: metav1.ObjectMeta{
			Name:   volumeportal.DriverName,
			Labels: labels,
		},
		Spec: storagev1.CSIDriverSpec{
			AttachRequired:       &attachRequired,
			PodInfoOnMount:       &podInfoOnMount,
			VolumeLifecycleModes: lifecycleModes,
		},
	}
	current := &storagev1.CSIDriver{}
	err := r.Resources.Client.Get(ctx, types.NamespacedName{Name: volumeportal.DriverName}, current)
	if apierrors.IsNotFound(err) {
		return r.Resources.Client.Create(ctx, desired)
	}
	if err != nil {
		return err
	}
	return r.Resources.UpdateObjectIfChanged(ctx, current, func() {
		current.Labels = desired.Labels
		current.Spec = desired.Spec
	})
}

func (r *Reconciler) buildStorageConfig(ctx context.Context, infra *infrav1alpha1.Sandbox0Infra) (*apiconfig.StorageProxyConfig, error) {
	cfg := &apiconfig.StorageProxyConfig{}
	if infra != nil && infra.Spec.Services != nil && infra.Spec.Services.StorageProxy != nil {
		cfg = runtimeconfig.ToStorageProxy(infra.Spec.Services.StorageProxy.Config)
	}
	cfg.RegionID = common.ResolveRegionID(infra)
	cfg.DefaultClusterId = common.ResolveClusterID(infra)
	if infra != nil && infra.Spec.Database != nil && r != nil && r.Resources != nil && r.Resources.Client != nil {
		if dsn, err := database.GetDatabaseDSN(ctx, r.Resources.Client, infra); err == nil {
			cfg.DatabaseURL = dsn
		}
	}
	if infra == nil || infra.Spec.Storage == nil {
		return cfg, nil
	}
	storageConfig, err := storage.GetStorageConfig(ctx, r.Resources.Client, infra)
	if err != nil {
		return nil, err
	}
	cfg.ObjectStorageType = normalizeObjectStorageType(storageConfig.Type)
	cfg.S3Bucket = storageConfig.Bucket
	cfg.S3Region = storageConfig.Region
	cfg.S3Endpoint = storageConfig.Endpoint
	cfg.S3AccessKey = storageConfig.AccessKey
	cfg.S3SecretKey = storageConfig.SecretKey
	cfg.S3SessionToken = storageConfig.SessionToken
	return cfg, nil
}

func normalizeObjectStorageType(storageType infrav1alpha1.StorageType) string {
	if storageType == infrav1alpha1.StorageTypeBuiltin {
		return "s3"
	}
	return string(storageType)
}

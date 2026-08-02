package ctld

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	nodev1 "k8s.io/api/node/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"

	apiconfig "github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	infrav1alpha1 "github.com/sandbox0-ai/sandbox0/infra-operator/api/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/infra-operator/internal/controller/pkg/common"
	credentialstoresvc "github.com/sandbox0-ai/sandbox0/infra-operator/internal/controller/services/credentialstore"
)

const (
	rootFSSnapshotterSocketDir     = "/run/sandbox0-rootfs-snapshotter"
	rootFSSnapshotterSocketPath    = rootFSSnapshotterSocketDir + "/snapshotter.sock"
	rootFSSnapshotterContainerdDir = "/host-run/containerd"
	rootFSSnapshotterContainerdAPI = rootFSSnapshotterContainerdDir + "/containerd.sock"
	rootFSSnapshotterHealthPort    = int32(8096)
	rootFSSnapshotterRootDirName   = "sandbox0-rootfs-snapshotter"
	rootFSSnapshotterCPURequest    = "250m"
	rootFSSnapshotterMemoryRequest = "256Mi"
	rootFSSnapshotterRevision      = "infra.sandbox0.ai/rootfs-snapshotter-revision"
	rootFSSnapshotterServiceName   = "rootfs-snapshotter"
)

func (r *Reconciler) reconcileRootFSSnapshotter(
	ctx context.Context,
	infra *infrav1alpha1.Sandbox0Infra,
	image string,
	pullPolicy corev1.PullPolicy,
	nodeSelector map[string]string,
	tolerations []corev1.Toleration,
	configRef common.ServiceConfigRef,
	storageConfig *apiconfig.StorageProxyConfig,
	containerdHostDataRoot string,
) error {
	if infra == nil || storageConfig == nil {
		return fmt.Errorf("rootfs snapshotter requires infra and storage config")
	}
	runtimeConfig := infrav1alpha1.ResolveRootFSSnapshotterConfig(infra)
	if err := r.ensureRootFSRuntimeClass(ctx, infra, runtimeConfig); err != nil {
		return err
	}
	desired := buildRootFSSnapshotterDaemonSet(rootFSSnapshotterDaemonSetConfig{
		Name:                   rootFSSnapshotterDaemonSetName(infra),
		Namespace:              infra.Namespace,
		Labels:                 common.GetServiceLabels(infra.Name, rootFSSnapshotterServiceName),
		PodAnnotations:         configRef.PodAnnotations(),
		ConfigMapName:          configRef.ConfigMapName,
		Image:                  image,
		PullPolicy:             pullPolicy,
		NodeSelector:           nodeSelector,
		Tolerations:            tolerations,
		ContainerdHostDataRoot: containerdHostDataRoot,
		StorageConfig:          storageConfig,
		Infra:                  infra,
	})
	revision, err := common.ConfigHash(desired.Spec)
	if err != nil {
		return fmt.Errorf("hash rootfs snapshotter rollout: %w", err)
	}
	desired.Spec.Template.Annotations[rootFSSnapshotterRevision] = revision
	return r.Resources.ApplyDaemonSet(ctx, infra, desired)
}

func (r *Reconciler) ensureRootFSRuntimeClass(ctx context.Context, infra *infrav1alpha1.Sandbox0Infra, cfg infrav1alpha1.RootFSSnapshotterConfig) error {
	name := strings.TrimSpace(cfg.RuntimeClassName)
	handler := strings.TrimSpace(cfg.Handler)
	if name == "" || handler == "" {
		return fmt.Errorf("rootfs snapshotter runtime class name and handler are required")
	}
	labels := common.GetServiceLabels(infra.Name, rootFSSnapshotterServiceName)
	current := &nodev1.RuntimeClass{}
	err := r.Resources.Client.Get(ctx, types.NamespacedName{Name: name}, current)
	if apierrors.IsNotFound(err) {
		return r.Resources.Client.Create(ctx, &nodev1.RuntimeClass{
			ObjectMeta: metav1.ObjectMeta{Name: name, Labels: labels},
			Handler:    handler,
		})
	}
	if err != nil {
		return err
	}
	return r.Resources.UpdateObjectIfChanged(ctx, current, func() {
		current.Handler = handler
		if current.Labels == nil {
			current.Labels = make(map[string]string, len(labels))
		}
		for key, value := range labels {
			current.Labels[key] = value
		}
	})
}

type rootFSSnapshotterDaemonSetConfig struct {
	Name                   string
	Namespace              string
	Labels                 map[string]string
	PodAnnotations         map[string]string
	ConfigMapName          string
	Image                  string
	PullPolicy             corev1.PullPolicy
	NodeSelector           map[string]string
	Tolerations            []corev1.Toleration
	ContainerdHostDataRoot string
	StorageConfig          *apiconfig.StorageProxyConfig
	Infra                  *infrav1alpha1.Sandbox0Infra
}

func buildRootFSSnapshotterDaemonSet(cfg rootFSSnapshotterDaemonSetConfig) *appsv1.DaemonSet {
	hostDataRoot := filepath.Clean(strings.TrimSpace(cfg.ContainerdHostDataRoot))
	if hostDataRoot == "" || hostDataRoot == "." {
		hostDataRoot = defaultContainerdHostDataRoot
	}
	hostPathDirectoryOrCreate := corev1.HostPathDirectoryOrCreate
	hostPathCharDevice := corev1.HostPathCharDev
	bidirectional := corev1.MountPropagationBidirectional
	volumeMounts := []corev1.VolumeMount{
		{Name: "config", MountPath: "/config/config.yaml", SubPath: "config.yaml", ReadOnly: true},
		{Name: "containerd-data", MountPath: hostDataRoot, MountPropagation: &bidirectional},
		{Name: "containerd-run", MountPath: rootFSSnapshotterContainerdDir, ReadOnly: true},
		{Name: "snapshotter-run", MountPath: rootFSSnapshotterSocketDir},
		{Name: "fuse", MountPath: "/dev/fuse"},
	}
	volumes := []corev1.Volume{
		{Name: "config", VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{
			LocalObjectReference: corev1.LocalObjectReference{Name: cfg.ConfigMapName},
		}}},
		{Name: "containerd-data", VolumeSource: corev1.VolumeSource{HostPath: &corev1.HostPathVolumeSource{Path: hostDataRoot, Type: &hostPathDirectoryOrCreate}}},
		{Name: "containerd-run", VolumeSource: corev1.VolumeSource{HostPath: &corev1.HostPathVolumeSource{Path: defaultContainerdHostStateRoot, Type: &hostPathDirectoryOrCreate}}},
		{Name: "snapshotter-run", VolumeSource: corev1.VolumeSource{HostPath: &corev1.HostPathVolumeSource{Path: rootFSSnapshotterSocketDir, Type: &hostPathDirectoryOrCreate}}},
		{Name: "fuse", VolumeSource: corev1.VolumeSource{HostPath: &corev1.HostPathVolumeSource{Path: "/dev/fuse", Type: &hostPathCharDevice}}},
	}
	credentialMounts, credentialVolumes := credentialstoresvc.CredentialStoreVolumes(common.NewObjectScope(cfg.Infra), &cfg.StorageConfig.CredentialStore)
	volumeMounts = append(volumeMounts, credentialMounts...)
	volumes = append(volumes, credentialVolumes...)
	if cfg.StorageConfig.ObjectEncryptionEnabled {
		volumeMounts = append(volumeMounts, corev1.VolumeMount{Name: "object-encryption-key", MountPath: common.ObjectEncryptionMountDir, ReadOnly: true})
		volumes = append(volumes, corev1.Volume{Name: "object-encryption-key", VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{
			SecretName: common.ObjectEncryptionSecretName(cfg.Infra.Name),
			Items:      []corev1.KeyToPath{{Key: common.ObjectEncryptionSecretKey, Path: common.ObjectEncryptionKeyFilename}},
		}}})
	}
	container := corev1.Container{
		Name:            rootFSSnapshotterServiceName,
		Image:           cfg.Image,
		ImagePullPolicy: cfg.PullPolicy,
		Args: []string{
			"-root=" + filepath.Join(hostDataRoot, rootFSSnapshotterRootDirName),
			"-address=" + rootFSSnapshotterSocketPath,
			"-namespace=k8s.io",
			"-containerd-address=" + rootFSSnapshotterContainerdAPI,
			fmt.Sprintf("-health-address=:%d", rootFSSnapshotterHealthPort),
		},
		Env: []corev1.EnvVar{
			{Name: "SERVICE", Value: rootFSSnapshotterServiceName},
			{Name: "CONFIG_PATH", Value: "/config/config.yaml"},
			{Name: "NODE_NAME", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "spec.nodeName"}}},
			{Name: "POD_NAME", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.name"}}},
		},
		Ports: []corev1.ContainerPort{{Name: "health", ContainerPort: rootFSSnapshotterHealthPort}},
		LivenessProbe: &corev1.Probe{ProbeHandler: corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{
			Path: "/healthz", Port: intstr.FromInt32(rootFSSnapshotterHealthPort),
		}}, PeriodSeconds: 10, TimeoutSeconds: 3, FailureThreshold: 6},
		ReadinessProbe: &corev1.Probe{ProbeHandler: corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{
			Path: "/readyz", Port: intstr.FromInt32(rootFSSnapshotterHealthPort),
		}}, PeriodSeconds: 5, TimeoutSeconds: 3, FailureThreshold: 12},
		SecurityContext: &corev1.SecurityContext{Privileged: common.BoolPtr(true)},
		Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse(rootFSSnapshotterCPURequest),
			corev1.ResourceMemory: resource.MustParse(rootFSSnapshotterMemoryRequest),
		}},
		VolumeMounts: volumeMounts,
	}
	terminationGrace := int64(60)
	return &appsv1.DaemonSet{
		ObjectMeta: metav1.ObjectMeta{Name: cfg.Name, Namespace: cfg.Namespace},
		Spec: appsv1.DaemonSetSpec{
			Selector: &metav1.LabelSelector{MatchLabels: cfg.Labels},
			// A snapshotter process owns live FUSE connections. OnDelete prevents an
			// operator image change from silently breaking long-running sandboxes;
			// nodes must be drained before replacing this Pod.
			UpdateStrategy: appsv1.DaemonSetUpdateStrategy{Type: appsv1.OnDeleteDaemonSetStrategyType},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: cfg.Labels, Annotations: common.EnsurePodTemplateAnnotations(cfg.PodAnnotations)},
				Spec: corev1.PodSpec{
					ServiceAccountName:            fmt.Sprintf("%s-ctld", cfg.Infra.Name),
					NodeSelector:                  cfg.NodeSelector,
					Tolerations:                   cfg.Tolerations,
					TerminationGracePeriodSeconds: &terminationGrace,
					Containers:                    []corev1.Container{container},
					Volumes:                       volumes,
				},
			},
		},
	}
}

func rootFSSnapshotterDaemonSetName(infra *infrav1alpha1.Sandbox0Infra) string {
	if infra == nil {
		return "rootfs-snapshotter"
	}
	return fmt.Sprintf("%s-rootfs-snapshotter", infra.Name)
}

package netd

import (
	"context"
	"fmt"
	"net"

	"github.com/go-logr/logr"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	ctrl "sigs.k8s.io/controller-runtime"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	apiconfig "github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	infrav1alpha1 "github.com/sandbox0-ai/sandbox0/infra-operator/api/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/infra-operator/internal/controller/pkg/common"
)

type Reconciler struct {
	Resources *common.ResourceManager
}

func NewReconciler(resources *common.ResourceManager) *Reconciler {
	return &Reconciler{Resources: resources}
}

func (r *Reconciler) Reconcile(ctx context.Context, infra *infrav1alpha1.Sandbox0Infra, imageRepo, imageTag string) error {
	logger := log.FromContext(ctx)
	if infra.Spec.Services != nil && infra.Spec.Services.Netd != nil && !infra.Spec.Services.Netd.Enabled {
		logger.Info("netd is disabled, skipping")
		return nil
	}
	if !infrav1alpha1.HasDataPlaneServices(infra) {
		logger.Info("Data-plane services are disabled, skipping netd")
		return nil
	}

	name := fmt.Sprintf("%s-netd", infra.Name)
	labels := common.GetServiceLabels(infra.Name, "netd")
	image := fmt.Sprintf("%s:%s", imageRepo, imageTag)
	pullPolicy := corev1.PullIfNotPresent
	if r.Resources.ImagePullPolicy != nil {
		pullPolicy = *r.Resources.ImagePullPolicy
	}

	config := &apiconfig.NetdConfig{}
	runtimeClassName := (*string)(nil)
	nodeSelector := map[string]string(nil)
	tolerations := []corev1.Toleration(nil)
	if infra.Spec.Services != nil && infra.Spec.Services.Netd != nil && infra.Spec.Services.Netd.Config != nil {
		config = infra.Spec.Services.Netd.Config
	}
	if infra.Spec.Services != nil && infra.Spec.Services.Netd != nil {
		runtimeClassName = infra.Spec.Services.Netd.RuntimeClassName
	}
	nodeSelector, tolerations = common.ResolveSandboxNodePlacement(infra)
	if config.NodeName == "" {
		config.NodeName = "${NODE_NAME}"
	}
	if config.MetricsPort == 0 {
		config.MetricsPort = 9091
	}
	if config.HealthPort == 0 {
		config.HealthPort = 8081
	}
	if config.ClusterDNSCIDR == "" {
		cidr, err := resolveClusterDNSCIDR(ctx, r.Resources.Client, logger)
		if err != nil {
			return err
		}
		config.ClusterDNSCIDR = cidr
	}

	if err := r.Resources.ReconcileServiceConfigMap(ctx, infra, name, labels, config); err != nil {
		return err
	}

	ds := &appsv1.DaemonSet{}
	err := r.Resources.Client.Get(ctx, types.NamespacedName{Name: name, Namespace: infra.Namespace}, ds)
	if err != nil && !apierrors.IsNotFound(err) {
		return err
	}

	volumes := []corev1.Volume{
		{
			Name: "config",
			VolumeSource: corev1.VolumeSource{
				ConfigMap: &corev1.ConfigMapVolumeSource{
					LocalObjectReference: corev1.LocalObjectReference{Name: name},
				},
			},
		},
		{
			Name: "bpf-fs",
			VolumeSource: corev1.VolumeSource{
				HostPath: &corev1.HostPathVolumeSource{
					Path: "/sys/fs/bpf",
					Type: func() *corev1.HostPathType { t := corev1.HostPathDirectoryOrCreate; return &t }(),
				},
			},
		},
		{
			Name: "modules",
			VolumeSource: corev1.VolumeSource{
				HostPath: &corev1.HostPathVolumeSource{
					Path: "/lib/modules",
					Type: func() *corev1.HostPathType { t := corev1.HostPathDirectoryOrCreate; return &t }(),
				},
			},
		},
		{
			Name: "run",
			VolumeSource: corev1.VolumeSource{
				EmptyDir: &corev1.EmptyDirVolumeSource{},
			},
		},
	}

	volumeMounts := []corev1.VolumeMount{
		{
			Name:      "config",
			MountPath: "/config/config.yaml",
			SubPath:   "config.yaml",
			ReadOnly:  true,
		},
		{
			Name:      "bpf-fs",
			MountPath: "/sys/fs/bpf",
		},
		{
			Name:      "modules",
			MountPath: "/lib/modules",
			ReadOnly:  true,
		},
		{
			Name:      "run",
			MountPath: "/run",
		},
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
					Annotations: common.EnsurePodTemplateAnnotations(infra, nil),
				},
				Spec: corev1.PodSpec{
					ServiceAccountName: name,
					RuntimeClassName:   runtimeClassName,
					HostNetwork:        true,
					DNSPolicy:          corev1.DNSClusterFirstWithHostNet,
					NodeSelector:       nodeSelector,
					Tolerations:        tolerations,
					Containers: []corev1.Container{
						{
							Name:            "netd",
							Image:           image,
							ImagePullPolicy: pullPolicy,
							Env: []corev1.EnvVar{
								{
									Name:  "SERVICE",
									Value: "netd",
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
							},
							Ports: []corev1.ContainerPort{
								{Name: "metrics", ContainerPort: int32(config.MetricsPort)},
								{Name: "health", ContainerPort: int32(config.HealthPort)},
							},
							SecurityContext: &corev1.SecurityContext{
								AllowPrivilegeEscalation: common.BoolPtr(false),
								Privileged:               common.BoolPtr(false),
								Capabilities: &corev1.Capabilities{
									Add: []corev1.Capability{"NET_ADMIN", "NET_RAW"},
								},
							},
							LivenessProbe: &corev1.Probe{
								ProbeHandler: corev1.ProbeHandler{
									HTTPGet: &corev1.HTTPGetAction{
										Path: "/healthz",
										Port: intstr.FromString("health"),
									},
								},
								InitialDelaySeconds: 10,
								PeriodSeconds:       10,
							},
							ReadinessProbe: &corev1.Probe{
								ProbeHandler: corev1.ProbeHandler{
									HTTPGet: &corev1.HTTPGetAction{
										Path: "/readyz",
										Port: intstr.FromString("health"),
									},
								},
								InitialDelaySeconds: 5,
								PeriodSeconds:       5,
							},
							VolumeMounts: volumeMounts,
						},
					},
					Volumes: volumes,
				},
			},
		},
	}

	if err := ctrl.SetControllerReference(infra, desired, r.Resources.Scheme); err != nil {
		return err
	}

	if apierrors.IsNotFound(err) {
		return r.Resources.Client.Create(ctx, desired)
	}

	ds.Spec = desired.Spec
	return r.Resources.Client.Update(ctx, ds)
}

func resolveClusterDNSCIDR(ctx context.Context, client ctrlclient.Client, logger logr.Logger) (string, error) {
	if client == nil {
		return "", fmt.Errorf("client is nil")
	}
	serviceNames := []string{"kube-dns", "coredns"}
	for _, name := range serviceNames {
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
	return "", fmt.Errorf("failed to resolve cluster DNS CIDR from kube-dns/coredns, please specify it in the netd config")
}

package fuseplugin

import (
	"context"
	"fmt"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/log"

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
	if !infrav1alpha1.HasDataPlaneServices(infra) {
		logger.Info("Data-plane services are disabled, skipping ctld")
		return nil
	}

	const ctldHTTPPort = 8095

	name := fmt.Sprintf("%s-ctld", infra.Name)
	labels := common.GetServiceLabels(infra.Name, "ctld")
	image := fmt.Sprintf("%s:%s", imageRepo, imageTag)
	pullPolicy := corev1.PullIfNotPresent
	if r.Resources.ImagePullPolicy != nil {
		pullPolicy = *r.Resources.ImagePullPolicy
	}

	nodeSelector, tolerations := common.ResolveSandboxNodePlacement(infra)
	args := ctldArgs(infra)

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
					Annotations: common.EnsurePodTemplateAnnotations(nil),
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
							Env: []corev1.EnvVar{
								{
									Name:  "SERVICE",
									Value: "ctld",
								},
								{
									Name: "NODE_NAME",
									ValueFrom: &corev1.EnvVarSource{
										FieldRef: &corev1.ObjectFieldSelector{FieldPath: "spec.nodeName"},
									},
								},
							},
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
							},
							ReadinessProbe: &corev1.Probe{
								ProbeHandler: corev1.ProbeHandler{
									HTTPGet: &corev1.HTTPGetAction{Path: "/readyz", Port: intstr.FromString("http")},
								},
								InitialDelaySeconds: 2,
								PeriodSeconds:       5,
							},
							SecurityContext: &corev1.SecurityContext{
								Privileged: common.BoolPtr(true),
							},
							VolumeMounts: []corev1.VolumeMount{
								{
									Name:      "device-plugin",
									MountPath: "/var/lib/kubelet/device-plugins",
								},
								{
									Name:      "host-cgroup",
									MountPath: "/host-sys/fs/cgroup",
								},
								{
									Name:      "containerd-sock",
									MountPath: "/host-run/containerd",
								},
							},
						},
					},
					Volumes: []corev1.Volume{
						{
							Name: "device-plugin",
							VolumeSource: corev1.VolumeSource{
								HostPath: &corev1.HostPathVolumeSource{
									Path: "/var/lib/kubelet/device-plugins",
								},
							},
						},
						{
							Name: "host-cgroup",
							VolumeSource: corev1.VolumeSource{
								HostPath: &corev1.HostPathVolumeSource{
									Path: "/sys/fs/cgroup",
								},
							},
						},
						{
							Name: "containerd-sock",
							VolumeSource: corev1.VolumeSource{
								HostPath: &corev1.HostPathVolumeSource{
									Path: "/run/containerd",
								},
							},
						},
					},
				},
			},
		},
	}

	return r.Resources.ApplyDaemonSet(ctx, infra, desired)
}

func ctldArgs(infra *infrav1alpha1.Sandbox0Infra) []string {
	cfg := ctldManagerConfig(infra)
	pauseMinMemoryRequest := "10Mi"
	pauseMinMemoryLimit := "32Mi"
	pauseMemoryBufferRatio := "1.1"
	pauseMinCPU := "10m"
	defaultTTL := "0s"
	if cfg != nil {
		pauseMinMemoryRequest = stringOrDefault(cfg.PauseMinMemoryRequest, pauseMinMemoryRequest)
		pauseMinMemoryLimit = stringOrDefault(cfg.PauseMinMemoryLimit, pauseMinMemoryLimit)
		pauseMemoryBufferRatio = stringOrDefault(cfg.PauseMemoryBufferRatio, pauseMemoryBufferRatio)
		pauseMinCPU = stringOrDefault(cfg.PauseMinCPU, pauseMinCPU)
		if cfg.DefaultSandboxTTL.Duration > 0 {
			defaultTTL = cfg.DefaultSandboxTTL.Duration.String()
		}
	}

	args := []string{
		"-http-addr=:8095",
		"-cgroup-root=/host-sys/fs/cgroup",
		"-cri-endpoint=/host-run/containerd/containerd.sock",
		fmt.Sprintf("-pause-min-memory-request=%s", pauseMinMemoryRequest),
		fmt.Sprintf("-pause-min-memory-limit=%s", pauseMinMemoryLimit),
		fmt.Sprintf("-pause-memory-buffer-ratio=%s", pauseMemoryBufferRatio),
		fmt.Sprintf("-pause-min-cpu=%s", pauseMinCPU),
		fmt.Sprintf("-default-sandbox-ttl=%s", defaultTTL),
	}
	return args
}

func ctldManagerConfig(infra *infrav1alpha1.Sandbox0Infra) *infrav1alpha1.ManagerConfig {
	if infra == nil || infra.Spec.Services == nil || infra.Spec.Services.Manager == nil {
		return nil
	}
	return infra.Spec.Services.Manager.Config
}

func stringOrDefault(value, fallback string) string {
	if value == "" {
		return fallback
	}
	return value
}

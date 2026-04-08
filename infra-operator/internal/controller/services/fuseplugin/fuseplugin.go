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
							Args:            []string{"-http-addr=:8095", "-cgroup-root=/host-sys/fs/cgroup"},
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
					},
				},
			},
		},
	}

	return r.Resources.ApplyDaemonSet(ctx, infra, desired)
}

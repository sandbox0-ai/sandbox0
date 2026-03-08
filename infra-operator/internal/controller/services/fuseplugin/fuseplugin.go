package fuseplugin

import (
	"context"
	"fmt"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
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
		logger.Info("Data-plane services are disabled, skipping fuse device plugin")
		return nil
	}

	name := fmt.Sprintf("%s-k8s-plugin", infra.Name)
	labels := common.GetServiceLabels(infra.Name, "k8s-plugin")
	image := fmt.Sprintf("%s:%s", imageRepo, imageTag)
	pullPolicy := corev1.PullIfNotPresent
	if r.Resources.ImagePullPolicy != nil {
		pullPolicy = *r.Resources.ImagePullPolicy
	}

	ds := &appsv1.DaemonSet{}
	err := r.Resources.Client.Get(ctx, types.NamespacedName{Name: name, Namespace: infra.Namespace}, ds)
	if err != nil && !apierrors.IsNotFound(err) {
		return err
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
					Annotations: common.EnsurePodTemplateAnnotations(infra, nil),
				},
				Spec: corev1.PodSpec{
					NodeSelector: nodeSelector,
					Tolerations:  tolerations,
					HostNetwork:  true,
					DNSPolicy:    corev1.DNSClusterFirstWithHostNet,
					Containers: []corev1.Container{
						{
							Name:            "fuse-device-plugin",
							Image:           image,
							ImagePullPolicy: pullPolicy,
							Env: []corev1.EnvVar{
								{
									Name:  "SERVICE",
									Value: "k8s-plugin",
								},
							},
							SecurityContext: &corev1.SecurityContext{
								AllowPrivilegeEscalation: common.BoolPtr(false),
								Privileged:               common.BoolPtr(false),
								Capabilities: &corev1.Capabilities{
									Drop: []corev1.Capability{"ALL"},
								},
							},
							VolumeMounts: []corev1.VolumeMount{
								{
									Name:      "device-plugin",
									MountPath: "/var/lib/kubelet/device-plugins",
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
					},
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

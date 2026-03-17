package egressbroker

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
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

// Reconcile reconciles the egress-broker deployment.
func (r *Reconciler) Reconcile(ctx context.Context, infra *infrav1alpha1.Sandbox0Infra, imageRepo, imageTag string) error {
	logger := log.FromContext(ctx)

	if infra.Spec.Services != nil && infra.Spec.Services.EgressBroker != nil && !infra.Spec.Services.EgressBroker.Enabled {
		logger.Info("egress-broker is disabled, skipping")
		return nil
	}

	deploymentName := fmt.Sprintf("%s-egress-broker", infra.Name)
	serviceName := deploymentName
	replicas := int32(1)
	if infra.Spec.Services != nil && infra.Spec.Services.EgressBroker != nil {
		replicas = infra.Spec.Services.EgressBroker.Replicas
	}

	labels := common.GetServiceLabels(infra.Name, "egress-broker")
	config := &apiconfig.EgressBrokerConfig{}
	if infra.Spec.Services != nil && infra.Spec.Services.EgressBroker != nil && infra.Spec.Services.EgressBroker.Config != nil {
		config = infra.Spec.Services.EgressBroker.Config
	}
	if config.HTTPPort == 0 {
		config.HTTPPort = 8082
	}
	if infra.Spec.Region != "" {
		config.RegionID = infra.Spec.Region
	}
	if infra.Spec.Cluster != nil && infra.Spec.Cluster.ID != "" {
		config.ClusterID = infra.Spec.Cluster.ID
	}

	if err := r.Resources.ReconcileServiceConfigMap(ctx, infra, deploymentName, labels, config); err != nil {
		return err
	}

	var resources *corev1.ResourceRequirements
	serviceConfig := (*infrav1alpha1.ServiceNetworkConfig)(nil)
	if infra.Spec.Services != nil && infra.Spec.Services.EgressBroker != nil {
		resources = infra.Spec.Services.EgressBroker.Resources
		serviceConfig = infra.Spec.Services.EgressBroker.Service
	}

	httpPort := int32(config.HTTPPort)
	if err := r.Resources.ReconcileDeployment(ctx, infra, deploymentName, labels, replicas, common.ServiceDefinition{
		Name:       "egress-broker",
		Port:       httpPort,
		TargetPort: httpPort,
		Ports: []corev1.ContainerPort{
			{
				Name:          "http",
				ContainerPort: httpPort,
			},
		},
		Image: fmt.Sprintf("%s:%s", imageRepo, imageTag),
		EnvVars: []corev1.EnvVar{
			{
				Name:  "SERVICE",
				Value: "egress-broker",
			},
			{
				Name:  "CONFIG_PATH",
				Value: "/config/config.yaml",
			},
		},
		VolumeMounts: []corev1.VolumeMount{
			{
				Name:      "config",
				MountPath: "/config/config.yaml",
				SubPath:   "config.yaml",
				ReadOnly:  true,
			},
		},
		Volumes: []corev1.Volume{
			{
				Name: "config",
				VolumeSource: corev1.VolumeSource{
					ConfigMap: &corev1.ConfigMapVolumeSource{
						LocalObjectReference: corev1.LocalObjectReference{Name: deploymentName},
					},
				},
			},
		},
		LivenessProbe: &corev1.Probe{
			ProbeHandler: corev1.ProbeHandler{
				HTTPGet: &corev1.HTTPGetAction{
					Path: "/healthz",
					Port: intstr.FromString("http"),
				},
			},
			InitialDelaySeconds: 10,
			PeriodSeconds:       10,
		},
		ReadinessProbe: &corev1.Probe{
			ProbeHandler: corev1.ProbeHandler{
				HTTPGet: &corev1.HTTPGetAction{
					Path: "/readyz",
					Port: intstr.FromString("http"),
				},
			},
			InitialDelaySeconds: 5,
			PeriodSeconds:       5,
		},
		Resources: resources,
	}); err != nil {
		return err
	}

	serviceType := common.ResolveServiceType(serviceConfig)
	servicePort := common.ResolveServicePort(serviceConfig, httpPort)
	if err := r.Resources.ReconcileService(ctx, infra, serviceName, labels, serviceType, servicePort, httpPort); err != nil {
		return err
	}

	return r.Resources.EnsureDeploymentReady(ctx, infra, deploymentName, replicas)
}

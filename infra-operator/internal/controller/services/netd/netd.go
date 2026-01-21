/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package netd

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/log"

	apiconfig "github.com/sandbox0-ai/infra/infra-operator/api/config"
	infrav1alpha1 "github.com/sandbox0-ai/infra/infra-operator/api/v1alpha1"
	"github.com/sandbox0-ai/infra/infra-operator/internal/controller/pkg/common"
)

type Reconciler struct {
	Resources *common.ResourceManager
}

func NewReconciler(resources *common.ResourceManager) *Reconciler {
	return &Reconciler{Resources: resources}
}

// Reconcile reconciles the netd daemonset.
func (r *Reconciler) Reconcile(ctx context.Context, infra *infrav1alpha1.Sandbox0Infra, imageRepo string) error {
	logger := log.FromContext(ctx)

	// Skip if not enabled
	if infra.Spec.Services != nil && infra.Spec.Services.Netd != nil && !infra.Spec.Services.Netd.Enabled {
		logger.Info("Netd is disabled, skipping")
		return nil
	}

	dsName := fmt.Sprintf("%s-netd", infra.Name)
	labels := common.GetServiceLabels(infra.Name, "netd")
	config, err := r.buildConfig(ctx, infra)
	if err != nil {
		return err
	}
	if err := r.Resources.ReconcileServiceConfigMap(ctx, infra, dsName, labels, config); err != nil {
		return err
	}

	// Create DaemonSet
	if err := r.Resources.ReconcileDaemonSet(ctx, infra, dsName, labels, common.ServiceDefinition{
		Name:               "netd",
		Port:               8080,
		TargetPort:         8080,
		ServiceAccountName: fmt.Sprintf("%s-netd", infra.Name),
		Ports: []corev1.ContainerPort{
			{
				Name:          "metrics",
				ContainerPort: 9090,
			},
			{
				Name:          "health",
				ContainerPort: 8080,
			},
			{
				Name:          "proxy-http",
				ContainerPort: 18080,
			},
			{
				Name:          "proxy-https",
				ContainerPort: 18443,
			},
		},
		Image: fmt.Sprintf("%s:%s", imageRepo, infra.Spec.Version),
		EnvVars: []corev1.EnvVar{
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
					FieldRef: &corev1.ObjectFieldSelector{
						FieldPath: "spec.nodeName",
					},
				},
			},
		},
		VolumeMounts: []corev1.VolumeMount{
			{
				Name:      "config",
				MountPath: "/config",
				ReadOnly:  true,
			},
			{
				Name:             "bpf-fs",
				MountPath:        "/sys/fs/bpf",
				MountPropagation: func() *corev1.MountPropagationMode { mode := corev1.MountPropagationBidirectional; return &mode }(),
			},
			{
				Name:      "cgroup",
				MountPath: "/sys/fs/cgroup",
				ReadOnly:  true,
			},
		},
		Volumes: []corev1.Volume{
			{
				Name: "config",
				VolumeSource: corev1.VolumeSource{
					ConfigMap: &corev1.ConfigMapVolumeSource{
						LocalObjectReference: corev1.LocalObjectReference{Name: dsName},
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
				Name: "cgroup",
				VolumeSource: corev1.VolumeSource{
					HostPath: &corev1.HostPathVolumeSource{
						Path: "/sys/fs/cgroup",
						Type: func() *corev1.HostPathType { t := corev1.HostPathDirectory; return &t }(),
					},
				},
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
	}); err != nil {
		return err
	}

	if err := r.Resources.ReconcileService(ctx, infra, fmt.Sprintf("%s-metrics", dsName), labels, corev1.ServiceTypeClusterIP, 9090, 9090); err != nil {
		return err
	}

	logger.Info("Netd reconciled successfully")
	return nil
}

func (r *Reconciler) buildConfig(ctx context.Context, infra *infrav1alpha1.Sandbox0Infra) (*apiconfig.NetdConfig, error) {
	var raw *runtime.RawExtension
	if infra.Spec.Services != nil && infra.Spec.Services.Netd != nil {
		raw = infra.Spec.Services.Netd.Config
	}

	cfg := apiconfig.DefaultNetdConfig()
	if err := common.DecodeServiceConfig(raw, cfg); err != nil {
		return nil, err
	}

	if cfg.NodeName == "" {
		cfg.NodeName = "${NODE_NAME}"
	}
	if cfg.Namespace == "" {
		cfg.Namespace = infra.Namespace
	}

	return cfg, nil
}

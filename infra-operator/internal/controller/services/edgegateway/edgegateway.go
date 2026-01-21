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

package edgegateway

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
	"github.com/sandbox0-ai/infra/infra-operator/internal/controller/services/database"
	"github.com/sandbox0-ai/infra/infra-operator/internal/controller/services/internalauth"
)

type Reconciler struct {
	Resources *common.ResourceManager
}

func NewReconciler(resources *common.ResourceManager) *Reconciler {
	return &Reconciler{Resources: resources}
}

// Reconcile reconciles the edge-gateway deployment.
func (r *Reconciler) Reconcile(ctx context.Context, infra *infrav1alpha1.Sandbox0Infra, imageRepo string) error {
	logger := log.FromContext(ctx)

	// Skip if not enabled
	if infra.Spec.Services != nil && infra.Spec.Services.EdgeGateway != nil && !infra.Spec.Services.EdgeGateway.Enabled {
		logger.Info("Edge gateway is disabled, skipping")
		return nil
	}

	deploymentName := fmt.Sprintf("%s-edge-gateway", infra.Name)
	serviceName := deploymentName

	replicas := int32(1)
	if infra.Spec.Services != nil && infra.Spec.Services.EdgeGateway != nil {
		replicas = infra.Spec.Services.EdgeGateway.Replicas
	}

	labels := common.GetServiceLabels(infra.Name, "edge-gateway")
	keySecretName, privateKeyKey, _ := internalauth.GetControlPlaneKeyRefs(infra)

	config, err := r.buildConfig(ctx, infra)
	if err != nil {
		return err
	}
	if err := r.Resources.ReconcileServiceConfigMap(ctx, infra, deploymentName, labels, config); err != nil {
		return err
	}

	// Create deployment
	if err := r.Resources.ReconcileDeployment(ctx, infra, deploymentName, labels, replicas, common.ServiceDefinition{
		Name:       "edge-gateway",
		Port:       8080,
		TargetPort: 8080,
		Ports: []corev1.ContainerPort{
			{
				Name:          "http",
				ContainerPort: 8080,
			},
		},
		Image: fmt.Sprintf("%s:%s", imageRepo, infra.Spec.Version),
		EnvVars: []corev1.EnvVar{
			{
				Name:  "SERVICE",
				Value: "edge-gateway",
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
			{
				Name:      "internal-jwt-private-key",
				MountPath: "/secrets/internal_jwt_private.key",
				SubPath:   "internal_jwt_private.key",
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
			{
				Name: "internal-jwt-private-key",
				VolumeSource: corev1.VolumeSource{
					Secret: &corev1.SecretVolumeSource{
						SecretName: keySecretName,
						Items: []corev1.KeyToPath{
							{
								Key:  privateKeyKey,
								Path: "internal_jwt_private.key",
							},
						},
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
	}); err != nil {
		return err
	}

	// Create service
	serviceType := corev1.ServiceTypeClusterIP
	servicePort := int32(80)
	if infra.Spec.Services != nil && infra.Spec.Services.EdgeGateway != nil && infra.Spec.Services.EdgeGateway.Service != nil {
		serviceType = infra.Spec.Services.EdgeGateway.Service.Type
		servicePort = infra.Spec.Services.EdgeGateway.Service.Port
	}
	if err := r.Resources.ReconcileService(ctx, infra, serviceName, labels, serviceType, servicePort, 8080); err != nil {
		return err
	}

	// Create ingress if enabled
	if infra.Spec.Services != nil && infra.Spec.Services.EdgeGateway != nil &&
		infra.Spec.Services.EdgeGateway.Ingress != nil && infra.Spec.Services.EdgeGateway.Ingress.Enabled {
		if err := r.Resources.ReconcileIngress(ctx, infra, serviceName, infra.Spec.Services.EdgeGateway.Ingress); err != nil {
			return err
		}
	}

	// Update endpoints in status
	updateEndpoints(infra, serviceName, servicePort)

	logger.Info("Edge gateway reconciled successfully")
	return nil
}

func (r *Reconciler) buildConfig(ctx context.Context, infra *infrav1alpha1.Sandbox0Infra) (*apiconfig.EdgeGatewayConfig, error) {
	var raw *runtime.RawExtension
	if infra.Spec.Services != nil && infra.Spec.Services.EdgeGateway != nil {
		raw = infra.Spec.Services.EdgeGateway.Config
	}

	cfg := apiconfig.DefaultEdgeGatewayConfig()
	if err := common.DecodeServiceConfig(raw, cfg); err != nil {
		return nil, err
	}

	if dsn, err := database.GetDatabaseDSN(ctx, r.Resources.Client, infra); err == nil {
		if cfg.DatabaseURL == "" {
			cfg.DatabaseURL = dsn
		}
	}

	internalGatewayURL := fmt.Sprintf("http://%s-internal-gateway:8443", infra.Name)
	if cfg.DefaultInternalGatewayURL == "" {
		cfg.DefaultInternalGatewayURL = internalGatewayURL
	}

	if infra.Spec.InitUser != nil && infra.Spec.InitUser.Enabled {
		password, err := common.GetSecretValue(ctx, r.Resources.Client, infra.Namespace, infra.Spec.InitUser.PasswordSecret)
		if err != nil {
			return nil, err
		}

		if cfg.BuiltInAuth.InitUser == nil {
			cfg.BuiltInAuth.InitUser = &apiconfig.InitUserConfig{
				Email:    infra.Spec.InitUser.Email,
				Password: password,
				Name:     infra.Spec.InitUser.Name,
			}
		}
	}

	for i := range cfg.OIDCProviders {
		if len(cfg.OIDCProviders[i].Scopes) == 0 {
			cfg.OIDCProviders[i].Scopes = []string{"openid", "email", "profile"}
		}
	}

	return cfg, nil
}

func updateEndpoints(infra *infrav1alpha1.Sandbox0Infra, serviceName string, servicePort int32) {
	if infra.Status.Endpoints == nil {
		infra.Status.Endpoints = &infrav1alpha1.EndpointsStatus{}
	}

	internalURL := fmt.Sprintf("http://%s:%d", serviceName, servicePort)
	infra.Status.Endpoints.EdgeGatewayInternal = internalURL

	if infra.Spec.Services != nil && infra.Spec.Services.EdgeGateway != nil &&
		infra.Spec.Services.EdgeGateway.Ingress != nil && infra.Spec.Services.EdgeGateway.Ingress.Enabled {
		ingress := infra.Spec.Services.EdgeGateway.Ingress
		scheme := "http"
		if ingress.TLSSecret != "" {
			scheme = "https"
		}
		infra.Status.Endpoints.EdgeGateway = fmt.Sprintf("%s://%s", scheme, ingress.Host)
	}
}

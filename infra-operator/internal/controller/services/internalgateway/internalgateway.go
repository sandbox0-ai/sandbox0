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

package internalgateway

import (
	"context"
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/log"

	apiconfig "github.com/sandbox0-ai/infra/infra-operator/api/config"
	infrav1alpha1 "github.com/sandbox0-ai/infra/infra-operator/api/v1alpha1"
	"github.com/sandbox0-ai/infra/infra-operator/internal/controller/pkg/common"
	"github.com/sandbox0-ai/infra/infra-operator/internal/controller/services/database"
	"github.com/sandbox0-ai/infra/infra-operator/internal/controller/services/internalauth"
	pkginternalauth "github.com/sandbox0-ai/infra/pkg/internalauth"
)

type Reconciler struct {
	Resources *common.ResourceManager
}

func NewReconciler(resources *common.ResourceManager) *Reconciler {
	return &Reconciler{Resources: resources}
}

// Reconcile reconciles the internal-gateway deployment.
func (r *Reconciler) Reconcile(ctx context.Context, infra *infrav1alpha1.Sandbox0Infra, imageRepo string) error {
	logger := log.FromContext(ctx)

	// Skip if not enabled
	if infra.Spec.Services != nil && infra.Spec.Services.InternalGateway != nil && !infra.Spec.Services.InternalGateway.Enabled {
		logger.Info("Internal gateway is disabled, skipping")
		return nil
	}

	deploymentName := fmt.Sprintf("%s-internal-gateway", infra.Name)
	serviceName := deploymentName

	replicas := int32(1)
	if infra.Spec.Services != nil && infra.Spec.Services.InternalGateway != nil {
		replicas = infra.Spec.Services.InternalGateway.Replicas
	}

	labels := common.GetServiceLabels(infra.Name, "internal-gateway")
	dataPlaneSecretName, dataPlanePrivateKey, _ := internalauth.GetDataPlaneKeyRefs(infra)

	config, err := r.buildConfig(ctx, infra)
	if err != nil {
		return err
	}
	needEnterpriseLicense := internalGatewayPublicAuthEnabled(config.AuthMode) &&
		apiconfig.HasEnabledOIDCProviders(config.OIDCProviders)
	if needEnterpriseLicense && strings.TrimSpace(config.LicenseFile) == "" {
		config.LicenseFile = common.EnterpriseLicenseDefaultPath
	}
	if needEnterpriseLicense {
		_, err := common.GetSecretValue(ctx, r.Resources.Client, infra.Namespace, infrav1alpha1.SecretKeyRef{
			Name: common.EnterpriseLicenseSecretName(infra.Name),
			Key:  common.EnterpriseLicenseSecretKey,
		})
		if err != nil {
			return fmt.Errorf("enterprise license secret is required for OIDC SSO: %w", err)
		}
	}
	if err := r.Resources.ReconcileServiceConfigMap(ctx, infra, deploymentName, labels, config); err != nil {
		return err
	}

	needsControlPlanePublicKey := internalAuthRequiresControlPlaneKey(config)
	controlPlanePublicSecretName := ""
	controlPlanePublicKeyKey := ""
	if needsControlPlanePublicKey {
		controlPlaneSecretName, _, controlPlanePublicKey := internalauth.GetControlPlaneKeyRefs(infra)
		controlPlanePublicSecretName, controlPlanePublicKeyKey = internalauth.GetControlPlanePublicKeyRef(infra)
		if controlPlanePublicSecretName == "" {
			controlPlanePublicSecretName = controlPlaneSecretName
			controlPlanePublicKeyKey = controlPlanePublicKey
		}
	}
	var resources *corev1.ResourceRequirements
	serviceConfig := (*infrav1alpha1.ServiceNetworkConfig)(nil)
	if infra.Spec.Services != nil && infra.Spec.Services.InternalGateway != nil {
		resources = infra.Spec.Services.InternalGateway.Resources
		serviceConfig = infra.Spec.Services.InternalGateway.Service
	}

	httpPort := int32(config.HTTPPort)

	// Create deployment
	volumeMounts := []corev1.VolumeMount{
		{
			Name:      "config",
			MountPath: "/config/config.yaml",
			SubPath:   "config.yaml",
			ReadOnly:  true,
		},
		{
			Name:      "internal-jwt-private-key",
			MountPath: pkginternalauth.DefaultInternalJWTPrivateKeyPath,
			SubPath:   "internal_jwt_private.key",
			ReadOnly:  true,
		},
	}
	volumes := []corev1.Volume{
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
					SecretName: dataPlaneSecretName,
					Items: []corev1.KeyToPath{
						{
							Key:  dataPlanePrivateKey,
							Path: "internal_jwt_private.key",
						},
					},
				},
			},
		},
	}
	if needsControlPlanePublicKey {
		volumeMounts = append(volumeMounts, corev1.VolumeMount{
			Name:      "internal-jwt-public-key",
			MountPath: pkginternalauth.DefaultInternalJWTPublicKeyPath,
			SubPath:   "internal_jwt_public.key",
			ReadOnly:  true,
		})
		volumes = append(volumes, corev1.Volume{
			Name: "internal-jwt-public-key",
			VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{
					SecretName: controlPlanePublicSecretName,
					Items: []corev1.KeyToPath{
						{
							Key:  controlPlanePublicKeyKey,
							Path: "internal_jwt_public.key",
						},
					},
				},
			},
		})
	}
	if needEnterpriseLicense {
		volumeMounts = append(volumeMounts, corev1.VolumeMount{
			Name:      "enterprise-license",
			MountPath: config.LicenseFile,
			SubPath:   common.EnterpriseLicenseSecretKey,
			ReadOnly:  true,
		})
		volumes = append(volumes, corev1.Volume{
			Name: "enterprise-license",
			VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{
					SecretName: common.EnterpriseLicenseSecretName(infra.Name),
					Items: []corev1.KeyToPath{
						{
							Key:  common.EnterpriseLicenseSecretKey,
							Path: common.EnterpriseLicenseSecretKey,
						},
					},
				},
			},
		})
	}

	if err := r.Resources.ReconcileDeployment(ctx, infra, deploymentName, labels, replicas, common.ServiceDefinition{
		Name:       "internal-gateway",
		Port:       httpPort,
		TargetPort: httpPort,
		Ports: []corev1.ContainerPort{
			{
				Name:          "http",
				ContainerPort: httpPort,
			},
		},
		Image: fmt.Sprintf("%s:%s", imageRepo, infra.Spec.Version),
		EnvVars: []corev1.EnvVar{
			{
				Name:  "SERVICE",
				Value: "internal-gateway",
			},
			{
				Name:  "CONFIG_PATH",
				Value: "/config/config.yaml",
			},
		},
		VolumeMounts: volumeMounts,
		Volumes:      volumes,
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

	// Create service
	serviceType := common.ResolveServiceType(serviceConfig)
	servicePort := common.ResolveServicePort(serviceConfig, httpPort)
	if err := r.Resources.ReconcileService(ctx, infra, serviceName, labels, serviceType, servicePort, httpPort); err != nil {
		return err
	}

	if err := r.Resources.EnsureDeploymentReady(ctx, infra, deploymentName, replicas); err != nil {
		return err
	}

	// Update endpoints in status
	if infra.Status.Endpoints == nil {
		infra.Status.Endpoints = &infrav1alpha1.EndpointsStatus{}
	}
	infra.Status.Endpoints.InternalGateway = fmt.Sprintf("http://%s:%d", serviceName, servicePort)

	logger.Info("Internal gateway reconciled successfully")
	return nil
}

func (r *Reconciler) buildConfig(ctx context.Context, infra *infrav1alpha1.Sandbox0Infra) (*apiconfig.InternalGatewayConfig, error) {
	cfg := &apiconfig.InternalGatewayConfig{}
	if infra.Spec.Services != nil && infra.Spec.Services.InternalGateway != nil && infra.Spec.Services.InternalGateway.Config != nil {
		cfg = infra.Spec.Services.InternalGateway.Config
	}

	if dsn, err := database.GetDatabaseDSN(ctx, r.Resources.Client, infra); err == nil {
		cfg.DatabaseURL = dsn
	}

	managerConfig := &apiconfig.ManagerConfig{}
	if infra.Spec.Services != nil && infra.Spec.Services.Manager != nil && infra.Spec.Services.Manager.Config != nil {
		managerConfig = infra.Spec.Services.Manager.Config
	}
	managerServiceConfig := (*infrav1alpha1.ServiceNetworkConfig)(nil)
	if infra.Spec.Services != nil && infra.Spec.Services.Manager != nil {
		managerServiceConfig = infra.Spec.Services.Manager.Service
	}
	if infrav1alpha1.IsManagerEnabled(infra) {
		managerServicePort := common.ResolveServicePort(managerServiceConfig, int32(managerConfig.HTTPPort))
		managerURL := fmt.Sprintf("http://%s-manager:%d", infra.Name, managerServicePort)
		cfg.ManagerURL = managerURL
	} else {
		cfg.ManagerURL = ""
	}

	storageProxyConfig := &apiconfig.StorageProxyConfig{}
	if infra.Spec.Services != nil && infra.Spec.Services.StorageProxy != nil && infra.Spec.Services.StorageProxy.Config != nil {
		storageProxyConfig = infra.Spec.Services.StorageProxy.Config
	}
	if infrav1alpha1.IsStorageProxyEnabled(infra) {
		storageProxyHTTPPort := int32(storageProxyConfig.HTTPPort)
		storageProxyURL := fmt.Sprintf("http://%s-storage-proxy:%d", infra.Name, storageProxyHTTPPort)
		cfg.StorageProxyURL = storageProxyURL
	} else {
		cfg.StorageProxyURL = ""
	}

	if infra.Spec.InitUser != nil {
		secretRef := common.ResolveSecretKeyRef(infra.Spec.InitUser.PasswordSecret, "admin-password", "password")
		password, err := common.GetSecretValue(ctx, r.Resources.Client, infra.Namespace, secretRef)
		if err != nil {
			return nil, err
		}

		cfg.BuiltInAuth.InitUser = &apiconfig.InitUserConfig{
			Email:    infra.Spec.InitUser.Email,
			Password: password,
			Name:     infra.Spec.InitUser.Name,
		}
	}

	if strings.TrimSpace(cfg.JWTIssuer) == "" {
		cfg.JWTIssuer = "internal-gateway"
	}

	if internalGatewayPublicAuthEnabled(cfg.AuthMode) && strings.TrimSpace(cfg.JWTSecret) == "" {
		jwtSecret, err := common.EnsureSecretValue(
			ctx,
			r.Resources.Client,
			r.Resources.Scheme,
			infra,
			fmt.Sprintf("%s-internal-gateway-jwt", infra.Name),
			"jwt_secret",
			32,
		)
		if err != nil {
			return nil, err
		}
		cfg.JWTSecret = jwtSecret
	}

	// Copy public exposure config from CRD top-level spec
	if infra.Spec.PublicExposure != nil {
		cfg.PublicExposureEnabled = infra.Spec.PublicExposure.Enabled
		cfg.PublicRootDomain = infra.Spec.PublicExposure.RootDomain
		cfg.PublicRegionID = infra.Spec.PublicExposure.RegionID
	}

	return cfg, nil
}

func internalGatewayPublicAuthEnabled(mode string) bool {
	mode = strings.TrimSpace(strings.ToLower(mode))
	return mode == "public" || mode == "both"
}

func internalAuthRequiresControlPlaneKey(cfg *apiconfig.InternalGatewayConfig) bool {
	if cfg == nil {
		return true
	}
	mode := strings.TrimSpace(strings.ToLower(cfg.AuthMode))
	if mode == "" {
		mode = "internal"
	}
	return mode == "internal" || mode == "both"
}

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

package clustergateway

import (
	"context"
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/log"

	apiconfig "github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	infrav1alpha1 "github.com/sandbox0-ai/sandbox0/infra-operator/api/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/infra-operator/internal/controller/pkg/common"
	"github.com/sandbox0-ai/sandbox0/infra-operator/internal/controller/services/database"
	"github.com/sandbox0-ai/sandbox0/infra-operator/internal/controller/services/internalauth"
	infraplan "github.com/sandbox0-ai/sandbox0/infra-operator/internal/plan"
	"github.com/sandbox0-ai/sandbox0/infra-operator/internal/runtimeconfig"
	pkginternalauth "github.com/sandbox0-ai/sandbox0/pkg/internalauth"
)

type Reconciler struct {
	Resources *common.ResourceManager
}

func NewReconciler(resources *common.ResourceManager) *Reconciler {
	return &Reconciler{Resources: resources}
}

// Reconcile reconciles the cluster-gateway deployment.
func (r *Reconciler) Reconcile(ctx context.Context, infra *infrav1alpha1.Sandbox0Infra, imageRepo, imageTag string, compiledPlan *infraplan.InfraPlan) error {
	logger := log.FromContext(ctx)
	if compiledPlan == nil {
		compiledPlan = infraplan.Compile(infra)
	}

	// Skip if not enabled
	if infra.Spec.Services != nil && infra.Spec.Services.ClusterGateway != nil && !infra.Spec.Services.ClusterGateway.Enabled {
		logger.Info("Internal gateway is disabled, skipping")
		return nil
	}

	deploymentName := fmt.Sprintf("%s-cluster-gateway", infra.Name)
	serviceName := deploymentName

	replicas := int32(1)
	if infra.Spec.Services != nil && infra.Spec.Services.ClusterGateway != nil {
		replicas = infra.Spec.Services.ClusterGateway.Replicas
	}

	labels := common.GetServiceLabels(infra.Name, "cluster-gateway")
	dataPlaneSecretName, dataPlanePrivateKey, _ := internalauth.GetDataPlaneKeyRefs(infra)

	config, err := r.buildConfig(ctx, infra)
	if err != nil {
		return err
	}
	needEnterpriseLicense := compiledPlan.Enterprise.ClusterGateway
	common.NormalizeEnterpriseLicenseFile(&config.LicenseFile, needEnterpriseLicense)
	podAnnotations, err := common.ConfigHashAnnotation(config)
	if err != nil {
		return err
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
	if infra.Spec.Services != nil && infra.Spec.Services.ClusterGateway != nil {
		resources = infra.Spec.Services.ClusterGateway.Resources
		serviceConfig = infra.Spec.Services.ClusterGateway.Service
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
		volumeMounts, volumes = common.AppendEnterpriseLicenseVolume(infra.Name, config.LicenseFile, volumeMounts, volumes)
	}

	if err := r.Resources.ReconcileDeployment(ctx, infra, deploymentName, labels, replicas, common.ServiceDefinition{
		Name:       "cluster-gateway",
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
				Value: "cluster-gateway",
			},
			{
				Name:  "CONFIG_PATH",
				Value: "/config/config.yaml",
			},
		},
		VolumeMounts:   volumeMounts,
		Volumes:        volumes,
		PodAnnotations: podAnnotations,
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

	logger.Info("Internal gateway reconciled successfully")
	return nil
}

func (r *Reconciler) buildConfig(ctx context.Context, infra *infrav1alpha1.Sandbox0Infra) (*apiconfig.ClusterGatewayConfig, error) {
	cfg := &apiconfig.ClusterGatewayConfig{}
	if infra.Spec.Services != nil && infra.Spec.Services.ClusterGateway != nil {
		cfg = runtimeconfig.ToClusterGateway(infra.Spec.Services.ClusterGateway.Config)
	}

	if dsn, err := database.GetDatabaseDSN(ctx, r.Resources.Client, infra); err == nil {
		cfg.DatabaseURL = dsn
	}

	managerConfig := &apiconfig.ManagerConfig{}
	if infra.Spec.Services != nil && infra.Spec.Services.Manager != nil {
		managerConfig = runtimeconfig.ToManager(infra.Spec.Services.Manager.Config)
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
	if infra.Spec.Services != nil && infra.Spec.Services.StorageProxy != nil {
		storageProxyConfig = runtimeconfig.ToStorageProxy(infra.Spec.Services.StorageProxy.Config)
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
		cfg.JWTIssuer = "cluster-gateway"
	}

	if clusterGatewayPublicAuthEnabled(cfg.AuthMode) && strings.TrimSpace(cfg.JWTSecret) == "" {
		jwtSecret, err := common.EnsureSecretValue(
			ctx,
			r.Resources.Client,
			r.Resources.Scheme,
			infra,
			fmt.Sprintf("%s-cluster-gateway-jwt", infra.Name),
			"jwt_secret",
			32,
		)
		if err != nil {
			return nil, err
		}
		cfg.JWTSecret = jwtSecret
	}

	if strings.TrimSpace(infra.Spec.Region) != "" {
		cfg.RegionID = infra.Spec.Region
	}

	// Copy public exposure config from CRD top-level spec
	if infra.Spec.PublicExposure != nil {
		cfg.PublicExposureEnabled = infra.Spec.PublicExposure.Enabled
		cfg.PublicRootDomain = infra.Spec.PublicExposure.RootDomain
		cfg.PublicRegionID = infra.Spec.PublicExposure.RegionID
	}

	return cfg, nil
}

func clusterGatewayPublicAuthEnabled(mode string) bool {
	mode = strings.TrimSpace(strings.ToLower(mode))
	return mode == "public" || mode == "both"
}

func internalAuthRequiresControlPlaneKey(cfg *apiconfig.ClusterGatewayConfig) bool {
	if cfg == nil {
		return true
	}
	mode := strings.TrimSpace(strings.ToLower(cfg.AuthMode))
	if mode == "" {
		mode = "internal"
	}
	return mode == "internal" || mode == "both"
}

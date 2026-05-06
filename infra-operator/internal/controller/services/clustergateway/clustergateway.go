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
	infraplan "github.com/sandbox0-ai/sandbox0/infra-operator/internal/plan"
	"github.com/sandbox0-ai/sandbox0/infra-operator/internal/runtimeconfig"
	pkginternalauth "github.com/sandbox0-ai/sandbox0/pkg/internalauth"
)

type Reconciler struct {
	Resources *common.ResourceManager
}

const defaultClusterGatewayHTTPPort = 8443

func NewReconciler(resources *common.ResourceManager) *Reconciler {
	return &Reconciler{Resources: resources}
}

// Reconcile reconciles the cluster-gateway deployment.
func (r *Reconciler) Reconcile(ctx context.Context, imageRepo, imageTag string, compiledPlan *infraplan.InfraPlan) error {
	logger := log.FromContext(ctx)
	if compiledPlan == nil {
		return fmt.Errorf("compiled plan is required")
	}

	// Skip if not enabled
	if !compiledPlan.Components.EnableClusterGateway {
		logger.Info("Internal gateway is disabled, skipping")
		return nil
	}

	scope := compiledPlan.Scope
	deploymentName := fmt.Sprintf("%s-cluster-gateway", scope.Name)
	serviceName := deploymentName
	replicas := int32(1)
	var resources *corev1.ResourceRequirements
	serviceConfig := (*infrav1alpha1.ServiceNetworkConfig)(nil)
	if owner := scope.Owner(); owner != nil && owner.Spec.Services != nil && owner.Spec.Services.ClusterGateway != nil {
		replicas = owner.Spec.Services.ClusterGateway.Replicas
		resources = owner.Spec.Services.ClusterGateway.Resources
		serviceConfig = owner.Spec.Services.ClusterGateway.Service
	}

	labels := common.GetServiceLabels(scope.Name, "cluster-gateway")
	dataPlaneSecretName, dataPlanePrivateKey, _ := compiledPlan.DataPlaneKeyRefs()

	config, err := r.buildConfig(ctx, compiledPlan)
	if err != nil {
		return err
	}
	needEnterpriseLicense := compiledPlan.Enterprise.ClusterGateway
	common.NormalizeEnterpriseLicenseFile(&config.LicenseFile, needEnterpriseLicense)
	podAnnotations, err := common.ConfigHashAnnotation(config)
	if err != nil {
		return err
	}
	if err := r.Resources.ReconcileServiceConfigMapWithScope(ctx, scope, deploymentName, labels, config); err != nil {
		return err
	}

	needsControlPlanePublicKey := internalAuthRequiresControlPlaneKey(config)
	controlPlanePublicSecretName := ""
	controlPlanePublicKeyKey := ""
	if needsControlPlanePublicKey {
		controlPlaneSecretName, _, controlPlanePublicKey := compiledPlan.ControlPlaneKeyRefs()
		controlPlanePublicSecretName, controlPlanePublicKeyKey = compiledPlan.ControlPlanePublicKeyRef()
		if controlPlanePublicSecretName == "" {
			controlPlanePublicSecretName = controlPlaneSecretName
			controlPlanePublicKeyKey = controlPlanePublicKey
		}
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
		volumeMounts, volumes = common.AppendEnterpriseLicenseVolumeWithSecretRef(compiledPlan.EnterpriseLicenseSecretRef(), config.LicenseFile, volumeMounts, volumes)
	}
	envVars := []corev1.EnvVar{
		{
			Name:  "SERVICE",
			Value: "cluster-gateway",
		},
		{
			Name:  "CONFIG_PATH",
			Value: "/config/config.yaml",
		},
	}
	envVars = append(envVars, compiledPlan.ObservabilityEnvVars()...)

	if err := r.Resources.ReconcileDeploymentWithScope(ctx, scope, deploymentName, labels, replicas, common.ServiceDefinition{
		Name:       "cluster-gateway",
		Port:       httpPort,
		TargetPort: httpPort,
		Ports: []corev1.ContainerPort{
			{
				Name:          "http",
				ContainerPort: httpPort,
			},
		},
		Image:          fmt.Sprintf("%s:%s", imageRepo, imageTag),
		EnvVars:        envVars,
		VolumeMounts:   volumeMounts,
		Volumes:        volumes,
		PodAnnotations: podAnnotations,
		LivenessProbe: &corev1.Probe{
			ProbeHandler: corev1.ProbeHandler{
				HTTPGet: &corev1.HTTPGetAction{
					Path:   "/healthz",
					Port:   intstr.FromString("http"),
					Scheme: corev1.URISchemeHTTP,
				},
			},
			InitialDelaySeconds: 10,
			PeriodSeconds:       10,
		},
		ReadinessProbe: &corev1.Probe{
			ProbeHandler: corev1.ProbeHandler{
				HTTPGet: &corev1.HTTPGetAction{
					Path:   "/readyz",
					Port:   intstr.FromString("http"),
					Scheme: corev1.URISchemeHTTP,
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
	serviceAnnotations := common.ResolveServiceAnnotations(serviceConfig)
	if err := r.Resources.ReconcileServiceWithScope(ctx, scope, serviceName, labels, serviceType, serviceAnnotations, servicePort, httpPort); err != nil {
		return err
	}

	if err := r.Resources.EnsureDeploymentReadyWithScope(ctx, scope, deploymentName, replicas); err != nil {
		return err
	}

	logger.Info("Internal gateway reconciled successfully")
	return nil
}

func (r *Reconciler) buildConfig(ctx context.Context, compiledPlan *infraplan.InfraPlan) (*apiconfig.ClusterGatewayConfig, error) {
	cfg := &apiconfig.ClusterGatewayConfig{}
	if compiledPlan != nil && compiledPlan.Components.EnableClusterGateway && compiledPlan.Services.ClusterGateway.Name != "" {
		if compiledPlan.Scope.Owner() != nil && compiledPlan.Scope.Owner().Spec.Services != nil && compiledPlan.Scope.Owner().Spec.Services.ClusterGateway != nil {
			cfg = runtimeconfig.ToClusterGateway(compiledPlan.Scope.Owner().Spec.Services.ClusterGateway.Config)
		}
	}
	if compiledPlan == nil {
		return nil, fmt.Errorf("compiled plan is required")
	}
	if cfg.HTTPPort == 0 {
		cfg.HTTPPort = defaultClusterGatewayHTTPPort
	}
	cfg.AuthMode = deriveClusterGatewayAuthMode(cfg.AuthMode, compiledPlan)
	resolvedRegionID := strings.TrimSpace(cfg.RegionID)

	if dsn, err := compiledPlan.DatabaseDSN(ctx, r.Resources.Client); err == nil {
		cfg.DatabaseURL = dsn
	}
	if owner := compiledPlan.Scope.Owner(); owner != nil {
		sshPort := int32(2222)
		if owner.Spec.Services != nil && owner.Spec.Services.SSHGateway != nil && owner.Spec.Services.SSHGateway.Config != nil && owner.Spec.Services.SSHGateway.Config.SSHPort != 0 {
			sshPort = int32(owner.Spec.Services.SSHGateway.Config.SSHPort)
		}
		if sshHost, advertisedPort, ok := common.ResolveSSHEndpoint(owner, sshPort); ok {
			cfg.SSHEndpointHost = sshHost
			cfg.SSHEndpointPort = int(advertisedPort)
		}
	}

	if compiledPlan.Components.EnableManager {
		cfg.ManagerURL = compiledPlan.Services.Manager.URL
	} else {
		cfg.ManagerURL = ""
	}

	if compiledPlan.Components.EnableStorageProxy {
		cfg.StorageProxyURL = compiledPlan.Services.StorageProxy.URL
	} else {
		cfg.StorageProxyURL = ""
	}

	if initUser := compiledPlan.InitUser(); initUser != nil && clusterGatewayPublicAuthEnabled(cfg.AuthMode) {
		password := ""
		if cfg.BuiltInAuth.Enabled || !apiconfig.HasEnabledOIDCProviders(cfg.OIDCProviders) {
			secretRef := common.ResolveSecretKeyRef(initUser.PasswordSecret, "admin-password", "password")
			var err error
			password, err = common.GetSecretValue(ctx, r.Resources.Client, compiledPlan.Scope.Namespace, secretRef)
			if err != nil {
				return nil, err
			}
		}

		homeRegionID := strings.TrimSpace(initUser.HomeRegionID)
		if homeRegionID == "" {
			homeRegionID = resolvedRegionID
		}

		cfg.BuiltInAuth.InitUser = &apiconfig.InitUserConfig{
			Email:        initUser.Email,
			Password:     password,
			Name:         initUser.Name,
			HomeRegionID: homeRegionID,
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
			compiledPlan.Scope.Owner(),
			compiledPlan.ClusterGatewayJWTSecretName(),
			"jwt_secret",
			32,
		)
		if err != nil {
			return nil, err
		}
		cfg.JWTSecret = jwtSecret
	}
	if owner := compiledPlan.Scope.Owner(); owner != nil {
		if strings.TrimSpace(owner.Spec.Region) != "" {
			resolvedRegionID = strings.TrimSpace(owner.Spec.Region)
		}
		if owner.Spec.PublicExposure != nil {
			cfg.PublicExposureEnabled = owner.Spec.PublicExposure.Enabled
			cfg.PublicRootDomain = owner.Spec.PublicExposure.RootDomain
			cfg.PublicRegionID = owner.Spec.PublicExposure.RegionID
			if resolvedRegionID == "" {
				resolvedRegionID = strings.TrimSpace(owner.Spec.PublicExposure.RegionID)
			}
		}
	}

	cfg.RegionID = resolvedRegionID
	if cfg.BuiltInAuth.InitUser != nil && strings.TrimSpace(cfg.BuiltInAuth.InitUser.HomeRegionID) == "" {
		cfg.BuiltInAuth.InitUser.HomeRegionID = resolvedRegionID
	}

	return cfg, nil
}

func clusterGatewayPublicAuthEnabled(mode string) bool {
	mode = strings.TrimSpace(strings.ToLower(mode))
	return mode == "public" || mode == "both"
}

func deriveClusterGatewayAuthMode(mode string, compiledPlan *infraplan.InfraPlan) string {
	normalized := strings.TrimSpace(strings.ToLower(mode))
	if normalized == "" {
		normalized = "internal"
	}
	if normalized == "public" && compiledPlan != nil && compiledPlan.RegionalGateway.Enabled {
		return "both"
	}
	return normalized
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

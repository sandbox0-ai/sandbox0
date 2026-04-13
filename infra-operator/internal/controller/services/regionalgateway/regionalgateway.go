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

package regionalgateway

import (
	"context"
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/log"

	apiconfig "github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	infrav1alpha1 "github.com/sandbox0-ai/sandbox0/infra-operator/api/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/infra-operator/internal/controller/pkg/common"
	infraplan "github.com/sandbox0-ai/sandbox0/infra-operator/internal/plan"
	pkginternalauth "github.com/sandbox0-ai/sandbox0/pkg/internalauth"
)

type Reconciler struct {
	Resources *common.ResourceManager
}

func NewReconciler(resources *common.ResourceManager) *Reconciler {
	return &Reconciler{Resources: resources}
}

// Reconcile reconciles the regional-gateway deployment.
func (r *Reconciler) Reconcile(ctx context.Context, imageRepo, imageTag string, compiledPlan *infraplan.InfraPlan) error {
	logger := log.FromContext(ctx)
	if compiledPlan == nil {
		return fmt.Errorf("compiled plan is required")
	}

	// Skip if not enabled
	if !compiledPlan.RegionalGateway.Enabled {
		logger.Info("Edge gateway is disabled, skipping")
		return nil
	}

	scope := compiledPlan.Scope
	deploymentName := fmt.Sprintf("%s-regional-gateway", scope.Name)
	serviceName := deploymentName

	replicas := compiledPlan.RegionalGateway.Replicas

	labels := common.GetServiceLabels(scope.Name, "regional-gateway")
	keySecretName, privateKeyKey, publicKeyKey := compiledPlan.ControlPlaneKeyRefs()

	config, registryEnvVars, err := r.buildConfig(ctx, compiledPlan)
	if err != nil {
		return err
	}
	needEnterpriseLicense := compiledPlan.Enterprise.RegionalGateway
	common.NormalizeEnterpriseLicenseFile(&config.LicenseFile, needEnterpriseLicense)
	podAnnotations, err := common.ConfigHashAnnotation(config)
	if err != nil {
		return err
	}
	if err := r.Resources.ReconcileServiceConfigMapWithScope(ctx, scope, deploymentName, labels, config); err != nil {
		return err
	}

	resources := compiledPlan.RegionalGateway.Resources
	serviceConfig := compiledPlan.RegionalGateway.ServiceConfig

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
		{
			Name:      "internal-jwt-public-key",
			MountPath: pkginternalauth.DefaultInternalJWTPublicKeyPath,
			SubPath:   "internal_jwt_public.key",
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
		{
			Name: "internal-jwt-public-key",
			VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{
					SecretName: keySecretName,
					Items: []corev1.KeyToPath{
						{
							Key:  publicKeyKey,
							Path: "internal_jwt_public.key",
						},
					},
				},
			},
		},
	}
	if needEnterpriseLicense {
		volumeMounts, volumes = common.AppendEnterpriseLicenseVolumeWithSecretRef(compiledPlan.EnterpriseLicenseSecretRef(), config.LicenseFile, volumeMounts, volumes)
	}

	envVars := []corev1.EnvVar{
		{
			Name:  "SERVICE",
			Value: "regional-gateway",
		},
		{
			Name:  "CONFIG_PATH",
			Value: "/config/config.yaml",
		},
	}
	envVars = append(envVars, registryEnvVars...)

	// Create deployment
	httpPort := int32(config.HTTPPort)
	if err := r.Resources.ReconcileDeploymentWithScope(ctx, scope, deploymentName, labels, replicas, common.ServiceDefinition{
		Name:       "regional-gateway",
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
	if err := r.Resources.ReconcileServicePortsWithScopeAndSpecMutator(ctx, scope, serviceName, labels, serviceType, serviceAnnotations, []corev1.ServicePort{
		{
			Name:       "http",
			Port:       servicePort,
			TargetPort: intstr.FromInt(int(httpPort)),
			Protocol:   corev1.ProtocolTCP,
			NodePort: func() int32 {
				if serviceType == corev1.ServiceTypeNodePort {
					return servicePort
				}
				return 0
			}(),
		},
	}, func(spec *corev1.ServiceSpec) {
		if serviceType == corev1.ServiceTypeLoadBalancer {
			spec.ExternalTrafficPolicy = corev1.ServiceExternalTrafficPolicyLocal
		}
	}); err != nil {
		return err
	}

	// Create ingress if enabled
	if compiledPlan.RegionalGateway.IngressConfig != nil && compiledPlan.RegionalGateway.IngressConfig.Enabled {
		if err := r.Resources.ReconcileIngressWithScope(ctx, scope, serviceName, servicePort, compiledPlan.RegionalGateway.IngressConfig); err != nil {
			return err
		}
	} else {
		if err := r.deleteIngressIfExists(ctx, scope, serviceName); err != nil {
			return err
		}
	}

	if err := r.Resources.EnsureDeploymentReadyWithScope(ctx, scope, deploymentName, replicas); err != nil {
		return err
	}

	logger.Info("Edge gateway reconciled successfully")
	return nil
}

func (r *Reconciler) buildConfig(ctx context.Context, compiledPlan *infraplan.InfraPlan) (*apiconfig.RegionalGatewayConfig, []corev1.EnvVar, error) {
	cfg := &apiconfig.RegionalGatewayConfig{}
	if compiledPlan == nil {
		return nil, nil, fmt.Errorf("compiled plan is required")
	}
	if compiledPlan.RegionalGateway.Config != nil {
		cfg = compiledPlan.RegionalGateway.Config.DeepCopy()
	}

	if dsn, err := compiledPlan.DatabaseDSN(ctx, r.Resources.Client); err == nil {
		cfg.DatabaseURL = dsn
	}
	cfg.DefaultClusterGatewayURL = compiledPlan.RegionalGateway.DefaultClusterGatewayURL
	cfg.SchedulerEnabled = compiledPlan.Components.EnableScheduler
	cfg.SchedulerURL = compiledPlan.Services.Scheduler.URL

	authMode := strings.TrimSpace(strings.ToLower(cfg.AuthMode))
	if authMode == "" {
		authMode = "self_hosted"
	}
	if initUser := compiledPlan.InitUser(); initUser != nil && authMode != "federated_global" {
		password := ""
		if cfg.BuiltInAuth.Enabled || !apiconfig.HasEnabledOIDCProviders(cfg.OIDCProviders) {
			secretRef := common.ResolveSecretKeyRef(initUser.PasswordSecret, "admin-password", "password")
			var err error
			password, err = common.GetSecretValue(ctx, r.Resources.Client, compiledPlan.Scope.Namespace, secretRef)
			if err != nil {
				return nil, nil, err
			}
		}

		cfg.BuiltInAuth.InitUser = &apiconfig.InitUserConfig{
			Email:    initUser.Email,
			Password: password,
			Name:     initUser.Name,
		}
	}

	if authMode == "federated_global" {
		if strings.TrimSpace(cfg.JWTIssuer) == "" {
			cfg.JWTIssuer = compiledPlan.DefaultFederatedGlobalJWTIssuer()
		}
		if strings.TrimSpace(cfg.JWTPublicKeyPEM) == "" {
			_, publicKeyPEM, err := common.EnsureEd25519KeyPair(
				ctx,
				r.Resources.Client,
				r.Resources.Scheme,
				compiledPlan.Scope.Owner(),
				compiledPlan.SharedUserJWTSecretName(),
				"jwt_private_key_pem",
				"jwt_public_key_pem",
			)
			if err != nil {
				return nil, nil, err
			}
			cfg.JWTPublicKeyPEM = publicKeyPEM
		}
		cfg.JWTSecret = ""
		cfg.JWTPrivateKeyPEM = ""
	} else {
		if strings.TrimSpace(cfg.JWTIssuer) == "" {
			cfg.JWTIssuer = "regional-gateway"
		}

		if strings.TrimSpace(cfg.JWTSecret) == "" {
			jwtSecret, err := common.EnsureSecretValue(
				ctx,
				r.Resources.Client,
				r.Resources.Scheme,
				compiledPlan.Scope.Owner(),
				compiledPlan.RegionalGatewayJWTSecretName(),
				"jwt_secret",
				32,
			)
			if err != nil {
				return nil, nil, err
			}
			cfg.JWTSecret = jwtSecret
		}
	}
	registryEnvVars, err := compiledPlan.ConfigureRegionalGatewayRegistry(cfg)
	if err != nil {
		return nil, nil, err
	}

	return cfg, registryEnvVars, nil
}

func (r *Reconciler) applyRegistryConfig(infra *infrav1alpha1.Sandbox0Infra, cfg *apiconfig.RegionalGatewayConfig) ([]corev1.EnvVar, error) {
	return infraplan.Compile(infra).ConfigureRegionalGatewayRegistry(cfg)
}

func (r *Reconciler) deleteIngressIfExists(ctx context.Context, scope common.ObjectScope, name string) error {
	obj := &networkingv1.Ingress{}
	err := r.Resources.Client.Get(ctx, types.NamespacedName{Name: name, Namespace: scope.Namespace}, obj)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return err
	}
	if err := r.Resources.Client.Delete(ctx, obj); err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	return nil
}

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

package globaldirectory

import (
	"context"
	"fmt"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/log"

	apiconfig "github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	infrav1alpha1 "github.com/sandbox0-ai/sandbox0/infra-operator/api/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/infra-operator/internal/controller/pkg/common"
	"github.com/sandbox0-ai/sandbox0/infra-operator/internal/controller/services/database"
)

type Reconciler struct {
	Resources *common.ResourceManager
}

func NewReconciler(resources *common.ResourceManager) *Reconciler {
	return &Reconciler{Resources: resources}
}

// Reconcile reconciles the global-directory deployment.
func (r *Reconciler) Reconcile(ctx context.Context, infra *infrav1alpha1.Sandbox0Infra, imageRepo, imageTag string) error {
	logger := log.FromContext(ctx)

	if !infrav1alpha1.IsGlobalDirectoryEnabled(infra) {
		logger.Info("Global directory is disabled, skipping")
		return nil
	}

	deploymentName := fmt.Sprintf("%s-global-directory", infra.Name)
	serviceName := deploymentName
	replicas := infra.Spec.Services.GlobalDirectory.Replicas
	labels := common.GetServiceLabels(infra.Name, "global-directory")

	config, err := r.buildConfig(ctx, infra)
	if err != nil {
		return err
	}
	needEnterpriseLicense := apiconfig.HasEnabledOIDCProviders(config.OIDCProviders)
	if err := common.EnsureEnterpriseLicense(ctx, r.Resources, infra, &config.LicenseFile, needEnterpriseLicense, "global-directory enterprise SSO"); err != nil {
		return err
	}
	if err := r.Resources.ReconcileServiceConfigMap(ctx, infra, deploymentName, labels, config); err != nil {
		return err
	}

	var resources *corev1.ResourceRequirements
	serviceConfig := (*infrav1alpha1.ServiceNetworkConfig)(nil)
	ingressConfig := (*infrav1alpha1.IngressConfig)(nil)
	if infra.Spec.Services != nil && infra.Spec.Services.GlobalDirectory != nil {
		resources = infra.Spec.Services.GlobalDirectory.Resources
		serviceConfig = infra.Spec.Services.GlobalDirectory.Service
		ingressConfig = infra.Spec.Services.GlobalDirectory.Ingress
	}

	volumeMounts := []corev1.VolumeMount{
		{
			Name:      "config",
			MountPath: "/config/config.yaml",
			SubPath:   "config.yaml",
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
	}
	if needEnterpriseLicense {
		volumeMounts, volumes = common.AppendEnterpriseLicenseVolume(infra.Name, config.LicenseFile, volumeMounts, volumes)
	}

	httpPort := int32(config.HTTPPort)
	if err := r.Resources.ReconcileDeployment(ctx, infra, deploymentName, labels, replicas, common.ServiceDefinition{
		Name:       "global-directory",
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
				Value: "global-directory",
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

	serviceType := common.ResolveServiceType(serviceConfig)
	servicePort := common.ResolveServicePort(serviceConfig, httpPort)
	if err := r.Resources.ReconcileService(ctx, infra, serviceName, labels, serviceType, servicePort, httpPort); err != nil {
		return err
	}

	if ingressConfig != nil && ingressConfig.Enabled {
		if err := r.Resources.ReconcileIngress(ctx, infra, serviceName, servicePort, ingressConfig); err != nil {
			return err
		}
	} else {
		if err := r.deleteIngressIfExists(ctx, infra, serviceName); err != nil {
			return err
		}
	}

	if err := r.Resources.EnsureDeploymentReady(ctx, infra, deploymentName, replicas); err != nil {
		return err
	}

	updateEndpoints(infra, serviceName, servicePort)
	logger.Info("Global directory reconciled successfully")
	return nil
}

func (r *Reconciler) buildConfig(ctx context.Context, infra *infrav1alpha1.Sandbox0Infra) (*apiconfig.GlobalDirectoryConfig, error) {
	cfg := &apiconfig.GlobalDirectoryConfig{}
	if infra.Spec.Services != nil && infra.Spec.Services.GlobalDirectory != nil && infra.Spec.Services.GlobalDirectory.Config != nil {
		cfg = infra.Spec.Services.GlobalDirectory.Config
	}
	applyConfigDefaults(cfg)

	dsn, err := database.GetDatabaseDSN(ctx, r.Resources.Client, infra)
	if err != nil {
		return nil, err
	}
	cfg.DatabaseURL = dsn

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
		cfg.JWTIssuer = "global-directory"
	}

	if strings.TrimSpace(cfg.JWTSecret) == "" {
		jwtSecret, err := common.EnsureSecretValue(
			ctx,
			r.Resources.Client,
			r.Resources.Scheme,
			infra,
			fmt.Sprintf("%s-global-directory-jwt", infra.Name),
			"jwt_secret",
			32,
		)
		if err != nil {
			return nil, err
		}
		cfg.JWTSecret = jwtSecret
	}

	return cfg, nil
}

func applyConfigDefaults(cfg *apiconfig.GlobalDirectoryConfig) {
	if cfg.HTTPPort == 0 {
		cfg.HTTPPort = 8080
	}
	if strings.TrimSpace(cfg.LogLevel) == "" {
		cfg.LogLevel = "info"
	}
	if cfg.DatabaseMaxConns == 0 {
		cfg.DatabaseMaxConns = 30
	}
	if cfg.DatabaseMinConns == 0 {
		cfg.DatabaseMinConns = 8
	}
	if strings.TrimSpace(cfg.DatabaseSchema) == "" {
		cfg.DatabaseSchema = "gd"
	}
	if cfg.RegionTokenTTL.Duration == 0 {
		cfg.RegionTokenTTL = metav1.Duration{Duration: 5 * time.Minute}
	}
	if cfg.ShutdownTimeout.Duration == 0 {
		cfg.ShutdownTimeout = metav1.Duration{Duration: 30 * time.Second}
	}
	if cfg.ServerReadTimeout.Duration == 0 {
		cfg.ServerReadTimeout = metav1.Duration{Duration: 30 * time.Second}
	}
	if cfg.ServerWriteTimeout.Duration == 0 {
		cfg.ServerWriteTimeout = metav1.Duration{Duration: 60 * time.Second}
	}
	if cfg.ServerIdleTimeout.Duration == 0 {
		cfg.ServerIdleTimeout = metav1.Duration{Duration: 120 * time.Second}
	}

	if cfg.JWTAccessTokenTTL.Duration == 0 {
		cfg.JWTAccessTokenTTL = metav1.Duration{Duration: 15 * time.Minute}
	}
	if cfg.JWTRefreshTokenTTL.Duration == 0 {
		cfg.JWTRefreshTokenTTL = metav1.Duration{Duration: 168 * time.Hour}
	}
	if cfg.RateLimitRPS == 0 {
		cfg.RateLimitRPS = 100
	}
	if cfg.RateLimitBurst == 0 {
		cfg.RateLimitBurst = 200
	}
	if cfg.RateLimitCleanupInterval.Duration == 0 {
		cfg.RateLimitCleanupInterval = metav1.Duration{Duration: 10 * time.Minute}
	}
	if strings.TrimSpace(cfg.DefaultTeamName) == "" {
		cfg.DefaultTeamName = "Personal Team"
	}
	if cfg.OIDCStateTTL.Duration == 0 {
		cfg.OIDCStateTTL = metav1.Duration{Duration: 10 * time.Minute}
	}
	if cfg.OIDCStateCleanupInterval.Duration == 0 {
		cfg.OIDCStateCleanupInterval = metav1.Duration{Duration: 5 * time.Minute}
	}
	if strings.TrimSpace(cfg.BaseURL) == "" {
		cfg.BaseURL = "http://localhost:8080"
	}
	if strings.TrimSpace(cfg.PublicRootDomain) == "" {
		cfg.PublicRootDomain = "sandbox0.app"
	}
	if strings.TrimSpace(cfg.PublicRegionID) == "" {
		cfg.PublicRegionID = "aws-us-east-1"
	}
}

func (r *Reconciler) deleteIngressIfExists(ctx context.Context, infra *infrav1alpha1.Sandbox0Infra, name string) error {
	ingress := &networkingv1.Ingress{}
	err := r.Resources.Client.Get(ctx, types.NamespacedName{Name: name, Namespace: infra.Namespace}, ingress)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return err
	}
	return r.Resources.Client.Delete(ctx, ingress)
}

func updateEndpoints(infra *infrav1alpha1.Sandbox0Infra, serviceName string, servicePort int32) {
	if infra.Status.Endpoints == nil {
		infra.Status.Endpoints = &infrav1alpha1.EndpointsStatus{}
	}
	infra.Status.Endpoints.GlobalDirectory = fmt.Sprintf("http://%s:%d", serviceName, servicePort)
}

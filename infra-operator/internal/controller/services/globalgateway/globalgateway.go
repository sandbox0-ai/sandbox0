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

package globalgateway

import (
	"context"
	"errors"
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
	infraplan "github.com/sandbox0-ai/sandbox0/infra-operator/internal/plan"
	"github.com/sandbox0-ai/sandbox0/infra-operator/internal/runtimeconfig"
	"github.com/sandbox0-ai/sandbox0/pkg/dbpool"
	gatewaymigrations "github.com/sandbox0-ai/sandbox0/pkg/gateway/migrations"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/tenantdir"
	"github.com/sandbox0-ai/sandbox0/pkg/migrate"
)

type Reconciler struct {
	Resources *common.ResourceManager
}

func NewReconciler(resources *common.ResourceManager) *Reconciler {
	return &Reconciler{Resources: resources}
}

// Reconcile reconciles the global-gateway deployment.
func (r *Reconciler) Reconcile(ctx context.Context, infra *infrav1alpha1.Sandbox0Infra, imageRepo, imageTag string, compiledPlan *infraplan.InfraPlan) error {
	logger := log.FromContext(ctx)
	if compiledPlan == nil {
		compiledPlan = infraplan.Compile(infra)
	}

	if !infrav1alpha1.IsGlobalGatewayEnabled(infra) {
		logger.Info("Global gateway is disabled, skipping")
		return nil
	}

	deploymentName := fmt.Sprintf("%s-global-gateway", infra.Name)
	serviceName := deploymentName
	replicas := infra.Spec.Services.GlobalGateway.Replicas
	labels := common.GetServiceLabels(infra.Name, "global-gateway")

	config, err := r.buildConfig(ctx, infra)
	if err != nil {
		return err
	}
	needEnterpriseLicense := compiledPlan.Enterprise.GlobalGateway
	common.NormalizeEnterpriseLicenseFile(&config.LicenseFile, needEnterpriseLicense)
	if err := ensureGlobalGatewayBootstrapState(ctx, infra, config); err != nil {
		return err
	}
	podAnnotations, err := common.ConfigHashAnnotation(config)
	if err != nil {
		return err
	}
	if err := r.Resources.ReconcileServiceConfigMap(ctx, infra, deploymentName, labels, config); err != nil {
		return err
	}

	var resources *corev1.ResourceRequirements
	serviceConfig := (*infrav1alpha1.ServiceNetworkConfig)(nil)
	ingressConfig := (*infrav1alpha1.IngressConfig)(nil)
	if infra.Spec.Services != nil && infra.Spec.Services.GlobalGateway != nil {
		resources = infra.Spec.Services.GlobalGateway.Resources
		serviceConfig = infra.Spec.Services.GlobalGateway.Service
		ingressConfig = infra.Spec.Services.GlobalGateway.Ingress
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
		volumeMounts, volumes = common.AppendEnterpriseLicenseVolume(infra, config.LicenseFile, volumeMounts, volumes)
	}
	envVars := []corev1.EnvVar{
		{
			Name:  "SERVICE",
			Value: "global-gateway",
		},
		{
			Name:  "CONFIG_PATH",
			Value: "/config/config.yaml",
		},
	}
	envVars = append(envVars, compiledPlan.ObservabilityEnvVars()...)

	httpPort := int32(config.HTTPPort)
	if err := r.Resources.ReconcileDeployment(ctx, infra, deploymentName, labels, replicas, common.ServiceDefinition{
		Name:       "global-gateway",
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
	serviceAnnotations := common.ResolveServiceAnnotations(serviceConfig)
	if err := r.Resources.ReconcileService(ctx, infra, serviceName, labels, serviceType, serviceAnnotations, servicePort, httpPort); err != nil {
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

	logger.Info("Global gateway reconciled successfully")
	return nil
}

func (r *Reconciler) buildConfig(ctx context.Context, infra *infrav1alpha1.Sandbox0Infra) (*apiconfig.GlobalGatewayConfig, error) {
	cfg := &apiconfig.GlobalGatewayConfig{}
	if infra.Spec.Services != nil && infra.Spec.Services.GlobalGateway != nil {
		cfg = runtimeconfig.ToGlobalGateway(infra.Spec.Services.GlobalGateway.Config)
	}
	applyConfigDefaults(cfg)
	cfg.RegionID = resolveGlobalRegionID(infra, cfg.RegionID)
	if infra.Spec.PublicExposure != nil {
		cfg.PublicExposureEnabled = infra.Spec.PublicExposure.Enabled
		if strings.TrimSpace(infra.Spec.PublicExposure.RootDomain) != "" {
			cfg.PublicRootDomain = infra.Spec.PublicExposure.RootDomain
		}
		if strings.TrimSpace(infra.Spec.PublicExposure.RegionID) != "" {
			cfg.PublicRegionID = infra.Spec.PublicExposure.RegionID
		}
	}

	dsn, err := database.GetDatabaseDSN(ctx, r.Resources.Client, infra)
	if err != nil {
		return nil, err
	}
	cfg.DatabaseURL = dsn

	if infra.Spec.InitUser != nil {
		password := ""
		if cfg.BuiltInAuth.Enabled || !apiconfig.HasEnabledOIDCProviders(cfg.OIDCProviders) {
			secretRef := common.ResolveSecretKeyRef(infra.Spec.InitUser.PasswordSecret, "admin-password", "password")
			password, err = common.GetSecretValue(ctx, r.Resources.Client, infra.Namespace, secretRef)
			if err != nil {
				return nil, err
			}
		}
		homeRegionID := strings.TrimSpace(infra.Spec.InitUser.HomeRegionID)
		if homeRegionID == "" {
			homeRegionID = cfg.RegionID
		}

		cfg.BuiltInAuth.InitUser = &apiconfig.InitUserConfig{
			Email:        infra.Spec.InitUser.Email,
			Password:     password,
			Name:         infra.Spec.InitUser.Name,
			HomeRegionID: homeRegionID,
		}
	}

	if strings.TrimSpace(cfg.JWTIssuer) == "" {
		cfg.JWTIssuer = "global-gateway"
	}

	if usesFederatedGlobalUserAuth(infra) {
		privateKeyPEM, publicKeyPEM, err := common.EnsureEd25519KeyPair(
			ctx,
			r.Resources.Client,
			r.Resources.Scheme,
			infra,
			sharedUserJWTSecretName(infra),
			"jwt_private_key_pem",
			"jwt_public_key_pem",
		)
		if err != nil {
			return nil, err
		}
		cfg.JWTSecret = ""
		cfg.JWTPrivateKeyPEM = privateKeyPEM
		cfg.JWTPublicKeyPEM = publicKeyPEM
		return cfg, nil
	}

	if strings.TrimSpace(cfg.JWTSecret) == "" {
		jwtSecret, err := common.EnsureSecretValue(
			ctx,
			r.Resources.Client,
			r.Resources.Scheme,
			infra,
			fmt.Sprintf("%s-global-gateway-jwt", infra.Name),
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

func usesFederatedGlobalUserAuth(infra *infrav1alpha1.Sandbox0Infra) bool {
	if infra == nil || infra.Spec.Services == nil || infra.Spec.Services.RegionalGateway == nil {
		return false
	}
	mode := ""
	if infra.Spec.Services.RegionalGateway.Config != nil {
		mode = infra.Spec.Services.RegionalGateway.Config.AuthMode
	}
	return strings.EqualFold(strings.TrimSpace(mode), "federated_global")
}

func sharedUserJWTSecretName(infra *infrav1alpha1.Sandbox0Infra) string {
	return fmt.Sprintf("%s-user-jwt", infra.Name)
}

func resolveGlobalRegionID(infra *infrav1alpha1.Sandbox0Infra, current string) string {
	if trimmed := strings.TrimSpace(current); trimmed != "" {
		return trimmed
	}
	if trimmed := strings.TrimSpace(infra.Spec.Region); trimmed != "" {
		return trimmed
	}
	if infra.Spec.PublicExposure == nil {
		return ""
	}
	return strings.TrimSpace(infra.Spec.PublicExposure.RegionID)
}

func desiredBootstrapRegion(infra *infrav1alpha1.Sandbox0Infra, regionID string) *tenantdir.Region {
	if strings.TrimSpace(regionID) == "" || infra.Spec.Services == nil || infra.Spec.Services.RegionalGateway == nil || !infra.Spec.Services.RegionalGateway.Enabled {
		return nil
	}

	regionalHTTPPort := int32(8080)
	if infra.Spec.Services.RegionalGateway.Config != nil && infra.Spec.Services.RegionalGateway.Config.HTTPPort != 0 {
		regionalHTTPPort = int32(infra.Spec.Services.RegionalGateway.Config.HTTPPort)
	}

	servicePort := common.ResolveServicePort(infra.Spec.Services.RegionalGateway.Service, regionalHTTPPort)
	serviceName := fmt.Sprintf("%s-regional-gateway", infra.Name)
	return &tenantdir.Region{
		ID:                 regionID,
		DisplayName:        regionID,
		RegionalGatewayURL: fmt.Sprintf("http://%s:%d", serviceName, servicePort),
		Enabled:            true,
	}
}

func ensureGlobalGatewayBootstrapState(ctx context.Context, infra *infrav1alpha1.Sandbox0Infra, cfg *apiconfig.GlobalGatewayConfig) error {
	if cfg == nil || strings.TrimSpace(cfg.DatabaseURL) == "" {
		return nil
	}

	bootstrapRegion := desiredBootstrapRegion(infra, cfg.RegionID)
	if bootstrapRegion == nil {
		return nil
	}

	pool, err := dbpool.New(ctx, dbpool.Options{
		DatabaseURL: cfg.DatabaseURL,
		MaxConns:    int32(cfg.DatabaseMaxConns),
		MinConns:    int32(cfg.DatabaseMinConns),
		Schema:      cfg.DatabaseSchema,
	})
	if err != nil {
		return fmt.Errorf("connect global-gateway database: %w", err)
	}
	defer pool.Close()

	if err := migrate.Up(ctx, pool, ".",
		migrate.WithBaseFS(gatewaymigrations.FS),
		migrate.WithSchema(cfg.DatabaseSchema),
	); err != nil {
		return fmt.Errorf("run global-gateway migrations: %w", err)
	}

	repo := tenantdir.NewRepository(pool)
	existing, err := repo.GetRegion(ctx, bootstrapRegion.ID)
	if err != nil {
		if errors.Is(err, tenantdir.ErrRegionNotFound) {
			if err := repo.CreateRegion(ctx, bootstrapRegion); err != nil {
				return fmt.Errorf("create bootstrap region: %w", err)
			}
			return nil
		}
		return fmt.Errorf("get bootstrap region: %w", err)
	}

	if existing.DisplayName == bootstrapRegion.DisplayName &&
		existing.RegionalGatewayURL == bootstrapRegion.RegionalGatewayURL &&
		existing.MeteringExportURL == bootstrapRegion.MeteringExportURL &&
		existing.Enabled == bootstrapRegion.Enabled {
		return nil
	}

	if err := repo.UpdateRegion(ctx, bootstrapRegion); err != nil {
		return fmt.Errorf("update bootstrap region: %w", err)
	}
	return nil
}

func applyConfigDefaults(cfg *apiconfig.GlobalGatewayConfig) {
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
		cfg.DatabaseSchema = "global_gateway"
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

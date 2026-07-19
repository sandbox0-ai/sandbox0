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
	"math"
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/log"

	apiconfig "github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	infrav1alpha1 "github.com/sandbox0-ai/sandbox0/infra-operator/api/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/infra-operator/internal/controller/pkg/common"
	meteringsvc "github.com/sandbox0-ai/sandbox0/infra-operator/internal/controller/services/metering"
	redissvc "github.com/sandbox0-ai/sandbox0/infra-operator/internal/controller/services/redis"
	sandboxobssvc "github.com/sandbox0-ai/sandbox0/infra-operator/internal/controller/services/sandboxobservability"
	infraplan "github.com/sandbox0-ai/sandbox0/infra-operator/internal/plan"
	"github.com/sandbox0-ai/sandbox0/infra-operator/internal/runtimeconfig"
	pkginternalauth "github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"github.com/sandbox0-ai/sandbox0/pkg/rediscache"
)

type Reconciler struct {
	Resources *common.ResourceManager
}

const (
	defaultClusterGatewayHTTPPort      = 8443
	defaultAuditResultSpoolPVCSize     = "3Gi"
	auditResultSpoolVolumeName         = "audit-result-spool"
	auditResultSpoolContainerMountPath = "/var/lib/sandbox0/cluster-gateway"
)

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
	auditKeySecretName, _, auditPublicKey := compiledPlan.AuditNetdKeyRefs()
	auditSigningSecretName, auditSigningPrivateKey, auditSigningPublicKey := compiledPlan.AuditSigningKeyRefs()

	config, err := r.buildConfig(ctx, compiledPlan)
	if err != nil {
		return err
	}
	if config.SandboxObservability.AuditEnabled {
		if replicas != 1 {
			return fmt.Errorf("sandbox audit requires exactly one cluster-gateway replica for exclusive durable delivery replay")
		}
		if err := r.reconcileAuditResultSpoolPVC(ctx, scope); err != nil {
			return fmt.Errorf("reconcile sandbox audit result delivery PVC: %w", err)
		}
	}
	needEnterpriseLicense := compiledPlan.Enterprise.ClusterGateway
	common.NormalizeEnterpriseLicenseFile(&config.LicenseFile, needEnterpriseLicense)
	configRef, err := r.Resources.ReconcileHashedServiceConfigMapWithScope(ctx, scope, deploymentName, labels, config)
	if err != nil {
		return err
	}
	podAnnotations := configRef.PodAnnotations()

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
					LocalObjectReference: corev1.LocalObjectReference{Name: configRef.ConfigMapName},
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
	if config.SandboxObservability.AuditEnabled {
		volumeMounts = append(volumeMounts,
			corev1.VolumeMount{
				Name:      "audit-jwt-public-key",
				MountPath: pkginternalauth.DefaultAuditJWTPublicKeyPath,
				SubPath:   "audit_jwt_public.key",
				ReadOnly:  true,
			},
			corev1.VolumeMount{
				Name:      "audit-signing-private-key",
				MountPath: pkginternalauth.DefaultAuditSigningPrivateKeyPath,
				SubPath:   "audit_signing_private.key",
				ReadOnly:  true,
			},
			corev1.VolumeMount{
				Name:      "audit-signing-public-key",
				MountPath: pkginternalauth.DefaultAuditSigningPublicKeyPath,
				SubPath:   "audit_signing_public.key",
				ReadOnly:  true,
			},
		)
		volumes = append(volumes,
			corev1.Volume{
				Name: auditResultSpoolVolumeName,
				VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
					ClaimName: auditResultSpoolPVCName(scope.Name),
				}},
			},
			corev1.Volume{
				Name: "audit-jwt-public-key",
				VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{
					SecretName: auditKeySecretName,
					Items:      []corev1.KeyToPath{{Key: auditPublicKey, Path: "audit_jwt_public.key"}},
				}},
			},
			corev1.Volume{
				Name: "audit-signing-private-key",
				VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{
					SecretName: auditSigningSecretName,
					Items:      []corev1.KeyToPath{{Key: auditSigningPrivateKey, Path: "audit_signing_private.key"}},
				}},
			},
			corev1.Volume{
				Name: "audit-signing-public-key",
				VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{
					SecretName: auditSigningSecretName,
					Items:      []corev1.KeyToPath{{Key: auditSigningPublicKey, Path: "audit_signing_public.key"}},
				}},
			},
		)
		volumeMounts = append(volumeMounts, corev1.VolumeMount{
			Name:      auditResultSpoolVolumeName,
			MountPath: auditResultSpoolContainerMountPath,
		})
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

	var strategy *appsv1.DeploymentStrategy
	if config.SandboxObservability.AuditEnabled {
		// A single-writer RWO PVC must be detached before the replacement pod can
		// mount it on another node. Recreate preserves replay continuity.
		strategy = &appsv1.DeploymentStrategy{Type: appsv1.RecreateDeploymentStrategyType}
	}
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
		Image: fmt.Sprintf("%s:%s", imageRepo, imageTag),
		EnvVars: common.AppendObservabilityEnvVars([]corev1.EnvVar{
			{
				Name:  "SERVICE",
				Value: "cluster-gateway",
			},
			{
				Name:  "CONFIG_PATH",
				Value: "/config/config.yaml",
			},
		}, scope.Owner(), common.ObservabilityEnvConfig{
			ServiceName: "cluster-gateway",
			RegionID:    common.ResolveRegionID(scope.Owner()),
			ClusterID:   common.ResolveClusterID(scope.Owner()),
		}),
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
		Resources:          resources,
		DeploymentStrategy: strategy,
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

func auditResultSpoolPVCName(infraName string) string {
	return fmt.Sprintf("%s-cluster-gateway-audit-spool", infraName)
}

func (r *Reconciler) reconcileAuditResultSpoolPVC(ctx context.Context, scope common.ObjectScope) error {
	size := resource.MustParse(defaultAuditResultSpoolPVCSize)
	storageClass := ""
	var audit *infrav1alpha1.SandboxObservabilityAuditConfig
	if owner := scope.Owner(); owner != nil && owner.Spec.SandboxObservability != nil {
		audit = owner.Spec.SandboxObservability.Audit
	}
	if audit != nil && audit.DeliveryPersistence != nil {
		if !audit.DeliveryPersistence.Size.IsZero() {
			size = audit.DeliveryPersistence.Size
		}
		storageClass = strings.TrimSpace(audit.DeliveryPersistence.StorageClass)
	}
	requiredBytes, err := auditSpoolBacklogAndHeadroomBytes(audit)
	if err != nil {
		return err
	}
	if size.Value() <= requiredBytes {
		return fmt.Errorf(
			"audit delivery persistence size %s must exceed configured backlog plus free-space floor %s",
			size.String(),
			resource.NewQuantity(requiredBytes, resource.BinarySI).String(),
		)
	}

	name := auditResultSpoolPVCName(scope.Name)
	current := &corev1.PersistentVolumeClaim{}
	err = r.Resources.Client.Get(ctx, types.NamespacedName{Name: name, Namespace: scope.Namespace}, current)
	if err == nil {
		return nil
	}
	if !apierrors.IsNotFound(err) {
		return err
	}

	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: scope.Namespace},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceStorage: size},
			},
		},
	}
	if storageClass != "" {
		pvc.Spec.StorageClassName = &storageClass
	}
	if err := scope.SetControllerReference(pvc, r.Resources.Scheme); err != nil {
		return err
	}
	if err := r.Resources.Client.Create(ctx, pvc); err != nil && !apierrors.IsAlreadyExists(err) {
		return err
	}
	return nil
}

func auditSpoolBacklogAndHeadroomBytes(audit *infrav1alpha1.SandboxObservabilityAuditConfig) (int64, error) {
	maxBytes := apiconfig.DefaultAuditSpoolMaxBytes
	minFreeBytes := apiconfig.DefaultAuditSpoolMinFreeBytes
	if audit != nil {
		if audit.SpoolLimits.MaxBytes != 0 {
			maxBytes = audit.SpoolLimits.MaxBytes
		}
		if audit.SpoolLimits.MinFreeBytes != 0 {
			minFreeBytes = audit.SpoolLimits.MinFreeBytes
		}
	}
	if maxBytes <= 0 || minFreeBytes < 0 {
		return 0, fmt.Errorf("audit spool byte limits are invalid")
	}
	if maxBytes > math.MaxInt64-minFreeBytes {
		return 0, fmt.Errorf("audit spool backlog plus free-space floor overflows int64")
	}
	return maxBytes + minFreeBytes, nil
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
	if owner := compiledPlan.Scope.Owner(); owner != nil {
		cfg.ClusterID = common.ResolveClusterID(owner)
		if err := meteringsvc.ApplyClusterGatewayConfig(ctx, r.Resources.Client, owner, cfg); err != nil {
			return nil, fmt.Errorf("apply metering config: %w", err)
		}
		if err := sandboxobssvc.ApplyClusterGatewayConfig(ctx, r.Resources.Client, owner, cfg); err != nil {
			return nil, err
		}
	}
	if cfg.HTTPPort == 0 {
		cfg.HTTPPort = defaultClusterGatewayHTTPPort
	}
	cfg.AuthMode = deriveClusterGatewayAuthMode(cfg.AuthMode, compiledPlan)
	owner := compiledPlan.Scope.Owner()
	teamQuota := runtimeconfig.ToTeamQuota(compiledPlan.EffectiveTeamQuotaConfig())
	runtimeconfig.SetTeamQuotaOwnerVersion(&teamQuota, owner)
	teamQuota.PolicyOwner = !compiledPlan.RegionalGateway.Enabled &&
		owner != nil &&
		owner.Spec.ControlPlane == nil &&
		clusterGatewayPublicAuthEnabled(cfg.AuthMode)
	if !teamQuota.PolicyOwner {
		teamQuota.Defaults = nil
		teamQuota.DefaultsOwnerEpoch = ""
		teamQuota.DefaultsGeneration = 0
	}
	cfg.TeamQuota = teamQuota
	resolvedRegionID := strings.TrimSpace(cfg.RegionID)

	if dsn, err := compiledPlan.DatabaseDSN(ctx, r.Resources.Client); err == nil {
		cfg.DatabaseURL = dsn
	}
	if err := redissvc.ApplyGatewayRedisConfig(ctx, r.Resources.Client, compiledPlan.Scope.Owner(), &cfg.GatewayConfig); err != nil {
		return nil, err
	}
	if err := redissvc.ApplyOverloadGuardConfig(
		ctx,
		r.Resources.Client,
		compiledPlan.Scope.Owner(),
		rediscache.JoinKeyPrefix(
			"cluster-gateway",
			common.ResolveRegionID(compiledPlan.Scope.Owner()),
		),
		&cfg.OverloadGuard,
	); err != nil {
		return nil, err
	}
	if err := redissvc.ApplyTeamQuotaDistributedEnforcementConfig(ctx, r.Resources.Client, compiledPlan.Scope.Owner(), &cfg.TeamQuota.DistributedEnforcement); err != nil {
		return nil, err
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

	if compiledPlan.Components.EnableStorageRuntime {
		cfg.ManagerStorageURL = compiledPlan.Services.ManagerStorage.URL
	} else {
		cfg.ManagerStorageURL = ""
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

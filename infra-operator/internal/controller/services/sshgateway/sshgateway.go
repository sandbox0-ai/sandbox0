package sshgateway

import (
	"context"
	"fmt"

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

const (
	defaultSSHPort             = 2222
	defaultDatabaseMaxConns    = 30
	defaultDatabaseMinConns    = 8
	sshHostPrivateKeyKey       = "ssh_host_ed25519_key"
	sshHostPublicKeyKey        = "ssh_host_ed25519_key.pub"
	defaultSSHHostKeyMountPath = "/secrets/ssh_host_ed25519_key"
	controlPlaneKeyMountPath   = "/secrets/control_plane_internal_jwt_private.key"
	dataPlaneKeyMountPath      = "/secrets/data_plane_internal_jwt_private.key"
)

type Reconciler struct {
	Resources *common.ResourceManager
}

func NewReconciler(resources *common.ResourceManager) *Reconciler {
	return &Reconciler{Resources: resources}
}

// Reconcile reconciles the ssh-gateway deployment.
func (r *Reconciler) Reconcile(ctx context.Context, infra *infrav1alpha1.Sandbox0Infra, imageRepo, imageTag string, compiledPlan *infraplan.InfraPlan) error {
	logger := log.FromContext(ctx)
	if compiledPlan == nil {
		compiledPlan = infraplan.Compile(infra)
	}
	if infra.Spec.Services == nil || infra.Spec.Services.SSHGateway == nil || !infra.Spec.Services.SSHGateway.Enabled {
		logger.Info("SSH gateway is disabled, skipping")
		return nil
	}

	deploymentName := fmt.Sprintf("%s-ssh-gateway", infra.Name)
	labels := common.GetServiceLabels(infra.Name, "ssh-gateway")
	replicas := infra.Spec.Services.SSHGateway.Replicas
	controlPlaneKeySecretName, controlPlanePrivateKeyKey, _ := internalauth.GetControlPlaneKeyRefs(infra)
	dataPlaneKeySecretName, dataPlanePrivateKeyKey, _ := internalauth.GetDataPlaneKeyRefs(infra)
	hostKeySecretName := fmt.Sprintf("%s-ssh-gateway-host-key", infra.Name)

	config, err := r.buildConfig(ctx, infra, compiledPlan)
	if err != nil {
		return err
	}
	if _, _, err := common.EnsureEd25519KeyPair(ctx, r.Resources.Client, r.Resources.Scheme, infra, hostKeySecretName, sshHostPrivateKeyKey, sshHostPublicKeyKey); err != nil {
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
	if infra.Spec.Services != nil && infra.Spec.Services.SSHGateway != nil {
		resources = infra.Spec.Services.SSHGateway.Resources
		serviceConfig = infra.Spec.Services.SSHGateway.Service
	}

	volumeMounts := []corev1.VolumeMount{
		{
			Name:      "config",
			MountPath: "/config/config.yaml",
			SubPath:   "config.yaml",
			ReadOnly:  true,
		},
		{
			Name:      "control-plane-internal-jwt-private-key",
			MountPath: config.ControlPlanePrivateKeyPath,
			SubPath:   controlPlanePrivateKeyKey,
			ReadOnly:  true,
		},
		{
			Name:      "data-plane-internal-jwt-private-key",
			MountPath: config.DataPlanePrivateKeyPath,
			SubPath:   dataPlanePrivateKeyKey,
			ReadOnly:  true,
		},
		{
			Name:      "ssh-host-key",
			MountPath: config.SSHHostKeyPath,
			SubPath:   sshHostPrivateKeyKey,
			ReadOnly:  true,
		},
	}
	volumes := []corev1.Volume{
		{
			Name: "config",
			VolumeSource: corev1.VolumeSource{
				ConfigMap: &corev1.ConfigMapVolumeSource{LocalObjectReference: corev1.LocalObjectReference{Name: deploymentName}},
			},
		},
		{
			Name: "control-plane-internal-jwt-private-key",
			VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{
					SecretName: controlPlaneKeySecretName,
					Items:      []corev1.KeyToPath{{Key: controlPlanePrivateKeyKey, Path: controlPlanePrivateKeyKey}},
				},
			},
		},
		{
			Name: "data-plane-internal-jwt-private-key",
			VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{
					SecretName: dataPlaneKeySecretName,
					Items:      []corev1.KeyToPath{{Key: dataPlanePrivateKeyKey, Path: dataPlanePrivateKeyKey}},
				},
			},
		},
		{
			Name: "ssh-host-key",
			VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{
					SecretName: hostKeySecretName,
					Items:      []corev1.KeyToPath{{Key: sshHostPrivateKeyKey, Path: sshHostPrivateKeyKey}},
				},
			},
		},
	}

	sshPort := int32(config.SSHPort)
	if err := r.Resources.ReconcileDeployment(ctx, infra, deploymentName, labels, replicas, common.ServiceDefinition{
		Name:       "ssh-gateway",
		Port:       sshPort,
		TargetPort: sshPort,
		Ports: []corev1.ContainerPort{{
			Name:          "ssh",
			ContainerPort: sshPort,
		}},
		Image: fmt.Sprintf("%s:%s", imageRepo, imageTag),
		EnvVars: []corev1.EnvVar{
			{Name: "SERVICE", Value: "ssh-gateway"},
			{Name: "CONFIG_PATH", Value: "/config/config.yaml"},
		},
		VolumeMounts:   volumeMounts,
		Volumes:        volumes,
		PodAnnotations: podAnnotations,
		LivenessProbe:  &corev1.Probe{ProbeHandler: corev1.ProbeHandler{TCPSocket: &corev1.TCPSocketAction{Port: intstr.FromInt(config.SSHPort)}}, InitialDelaySeconds: 10, PeriodSeconds: 10},
		ReadinessProbe: &corev1.Probe{ProbeHandler: corev1.ProbeHandler{TCPSocket: &corev1.TCPSocketAction{Port: intstr.FromInt(config.SSHPort)}}, InitialDelaySeconds: 5, PeriodSeconds: 5},
		Resources:      resources,
	}); err != nil {
		return err
	}

	serviceType := common.ResolveServiceType(serviceConfig)
	servicePort := common.ResolveServicePort(serviceConfig, sshPort)
	serviceAnnotations := common.ResolveServiceAnnotations(serviceConfig)
	if err := r.Resources.ReconcileServicePorts(ctx, infra, deploymentName, labels, serviceType, serviceAnnotations, []corev1.ServicePort{common.BuildServicePort("ssh", servicePort, sshPort, serviceType)}); err != nil {
		return err
	}
	if err := r.Resources.EnsureDeploymentReady(ctx, infra, deploymentName, replicas); err != nil {
		return err
	}

	logger.Info("SSH gateway reconciled successfully")
	return nil
}

func (r *Reconciler) buildConfig(ctx context.Context, infra *infrav1alpha1.Sandbox0Infra, compiledPlan *infraplan.InfraPlan) (*apiconfig.SSHGatewayConfig, error) {
	cfg := &apiconfig.SSHGatewayConfig{}
	if infra.Spec.Services != nil && infra.Spec.Services.SSHGateway != nil {
		cfg = runtimeconfig.ToSSHGateway(infra.Spec.Services.SSHGateway.Config)
	}
	if cfg.SSHPort == 0 {
		cfg.SSHPort = defaultSSHPort
	}
	if cfg.LogLevel == "" {
		cfg.LogLevel = "info"
	}
	if cfg.DatabaseMaxConns == 0 {
		cfg.DatabaseMaxConns = defaultDatabaseMaxConns
	}
	if cfg.DatabaseMinConns == 0 {
		cfg.DatabaseMinConns = defaultDatabaseMinConns
	}
	if cfg.InternalAuthCaller == "" {
		cfg.InternalAuthCaller = pkginternalauth.ServiceSSHGateway
	}
	if cfg.ControlPlanePrivateKeyPath == "" {
		cfg.ControlPlanePrivateKeyPath = controlPlaneKeyMountPath
	}
	if cfg.DataPlanePrivateKeyPath == "" {
		cfg.DataPlanePrivateKeyPath = dataPlaneKeyMountPath
	}
	if cfg.SSHHostKeyPath == "" {
		cfg.SSHHostKeyPath = defaultSSHHostKeyMountPath
	}

	if dsn, err := database.GetDatabaseDSN(ctx, r.Resources.Client, infra); err == nil {
		cfg.DatabaseURL = dsn
	}
	if compiledPlan != nil {
		cfg.RegionalGatewayURL = compiledPlan.Status.Endpoints.RegionalGatewayInternal
	}
	if cfg.RegionalGatewayURL == "" {
		return nil, fmt.Errorf("regional gateway URL is required for ssh-gateway")
	}
	if cfg.DatabaseURL == "" {
		return nil, fmt.Errorf("database URL is required for ssh-gateway")
	}
	return cfg, nil
}

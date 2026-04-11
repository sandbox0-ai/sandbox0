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

package scheduler

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/log"

	apiconfig "github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	"github.com/sandbox0-ai/sandbox0/infra-operator/internal/controller/pkg/common"
	infraplan "github.com/sandbox0-ai/sandbox0/infra-operator/internal/plan"
	"github.com/sandbox0-ai/sandbox0/pkg/dbpool"
	pkginternalauth "github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"github.com/sandbox0-ai/sandbox0/pkg/migrate"
	templmigrations "github.com/sandbox0-ai/sandbox0/pkg/template/migrations"
	schedulerdb "github.com/sandbox0-ai/sandbox0/scheduler/pkg/db"
)

type Reconciler struct {
	Resources *common.ResourceManager
}

func NewReconciler(resources *common.ResourceManager) *Reconciler {
	return &Reconciler{Resources: resources}
}

// Reconcile reconciles the scheduler deployment.
func (r *Reconciler) Reconcile(ctx context.Context, imageRepo, imageTag string, compiledPlan *infraplan.InfraPlan) error {
	logger := log.FromContext(ctx)
	if compiledPlan == nil {
		return fmt.Errorf("compiled plan is required")
	}

	// Skip if not enabled (scheduler is optional by default)
	if !compiledPlan.Scheduler.Enabled {
		logger.Info("Scheduler is disabled, skipping")
		return nil
	}

	scope := compiledPlan.Scope
	deploymentName := fmt.Sprintf("%s-scheduler", scope.Name)
	replicas := compiledPlan.Scheduler.Replicas
	labels := common.GetServiceLabels(scope.Name, "scheduler")
	keySecretName, privateKeyKey, publicKeyKey := compiledPlan.ControlPlaneKeyRefs()

	config, err := r.buildConfig(ctx, compiledPlan)
	if err != nil {
		return err
	}
	common.NormalizeEnterpriseLicenseFile(&config.LicenseFile, compiledPlan.Enterprise.Scheduler)
	if err := common.EnsureBuiltinTemplates(ctx, compiledPlan.BuiltinTemplates(), common.BuiltinTemplateOptions{
		DatabaseURL:          config.DatabaseURL,
		DatabaseMaxConns:     config.DatabasePool.MaxConns,
		DatabaseMinConns:     config.DatabasePool.MinConns,
		TemplateStoreEnabled: true,
		Owner:                "scheduler",
	}); err != nil {
		return err
	}
	if err := r.ensureHomeCluster(ctx, compiledPlan, config); err != nil {
		return err
	}
	podAnnotations, err := common.ConfigHashAnnotation(config)
	if err != nil {
		return err
	}
	if err := r.Resources.ReconcileServiceConfigMapWithScope(ctx, scope, deploymentName, labels, config); err != nil {
		return err
	}

	resources := compiledPlan.Scheduler.Resources
	serviceConfig := compiledPlan.Scheduler.ServiceConfig

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
	if compiledPlan.Enterprise.Scheduler {
		volumeMounts, volumes = common.AppendEnterpriseLicenseVolumeWithSecretRef(compiledPlan.EnterpriseLicenseSecretRef(), config.LicenseFile, volumeMounts, volumes)
	}

	// Create deployment
	httpPort := int32(config.HTTPPort)
	if err := r.Resources.ReconcileDeploymentWithScope(ctx, scope, deploymentName, labels, replicas, common.ServiceDefinition{
		Name:               "scheduler",
		Port:               httpPort,
		TargetPort:         httpPort,
		ServiceAccountName: fmt.Sprintf("%s-scheduler", scope.Name),
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
				Value: "scheduler",
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
	serviceAnnotations := common.ResolveServiceAnnotations(serviceConfig)
	if err := r.Resources.ReconcileServiceWithScope(ctx, scope, deploymentName, labels, serviceType, serviceAnnotations, servicePort, httpPort); err != nil {
		return err
	}

	if err := r.Resources.EnsureDeploymentReadyWithScope(ctx, scope, deploymentName, replicas); err != nil {
		return err
	}

	logger.Info("Scheduler reconciled successfully")
	return nil
}

func (r *Reconciler) buildConfig(ctx context.Context, compiledPlan *infraplan.InfraPlan) (*apiconfig.SchedulerConfig, error) {
	cfg := &apiconfig.SchedulerConfig{}
	if compiledPlan != nil && compiledPlan.Scheduler.Config != nil {
		cfg = compiledPlan.Scheduler.Config.DeepCopy()
	}
	if dsn, err := compiledPlan.DatabaseDSN(ctx, r.Resources.Client); err == nil {
		cfg.DatabaseURL = dsn
	}

	return cfg, nil
}

func (r *Reconciler) ensureHomeCluster(ctx context.Context, compiledPlan *infraplan.InfraPlan, cfg *apiconfig.SchedulerConfig) error {
	if compiledPlan == nil {
		return fmt.Errorf("compiled plan is required")
	}
	desired := compiledPlan.Scheduler.HomeCluster
	if desired == nil {
		return nil
	}
	if cfg == nil || cfg.DatabaseURL == "" {
		return fmt.Errorf("database_url is required to sync scheduler home cluster")
	}

	pool, err := dbpool.New(ctx, dbpool.Options{
		DatabaseURL:     cfg.DatabaseURL,
		MaxConns:        cfg.DatabasePool.MaxConns,
		MinConns:        cfg.DatabasePool.MinConns,
		DefaultMaxConns: 10,
		DefaultMinConns: 2,
		Schema:          "scheduler",
	})
	if err != nil {
		return fmt.Errorf("init scheduler cluster database: %w", err)
	}
	defer pool.Close()

	if err := migrate.Up(ctx, pool, ".",
		migrate.WithBaseFS(templmigrations.FS),
		migrate.WithSchema("scheduler"),
	); err != nil {
		return fmt.Errorf("migrate scheduler cluster store: %w", err)
	}

	repo := schedulerdb.NewRepository(pool)
	existing, err := repo.GetCluster(ctx, desired.ClusterID)
	if err != nil {
		return fmt.Errorf("get scheduler home cluster: %w", err)
	}
	if existing == nil {
		if err := repo.CreateCluster(ctx, desired); err != nil {
			return fmt.Errorf("create scheduler home cluster: %w", err)
		}
	} else {
		existing.ClusterName = desired.ClusterName
		existing.ClusterGatewayURL = desired.ClusterGatewayURL
		existing.Weight = desired.Weight
		existing.Enabled = desired.Enabled
		if err := repo.UpdateCluster(ctx, existing); err != nil {
			return fmt.Errorf("update scheduler home cluster: %w", err)
		}
	}
	if err := repo.UpdateClusterLastSeen(ctx, desired.ClusterID); err != nil {
		return fmt.Errorf("update scheduler home cluster heartbeat: %w", err)
	}
	return nil
}

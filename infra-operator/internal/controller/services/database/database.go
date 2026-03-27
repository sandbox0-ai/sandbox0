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

package database

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	infrav1alpha1 "github.com/sandbox0-ai/sandbox0/infra-operator/api/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/infra-operator/internal/controller/pkg/common"
	"github.com/sandbox0-ai/sandbox0/pkg/framework"
)

const (
	databaseName       = "postgres"
	databaseSecretName = "sandbox0-database-credentials"
	databasePort       = 5432
)

type Reconciler struct {
	Resources *common.ResourceManager
}

func NewReconciler(resources *common.ResourceManager) *Reconciler {
	return &Reconciler{Resources: resources}
}

// CleanupBuiltinResources removes builtin database resources according to the
// configured stateful resource policy.
func (r *Reconciler) CleanupBuiltinResources(ctx context.Context, infra *infrav1alpha1.Sandbox0Infra) error {
	return r.cleanupBuiltinDatabaseResources(ctx, infra)
}

// Reconcile reconciles the database component.
func (r *Reconciler) Reconcile(ctx context.Context, infra *infrav1alpha1.Sandbox0Infra) error {
	logger := log.FromContext(ctx)

	if infra.Spec.Database == nil {
		return r.cleanupBuiltinDatabaseResources(ctx, infra)
	}

	switch infra.Spec.Database.Type {
	case infrav1alpha1.DatabaseTypeBuiltin:
		logger.Info("Reconciling builtin database")
		if !resolveBuiltinDatabaseConfig(infra).Enabled {
			return r.cleanupBuiltinDatabaseResources(ctx, infra)
		}
		return r.reconcileBuiltinDatabase(ctx, infra)
	case infrav1alpha1.DatabaseTypeExternal:
		logger.Info("Using external database")
		if err := r.cleanupBuiltinDatabaseResources(ctx, infra); err != nil {
			return err
		}
		return ValidateExternalDatabase(ctx, r.Resources.Client, infra)
	default:
		return r.reconcileBuiltinDatabase(ctx, infra)
	}
}

// ValidateExternalDatabase validates connection to external database.
func ValidateExternalDatabase(ctx context.Context, client client.Client, infra *infrav1alpha1.Sandbox0Infra) error {
	if infra.Spec.Database.External == nil {
		return fmt.Errorf("external database configuration is required")
	}

	// Check if password secret exists
	secret := &corev1.Secret{}
	if err := client.Get(ctx, types.NamespacedName{
		Name:      infra.Spec.Database.External.PasswordSecret.Name,
		Namespace: infra.Namespace,
	}, secret); err != nil {
		return fmt.Errorf("database password secret not found: %w", err)
	}

	key := infra.Spec.Database.External.PasswordSecret.Key
	if key == "" {
		key = "password"
	}
	if _, ok := secret.Data[key]; !ok {
		return fmt.Errorf("key %s not found in database password secret", key)
	}

	return nil
}

// reconcileBuiltinDatabase creates a single-node PostgreSQL StatefulSet.
func (r *Reconciler) reconcileBuiltinDatabase(ctx context.Context, infra *infrav1alpha1.Sandbox0Infra) error {
	logger := log.FromContext(ctx)

	// Create database credentials secret
	if err := r.reconcileDatabaseSecret(ctx, infra); err != nil {
		return err
	}

	// Create PVC for database
	if err := r.reconcileDatabasePVC(ctx, infra); err != nil {
		return err
	}

	// Create StatefulSet for PostgreSQL
	if err := r.reconcileDatabaseStatefulSet(ctx, infra); err != nil {
		return err
	}

	// Create Service for PostgreSQL
	if err := r.reconcileDatabaseService(ctx, infra); err != nil {
		return err
	}

	if err := r.ensureDatabaseReady(ctx, infra); err != nil {
		return err
	}

	logger.Info("Builtin database reconciled successfully")
	return nil
}

// reconcileDatabaseSecret creates or updates the database credentials secret.
func (r *Reconciler) reconcileDatabaseSecret(ctx context.Context, infra *infrav1alpha1.Sandbox0Infra) error {
	secretName := fmt.Sprintf("%s-%s", infra.Name, databaseSecretName)
	builtin := resolveBuiltinDatabaseConfig(infra)

	secret := &corev1.Secret{}
	err := r.Resources.Client.Get(ctx, types.NamespacedName{Name: secretName, Namespace: infra.Namespace}, secret)
	if err != nil && !errors.IsNotFound(err) {
		return err
	}

	if errors.IsNotFound(err) {
		// Create new secret with generated password
		password := common.GenerateRandomString(24)
		host := buildDatabaseHost(infra)
		secret = &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Name:      secretName,
				Namespace: infra.Namespace,
			},
			Type: corev1.SecretTypeOpaque,
			StringData: map[string]string{
				"username": builtin.Username,
				"password": password,
				"database": builtin.Database,
				"host":     host,
				"port":     fmt.Sprintf("%d", builtin.Port),
				"dsn":      fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=%s", builtin.Username, password, host, builtin.Port, builtin.Database, builtin.SSLMode),
			},
		}

		if err := ctrl.SetControllerReference(infra, secret, r.Resources.Scheme); err != nil {
			return err
		}

		return r.Resources.Client.Create(ctx, secret)
	}

	return nil
}

// reconcileDatabasePVC creates or updates the database PVC.
func (r *Reconciler) reconcileDatabasePVC(ctx context.Context, infra *infrav1alpha1.Sandbox0Infra) error {
	pvcName := fmt.Sprintf("%s-postgres-data", infra.Name)

	pvc := &corev1.PersistentVolumeClaim{}
	err := r.Resources.Client.Get(ctx, types.NamespacedName{Name: pvcName, Namespace: infra.Namespace}, pvc)
	if err != nil && !errors.IsNotFound(err) {
		return err
	}

	if errors.IsNotFound(err) {
		size := resource.MustParse("20Gi")
		if infra.Spec.Database.Builtin != nil && infra.Spec.Database.Builtin.Persistence != nil {
			size = infra.Spec.Database.Builtin.Persistence.Size
		}

		pvc = &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:      pvcName,
				Namespace: infra.Namespace,
			},
			Spec: corev1.PersistentVolumeClaimSpec{
				AccessModes: []corev1.PersistentVolumeAccessMode{
					corev1.ReadWriteOnce,
				},
				Resources: corev1.VolumeResourceRequirements{
					Requests: corev1.ResourceList{
						corev1.ResourceStorage: size,
					},
				},
			},
		}

		// Set storage class if specified
		if infra.Spec.Database.Builtin != nil &&
			infra.Spec.Database.Builtin.Persistence != nil &&
			infra.Spec.Database.Builtin.Persistence.StorageClass != "" {
			pvc.Spec.StorageClassName = &infra.Spec.Database.Builtin.Persistence.StorageClass
		}

		if err := ctrl.SetControllerReference(infra, pvc, r.Resources.Scheme); err != nil {
			return err
		}

		return r.Resources.Client.Create(ctx, pvc)
	}

	return nil
}

// reconcileDatabaseStatefulSet creates or updates the PostgreSQL StatefulSet.
func (r *Reconciler) reconcileDatabaseStatefulSet(ctx context.Context, infra *infrav1alpha1.Sandbox0Infra) error {
	stsName := fmt.Sprintf("%s-postgres", infra.Name)
	secretName := fmt.Sprintf("%s-%s", infra.Name, databaseSecretName)
	pvcName := fmt.Sprintf("%s-postgres-data", infra.Name)
	builtin := resolveBuiltinDatabaseConfig(infra)

	sts := &appsv1.StatefulSet{}
	err := r.Resources.Client.Get(ctx, types.NamespacedName{Name: stsName, Namespace: infra.Namespace}, sts)
	if err != nil && !errors.IsNotFound(err) {
		return err
	}

	replicas := int32(1)
	labels := map[string]string{
		"app.kubernetes.io/name":       databaseName,
		"app.kubernetes.io/instance":   infra.Name,
		"app.kubernetes.io/component":  "database",
		"app.kubernetes.io/managed-by": "sandbox0infra-operator",
	}

	desiredSts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      stsName,
			Namespace: infra.Namespace,
		},
		Spec: appsv1.StatefulSetSpec{
			ServiceName: stsName,
			Replicas:    &replicas,
			Selector: &metav1.LabelSelector{
				MatchLabels: labels,
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels:      labels,
					Annotations: common.EnsurePodTemplateAnnotations(nil),
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:  databaseName,
							Image: builtin.Image,
							Ports: []corev1.ContainerPort{
								{
									Name:          "postgres",
									ContainerPort: builtin.Port,
								},
							},
							Env: []corev1.EnvVar{
								{
									Name: "POSTGRES_USER",
									ValueFrom: &corev1.EnvVarSource{
										SecretKeyRef: &corev1.SecretKeySelector{
											LocalObjectReference: corev1.LocalObjectReference{
												Name: secretName,
											},
											Key: "username",
										},
									},
								},
								{
									Name: "POSTGRES_PASSWORD",
									ValueFrom: &corev1.EnvVarSource{
										SecretKeyRef: &corev1.SecretKeySelector{
											LocalObjectReference: corev1.LocalObjectReference{
												Name: secretName,
											},
											Key: "password",
										},
									},
								},
								{
									Name: "POSTGRES_DB",
									ValueFrom: &corev1.EnvVarSource{
										SecretKeyRef: &corev1.SecretKeySelector{
											LocalObjectReference: corev1.LocalObjectReference{
												Name: secretName,
											},
											Key: "database",
										},
									},
								},
								{
									Name:  "PGDATA",
									Value: "/var/lib/postgresql/data/pgdata",
								},
							},
							VolumeMounts: []corev1.VolumeMount{
								{
									Name:      "data",
									MountPath: "/var/lib/postgresql/data",
								},
							},
							Resources: corev1.ResourceRequirements{
								Requests: corev1.ResourceList{
									corev1.ResourceCPU:    resource.MustParse("100m"),
									corev1.ResourceMemory: resource.MustParse("256Mi"),
								},
								Limits: corev1.ResourceList{
									corev1.ResourceCPU:    resource.MustParse("1"),
									corev1.ResourceMemory: resource.MustParse("1Gi"),
								},
							},
							LivenessProbe: &corev1.Probe{
								ProbeHandler: corev1.ProbeHandler{
									Exec: &corev1.ExecAction{
										Command: []string{"pg_isready", "-U", builtin.Username},
									},
								},
								InitialDelaySeconds: 30,
								PeriodSeconds:       10,
							},
							ReadinessProbe: &corev1.Probe{
								ProbeHandler: corev1.ProbeHandler{
									Exec: &corev1.ExecAction{
										Command: []string{"pg_isready", "-U", builtin.Username},
									},
								},
								InitialDelaySeconds: 5,
								PeriodSeconds:       5,
							},
						},
					},
					Volumes: []corev1.Volume{
						{
							Name: "data",
							VolumeSource: corev1.VolumeSource{
								PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
									ClaimName: pvcName,
								},
							},
						},
					},
				},
			},
		},
	}

	if err := ctrl.SetControllerReference(infra, desiredSts, r.Resources.Scheme); err != nil {
		return err
	}

	if errors.IsNotFound(err) {
		return r.Resources.Client.Create(ctx, desiredSts)
	}

	// Update existing StatefulSet
	sts.Spec = desiredSts.Spec
	return r.Resources.Client.Update(ctx, sts)
}

// reconcileDatabaseService creates or updates the PostgreSQL Service.
func (r *Reconciler) reconcileDatabaseService(ctx context.Context, infra *infrav1alpha1.Sandbox0Infra) error {
	svcName := fmt.Sprintf("%s-postgres", infra.Name)
	builtin := resolveBuiltinDatabaseConfig(infra)

	svc := &corev1.Service{}
	err := r.Resources.Client.Get(ctx, types.NamespacedName{Name: svcName, Namespace: infra.Namespace}, svc)
	if err != nil && !errors.IsNotFound(err) {
		return err
	}

	labels := map[string]string{
		"app.kubernetes.io/name":       databaseName,
		"app.kubernetes.io/instance":   infra.Name,
		"app.kubernetes.io/component":  "database",
		"app.kubernetes.io/managed-by": "sandbox0infra-operator",
	}

	desiredSvc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      svcName,
			Namespace: infra.Namespace,
		},
		Spec: corev1.ServiceSpec{
			Type:     corev1.ServiceTypeClusterIP,
			Selector: labels,
			Ports: []corev1.ServicePort{
				{
					Name:       "postgres",
					Port:       builtin.Port,
					TargetPort: intstr.FromInt(int(builtin.Port)),
				},
			},
		},
	}

	if err := ctrl.SetControllerReference(infra, desiredSvc, r.Resources.Scheme); err != nil {
		return err
	}

	if errors.IsNotFound(err) {
		return r.Resources.Client.Create(ctx, desiredSvc)
	}

	// Update existing Service
	svc.Spec = desiredSvc.Spec
	return r.Resources.Client.Update(ctx, svc)
}

func (r *Reconciler) ensureDatabaseReady(ctx context.Context, infra *infrav1alpha1.Sandbox0Infra) error {
	stsName := fmt.Sprintf("%s-postgres", infra.Name)
	sts := &appsv1.StatefulSet{}
	if err := r.Resources.Client.Get(ctx, types.NamespacedName{Name: stsName, Namespace: infra.Namespace}, sts); err != nil {
		return err
	}

	replicas := int32(1)
	if sts.Spec.Replicas != nil {
		replicas = *sts.Spec.Replicas
	}
	if sts.Status.ReadyReplicas < replicas {
		return fmt.Errorf("database statefulset %q not ready: %d/%d ready", stsName, sts.Status.ReadyReplicas, replicas)
	}

	serviceName := fmt.Sprintf("%s-postgres", infra.Name)
	if r.Resources.LocalDev.LocalDevMode {
		localURL, cleanup, err := framework.PortForwardService(ctx, r.Resources.LocalDev.KubeconfigPath, infra.Namespace, serviceName, int(databasePort))
		if err != nil {
			return fmt.Errorf("port-forward database service %q: %w", serviceName, err)
		}
		defer cleanup()

		host, port, err := parsePortForwardURL(localURL)
		if err != nil {
			return err
		}
		if err := waitForTCP(ctx, host, port, 3*time.Second); err != nil {
			return fmt.Errorf("database port-forward %q not reachable: %w", localURL, err)
		}
		return nil
	}

	serviceHost := fmt.Sprintf("%s.%s.svc", serviceName, infra.Namespace)
	if err := waitForTCP(ctx, serviceHost, databasePort, 3*time.Second); err != nil {
		return fmt.Errorf("database service %q not reachable: %w", serviceHost, err)
	}

	return nil
}

func waitForTCP(ctx context.Context, host string, port int32, timeout time.Duration) error {
	dialer := net.Dialer{}
	dialCtx := ctx
	cancel := func() {}
	if timeout > 0 {
		dialCtx, cancel = context.WithTimeout(ctx, timeout)
	}
	defer cancel()

	conn, err := dialer.DialContext(dialCtx, "tcp", net.JoinHostPort(host, fmt.Sprintf("%d", port)))
	if err != nil {
		return err
	}
	_ = conn.Close()
	return nil
}

func parsePortForwardURL(raw string) (string, int32, error) {
	parsed, err := url.Parse(raw)
	if err != nil {
		return "", 0, fmt.Errorf("parse port-forward url %q: %w", raw, err)
	}
	host := parsed.Hostname()
	if host == "" {
		return "", 0, fmt.Errorf("port-forward url %q missing host", raw)
	}
	portValue := parsed.Port()
	if portValue == "" {
		return "", 0, fmt.Errorf("port-forward url %q missing port", raw)
	}
	portInt, err := strconv.ParseInt(portValue, 10, 32)
	if err != nil {
		return "", 0, fmt.Errorf("parse port-forward port %q: %w", portValue, err)
	}
	return host, int32(portInt), nil
}

func (r *Reconciler) cleanupBuiltinDatabaseResources(ctx context.Context, infra *infrav1alpha1.Sandbox0Infra) error {
	stsName := fmt.Sprintf("%s-postgres", infra.Name)
	sts := &appsv1.StatefulSet{}
	if err := r.Resources.Client.Get(ctx, types.NamespacedName{Name: stsName, Namespace: infra.Namespace}, sts); err == nil {
		if err := r.Resources.Client.Delete(ctx, sts); err != nil && !errors.IsNotFound(err) {
			return err
		}
	} else if !errors.IsNotFound(err) {
		return err
	}

	svc := &corev1.Service{}
	if err := r.Resources.Client.Get(ctx, types.NamespacedName{Name: stsName, Namespace: infra.Namespace}, svc); err == nil {
		if err := r.Resources.Client.Delete(ctx, svc); err != nil && !errors.IsNotFound(err) {
			return err
		}
	} else if !errors.IsNotFound(err) {
		return err
	}

	if resolveBuiltinDatabaseConfig(infra).StatefulResourcePolicy != infrav1alpha1.BuiltinStatefulResourcePolicyDelete {
		return nil
	}

	pvc := &corev1.PersistentVolumeClaim{}
	if err := r.Resources.Client.Get(ctx, types.NamespacedName{Name: fmt.Sprintf("%s-postgres-data", infra.Name), Namespace: infra.Namespace}, pvc); err == nil {
		if err := r.Resources.Client.Delete(ctx, pvc); err != nil && !errors.IsNotFound(err) {
			return err
		}
	} else if !errors.IsNotFound(err) {
		return err
	}

	secret := &corev1.Secret{}
	if err := r.Resources.Client.Get(ctx, types.NamespacedName{Name: fmt.Sprintf("%s-%s", infra.Name, databaseSecretName), Namespace: infra.Namespace}, secret); err == nil {
		if err := r.Resources.Client.Delete(ctx, secret); err != nil && !errors.IsNotFound(err) {
			return err
		}
	} else if !errors.IsNotFound(err) {
		return err
	}

	return nil
}

// GetDatabaseDSN returns the database DSN for services to use.
func GetDatabaseDSN(ctx context.Context, client client.Client, infra *infrav1alpha1.Sandbox0Infra) (string, error) {
	switch infra.Spec.Database.Type {
	case infrav1alpha1.DatabaseTypeBuiltin:
		builtin := resolveBuiltinDatabaseConfig(infra)
		secretName := fmt.Sprintf("%s-%s", infra.Name, databaseSecretName)
		secret := &corev1.Secret{}
		if err := client.Get(ctx, types.NamespacedName{Name: secretName, Namespace: infra.Namespace}, secret); err != nil {
			return "", err
		}
		username := string(secret.Data["username"])
		if username == "" {
			username = builtin.Username
		}
		password := string(secret.Data["password"])
		if password == "" {
			return "", fmt.Errorf("database secret %q missing password", secretName)
		}
		databaseName := string(secret.Data["database"])
		if databaseName == "" {
			databaseName = builtin.Database
		}
		port := builtin.Port
		if portValue := string(secret.Data["port"]); portValue != "" {
			if parsed, err := strconv.ParseInt(portValue, 10, 32); err == nil {
				port = int32(parsed)
			}
		}
		host := buildDatabaseHost(infra)
		return fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=%s",
			username, password, host, port, databaseName, builtin.SSLMode), nil

	case infrav1alpha1.DatabaseTypeExternal:
		if infra.Spec.Database.External == nil {
			return "", fmt.Errorf("external database configuration is required")
		}
		ext := infra.Spec.Database.External

		// Get password from secret
		secret := &corev1.Secret{}
		if err := client.Get(ctx, types.NamespacedName{Name: ext.PasswordSecret.Name, Namespace: infra.Namespace}, secret); err != nil {
			return "", err
		}

		key := ext.PasswordSecret.Key
		if key == "" {
			key = "password"
		}
		password := string(secret.Data[key])

		sslMode := ext.SSLMode
		if sslMode == "" {
			sslMode = "require"
		}

		return fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=%s",
			ext.Username, password, ext.Host, ext.Port, ext.Database, sslMode), nil

	default:
		return "", fmt.Errorf("unsupported database type: %s", infra.Spec.Database.Type)
	}
}

func buildDatabaseHost(infra *infrav1alpha1.Sandbox0Infra) string {
	if infra.Namespace == "" {
		return fmt.Sprintf("%s-postgres", infra.Name)
	}
	return fmt.Sprintf("%s-postgres.%s.svc", infra.Name, infra.Namespace)
}

// GetJuicefsMetaURL returns the JuiceFS metadata database URL.
func GetJuicefsMetaURL(ctx context.Context, client client.Client, infra *infrav1alpha1.Sandbox0Infra) (string, error) {
	if infra.Spec.JuicefsDatabase == nil || infra.Spec.JuicefsDatabase.ShareWithMain {
		return GetDatabaseDSN(ctx, client, infra)
	}

	ext := infra.Spec.JuicefsDatabase.External
	if ext == nil {
		return "", fmt.Errorf("juicefs external database configuration is required")
	}

	secret := &corev1.Secret{}
	if err := client.Get(ctx, types.NamespacedName{Name: ext.PasswordSecret.Name, Namespace: infra.Namespace}, secret); err != nil {
		return "", err
	}

	key := ext.PasswordSecret.Key
	if key == "" {
		key = "password"
	}
	password := string(secret.Data[key])

	sslMode := ext.SSLMode
	if sslMode == "" {
		sslMode = "require"
	}

	port := ext.Port
	if port == 0 {
		port = databasePort
	}

	return fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=%s",
		ext.Username, password, ext.Host, port, ext.Database, sslMode), nil
}

func resolveBuiltinDatabaseConfig(infra *infrav1alpha1.Sandbox0Infra) infrav1alpha1.BuiltinDatabaseConfig {
	cfg := infrav1alpha1.BuiltinDatabaseConfig{
		Enabled:                true,
		Image:                  "postgres:16-alpine",
		Port:                   databasePort,
		Username:               "sandbox0",
		Database:               "sandbox0",
		SSLMode:                "disable",
		StatefulResourcePolicy: infrav1alpha1.BuiltinStatefulResourcePolicyRetain,
	}
	if infra.Spec.Database.Builtin == nil {
		return cfg
	}

	builtin := infra.Spec.Database.Builtin
	cfg.Enabled = builtin.Enabled
	cfg.Persistence = builtin.Persistence
	if builtin.Image != "" {
		cfg.Image = builtin.Image
	}
	if builtin.Port != 0 {
		cfg.Port = builtin.Port
	}
	if builtin.Username != "" {
		cfg.Username = builtin.Username
	}
	if builtin.Database != "" {
		cfg.Database = builtin.Database
	}
	if builtin.SSLMode != "" {
		cfg.SSLMode = builtin.SSLMode
	}
	if builtin.StatefulResourcePolicy != "" {
		cfg.StatefulResourcePolicy = builtin.StatefulResourcePolicy
	}

	return cfg
}

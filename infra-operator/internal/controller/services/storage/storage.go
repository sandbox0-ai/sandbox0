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

package storage

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/juicedata/juicefs/pkg/object"
	infrav1alpha1 "github.com/sandbox0-ai/sandbox0/infra-operator/api/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/infra-operator/internal/controller/pkg/common"
	"github.com/sandbox0-ai/sandbox0/pkg/framework"
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
)

const (
	rustfsSecretName = "sandbox0-rustfs-credentials"
	rustfsPort       = 9000
	rustfsConsole    = 9001
	rustfsUID        = int64(10001)
)

type Reconciler struct {
	Resources *common.ResourceManager
}

func NewReconciler(resources *common.ResourceManager) *Reconciler {
	return &Reconciler{Resources: resources}
}

// CleanupBuiltinResources removes builtin storage resources according to the
// configured stateful resource policy.
func (r *Reconciler) CleanupBuiltinResources(ctx context.Context, infra *infrav1alpha1.Sandbox0Infra) error {
	return r.cleanupBuiltinStorageResources(ctx, infra)
}

// Reconcile reconciles the storage component.
func (r *Reconciler) Reconcile(ctx context.Context, infra *infrav1alpha1.Sandbox0Infra) error {
	logger := log.FromContext(ctx)

	if infra.Spec.Storage == nil {
		return r.cleanupBuiltinStorageResources(ctx, infra)
	}

	switch infra.Spec.Storage.Type {
	case infrav1alpha1.StorageTypeBuiltin:
		logger.Info("Reconciling builtin storage (RustFS)")
		if !resolveBuiltinStorageConfig(infra).Enabled {
			return r.cleanupBuiltinStorageResources(ctx, infra)
		}
		if err := r.reconcileBuiltinStorage(ctx, infra); err != nil {
			return err
		}
		return r.ensureStorageBucket(ctx, infra)
	case infrav1alpha1.StorageTypeS3, infrav1alpha1.StorageTypeOSS:
		logger.Info("Using external storage")
		if err := r.cleanupBuiltinStorageResources(ctx, infra); err != nil {
			return err
		}
		if err := ValidateExternalStorage(ctx, r.Resources.Client, infra); err != nil {
			return err
		}
		return r.ensureStorageBucket(ctx, infra)
	default:
		if err := r.reconcileBuiltinStorage(ctx, infra); err != nil {
			return err
		}
		return r.ensureStorageBucket(ctx, infra)
	}
}

// ValidateExternalStorage validates external storage configuration.
func ValidateExternalStorage(ctx context.Context, client client.Client, infra *infrav1alpha1.Sandbox0Infra) error {
	switch infra.Spec.Storage.Type {
	case infrav1alpha1.StorageTypeS3:
		if infra.Spec.Storage.S3 == nil {
			return fmt.Errorf("S3 configuration is required")
		}
		secret := &corev1.Secret{}
		if err := client.Get(ctx, types.NamespacedName{
			Name:      infra.Spec.Storage.S3.CredentialsSecret.Name,
			Namespace: infra.Namespace,
		}, secret); err != nil {
			return fmt.Errorf("S3 credentials secret not found: %w", err)
		}

	case infrav1alpha1.StorageTypeOSS:
		if infra.Spec.Storage.OSS == nil {
			return fmt.Errorf("OSS configuration is required")
		}
		secret := &corev1.Secret{}
		if err := client.Get(ctx, types.NamespacedName{
			Name:      infra.Spec.Storage.OSS.CredentialsSecret.Name,
			Namespace: infra.Namespace,
		}, secret); err != nil {
			return fmt.Errorf("OSS credentials secret not found: %w", err)
		}
	}

	return nil
}

// reconcileBuiltinStorage creates a single-node RustFS (MinIO-compatible) deployment.
func (r *Reconciler) reconcileBuiltinStorage(ctx context.Context, infra *infrav1alpha1.Sandbox0Infra) error {
	logger := log.FromContext(ctx)

	// Create storage credentials secret
	if err := r.reconcileStorageSecret(ctx, infra); err != nil {
		return err
	}

	// Create PVC for storage
	if err := r.reconcileStoragePVC(ctx, infra); err != nil {
		return err
	}

	// Create StatefulSet for RustFS/MinIO
	if err := r.reconcileStorageStatefulSet(ctx, infra); err != nil {
		return err
	}

	// Create Service for RustFS/MinIO
	if err := r.reconcileStorageService(ctx, infra); err != nil {
		return err
	}

	logger.Info("Builtin storage reconciled successfully")
	return nil
}

// reconcileStorageSecret creates or updates the storage credentials secret.
func (r *Reconciler) reconcileStorageSecret(ctx context.Context, infra *infrav1alpha1.Sandbox0Infra) error {
	secretName := fmt.Sprintf("%s-%s", infra.Name, rustfsSecretName)
	builtin := resolveBuiltinStorageConfig(infra)
	endpoint := builtinEndpoint(infra, builtin)

	secret := &corev1.Secret{}
	err := r.Resources.Client.Get(ctx, types.NamespacedName{Name: secretName, Namespace: infra.Namespace}, secret)
	if err != nil && !errors.IsNotFound(err) {
		return err
	}

	if errors.IsNotFound(err) {
		accessKey := ""
		secretKey := ""
		if builtin.Credentials != nil {
			accessKey = builtin.Credentials.AccessKey
			secretKey = builtin.Credentials.SecretKey
		}
		if accessKey == "" {
			accessKey = common.GenerateRandomString(16)
		}
		if secretKey == "" {
			secretKey = common.GenerateRandomString(32)
		}

		// Use configured credentials if provided
		secret = &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Name:      secretName,
				Namespace: infra.Namespace,
			},
			Type: corev1.SecretTypeOpaque,
			StringData: map[string]string{
				"RUSTFS_ACCESS_KEY": accessKey,
				"RUSTFS_SECRET_KEY": secretKey,
				"endpoint":          endpoint,
			},
		}

		if err := ctrl.SetControllerReference(infra, secret, r.Resources.Scheme); err != nil {
			return err
		}

		return r.Resources.Client.Create(ctx, secret)
	}

	if secret.Data == nil {
		return nil
	}
	if current := string(secret.Data["endpoint"]); current != endpoint {
		secret.Data["endpoint"] = []byte(endpoint)
		return r.Resources.Client.Update(ctx, secret)
	}

	return nil
}

// reconcileStoragePVC creates or updates the storage PVC.
func (r *Reconciler) reconcileStoragePVC(ctx context.Context, infra *infrav1alpha1.Sandbox0Infra) error {
	pvcName := fmt.Sprintf("%s-rustfs-data", infra.Name)

	pvc := &corev1.PersistentVolumeClaim{}
	err := r.Resources.Client.Get(ctx, types.NamespacedName{Name: pvcName, Namespace: infra.Namespace}, pvc)
	if err != nil && !errors.IsNotFound(err) {
		return err
	}

	if errors.IsNotFound(err) {
		size := resource.MustParse("50Gi")
		if infra.Spec.Storage.Builtin != nil && infra.Spec.Storage.Builtin.Persistence != nil {
			size = infra.Spec.Storage.Builtin.Persistence.Size
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
		if infra.Spec.Storage.Builtin != nil &&
			infra.Spec.Storage.Builtin.Persistence != nil &&
			infra.Spec.Storage.Builtin.Persistence.StorageClass != "" {
			pvc.Spec.StorageClassName = &infra.Spec.Storage.Builtin.Persistence.StorageClass
		}

		if err := ctrl.SetControllerReference(infra, pvc, r.Resources.Scheme); err != nil {
			return err
		}

		return r.Resources.Client.Create(ctx, pvc)
	}

	return nil
}

// reconcileStorageStatefulSet creates or updates the RustFS/MinIO StatefulSet.
func (r *Reconciler) reconcileStorageStatefulSet(ctx context.Context, infra *infrav1alpha1.Sandbox0Infra) error {
	stsName := fmt.Sprintf("%s-rustfs", infra.Name)
	secretName := fmt.Sprintf("%s-%s", infra.Name, rustfsSecretName)
	pvcName := fmt.Sprintf("%s-rustfs-data", infra.Name)
	builtin := resolveBuiltinStorageConfig(infra)

	sts := &appsv1.StatefulSet{}
	err := r.Resources.Client.Get(ctx, types.NamespacedName{Name: stsName, Namespace: infra.Namespace}, sts)
	if err != nil && !errors.IsNotFound(err) {
		return err
	}

	replicas := int32(1)
	labels := map[string]string{
		"app.kubernetes.io/name":       "rustfs",
		"app.kubernetes.io/instance":   infra.Name,
		"app.kubernetes.io/component":  "storage",
		"app.kubernetes.io/managed-by": "sandbox0infra-operator",
	}

	// Using RustFS as the built-in S3-compatible storage
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
					Annotations: common.EnsurePodTemplateAnnotations(infra, nil),
				},
				Spec: corev1.PodSpec{
					SecurityContext: rustfsPodSecurityContext(),
					Containers: []corev1.Container{
						{
							Name:    "rustfs",
							Image:   builtin.Image,
							Command: []string{"/usr/bin/rustfs"},
							Ports: []corev1.ContainerPort{
								{
									Name:          "endpoint",
									ContainerPort: builtin.Port,
								},
								{
									Name:          "console",
									ContainerPort: builtin.ConsolePort,
								},
							},
							Env: []corev1.EnvVar{
								{
									Name: "RUSTFS_ACCESS_KEY",
									ValueFrom: &corev1.EnvVarSource{
										SecretKeyRef: &corev1.SecretKeySelector{
											LocalObjectReference: corev1.LocalObjectReference{
												Name: secretName,
											},
											Key: "RUSTFS_ACCESS_KEY",
										},
									},
								},
								{
									Name: "RUSTFS_SECRET_KEY",
									ValueFrom: &corev1.EnvVarSource{
										SecretKeyRef: &corev1.SecretKeySelector{
											LocalObjectReference: corev1.LocalObjectReference{
												Name: secretName,
											},
											Key: "RUSTFS_SECRET_KEY",
										},
									},
								},
								{
									Name:  "RUSTFS_ADDRESS",
									Value: fmt.Sprintf(":%d", builtin.Port),
								},
								{
									Name:  "RUSTFS_CONSOLE_ADDRESS",
									Value: fmt.Sprintf(":%d", builtin.ConsolePort),
								},
								{
									Name:  "RUSTFS_CONSOLE_ENABLE",
									Value: fmt.Sprintf("%t", builtin.ConsoleEnabled),
								},
								{
									Name:  "RUSTFS_VOLUMES",
									Value: builtin.Volumes,
								},
								{
									Name:  "RUSTFS_REGION",
									Value: builtin.Region,
								},
								{
									Name:  "RUSTFS_OBS_LOG_DIRECTORY",
									Value: builtin.ObsLogDirectory,
								},
								{
									Name:  "RUSTFS_OBS_LOGGER_LEVEL",
									Value: builtin.ObsLoggerLevel,
								},
								{
									Name:  "RUSTFS_OBS_ENVIRONMENT",
									Value: builtin.ObsEnvironment,
								},
							},
							VolumeMounts: []corev1.VolumeMount{
								{
									Name:      "data",
									MountPath: "/data",
								},
							},
							Resources: corev1.ResourceRequirements{
								Requests: corev1.ResourceList{
									corev1.ResourceCPU:    resource.MustParse("100m"),
									corev1.ResourceMemory: resource.MustParse("256Mi"),
								},
								Limits: corev1.ResourceList{
									corev1.ResourceCPU:    resource.MustParse("1"),
									corev1.ResourceMemory: resource.MustParse("2Gi"),
								},
							},
							LivenessProbe: &corev1.Probe{
								ProbeHandler: corev1.ProbeHandler{
									HTTPGet: &corev1.HTTPGetAction{
										Path: "/health",
										Port: intstr.FromInt(int(builtin.Port)),
									},
								},
								InitialDelaySeconds: 10,
								PeriodSeconds:       5,
							},
							ReadinessProbe: &corev1.Probe{
								ProbeHandler: corev1.ProbeHandler{
									HTTPGet: &corev1.HTTPGetAction{
										Path: "/health",
										Port: intstr.FromInt(int(builtin.Port)),
									},
								},
								InitialDelaySeconds: 30,
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

// reconcileStorageService creates or updates the RustFS/MinIO Service.
func (r *Reconciler) reconcileStorageService(ctx context.Context, infra *infrav1alpha1.Sandbox0Infra) error {
	svcName := fmt.Sprintf("%s-rustfs", infra.Name)
	builtin := resolveBuiltinStorageConfig(infra)

	svc := &corev1.Service{}
	err := r.Resources.Client.Get(ctx, types.NamespacedName{Name: svcName, Namespace: infra.Namespace}, svc)
	if err != nil && !errors.IsNotFound(err) {
		return err
	}

	labels := map[string]string{
		"app.kubernetes.io/name":       "rustfs",
		"app.kubernetes.io/instance":   infra.Name,
		"app.kubernetes.io/component":  "storage",
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
					Name:       "api",
					Port:       builtin.Port,
					TargetPort: intstr.FromInt(int(builtin.Port)),
				},
				{
					Name:       "console",
					Port:       builtin.ConsolePort,
					TargetPort: intstr.FromInt(int(builtin.ConsolePort)),
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

func (r *Reconciler) ensureStorageBucket(ctx context.Context, infra *infrav1alpha1.Sandbox0Infra) error {
	config, err := GetStorageConfig(ctx, r.Resources.Client, infra)
	if err != nil {
		return err
	}

	if config.Type == infrav1alpha1.StorageTypeBuiltin {
		if err := r.ensureBuiltinStorageReady(ctx, infra); err != nil {
			return err
		}
		builtin := resolveBuiltinStorageConfig(infra)
		if r.Resources.LocalDev.LocalDevMode {
			serviceName := fmt.Sprintf("%s-rustfs", infra.Name)
			localURL, cleanup, err := framework.PortForwardService(ctx, r.Resources.LocalDev.KubeconfigPath, infra.Namespace, serviceName, int(builtin.Port))
			if err != nil {
				return fmt.Errorf("port-forward rustfs service %q: %w", serviceName, err)
			}
			defer cleanup()

			host, port, err := parsePortForwardURL(localURL)
			if err != nil {
				return err
			}
			if err := waitForTCP(ctx, host, port, 3*time.Second); err != nil {
				return fmt.Errorf("rustfs port-forward %q not reachable: %w", localURL, err)
			}

			config.Endpoint = localURL
		} else {
			config.Endpoint = builtinEndpoint(infra, builtin)
		}
	}

	store, err := r.createObjectStorage(config)
	if err != nil {
		return err
	}

	if _, err := store.Head(""); err == nil {
		return nil
	}

	if err := store.Create(); err != nil {
		return fmt.Errorf("%s: %w", bucketCreateHint(config, err), err)
	}

	return nil
}

func (r *Reconciler) ensureBuiltinStorageReady(ctx context.Context, infra *infrav1alpha1.Sandbox0Infra) error {
	stsName := fmt.Sprintf("%s-rustfs", infra.Name)
	sts := &appsv1.StatefulSet{}
	if err := r.Resources.Client.Get(ctx, types.NamespacedName{Name: stsName, Namespace: infra.Namespace}, sts); err != nil {
		return err
	}

	replicas := int32(1)
	if sts.Spec.Replicas != nil {
		replicas = *sts.Spec.Replicas
	}
	if sts.Status.ReadyReplicas < replicas {
		return fmt.Errorf("rustfs statefulset %q not ready: %d/%d ready", stsName, sts.Status.ReadyReplicas, replicas)
	}

	builtin := resolveBuiltinStorageConfig(infra)
	if r.Resources.LocalDev.LocalDevMode {
		serviceName := fmt.Sprintf("%s-rustfs", infra.Name)
		localURL, cleanup, err := framework.PortForwardService(ctx, r.Resources.LocalDev.KubeconfigPath, infra.Namespace, serviceName, int(builtin.Port))
		if err != nil {
			return fmt.Errorf("port-forward rustfs service %q: %w", serviceName, err)
		}
		defer cleanup()

		host, port, err := parsePortForwardURL(localURL)
		if err != nil {
			return err
		}
		if err := waitForTCP(ctx, host, port, 3*time.Second); err != nil {
			return fmt.Errorf("rustfs port-forward %q not reachable: %w", localURL, err)
		}
		return nil
	}

	endpoint := builtinEndpoint(infra, builtin)
	host, port, err := parseEndpointHostPort(endpoint, builtin.Port)
	if err != nil {
		return err
	}
	if err := waitForTCP(ctx, host, port, 3*time.Second); err != nil {
		return fmt.Errorf("rustfs endpoint %q not reachable: %w", endpoint, err)
	}

	return nil
}

func builtinEndpoint(infra *infrav1alpha1.Sandbox0Infra, builtin infrav1alpha1.BuiltinStorageConfig) string {
	return fmt.Sprintf("http://%s-rustfs.%s.svc:%d", infra.Name, infra.Namespace, builtin.Port)
}

func bucketCreateHint(config *StorageConfig, err error) string {
	if config == nil {
		return "failed to ensure bucket"
	}
	if config.Type == infrav1alpha1.StorageTypeBuiltin {
		return fmt.Sprintf("failed to auto-create builtin bucket %q", config.Bucket)
	}
	if isAccessDeniedError(err) {
		return fmt.Sprintf(
			"bucket %q not accessible; pre-create it or grant CreateBucket/ListBucket permissions",
			config.Bucket,
		)
	}
	if isNoSuchBucketError(err) {
		return fmt.Sprintf("bucket %q does not exist; pre-create it or grant CreateBucket permission", config.Bucket)
	}
	return fmt.Sprintf("failed to ensure bucket %q", config.Bucket)
}

func isAccessDeniedError(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "accessdenied") ||
		strings.Contains(msg, "access denied") ||
		strings.Contains(msg, "forbidden") ||
		strings.Contains(msg, "authorization") ||
		strings.Contains(msg, "not authorized") ||
		strings.Contains(msg, "signaturedoesnotmatch") ||
		strings.Contains(msg, "invalidaccesskeyid")
}

func isNoSuchBucketError(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "nosuchbucket") ||
		strings.Contains(msg, "bucket does not exist") ||
		strings.Contains(msg, "404")
}

func (r *Reconciler) createObjectStorage(config *StorageConfig) (object.ObjectStorage, error) {
	endpoint := strings.TrimRight(strings.TrimSpace(config.Endpoint), "/")
	if endpoint == "" && config.Type == infrav1alpha1.StorageTypeS3 {
		endpoint = fmt.Sprintf("https://s3.%s.amazonaws.com", config.Region)
	}
	if endpoint == "" {
		return nil, fmt.Errorf("storage endpoint is required to verify bucket")
	}

	bucketURL := fmt.Sprintf("%s/%s", endpoint, config.Bucket)
	storageType := "s3"
	if config.Type == infrav1alpha1.StorageTypeOSS {
		storageType = "oss"
	}

	store, err := object.CreateStorage(
		storageType,
		bucketURL,
		config.AccessKey,
		config.SecretKey,
		config.SessionToken,
	)
	if err != nil {
		return nil, fmt.Errorf("create storage client: %w", err)
	}

	return store, nil
}

func parseEndpointHostPort(endpoint string, fallbackPort int32) (string, int32, error) {
	parsed, err := url.Parse(endpoint)
	if err != nil {
		return "", 0, fmt.Errorf("parse storage endpoint %q: %w", endpoint, err)
	}
	host := parsed.Hostname()
	if host == "" {
		return "", 0, fmt.Errorf("storage endpoint %q missing host", endpoint)
	}
	portValue := parsed.Port()
	if portValue == "" {
		return host, fallbackPort, nil
	}
	portInt, err := net.LookupPort("tcp", portValue)
	if err != nil {
		return "", 0, fmt.Errorf("parse storage endpoint port %q: %w", portValue, err)
	}
	return host, int32(portInt), nil
}

func (r *Reconciler) cleanupBuiltinStorageResources(ctx context.Context, infra *infrav1alpha1.Sandbox0Infra) error {
	stsName := fmt.Sprintf("%s-rustfs", infra.Name)
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

	if resolveBuiltinStorageConfig(infra).StatefulResourcePolicy != infrav1alpha1.BuiltinStatefulResourcePolicyDelete {
		return nil
	}

	pvc := &corev1.PersistentVolumeClaim{}
	if err := r.Resources.Client.Get(ctx, types.NamespacedName{Name: fmt.Sprintf("%s-rustfs-data", infra.Name), Namespace: infra.Namespace}, pvc); err == nil {
		if err := r.Resources.Client.Delete(ctx, pvc); err != nil && !errors.IsNotFound(err) {
			return err
		}
	} else if !errors.IsNotFound(err) {
		return err
	}

	secret := &corev1.Secret{}
	if err := r.Resources.Client.Get(ctx, types.NamespacedName{Name: fmt.Sprintf("%s-%s", infra.Name, rustfsSecretName), Namespace: infra.Namespace}, secret); err == nil {
		if err := r.Resources.Client.Delete(ctx, secret); err != nil && !errors.IsNotFound(err) {
			return err
		}
	} else if !errors.IsNotFound(err) {
		return err
	}

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

func int64Ptr(v int64) *int64 {
	return &v
}

func fsGroupPolicyPtr(v corev1.PodFSGroupChangePolicy) *corev1.PodFSGroupChangePolicy {
	return &v
}

func rustfsPodSecurityContext() *corev1.PodSecurityContext {
	return &corev1.PodSecurityContext{
		RunAsNonRoot:        common.BoolPtr(true),
		RunAsUser:           int64Ptr(rustfsUID),
		RunAsGroup:          int64Ptr(rustfsUID),
		FSGroup:             int64Ptr(rustfsUID),
		FSGroupChangePolicy: fsGroupPolicyPtr(corev1.FSGroupChangeOnRootMismatch),
	}
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

// StorageConfig contains storage configuration for services.
type StorageConfig struct {
	Type         infrav1alpha1.StorageType
	Endpoint     string
	Bucket       string
	Region       string
	AccessKey    string
	SecretKey    string
	SessionToken string
	SecretName   string
}

// GetStorageConfig returns the storage configuration for services to use.
func GetStorageConfig(ctx context.Context, client client.Client, infra *infrav1alpha1.Sandbox0Infra) (*StorageConfig, error) {
	config := &StorageConfig{
		Type: infra.Spec.Storage.Type,
	}

	switch infra.Spec.Storage.Type {
	case infrav1alpha1.StorageTypeBuiltin:
		builtin := resolveBuiltinStorageConfig(infra)
		secretName := fmt.Sprintf("%s-%s", infra.Name, rustfsSecretName)
		secret := &corev1.Secret{}
		if err := client.Get(ctx, types.NamespacedName{Name: secretName, Namespace: infra.Namespace}, secret); err != nil {
			return nil, err
		}
		config.Endpoint = string(secret.Data["endpoint"])
		config.AccessKey = string(secret.Data["RUSTFS_ACCESS_KEY"])
		config.SecretKey = string(secret.Data["RUSTFS_SECRET_KEY"])
		config.SecretName = secretName
		config.Bucket = builtin.Bucket
		config.Region = builtin.Region

	case infrav1alpha1.StorageTypeS3:
		if infra.Spec.Storage.S3 == nil {
			return nil, fmt.Errorf("S3 configuration is required")
		}
		s3 := infra.Spec.Storage.S3
		config.Bucket = s3.Bucket
		config.Region = s3.Region
		config.Endpoint = s3.Endpoint
		config.SecretName = s3.CredentialsSecret.Name

		// Get credentials from secret
		secret := &corev1.Secret{}
		if err := client.Get(ctx, types.NamespacedName{Name: s3.CredentialsSecret.Name, Namespace: infra.Namespace}, secret); err != nil {
			return nil, err
		}
		accessKeyKey := s3.CredentialsSecret.AccessKeyKey
		if accessKeyKey == "" {
			accessKeyKey = "accessKeyId"
		}
		secretKeyKey := s3.CredentialsSecret.SecretKeyKey
		if secretKeyKey == "" {
			secretKeyKey = "secretAccessKey"
		}
		config.AccessKey = string(secret.Data[accessKeyKey])
		config.SecretKey = string(secret.Data[secretKeyKey])
		sessionTokenKey := s3.SessionTokenKey
		if sessionTokenKey != "" {
			config.SessionToken = string(secret.Data[sessionTokenKey])
		}

	case infrav1alpha1.StorageTypeOSS:
		if infra.Spec.Storage.OSS == nil {
			return nil, fmt.Errorf("OSS configuration is required")
		}
		oss := infra.Spec.Storage.OSS
		config.Bucket = oss.Bucket
		config.Region = oss.Region
		config.Endpoint = oss.Endpoint
		config.SecretName = oss.CredentialsSecret.Name

		// Get credentials from secret
		secret := &corev1.Secret{}
		if err := client.Get(ctx, types.NamespacedName{Name: oss.CredentialsSecret.Name, Namespace: infra.Namespace}, secret); err != nil {
			return nil, err
		}
		accessKeyKey := oss.CredentialsSecret.AccessKeyKey
		if accessKeyKey == "" {
			accessKeyKey = "accessKeyId"
		}
		secretKeyKey := oss.CredentialsSecret.SecretKeyKey
		if secretKeyKey == "" {
			secretKeyKey = "accessKeySecret"
		}
		config.AccessKey = string(secret.Data[accessKeyKey])
		config.SecretKey = string(secret.Data[secretKeyKey])

	default:
		return nil, fmt.Errorf("unsupported storage type: %s", infra.Spec.Storage.Type)
	}

	return config, nil
}

func resolveBuiltinStorageConfig(infra *infrav1alpha1.Sandbox0Infra) infrav1alpha1.BuiltinStorageConfig {
	cfg := infrav1alpha1.BuiltinStorageConfig{
		Enabled:                true,
		Image:                  "rustfs/rustfs:1.0.0-alpha.79",
		Port:                   rustfsPort,
		ConsolePort:            rustfsConsole,
		Bucket:                 "sandbox0",
		Region:                 "us-east-1",
		ConsoleEnabled:         true,
		Volumes:                "/data",
		ObsLogDirectory:        "/data/logs",
		ObsLoggerLevel:         "debug",
		ObsEnvironment:         "develop",
		StatefulResourcePolicy: infrav1alpha1.BuiltinStatefulResourcePolicyRetain,
	}

	if infra.Spec.Storage.Builtin == nil {
		return cfg
	}

	builtin := infra.Spec.Storage.Builtin
	cfg.Enabled = builtin.Enabled
	cfg.Persistence = builtin.Persistence
	cfg.Credentials = builtin.Credentials
	if builtin.Image != "" {
		cfg.Image = builtin.Image
	}
	if builtin.Port != 0 {
		cfg.Port = builtin.Port
	}
	if builtin.ConsolePort != 0 {
		cfg.ConsolePort = builtin.ConsolePort
	}
	if builtin.Bucket != "" {
		cfg.Bucket = builtin.Bucket
	}
	if builtin.Region != "" {
		cfg.Region = builtin.Region
	}
	if builtin.Volumes != "" {
		cfg.Volumes = builtin.Volumes
	}
	if builtin.ObsLogDirectory != "" {
		cfg.ObsLogDirectory = builtin.ObsLogDirectory
	}
	if builtin.ObsLoggerLevel != "" {
		cfg.ObsLoggerLevel = builtin.ObsLoggerLevel
	}
	if builtin.ObsEnvironment != "" {
		cfg.ObsEnvironment = builtin.ObsEnvironment
	}
	cfg.ConsoleEnabled = builtin.ConsoleEnabled
	if builtin.StatefulResourcePolicy != "" {
		cfg.StatefulResourcePolicy = builtin.StatefulResourcePolicy
	}

	return cfg
}

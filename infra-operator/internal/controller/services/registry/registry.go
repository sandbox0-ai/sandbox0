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

package registry

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"

	infrav1alpha1 "github.com/sandbox0-ai/sandbox0/infra-operator/api/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/infra-operator/internal/controller/pkg/common"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"golang.org/x/crypto/bcrypt"
)

const (
	registryAuthSecretSuffix = "registry-auth"
	registryPullSecretSuffix = "registry-pull"
	registryPVCNameSuffix    = "registry-data"
	defaultRegistryPort      = 5000
	defaultRegistryImage     = "registry:2.8.3"
	defaultRegistryPVCSize   = "20Gi"
)

type Reconciler struct {
	Resources *common.ResourceManager
}

func NewReconciler(resources *common.ResourceManager) *Reconciler {
	return &Reconciler{Resources: resources}
}

// CleanupBuiltinResources removes builtin registry resources according to the
// configured stateful resource policy.
func (r *Reconciler) CleanupBuiltinResources(ctx context.Context, infra *infrav1alpha1.Sandbox0Infra) error {
	return r.cleanupBuiltinRegistryResources(ctx, infra)
}

// ResolvedRegistryConfig defines resolved registry settings for services.
type ResolvedRegistryConfig struct {
	Provider         infrav1alpha1.RegistryProvider
	PushRegistry     string
	PullRegistry     string
	SourceSecretName string
	SourceSecretKey  string
	TargetSecretName string
}

// ResolveRegistryConfig resolves explicitly configured registry settings for dependent services.
func ResolveRegistryConfig(infra *infrav1alpha1.Sandbox0Infra) *ResolvedRegistryConfig {
	if infra == nil || infra.Spec.Registry == nil {
		return nil
	}

	cfg := infra.Spec.Registry
	provider := infrav1alpha1.RegistryProviderBuiltin
	targetSecretName := "sandbox0-registry-pull"

	if cfg.Provider != "" {
		provider = cfg.Provider
	}
	if cfg.ImagePullSecretName != "" {
		targetSecretName = cfg.ImagePullSecretName
	}

	switch provider {
	case infrav1alpha1.RegistryProviderBuiltin:
		builtin := resolveBuiltinRegistryConfig(infra)
		if !builtin.Enabled {
			return nil
		}
		pushRegistry := builtinPushRegistry(infra, builtin)
		pullRegistry := builtinPullRegistry(infra, builtin.Port)
		return &ResolvedRegistryConfig{
			Provider:         provider,
			PushRegistry:     pushRegistry,
			PullRegistry:     pullRegistry,
			SourceSecretName: fmt.Sprintf("%s-%s", infra.Name, registryPullSecretSuffix),
			SourceSecretKey:  ".dockerconfigjson",
			TargetSecretName: targetSecretName,
		}
	case infrav1alpha1.RegistryProviderAWS:
		if cfg == nil || cfg.AWS == nil {
			return nil
		}
		return resolveExternalRegistry(provider, cfg.AWS, targetSecretName)
	case infrav1alpha1.RegistryProviderGCP:
		if cfg == nil || cfg.GCP == nil {
			return nil
		}
		return resolveExternalRegistry(provider, cfg.GCP, targetSecretName)
	case infrav1alpha1.RegistryProviderAzure:
		if cfg == nil || cfg.Azure == nil {
			return nil
		}
		return resolveExternalRegistry(provider, cfg.Azure, targetSecretName)
	case infrav1alpha1.RegistryProviderAliyun:
		if cfg == nil || cfg.Aliyun == nil {
			return nil
		}
		return resolveExternalRegistry(provider, cfg.Aliyun, targetSecretName)
	case infrav1alpha1.RegistryProviderHarbor:
		if cfg == nil || cfg.Harbor == nil {
			return nil
		}
		return resolveExternalRegistry(provider, cfg.Harbor, targetSecretName)
	default:
		return nil
	}
}

// Reconcile reconciles the registry component when explicitly configured.
func (r *Reconciler) Reconcile(ctx context.Context, infra *infrav1alpha1.Sandbox0Infra) error {
	logger := log.FromContext(ctx)

	if infra.Spec.Registry == nil {
		logger.Info("Registry is not configured, cleaning up runtime state")
		return r.cleanupBuiltinRegistryResources(ctx, infra)
	}

	provider := infrav1alpha1.RegistryProviderBuiltin
	if infra.Spec.Registry != nil && infra.Spec.Registry.Provider != "" {
		provider = infra.Spec.Registry.Provider
	}

	switch provider {
	case infrav1alpha1.RegistryProviderBuiltin:
		logger.Info("Reconciling builtin registry")
		builtin := resolveBuiltinRegistryConfig(infra)
		if !builtin.Enabled {
			return r.cleanupBuiltinRegistryResources(ctx, infra)
		}
		return r.reconcileBuiltinRegistry(ctx, infra)
	case infrav1alpha1.RegistryProviderAWS,
		infrav1alpha1.RegistryProviderGCP,
		infrav1alpha1.RegistryProviderAzure,
		infrav1alpha1.RegistryProviderAliyun,
		infrav1alpha1.RegistryProviderHarbor:
		if err := r.cleanupBuiltinRegistryResources(ctx, infra); err != nil {
			return err
		}
		logger.Info("Validating external registry configuration")
		return r.validateExternalRegistry(ctx, infra)
	default:
		if err := r.cleanupBuiltinRegistryResources(ctx, infra); err != nil {
			return err
		}
		return r.validateExternalRegistry(ctx, infra)
	}
}

func (r *Reconciler) validateExternalRegistry(ctx context.Context, infra *infrav1alpha1.Sandbox0Infra) error {
	if infra.Spec.Registry == nil {
		return nil
	}
	switch infra.Spec.Registry.Provider {
	case infrav1alpha1.RegistryProviderAWS:
		if infra.Spec.Registry.AWS == nil {
			return fmt.Errorf("registry.aws configuration is required")
		}
		if infra.Spec.Registry.AWS.Registry == "" &&
			(infra.Spec.Registry.AWS.Region == "" || infra.Spec.Registry.AWS.RegistryID == "") {
			return fmt.Errorf("registry.aws.region and registry.aws.registryId are required when registry.aws.registry is empty")
		}
	case infrav1alpha1.RegistryProviderGCP:
		if infra.Spec.Registry.GCP == nil {
			return fmt.Errorf("registry.gcp configuration is required")
		}
	case infrav1alpha1.RegistryProviderAzure:
		if infra.Spec.Registry.Azure == nil {
			return fmt.Errorf("registry.azure configuration is required")
		}
	case infrav1alpha1.RegistryProviderAliyun:
		if infra.Spec.Registry.Aliyun == nil {
			return fmt.Errorf("registry.aliyun configuration is required")
		}
	case infrav1alpha1.RegistryProviderHarbor:
		if infra.Spec.Registry.Harbor == nil {
			return fmt.Errorf("registry.harbor configuration is required")
		}
	}
	resolved := ResolveRegistryConfig(infra)
	if resolved == nil {
		return nil
	}
	if resolved.PushRegistry == "" {
		return fmt.Errorf("registry push endpoint is required")
	}
	if resolved.PullRegistry == "" {
		return fmt.Errorf("registry pull endpoint is required")
	}
	if resolved.SourceSecretName == "" {
		return fmt.Errorf("registry pull secret name is required")
	}
	secret := &corev1.Secret{}
	if err := r.Resources.Client.Get(ctx, types.NamespacedName{
		Name:      resolved.SourceSecretName,
		Namespace: infra.Namespace,
	}, secret); err != nil {
		return fmt.Errorf("registry pull secret not found: %w", err)
	}
	key := resolved.SourceSecretKey
	if key == "" {
		key = ".dockerconfigjson"
	}
	if secret.Data == nil || len(secret.Data[key]) == 0 {
		return fmt.Errorf("registry pull secret %q missing key %q", resolved.SourceSecretName, key)
	}
	return nil
}

func (r *Reconciler) reconcileBuiltinRegistry(ctx context.Context, infra *infrav1alpha1.Sandbox0Infra) error {
	builtin := resolveBuiltinRegistryConfig(infra)
	if !builtin.Enabled {
		return r.cleanupBuiltinRegistryResources(ctx, infra)
	}

	if err := r.reconcileRegistryPVC(ctx, infra, builtin); err != nil {
		return err
	}

	username, password, err := r.reconcileRegistryAuthSecret(ctx, infra, builtin)
	if err != nil {
		return err
	}

	if err := r.reconcileRegistryPullSecret(ctx, infra, builtin, username, password); err != nil {
		return err
	}

	if err := r.reconcileRegistryDeployment(ctx, infra, builtin); err != nil {
		return err
	}

	if err := r.reconcileRegistryService(ctx, infra, builtin); err != nil {
		return err
	}

	if builtin.Ingress != nil && builtin.Ingress.Enabled {
		if err := r.Resources.ReconcileIngress(ctx, infra, fmt.Sprintf("%s-registry", infra.Name), common.ResolveServicePort(builtin.Service, builtin.Port), builtin.Ingress); err != nil {
			return err
		}
	} else {
		ingress := &networkingv1.Ingress{}
		name := fmt.Sprintf("%s-registry", infra.Name)
		err := r.Resources.Client.Get(ctx, types.NamespacedName{Name: name, Namespace: infra.Namespace}, ingress)
		if err == nil {
			if err := r.Resources.Client.Delete(ctx, ingress); err != nil && !errors.IsNotFound(err) {
				return err
			}
		} else if !errors.IsNotFound(err) {
			return err
		}
	}

	return r.Resources.EnsureDeploymentReady(ctx, infra, fmt.Sprintf("%s-registry", infra.Name), 1)
}

func (r *Reconciler) cleanupBuiltinRegistryResources(ctx context.Context, infra *infrav1alpha1.Sandbox0Infra) error {
	name := fmt.Sprintf("%s-registry", infra.Name)

	deploy := &appsv1.Deployment{}
	if err := r.Resources.Client.Get(ctx, types.NamespacedName{Name: name, Namespace: infra.Namespace}, deploy); err == nil {
		if err := r.Resources.Client.Delete(ctx, deploy); err != nil && !errors.IsNotFound(err) {
			return err
		}
	} else if !errors.IsNotFound(err) {
		return err
	}

	service := &corev1.Service{}
	if err := r.Resources.Client.Get(ctx, types.NamespacedName{Name: name, Namespace: infra.Namespace}, service); err == nil {
		if err := r.Resources.Client.Delete(ctx, service); err != nil && !errors.IsNotFound(err) {
			return err
		}
	} else if !errors.IsNotFound(err) {
		return err
	}

	ingress := &networkingv1.Ingress{}
	if err := r.Resources.Client.Get(ctx, types.NamespacedName{Name: name, Namespace: infra.Namespace}, ingress); err == nil {
		if err := r.Resources.Client.Delete(ctx, ingress); err != nil && !errors.IsNotFound(err) {
			return err
		}
	} else if !errors.IsNotFound(err) {
		return err
	}

	for _, secretName := range []string{
		fmt.Sprintf("%s-%s", infra.Name, registryAuthSecretSuffix),
		fmt.Sprintf("%s-%s", infra.Name, registryPullSecretSuffix),
	} {
		secret := &corev1.Secret{}
		if err := r.Resources.Client.Get(ctx, types.NamespacedName{Name: secretName, Namespace: infra.Namespace}, secret); err == nil {
			if err := r.Resources.Client.Delete(ctx, secret); err != nil && !errors.IsNotFound(err) {
				return err
			}
		} else if !errors.IsNotFound(err) {
			return err
		}
	}

	if resolveBuiltinRegistryConfig(infra).StatefulResourcePolicy != infrav1alpha1.BuiltinStatefulResourcePolicyDelete {
		return nil
	}

	pvc := &corev1.PersistentVolumeClaim{}
	if err := r.Resources.Client.Get(ctx, types.NamespacedName{Name: fmt.Sprintf("%s-%s", infra.Name, registryPVCNameSuffix), Namespace: infra.Namespace}, pvc); err == nil {
		if err := r.Resources.Client.Delete(ctx, pvc); err != nil && !errors.IsNotFound(err) {
			return err
		}
	} else if !errors.IsNotFound(err) {
		return err
	}

	return nil
}

func (r *Reconciler) reconcileRegistryPVC(ctx context.Context, infra *infrav1alpha1.Sandbox0Infra, builtin infrav1alpha1.BuiltinRegistryConfig) error {
	pvcName := fmt.Sprintf("%s-%s", infra.Name, registryPVCNameSuffix)

	pvc := &corev1.PersistentVolumeClaim{}
	err := r.Resources.Client.Get(ctx, types.NamespacedName{Name: pvcName, Namespace: infra.Namespace}, pvc)
	if err != nil && !errors.IsNotFound(err) {
		return err
	}

	if errors.IsNotFound(err) {
		size := resource.MustParse(defaultRegistryPVCSize)
		if builtin.Persistence != nil {
			size = builtin.Persistence.Size
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

		if builtin.Persistence != nil && builtin.Persistence.StorageClass != "" {
			pvc.Spec.StorageClassName = &builtin.Persistence.StorageClass
		}

		if err := ctrl.SetControllerReference(infra, pvc, r.Resources.Scheme); err != nil {
			return err
		}

		return r.Resources.Client.Create(ctx, pvc)
	}

	return nil
}

func (r *Reconciler) reconcileRegistryAuthSecret(ctx context.Context, infra *infrav1alpha1.Sandbox0Infra, builtin infrav1alpha1.BuiltinRegistryConfig) (string, string, error) {
	secretName := fmt.Sprintf("%s-%s", infra.Name, registryAuthSecretSuffix)
	secret := &corev1.Secret{}
	err := r.Resources.Client.Get(ctx, types.NamespacedName{Name: secretName, Namespace: infra.Namespace}, secret)
	if err != nil && !errors.IsNotFound(err) {
		return "", "", err
	}
	authSecretMissing := errors.IsNotFound(err)

	username, password, err := r.resolveRegistryCredentials(ctx, infra, builtin)
	if err != nil {
		return "", "", err
	}

	htpasswd, err := buildHtpasswd(username, password)
	if err != nil {
		return "", "", err
	}

	if authSecretMissing {
		secret = &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Name:      secretName,
				Namespace: infra.Namespace,
			},
			Type: corev1.SecretTypeOpaque,
			StringData: map[string]string{
				"username": username,
				"password": password,
				"htpasswd": htpasswd,
			},
		}

		if err := ctrl.SetControllerReference(infra, secret, r.Resources.Scheme); err != nil {
			return "", "", err
		}

		if err := r.Resources.Client.Create(ctx, secret); err != nil {
			return "", "", err
		}
		return username, password, nil
	}

	updated := secret.DeepCopy()
	if updated.Data == nil {
		updated.Data = map[string][]byte{}
	}
	updated.Data["username"] = []byte(username)
	updated.Data["password"] = []byte(password)
	updated.Data["htpasswd"] = []byte(htpasswd)

	if err := r.Resources.Client.Update(ctx, updated); err != nil {
		return "", "", err
	}

	return username, password, nil
}

func (r *Reconciler) reconcileRegistryPullSecret(ctx context.Context, infra *infrav1alpha1.Sandbox0Infra, builtin infrav1alpha1.BuiltinRegistryConfig, username, password string) error {
	secretName := fmt.Sprintf("%s-%s", infra.Name, registryPullSecretSuffix)
	pullRegistry := builtinPullRegistry(infra, builtin.Port)
	dockerConfig, err := buildDockerConfigJSON(pullRegistry, username, password)
	if err != nil {
		return err
	}

	secret := &corev1.Secret{}
	err = r.Resources.Client.Get(ctx, types.NamespacedName{Name: secretName, Namespace: infra.Namespace}, secret)
	if err != nil && !errors.IsNotFound(err) {
		return err
	}

	if errors.IsNotFound(err) {
		secret = &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Name:      secretName,
				Namespace: infra.Namespace,
			},
			Type: corev1.SecretTypeDockerConfigJson,
			Data: map[string][]byte{
				corev1.DockerConfigJsonKey: dockerConfig,
			},
		}
		if err := ctrl.SetControllerReference(infra, secret, r.Resources.Scheme); err != nil {
			return err
		}
		return r.Resources.Client.Create(ctx, secret)
	}

	if secret.Data == nil {
		secret.Data = map[string][]byte{}
	}
	if existing := secret.Data[corev1.DockerConfigJsonKey]; string(existing) == string(dockerConfig) {
		return nil
	}
	secret.Data[corev1.DockerConfigJsonKey] = dockerConfig
	secret.Type = corev1.SecretTypeDockerConfigJson
	return r.Resources.Client.Update(ctx, secret)
}

func (r *Reconciler) reconcileRegistryDeployment(ctx context.Context, infra *infrav1alpha1.Sandbox0Infra, builtin infrav1alpha1.BuiltinRegistryConfig) error {
	deploymentName := fmt.Sprintf("%s-registry", infra.Name)
	labels := common.GetServiceLabels(infra.Name, "registry")
	authSecretName := fmt.Sprintf("%s-%s", infra.Name, registryAuthSecretSuffix)
	pvcName := fmt.Sprintf("%s-%s", infra.Name, registryPVCNameSuffix)

	env := []corev1.EnvVar{
		{
			Name:  "REGISTRY_HTTP_ADDR",
			Value: fmt.Sprintf("0.0.0.0:%d", builtin.Port),
		},
		{
			Name:  "REGISTRY_STORAGE_FILESYSTEM_ROOTDIRECTORY",
			Value: "/var/lib/registry",
		},
		{
			Name:  "REGISTRY_AUTH",
			Value: "htpasswd",
		},
		{
			Name:  "REGISTRY_AUTH_HTPASSWD_REALM",
			Value: "Sandbox0 Registry",
		},
		{
			Name:  "REGISTRY_AUTH_HTPASSWD_PATH",
			Value: "/auth/htpasswd",
		},
	}

	volumeMounts := []corev1.VolumeMount{
		{
			Name:      "data",
			MountPath: "/var/lib/registry",
		},
		{
			Name:      "auth",
			MountPath: "/auth/htpasswd",
			SubPath:   "htpasswd",
			ReadOnly:  true,
		},
	}

	volumes := []corev1.Volume{
		{
			Name: "data",
			VolumeSource: corev1.VolumeSource{
				PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
					ClaimName: pvcName,
				},
			},
		},
		{
			Name: "auth",
			VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{
					SecretName: authSecretName,
					Items: []corev1.KeyToPath{
						{
							Key:  "htpasswd",
							Path: "htpasswd",
						},
					},
				},
			},
		},
	}

	def := common.ServiceDefinition{
		Name:         "registry",
		Port:         builtin.Port,
		TargetPort:   builtin.Port,
		Ports:        []corev1.ContainerPort{{Name: "http", ContainerPort: builtin.Port}},
		Image:        builtin.Image,
		EnvVars:      env,
		VolumeMounts: volumeMounts,
		Volumes:      volumes,
		LivenessProbe: &corev1.Probe{
			ProbeHandler: corev1.ProbeHandler{
				TCPSocket: &corev1.TCPSocketAction{
					Port: intstr.FromInt(int(builtin.Port)),
				},
			},
			InitialDelaySeconds: 10,
			PeriodSeconds:       10,
		},
		ReadinessProbe: &corev1.Probe{
			ProbeHandler: corev1.ProbeHandler{
				TCPSocket: &corev1.TCPSocketAction{
					Port: intstr.FromInt(int(builtin.Port)),
				},
			},
			InitialDelaySeconds: 5,
			PeriodSeconds:       5,
		},
		Resources: &corev1.ResourceRequirements{
			Requests: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("100m"),
				corev1.ResourceMemory: resource.MustParse("128Mi"),
			},
			Limits: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("500m"),
				corev1.ResourceMemory: resource.MustParse("512Mi"),
			},
		},
	}

	if err := r.Resources.ReconcileDeployment(ctx, infra, deploymentName, labels, 1, def); err != nil {
		return err
	}

	return nil
}

func (r *Reconciler) reconcileRegistryService(ctx context.Context, infra *infrav1alpha1.Sandbox0Infra, builtin infrav1alpha1.BuiltinRegistryConfig) error {
	serviceName := fmt.Sprintf("%s-registry", infra.Name)
	labels := common.GetServiceLabels(infra.Name, "registry")
	serviceType := common.ResolveServiceType(builtin.Service)
	servicePort := common.ResolveServicePort(builtin.Service, builtin.Port)

	return r.Resources.ReconcileService(ctx, infra, serviceName, labels, serviceType, servicePort, builtin.Port)
}

func resolveBuiltinRegistryConfig(infra *infrav1alpha1.Sandbox0Infra) infrav1alpha1.BuiltinRegistryConfig {
	cfg := infrav1alpha1.BuiltinRegistryConfig{
		Enabled:                false,
		Image:                  defaultRegistryImage,
		Port:                   defaultRegistryPort,
		StatefulResourcePolicy: infrav1alpha1.BuiltinStatefulResourcePolicyRetain,
	}
	if infra == nil || infra.Spec.Registry == nil {
		return cfg
	}
	if infra.Spec.Registry.Builtin == nil {
		if infra.Spec.Registry.Provider == "" || infra.Spec.Registry.Provider == infrav1alpha1.RegistryProviderBuiltin {
			cfg.Enabled = true
		}
		return cfg
	}
	builtin := infra.Spec.Registry.Builtin
	cfg.Enabled = builtin.Enabled
	cfg.Persistence = builtin.Persistence
	cfg.Service = builtin.Service
	cfg.PushEndpoint = builtin.PushEndpoint
	cfg.Ingress = builtin.Ingress
	cfg.CredentialsSecret = builtin.CredentialsSecret
	if builtin.StatefulResourcePolicy != "" {
		cfg.StatefulResourcePolicy = builtin.StatefulResourcePolicy
	}
	if builtin.Image != "" {
		cfg.Image = builtin.Image
	}
	if builtin.Port != 0 {
		cfg.Port = builtin.Port
	}
	return cfg
}

func (r *Reconciler) resolveRegistryCredentials(ctx context.Context, infra *infrav1alpha1.Sandbox0Infra, builtin infrav1alpha1.BuiltinRegistryConfig) (string, string, error) {
	secretName := ""
	usernameKey := "username"
	passwordKey := "password"
	if builtin.CredentialsSecret != nil {
		secretName = builtin.CredentialsSecret.Name
		if builtin.CredentialsSecret.UsernameKey != "" {
			usernameKey = builtin.CredentialsSecret.UsernameKey
		}
		if builtin.CredentialsSecret.PasswordKey != "" {
			passwordKey = builtin.CredentialsSecret.PasswordKey
		}
	}
	if secretName == "" {
		secretName = fmt.Sprintf("%s-registry-credentials", infra.Name)
	}
	secret := &corev1.Secret{}
	if err := r.Resources.Client.Get(ctx, types.NamespacedName{
		Name:      secretName,
		Namespace: infra.Namespace,
	}, secret); err != nil {
		if !errors.IsNotFound(err) {
			return "", "", fmt.Errorf("get registry credentials secret: %w", err)
		}
		username := "sandbox0"
		password := common.GenerateRandomString(24)
		secret = &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Name:      secretName,
				Namespace: infra.Namespace,
			},
			Type: corev1.SecretTypeOpaque,
			StringData: map[string]string{
				usernameKey: username,
				passwordKey: password,
			},
		}
		if err := ctrl.SetControllerReference(infra, secret, r.Resources.Scheme); err != nil {
			return "", "", err
		}
		if err := r.Resources.Client.Create(ctx, secret); err != nil {
			return "", "", err
		}
		return username, password, nil
	}
	username := ""
	password := ""
	if secret.Data != nil {
		username = string(secret.Data[usernameKey])
		password = string(secret.Data[passwordKey])
	}
	if username == "" || password == "" {
		return "", "", fmt.Errorf("registry credentials secret %q missing keys %q/%q", secretName, usernameKey, passwordKey)
	}
	return username, password, nil
}

func builtinPushRegistry(infra *infrav1alpha1.Sandbox0Infra, builtin infrav1alpha1.BuiltinRegistryConfig) string {
	if builtin.PushEndpoint != "" {
		return normalizeRegistryHost(builtin.PushEndpoint)
	}
	if builtin.Ingress != nil && builtin.Ingress.Enabled && builtin.Ingress.Host != "" {
		return normalizeRegistryHost(builtin.Ingress.Host)
	}
	return builtinPullRegistry(infra, builtin.Port)
}

func builtinPullRegistry(infra *infrav1alpha1.Sandbox0Infra, port int32) string {
	return fmt.Sprintf("%s-registry.%s.svc:%d", infra.Name, infra.Namespace, port)
}

type registryProviderConfig struct {
	Registry   string
	PullSecret infrav1alpha1.DockerConfigSecretRef
	Region     string
	RegistryID string
}

func resolveExternalRegistry(provider infrav1alpha1.RegistryProvider, cfg interface{}, targetSecretName string) *ResolvedRegistryConfig {
	var resolved registryProviderConfig
	switch typed := cfg.(type) {
	case *infrav1alpha1.AWSRegistryConfig:
		if typed == nil {
			return nil
		}
		resolved.Registry = typed.Registry
		resolved.Region = typed.Region
		resolved.RegistryID = typed.RegistryID
		resolved.PullSecret = typed.PullSecret
	case *infrav1alpha1.GCPRegistryConfig:
		if typed == nil {
			return nil
		}
		resolved.Registry = typed.Registry
		resolved.PullSecret = typed.PullSecret
	case *infrav1alpha1.AzureRegistryConfig:
		if typed == nil {
			return nil
		}
		resolved.Registry = typed.Registry
		resolved.PullSecret = typed.PullSecret
	case *infrav1alpha1.AliyunRegistryConfig:
		if typed == nil {
			return nil
		}
		resolved.Registry = typed.Registry
		resolved.PullSecret = typed.PullSecret
	case *infrav1alpha1.HarborRegistryConfig:
		if typed == nil {
			return nil
		}
		resolved.Registry = typed.Registry
		resolved.PullSecret = typed.PullSecret
	default:
		return nil
	}

	secretKey := resolved.PullSecret.Key
	if secretKey == "" {
		secretKey = corev1.DockerConfigJsonKey
	}
	if resolved.Registry == "" && resolved.Region != "" && resolved.RegistryID != "" {
		resolved.Registry = fmt.Sprintf("%s.dkr.ecr.%s.amazonaws.com", resolved.RegistryID, resolved.Region)
	}
	return &ResolvedRegistryConfig{
		Provider:         provider,
		PushRegistry:     resolved.Registry,
		PullRegistry:     resolved.Registry,
		SourceSecretName: resolved.PullSecret.Name,
		SourceSecretKey:  secretKey,
		TargetSecretName: targetSecretName,
	}
}

func normalizeRegistryHost(raw string) string {
	value := raw
	value = strings.TrimSpace(value)
	value = strings.TrimPrefix(value, "https://")
	value = strings.TrimPrefix(value, "http://")
	return value
}

func buildHtpasswd(username, password string) (string, error) {
	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	if err != nil {
		return "", fmt.Errorf("hash registry password: %w", err)
	}
	return fmt.Sprintf("%s:%s", username, string(hash)), nil
}

func buildDockerConfigJSON(registry, username, password string) ([]byte, error) {
	auth := base64.StdEncoding.EncodeToString([]byte(fmt.Sprintf("%s:%s", username, password)))
	payload := map[string]map[string]map[string]string{
		"auths": {
			registry: {
				"username": username,
				"password": password,
				"auth":     auth,
			},
		},
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("marshal dockerconfigjson: %w", err)
	}
	return raw, nil
}

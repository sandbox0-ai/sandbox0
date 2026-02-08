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

	infrav1alpha1 "github.com/sandbox0-ai/infra/infra-operator/api/v1alpha1"
	"github.com/sandbox0-ai/infra/infra-operator/internal/controller/pkg/common"
	corev1 "k8s.io/api/core/v1"
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

// ResolvedRegistryConfig defines resolved registry settings for services.
type ResolvedRegistryConfig struct {
	Provider         infrav1alpha1.RegistryProvider
	Registry         string
	SourceSecretName string
	SourceSecretKey  string
	TargetSecretName string
}

// ResolveRegistryConfig resolves registry configuration for dependent services.
func ResolveRegistryConfig(infra *infrav1alpha1.Sandbox0Infra) *ResolvedRegistryConfig {
	if infra == nil || infra.Spec.Registry == nil {
		return nil
	}
	cfg := infra.Spec.Registry
	targetSecretName := cfg.ImagePullSecretName
	if targetSecretName == "" {
		targetSecretName = "sandbox0-registry-pull"
	}
	switch cfg.Provider {
	case infrav1alpha1.RegistryProviderBuiltin:
		builtin := resolveBuiltinRegistryConfig(infra)
		if !builtin.Enabled {
			return nil
		}
		registryHost := builtinRegistryHost(infra, builtin.Port)
		return &ResolvedRegistryConfig{
			Provider:         cfg.Provider,
			Registry:         registryHost,
			SourceSecretName: fmt.Sprintf("%s-%s", infra.Name, registryPullSecretSuffix),
			SourceSecretKey:  ".dockerconfigjson",
			TargetSecretName: targetSecretName,
		}
	case infrav1alpha1.RegistryProviderAWS:
		return resolveExternalRegistry(cfg.Provider, cfg.AWS, targetSecretName)
	case infrav1alpha1.RegistryProviderGCP:
		return resolveExternalRegistry(cfg.Provider, cfg.GCP, targetSecretName)
	case infrav1alpha1.RegistryProviderAzure:
		return resolveExternalRegistry(cfg.Provider, cfg.Azure, targetSecretName)
	case infrav1alpha1.RegistryProviderAliyun:
		return resolveExternalRegistry(cfg.Provider, cfg.Aliyun, targetSecretName)
	default:
		return nil
	}
}

// Reconcile reconciles the registry component.
func (r *Reconciler) Reconcile(ctx context.Context, infra *infrav1alpha1.Sandbox0Infra) error {
	logger := log.FromContext(ctx)

	if infra.Spec.Registry == nil {
		return nil
	}

	switch infra.Spec.Registry.Provider {
	case infrav1alpha1.RegistryProviderBuiltin:
		logger.Info("Reconciling builtin registry")
		return r.reconcileBuiltinRegistry(ctx, infra)
	case infrav1alpha1.RegistryProviderAWS,
		infrav1alpha1.RegistryProviderGCP,
		infrav1alpha1.RegistryProviderAzure,
		infrav1alpha1.RegistryProviderAliyun:
		logger.Info("Validating external registry configuration")
		return r.validateExternalRegistry(ctx, infra)
	default:
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
	}
	resolved := ResolveRegistryConfig(infra)
	if resolved == nil {
		return nil
	}
	if resolved.Registry == "" {
		return fmt.Errorf("registry endpoint is required")
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
		return nil
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

	username := ""
	password := ""
	if builtin.Credentials != nil {
		username = builtin.Credentials.Username
		password = builtin.Credentials.Password
	}

	if errors.IsNotFound(err) {
		if username == "" {
			username = "sandbox0"
		}
		if password == "" {
			password = common.GenerateRandomString(24)
		}

		htpasswd, err := buildHtpasswd(username, password)
		if err != nil {
			return "", "", err
		}

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

	if secret.Data != nil {
		if stored := string(secret.Data["username"]); stored != "" {
			username = stored
		}
		if stored := string(secret.Data["password"]); stored != "" {
			password = stored
		}
	}
	if username == "" {
		username = "sandbox0"
	}
	if password == "" {
		password = common.GenerateRandomString(24)
	}

	htpasswd, err := buildHtpasswd(username, password)
	if err != nil {
		return "", "", err
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
	registryHost := builtinRegistryHost(infra, builtin.Port)
	dockerConfig, err := buildDockerConfigJSON(registryHost, username, password)
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
				HTTPGet: &corev1.HTTPGetAction{
					Path: "/v2/",
					Port: intstr.FromInt(int(builtin.Port)),
				},
			},
			InitialDelaySeconds: 10,
			PeriodSeconds:       10,
		},
		ReadinessProbe: &corev1.Probe{
			ProbeHandler: corev1.ProbeHandler{
				HTTPGet: &corev1.HTTPGetAction{
					Path: "/v2/",
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

	return r.Resources.EnsureDeploymentReady(ctx, infra, deploymentName, 1)
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
		Enabled: true,
		Image:   defaultRegistryImage,
		Port:    defaultRegistryPort,
	}
	if infra.Spec.Registry == nil || infra.Spec.Registry.Builtin == nil {
		return cfg
	}
	builtin := infra.Spec.Registry.Builtin
	cfg.Enabled = builtin.Enabled
	cfg.Persistence = builtin.Persistence
	cfg.Service = builtin.Service
	cfg.Credentials = builtin.Credentials
	if builtin.Image != "" {
		cfg.Image = builtin.Image
	}
	if builtin.Port != 0 {
		cfg.Port = builtin.Port
	}
	return cfg
}

func builtinRegistryHost(infra *infrav1alpha1.Sandbox0Infra, port int32) string {
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
		Registry:         resolved.Registry,
		SourceSecretName: resolved.PullSecret.Name,
		SourceSecretKey:  secretKey,
		TargetSecretName: targetSecretName,
	}
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

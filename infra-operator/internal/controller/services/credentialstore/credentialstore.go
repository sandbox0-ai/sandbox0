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

package credentialstore

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	pathpkg "path"
	"strings"
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

	apiconfig "github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	infrav1alpha1 "github.com/sandbox0-ai/sandbox0/infra-operator/api/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/infra-operator/internal/controller/pkg/common"
	"github.com/sandbox0-ai/sandbox0/pkg/framework"
)

const (
	componentName        = "credential-vault"
	openBaoName          = "openbao"
	defaultOpenBaoImage  = "openbao/openbao:2.3.1"
	defaultOpenBaoPort   = int32(8200)
	defaultOpenBaoMount  = "secret"
	defaultKeyID         = "default"
	managerPolicyName    = "sandbox0-manager"
	rootTokenKey         = "root_token"
	unsealKeyKey         = "unseal_key"
	managerTokenKey      = "token"
	defaultCATokenKey    = "ca.crt"
	encryptedPGKeyKey    = "key"
	encryptedPGKeyIDKey  = "key_id"
	encryptedPGKeyLength = 32
)

const (
	EncryptedPGKeyDir      = "/etc/sandbox0/credential-store/encrypted-pg"
	EncryptedPGKeyFilePath = EncryptedPGKeyDir + "/key"
	vaultDir               = "/etc/sandbox0/credential-store/vault"
)

type Reconciler struct {
	Resources *common.ResourceManager
}

type EncryptedPGKeyRef struct {
	SecretName string
	Key        string
	KeyID      string
}

func NewReconciler(resources *common.ResourceManager) *Reconciler {
	return &Reconciler{Resources: resources}
}

// Reconcile reconciles the optional Vault-compatible credential backend.
func (r *Reconciler) Reconcile(ctx context.Context, infra *infrav1alpha1.Sandbox0Infra) error {
	if infra == nil {
		return fmt.Errorf("sandbox0infra is required")
	}
	if infra.Spec.CredentialVault == nil {
		return r.CleanupBuiltinResources(ctx, infra)
	}

	switch infra.Spec.CredentialVault.Type {
	case infrav1alpha1.CredentialVaultTypeBuiltin, "":
		cfg := resolveBuiltinCredentialVaultConfig(infra)
		if !cfg.Enabled {
			return r.CleanupBuiltinResources(ctx, infra)
		}
		return r.reconcileBuiltinOpenBao(ctx, infra, cfg)
	case infrav1alpha1.CredentialVaultTypeExternal:
		if err := r.CleanupBuiltinResources(ctx, infra); err != nil {
			return err
		}
		return ValidateExternalCredentialVault(ctx, r.Resources.Client, infra)
	default:
		return fmt.Errorf("unsupported credentialVault type: %s", infra.Spec.CredentialVault.Type)
	}
}

// CleanupBuiltinResources removes builtin OpenBao resources according to the
// configured stateful resource policy.
func (r *Reconciler) CleanupBuiltinResources(ctx context.Context, infra *infrav1alpha1.Sandbox0Infra) error {
	if infra == nil {
		return nil
	}
	name := builtinOpenBaoName(infra)
	for _, obj := range []client.Object{
		&appsv1.StatefulSet{},
		&corev1.Service{},
		&corev1.ConfigMap{},
	} {
		if err := deleteNamespaced(ctx, r.Resources.Client, infra.Namespace, name, obj); err != nil {
			return err
		}
	}

	if resolveBuiltinCredentialVaultConfig(infra).StatefulResourcePolicy != infrav1alpha1.BuiltinStatefulResourcePolicyDelete {
		return nil
	}

	for _, ref := range []struct {
		name string
		obj  client.Object
	}{
		{name: common.BuiltinCredentialVaultPVCName(infra.Name), obj: &corev1.PersistentVolumeClaim{}},
		{name: common.BuiltinCredentialVaultSecretName(infra.Name), obj: &corev1.Secret{}},
		{name: common.BuiltinCredentialVaultManagerTokenSecretName(infra.Name), obj: &corev1.Secret{}},
	} {
		if err := deleteNamespaced(ctx, r.Resources.Client, infra.Namespace, ref.name, ref.obj); err != nil {
			return err
		}
	}
	return nil
}

func (r *Reconciler) reconcileBuiltinOpenBao(ctx context.Context, infra *infrav1alpha1.Sandbox0Infra, cfg infrav1alpha1.BuiltinCredentialVaultConfig) error {
	if err := r.reconcileConfigMap(ctx, infra, cfg); err != nil {
		return err
	}
	if err := r.reconcilePVC(ctx, infra, cfg); err != nil {
		return err
	}
	if err := r.reconcileStatefulSet(ctx, infra, cfg); err != nil {
		return err
	}
	if err := r.reconcileService(ctx, infra, cfg); err != nil {
		return err
	}
	return r.ensureOpenBaoConfigured(ctx, infra, cfg)
}

func (r *Reconciler) reconcileConfigMap(ctx context.Context, infra *infrav1alpha1.Sandbox0Infra, cfg infrav1alpha1.BuiltinCredentialVaultConfig) error {
	name := builtinOpenBaoName(infra)
	labels := credentialVaultLabels(infra)
	desired := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: infra.Namespace,
			Labels:    common.EnsureManagedLabels(labels, name),
		},
		Data: map[string]string{
			"openbao.hcl": openBaoConfig(cfg.Port),
		},
	}
	if err := ctrl.SetControllerReference(infra, desired, r.Resources.Scheme); err != nil {
		return err
	}

	current := &corev1.ConfigMap{}
	err := r.Resources.Client.Get(ctx, types.NamespacedName{Name: name, Namespace: infra.Namespace}, current)
	if errors.IsNotFound(err) {
		return r.Resources.Client.Create(ctx, desired)
	}
	if err != nil {
		return err
	}
	current.Labels = desired.Labels
	current.Data = desired.Data
	return r.Resources.Client.Update(ctx, current)
}

func (r *Reconciler) reconcilePVC(ctx context.Context, infra *infrav1alpha1.Sandbox0Infra, cfg infrav1alpha1.BuiltinCredentialVaultConfig) error {
	name := common.BuiltinCredentialVaultPVCName(infra.Name)
	current := &corev1.PersistentVolumeClaim{}
	err := r.Resources.Client.Get(ctx, types.NamespacedName{Name: name, Namespace: infra.Namespace}, current)
	if errors.IsNotFound(err) {
		size := resource.MustParse("1Gi")
		var storageClassName *string
		if cfg.Persistence != nil {
			if !cfg.Persistence.Size.IsZero() {
				size = cfg.Persistence.Size
			}
			if cfg.Persistence.StorageClass != "" {
				storageClassName = &cfg.Persistence.StorageClass
			}
		}
		desired := &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: infra.Namespace,
			},
			Spec: corev1.PersistentVolumeClaimSpec{
				AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
				Resources: corev1.VolumeResourceRequirements{
					Requests: corev1.ResourceList{
						corev1.ResourceStorage: size,
					},
				},
				StorageClassName: storageClassName,
			},
		}
		if err := ctrl.SetControllerReference(infra, desired, r.Resources.Scheme); err != nil {
			return err
		}
		return r.Resources.Client.Create(ctx, desired)
	}
	return err
}

func (r *Reconciler) reconcileStatefulSet(ctx context.Context, infra *infrav1alpha1.Sandbox0Infra, cfg infrav1alpha1.BuiltinCredentialVaultConfig) error {
	name := builtinOpenBaoName(infra)
	labels := credentialVaultLabels(infra)
	replicas := int32(1)
	desiredLabels := common.EnsureManagedLabels(labels, name)
	desired := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: infra.Namespace,
			Labels:    desiredLabels,
		},
		Spec: appsv1.StatefulSetSpec{
			ServiceName: name,
			Replicas:    &replicas,
			Selector:    &metav1.LabelSelector{MatchLabels: labels},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels:      desiredLabels,
					Annotations: common.EnsurePodTemplateAnnotations(nil),
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:  openBaoName,
							Image: cfg.Image,
							Args:  []string{"server", "-config=/etc/openbao/config/openbao.hcl"},
							Env: []corev1.EnvVar{
								{Name: "BAO_ADDR", Value: fmt.Sprintf("http://127.0.0.1:%d", cfg.Port)},
							},
							Ports: []corev1.ContainerPort{
								{Name: "api", ContainerPort: cfg.Port},
							},
							VolumeMounts: []corev1.VolumeMount{
								{Name: "config", MountPath: "/etc/openbao/config", ReadOnly: true},
								{Name: "data", MountPath: "/openbao/data"},
							},
							Resources: corev1.ResourceRequirements{
								Requests: corev1.ResourceList{
									corev1.ResourceCPU:    resource.MustParse("50m"),
									corev1.ResourceMemory: resource.MustParse("128Mi"),
								},
								Limits: corev1.ResourceList{
									corev1.ResourceCPU:    resource.MustParse("500m"),
									corev1.ResourceMemory: resource.MustParse("512Mi"),
								},
							},
							LivenessProbe:  tcpProbe(cfg.Port, 10),
							ReadinessProbe: tcpProbe(cfg.Port, 5),
						},
					},
					Volumes: []corev1.Volume{
						{
							Name: "config",
							VolumeSource: corev1.VolumeSource{
								ConfigMap: &corev1.ConfigMapVolumeSource{
									LocalObjectReference: corev1.LocalObjectReference{Name: name},
									Items: []corev1.KeyToPath{
										{Key: "openbao.hcl", Path: "openbao.hcl"},
									},
								},
							},
						},
						{
							Name: "data",
							VolumeSource: corev1.VolumeSource{
								PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
									ClaimName: common.BuiltinCredentialVaultPVCName(infra.Name),
								},
							},
						},
					},
				},
			},
		},
	}
	if err := ctrl.SetControllerReference(infra, desired, r.Resources.Scheme); err != nil {
		return err
	}

	current := &appsv1.StatefulSet{}
	err := r.Resources.Client.Get(ctx, types.NamespacedName{Name: name, Namespace: infra.Namespace}, current)
	if errors.IsNotFound(err) {
		return r.Resources.Client.Create(ctx, desired)
	}
	if err != nil {
		return err
	}
	current.Labels = desired.Labels
	current.Spec = desired.Spec
	return r.Resources.Client.Update(ctx, current)
}

func (r *Reconciler) reconcileService(ctx context.Context, infra *infrav1alpha1.Sandbox0Infra, cfg infrav1alpha1.BuiltinCredentialVaultConfig) error {
	name := builtinOpenBaoName(infra)
	labels := credentialVaultLabels(infra)
	return r.Resources.ReconcileServicePorts(ctx, infra, name, labels, corev1.ServiceTypeClusterIP, nil, []corev1.ServicePort{
		{
			Name:       "api",
			Port:       cfg.Port,
			TargetPort: intstr.FromInt(int(cfg.Port)),
			Protocol:   corev1.ProtocolTCP,
		},
	})
}

func (r *Reconciler) ensureOpenBaoConfigured(ctx context.Context, infra *infrav1alpha1.Sandbox0Infra, cfg infrav1alpha1.BuiltinCredentialVaultConfig) error {
	name := builtinOpenBaoName(infra)
	sts := &appsv1.StatefulSet{}
	if err := r.Resources.Client.Get(ctx, types.NamespacedName{Name: name, Namespace: infra.Namespace}, sts); err != nil {
		return err
	}
	replicas := int32(1)
	if sts.Spec.Replicas != nil {
		replicas = *sts.Spec.Replicas
	}
	if sts.Status.ReadyReplicas < replicas {
		return fmt.Errorf("credential vault statefulset %q not ready: %d/%d ready", name, sts.Status.ReadyReplicas, replicas)
	}

	address, cleanup, err := r.openBaoAddress(ctx, infra, cfg)
	if err != nil {
		return err
	}
	defer cleanup()

	api := newOpenBaoAPI(address)
	initialized, err := api.initialized(ctx)
	if err != nil {
		return err
	}

	root, err := r.loadRootSecret(ctx, infra)
	if !initialized {
		root, err = api.initialize(ctx)
		if err != nil {
			return err
		}
		if err := r.upsertRootSecret(ctx, infra, root); err != nil {
			return err
		}
	} else if err != nil {
		return fmt.Errorf("builtin credential vault is initialized but root secret is unavailable: %w", err)
	}

	sealed, err := api.sealed(ctx)
	if err != nil {
		return err
	}
	if sealed {
		if err := api.unseal(ctx, root.UnsealKey); err != nil {
			return err
		}
	}

	mount := credentialVaultMount(cfg.Mount)
	if err := api.ensureKV2Mount(ctx, root.RootToken, mount); err != nil {
		return err
	}
	if err := api.writeManagerPolicy(ctx, root.RootToken, mount); err != nil {
		return err
	}
	if err := r.ensureManagerTokenSecret(ctx, infra, api, root.RootToken); err != nil {
		return err
	}
	log.FromContext(ctx).Info("Builtin credential vault reconciled", "service", name, "mount", mount)
	return nil
}

func (r *Reconciler) openBaoAddress(ctx context.Context, infra *infrav1alpha1.Sandbox0Infra, cfg infrav1alpha1.BuiltinCredentialVaultConfig) (string, func(), error) {
	if r.Resources.LocalDev.LocalDevMode {
		localURL, cleanup, err := framework.PortForwardService(ctx, r.Resources.LocalDev.KubeconfigPath, infra.Namespace, builtinOpenBaoName(infra), int(cfg.Port))
		if err != nil {
			return "", nil, fmt.Errorf("port-forward credential vault service %q: %w", builtinOpenBaoName(infra), err)
		}
		return localURL, cleanup, nil
	}
	return builtinOpenBaoAddress(infra, cfg), func() {}, nil
}

type rootMaterial struct {
	RootToken string
	UnsealKey string
}

func (r *Reconciler) loadRootSecret(ctx context.Context, infra *infrav1alpha1.Sandbox0Infra) (rootMaterial, error) {
	secret := &corev1.Secret{}
	if err := r.Resources.Client.Get(ctx, types.NamespacedName{Name: common.BuiltinCredentialVaultSecretName(infra.Name), Namespace: infra.Namespace}, secret); err != nil {
		return rootMaterial{}, err
	}
	root := rootMaterial{
		RootToken: strings.TrimSpace(string(secret.Data[rootTokenKey])),
		UnsealKey: strings.TrimSpace(string(secret.Data[unsealKeyKey])),
	}
	if root.RootToken == "" || root.UnsealKey == "" {
		return rootMaterial{}, fmt.Errorf("credential vault root secret is missing required keys")
	}
	return root, nil
}

func (r *Reconciler) upsertRootSecret(ctx context.Context, infra *infrav1alpha1.Sandbox0Infra, root rootMaterial) error {
	name := common.BuiltinCredentialVaultSecretName(infra.Name)
	desiredData := map[string][]byte{
		rootTokenKey: []byte(root.RootToken),
		unsealKeyKey: []byte(root.UnsealKey),
	}
	secret := &corev1.Secret{}
	err := r.Resources.Client.Get(ctx, types.NamespacedName{Name: name, Namespace: infra.Namespace}, secret)
	if errors.IsNotFound(err) {
		secret = &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: infra.Namespace},
			Type:       corev1.SecretTypeOpaque,
			Data:       desiredData,
		}
		if err := ctrl.SetControllerReference(infra, secret, r.Resources.Scheme); err != nil {
			return err
		}
		return r.Resources.Client.Create(ctx, secret)
	}
	if err != nil {
		return err
	}
	secret.Data = desiredData
	return r.Resources.Client.Update(ctx, secret)
}

func (r *Reconciler) ensureManagerTokenSecret(ctx context.Context, infra *infrav1alpha1.Sandbox0Infra, api *openBaoAPI, rootToken string) error {
	name := common.BuiltinCredentialVaultManagerTokenSecretName(infra.Name)
	secret := &corev1.Secret{}
	err := r.Resources.Client.Get(ctx, types.NamespacedName{Name: name, Namespace: infra.Namespace}, secret)
	notFound := errors.IsNotFound(err)
	if err == nil {
		token := strings.TrimSpace(string(secret.Data[managerTokenKey]))
		if token != "" && api.lookupToken(ctx, rootToken, token) {
			return nil
		}
	} else if !notFound {
		return err
	}

	token, err := api.createManagerToken(ctx, rootToken)
	if err != nil {
		return err
	}
	if notFound {
		secret = &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: infra.Namespace},
			Type:       corev1.SecretTypeOpaque,
			Data:       map[string][]byte{managerTokenKey: []byte(token)},
		}
		if err := ctrl.SetControllerReference(infra, secret, r.Resources.Scheme); err != nil {
			return err
		}
		return r.Resources.Client.Create(ctx, secret)
	}
	if secret.Data == nil {
		secret.Data = map[string][]byte{}
	}
	secret.Data[managerTokenKey] = []byte(token)
	return r.Resources.Client.Update(ctx, secret)
}

// EnsureEncryptedPGKey ensures manager has a stable AES-256 key for encrypted_pg.
func EnsureEncryptedPGKey(ctx context.Context, resources *common.ResourceManager, scope common.ObjectScope) (EncryptedPGKeyRef, error) {
	if resources == nil || resources.Client == nil {
		return EncryptedPGKeyRef{}, fmt.Errorf("resource manager is required")
	}
	name := EncryptedPGKeySecretName(scope.Name)
	secret := &corev1.Secret{}
	err := resources.Client.Get(ctx, types.NamespacedName{Name: name, Namespace: scope.Namespace}, secret)
	if errors.IsNotFound(err) {
		key, err := generateAES256Key()
		if err != nil {
			return EncryptedPGKeyRef{}, err
		}
		secret = &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: scope.Namespace},
			Type:       corev1.SecretTypeOpaque,
			Data: map[string][]byte{
				encryptedPGKeyKey:   key,
				encryptedPGKeyIDKey: []byte(defaultKeyID),
			},
		}
		if err := scope.SetControllerReference(secret, resources.Scheme); err != nil {
			return EncryptedPGKeyRef{}, err
		}
		if err := resources.Client.Create(ctx, secret); err != nil {
			return EncryptedPGKeyRef{}, err
		}
		return EncryptedPGKeyRef{SecretName: name, Key: encryptedPGKeyKey, KeyID: defaultKeyID}, nil
	}
	if err != nil {
		return EncryptedPGKeyRef{}, err
	}

	changed := false
	if secret.Data == nil {
		secret.Data = map[string][]byte{}
	}
	if len(secret.Data[encryptedPGKeyKey]) == 0 {
		key, err := generateAES256Key()
		if err != nil {
			return EncryptedPGKeyRef{}, err
		}
		secret.Data[encryptedPGKeyKey] = key
		changed = true
	}
	keyID := strings.TrimSpace(string(secret.Data[encryptedPGKeyIDKey]))
	if keyID == "" {
		keyID = defaultKeyID
		secret.Data[encryptedPGKeyIDKey] = []byte(keyID)
		changed = true
	}
	if changed {
		if err := resources.Client.Update(ctx, secret); err != nil {
			return EncryptedPGKeyRef{}, err
		}
	}
	return EncryptedPGKeyRef{SecretName: name, Key: encryptedPGKeyKey, KeyID: keyID}, nil
}

func ApplyManagerCredentialStoreConfig(ctx context.Context, resources *common.ResourceManager, scope common.ObjectScope, cfg *apiconfig.ManagerConfig) error {
	if cfg == nil {
		return nil
	}
	infra := scope.Owner()
	keyRef, err := EnsureEncryptedPGKey(ctx, resources, scope)
	if err != nil {
		return err
	}
	cfg.CredentialStore.DefaultStorageKind = "encrypted_pg"
	if infrav1alpha1.IsCredentialVaultEnabled(infra) {
		cfg.CredentialStore.DefaultStorageKind = "hashicorp_vault"
	}
	cfg.CredentialStore.EncryptedPG.KeyID = keyRef.KeyID
	cfg.CredentialStore.EncryptedPG.KeyFile = EncryptedPGKeyFilePath
	cfg.CredentialStore.EncryptedPG.Key = ""
	if cfg.CredentialStore.EncryptedPG.BackfillOnStart == nil {
		backfill := true
		cfg.CredentialStore.EncryptedPG.BackfillOnStart = &backfill
	}

	cfg.CredentialStore.Vault.Connections = nil
	if !infrav1alpha1.IsCredentialVaultEnabled(infra) || infra.Spec.CredentialVault == nil {
		return nil
	}

	switch infra.Spec.CredentialVault.Type {
	case infrav1alpha1.CredentialVaultTypeBuiltin, "":
		builtin := resolveBuiltinCredentialVaultConfig(infra)
		cfg.CredentialStore.Vault.Connections = []apiconfig.CredentialVaultConnectionConfig{
			{
				Name:                "default",
				Provider:            "hashicorp_vault",
				Address:             builtinOpenBaoAddress(infra, builtin),
				TokenFile:           VaultTokenFilePath("default"),
				DefaultMount:        credentialVaultMount(builtin.Mount),
				KVVersion:           2,
				AllowedPathPrefixes: defaultAllowedPathPrefixes(nil),
			},
		}
	case infrav1alpha1.CredentialVaultTypeExternal:
		if infra.Spec.CredentialVault.External == nil {
			return nil
		}
		external := infra.Spec.CredentialVault.External
		conn := apiconfig.CredentialVaultConnectionConfig{
			Name:                "default",
			Provider:            "hashicorp_vault",
			Address:             strings.TrimSpace(external.Address),
			TokenFile:           VaultTokenFilePath("default"),
			Namespace:           external.Namespace,
			DefaultMount:        credentialVaultMount(external.Mount),
			KVVersion:           2,
			SkipTLSVerify:       external.SkipTLSVerify,
			AllowedPathPrefixes: defaultAllowedPathPrefixes(external.AllowedPathPrefixes),
		}
		if external.CACertSecret != nil && strings.TrimSpace(external.CACertSecret.Name) != "" {
			conn.CACertFile = VaultCAFilePath("default")
		}
		cfg.CredentialStore.Vault.Connections = []apiconfig.CredentialVaultConnectionConfig{conn}
	}
	return nil
}

func ManagerCredentialStoreVolumes(scope common.ObjectScope, cfg *apiconfig.ManagerConfig) ([]corev1.VolumeMount, []corev1.Volume) {
	if cfg == nil {
		return nil, nil
	}
	infra := scope.Owner()
	var mounts []corev1.VolumeMount
	var volumes []corev1.Volume
	addSecretFile := func(name, secretName, secretKey, mountPath string) {
		if secretName == "" || secretKey == "" || mountPath == "" {
			return
		}
		volName := volumeName(name)
		mounts = append(mounts, corev1.VolumeMount{
			Name:      volName,
			MountPath: mountPath,
			SubPath:   pathpkg.Base(mountPath),
			ReadOnly:  true,
		})
		volumes = append(volumes, corev1.Volume{
			Name: volName,
			VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{
					SecretName: secretName,
					Items: []corev1.KeyToPath{
						{Key: secretKey, Path: pathpkg.Base(mountPath)},
					},
				},
			},
		})
	}

	if cfg.CredentialStore.EncryptedPG.KeyFile == EncryptedPGKeyFilePath {
		addSecretFile("credential-store-encrypted-pg", EncryptedPGKeySecretName(scope.Name), encryptedPGKeyKey, EncryptedPGKeyFilePath)
	}

	if infra == nil || infra.Spec.CredentialVault == nil || len(cfg.CredentialStore.Vault.Connections) == 0 {
		return mounts, volumes
	}

	switch infra.Spec.CredentialVault.Type {
	case infrav1alpha1.CredentialVaultTypeBuiltin, "":
		addSecretFile("credential-vault-token-default", common.BuiltinCredentialVaultManagerTokenSecretName(scope.Name), managerTokenKey, VaultTokenFilePath("default"))
	case infrav1alpha1.CredentialVaultTypeExternal:
		if infra.Spec.CredentialVault.External == nil {
			return mounts, volumes
		}
		external := infra.Spec.CredentialVault.External
		addSecretFile("credential-vault-token-default", external.TokenSecret.Name, credentialVaultTokenKey(external.TokenSecret.Key), VaultTokenFilePath("default"))
		if external.CACertSecret != nil && strings.TrimSpace(external.CACertSecret.Name) != "" {
			addSecretFile("credential-vault-ca-default", external.CACertSecret.Name, credentialVaultCAKey(external.CACertSecret.Key), VaultCAFilePath("default"))
		}
	}
	return mounts, volumes
}

func ValidateExternalCredentialVault(ctx context.Context, c client.Client, infra *infrav1alpha1.Sandbox0Infra) error {
	if infra == nil || infra.Spec.CredentialVault == nil || infra.Spec.CredentialVault.External == nil {
		return fmt.Errorf("external credentialVault configuration is required")
	}
	external := infra.Spec.CredentialVault.External
	if strings.TrimSpace(external.Address) == "" {
		return fmt.Errorf("credentialVault.external.address is required")
	}
	parsed, err := url.Parse(external.Address)
	if err != nil {
		return fmt.Errorf("credentialVault.external.address is invalid: %w", err)
	}
	if parsed.Scheme == "" || parsed.Host == "" {
		return fmt.Errorf("credentialVault.external.address must include scheme and host")
	}
	if strings.TrimSpace(external.TokenSecret.Name) == "" {
		return fmt.Errorf("credentialVault.external.tokenSecret.name is required")
	}
	secret := &corev1.Secret{}
	if err := c.Get(ctx, types.NamespacedName{Name: external.TokenSecret.Name, Namespace: infra.Namespace}, secret); err != nil {
		return fmt.Errorf("credential vault token secret not found: %w", err)
	}
	tokenKey := credentialVaultTokenKey(external.TokenSecret.Key)
	if len(secret.Data[tokenKey]) == 0 {
		return fmt.Errorf("credential vault token secret key %q is missing", tokenKey)
	}
	if external.CACertSecret != nil && strings.TrimSpace(external.CACertSecret.Name) != "" {
		caSecret := &corev1.Secret{}
		if err := c.Get(ctx, types.NamespacedName{Name: external.CACertSecret.Name, Namespace: infra.Namespace}, caSecret); err != nil {
			return fmt.Errorf("credential vault caCertSecret not found: %w", err)
		}
		caKey := credentialVaultCAKey(external.CACertSecret.Key)
		if len(caSecret.Data[caKey]) == 0 {
			return fmt.Errorf("credential vault caCertSecret key %q is missing", caKey)
		}
	}
	return nil
}

func EncryptedPGKeySecretName(infraName string) string {
	return fmt.Sprintf("%s-credential-source-encryption", infraName)
}

func VaultTokenFilePath(connectionName string) string {
	return pathpkg.Join(vaultConnectionDir(connectionName), "token")
}

func VaultCAFilePath(connectionName string) string {
	return pathpkg.Join(vaultConnectionDir(connectionName), "ca.crt")
}

func builtinOpenBaoName(infra *infrav1alpha1.Sandbox0Infra) string {
	return fmt.Sprintf("%s-%s", infra.Name, openBaoName)
}

func builtinOpenBaoAddress(infra *infrav1alpha1.Sandbox0Infra, cfg infrav1alpha1.BuiltinCredentialVaultConfig) string {
	return fmt.Sprintf("http://%s.%s.svc:%d", builtinOpenBaoName(infra), infra.Namespace, cfg.Port)
}

func credentialVaultLabels(infra *infrav1alpha1.Sandbox0Infra) map[string]string {
	return map[string]string{
		"app.kubernetes.io/name":      openBaoName,
		"app.kubernetes.io/instance":  infra.Name,
		"app.kubernetes.io/component": componentName,
	}
}

func resolveBuiltinCredentialVaultConfig(infra *infrav1alpha1.Sandbox0Infra) infrav1alpha1.BuiltinCredentialVaultConfig {
	cfg := infrav1alpha1.BuiltinCredentialVaultConfig{
		Enabled:                true,
		Image:                  defaultOpenBaoImage,
		Port:                   defaultOpenBaoPort,
		Mount:                  defaultOpenBaoMount,
		StatefulResourcePolicy: infrav1alpha1.BuiltinStatefulResourcePolicyRetain,
	}
	if infra == nil || infra.Spec.CredentialVault == nil || infra.Spec.CredentialVault.Builtin == nil {
		return cfg
	}
	builtin := infra.Spec.CredentialVault.Builtin
	cfg.Enabled = builtin.Enabled
	cfg.Persistence = builtin.Persistence
	if builtin.Image != "" {
		cfg.Image = builtin.Image
	}
	if builtin.Port != 0 {
		cfg.Port = builtin.Port
	}
	if builtin.Mount != "" {
		cfg.Mount = builtin.Mount
	}
	if builtin.StatefulResourcePolicy != "" {
		cfg.StatefulResourcePolicy = builtin.StatefulResourcePolicy
	}
	return cfg
}

func openBaoConfig(port int32) string {
	return fmt.Sprintf(`ui = false
disable_mlock = true

storage "file" {
  path = "/openbao/data"
}

listener "tcp" {
  address = "0.0.0.0:%d"
  tls_disable = true
}
`, port)
}

func tcpProbe(port int32, initialDelay int32) *corev1.Probe {
	return &corev1.Probe{
		ProbeHandler: corev1.ProbeHandler{
			TCPSocket: &corev1.TCPSocketAction{Port: intstr.FromInt(int(port))},
		},
		InitialDelaySeconds: initialDelay,
		PeriodSeconds:       5,
	}
}

func generateAES256Key() ([]byte, error) {
	key := make([]byte, encryptedPGKeyLength)
	if _, err := rand.Read(key); err != nil {
		return nil, fmt.Errorf("generate credential encryption key: %w", err)
	}
	return key, nil
}

func credentialVaultMount(value string) string {
	value = strings.Trim(strings.TrimSpace(value), "/")
	if value == "" {
		return defaultOpenBaoMount
	}
	return value
}

func defaultAllowedPathPrefixes(values []string) []string {
	if len(values) > 0 {
		out := make([]string, 0, len(values))
		for _, value := range values {
			if strings.TrimSpace(value) != "" {
				out = append(out, value)
			}
		}
		if len(out) > 0 {
			return out
		}
	}
	return []string{"sandbox0/credential-sources/{{teamID}}/"}
}

func credentialVaultTokenKey(key string) string {
	if strings.TrimSpace(key) == "" {
		return managerTokenKey
	}
	return key
}

func credentialVaultCAKey(key string) string {
	if strings.TrimSpace(key) == "" {
		return defaultCATokenKey
	}
	return key
}

func vaultConnectionDir(connectionName string) string {
	return pathpkg.Join(vaultDir, cleanName(connectionName))
}

func cleanName(value string) string {
	value = strings.TrimSpace(strings.ToLower(value))
	if value == "" {
		return "default"
	}
	var b strings.Builder
	lastDash := false
	for _, r := range value {
		ok := (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9')
		if ok {
			b.WriteRune(r)
			lastDash = false
			continue
		}
		if !lastDash {
			b.WriteByte('-')
			lastDash = true
		}
	}
	out := strings.Trim(b.String(), "-")
	if out == "" {
		return "default"
	}
	if len(out) > 40 {
		return out[:40]
	}
	return out
}

func volumeName(name string) string {
	out := cleanName(name)
	if len(out) > 63 {
		return out[:63]
	}
	return out
}

func deleteNamespaced(ctx context.Context, c client.Client, namespace, name string, obj client.Object) error {
	if err := c.Get(ctx, types.NamespacedName{Name: name, Namespace: namespace}, obj); err != nil {
		if errors.IsNotFound(err) {
			return nil
		}
		return err
	}
	if err := c.Delete(ctx, obj); err != nil && !errors.IsNotFound(err) {
		return err
	}
	return nil
}

type openBaoAPI struct {
	address string
	client  *http.Client
}

func newOpenBaoAPI(address string) *openBaoAPI {
	return &openBaoAPI{
		address: strings.TrimRight(address, "/"),
		client:  &http.Client{Timeout: 10 * time.Second},
	}
}

func (a *openBaoAPI) initialized(ctx context.Context) (bool, error) {
	var out struct {
		Initialized bool `json:"initialized"`
	}
	if _, _, err := a.do(ctx, http.MethodGet, "/v1/sys/init", "", nil, &out, http.StatusOK); err != nil {
		return false, fmt.Errorf("read credential vault init status: %w", err)
	}
	return out.Initialized, nil
}

func (a *openBaoAPI) initialize(ctx context.Context) (rootMaterial, error) {
	var out struct {
		KeysBase64 []string `json:"keys_base64"`
		Keys       []string `json:"keys"`
		RootToken  string   `json:"root_token"`
	}
	body := map[string]any{
		"secret_shares":    1,
		"secret_threshold": 1,
	}
	if _, _, err := a.do(ctx, http.MethodPost, "/v1/sys/init", "", body, &out, http.StatusOK); err != nil {
		return rootMaterial{}, fmt.Errorf("initialize credential vault: %w", err)
	}
	unsealKey := ""
	if len(out.KeysBase64) > 0 {
		unsealKey = out.KeysBase64[0]
	} else if len(out.Keys) > 0 {
		unsealKey = out.Keys[0]
	}
	if out.RootToken == "" || unsealKey == "" {
		return rootMaterial{}, fmt.Errorf("credential vault init response missing root token or unseal key")
	}
	return rootMaterial{RootToken: out.RootToken, UnsealKey: unsealKey}, nil
}

func (a *openBaoAPI) sealed(ctx context.Context) (bool, error) {
	var out struct {
		Sealed bool `json:"sealed"`
	}
	if _, _, err := a.do(ctx, http.MethodGet, "/v1/sys/seal-status", "", nil, &out, http.StatusOK); err != nil {
		return false, fmt.Errorf("read credential vault seal status: %w", err)
	}
	return out.Sealed, nil
}

func (a *openBaoAPI) unseal(ctx context.Context, key string) error {
	var out struct {
		Sealed bool `json:"sealed"`
	}
	if _, _, err := a.do(ctx, http.MethodPost, "/v1/sys/unseal", "", map[string]any{"key": key}, &out, http.StatusOK); err != nil {
		return fmt.Errorf("unseal credential vault: %w", err)
	}
	if out.Sealed {
		return fmt.Errorf("credential vault remains sealed after unseal")
	}
	return nil
}

func (a *openBaoAPI) ensureKV2Mount(ctx context.Context, token, mount string) error {
	mount = credentialVaultMount(mount)
	var existing struct {
		Type    string            `json:"type"`
		Options map[string]string `json:"options"`
	}
	status, data, err := a.do(ctx, http.MethodGet, "/v1/sys/mounts/"+mount, token, nil, &existing, http.StatusOK, http.StatusNotFound, http.StatusBadRequest)
	if err != nil {
		return fmt.Errorf("read credential vault mount %q: %w", mount, err)
	}
	if status == http.StatusOK {
		if existing.Type != "kv" || existing.Options["version"] != "2" {
			return fmt.Errorf("credential vault mount %q exists but is not kv-v2", mount)
		}
		return nil
	}
	if status == http.StatusBadRequest && !isMissingCredentialVaultMount(data) {
		return fmt.Errorf("read credential vault mount %q: %s", mount, strings.TrimSpace(string(data)))
	}
	body := map[string]any{
		"type":    "kv",
		"options": map[string]string{"version": "2"},
	}
	if _, data, err := a.do(ctx, http.MethodPost, "/v1/sys/mounts/"+mount, token, body, nil, http.StatusNoContent, http.StatusBadRequest); err != nil {
		return fmt.Errorf("enable credential vault kv-v2 mount %q: %w", mount, err)
	} else if len(data) > 0 && strings.Contains(strings.ToLower(string(data)), "path is already in use") {
		return nil
	} else if len(data) > 0 {
		return fmt.Errorf("enable credential vault kv-v2 mount %q: %s", mount, strings.TrimSpace(string(data)))
	}
	return nil
}

func isMissingCredentialVaultMount(data []byte) bool {
	return strings.Contains(strings.ToLower(string(data)), "no secret engine mount")
}

func (a *openBaoAPI) writeManagerPolicy(ctx context.Context, token, mount string) error {
	policy := managerPolicy(credentialVaultMount(mount))
	body := map[string]string{"policy": policy}
	if _, _, err := a.do(ctx, http.MethodPut, "/v1/sys/policies/acl/"+managerPolicyName, token, body, nil, http.StatusNoContent); err != nil {
		return fmt.Errorf("write credential vault manager policy: %w", err)
	}
	return nil
}

func (a *openBaoAPI) lookupToken(ctx context.Context, rootToken, token string) bool {
	if strings.TrimSpace(token) == "" {
		return false
	}
	_, _, err := a.do(ctx, http.MethodPost, "/v1/auth/token/lookup", rootToken, map[string]string{"token": token}, nil, http.StatusOK)
	return err == nil
}

func (a *openBaoAPI) createManagerToken(ctx context.Context, rootToken string) (string, error) {
	var out struct {
		Auth struct {
			ClientToken string `json:"client_token"`
		} `json:"auth"`
	}
	body := map[string]any{
		"policies":          []string{managerPolicyName},
		"no_parent":         true,
		"no_default_policy": true,
		"display_name":      "sandbox0-manager",
		"renewable":         true,
		"ttl":               "87600h",
	}
	if _, _, err := a.do(ctx, http.MethodPost, "/v1/auth/token/create", rootToken, body, &out, http.StatusOK); err != nil {
		return "", fmt.Errorf("create credential vault manager token: %w", err)
	}
	if out.Auth.ClientToken == "" {
		return "", fmt.Errorf("credential vault token create response missing client token")
	}
	return out.Auth.ClientToken, nil
}

func (a *openBaoAPI) do(ctx context.Context, method, apiPath, token string, body any, out any, okStatuses ...int) (int, []byte, error) {
	var reader io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return 0, nil, fmt.Errorf("marshal request: %w", err)
		}
		reader = bytes.NewReader(data)
	}
	reqURL, err := joinURL(a.address, apiPath)
	if err != nil {
		return 0, nil, err
	}
	req, err := http.NewRequestWithContext(ctx, method, reqURL, reader)
	if err != nil {
		return 0, nil, err
	}
	if token != "" {
		req.Header.Set("X-Vault-Token", token)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := a.client.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	for _, ok := range okStatuses {
		if resp.StatusCode == ok {
			if out != nil && len(data) > 0 {
				if err := json.Unmarshal(data, out); err != nil {
					return resp.StatusCode, data, fmt.Errorf("decode response: %w", err)
				}
			}
			return resp.StatusCode, data, nil
		}
	}
	return resp.StatusCode, data, fmt.Errorf("vault request returned %d: %s", resp.StatusCode, strings.TrimSpace(string(data)))
}

func joinURL(baseURL, apiPath string) (string, error) {
	parsed, err := url.Parse(baseURL)
	if err != nil {
		return "", fmt.Errorf("parse vault address: %w", err)
	}
	parsed.Path = pathpkg.Join(parsed.Path, apiPath)
	return parsed.String(), nil
}

func managerPolicy(mount string) string {
	mount = credentialVaultMount(mount)
	return fmt.Sprintf(`path "%s/data/sandbox0/credential-sources/*" {
  capabilities = ["create", "update", "read"]
}

path "%s/metadata/sandbox0/credential-sources/*" {
  capabilities = ["read", "list"]
}
`, mount, mount)
}

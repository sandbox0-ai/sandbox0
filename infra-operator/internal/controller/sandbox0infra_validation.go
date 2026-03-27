package controller

import (
	"context"
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"

	infrav1alpha1 "github.com/sandbox0-ai/sandbox0/infra-operator/api/v1alpha1"
)

const (
	builtinDatabaseSecretSuffix = "sandbox0-database-credentials"
	builtinDatabasePVCSuffix    = "postgres-data"
	builtinStorageSecretSuffix  = "sandbox0-rustfs-credentials"
	builtinStoragePVCSuffix     = "rustfs-data"
	builtinRegistryPVCSuffix    = "registry-data"
	defaultDatabasePVCSize      = "20Gi"
	defaultStoragePVCSize       = "50Gi"
	defaultRegistryPVCSize      = "20Gi"
)

func (r *Sandbox0InfraReconciler) validateSpecSemantics(ctx context.Context, infra *infrav1alpha1.Sandbox0Infra) error {
	return validateSpecSemantics(ctx, r.Client, infra)
}

func validateSpecSemantics(ctx context.Context, kubeClient ctrlclient.Client, infra *infrav1alpha1.Sandbox0Infra) error {
	if infra == nil {
		return nil
	}

	var errs []error
	errs = append(errs, validateUnsupportedServiceCapabilities(infra)...)
	errs = append(errs, validatePersistenceFlags(infra)...)

	if kubeClient != nil {
		errs = append(errs, validateBuiltinDatabaseSemantics(ctx, kubeClient, infra)...)
		errs = append(errs, validateBuiltinStorageSemantics(ctx, kubeClient, infra)...)
		errs = append(errs, validateBuiltinRegistrySemantics(ctx, kubeClient, infra)...)
	}

	return utilerrors.NewAggregate(errs)
}

func validateUnsupportedServiceCapabilities(infra *infrav1alpha1.Sandbox0Infra) []error {
	if infra.Spec.Services == nil {
		return nil
	}

	var errs []error
	if netd := infra.Spec.Services.Netd; netd != nil {
		if netd.Service != nil {
			errs = append(errs, fmt.Errorf("spec.services.netd.service is not supported: netd runs as a hostNetwork DaemonSet and does not create a Service"))
		}
		if netd.Ingress != nil {
			errs = append(errs, fmt.Errorf("spec.services.netd.ingress is not supported"))
		}
		if netd.Resources != nil {
			errs = append(errs, fmt.Errorf("spec.services.netd.resources is not supported"))
		}
		if netd.Replicas > 1 {
			errs = append(errs, fmt.Errorf("spec.services.netd.replicas is not supported: netd runs as a DaemonSet"))
		}
	}
	if scheduler := infra.Spec.Services.Scheduler; scheduler != nil && scheduler.Ingress != nil {
		errs = append(errs, fmt.Errorf("spec.services.scheduler.ingress is not supported"))
	}
	if clusterGateway := infra.Spec.Services.ClusterGateway; clusterGateway != nil && clusterGateway.Ingress != nil {
		errs = append(errs, fmt.Errorf("spec.services.clusterGateway.ingress is not supported"))
	}
	if manager := infra.Spec.Services.Manager; manager != nil && manager.Ingress != nil {
		errs = append(errs, fmt.Errorf("spec.services.manager.ingress is not supported"))
	}
	if storageProxy := infra.Spec.Services.StorageProxy; storageProxy != nil && storageProxy.Ingress != nil {
		errs = append(errs, fmt.Errorf("spec.services.storageProxy.ingress is not supported"))
	}

	return errs
}

func validatePersistenceFlags(infra *infrav1alpha1.Sandbox0Infra) []error {
	var errs []error

	if infra.Spec.Database != nil && infra.Spec.Database.Type == infrav1alpha1.DatabaseTypeBuiltin &&
		infra.Spec.Database.Builtin != nil && infra.Spec.Database.Builtin.Persistence != nil &&
		!infra.Spec.Database.Builtin.Persistence.Enabled {
		errs = append(errs, fmt.Errorf("spec.database.builtin.persistence.enabled=false is not supported: builtin database persistence cannot be disabled"))
	}

	if infra.Spec.Storage != nil && infra.Spec.Storage.Type == infrav1alpha1.StorageTypeBuiltin &&
		infra.Spec.Storage.Builtin != nil && infra.Spec.Storage.Builtin.Persistence != nil &&
		!infra.Spec.Storage.Builtin.Persistence.Enabled {
		errs = append(errs, fmt.Errorf("spec.storage.builtin.persistence.enabled=false is not supported: builtin storage persistence cannot be disabled"))
	}

	if infra.Spec.Registry != nil && infra.Spec.Registry.Provider == infrav1alpha1.RegistryProviderBuiltin &&
		infra.Spec.Registry.Builtin != nil && infra.Spec.Registry.Builtin.Persistence != nil &&
		!infra.Spec.Registry.Builtin.Persistence.Enabled {
		errs = append(errs, fmt.Errorf("spec.registry.builtin.persistence.enabled=false is not supported: builtin registry persistence cannot be disabled"))
	}

	return errs
}

func validateBuiltinDatabaseSemantics(ctx context.Context, kubeClient ctrlclient.Client, infra *infrav1alpha1.Sandbox0Infra) []error {
	if infra.Spec.Database == nil || infra.Spec.Database.Type != infrav1alpha1.DatabaseTypeBuiltin || infra.Spec.Database.Builtin == nil {
		return nil
	}

	var errs []error
	builtin := infra.Spec.Database.Builtin

	secret := &corev1.Secret{}
	secretName := fmt.Sprintf("%s-%s", infra.Name, builtinDatabaseSecretSuffix)
	if err := kubeClient.Get(ctx, ctrlclient.ObjectKey{Namespace: infra.Namespace, Name: secretName}, secret); err == nil {
		if builtin.Username != "" && string(secret.Data["username"]) != "" && string(secret.Data["username"]) != builtin.Username {
			errs = append(errs, fmt.Errorf("spec.database.builtin.username cannot be changed after the builtin database credentials secret has been created"))
		}
		if builtin.Database != "" && string(secret.Data["database"]) != "" && string(secret.Data["database"]) != builtin.Database {
			errs = append(errs, fmt.Errorf("spec.database.builtin.database cannot be changed after the builtin database credentials secret has been created"))
		}
		if builtin.Port != 0 {
			currentPort := strings.TrimSpace(string(secret.Data["port"]))
			if currentPort != "" && currentPort != fmt.Sprintf("%d", builtin.Port) {
				errs = append(errs, fmt.Errorf("spec.database.builtin.port cannot be changed after the builtin database credentials secret has been created"))
			}
		}
	} else if !apierrors.IsNotFound(err) {
		errs = append(errs, fmt.Errorf("validate builtin database secret semantics: %w", err))
	}

	errs = append(errs, validatePersistencePVCSemantics(
		ctx,
		kubeClient,
		infra.Namespace,
		fmt.Sprintf("%s-%s", infra.Name, builtinDatabasePVCSuffix),
		builtin.Persistence,
		defaultDatabasePVCSize,
		"spec.database.builtin.persistence",
	)...)

	return errs
}

func validateBuiltinStorageSemantics(ctx context.Context, kubeClient ctrlclient.Client, infra *infrav1alpha1.Sandbox0Infra) []error {
	if infra.Spec.Storage == nil || infra.Spec.Storage.Type != infrav1alpha1.StorageTypeBuiltin || infra.Spec.Storage.Builtin == nil {
		return nil
	}

	var errs []error
	builtin := infra.Spec.Storage.Builtin

	secret := &corev1.Secret{}
	secretName := fmt.Sprintf("%s-%s", infra.Name, builtinStorageSecretSuffix)
	if err := kubeClient.Get(ctx, ctrlclient.ObjectKey{Namespace: infra.Namespace, Name: secretName}, secret); err == nil {
		if builtin.Credentials != nil {
			if builtin.Credentials.AccessKey != "" && string(secret.Data["RUSTFS_ACCESS_KEY"]) != "" &&
				string(secret.Data["RUSTFS_ACCESS_KEY"]) != builtin.Credentials.AccessKey {
				errs = append(errs, fmt.Errorf("spec.storage.builtin.credentials.accessKey cannot be changed after the builtin storage credentials secret has been created"))
			}
			if builtin.Credentials.SecretKey != "" && string(secret.Data["RUSTFS_SECRET_KEY"]) != "" &&
				string(secret.Data["RUSTFS_SECRET_KEY"]) != builtin.Credentials.SecretKey {
				errs = append(errs, fmt.Errorf("spec.storage.builtin.credentials.secretKey cannot be changed after the builtin storage credentials secret has been created"))
			}
		}
	} else if !apierrors.IsNotFound(err) {
		errs = append(errs, fmt.Errorf("validate builtin storage secret semantics: %w", err))
	}

	errs = append(errs, validatePersistencePVCSemantics(
		ctx,
		kubeClient,
		infra.Namespace,
		fmt.Sprintf("%s-%s", infra.Name, builtinStoragePVCSuffix),
		builtin.Persistence,
		defaultStoragePVCSize,
		"spec.storage.builtin.persistence",
	)...)

	return errs
}

func validateBuiltinRegistrySemantics(ctx context.Context, kubeClient ctrlclient.Client, infra *infrav1alpha1.Sandbox0Infra) []error {
	if infra.Spec.Registry == nil || infra.Spec.Registry.Provider != infrav1alpha1.RegistryProviderBuiltin || infra.Spec.Registry.Builtin == nil {
		return nil
	}

	return validatePersistencePVCSemantics(
		ctx,
		kubeClient,
		infra.Namespace,
		fmt.Sprintf("%s-%s", infra.Name, builtinRegistryPVCSuffix),
		infra.Spec.Registry.Builtin.Persistence,
		defaultRegistryPVCSize,
		"spec.registry.builtin.persistence",
	)
}

func validatePersistencePVCSemantics(
	ctx context.Context,
	kubeClient ctrlclient.Client,
	namespace, pvcName string,
	persistence *infrav1alpha1.PersistenceConfig,
	defaultSize string,
	fieldPath string,
) []error {
	if persistence == nil {
		return nil
	}

	var errs []error
	pvc := &corev1.PersistentVolumeClaim{}
	if err := kubeClient.Get(ctx, ctrlclient.ObjectKey{Namespace: namespace, Name: pvcName}, pvc); err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return []error{fmt.Errorf("validate %s semantics: %w", fieldPath, err)}
	}

	if !persistence.Size.IsZero() {
		currentSize := pvc.Spec.Resources.Requests[corev1.ResourceStorage]
		if currentSize.Cmp(persistence.Size) != 0 {
			errs = append(errs, fmt.Errorf("%s.size cannot be changed after the builtin PVC has been created", fieldPath))
		}
	} else {
		defaultQuantity := resource.MustParse(defaultSize)
		currentSize := pvc.Spec.Resources.Requests[corev1.ResourceStorage]
		if currentSize.Cmp(defaultQuantity) != 0 {
			errs = append(errs, fmt.Errorf("%s.size cannot be changed after the builtin PVC has been created", fieldPath))
		}
	}

	if persistence.StorageClass != "" {
		currentStorageClass := ""
		if pvc.Spec.StorageClassName != nil {
			currentStorageClass = *pvc.Spec.StorageClassName
		}
		if currentStorageClass != persistence.StorageClass {
			errs = append(errs, fmt.Errorf("%s.storageClass cannot be changed after the builtin PVC has been created", fieldPath))
		}
	}

	return errs
}

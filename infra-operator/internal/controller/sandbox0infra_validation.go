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
	"github.com/sandbox0-ai/sandbox0/infra-operator/internal/controller/pkg/common"
	"github.com/sandbox0-ai/sandbox0/pkg/naming"
)

const (
	defaultDatabasePVCSize = "20Gi"
	defaultStoragePVCSize  = "50Gi"
	defaultRegistryPVCSize = "20Gi"
	nodePortMin            = 30000
	nodePortMax            = 32767
)

func (r *Sandbox0InfraReconciler) validateSpecSemantics(ctx context.Context, infra *infrav1alpha1.Sandbox0Infra) error {
	return validateSpecSemantics(ctx, r.Client, infra)
}

func validateSpecSemantics(ctx context.Context, kubeClient ctrlclient.Client, infra *infrav1alpha1.Sandbox0Infra) error {
	if infra == nil {
		return nil
	}

	var errs []error

	if kubeClient != nil {
		errs = append(errs, validateBuiltinDatabaseSemantics(ctx, kubeClient, infra)...)
		errs = append(errs, validateBuiltinStorageSemantics(ctx, kubeClient, infra)...)
		errs = append(errs, validateBuiltinRegistrySemantics(ctx, kubeClient, infra)...)
	}
	errs = append(errs, validateClusterSemantics(infra)...)
	errs = append(errs, validateServiceSemantics(infra)...)

	return utilerrors.NewAggregate(errs)
}

func validateClusterSemantics(infra *infrav1alpha1.Sandbox0Infra) []error {
	if infra == nil || infra.Spec.Cluster == nil {
		return nil
	}
	if err := naming.ValidateClusterID(infra.Spec.Cluster.ID); err != nil {
		return []error{fmt.Errorf("spec.cluster.id is invalid: %w", err)}
	}
	return nil
}

func validateServiceSemantics(infra *infrav1alpha1.Sandbox0Infra) []error {
	if infra == nil || infra.Spec.Services == nil {
		return nil
	}

	var errs []error
	services := infra.Spec.Services
	if services.GlobalGateway != nil {
		errs = append(errs, validateServiceNetworkConfig("spec.services.globalGateway.service", services.GlobalGateway.Service)...)
	}
	if services.RegionalGateway != nil {
		errs = append(errs, validateServiceNetworkConfig("spec.services.regionalGateway.service", services.RegionalGateway.Service)...)
	}
	if services.Scheduler != nil {
		errs = append(errs, validateServiceNetworkConfig("spec.services.scheduler.service", services.Scheduler.Service)...)
	}
	if services.ClusterGateway != nil {
		errs = append(errs, validateServiceNetworkConfig("spec.services.clusterGateway.service", services.ClusterGateway.Service)...)
	}
	if services.Manager != nil {
		errs = append(errs, validateServiceNetworkConfig("spec.services.manager.service", services.Manager.Service)...)
	}
	if services.StorageProxy != nil {
		errs = append(errs, validateServiceNetworkConfig("spec.services.storageProxy.service", services.StorageProxy.Service)...)
	}

	return errs
}

func validateServiceNetworkConfig(fieldPath string, service *infrav1alpha1.ServiceNetworkConfig) []error {
	if service == nil {
		return nil
	}

	var errs []error
	if service.Type == corev1.ServiceTypeNodePort && service.Port != 0 {
		if service.Port < nodePortMin || service.Port > nodePortMax {
			errs = append(errs, fmt.Errorf("%s.port must be within %d-%d when service.type is NodePort", fieldPath, nodePortMin, nodePortMax))
		}
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
	secretName := common.BuiltinDatabaseSecretName(infra.Name)
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
		common.BuiltinDatabasePVCName(infra.Name),
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
	secretName := common.BuiltinStorageSecretName(infra.Name)
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
		common.BuiltinStoragePVCName(infra.Name),
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
		common.BuiltinRegistryPVCName(infra.Name),
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

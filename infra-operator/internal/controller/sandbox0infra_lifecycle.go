package controller

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"

	infrav1alpha1 "github.com/sandbox0-ai/sandbox0/infra-operator/api/v1alpha1"
)

func collectRetainedResources(ctx context.Context, kubeClient ctrlclient.Client, infra *infrav1alpha1.Sandbox0Infra) ([]infrav1alpha1.RetainedResourceStatus, error) {
	if infra == nil || kubeClient == nil {
		return nil, nil
	}

	var retained []infrav1alpha1.RetainedResourceStatus

	if !isBuiltinDatabaseActive(infra) && resolveDatabaseStatefulResourcePolicy(infra) == infrav1alpha1.BuiltinStatefulResourcePolicyRetain {
		if exists, err := resourceExists(ctx, kubeClient, infra.Namespace, fmt.Sprintf("%s-%s", infra.Name, builtinDatabaseSecretSuffix), &corev1.Secret{}); err != nil {
			return nil, err
		} else if exists {
			retained = append(retained, newRetainedResourceStatus("database", "Secret", fmt.Sprintf("%s-%s", infra.Name, builtinDatabaseSecretSuffix)))
		}
		if exists, err := resourceExists(ctx, kubeClient, infra.Namespace, fmt.Sprintf("%s-%s", infra.Name, builtinDatabasePVCSuffix), &corev1.PersistentVolumeClaim{}); err != nil {
			return nil, err
		} else if exists {
			retained = append(retained, newRetainedResourceStatus("database", "PersistentVolumeClaim", fmt.Sprintf("%s-%s", infra.Name, builtinDatabasePVCSuffix)))
		}
	}

	if !isBuiltinStorageActive(infra) && resolveStorageStatefulResourcePolicy(infra) == infrav1alpha1.BuiltinStatefulResourcePolicyRetain {
		if exists, err := resourceExists(ctx, kubeClient, infra.Namespace, fmt.Sprintf("%s-%s", infra.Name, builtinStorageSecretSuffix), &corev1.Secret{}); err != nil {
			return nil, err
		} else if exists {
			retained = append(retained, newRetainedResourceStatus("storage", "Secret", fmt.Sprintf("%s-%s", infra.Name, builtinStorageSecretSuffix)))
		}
		if exists, err := resourceExists(ctx, kubeClient, infra.Namespace, fmt.Sprintf("%s-%s", infra.Name, builtinStoragePVCSuffix), &corev1.PersistentVolumeClaim{}); err != nil {
			return nil, err
		} else if exists {
			retained = append(retained, newRetainedResourceStatus("storage", "PersistentVolumeClaim", fmt.Sprintf("%s-%s", infra.Name, builtinStoragePVCSuffix)))
		}
	}

	if !isBuiltinRegistryActive(infra) && resolveRegistryStatefulResourcePolicy(infra) == infrav1alpha1.BuiltinStatefulResourcePolicyRetain {
		if exists, err := resourceExists(ctx, kubeClient, infra.Namespace, fmt.Sprintf("%s-%s", infra.Name, builtinRegistryPVCSuffix), &corev1.PersistentVolumeClaim{}); err != nil {
			return nil, err
		} else if exists {
			retained = append(retained, newRetainedResourceStatus("registry", "PersistentVolumeClaim", fmt.Sprintf("%s-%s", infra.Name, builtinRegistryPVCSuffix)))
		}
	}

	return retained, nil
}

func resourceExists(ctx context.Context, kubeClient ctrlclient.Client, namespace, name string, obj ctrlclient.Object) (bool, error) {
	if err := kubeClient.Get(ctx, ctrlclient.ObjectKey{Namespace: namespace, Name: name}, obj); err != nil {
		if apierrors.IsNotFound(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func newRetainedResourceStatus(component, kind, name string) infrav1alpha1.RetainedResourceStatus {
	return infrav1alpha1.RetainedResourceStatus{
		Component: component,
		Kind:      kind,
		Name:      name,
		Policy:    infrav1alpha1.BuiltinStatefulResourcePolicyRetain,
		Reason:    "Retained during builtin lifecycle transition by statefulResourcePolicy",
	}
}

func isBuiltinDatabaseActive(infra *infrav1alpha1.Sandbox0Infra) bool {
	return infra != nil &&
		infra.Spec.Database != nil &&
		infra.Spec.Database.Type == infrav1alpha1.DatabaseTypeBuiltin &&
		resolveBuiltinDatabaseEnabled(infra)
}

func isBuiltinStorageActive(infra *infrav1alpha1.Sandbox0Infra) bool {
	return infra != nil &&
		infra.Spec.Storage != nil &&
		infra.Spec.Storage.Type == infrav1alpha1.StorageTypeBuiltin &&
		resolveBuiltinStorageEnabled(infra)
}

func isBuiltinRegistryActive(infra *infrav1alpha1.Sandbox0Infra) bool {
	return infra != nil &&
		infra.Spec.Registry != nil &&
		resolveBuiltinRegistryEnabled(infra)
}

func resolveDatabaseStatefulResourcePolicy(infra *infrav1alpha1.Sandbox0Infra) infrav1alpha1.BuiltinStatefulResourcePolicy {
	if infra == nil || infra.Spec.Database == nil || infra.Spec.Database.Builtin == nil || infra.Spec.Database.Builtin.StatefulResourcePolicy == "" {
		return infrav1alpha1.BuiltinStatefulResourcePolicyRetain
	}
	return infra.Spec.Database.Builtin.StatefulResourcePolicy
}

func resolveStorageStatefulResourcePolicy(infra *infrav1alpha1.Sandbox0Infra) infrav1alpha1.BuiltinStatefulResourcePolicy {
	if infra == nil || infra.Spec.Storage == nil || infra.Spec.Storage.Builtin == nil || infra.Spec.Storage.Builtin.StatefulResourcePolicy == "" {
		return infrav1alpha1.BuiltinStatefulResourcePolicyRetain
	}
	return infra.Spec.Storage.Builtin.StatefulResourcePolicy
}

func resolveRegistryStatefulResourcePolicy(infra *infrav1alpha1.Sandbox0Infra) infrav1alpha1.BuiltinStatefulResourcePolicy {
	if infra == nil || infra.Spec.Registry == nil || infra.Spec.Registry.Builtin == nil || infra.Spec.Registry.Builtin.StatefulResourcePolicy == "" {
		return infrav1alpha1.BuiltinStatefulResourcePolicyRetain
	}
	return infra.Spec.Registry.Builtin.StatefulResourcePolicy
}

func resolveBuiltinDatabaseEnabled(infra *infrav1alpha1.Sandbox0Infra) bool {
	if infra == nil || infra.Spec.Database == nil || infra.Spec.Database.Type != infrav1alpha1.DatabaseTypeBuiltin {
		return false
	}
	if infra.Spec.Database.Builtin == nil {
		return true
	}
	return infra.Spec.Database.Builtin.Enabled
}

func resolveBuiltinStorageEnabled(infra *infrav1alpha1.Sandbox0Infra) bool {
	if infra == nil || infra.Spec.Storage == nil || infra.Spec.Storage.Type != infrav1alpha1.StorageTypeBuiltin {
		return false
	}
	if infra.Spec.Storage.Builtin == nil {
		return true
	}
	return infra.Spec.Storage.Builtin.Enabled
}

func resolveBuiltinRegistryEnabled(infra *infrav1alpha1.Sandbox0Infra) bool {
	if infra == nil || infra.Spec.Registry == nil {
		return false
	}
	provider := infra.Spec.Registry.Provider
	if provider == "" {
		provider = infrav1alpha1.RegistryProviderBuiltin
	}
	if provider != infrav1alpha1.RegistryProviderBuiltin {
		return false
	}
	if infra.Spec.Registry.Builtin == nil {
		return true
	}
	return infra.Spec.Registry.Builtin.Enabled
}

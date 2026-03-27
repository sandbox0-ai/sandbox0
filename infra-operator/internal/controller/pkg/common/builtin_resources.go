package common

import (
	"fmt"

	infrav1alpha1 "github.com/sandbox0-ai/sandbox0/infra-operator/api/v1alpha1"
)

const (
	BuiltinDatabaseSecretSuffix = "sandbox0-database-credentials"
	BuiltinDatabasePVCSuffix    = "postgres-data"
	BuiltinStorageSecretSuffix  = "sandbox0-rustfs-credentials"
	BuiltinStoragePVCSuffix     = "rustfs-data"
	BuiltinRegistryPVCSuffix    = "registry-data"
)

func BuiltinDatabaseSecretName(infraName string) string {
	return fmt.Sprintf("%s-%s", infraName, BuiltinDatabaseSecretSuffix)
}

func BuiltinDatabasePVCName(infraName string) string {
	return fmt.Sprintf("%s-%s", infraName, BuiltinDatabasePVCSuffix)
}

func BuiltinStorageSecretName(infraName string) string {
	return fmt.Sprintf("%s-%s", infraName, BuiltinStorageSecretSuffix)
}

func BuiltinStoragePVCName(infraName string) string {
	return fmt.Sprintf("%s-%s", infraName, BuiltinStoragePVCSuffix)
}

func BuiltinRegistryPVCName(infraName string) string {
	return fmt.Sprintf("%s-%s", infraName, BuiltinRegistryPVCSuffix)
}

func NewRetainedResourceStatus(component, kind, name string) infrav1alpha1.RetainedResourceStatus {
	return infrav1alpha1.RetainedResourceStatus{
		Component: component,
		Kind:      kind,
		Name:      name,
		Policy:    infrav1alpha1.BuiltinStatefulResourcePolicyRetain,
		Reason:    "Retained during builtin lifecycle transition by statefulResourcePolicy",
	}
}

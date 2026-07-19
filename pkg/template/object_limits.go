package template

import (
	"fmt"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/pkg/egressauth"
	"github.com/sandbox0-ai/sandbox0/pkg/resourceguard"
)

const (
	// MaxObjectRequestBytes bounds template and sandbox claim JSON bodies.
	MaxObjectRequestBytes int64 = 512 << 10
	// MaxCanonicalSpecBytes bounds persisted template and claim configuration.
	MaxCanonicalSpecBytes     int64 = 256 << 10
	MaxDescriptionBytes       int64 = 8 << 10
	MaxDisplayNameBytes       int64 = 256
	MaxTagCount                     = 64
	MaxTagBytes               int64 = 128
	MaxMapItems                     = 256
	MaxMapValueBytes          int64 = 32 << 10
	MaxNetworkCollectionItems       = 256
)

// ValidateTemplateSpecSize protects every template persistence path, including
// trusted internal callers that bypass the public HTTP handlers.
func ValidateTemplateSpecSize(spec *v1alpha1.SandboxTemplateSpec) error {
	if spec == nil {
		return fmt.Errorf("template spec is required")
	}
	if err := resourceguard.String(
		"template description",
		spec.Description,
		MaxDescriptionBytes,
	); err != nil {
		return err
	}
	if err := resourceguard.String(
		"template display name",
		spec.DisplayName,
		MaxDisplayNameBytes,
	); err != nil {
		return err
	}
	if err := resourceguard.Slice("template tags", len(spec.Tags), MaxTagCount); err != nil {
		return err
	}
	for _, tag := range spec.Tags {
		if err := resourceguard.String("template tag", tag, MaxTagBytes); err != nil {
			return err
		}
	}
	if err := resourceguard.Structure(
		"template spec",
		spec,
		resourceguard.StructureLimits{
			MaxMapItems:            MaxMapItems,
			MaxSliceItems:          -1,
			MaxMapStringValueBytes: MaxMapValueBytes,
		},
	); err != nil {
		return err
	}
	if err := ValidateNetworkPolicySize(spec.Network); err != nil {
		return err
	}
	_, err := resourceguard.CanonicalJSON(
		"template spec",
		spec,
		MaxCanonicalSpecBytes,
	)
	return err
}

// ValidateNetworkPolicySize bounds all nested network collections and
// credential binding payloads.
func ValidateNetworkPolicySize(policy *v1alpha1.SandboxNetworkPolicy) error {
	if policy == nil {
		return nil
	}
	if err := ValidateCredentialBindingsSize(policy.CredentialBindings); err != nil {
		return err
	}
	return resourceguard.Structure(
		"sandbox network policy",
		policy,
		resourceguard.StructureLimits{
			MaxMapItems:            MaxMapItems,
			MaxSliceItems:          MaxNetworkCollectionItems,
			MaxMapStringValueBytes: MaxMapValueBytes,
		},
	)
}

// ValidateCredentialBindingsSize applies both per-binding and aggregate
// canonical JSON limits to public template and claim bindings.
func ValidateCredentialBindingsSize(bindings []v1alpha1.CredentialBinding) error {
	if err := resourceguard.Slice(
		"credential bindings",
		len(bindings),
		egressauth.MaxCredentialBindingCount,
	); err != nil {
		return err
	}
	for i := range bindings {
		if _, err := resourceguard.CanonicalJSON(
			fmt.Sprintf("credential binding %d", i),
			bindings[i],
			egressauth.MaxCredentialBindingBytes,
		); err != nil {
			return err
		}
	}
	_, err := resourceguard.CanonicalJSON(
		"credential bindings",
		bindings,
		egressauth.MaxCredentialBindingsBytes,
	)
	return err
}

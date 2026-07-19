package service

import (
	"fmt"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/pkg/resourceguard"
	templatepkg "github.com/sandbox0-ai/sandbox0/pkg/template"
)

// ValidateClaimRequestSize protects claim paths that bypass the manager HTTP
// handler.
func ValidateClaimRequestSize(req *ClaimRequest) error {
	if req == nil {
		return fmt.Errorf("claim request is required")
	}
	if err := resourceguard.Slice(
		"claim mounts",
		len(req.Mounts),
		templatepkg.MaxNetworkCollectionItems,
	); err != nil {
		return err
	}
	if err := ValidateSandboxConfigSize(req.Config); err != nil {
		return err
	}
	_, err := resourceguard.CanonicalJSON(
		"sandbox claim",
		req,
		templatepkg.MaxObjectRequestBytes,
	)
	return err
}

// ValidateSandboxConfigSize bounds persisted claim configuration.
func ValidateSandboxConfigSize(config *SandboxConfig) error {
	if config == nil {
		return nil
	}
	if err := validateSandboxConfigurationStructure("sandbox config", config); err != nil {
		return err
	}
	if err := templatepkg.ValidateNetworkPolicySize(config.Network); err != nil {
		return err
	}
	_, err := resourceguard.CanonicalJSON(
		"sandbox config",
		config,
		templatepkg.MaxCanonicalSpecBytes,
	)
	return err
}

// ValidateSandboxUpdateConfigSize bounds mutable sandbox configuration.
func ValidateSandboxUpdateConfigSize(config *SandboxUpdateConfig) error {
	if config == nil {
		return fmt.Errorf("sandbox config is required")
	}
	if err := validateSandboxConfigurationStructure("sandbox update config", config); err != nil {
		return err
	}
	if err := templatepkg.ValidateNetworkPolicySize(config.Network); err != nil {
		return err
	}
	_, err := resourceguard.CanonicalJSON(
		"sandbox update config",
		config,
		templatepkg.MaxCanonicalSpecBytes,
	)
	return err
}

// ValidateSandboxNetworkPolicySize bounds direct network update calls.
func ValidateSandboxNetworkPolicySize(policy *v1alpha1.SandboxNetworkPolicy) error {
	if err := templatepkg.ValidateNetworkPolicySize(policy); err != nil {
		return err
	}
	_, err := resourceguard.CanonicalJSON(
		"sandbox network policy",
		policy,
		templatepkg.MaxCanonicalSpecBytes,
	)
	return err
}

// ValidateCompiledNetworkPolicySize bounds the effective annotation after
// template, request, and platform rules have been merged.
func ValidateCompiledNetworkPolicySize(policy *v1alpha1.NetworkPolicySpec) error {
	if policy == nil {
		return nil
	}
	if err := resourceguard.Structure(
		"compiled sandbox network policy",
		policy,
		resourceguard.StructureLimits{
			MaxMapItems:            templatepkg.MaxMapItems,
			MaxSliceItems:          templatepkg.MaxNetworkCollectionItems,
			MaxMapStringValueBytes: templatepkg.MaxMapValueBytes,
		},
	); err != nil {
		return err
	}
	_, err := resourceguard.CanonicalJSON(
		"compiled sandbox network policy",
		policy,
		templatepkg.MaxCanonicalSpecBytes,
	)
	return err
}

func validateSandboxConfigurationStructure(resource string, value any) error {
	return resourceguard.Structure(
		resource,
		value,
		resourceguard.StructureLimits{
			MaxMapItems:            templatepkg.MaxMapItems,
			MaxSliceItems:          templatepkg.MaxNetworkCollectionItems,
			MaxMapStringValueBytes: templatepkg.MaxMapValueBytes,
		},
	)
}

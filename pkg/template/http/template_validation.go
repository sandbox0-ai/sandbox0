package http

import (
	"fmt"
	"net"
	"strings"

	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"github.com/sandbox0-ai/sandbox0/pkg/naming"
	"github.com/sandbox0-ai/sandbox0/pkg/quantity"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsimporter"
	v1alpha1 "github.com/sandbox0-ai/sandbox0/pkg/sandboxspec"
	s0template "github.com/sandbox0-ai/sandbox0/pkg/template"
)

func validateTemplateSpecForClaims(
	spec v1alpha1.SandboxTemplateSpec,
	claims *internalauth.Claims,
	resourcePolicy s0template.ResourcePolicy,
) error {
	isSystem := claims != nil && claims.IsSystemToken()
	subject := "team-owned template"
	if isSystem {
		subject = "system template"
	}
	if err := resourcePolicy.ValidateTemplate(spec, subject); err != nil {
		return err
	}
	return nil
}

func validateTemplateImagesForClaims(spec v1alpha1.SandboxTemplateSpec, claims *internalauth.Claims, privateRegistryHosts []string) error {
	if _, err := rootfsimporter.PinnedSourceDigest(spec.MainContainer.Image); err != nil {
		return fmt.Errorf("spec.mainContainer.image must be a normalized digest-pinned SHA-256 OCI reference: %w", err)
	}
	if claims == nil || claims.IsSystemToken() || strings.TrimSpace(claims.TeamID) == "" {
		return nil
	}
	if err := validateImageOwnershipForTeam(spec.MainContainer.Image, "spec.mainContainer.image", claims.TeamID, privateRegistryHosts); err != nil {
		return err
	}
	return nil
}

func validateImageOwnershipForTeam(imageRef, field, teamID string, privateRegistryHosts []string) error {
	if err := naming.ValidateTeamScopedImageReference(imageRef, teamID, privateRegistryHosts); err != nil {
		return fmt.Errorf("%s %w", field, err)
	}
	return nil
}

func validateTemplateSpec(spec v1alpha1.SandboxTemplateSpec) error {
	if strings.TrimSpace(spec.MainContainer.Image) == "" {
		return fmt.Errorf("spec.mainContainer.image is required")
	}
	if _, err := rootfsimporter.PinnedSourceDigest(spec.MainContainer.Image); err != nil {
		return fmt.Errorf("spec.mainContainer.image must be a normalized digest-pinned SHA-256 OCI reference: %w", err)
	}
	memory, err := quantity.Parse(strings.TrimSpace(spec.MainContainer.Resources.Memory))
	if err != nil || memory.Sign() <= 0 {
		return fmt.Errorf("spec.mainContainer.resources.memory must be > 0")
	}
	cpu, err := quantity.Parse(strings.TrimSpace(spec.MainContainer.Resources.CPU))
	if err != nil || cpu.Sign() <= 0 {
		return fmt.Errorf("derived spec.mainContainer.resources.cpu must be > 0")
	}
	if _, err := s0template.ResolveRootFSLogicalSize(spec); err != nil {
		return err
	}
	if _, ok := v1alpha1.EffectiveSandboxSecurityClass(spec.MainContainer.SecurityClass); !ok {
		return fmt.Errorf("spec.mainContainer.securityClass must be one of: standard, privileged")
	}
	if _, err := s0template.ResolveEphemeralMounts(spec); err != nil {
		return err
	}

	if spec.Network != nil {
		if spec.Network.Mode != v1alpha1.NetworkModeAllowAll && spec.Network.Mode != v1alpha1.NetworkModeBlockAll {
			return fmt.Errorf("spec.network.mode must be one of: allow-all, block-all")
		}
		if spec.Network.Egress != nil {
			if err := validateCIDRs(spec.Network.Egress.AllowedCIDRs, "spec.network.egress.allowedCidrs"); err != nil {
				return err
			}
			if err := validateCIDRs(spec.Network.Egress.DeniedCIDRs, "spec.network.egress.deniedCidrs"); err != nil {
				return err
			}
			if err := validatePorts(spec.Network.Egress.AllowedPorts, "spec.network.egress.allowedPorts"); err != nil {
				return err
			}
			if err := validatePorts(spec.Network.Egress.DeniedPorts, "spec.network.egress.deniedPorts"); err != nil {
				return err
			}
		}
		if err := v1alpha1.ValidateSandboxNetworkPolicy(spec.Network, spec.Network.CredentialBindings); err != nil {
			return fmt.Errorf("spec.network: %w", err)
		}
	}

	return nil
}

func validateTemplateClaimNameBudget(templateID string, spec v1alpha1.SandboxTemplateSpec) error {
	sandboxName, err := naming.SandboxNameForOperation(naming.DefaultClusterID, templateID, "name-budget-validation")
	if err != nil {
		return fmt.Errorf("template_id cannot generate claimable sandbox names: %w", err)
	}
	if _, err := naming.BuildExposureHostLabel(sandboxName, 65535); err != nil {
		return fmt.Errorf("template_id cannot generate claimable sandbox exposure labels: %w", err)
	}
	return nil
}

func validateCIDRs(values []string, field string) error {
	for i, value := range values {
		cidr := strings.TrimSpace(value)
		if cidr == "" {
			return fmt.Errorf("%s[%d] must not be empty", field, i)
		}
		if _, _, err := net.ParseCIDR(cidr); err != nil {
			return fmt.Errorf("%s[%d] must be valid CIDR: %w", field, i, err)
		}
	}
	return nil
}

func validatePorts(values []v1alpha1.PortSpec, field string) error {
	for i, port := range values {
		if port.Port < 1 || port.Port > 65535 {
			return fmt.Errorf("%s[%d].port must be between 1 and 65535", field, i)
		}
		if port.EndPort != nil {
			if *port.EndPort < port.Port || *port.EndPort > 65535 {
				return fmt.Errorf("%s[%d].endPort must be between port and 65535", field, i)
			}
		}
		protocol := strings.TrimSpace(port.Protocol)
		if protocol != "" && !strings.EqualFold(protocol, "tcp") && !strings.EqualFold(protocol, "udp") {
			return fmt.Errorf("%s[%d].protocol must be tcp or udp", field, i)
		}
	}
	return nil
}

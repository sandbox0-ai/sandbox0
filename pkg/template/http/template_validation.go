package http

import (
	"fmt"
	"net"
	"strings"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
)

func validateTemplateSpecForClaims(spec v1alpha1.SandboxTemplateSpec, claims *internalauth.Claims) error {
	if claims != nil && claims.IsSystemToken() {
		return nil
	}

	if spec.Pod != nil {
		return fmt.Errorf("spec.pod requires system identity")
	}
	if spec.MainContainer.SecurityContext != nil {
		return fmt.Errorf("spec.mainContainer.securityContext requires system identity")
	}
	if strings.TrimSpace(spec.MainContainer.ImagePullPolicy) != "" {
		return fmt.Errorf("spec.mainContainer.imagePullPolicy requires system identity")
	}
	if len(spec.Sidecars) > 0 {
		return fmt.Errorf("spec.sidecars requires system identity")
	}
	if spec.RuntimeClassName != nil {
		return fmt.Errorf("spec.runtimeClassName requires system identity")
	}
	if spec.ClusterId != nil {
		return fmt.Errorf("spec.clusterId requires system identity")
	}
	return nil
}

func validateTemplateSpec(spec v1alpha1.SandboxTemplateSpec) error {
	if strings.TrimSpace(spec.MainContainer.Image) == "" {
		return fmt.Errorf("spec.mainContainer.image is required")
	}
	if spec.MainContainer.Resources.CPU.Sign() <= 0 {
		return fmt.Errorf("spec.mainContainer.resources.cpu must be > 0")
	}
	if spec.MainContainer.Resources.Memory.Sign() <= 0 {
		return fmt.Errorf("spec.mainContainer.resources.memory must be > 0")
	}

	if spec.Pool.MinIdle < 0 {
		return fmt.Errorf("spec.pool.minIdle must be >= 0")
	}
	if spec.Pool.MaxIdle < 0 {
		return fmt.Errorf("spec.pool.maxIdle must be >= 0")
	}
	if spec.Pool.MaxIdle < spec.Pool.MinIdle {
		return fmt.Errorf("spec.pool.maxIdle must be >= spec.pool.minIdle")
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

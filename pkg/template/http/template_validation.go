package http

import (
	"fmt"
	"net"
	"path/filepath"
	"strings"

	config "github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"github.com/sandbox0-ai/sandbox0/pkg/naming"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
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
	if spec.ClusterId != nil {
		return fmt.Errorf("spec.clusterId requires system identity")
	}
	if err := validateTeamTemplateResourceRatio(spec); err != nil {
		return err
	}
	return nil
}

func validateTemplateImagesForClaims(spec v1alpha1.SandboxTemplateSpec, claims *internalauth.Claims, privateRegistryHosts []string) error {
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
	if spec.MainContainer.Resources.CPU.Sign() <= 0 {
		return fmt.Errorf("spec.mainContainer.resources.cpu must be > 0")
	}
	if spec.MainContainer.Resources.Memory.Sign() <= 0 {
		return fmt.Errorf("spec.mainContainer.resources.memory must be > 0")
	}
	if err := validateWarmProcesses(spec.WarmProcesses); err != nil {
		return err
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

func validateWarmProcesses(processes []v1alpha1.WarmProcessSpec) error {
	for i, proc := range processes {
		field := fmt.Sprintf("spec.warmProcesses[%d]", i)
		switch proc.Type {
		case v1alpha1.WarmProcessTypeREPL:
			if len(proc.Command) > 0 {
				return fmt.Errorf("%s.command is only valid for cmd warm processes", field)
			}
		case v1alpha1.WarmProcessTypeCMD:
			if len(proc.Command) == 0 {
				return fmt.Errorf("%s.command is required for cmd warm processes", field)
			}
			if strings.TrimSpace(proc.Command[0]) == "" {
				return fmt.Errorf("%s.command[0] is required", field)
			}
		default:
			return fmt.Errorf("%s.type must be one of: repl, cmd", field)
		}
		if strings.TrimSpace(proc.CWD) != "" {
			cleanCWD, err := validateAbsoluteMountPath(proc.CWD, field+".cwd")
			if err != nil {
				return err
			}
			if err := validateReservedMountPath(cleanCWD, field+".cwd"); err != nil {
				return err
			}
		}
	}
	return nil
}

func validateAbsoluteMountPath(value, field string) (string, error) {
	cleaned := filepath.Clean(strings.TrimSpace(value))
	if !filepath.IsAbs(cleaned) || cleaned == string(filepath.Separator) || strings.Contains(cleaned, "..") {
		return "", fmt.Errorf("%s is invalid", field)
	}
	return cleaned, nil
}

func validateReservedMountPath(path, field string) error {
	reserved := []string{
		"/procd/bin",
		"/config",
		"/var/run/sandbox0/netd",
	}
	for _, prefix := range reserved {
		if path == prefix || strings.HasPrefix(path, prefix+string(filepath.Separator)) {
			return fmt.Errorf("%s uses reserved path %q", field, prefix)
		}
	}
	return nil
}

func validateTeamTemplateResourceRatio(spec v1alpha1.SandboxTemplateSpec) error {
	memoryPerCPU := configuredTeamTemplateMemoryPerCPU()
	totalCPU := spec.MainContainer.Resources.CPU.DeepCopy()
	totalMemory := spec.MainContainer.Resources.Memory.DeepCopy()
	requiredMemory := memoryForCPU(totalCPU, memoryPerCPU)
	if totalMemory.Cmp(requiredMemory) != 0 {
		return fmt.Errorf(
			"team-owned template total memory must equal total cpu * %s (got cpu=%s memory=%s expectedMemory=%s)",
			memoryPerCPU.String(),
			totalCPU.String(),
			totalMemory.String(),
			requiredMemory.String(),
		)
	}
	return nil
}

func configuredTeamTemplateMemoryPerCPU() resource.Quantity {
	defaultQuantity := resource.MustParse("4Gi")
	cfg := config.LoadManagerConfig()
	if cfg == nil {
		return defaultQuantity
	}
	parsed, err := resource.ParseQuantity(strings.TrimSpace(cfg.TeamTemplateMemoryPerCPU))
	if err != nil || parsed.Sign() <= 0 {
		return defaultQuantity
	}
	return parsed
}

func memoryForCPU(cpu, memoryPerCPU resource.Quantity) resource.Quantity {
	requiredBytes := cpu.MilliValue() * memoryPerCPU.Value() / 1000
	return *resource.NewQuantity(requiredBytes, resource.BinarySI)
}

func validateProbe(probe *corev1.Probe, field string) error {
	if probe == nil {
		return nil
	}
	handlerCount := 0
	if probe.Exec != nil {
		handlerCount++
	}
	if probe.HTTPGet != nil {
		handlerCount++
	}
	if probe.TCPSocket != nil {
		handlerCount++
	}
	if probe.GRPC != nil {
		handlerCount++
	}
	if handlerCount != 1 {
		return fmt.Errorf("%s must define exactly one handler", field)
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

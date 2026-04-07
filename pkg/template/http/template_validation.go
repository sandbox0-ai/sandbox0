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
	usesSharedVolumes := spec.UsesSharedVolumes()

	if spec.Pod != nil {
		return fmt.Errorf("spec.pod requires system identity")
	}
	if spec.MainContainer.SecurityContext != nil {
		return fmt.Errorf("spec.mainContainer.securityContext requires system identity")
	}
	if strings.TrimSpace(spec.MainContainer.ImagePullPolicy) != "" {
		return fmt.Errorf("spec.mainContainer.imagePullPolicy requires system identity")
	}
	if spec.RuntimeClassName != nil && !usesSharedVolumes {
		return fmt.Errorf("spec.runtimeClassName requires system identity")
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
	for i, sidecar := range spec.Sidecars {
		field := fmt.Sprintf("spec.sidecars[%d].image", i)
		if err := validateImageOwnershipForTeam(sidecar.Image, field, claims.TeamID, privateRegistryHosts); err != nil {
			return err
		}
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
	if err := validateSidecars(spec.Sidecars); err != nil {
		return err
	}
	if err := validateSharedVolumes(spec.SharedVolumes); err != nil {
		return err
	}
	if err := validateContainerMounts(spec.Sidecars, spec.SharedVolumes); err != nil {
		return err
	}
	if spec.UsesSharedVolumes() {
		runtimeClassName := ""
		if spec.RuntimeClassName != nil {
			runtimeClassName = strings.TrimSpace(*spec.RuntimeClassName)
		}
		if runtimeClassName == "" {
			return fmt.Errorf("spec.sharedVolumes requires spec.runtimeClassName to reference a Kata runtime")
		}
		if !isKataRuntimeClassName(runtimeClassName) {
			return fmt.Errorf("spec.runtimeClassName must reference a Kata runtime when spec.sharedVolumes is set")
		}
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

func validateSidecars(sidecars []v1alpha1.SidecarContainerSpec) error {
	seenNames := make(map[string]struct{}, len(sidecars))
	for i, sidecar := range sidecars {
		field := fmt.Sprintf("spec.sidecars[%d]", i)
		if strings.TrimSpace(sidecar.Name) == "" {
			return fmt.Errorf("%s.name is required", field)
		}
		if sidecar.Name == "procd" {
			return fmt.Errorf("%s.name must not be \"procd\"", field)
		}
		if _, exists := seenNames[sidecar.Name]; exists {
			return fmt.Errorf("duplicate sidecar name %q", sidecar.Name)
		}
		seenNames[sidecar.Name] = struct{}{}
		if strings.TrimSpace(sidecar.Image) == "" {
			return fmt.Errorf("%s.image is required", field)
		}
		if sidecar.Resources.CPU.Sign() <= 0 {
			return fmt.Errorf("%s.resources.cpu must be > 0", field)
		}
		if sidecar.Resources.Memory.Sign() <= 0 {
			return fmt.Errorf("%s.resources.memory must be > 0", field)
		}
		if err := validateProbe(sidecar.ReadinessProbe, field+".readinessProbe"); err != nil {
			return err
		}
		if err := validateProbe(sidecar.LivenessProbe, field+".livenessProbe"); err != nil {
			return err
		}
		if err := validateProbe(sidecar.StartupProbe, field+".startupProbe"); err != nil {
			return err
		}
	}
	return nil
}

func validateSharedVolumes(volumes []v1alpha1.SharedVolumeSpec) error {
	seenNames := make(map[string]struct{}, len(volumes))
	seenVolumeIDs := make(map[string]struct{}, len(volumes))
	seenMountPaths := make(map[string]struct{}, len(volumes))
	for i, volume := range volumes {
		field := fmt.Sprintf("spec.sharedVolumes[%d]", i)
		if strings.TrimSpace(volume.Name) == "" {
			return fmt.Errorf("%s.name is required", field)
		}
		if _, exists := seenNames[volume.Name]; exists {
			return fmt.Errorf("duplicate shared volume name %q", volume.Name)
		}
		seenNames[volume.Name] = struct{}{}
		if strings.TrimSpace(volume.SandboxVolumeID) == "" {
			return fmt.Errorf("%s.sandboxVolumeId is required", field)
		}
		if _, exists := seenVolumeIDs[volume.SandboxVolumeID]; exists {
			return fmt.Errorf("duplicate shared volume sandboxVolumeId %q", volume.SandboxVolumeID)
		}
		seenVolumeIDs[volume.SandboxVolumeID] = struct{}{}

		cleanMountPath, err := validateAbsoluteMountPath(volume.MountPath, field+".mountPath")
		if err != nil {
			return err
		}
		if err := validateReservedMountPath(cleanMountPath, field+".mountPath"); err != nil {
			return err
		}
		if _, exists := seenMountPaths[cleanMountPath]; exists {
			return fmt.Errorf("duplicate shared volume mountPath %q", cleanMountPath)
		}
		seenMountPaths[cleanMountPath] = struct{}{}
	}
	return nil
}

func validateContainerMounts(sidecars []v1alpha1.SidecarContainerSpec, volumes []v1alpha1.SharedVolumeSpec) error {
	knownVolumes := make(map[string]struct{}, len(volumes))
	for _, volume := range volumes {
		knownVolumes[volume.Name] = struct{}{}
	}

	for i, sidecar := range sidecars {
		seenMountNames := make(map[string]struct{}, len(sidecar.Mounts))
		seenMountPaths := make(map[string]struct{}, len(sidecar.Mounts))
		for j, mount := range sidecar.Mounts {
			field := fmt.Sprintf("spec.sidecars[%d].mounts[%d]", i, j)
			if strings.TrimSpace(mount.Name) == "" {
				return fmt.Errorf("%s.name is required", field)
			}
			if _, ok := knownVolumes[mount.Name]; !ok {
				return fmt.Errorf("%s.name references unknown shared volume %q", field, mount.Name)
			}
			if _, exists := seenMountNames[mount.Name]; exists {
				return fmt.Errorf("duplicate sidecar mount name %q in spec.sidecars[%d]", mount.Name, i)
			}
			seenMountNames[mount.Name] = struct{}{}

			cleanMountPath, err := validateAbsoluteMountPath(mount.MountPath, field+".mountPath")
			if err != nil {
				return err
			}
			if err := validateReservedMountPath(cleanMountPath, field+".mountPath"); err != nil {
				return err
			}
			if _, exists := seenMountPaths[cleanMountPath]; exists {
				return fmt.Errorf("duplicate sidecar mountPath %q in spec.sidecars[%d]", cleanMountPath, i)
			}
			seenMountPaths[cleanMountPath] = struct{}{}
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
		"/config/internal_jwt_public.key",
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
	for _, sidecar := range spec.Sidecars {
		totalCPU.Add(sidecar.Resources.CPU)
		totalMemory.Add(sidecar.Resources.Memory)
	}
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

func isKataRuntimeClassName(runtimeClassName string) bool {
	trimmed := strings.ToLower(strings.TrimSpace(runtimeClassName))
	return trimmed != "" && strings.Contains(trimmed, "kata")
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

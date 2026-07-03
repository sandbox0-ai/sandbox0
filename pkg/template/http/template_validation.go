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
	s0template "github.com/sandbox0-ai/sandbox0/pkg/template"
	"github.com/sandbox0-ai/sandbox0/pkg/volumeportal"
	"k8s.io/apimachinery/pkg/api/resource"
)

func validateTemplateSpecForClaims(spec v1alpha1.SandboxTemplateSpec, claims *internalauth.Claims) error {
	isSystem := claims != nil && claims.IsSystemToken()
	if !isSystem {
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
	}

	subject := "team-owned template"
	if isSystem {
		subject = "system template"
	}
	if err := validateTemplateResourceRatio(spec, subject); err != nil {
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
	if spec.MainContainer.Resources.EphemeralStorage.Sign() < 0 {
		return fmt.Errorf("spec.mainContainer.resources.ephemeralStorage must be >= 0")
	}
	if err := validateSecurityContext(spec.MainContainer.SecurityContext, "spec.mainContainer.securityContext"); err != nil {
		return err
	}
	if err := validateVolumeMounts(spec.VolumeMounts); err != nil {
		return err
	}
	if spec.Pod != nil {
		if err := validateEmptyDirMounts(spec.Pod.EmptyDirMounts, spec.VolumeMounts); err != nil {
			return err
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
		if err := v1alpha1.ValidateSandboxNetworkPolicy(spec.Network, spec.Network.CredentialBindings); err != nil {
			return fmt.Errorf("spec.network: %w", err)
		}
	}

	return nil
}

func validateSecurityContext(sc *v1alpha1.SecurityContext, field string) error {
	if sc == nil {
		return nil
	}
	if sc.Capabilities != nil {
		if err := validateCapabilities(sc.Capabilities.Add, field+".capabilities.add"); err != nil {
			return err
		}
		if err := validateCapabilities(sc.Capabilities.Drop, field+".capabilities.drop"); err != nil {
			return err
		}
	}
	if err := validateSeccompProfile(sc.SeccompProfile, field+".seccompProfile"); err != nil {
		return err
	}
	if err := validateAppArmorProfile(sc.AppArmorProfile, field+".appArmorProfile"); err != nil {
		return err
	}
	return nil
}

func validateCapabilities(caps []string, field string) error {
	for i, cap := range caps {
		if strings.TrimSpace(cap) == "" {
			return fmt.Errorf("%s[%d] is required", field, i)
		}
	}
	return nil
}

func validateSeccompProfile(profile *v1alpha1.SeccompProfile, field string) error {
	if profile == nil {
		return nil
	}
	switch profile.Type {
	case v1alpha1.SeccompProfileTypeUnconfined, v1alpha1.SeccompProfileTypeRuntimeDefault:
		if profile.LocalhostProfile != nil {
			return fmt.Errorf("%s.localhostProfile must be omitted unless type is Localhost", field)
		}
	case v1alpha1.SeccompProfileTypeLocalhost:
		if profile.LocalhostProfile == nil || strings.TrimSpace(*profile.LocalhostProfile) == "" {
			return fmt.Errorf("%s.localhostProfile is required when type is Localhost", field)
		}
	default:
		return fmt.Errorf("%s.type must be one of: Unconfined, RuntimeDefault, Localhost", field)
	}
	return nil
}

func validateAppArmorProfile(profile *v1alpha1.AppArmorProfile, field string) error {
	if profile == nil {
		return nil
	}
	switch profile.Type {
	case v1alpha1.AppArmorProfileTypeUnconfined, v1alpha1.AppArmorProfileTypeRuntimeDefault:
		if profile.LocalhostProfile != nil {
			return fmt.Errorf("%s.localhostProfile must be omitted unless type is Localhost", field)
		}
	case v1alpha1.AppArmorProfileTypeLocalhost:
		if profile.LocalhostProfile == nil || strings.TrimSpace(*profile.LocalhostProfile) == "" {
			return fmt.Errorf("%s.localhostProfile is required when type is Localhost", field)
		}
	default:
		return fmt.Errorf("%s.type must be one of: Unconfined, RuntimeDefault, Localhost", field)
	}
	return nil
}

func validateVolumeMounts(mounts []v1alpha1.VolumeMountSpec) error {
	seenNames := make(map[string]struct{}, len(mounts))
	seenPaths := make(map[string]struct{}, len(mounts))
	for i, mount := range mounts {
		field := fmt.Sprintf("spec.volumeMounts[%d]", i)
		name := strings.TrimSpace(mount.Name)
		if name == "" {
			return fmt.Errorf("%s.name is required", field)
		}
		if _, ok := seenNames[name]; ok {
			return fmt.Errorf("%s.name %q is duplicated", field, name)
		}
		seenNames[name] = struct{}{}

		mountPath := strings.TrimSpace(mount.MountPath)
		cleanMountPath := filepath.Clean(mountPath)
		if mountPath == "" || mountPath != cleanMountPath || !filepath.IsAbs(cleanMountPath) || cleanMountPath == string(filepath.Separator) {
			return fmt.Errorf("%s.mountPath is invalid", field)
		}
		if cleanMountPath == volumeportal.WebhookStateMountPath || strings.HasPrefix(cleanMountPath, volumeportal.WebhookStateMountPath+string(filepath.Separator)) {
			return fmt.Errorf("%s.mountPath uses a sandbox0 reserved path", field)
		}
		if _, ok := seenPaths[cleanMountPath]; ok {
			return fmt.Errorf("%s.mountPath %q is duplicated", field, cleanMountPath)
		}
		seenPaths[cleanMountPath] = struct{}{}
	}
	return nil
}

func validateEmptyDirMounts(mounts []v1alpha1.EmptyDirMountSpec, volumeMounts []v1alpha1.VolumeMountSpec) error {
	seenPaths := make(map[string]string, len(mounts)+len(volumeMounts))
	for i, mount := range volumeMounts {
		mountPath := strings.TrimSpace(mount.MountPath)
		if mountPath == "" {
			continue
		}
		cleanMountPath := filepath.Clean(mountPath)
		seenPaths[cleanMountPath] = fmt.Sprintf("spec.volumeMounts[%d]", i)
	}

	for i, mount := range mounts {
		field := fmt.Sprintf("spec.pod.emptyDirMounts[%d]", i)
		mountPath := strings.TrimSpace(mount.MountPath)
		cleanMountPath := filepath.Clean(mountPath)
		if mountPath == "" || mountPath != cleanMountPath || !filepath.IsAbs(cleanMountPath) || cleanMountPath == string(filepath.Separator) {
			return fmt.Errorf("%s.mountPath is invalid", field)
		}
		if err := validateReservedMountPath(cleanMountPath, field+".mountPath"); err != nil {
			return err
		}
		if existingField, ok := seenPaths[cleanMountPath]; ok {
			return fmt.Errorf("%s.mountPath %q duplicates %s.mountPath", field, cleanMountPath, existingField)
		}
		seenPaths[cleanMountPath] = field
		if mount.SizeLimit != nil && mount.SizeLimit.Sign() <= 0 {
			return fmt.Errorf("%s.sizeLimit must be > 0", field)
		}
	}
	return nil
}

func validateTemplateClaimNameBudget(scope, teamID, templateID string, spec v1alpha1.SandboxTemplateSpec) error {
	clusterTemplateID := naming.TemplateNameForCluster(scope, teamID, templateID)
	clusterID := naming.ClusterIDOrDefault(spec.ClusterId)
	sandboxName, err := naming.SandboxName(clusterID, clusterTemplateID, strings.Repeat("a", 5))
	if err != nil {
		return fmt.Errorf("template_id cannot generate claimable sandbox names: %w", err)
	}
	if _, err := naming.BuildExposureHostLabel(sandboxName, 65535); err != nil {
		return fmt.Errorf("template_id cannot generate claimable sandbox exposure labels: %w", err)
	}
	return nil
}

func validateReservedMountPath(path, field string) error {
	reserved := []string{
		"/procd-image",
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

func validateTemplateResourceRatio(spec v1alpha1.SandboxTemplateSpec, subject string) error {
	return s0template.ValidateResourceRatio(spec, configuredTemplateMemoryPerCPU(), subject)
}

func configuredTemplateMemoryPerCPU() resource.Quantity {
	cfg := config.LoadManagerConfig()
	if cfg == nil {
		return s0template.MemoryPerCPUOrDefault("")
	}
	return s0template.MemoryPerCPUOrDefault(cfg.TeamTemplateMemoryPerCPU)
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

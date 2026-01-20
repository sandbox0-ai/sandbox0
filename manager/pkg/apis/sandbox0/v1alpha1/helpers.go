package v1alpha1

import (
	"sort"

	"github.com/sandbox0-ai/infra/manager/pkg/config"
	corev1 "k8s.io/api/core/v1"
)

// buildPodSpec builds a pod spec from a template
func BuildPodSpec(template *SandboxTemplate) corev1.PodSpec {
	spec := corev1.PodSpec{
		RestartPolicy: corev1.RestartPolicyNever,
		Containers:    buildContainers(template),
	}

	applyProcdInit(&spec)

	// Apply runtime class if specified
	if template.Spec.RuntimeClassName != nil {
		spec.RuntimeClassName = template.Spec.RuntimeClassName
	}

	// Apply pod-level overrides
	if template.Spec.Pod != nil {
		if template.Spec.Pod.NodeSelector != nil {
			spec.NodeSelector = template.Spec.Pod.NodeSelector
		}
		if template.Spec.Pod.ServiceAccountName != "" {
			spec.ServiceAccountName = template.Spec.Pod.ServiceAccountName
		}
	}
	return spec
}

// buildContainers builds containers from template
func buildContainers(template *SandboxTemplate) []corev1.Container {
	containers := []corev1.Container{
		buildContainer(&template.Spec.MainContainer, template),
	}

	for i := range template.Spec.Sidecars {
		containers = append(containers, template.Spec.Sidecars[i])
	}
	return containers
}

// buildContainer builds a single container
func buildContainer(spec *ContainerSpec, template *SandboxTemplate) corev1.Container {
	name := "procd"

	container := corev1.Container{
		Name:            name,
		Image:           spec.Image,
		ImagePullPolicy: corev1.PullIfNotPresent,
	}

	if spec.ImagePullPolicy != "" {
		container.ImagePullPolicy = corev1.PullPolicy(spec.ImagePullPolicy)
	}

	// Environment variables
	var envVars []corev1.EnvVar
	for k, v := range template.Spec.EnvVars {
		envVars = append(envVars, corev1.EnvVar{Name: k, Value: v})
	}
	for _, ev := range spec.Env {
		envVars = append(envVars, corev1.EnvVar{Name: ev.Name, Value: ev.Value})
	}
	envVars = appendProcdConfigEnvVars(envVars)
	container.Env = envVars
	container.Command = []string{"/procd/bin/procd"}
	container.VolumeMounts = append(container.VolumeMounts, corev1.VolumeMount{
		Name:      "procd-bin",
		MountPath: "/procd/bin",
	})

	// Security context
	if spec.SecurityContext != nil {
		container.SecurityContext = &corev1.SecurityContext{}
		if spec.SecurityContext.RunAsUser != nil {
			container.SecurityContext.RunAsUser = spec.SecurityContext.RunAsUser
		}
		if spec.SecurityContext.RunAsGroup != nil {
			container.SecurityContext.RunAsGroup = spec.SecurityContext.RunAsGroup
		}
		if spec.SecurityContext.Capabilities != nil {
			container.SecurityContext.Capabilities = &corev1.Capabilities{
				Drop: convertCapabilities(spec.SecurityContext.Capabilities.Drop),
			}
		}
	}

	return container
}

func appendProcdConfigEnvVars(envVars []corev1.EnvVar) []corev1.EnvVar {
	cfg := config.LoadConfig()
	if cfg == nil {
		return envVars
	}

	envMap := cfg.ProcdConfig.EnvMap()
	if len(envMap) == 0 {
		return envVars
	}

	keys := make([]string, 0, len(envMap))
	for key := range envMap {
		if key == "" {
			continue
		}
		keys = append(keys, key)
	}
	sort.Strings(keys)

	for _, key := range keys {
		envVars = append(envVars, corev1.EnvVar{
			Name:  key,
			Value: envMap[key],
		})
	}

	return envVars
}

func applyProcdInit(spec *corev1.PodSpec) {
	cfg := config.LoadConfig()
	managerImage := cfg.ManagerImage

	spec.Volumes = append(spec.Volumes, corev1.Volume{
		Name: "procd-bin",
		VolumeSource: corev1.VolumeSource{
			EmptyDir: &corev1.EmptyDirVolumeSource{},
		},
	})

	spec.InitContainers = append(spec.InitContainers, corev1.Container{
		Name:            "procd-init",
		Image:           managerImage,
		ImagePullPolicy: corev1.PullIfNotPresent,
		Command: []string{
			"/bin/sh",
			"-c",
			"cp /app/procd /procd/bin/procd && chmod 0755 /procd/bin/procd",
		},
		VolumeMounts: []corev1.VolumeMount{
			{
				Name:      "procd-bin",
				MountPath: "/procd/bin",
			},
		},
	})
}

func convertCapabilities(caps []string) []corev1.Capability {
	if caps == nil {
		return nil
	}
	result := make([]corev1.Capability, len(caps))
	for i, cap := range caps {
		result[i] = corev1.Capability(cap)
	}
	return result
}

// BuildEgressSpec builds EgressPolicySpec from SandboxNetworkPolicy
func BuildEgressSpec(policy *TplSandboxNetworkPolicy) *EgressPolicySpec {
	if policy == nil {
		return &EgressPolicySpec{
			DefaultAction:     "deny",
			AlwaysDeniedCIDRs: PlatformDeniedCIDRs,
			EnforceProxyPorts: []int32{80, 443},
		}
	}

	spec := &EgressPolicySpec{
		AlwaysDeniedCIDRs: PlatformDeniedCIDRs,
		EnforceProxyPorts: []int32{80, 443},
	}

	switch policy.Mode {
	case NetworkModeAllowAll:
		spec.DefaultAction = "allow"
	case NetworkModeBlockAll:
		spec.DefaultAction = "deny"
	case NetworkModeCustom:
		spec.DefaultAction = "deny" // Custom defaults to deny
	default:
		spec.DefaultAction = "deny"
	}

	if policy.Egress != nil {
		spec.AllowedCIDRs = policy.Egress.AllowedIPs
		spec.DeniedCIDRs = policy.Egress.BlockedIPs
		spec.AllowedDomains = policy.Egress.AllowedDomains
		spec.DeniedDomains = policy.Egress.BlockedDomains
	}

	return spec
}

// BuildIngressSpec builds IngressPolicySpec from SandboxNetworkPolicy
func BuildIngressSpec(policy *TplSandboxNetworkPolicy) *IngressPolicySpec {
	spec := &IngressPolicySpec{
		DefaultAction: "deny", // Always default deny for ingress
		// Allow procd port from internal-gateway
		AllowedPorts: []PortSpec{
			{Port: 49983, Protocol: "tcp"},
		},
	}

	if policy != nil && policy.Ingress != nil {
		spec.AllowedSourceCIDRs = policy.Ingress.AllowedIPs
		spec.DeniedSourceCIDRs = policy.Ingress.BlockedIPs
	}

	return spec
}

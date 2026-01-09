package v1alpha1

import (
	"fmt"

	corev1 "k8s.io/api/core/v1"
)

// buildPodSpec builds a pod spec from a template
func BuildPodSpec(template *SandboxTemplate) corev1.PodSpec {
	spec := corev1.PodSpec{
		RestartPolicy: corev1.RestartPolicyNever,
		Containers:    buildContainers(template),
	}

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
		buildContainer(&template.Spec.MainContainer, template, true),
	}

	for i := range template.Spec.Sidecars {
		containers = append(containers, template.Spec.Sidecars[i])
	}
	return containers
}

// buildContainer builds a single container
func buildContainer(spec *ContainerSpec, template *SandboxTemplate, isMain bool) corev1.Container {
	name := "procd"
	if !isMain {
		name = fmt.Sprintf("sidecar-%s", spec.Image)
	}

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
	container.Env = envVars

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

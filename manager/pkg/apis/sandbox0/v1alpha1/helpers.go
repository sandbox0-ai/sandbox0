package v1alpha1

import (
	"sort"
	"strconv"
	"strings"

	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	"github.com/sandbox0-ai/sandbox0/pkg/naming"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

const (
	procdBinVolumeName = "procd-bin"
	procdConfigVolume  = "procd-config"
	netdMITMCAVolume   = "netd-mitm-ca"
	netdMITMCACertKey  = "ca.crt"
	netdMITMCAEnvVar   = "SANDBOX0_NETD_MITM_CA_FILE"
	netdMITMCADir      = "/var/run/sandbox0/netd"
	netdMITMCACertPath = netdMITMCADir + "/mitm-ca.crt"
)

// buildPodSpec builds a pod spec from a template
func BuildPodSpec(template *SandboxTemplate, restart bool) corev1.PodSpec {
	spec := corev1.PodSpec{
		RestartPolicy: corev1.RestartPolicyNever,
		Containers:    buildContainers(template),
	}
	if restart {
		spec.RestartPolicy = corev1.RestartPolicyAlways
	}

	applySharedTemplateVolumes(&spec, template)
	applyProcdSecretVolume(&spec, template)
	applyNetdMITMCATrustMaterial(&spec)
	applyProcdInit(&spec)
	applyFuseResource(&spec)
	applyDefaultSandboxPlacement(&spec)

	if runtimeClassName := configuredSandboxRuntimeClassName(); runtimeClassName != nil {
		spec.RuntimeClassName = runtimeClassName
	}

	// Apply pod-level overrides
	if template.Spec.Pod != nil {
		if template.Spec.Pod.NodeSelector != nil {
			// Keep manager-injected placement authoritative on conflicting keys so
			// sandbox workloads cannot drift outside the shared sandbox placement.
			spec.NodeSelector = mergeNodeSelectors(template.Spec.Pod.NodeSelector, spec.NodeSelector)
		}
		spec.Tolerations = mergeTolerations(spec.Tolerations, convertTolerations(template.Spec.Pod.Tolerations))
		if template.Spec.Pod.ServiceAccountName != "" {
			spec.ServiceAccountName = template.Spec.Pod.ServiceAccountName
		}
	}
	return spec
}

func applyDefaultSandboxPlacement(spec *corev1.PodSpec) {
	if spec == nil {
		return
	}
	cfg := config.LoadManagerConfig()
	if cfg == nil {
		return
	}

	spec.NodeSelector = mergeNodeSelectors(spec.NodeSelector, cfg.SandboxPodPlacement.NodeSelector)
	spec.Tolerations = mergeTolerations(spec.Tolerations, cfg.SandboxPodPlacement.Tolerations)
}

func configuredSandboxRuntimeClassName() *string {
	cfg := config.LoadManagerConfig()
	if cfg == nil {
		return nil
	}
	runtimeClassName := strings.TrimSpace(cfg.SandboxRuntimeClassName)
	if runtimeClassName == "" {
		return nil
	}
	return &runtimeClassName
}

func mergeNodeSelectors(base, override map[string]string) map[string]string {
	switch {
	case len(base) == 0 && len(override) == 0:
		return nil
	case len(base) == 0:
		return cloneNodeSelector(override)
	case len(override) == 0:
		return cloneNodeSelector(base)
	}

	merged := cloneNodeSelector(base)
	for key, value := range override {
		merged[key] = value
	}
	return merged
}

func cloneNodeSelector(src map[string]string) map[string]string {
	if len(src) == 0 {
		return nil
	}
	cloned := make(map[string]string, len(src))
	for key, value := range src {
		cloned[key] = value
	}
	return cloned
}

func convertTolerations(in []Toleration) []corev1.Toleration {
	if len(in) == 0 {
		return nil
	}
	out := make([]corev1.Toleration, 0, len(in))
	for _, tol := range in {
		out = append(out, corev1.Toleration{
			Key:      tol.Key,
			Operator: corev1.TolerationOperator(tol.Operator),
			Value:    tol.Value,
			Effect:   corev1.TaintEffect(tol.Effect),
		})
	}
	return out
}

func mergeTolerations(base, override []corev1.Toleration) []corev1.Toleration {
	if len(base) == 0 && len(override) == 0 {
		return nil
	}

	merged := make([]corev1.Toleration, 0, len(base)+len(override))
	seen := make(map[string]struct{}, len(base)+len(override))
	appendUnique := func(items []corev1.Toleration) {
		for _, tol := range items {
			key := tolerationKey(tol)
			if _, exists := seen[key]; exists {
				continue
			}
			seen[key] = struct{}{}
			merged = append(merged, tol)
		}
	}

	appendUnique(base)
	appendUnique(override)
	return merged
}

func tolerationKey(tol corev1.Toleration) string {
	return string(tol.Operator) + "\x00" + tol.Key + "\x00" + tol.Value + "\x00" + string(tol.Effect)
}

func applyFuseResource(spec *corev1.PodSpec) {
	if spec == nil {
		return
	}

	for i := range spec.Containers {
		if spec.Containers[i].Name != "procd" {
			continue
		}

		if spec.Containers[i].Resources.Requests == nil {
			spec.Containers[i].Resources.Requests = make(corev1.ResourceList)
		}
		if spec.Containers[i].Resources.Limits == nil {
			spec.Containers[i].Resources.Limits = make(corev1.ResourceList)
		}

		fuseResource := corev1.ResourceName("sandbox0.ai/fuse")
		fuseQuantity := resource.MustParse("1")
		if _, exists := spec.Containers[i].Resources.Requests[fuseResource]; !exists {
			spec.Containers[i].Resources.Requests[fuseResource] = fuseQuantity
		}
		if _, exists := spec.Containers[i].Resources.Limits[fuseResource]; !exists {
			spec.Containers[i].Resources.Limits[fuseResource] = fuseQuantity
		}
	}
}

// buildContainers builds containers from template
func buildContainers(template *SandboxTemplate) []corev1.Container {
	containers := []corev1.Container{
		buildContainer(&template.Spec.MainContainer, template),
	}

	for _, sidecar := range template.Spec.Sidecars {
		containers = append(containers, buildSidecarContainer(&sidecar, template))
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
		Resources:       buildResourceRequirements(spec.Resources),
	}

	if spec.ImagePullPolicy != "" {
		container.ImagePullPolicy = corev1.PullPolicy(spec.ImagePullPolicy)
	}

	// Environment variables
	var envVars []corev1.EnvVar
	for k, v := range template.Spec.EnvVars {
		envVars = append(envVars, corev1.EnvVar{Name: k, Value: v})
	}
	sort.Slice(envVars, func(i, j int) bool {
		if envVars[i].Name != envVars[j].Name {
			return envVars[i].Name < envVars[j].Name
		}
		return envVars[i].Value < envVars[j].Value
	})
	for _, ev := range spec.Env {
		envVars = append(envVars, corev1.EnvVar{Name: ev.Name, Value: ev.Value})
	}
	envVars = appendProcdConfigEnvVars(envVars)
	container.Env = envVars
	container.Command = []string{"/procd/bin/procd"}
	container.VolumeMounts = append(container.VolumeMounts, corev1.VolumeMount{
		Name:      procdBinVolumeName,
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
	if container.SecurityContext == nil {
		container.SecurityContext = &corev1.SecurityContext{}
	}
	if container.SecurityContext.Capabilities == nil {
		container.SecurityContext.Capabilities = &corev1.Capabilities{}
	}
	container.SecurityContext.Capabilities.Add = append(container.SecurityContext.Capabilities.Add, corev1.Capability("SYS_ADMIN"))
	if template.Spec.UsesSharedVolumes() {
		privileged := true
		container.SecurityContext.Privileged = &privileged
	}

	return container
}

func buildSidecarContainer(spec *SidecarContainerSpec, template *SandboxTemplate) corev1.Container {
	container := corev1.Container{
		Name:            spec.Name,
		Image:           spec.Image,
		ImagePullPolicy: corev1.PullIfNotPresent,
		Command:         append([]string(nil), spec.Command...),
		Args:            append([]string(nil), spec.Args...),
		Resources:       buildResourceRequirements(spec.Resources),
	}

	var envVars []corev1.EnvVar
	for k, v := range template.Spec.EnvVars {
		envVars = append(envVars, corev1.EnvVar{Name: k, Value: v})
	}
	sort.Slice(envVars, func(i, j int) bool {
		if envVars[i].Name != envVars[j].Name {
			return envVars[i].Name < envVars[j].Name
		}
		return envVars[i].Value < envVars[j].Value
	})
	for _, ev := range spec.Env {
		envVars = append(envVars, corev1.EnvVar{Name: ev.Name, Value: ev.Value})
	}
	container.Env = envVars
	if spec.ReadinessProbe != nil {
		container.ReadinessProbe = spec.ReadinessProbe.DeepCopy()
	}
	if spec.LivenessProbe != nil {
		container.LivenessProbe = spec.LivenessProbe.DeepCopy()
	}
	if spec.StartupProbe != nil {
		container.StartupProbe = spec.StartupProbe.DeepCopy()
	}

	return container
}

func buildResourceRequirements(quota ResourceQuota) corev1.ResourceRequirements {
	requests := corev1.ResourceList{}
	limits := corev1.ResourceList{}
	if quota.CPU.Sign() > 0 {
		requests[corev1.ResourceCPU] = quota.CPU.DeepCopy()
		limits[corev1.ResourceCPU] = quota.CPU.DeepCopy()
	}
	if quota.Memory.Sign() > 0 {
		requests[corev1.ResourceMemory] = quota.Memory.DeepCopy()
		limits[corev1.ResourceMemory] = quota.Memory.DeepCopy()
	}
	if len(requests) == 0 {
		requests = nil
	}
	if len(limits) == 0 {
		limits = nil
	}
	return corev1.ResourceRequirements{Requests: requests, Limits: limits}
}

func applySharedTemplateVolumes(spec *corev1.PodSpec, template *SandboxTemplate) {
	if spec == nil || template == nil || len(template.Spec.SharedVolumes) == 0 {
		return
	}

	bidirectional := corev1.MountPropagationBidirectional
	hostToContainer := corev1.MountPropagationHostToContainer
	volumeNames := make(map[string]string, len(template.Spec.SharedVolumes))
	for i, volume := range template.Spec.SharedVolumes {
		volumeName := sharedTemplateVolumeName(i)
		volumeNames[volume.Name] = volumeName
		spec.Volumes = append(spec.Volumes, corev1.Volume{
			Name: volumeName,
			VolumeSource: corev1.VolumeSource{
				EmptyDir: &corev1.EmptyDirVolumeSource{},
			},
		})
		ensureNamedContainerVolumeMount(spec, "procd", corev1.VolumeMount{
			Name:             volumeName,
			MountPath:        volume.MountPath,
			MountPropagation: &bidirectional,
		})
	}

	for _, sidecar := range template.Spec.Sidecars {
		for _, mount := range sidecar.Mounts {
			volumeName := volumeNames[mount.Name]
			ensureNamedContainerVolumeMount(spec, sidecar.Name, corev1.VolumeMount{
				Name:             volumeName,
				MountPath:        mount.MountPath,
				ReadOnly:         mount.ReadOnly,
				MountPropagation: &hostToContainer,
			})
		}
	}
}

func ensureNamedContainerVolumeMount(spec *corev1.PodSpec, containerName string, mount corev1.VolumeMount) {
	if spec == nil {
		return
	}
	for i := range spec.Containers {
		if spec.Containers[i].Name != containerName {
			continue
		}
		ensureContainerVolumeMount(&spec.Containers[i], mount)
		return
	}
}

func sharedTemplateVolumeName(index int) string {
	return "shared-volume-" + strconv.Itoa(index)
}

func appendProcdConfigEnvVars(envVars []corev1.EnvVar) []corev1.EnvVar {
	envIndex := make(map[string]int, len(envVars))
	for i, envVar := range envVars {
		envIndex[envVar.Name] = i
	}

	upsertProcdEnvVar(envIndex, &envVars, corev1.EnvVar{
		Name: "node_name",
		ValueFrom: &corev1.EnvVarSource{
			FieldRef: &corev1.ObjectFieldSelector{FieldPath: "spec.nodeName"},
		},
	})

	cfg := config.LoadManagerConfig()
	if cfg == nil {
		return envVars
	}

	// Procd runtime configuration is injected by manager and must remain
	// authoritative over any tenant-provided env vars with the same names.
	envMap := cfg.ProcdConfig.EnvMap()
	keys := make([]string, 0, len(envMap))
	for key := range envMap {
		if key == "" || key == "node_name" {
			continue
		}
		keys = append(keys, key)
	}
	sort.Strings(keys)

	for _, key := range keys {
		upsertProcdEnvVar(envIndex, &envVars, corev1.EnvVar{
			Name:  key,
			Value: envMap[key],
		})
	}

	// Storage-proxy connectivity must always remain manager-controlled, even
	// when the corresponding fields are omitted from manager config.
	upsertProcdEnvVar(envIndex, &envVars, corev1.EnvVar{
		Name:  "storage_proxy_base_url",
		Value: cfg.ProcdConfig.StorageProxyBaseURL,
	})
	upsertProcdEnvVar(envIndex, &envVars, corev1.EnvVar{
		Name:  "storage_proxy_port",
		Value: strconv.Itoa(cfg.ProcdConfig.StorageProxyPort),
	})

	return envVars
}

func upsertProcdEnvVar(envIndex map[string]int, envVars *[]corev1.EnvVar, envVar corev1.EnvVar) {
	if idx, ok := envIndex[envVar.Name]; ok {
		(*envVars)[idx] = envVar
		return
	}

	*envVars = append(*envVars, envVar)
	envIndex[envVar.Name] = len(*envVars) - 1
}

func applyProcdInit(spec *corev1.PodSpec) {
	cfg := config.LoadManagerConfig()
	managerImage := cfg.ManagerImage

	spec.Volumes = append(spec.Volumes, corev1.Volume{
		Name: procdBinVolumeName,
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
			"cp /usr/local/bin/procd /procd/bin/procd && chmod 0755 /procd/bin/procd",
		},
		VolumeMounts: []corev1.VolumeMount{
			{
				Name:      procdBinVolumeName,
				MountPath: "/procd/bin",
			},
		},
	})
}

func applyNetdMITMCATrustMaterial(spec *corev1.PodSpec) {
	if spec == nil {
		return
	}

	cfg := config.LoadManagerConfig()
	if cfg == nil || cfg.NetdMITMCASecretName == "" {
		return
	}

	volumeFound := false
	for i := range spec.Volumes {
		if spec.Volumes[i].Name != netdMITMCAVolume {
			continue
		}
		volumeFound = true
		spec.Volumes[i].Secret = &corev1.SecretVolumeSource{
			SecretName: cfg.NetdMITMCASecretName,
			Items: []corev1.KeyToPath{
				{
					Key:  netdMITMCACertKey,
					Path: "mitm-ca.crt",
				},
			},
		}
		break
	}
	if !volumeFound {
		spec.Volumes = append(spec.Volumes, corev1.Volume{
			Name: netdMITMCAVolume,
			VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{
					SecretName: cfg.NetdMITMCASecretName,
					Items: []corev1.KeyToPath{
						{
							Key:  netdMITMCACertKey,
							Path: "mitm-ca.crt",
						},
					},
				},
			},
		})
	}

	for i := range spec.Containers {
		ensureContainerEnvVar(&spec.Containers[i], corev1.EnvVar{
			Name:  netdMITMCAEnvVar,
			Value: netdMITMCACertPath,
		})
		ensureContainerVolumeMount(&spec.Containers[i], corev1.VolumeMount{
			Name:      netdMITMCAVolume,
			MountPath: netdMITMCADir,
			ReadOnly:  true,
		})
	}
}

func ensureContainerEnvVar(container *corev1.Container, envVar corev1.EnvVar) {
	for i := range container.Env {
		if container.Env[i].Name != envVar.Name {
			continue
		}
		container.Env[i] = envVar
		return
	}
	container.Env = append(container.Env, envVar)
}

func ensureContainerVolumeMount(container *corev1.Container, mount corev1.VolumeMount) {
	for i := range container.VolumeMounts {
		if container.VolumeMounts[i].Name != mount.Name {
			continue
		}
		container.VolumeMounts[i] = mount
		return
	}
	container.VolumeMounts = append(container.VolumeMounts, mount)
}

// applyProcdSecretVolume mounts the procd config Secret into the pod spec.
// Returns true if the spec was mutated.
func applyProcdSecretVolume(spec *corev1.PodSpec, template *SandboxTemplate) bool {
	clusterID := naming.ClusterIDOrDefault(template.Spec.ClusterId)
	name, err := naming.ProcdConfigSecretName(clusterID, template.Name)
	if err != nil {
		return false
	}

	changed := false
	volumeFound := false
	for i := range spec.Volumes {
		if spec.Volumes[i].Name != procdConfigVolume {
			continue
		}
		volumeFound = true
		if spec.Volumes[i].Secret == nil || spec.Volumes[i].Secret.SecretName != name {
			spec.Volumes[i].Secret = &corev1.SecretVolumeSource{
				SecretName: name,
				Items: []corev1.KeyToPath{
					{
						Key:  "internal_jwt_public.key",
						Path: "internal_jwt_public.key",
					},
				},
			}
			changed = true
		}
		break
	}
	if !volumeFound {
		spec.Volumes = append(spec.Volumes, corev1.Volume{
			Name: procdConfigVolume,
			VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{
					SecretName: name,
					Items: []corev1.KeyToPath{
						{
							Key:  "internal_jwt_public.key",
							Path: "internal_jwt_public.key",
						},
					},
				},
			},
		})
		changed = true
	}

	found := false
	for i := range spec.Containers {
		if spec.Containers[i].Name != "procd" {
			continue
		}
		found = true
		mountFound := false
		for j := range spec.Containers[i].VolumeMounts {
			if spec.Containers[i].VolumeMounts[j].Name != procdConfigVolume {
				continue
			}
			mountFound = true
			mount := &spec.Containers[i].VolumeMounts[j]
			if mount.MountPath != "/config/internal_jwt_public.key" || mount.SubPath != "internal_jwt_public.key" || !mount.ReadOnly {
				mount.MountPath = "/config/internal_jwt_public.key"
				mount.SubPath = "internal_jwt_public.key"
				mount.ReadOnly = true
				changed = true
			}
			break
		}
		if !mountFound {
			spec.Containers[i].VolumeMounts = append(spec.Containers[i].VolumeMounts, corev1.VolumeMount{
				Name:      procdConfigVolume,
				MountPath: "/config/internal_jwt_public.key",
				SubPath:   "internal_jwt_public.key",
				ReadOnly:  true,
			})
			changed = true
		}
	}
	if !found {
		return changed
	}
	return changed
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

// BuildEgressSpec builds a netd-consumed egress spec from SandboxNetworkPolicy.
func BuildEgressSpec(policy *SandboxNetworkPolicy) *NetworkEgressPolicy {
	if policy == nil || policy.Egress == nil {
		return nil
	}

	return &NetworkEgressPolicy{
		AllowedCIDRs:    policy.Egress.AllowedCIDRs,
		DeniedCIDRs:     policy.Egress.DeniedCIDRs,
		AllowedDomains:  policy.Egress.AllowedDomains,
		DeniedDomains:   policy.Egress.DeniedDomains,
		AllowedPorts:    policy.Egress.AllowedPorts,
		DeniedPorts:     policy.Egress.DeniedPorts,
		TrafficRules:    policy.Egress.TrafficRules,
		CredentialRules: policy.Egress.CredentialRules,
	}
}

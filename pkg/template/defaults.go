package template

import "github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"

const (
	DefaultTemplateID               = "default"
	DefaultTemplateImage            = "sandbox0ai/otemplates:default-v0.1.0"
	DefaultTemplateCPU              = "500m"
	DefaultTemplateMemory           = "2Gi"
	DefaultTemplateEphemeralStorage = v1alpha1.DefaultSandboxEphemeralStorage
	DefaultTemplateDisplayName      = "Default"
	DefaultTemplateMinIdle          = int32(1)
	DefaultTemplateMaxIdle          = int32(5)

	DockerInSandboxTemplateID               = "dins"
	DockerInSandboxTemplateDisplayName      = "Docker in Sandbox"
	DockerInSandboxTemplateDescription      = "Builtin Docker in Sandbox template installed by infra-operator."
	DockerInSandboxCPU                      = "1"
	DockerInSandboxMemory                   = "4Gi"
	DockerInSandboxEphemeralStorage         = "20Gi"
	DockerInSandboxDockerRoot               = "/var/lib/docker"
	DockerInSandboxDockerRootSizeLimit      = "20Gi"
	DockerInSandboxWarmProcessName          = "dockerd"
	DockerInSandboxWarmProcessCommand       = "/usr/local/bin/sandbox0-dockerd-entrypoint"
	DockerInSandboxWarmProcessReadinessTime = int32(2)
)

// ApplyDefaultPool applies default pool values when not explicitly set.
func ApplyDefaultPool(minIdle, maxIdle int32) (int32, int32) {
	if minIdle == 0 && maxIdle == 0 {
		return DefaultTemplateMinIdle, DefaultTemplateMaxIdle
	}
	if minIdle == 0 {
		minIdle = DefaultTemplateMinIdle
	}
	if maxIdle == 0 {
		maxIdle = DefaultTemplateMaxIdle
	}
	return minIdle, maxIdle
}

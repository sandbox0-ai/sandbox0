package template

import "github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"

const (
	DefaultTemplateID               = "default"
	DefaultTemplateImage            = "sandbox0ai/otemplates:default-v0.2.0"
	DefaultTemplateCPU              = "500m"
	DefaultTemplateMemory           = "2Gi"
	DefaultTemplateEphemeralStorage = v1alpha1.DefaultSandboxEphemeralStorage
	DefaultTemplateDisplayName      = "Default"
	DefaultTemplateMinIdle          = int32(1)
	DefaultTemplateMaxIdle          = int32(5)
	DefaultTemplateWorkspaceName    = "workspace"
	DefaultTemplateWorkspaceMount   = "/workspace"
	DefaultTemplateDockerRoot       = "/var/lib/docker"
	DefaultTemplateDockerRootSize   = v1alpha1.DefaultSandboxEphemeralStorage

	CodingAgentTemplateID          = "coding-agent"
	CodingAgentTemplateImage       = "sandbox0ai/otemplates:coding-agent-v0.1.0"
	CodingAgentTemplateDisplayName = "Coding Agents"
	CodingAgentTemplateDescription = "Builtin coding-agent template with Codex, Claude Code, OpenCode, and Pi installed by infra-operator."
	CodingAgentCPU                 = "1"
	CodingAgentMemory              = "4Gi"
	CodingAgentEphemeralStorage    = "16Gi"

	OpenClawTemplateID          = "openclaw"
	OpenClawTemplateImage       = "ghcr.io/openclaw/openclaw:latest"
	OpenClawTemplateDisplayName = "OpenClaw"
	OpenClawTemplateDescription = "Builtin OpenClaw agent-in-sandbox template installed by infra-operator."
	OpenClawCPU                 = "1"
	OpenClawMemory              = "4Gi"
	OpenClawEphemeralStorage    = "4Gi"
	OpenClawDataMount           = "/home/node/.openclaw"
	AgentWorkspaceMount         = DefaultTemplateWorkspaceMount
	AgentWorkspaceSizeLimit     = "4Gi"

	HermesTemplateID          = "hermes"
	HermesTemplateImage       = "nousresearch/hermes-agent:latest"
	HermesTemplateDisplayName = "Hermes"
	HermesTemplateDescription = "Builtin Hermes agent-in-sandbox template installed by infra-operator."
	HermesCPU                 = "1"
	HermesMemory              = "4Gi"
	HermesEphemeralStorage    = "4Gi"
	HermesDataMount           = "/opt/data"
	HermesRuntimeHome         = "/workspace/.hermes"

	BrowserTemplateID          = "browser"
	BrowserTemplateImage       = "ghcr.io/steel-dev/steel-browser:latest"
	BrowserTemplateDisplayName = "Browser"
	BrowserTemplateDescription = "Builtin browser automation template installed by infra-operator."
	BrowserCPU                 = "2"
	BrowserMemory              = "8Gi"
	BrowserEphemeralStorage    = "16Gi"
	BrowserProfileDir          = "/browser/profile"
	BrowserDownloadsMountName  = "browser-downloads"
	BrowserDownloadsMount      = "/files"
	BrowserDevShmMount         = "/dev/shm"
	BrowserDevShmSizeLimit     = "2Gi"
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

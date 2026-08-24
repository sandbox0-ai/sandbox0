// Package sandboxstore owns durable sandbox identity and rootfs persistence.
package sandboxstore

import (
	"github.com/sandbox0-ai/sandbox0/pkg/managerapi"
	"github.com/sandbox0-ai/sandbox0/pkg/runtimecontrol"
	v1alpha1 "github.com/sandbox0-ai/sandbox0/pkg/sandboxspec"
)

type SandboxResourceConfig = managerapi.SandboxResourceConfig
type SandboxAppService = managerapi.SandboxAppService
type WebhookConfig = runtimecontrol.WebhookConfig

// SandboxConfig is the durable subset of a sandbox claim configuration.
type SandboxConfig struct {
	EnvVars    map[string]string              `json:"env_vars,omitempty"`
	Resources  *SandboxResourceConfig         `json:"resources,omitempty"`
	TTL        *int32                         `json:"ttl,omitempty"`
	HardTTL    *int32                         `json:"hard_ttl,omitempty"`
	Network    *v1alpha1.SandboxNetworkPolicy `json:"network,omitempty"`
	Webhook    *WebhookConfig                 `json:"webhook,omitempty"`
	AutoResume *bool                          `json:"auto_resume,omitempty"`
	Services   []SandboxAppService            `json:"services,omitempty"`
}

// ListSandboxesRequest filters durable sandbox records.
type ListSandboxesRequest struct {
	TeamID     string
	Status     string
	TemplateID string
	Paused     *bool
	Limit      int
	Offset     int
}

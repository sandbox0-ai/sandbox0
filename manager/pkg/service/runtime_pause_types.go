package service

import "github.com/sandbox0-ai/sandbox0/pkg/procdapi"

type PauseSandboxResponse struct {
	SandboxID     string                         `json:"sandbox_id"`
	Paused        bool                           `json:"paused"`
	Status        string                         `json:"status,omitempty"`
	ResourceUsage *procdapi.SandboxResourceUsage `json:"resource_usage,omitempty"`
	UpdatedMemory string                         `json:"updated_memory,omitempty"`
	UpdatedCPU    string                         `json:"updated_cpu,omitempty"`
}

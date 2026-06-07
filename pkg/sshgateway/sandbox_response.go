package sshgateway

import (
	mgr "github.com/sandbox0-ai/sandbox0/manager/pkg/service"
	"github.com/sandbox0-ai/sandbox0/pkg/apispec"
)

// SandboxToAPI converts a manager sandbox record into the public API payload
// shape and optionally attaches SSH connection information.
func SandboxToAPI(sandbox *mgr.Sandbox, sshInfo *ConnectionInfo) *apispec.Sandbox {
	if sandbox == nil {
		return nil
	}

	payload := &apispec.Sandbox{
		AutoResume:        sandbox.AutoResume,
		ClaimedAt:         sandbox.ClaimedAt,
		CreatedAt:         sandbox.CreatedAt,
		ExpiresAt:         sandbox.ExpiresAt,
		HardExpiresAt:     sandbox.HardExpiresAt,
		Id:                sandbox.ID,
		Paused:            sandbox.Paused,
		PodName:           sandbox.PodName,
		RuntimeGeneration: sandbox.RuntimeGeneration,
		Status:            apispec.SandboxLifecycleStatus(sandbox.Status),
		TeamId:            sandbox.TeamID,
		TemplateId:        sandbox.TemplateID,
		UpdatedAt:         sandbox.UpdatedAt,
	}
	if sandbox.UserID != "" {
		payload.UserId = &sandbox.UserID
	}
	if sshInfo != nil {
		payload.Ssh = &apispec.SandboxSSHConnection{
			Host:     sshInfo.Host,
			Port:     sshInfo.Port,
			Username: sshInfo.Username,
		}
	}
	return payload
}

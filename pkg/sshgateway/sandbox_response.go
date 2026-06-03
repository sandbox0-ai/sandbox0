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
		AutoResume:    sandbox.AutoResume,
		ClaimedAt:     sandbox.ClaimedAt,
		CreatedAt:     sandbox.CreatedAt,
		ExpiresAt:     sandbox.ExpiresAt,
		HardExpiresAt: sandbox.HardExpiresAt,
		Id:            sandbox.ID,
		Paused:        sandbox.Paused,
		PowerState: apispec.SandboxPowerState{
			Desired:            apispec.SandboxPowerStateDesired(sandbox.PowerState.Desired),
			DesiredGeneration:  sandbox.PowerState.DesiredGeneration,
			Observed:           apispec.SandboxPowerStateObserved(sandbox.PowerState.Observed),
			ObservedGeneration: sandbox.PowerState.ObservedGeneration,
			Phase:              apispec.SandboxPowerStatePhase(sandbox.PowerState.Phase),
		},
		Status:     apispec.SandboxLifecycleStatus(sandbox.Status),
		TeamId:     sandbox.TeamID,
		TemplateId: sandbox.TemplateID,
	}
	if sandbox.UserID != "" {
		payload.UserId = &sandbox.UserID
	}
	if sandbox.PodName != "" {
		payload.PodName = &sandbox.PodName
	}
	if sandbox.FilesystemID != "" {
		payload.FilesystemId = &sandbox.FilesystemID
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

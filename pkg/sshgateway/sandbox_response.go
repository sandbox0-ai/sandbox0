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
		PodName:       sandbox.PodName,
		PowerState: apispec.SandboxPowerState{
			Desired:            apispec.SandboxPowerStateDesired(sandbox.PowerState.Desired),
			DesiredGeneration:  sandbox.PowerState.DesiredGeneration,
			Observed:           apispec.SandboxPowerStateObserved(sandbox.PowerState.Observed),
			ObservedGeneration: sandbox.PowerState.ObservedGeneration,
			Phase:              apispec.SandboxPowerStatePhase(sandbox.PowerState.Phase),
		},
		Status:     sandbox.Status,
		TeamId:     sandbox.TeamID,
		TemplateId: sandbox.TemplateID,
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

package http

import (
	mgr "github.com/sandbox0-ai/sandbox0/manager/pkg/service"
	"github.com/sandbox0-ai/sandbox0/pkg/apispec"
	sharedssh "github.com/sandbox0-ai/sandbox0/pkg/sshgateway"
)

// sandboxToAPI removes manager-only runtime fields while preserving the public
// sandbox detail contract and optionally attaching cluster-scoped SSH details.
func sandboxToAPI(sandbox *mgr.Sandbox, sshInfo *sharedssh.ConnectionInfo) *apispec.Sandbox {
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
	if sandbox.Resources != nil && sandbox.Resources.Memory != "" {
		memory := sandbox.Resources.Memory
		payload.Resources = &apispec.SandboxResourceConfig{Memory: &memory}
	}
	if sandbox.Mounts != nil {
		mounts := make([]apispec.ClaimMountRequest, len(sandbox.Mounts))
		for i, mount := range sandbox.Mounts {
			mounts[i] = apispec.ClaimMountRequest{
				MountPoint:      mount.MountPoint,
				SandboxvolumeId: mount.SandboxVolumeID,
			}
		}
		payload.Mounts = &mounts
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

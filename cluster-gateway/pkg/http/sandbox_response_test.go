package http

import (
	"testing"
	"time"

	mgr "github.com/sandbox0-ai/sandbox0/manager/pkg/service"
	sharedssh "github.com/sandbox0-ai/sandbox0/pkg/sshgateway"
)

func TestSandboxToAPIPreservesPublicDetailFields(t *testing.T) {
	now := time.Now().UTC()
	sandbox := &mgr.Sandbox{
		ID:         "sb_123",
		TemplateID: "default",
		TeamID:     "team-1",
		UserID:     "user-1",
		Status:     "running",
		PodName:    "pod-1",
		AutoResume: true,
		Paused:     false,
		Resources:  &mgr.SandboxResourceConfig{Memory: "512Mi"},
		Mounts: []mgr.ClaimMount{
			{
				SandboxVolumeID: "volume-1",
				MountPoint:      "/workspace",
			},
		},
		ClaimedAt:     now,
		CreatedAt:     now,
		ExpiresAt:     now,
		HardExpiresAt: now,
	}

	payload := sandboxToAPI(sandbox, sharedssh.BuildConnectionInfo("aws-us-east-1.ssh.sandbox0.app", 30222, sandbox.ID))
	if payload == nil {
		t.Fatal("expected payload")
	}
	if payload.Ssh == nil {
		t.Fatal("expected SSH connection")
	}
	if payload.Ssh.Host != "aws-us-east-1.ssh.sandbox0.app" {
		t.Fatalf("ssh host = %q", payload.Ssh.Host)
	}
	if payload.Ssh.Port != 30222 {
		t.Fatalf("ssh port = %d", payload.Ssh.Port)
	}
	if payload.Ssh.Username != sandbox.ID {
		t.Fatalf("ssh username = %q", payload.Ssh.Username)
	}
	if payload.Resources == nil || payload.Resources.Memory == nil || *payload.Resources.Memory != "512Mi" {
		t.Fatalf("resources = %#v, want memory 512Mi", payload.Resources)
	}
	if payload.Mounts == nil || len(*payload.Mounts) != 1 {
		t.Fatalf("mounts = %#v, want one mount", payload.Mounts)
	}
	mount := (*payload.Mounts)[0]
	if mount.SandboxvolumeId != "volume-1" || mount.MountPoint != "/workspace" {
		t.Fatalf("mount = %#v, want volume-1 at /workspace", mount)
	}
}

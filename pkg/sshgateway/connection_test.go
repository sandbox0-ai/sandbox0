package sshgateway

import (
	"testing"
	"time"

	mgr "github.com/sandbox0-ai/sandbox0/manager/pkg/service"
)

func TestBuildConnectionInfo(t *testing.T) {
	info := BuildConnectionInfo("ssh.aws-us-east-1.sandbox0.app", 30222, "sb_123")
	if info == nil {
		t.Fatal("expected connection info")
	}
	if info.Host != "ssh.aws-us-east-1.sandbox0.app" {
		t.Fatalf("host = %q", info.Host)
	}
	if info.Port != 30222 {
		t.Fatalf("port = %d", info.Port)
	}
	if info.Username != "sb_123" {
		t.Fatalf("username = %q", info.Username)
	}
}

func TestSandboxToAPIIncludesSSHInfo(t *testing.T) {
	now := time.Now().UTC()
	sandbox := &mgr.Sandbox{
		ID:         "sb_123",
		TemplateID: "default",
		TeamID:     "team-1",
		UserID:     "user-1",
		Status:     "running",
		PodName:    "pod-1",
		PowerState: mgr.SandboxPowerState{Desired: "active", Observed: "active", Phase: "stable"},
		AutoResume: true,
		Paused:     false,
		ClaimedAt:  now,
		CreatedAt:  now,
		ExpiresAt:  now,
		HardExpiresAt: now,
	}

	payload := SandboxToAPI(sandbox, BuildConnectionInfo("ssh.aws-us-east-1.sandbox0.app", 30222, sandbox.ID))
	if payload == nil {
		t.Fatal("expected payload")
	}
	if payload.Ssh == nil {
		t.Fatal("expected ssh payload")
	}
	if payload.Ssh.Host != "ssh.aws-us-east-1.sandbox0.app" {
		t.Fatalf("ssh host = %q", payload.Ssh.Host)
	}
	if payload.Ssh.Port != 30222 {
		t.Fatalf("ssh port = %d", payload.Ssh.Port)
	}
	if payload.Ssh.Username != sandbox.ID {
		t.Fatalf("ssh username = %q", payload.Ssh.Username)
	}
}

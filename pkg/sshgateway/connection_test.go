package sshgateway

import (
	"testing"
)

func TestBuildConnectionInfo(t *testing.T) {
	info := BuildConnectionInfo("aws-us-east-1.ssh.sandbox0.app", 30222, "sb_123")
	if info == nil {
		t.Fatal("expected connection info")
	}
	if info.Host != "aws-us-east-1.ssh.sandbox0.app" {
		t.Fatalf("host = %q", info.Host)
	}
	if info.Port != 30222 {
		t.Fatalf("port = %d", info.Port)
	}
	if info.Username != "sb_123" {
		t.Fatalf("username = %q", info.Username)
	}
}

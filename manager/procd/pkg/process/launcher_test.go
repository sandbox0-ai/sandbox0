package process

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
)

func TestNewCommandContextUsesSandboxRootFS(t *testing.T) {
	root := t.TempDir()
	binDir := filepath.Join(root, "usr", "bin")
	if err := os.MkdirAll(binDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(binDir, "hello"), []byte("#!/bin/sh\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	ConfigureLauncher(LauncherConfig{RootPath: root, Chroot: true})
	t.Cleanup(func() { ConfigureLauncher(LauncherConfig{}) })

	cmd, err := NewCommandContext(context.Background(), "hello", []string{"arg"}, LaunchOptions{
		CWD: "/workspace",
		Env: []string{"PATH=/usr/bin"},
	})
	if err != nil {
		t.Fatalf("NewCommandContext() error = %v", err)
	}
	if cmd.Path != "/usr/bin/hello" {
		t.Fatalf("cmd.Path = %q, want /usr/bin/hello", cmd.Path)
	}
	if cmd.Dir != "/workspace" {
		t.Fatalf("cmd.Dir = %q, want /workspace", cmd.Dir)
	}
	if cmd.SysProcAttr == nil || cmd.SysProcAttr.Chroot != root {
		t.Fatalf("cmd.SysProcAttr = %#v, want chroot %q", cmd.SysProcAttr, root)
	}
}

func TestSetProcessGroupPreservesLauncherSysProcAttr(t *testing.T) {
	cmd := exec.Command("/bin/true")
	cmd.SysProcAttr = &syscall.SysProcAttr{Chroot: "/sandbox0/rootfs"}

	SetProcessGroup(cmd)

	if cmd.SysProcAttr == nil {
		t.Fatal("SysProcAttr is nil")
	}
	if !cmd.SysProcAttr.Setpgid {
		t.Fatal("Setpgid = false, want true")
	}
	if cmd.SysProcAttr.Chroot != "/sandbox0/rootfs" {
		t.Fatalf("Chroot = %q, want /sandbox0/rootfs", cmd.SysProcAttr.Chroot)
	}
}

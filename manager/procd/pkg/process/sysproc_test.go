package process

import (
	"os/exec"
	"syscall"
	"testing"
)

func TestApplySysProcAttrSetsProcessGroupAndRootFS(t *testing.T) {
	cmd := exec.Command("/bin/true")
	cmd.SysProcAttr = &syscall.SysProcAttr{}

	ApplySysProcAttr(cmd, ProcessConfig{RootFS: " /sandbox0/rootfs "}, true)

	if cmd.SysProcAttr == nil {
		t.Fatal("SysProcAttr is nil")
	}
	if !cmd.SysProcAttr.Setpgid {
		t.Fatal("Setpgid = false, want true")
	}
	if cmd.SysProcAttr.Chroot != "/sandbox0/rootfs" {
		t.Fatalf("Chroot = %q, want /sandbox0/rootfs", cmd.SysProcAttr.Chroot)
	}
	if cmd.Dir != "/" {
		t.Fatalf("Dir = %q, want /", cmd.Dir)
	}
}

func TestApplySysProcAttrPreservesExplicitWorkingDirectory(t *testing.T) {
	cmd := exec.Command("/bin/true")
	cmd.Dir = "/workspace"

	ApplySysProcAttr(cmd, ProcessConfig{RootFS: " /sandbox0/rootfs "}, false)

	if cmd.Dir != "/workspace" {
		t.Fatalf("Dir = %q, want /workspace", cmd.Dir)
	}
}

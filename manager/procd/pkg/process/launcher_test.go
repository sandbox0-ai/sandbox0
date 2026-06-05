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
	ConfigureLauncher(LauncherConfig{RootPath: root, LauncherPath: "/procd/bin/procd-test"})
	t.Cleanup(func() { ConfigureLauncher(LauncherConfig{}) })

	cmd, err := NewCommandContext(context.Background(), "hello", []string{"arg"}, LaunchOptions{
		CWD: "/workspace",
		Env: []string{"PATH=/usr/bin"},
	})
	if err != nil {
		t.Fatalf("NewCommandContext() error = %v", err)
	}
	if cmd.Path != "/procd/bin/procd-test" {
		t.Fatalf("cmd.Path = %q, want launcher path", cmd.Path)
	}
	if cmd.Dir != "/" {
		t.Fatalf("cmd.Dir = %q, want /", cmd.Dir)
	}
	wantArgs := []string{
		"/procd/bin/procd-test",
		rootFSLauncherArg,
		"--root", root,
		"--cwd", "/workspace",
		"--",
		"/usr/bin/hello",
		"arg",
	}
	if len(cmd.Args) != len(wantArgs) {
		t.Fatalf("cmd.Args = %#v, want %#v", cmd.Args, wantArgs)
	}
	for i := range wantArgs {
		if cmd.Args[i] != wantArgs[i] {
			t.Fatalf("cmd.Args[%d] = %q, want %q; all args=%#v", i, cmd.Args[i], wantArgs[i], cmd.Args)
		}
	}
}

func TestNewCommandContextPassesExternalMounts(t *testing.T) {
	root := t.TempDir()
	binDir := filepath.Join(root, "usr", "bin")
	if err := os.MkdirAll(binDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(binDir, "hello"), []byte("#!/bin/sh\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	ConfigureLauncher(LauncherConfig{
		RootPath:       root,
		LauncherPath:   "/procd/bin/procd-test",
		ExternalMounts: []string{"/workspace/data"},
	})
	t.Cleanup(func() { ConfigureLauncher(LauncherConfig{}) })

	cmd, err := NewCommandContext(context.Background(), "hello", nil, LaunchOptions{Env: []string{"PATH=/usr/bin"}})
	if err != nil {
		t.Fatalf("NewCommandContext() error = %v", err)
	}
	wantArgs := []string{
		"/procd/bin/procd-test",
		rootFSLauncherArg,
		"--root", root,
		"--cwd", "/",
		"--external-mount", "/workspace/data",
		"--",
		"/usr/bin/hello",
	}
	if len(cmd.Args) != len(wantArgs) {
		t.Fatalf("cmd.Args = %#v, want %#v", cmd.Args, wantArgs)
	}
	for i := range wantArgs {
		if cmd.Args[i] != wantArgs[i] {
			t.Fatalf("cmd.Args[%d] = %q, want %q; all args=%#v", i, cmd.Args[i], wantArgs[i], cmd.Args)
		}
	}
}

func TestParseRootFSLauncherArgsRejectsRelativeExternalMount(t *testing.T) {
	_, err := parseRootFSLauncherArgs([]string{
		"--root", "/sandbox0/rootfs",
		"--cwd", "/",
		"--external-mount", "workspace/data",
		"--",
		"/bin/sh",
	})
	if err == nil {
		t.Fatal("parseRootFSLauncherArgs() error = nil, want error")
	}
}

func TestPrepareExternalMountTargetRejectsSymlinkComponent(t *testing.T) {
	root := t.TempDir()
	source := t.TempDir()
	info, err := os.Lstat(source)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("/tmp", filepath.Join(root, "workspace")); err != nil {
		t.Fatal(err)
	}

	_, err = prepareExternalMountTarget(root, "/workspace/data", info)
	if err == nil {
		t.Fatal("prepareExternalMountTarget() error = nil, want error")
	}
}

func TestSetProcessGroupPreservesExistingSysProcAttr(t *testing.T) {
	cmd := exec.Command("/bin/true")
	cmd.SysProcAttr = &syscall.SysProcAttr{Pdeathsig: syscall.SIGTERM}

	SetProcessGroup(cmd)

	if cmd.SysProcAttr == nil {
		t.Fatal("SysProcAttr is nil")
	}
	if !cmd.SysProcAttr.Setpgid {
		t.Fatal("Setpgid = false, want true")
	}
	if cmd.SysProcAttr.Pdeathsig != syscall.SIGTERM {
		t.Fatalf("Pdeathsig = %v, want SIGTERM", cmd.SysProcAttr.Pdeathsig)
	}
}

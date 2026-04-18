package volume

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"testing"

	ctldpower "github.com/sandbox0-ai/sandbox0/ctld/internal/ctld/power"
	"github.com/sandbox0-ai/sandbox0/pkg/ctldapi"
)

type fakeResolver struct {
	target ctldpower.Target
	err    error
}

func (f fakeResolver) Resolve(_ *http.Request, _ string) (ctldpower.Target, error) {
	return f.target, f.err
}

type recordedCommand struct {
	name string
	args []string
}

type recordingRunner struct {
	commands []recordedCommand
	err      error
}

func (r *recordingRunner) Run(_ context.Context, name string, args ...string) error {
	r.commands = append(r.commands, recordedCommand{name: name, args: append([]string(nil), args...)})
	return r.err
}

func TestControllerAttachVolumeBindsStagedMountIntoSandboxNamespace(t *testing.T) {
	stagingRoot := t.TempDir()
	procRoot := t.TempDir()
	cgroupRoot := t.TempDir()
	requireMkdir(t, filepath.Join(stagingRoot, "vol-1"))
	requireMkdir(t, filepath.Join(procRoot, "1234", "ns"))
	requireWriteFile(t, filepath.Join(procRoot, "1234", "ns", "mnt"), "")
	requireWriteFile(t, filepath.Join(cgroupRoot, "cgroup.procs"), "1234\n")

	runner := &recordingRunner{}
	controller := NewController(fakeResolver{target: ctldpower.Target{
		SandboxID: "sandbox-1",
		CgroupDir: cgroupRoot,
	}}, stagingRoot, procRoot)
	controller.Runner = runner

	resp, status := controller.AttachVolume(httptestRequest(), "sandbox-1", ctldapi.VolumeAttachRequest{
		SandboxVolumeID: "vol-1",
		MountPoint:      "/workspace/data",
	})
	if status != http.StatusOK {
		t.Fatalf("AttachVolume() status = %d response=%+v", status, resp)
	}
	if !resp.Attached || resp.AttachmentID == "" || resp.MountSessionID != resp.AttachmentID {
		t.Fatalf("AttachVolume() response = %+v", resp)
	}
	if len(runner.commands) != 2 {
		t.Fatalf("commands = %+v, want mkdir and bind mount", runner.commands)
	}
	mountNS := filepath.Join(procRoot, "1234", "ns/mnt")
	wantMkdirArgs := []string{"--mount=" + mountNS, "--", "mkdir", "-p", "/workspace/data"}
	wantSource := filepath.Join(procRoot, fmt.Sprintf("%d", os.Getpid()), "root", filepath.Join(stagingRoot, "vol-1"))
	wantMountArgs := []string{"--mount=" + mountNS, "--", "mount", "--bind", wantSource, "/workspace/data"}
	assertCommand(t, runner.commands[0], "nsenter", wantMkdirArgs)
	assertCommand(t, runner.commands[1], "nsenter", wantMountArgs)
}

func TestControllerDetachVolumeUnmountsFromSandboxNamespace(t *testing.T) {
	procRoot := t.TempDir()
	cgroupRoot := t.TempDir()
	requireMkdir(t, filepath.Join(procRoot, "1234", "ns"))
	requireWriteFile(t, filepath.Join(procRoot, "1234", "ns", "mnt"), "")
	requireWriteFile(t, filepath.Join(cgroupRoot, "nested", "cgroup.procs"), "1234\n")

	runner := &recordingRunner{}
	controller := NewController(fakeResolver{target: ctldpower.Target{
		SandboxID: "sandbox-1",
		CgroupDir: cgroupRoot,
	}}, t.TempDir(), procRoot)
	controller.Runner = runner

	resp, status := controller.DetachVolume(httptestRequest(), "sandbox-1", ctldapi.VolumeDetachRequest{
		SandboxVolumeID: "vol-1",
		MountPoint:      "/workspace/data",
		AttachmentID:    "attach-1",
		MountSessionID:  "session-1",
	})
	if status != http.StatusOK {
		t.Fatalf("DetachVolume() status = %d response=%+v", status, resp)
	}
	if !resp.Detached {
		t.Fatalf("DetachVolume() response = %+v", resp)
	}
	if len(runner.commands) != 1 {
		t.Fatalf("commands = %+v, want one umount", runner.commands)
	}
	mountNS := filepath.Join(procRoot, "1234", "ns/mnt")
	assertCommand(t, runner.commands[0], "nsenter", []string{"--mount=" + mountNS, "--", "umount", "/workspace/data"})
}

func TestControllerAttachVolumeRequiresStagedMount(t *testing.T) {
	cgroupRoot := t.TempDir()
	requireWriteFile(t, filepath.Join(cgroupRoot, "cgroup.procs"), "1234\n")

	controller := NewController(fakeResolver{target: ctldpower.Target{
		SandboxID: "sandbox-1",
		CgroupDir: cgroupRoot,
	}}, t.TempDir(), t.TempDir())

	resp, status := controller.AttachVolume(httptestRequest(), "sandbox-1", ctldapi.VolumeAttachRequest{
		SandboxVolumeID: "vol-1",
		MountPoint:      "/workspace/data",
	})
	if status != http.StatusServiceUnavailable {
		t.Fatalf("AttachVolume() status = %d response=%+v", status, resp)
	}
	if resp.Attached || resp.Error == "" {
		t.Fatalf("AttachVolume() response = %+v", resp)
	}
}

func TestControllerAttachVolumeRejectsUnsafeVolumeID(t *testing.T) {
	controller := NewController(fakeResolver{}, t.TempDir(), t.TempDir())
	resp, status := controller.AttachVolume(httptestRequest(), "sandbox-1", ctldapi.VolumeAttachRequest{
		SandboxVolumeID: "../vol-1",
		MountPoint:      "/workspace/data",
	})
	if status != http.StatusBadRequest {
		t.Fatalf("AttachVolume() status = %d response=%+v", status, resp)
	}
}

func TestControllerAttachVolumeRejectsInvalidMountPoint(t *testing.T) {
	controller := NewController(fakeResolver{}, t.TempDir(), t.TempDir())
	resp, status := controller.AttachVolume(httptestRequest(), "sandbox-1", ctldapi.VolumeAttachRequest{
		SandboxVolumeID: "vol-1",
		MountPoint:      "relative/path",
	})
	if status != http.StatusBadRequest {
		t.Fatalf("AttachVolume() status = %d response=%+v", status, resp)
	}
}

func TestControllerResolveSandboxNotFound(t *testing.T) {
	controller := NewController(fakeResolver{err: ctldpower.ErrSandboxNotFound}, t.TempDir(), t.TempDir())
	resp, status := controller.AttachVolume(httptestRequest(), "sandbox-1", ctldapi.VolumeAttachRequest{
		SandboxVolumeID: "vol-1",
		MountPoint:      "/workspace/data",
	})
	if status != http.StatusNotFound {
		t.Fatalf("AttachVolume() status = %d response=%+v", status, resp)
	}
}

func TestControllerCommandFailureReturnsInternalError(t *testing.T) {
	stagingRoot := t.TempDir()
	procRoot := t.TempDir()
	cgroupRoot := t.TempDir()
	requireMkdir(t, filepath.Join(stagingRoot, "vol-1"))
	requireMkdir(t, filepath.Join(procRoot, "1234", "ns"))
	requireWriteFile(t, filepath.Join(procRoot, "1234", "ns", "mnt"), "")
	requireWriteFile(t, filepath.Join(cgroupRoot, "cgroup.procs"), "1234\n")

	controller := NewController(fakeResolver{target: ctldpower.Target{CgroupDir: cgroupRoot}}, stagingRoot, procRoot)
	controller.Runner = &recordingRunner{err: errors.New("boom")}

	resp, status := controller.AttachVolume(httptestRequest(), "sandbox-1", ctldapi.VolumeAttachRequest{
		SandboxVolumeID: "vol-1",
		MountPoint:      "/workspace/data",
	})
	if status != http.StatusInternalServerError {
		t.Fatalf("AttachVolume() status = %d response=%+v", status, resp)
	}
	if resp.Attached || resp.Error == "" {
		t.Fatalf("AttachVolume() response = %+v", resp)
	}
}

func httptestRequest() *http.Request {
	req, _ := http.NewRequest(http.MethodPost, "http://ctld.local/", nil)
	return req
}

func requireMkdir(t *testing.T, path string) {
	t.Helper()
	if err := os.MkdirAll(path, 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", path, err)
	}
}

func requireWriteFile(t *testing.T, path string, data string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", filepath.Dir(path), err)
	}
	if err := os.WriteFile(path, []byte(data), 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

func assertCommand(t *testing.T, got recordedCommand, wantName string, wantArgs []string) {
	t.Helper()
	if got.name != wantName {
		t.Fatalf("command name = %q, want %q", got.name, wantName)
	}
	if len(got.args) != len(wantArgs) {
		t.Fatalf("command args = %#v, want %#v", got.args, wantArgs)
	}
	for i := range wantArgs {
		if got.args[i] != wantArgs[i] {
			t.Fatalf("command args = %#v, want %#v", got.args, wantArgs)
		}
	}
}

package volume

import (
	"context"
	"errors"
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

type recordedNamespaceCall struct {
	op        string
	mountNS   string
	source    string
	mountPath string
}

type recordingNamespaceOperator struct {
	calls []recordedNamespaceCall
	err   error
}

type recordingRunner struct {
	commands []recordedCommand
	err      error
}

type recordedCommand struct {
	name string
	args []string
}

func (r *recordingRunner) Run(_ context.Context, name string, args ...string) error {
	r.commands = append(r.commands, recordedCommand{name: name, args: append([]string(nil), args...)})
	return r.err
}

func (r *recordingNamespaceOperator) EnsureMountPoint(_ context.Context, mountNSPath, mountPoint string) error {
	r.calls = append(r.calls, recordedNamespaceCall{op: "mkdir", mountNS: mountNSPath, mountPath: mountPoint})
	return r.err
}

func (r *recordingNamespaceOperator) BindMount(_ context.Context, mountNSPath, sourcePath, mountPoint string) error {
	r.calls = append(r.calls, recordedNamespaceCall{op: "mount", mountNS: mountNSPath, source: sourcePath, mountPath: mountPoint})
	return r.err
}

func (r *recordingNamespaceOperator) Unmount(_ context.Context, mountNSPath, mountPoint string) error {
	r.calls = append(r.calls, recordedNamespaceCall{op: "umount", mountNS: mountNSPath, mountPath: mountPoint})
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

	operator := &recordingNamespaceOperator{}
	controller := NewController(fakeResolver{target: ctldpower.Target{
		SandboxID: "sandbox-1",
		CgroupDir: cgroupRoot,
	}}, stagingRoot, procRoot)
	controller.Namespace = operator

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
	if len(operator.calls) != 2 {
		t.Fatalf("calls = %+v, want mkdir and bind mount", operator.calls)
	}
	mountNS := filepath.Join(procRoot, "1234", "ns/mnt")
	wantTarget := "/workspace/data"
	wantSource := filepath.Join(stagingRoot, "vol-1")
	assertNamespaceCall(t, operator.calls[0], "mkdir", mountNS, "", wantTarget)
	assertNamespaceCall(t, operator.calls[1], "mount", mountNS, wantSource, wantTarget)
}

func TestControllerDetachVolumeUnmountsFromSandboxNamespace(t *testing.T) {
	procRoot := t.TempDir()
	cgroupRoot := t.TempDir()
	requireMkdir(t, filepath.Join(procRoot, "1234", "ns"))
	requireWriteFile(t, filepath.Join(procRoot, "1234", "ns", "mnt"), "")
	requireWriteFile(t, filepath.Join(cgroupRoot, "nested", "cgroup.procs"), "1234\n")

	operator := &recordingNamespaceOperator{}
	controller := NewController(fakeResolver{target: ctldpower.Target{
		SandboxID: "sandbox-1",
		CgroupDir: cgroupRoot,
	}}, t.TempDir(), procRoot)
	controller.Namespace = operator

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
	if len(operator.calls) != 1 {
		t.Fatalf("calls = %+v, want one umount", operator.calls)
	}
	mountNS := filepath.Join(procRoot, "1234", "ns/mnt")
	wantTarget := "/workspace/data"
	assertNamespaceCall(t, operator.calls[0], "umount", mountNS, "", wantTarget)
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
	controller.Namespace = &recordingNamespaceOperator{err: errors.New("boom")}

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

func TestPreferredProcessInCgroupTreePrefersProcdOverPause(t *testing.T) {
	procRoot := t.TempDir()
	cgroupRoot := t.TempDir()
	requireWriteFile(t, filepath.Join(cgroupRoot, "pod", "cgroup.procs"), "101\n100\n")
	requireWriteFile(t, filepath.Join(procRoot, "100", "cmdline"), "/pause\x00")
	requireWriteFile(t, filepath.Join(procRoot, "101", "cmdline"), "/procd/bin/procd\x00")

	pid, err := preferredProcessInCgroupTree(procRoot, cgroupRoot)
	if err != nil {
		t.Fatalf("preferredProcessInCgroupTree() error = %v", err)
	}
	if pid != "101" {
		t.Fatalf("preferredProcessInCgroupTree() pid = %q, want 101", pid)
	}
}

func TestPreferredProcessInCgroupTreeFallsBackToNonPauseProcess(t *testing.T) {
	procRoot := t.TempDir()
	cgroupRoot := t.TempDir()
	requireWriteFile(t, filepath.Join(cgroupRoot, "pod", "cgroup.procs"), "200\n201\n")
	requireWriteFile(t, filepath.Join(procRoot, "200", "cmdline"), "/pause\x00")
	requireWriteFile(t, filepath.Join(procRoot, "201", "cmdline"), "/bin/sh\x00-c\x00sleep 10\x00")

	pid, err := preferredProcessInCgroupTree(procRoot, cgroupRoot)
	if err != nil {
		t.Fatalf("preferredProcessInCgroupTree() error = %v", err)
	}
	if pid != "201" {
		t.Fatalf("preferredProcessInCgroupTree() pid = %q, want 201", pid)
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

func assertNamespaceCall(t *testing.T, got recordedNamespaceCall, wantOp, wantMountNS, wantSource, wantMountPath string) {
	t.Helper()
	if got.op != wantOp {
		t.Fatalf("op = %q, want %q", got.op, wantOp)
	}
	if got.mountNS != wantMountNS {
		t.Fatalf("mountNS = %q, want %q", got.mountNS, wantMountNS)
	}
	if got.source != wantSource {
		t.Fatalf("source = %q, want %q", got.source, wantSource)
	}
	if got.mountPath != wantMountPath {
		t.Fatalf("mountPath = %q, want %q", got.mountPath, wantMountPath)
	}
}

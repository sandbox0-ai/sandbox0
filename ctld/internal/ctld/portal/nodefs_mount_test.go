package portal

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestMountInfoContainsExactDecodedPath(t *testing.T) {
	mountInfo := strings.Join([]string{
		"31 24 0:27 / / rw,relatime - overlay overlay rw",
		"42 31 0:52 / /var/lib/kubelet/pods/pod-a/volumes/kubernetes.io~csi/sandbox0-volume-0-workspace/mount rw - fuse.sandbox0 sandbox0 rw",
		"43 31 0:52 / /path\\040with\\011escapes rw - fuse.sandbox0 sandbox0 rw",
	}, "\n")
	tests := []struct {
		path string
		want bool
	}{
		{"/var/lib/kubelet/pods/pod-a/volumes/kubernetes.io~csi/sandbox0-volume-0-workspace/mount", true},
		{"/var/lib/kubelet/pods/pod-a/volumes/kubernetes.io~csi/sandbox0-volume-0-workspace", false},
		{"/path with\tescapes", true},
	}
	for _, test := range tests {
		got, err := mountInfoContains(strings.NewReader(mountInfo), test.path)
		if err != nil {
			t.Fatalf("mountInfoContains(%q) error = %v", test.path, err)
		}
		if got != test.want {
			t.Fatalf("mountInfoContains(%q) = %v, want %v", test.path, got, test.want)
		}
	}
}

func TestDecodeMountInfoFieldRejectsBrokenEscape(t *testing.T) {
	if _, err := decodeMountInfoField(`/broken\04`); err == nil {
		t.Fatal("decodeMountInfoField() error = nil")
	}
}

func TestSystemNodeFSMounterRejectsUnsafeTargets(t *testing.T) {
	mounter := systemNodeFSMounter{kubeletPodsRoot: t.TempDir()}
	if err := mounter.EnsureBind(t.TempDir(), filepath.Join(t.TempDir(), "unsafe")); err == nil {
		t.Fatal("EnsureBind() error = nil for unsafe target")
	}
	if err := mounter.Unmount(filepath.Join(t.TempDir(), "unsafe")); err == nil {
		t.Fatal("Unmount() error = nil for unsafe target")
	}
}

func TestSystemNodeFSMounterRejectsNonEmptyTargetBeforeMount(t *testing.T) {
	root := t.TempDir()
	target := filepath.Join(root, "pod-a", "volumes", kubeletCSIVolumeDir, "sandbox0-volume-0-workspace", "mount")
	if err := os.MkdirAll(target, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(target, "unexpected"), []byte("data"), 0o600); err != nil {
		t.Fatal(err)
	}
	mounter := systemNodeFSMounter{kubeletPodsRoot: root}
	if err := mounter.EnsureBind(t.TempDir(), target); err == nil {
		t.Fatal("EnsureBind() error = nil for non-empty target")
	}
}

func TestSystemNodeFSMounterRejectsSymlinkedTarget(t *testing.T) {
	root := t.TempDir()
	target := filepath.Join(root, "pod-a", "volumes", kubeletCSIVolumeDir, "sandbox0-volume-0-workspace", "mount")
	if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(t.TempDir(), target); err != nil {
		t.Fatal(err)
	}
	mounter := systemNodeFSMounter{kubeletPodsRoot: root}
	if err := mounter.EnsureBind(t.TempDir(), target); err == nil {
		t.Fatal("EnsureBind() error = nil for symlink target")
	}
	if err := mounter.Unmount(target); err == nil {
		t.Fatal("Unmount() error = nil for symlink target")
	}
}

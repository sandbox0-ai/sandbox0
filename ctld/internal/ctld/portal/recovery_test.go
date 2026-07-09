package portal

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"testing"
)

func TestCleanupStaleCSIMountsCleansOnlySandboxCSIMounts(t *testing.T) {
	root := t.TempDir()
	validA := filepath.Join(root, "pod-a", "volumes", kubeletCSIVolumeDir, "sandbox0-volume-0-state", "mount")
	validB := filepath.Join(root, "pod-b", "volumes", kubeletCSIVolumeDir, "sandbox0-volume-1-workspace", "mount")
	otherCSI := filepath.Join(root, "pod-c", "volumes", kubeletCSIVolumeDir, "other-volume", "mount")
	otherPlugin := filepath.Join(root, "pod-d", "volumes", "kubernetes.io~secret", "sandbox0-volume-0-state", "mount")
	for _, path := range []string{validA, validB, otherCSI, otherPlugin} {
		if err := os.MkdirAll(path, 0o755); err != nil {
			t.Fatalf("MkdirAll(%q) error = %v", path, err)
		}
	}

	var cleaned []string
	mgr := NewManager(Config{
		RootDir:         t.TempDir(),
		KubeletPodsRoot: root,
		ActivePodUIDLister: func(context.Context) (map[string]struct{}, error) {
			return map[string]struct{}{}, nil
		},
		StaleMountCleaner: func(path string) error {
			cleaned = append(cleaned, path)
			return os.RemoveAll(path)
		},
	})
	if err := mgr.CleanupStaleCSIMounts(context.Background()); err != nil {
		t.Fatalf("CleanupStaleCSIMounts() error = %v", err)
	}

	slices.Sort(cleaned)
	want := []string{validA, validB}
	slices.Sort(want)
	if !slices.Equal(cleaned, want) {
		t.Fatalf("cleaned paths = %#v, want %#v", cleaned, want)
	}
	for _, path := range []string{otherCSI, otherPlugin} {
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("expected non-sandbox mount %q to remain, stat error = %v", path, err)
		}
	}
}

func TestCleanupStaleCSIMountsSkipsActivePodMounts(t *testing.T) {
	root := t.TempDir()
	inactive := filepath.Join(root, "pod-a", "volumes", kubeletCSIVolumeDir, "sandbox0-volume-0-state", "mount")
	active := filepath.Join(root, "pod-b", "volumes", kubeletCSIVolumeDir, "sandbox0-volume-1-workspace", "mount")
	for _, path := range []string{inactive, active} {
		if err := os.MkdirAll(path, 0o755); err != nil {
			t.Fatalf("MkdirAll(%q) error = %v", path, err)
		}
	}

	var cleaned []string
	mgr := NewManager(Config{
		RootDir:         t.TempDir(),
		KubeletPodsRoot: root,
		ActivePodUIDLister: func(context.Context) (map[string]struct{}, error) {
			return map[string]struct{}{"pod-b": {}}, nil
		},
		StaleMountCleaner: func(path string) error {
			cleaned = append(cleaned, path)
			return os.RemoveAll(path)
		},
	})
	if err := mgr.CleanupStaleCSIMounts(context.Background()); err != nil {
		t.Fatalf("CleanupStaleCSIMounts() error = %v", err)
	}
	if !slices.Equal(cleaned, []string{inactive}) {
		t.Fatalf("cleaned paths = %#v, want %#v", cleaned, []string{inactive})
	}
	if _, err := os.Stat(active); err != nil {
		t.Fatalf("expected active mount %q to remain, stat error = %v", active, err)
	}
}

func TestCleanupStaleCSIMountsDoesNotCleanReadableMountsWithoutActivePods(t *testing.T) {
	root := t.TempDir()
	valid := filepath.Join(root, "pod-a", "volumes", kubeletCSIVolumeDir, "sandbox0-volume-0-state", "mount")
	otherCSI := filepath.Join(root, "pod-b", "volumes", kubeletCSIVolumeDir, "other-volume", "mount")
	for _, path := range []string{valid, otherCSI} {
		if err := os.MkdirAll(path, 0o755); err != nil {
			t.Fatalf("MkdirAll(%q) error = %v", path, err)
		}
	}

	var cleaned []string
	mgr := NewManager(Config{
		RootDir:         t.TempDir(),
		KubeletPodsRoot: root,
		StaleMountCleaner: func(path string) error {
			cleaned = append(cleaned, path)
			return os.RemoveAll(path)
		},
	})
	if err := mgr.CleanupStaleCSIMounts(context.Background()); err != nil {
		t.Fatalf("CleanupStaleCSIMounts() error = %v", err)
	}
	if len(cleaned) != 0 {
		t.Fatalf("cleaned paths = %#v, want none", cleaned)
	}
	for _, path := range []string{valid, otherCSI} {
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("expected mount %q to remain, stat error = %v", path, err)
		}
	}
}

func TestIsSandboxCSIMountPathRequiresExactKubeletShape(t *testing.T) {
	root := filepath.Clean("/var/lib/kubelet/pods")
	valid := filepath.Join(root, "pod-a", "volumes", kubeletCSIVolumeDir, "sandbox0-volume-0-state", "mount")
	if !isSandboxCSIMountPath(root, valid) {
		t.Fatalf("expected %q to match sandbox CSI mount path", valid)
	}

	tests := []string{
		filepath.Join(root, "pod-a", "volumes", kubeletCSIVolumeDir, "sandbox0-volume-0-state"),
		filepath.Join(root, "pod-a", "volumes", kubeletCSIVolumeDir, "other-volume", "mount"),
		filepath.Join(root, "pod-a", "volumes", "kubernetes.io~secret", "sandbox0-volume-0-state", "mount"),
		filepath.Join(root+"-other", "pod-a", "volumes", kubeletCSIVolumeDir, "sandbox0-volume-0-state", "mount"),
	}
	for _, path := range tests {
		if isSandboxCSIMountPath(root, path) {
			t.Fatalf("expected %q not to match sandbox CSI mount path", path)
		}
	}
}

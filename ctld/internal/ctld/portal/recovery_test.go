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

func TestCleanupStaleCSIMountsCleansBrokenActivePodMount(t *testing.T) {
	root := t.TempDir()
	target := filepath.Join(root, "pod-a", "volumes", kubeletCSIVolumeDir, "sandbox0-volume-1-workspace", "mount")
	if err := os.MkdirAll(target, 0o755); err != nil {
		t.Fatalf("MkdirAll(%q) error = %v", target, err)
	}

	var cleaned []string
	mgr := NewManager(Config{
		RootDir:         t.TempDir(),
		KubeletPodsRoot: root,
		ActivePodUIDLister: func(context.Context) (map[string]struct{}, error) {
			return map[string]struct{}{"pod-a": {}}, nil
		},
		StaleMountChecker: func(path string) (bool, error) {
			return path == target, nil
		},
		StaleMountCleaner: func(path string) error {
			cleaned = append(cleaned, path)
			return os.RemoveAll(path)
		},
	})
	if err := mgr.CleanupStaleCSIMounts(context.Background()); err != nil {
		t.Fatalf("CleanupStaleCSIMounts() error = %v", err)
	}
	if !slices.Equal(cleaned, []string{target}) {
		t.Fatalf("cleaned paths = %#v, want %#v", cleaned, []string{target})
	}
}

func TestCleanupStaleCSIMountsRecoversBrokenActivePodMount(t *testing.T) {
	root := t.TempDir()
	volumeName := "sandbox0-volume-1-workspace"
	target := filepath.Join(root, "pod-a", "volumes", kubeletCSIVolumeDir, volumeName, "mount")
	if err := os.MkdirAll(target, 0o755); err != nil {
		t.Fatalf("MkdirAll(%q) error = %v", target, err)
	}

	var recovered RecoverablePortal
	var recoveredRuntime RuntimeRecoveryTarget
	var portalsRecovered bool
	mgr := NewManager(Config{
		RootDir:         t.TempDir(),
		KubeletPodsRoot: root,
		ActivePodPortalLister: func(context.Context) (map[string]ActivePodPortals, error) {
			return map[string]ActivePodPortals{
				"pod-a": {Portals: map[string]RecoverablePortal{
					volumeName: {
						VolumeName: volumeName,
						Namespace:  "tpl-default",
						PodName:    "sandbox-a",
						PodUID:     "pod-a",
						PortalName: "workspace",
						MountPath:  "/workspace",
					},
				}, RuntimeRecovery: &RuntimeRecoveryTarget{
					Namespace:       "tpl-default",
					PodName:         "sandbox-a",
					PodUID:          "pod-a",
					ContainerName:   "procd",
					StateVolumeName: "sandbox0-volume-0-state",
				}},
			}, nil
		},
		StaleMountChecker: func(string) (bool, error) { return true, nil },
		StaleMountCleaner: func(path string) error { return os.RemoveAll(path) },
		StaleMountRecoverer: func(_ context.Context, path string, portal RecoverablePortal) error {
			if path != target {
				t.Fatalf("recovery path = %q, want %q", path, target)
			}
			recovered = portal
			return nil
		},
		ActivePodRuntimeRecoverer: func(_ context.Context, target RuntimeRecoveryTarget, rebuilt bool) error {
			recoveredRuntime = target
			portalsRecovered = rebuilt
			return nil
		},
	})
	if err := mgr.CleanupStaleCSIMounts(context.Background()); err != nil {
		t.Fatalf("CleanupStaleCSIMounts() error = %v", err)
	}
	if recovered.PodUID != "pod-a" || recovered.VolumeName != volumeName {
		t.Fatalf("recovered portal = %#v", recovered)
	}
	if recoveredRuntime.PodUID != "pod-a" || !portalsRecovered {
		t.Fatalf("runtime recovery = %#v, portals recovered = %v", recoveredRuntime, portalsRecovered)
	}
}

func TestCleanupStaleCSIMountsChecksPendingRuntimeWithoutRebuildingPortal(t *testing.T) {
	var called bool
	mgr := NewManager(Config{
		RootDir:         t.TempDir(),
		KubeletPodsRoot: t.TempDir(),
		ActivePodPortalLister: func(context.Context) (map[string]ActivePodPortals, error) {
			return map[string]ActivePodPortals{
				"pod-a": {
					Portals: map[string]RecoverablePortal{},
					RuntimeRecovery: &RuntimeRecoveryTarget{
						Namespace:       "tpl-default",
						PodName:         "sandbox-a",
						PodUID:          "pod-a",
						ContainerName:   "procd",
						StateVolumeName: "sandbox0-volume-0-state",
					},
				},
			}, nil
		},
		ActivePodRuntimeRecoverer: func(_ context.Context, _ RuntimeRecoveryTarget, rebuilt bool) error {
			called = true
			if rebuilt {
				t.Fatal("rebuilt = true, want false")
			}
			return nil
		},
	})
	if err := mgr.CleanupStaleCSIMounts(context.Background()); err != nil {
		t.Fatalf("CleanupStaleCSIMounts() error = %v", err)
	}
	if !called {
		t.Fatal("runtime recoverer was not called")
	}
}

func TestShouldCleanSandboxCSIMountCleansBrokenActivePodMount(t *testing.T) {
	info := csiMountPathInfo{podUID: "pod-a"}
	activePods := map[string]struct{}{"pod-a": {}}

	if shouldCleanSandboxCSIMount(info, activePods, true, false) {
		t.Fatal("readable active pod mount should not be cleaned")
	}
	if !shouldCleanSandboxCSIMount(info, activePods, true, true) {
		t.Fatal("broken active pod mount should be cleaned")
	}
}

func TestUnpublishUnknownPortalCleansSandboxCSITarget(t *testing.T) {
	root := t.TempDir()
	target := filepath.Join(root, "pod-a", "volumes", kubeletCSIVolumeDir, "sandbox0-volume-1-workspace", "mount")
	var cleaned string
	mgr := &Manager{
		kubeletPodsRoot: root,
		staleMountCleaner: func(path string) error {
			cleaned = path
			return nil
		},
	}

	if err := mgr.UnpublishPortalContext(context.Background(), target); err != nil {
		t.Fatalf("UnpublishPortalContext() error = %v", err)
	}
	if cleaned != target {
		t.Fatalf("cleaned target = %q, want %q", cleaned, target)
	}
}

func TestUnpublishUnknownPortalRejectsTargetOutsideSandboxCSIPaths(t *testing.T) {
	root := t.TempDir()
	target := filepath.Join(root, "pod-a", "volumes", kubeletCSIVolumeDir, "other-volume", "mount")
	cleaned := false
	mgr := &Manager{
		kubeletPodsRoot: root,
		staleMountCleaner: func(string) error {
			cleaned = true
			return nil
		},
	}

	if err := mgr.UnpublishPortalContext(context.Background(), target); err == nil {
		t.Fatal("UnpublishPortalContext() error = nil, want unsafe target error")
	}
	if cleaned {
		t.Fatal("unsafe target was passed to stale mount cleaner")
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

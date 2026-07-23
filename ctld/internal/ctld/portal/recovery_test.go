package portal

import (
	"context"
	"errors"
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

func TestCleanupStalePortalsRemovesOnlyInactivePodState(t *testing.T) {
	root := t.TempDir()
	staleTarget := filepath.Join(root, "stale-target")
	activeTarget := filepath.Join(root, "active-target")
	mgr := NewManager(Config{
		RootDir: root,
		ActivePodUIDLister: func(context.Context) (map[string]struct{}, error) {
			return map[string]struct{}{"active-pod": {}}, nil
		},
		StaleMountCleaner: func(string) error { return nil },
	})
	stale := &portalMount{
		podUID:            "stale-pod",
		name:              "workspace",
		targetPath:        staleTarget,
		rootfsBackingPath: filepath.Join(root, "stale-rootfs"),
	}
	active := &portalMount{
		podUID:            "active-pod",
		name:              "workspace",
		targetPath:        activeTarget,
		rootfsBackingPath: filepath.Join(root, "active-rootfs"),
	}
	mgr.portals[portalKey(stale.podUID, stale.name)] = stale
	mgr.portals[portalKey(active.podUID, active.name)] = active
	mgr.portalsByTarget[staleTarget] = stale
	mgr.portalsByTarget[activeTarget] = active

	if err := mgr.CleanupStalePortals(context.Background()); err != nil {
		t.Fatalf("CleanupStalePortals() error = %v", err)
	}
	if _, ok := mgr.portals[portalKey(stale.podUID, stale.name)]; ok {
		t.Fatal("stale portal remains registered")
	}
	if _, ok := mgr.portals[portalKey(active.podUID, active.name)]; !ok {
		t.Fatal("active portal was removed")
	}
}

func TestCleanupStalePortalsFailsClosedWhenPodListingFails(t *testing.T) {
	mgr := NewManager(Config{
		RootDir: t.TempDir(),
		ActivePodUIDLister: func(context.Context) (map[string]struct{}, error) {
			return nil, errors.New("kubernetes unavailable")
		},
	})
	pm := &portalMount{podUID: "pod-a", name: "workspace", targetPath: "/target"}
	mgr.portals[portalKey(pm.podUID, pm.name)] = pm
	mgr.portalsByTarget[pm.targetPath] = pm

	if err := mgr.CleanupStalePortals(context.Background()); err != nil {
		t.Fatalf("CleanupStalePortals() error = %v", err)
	}
	if _, ok := mgr.portals[portalKey(pm.podUID, pm.name)]; !ok {
		t.Fatal("portal was removed without a reliable pod list")
	}
}

func TestPortalsForStandbySyncSkipsInactivePods(t *testing.T) {
	mgr := NewManager(Config{
		RootDir: t.TempDir(),
		ActivePodUIDLister: func(context.Context) (map[string]struct{}, error) {
			return map[string]struct{}{"active-pod": {}}, nil
		},
	})
	mgr.portals["stale"] = &portalMount{podUID: "stale-pod"}
	active := &portalMount{podUID: "active-pod"}
	mgr.portals["active"] = active

	portals, stale := mgr.portalsForStandbySync(context.Background())
	if stale != 1 {
		t.Fatalf("stale portal count = %d, want 1", stale)
	}
	if len(portals) != 1 || portals[0] != active {
		t.Fatalf("standby portals = %#v, want only active portal", portals)
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

func TestRestorePortalRetainsChannelWhenBackendRecoveryFails(t *testing.T) {
	root := t.TempDir()
	mgr := NewManager(Config{RootDir: root})
	channel, err := os.CreateTemp(t.TempDir(), "fuse-channel-*")
	if err != nil {
		t.Fatalf("CreateTemp(channel) error = %v", err)
	}
	defer channel.Close()
	manifest := RecoveryManifest{
		Version:           portalRecoveryVersion,
		Key:               "pod-a/workspace",
		PodUID:            "pod-a",
		Name:              "workspace",
		TargetPath:        filepath.Join(root, "target"),
		RootFSBackingPath: filepath.Join(root, "rootfs"),
		RootFSStatePath:   filepath.Join(root, "rootfs-state.jsonl"),
		VolumeID:          "volume-a",
		TeamID:            "team-a",
		InitRequest:       []byte{1},
	}

	if err := mgr.RestorePortal(context.Background(), manifest, channel); err == nil {
		t.Fatal("RestorePortal() error = nil, want unavailable registry error")
	}
	if _, err := channel.Stat(); err != nil {
		t.Fatalf("RestorePortal() closed caller channel on retryable error: %v", err)
	}
}

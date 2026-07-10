package portal

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestNodeFSJournalRoundTrip(t *testing.T) {
	root := t.TempDir()
	store, err := openNodeFSJournal(root, "node-a", 2)
	if err != nil {
		t.Fatalf("openNodeFSJournal() error = %v", err)
	}
	initial := store.Snapshot()
	if initial.ConnectionGeneration == "" {
		t.Fatal("connection generation is empty")
	}

	err = store.Update(func(state *nodeFSJournal) error {
		state.NextSlotByShard[1] = 8
		state.Shards = append(state.Shards, nodeFSShardState{
			Index:     1,
			Tag:       "ctld:node-a:1",
			MountPath: "/var/lib/kubelet/plugins/volume.sandbox0.ai/nodefs/shard-1",
		})
		state.Portals = append(state.Portals, nodeFSPortalState{
			PortalKey:     portalKey("pod-1", "workspace"),
			PodUID:        "pod-1",
			Namespace:     "tpl-default",
			PodName:       "sandbox-a",
			Name:          "workspace",
			MountPath:     "/workspace",
			TargetPath:    "/var/lib/kubelet/pods/pod-1/volumes/kubernetes.io~csi/sandbox0-volume-1-workspace/mount",
			Shard:         1,
			Slot:          7,
			RootFSBacking: "/var/lib/sandbox0/ctld/rootfs-portals/pod-1/workspace",
			Backend:       "rootfs",
			Phase:         nodeFSPortalPublished,
		})
		return nil
	})
	if err != nil {
		t.Fatalf("Update() error = %v", err)
	}

	reopened, err := openNodeFSJournal(root, "node-a", 2)
	if err != nil {
		t.Fatalf("openNodeFSJournal(reopen) error = %v", err)
	}
	got := reopened.Snapshot()
	if got.ConnectionGeneration != initial.ConnectionGeneration {
		t.Fatalf("connection generation = %q, want %q", got.ConnectionGeneration, initial.ConnectionGeneration)
	}
	if got.NextSlotByShard[1] != 8 || len(got.Portals) != 1 || got.Portals[0].Slot != 7 {
		t.Fatalf("reopened journal = %+v", got)
	}
	info, err := os.Stat(filepath.Join(root, nodeFSStateDirName, nodeFSStateFileName))
	if err != nil {
		t.Fatalf("stat journal error = %v", err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("journal mode = %o, want 600", info.Mode().Perm())
	}
}

func TestNodeFSJournalRejectsIncompatibleNodeAndShardCount(t *testing.T) {
	root := t.TempDir()
	if _, err := openNodeFSJournal(root, "node-a", 2); err != nil {
		t.Fatalf("openNodeFSJournal() error = %v", err)
	}
	if _, err := openNodeFSJournal(root, "node-b", 2); err == nil {
		t.Fatal("openNodeFSJournal(other node) error = nil")
	}
	if _, err := openNodeFSJournal(root, "node-a", 3); err == nil {
		t.Fatal("openNodeFSJournal(other shard count) error = nil")
	}
}

func TestNodeFSJournalFailedUpdateDoesNotMutateCommittedState(t *testing.T) {
	store, err := openNodeFSJournal(t.TempDir(), "node-a", 1)
	if err != nil {
		t.Fatalf("openNodeFSJournal() error = %v", err)
	}
	wantErr := errors.New("stop")
	err = store.Update(func(state *nodeFSJournal) error {
		state.NextSlotByShard[0] = 99
		return wantErr
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("Update() error = %v, want %v", err, wantErr)
	}
	if got := store.Snapshot().NextSlotByShard[0]; got != 1 {
		t.Fatalf("next slot after failed update = %d, want 1", got)
	}
}

func TestNodeFSJournalPortalLifecycleNeverReusesSlot(t *testing.T) {
	store, err := openNodeFSJournal(t.TempDir(), "node-a", 1)
	if err != nil {
		t.Fatalf("openNodeFSJournal() error = %v", err)
	}
	first, err := store.AllocatePortal(nodeFSPortalState{
		PortalKey:  portalKey("pod-a", "workspace"),
		PodUID:     "pod-a",
		Name:       "workspace",
		TargetPath: "/kubelet/pod-a/workspace",
	})
	if err != nil {
		t.Fatalf("AllocatePortal(first) error = %v", err)
	}
	if first.Slot != 1 || first.Phase != nodeFSPortalAllocating {
		t.Fatalf("first allocation = %+v", first)
	}
	duplicate, err := store.AllocatePortal(nodeFSPortalState{
		PortalKey:  first.PortalKey,
		PodUID:     first.PodUID,
		Name:       first.Name,
		TargetPath: first.TargetPath,
	})
	if err != nil || duplicate.Slot != first.Slot {
		t.Fatalf("AllocatePortal(idempotent) = %+v, %v", duplicate, err)
	}
	if err := store.MarkPortalPublished(first.PortalKey); err != nil {
		t.Fatalf("MarkPortalPublished() error = %v", err)
	}
	mountedAt := time.Now().UTC().Truncate(time.Nanosecond)
	if err := store.UpdatePortalBinding(first.PortalKey, "s0fs", "vol-a", "team-a", mountedAt); err != nil {
		t.Fatalf("UpdatePortalBinding() error = %v", err)
	}
	bound, ok := store.Portal(first.PortalKey)
	if !ok || bound.VolumeID != "vol-a" || bound.Backend != "s0fs" || !bound.MountedAt.Equal(mountedAt) {
		t.Fatalf("bound portal = %+v, ok=%v", bound, ok)
	}
	if err := store.BeginPortalUnpublish(first.PortalKey); err != nil {
		t.Fatalf("BeginPortalUnpublish() error = %v", err)
	}
	if err := store.RemovePortal(first.PortalKey); err != nil {
		t.Fatalf("RemovePortal() error = %v", err)
	}
	second, err := store.AllocatePortal(nodeFSPortalState{
		PortalKey:  portalKey("pod-b", "workspace"),
		PodUID:     "pod-b",
		Name:       "workspace",
		TargetPath: "/kubelet/pod-b/workspace",
	})
	if err != nil {
		t.Fatalf("AllocatePortal(second) error = %v", err)
	}
	if second.Slot != 2 {
		t.Fatalf("second slot = %d, want 2", second.Slot)
	}
}

func TestNodeFSJournalRejectsPortalAllocationConflict(t *testing.T) {
	store, err := openNodeFSJournal(t.TempDir(), "node-a", 1)
	if err != nil {
		t.Fatalf("openNodeFSJournal() error = %v", err)
	}
	portal := nodeFSPortalState{PortalKey: "portal-a", PodUID: "pod-a", Name: "workspace", TargetPath: "/target-a"}
	if _, err := store.AllocatePortal(portal); err != nil {
		t.Fatalf("AllocatePortal() error = %v", err)
	}
	portal.TargetPath = "/other-target"
	if _, err := store.AllocatePortal(portal); err == nil {
		t.Fatal("AllocatePortal(conflict) error = nil")
	}
}

func TestNodeFSJournalRejectsDuplicateSlots(t *testing.T) {
	state := newNodeFSJournal("node-a", 1)
	state.Portals = append(state.Portals,
		nodeFSPortalState{PortalKey: "a", PodUID: "pod-a", Name: "workspace", Shard: 0, Slot: 1, Phase: nodeFSPortalPublished},
		nodeFSPortalState{PortalKey: "b", PodUID: "pod-b", Name: "workspace", Shard: 0, Slot: 1, Phase: nodeFSPortalPublished},
	)
	state.NextSlotByShard[0] = 2
	if err := validateNodeFSJournal(state); err == nil {
		t.Fatal("validateNodeFSJournal(duplicate slot) error = nil")
	}
}

func TestNodeFSProcessLockFencesConcurrentOwner(t *testing.T) {
	root := t.TempDir()
	first, err := acquireNodeFSProcessLock(root)
	if err != nil {
		t.Fatalf("acquireNodeFSProcessLock(first) error = %v", err)
	}
	if _, err := acquireNodeFSProcessLock(root); err == nil {
		t.Fatal("acquireNodeFSProcessLock(second) error = nil")
	}
	if err := first.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	second, err := acquireNodeFSProcessLock(root)
	if err != nil {
		t.Fatalf("acquireNodeFSProcessLock(after release) error = %v", err)
	}
	if err := second.Close(); err != nil {
		t.Fatalf("Close(second) error = %v", err)
	}
}

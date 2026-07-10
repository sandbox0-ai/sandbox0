package portal

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/ctld/internal/ctld/portal/nodefs"
	"github.com/sandbox0-ai/sandbox0/pkg/ctldapi"
	"github.com/sandbox0-ai/sandbox0/pkg/volumefuse"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/s0fs"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/volume"
	pb "github.com/sandbox0-ai/sandbox0/storage-proxy/proto/fs"
)

func TestReserveBoundVolumeSharesROXAndRollsBack(t *testing.T) {
	mgr := &Manager{
		portals: map[string]*portalMount{
			"other": {mountPath: "/shared", volumeID: "volume-a"},
		},
		boundVolumes: make(map[string]*boundVolume),
		volumes:      newLocalVolumeManager(),
	}
	bound := &boundVolume{
		volumeID: "volume-a",
		teamID:   "team-a",
		access:   volume.AccessModeROX,
		refCount: 1,
		session:  unboundSession{},
	}
	mgr.boundVolumes[bound.volumeID] = bound
	req := ctldapi.BindVolumePortalRequest{SandboxVolumeID: bound.volumeID, TeamID: bound.teamID}
	reserved, created, err := mgr.reserveBoundVolume(context.Background(), req, nil, volume.AccessModeROX, "new")
	if err != nil {
		t.Fatalf("reserveBoundVolume() error = %v", err)
	}
	if reserved != bound || created || bound.refCount != 2 {
		t.Fatalf("reservation = (%p, %v), refCount=%d", reserved, created, bound.refCount)
	}
	if err := mgr.rollbackBoundVolumeReservation(context.Background(), bound, false); err != nil {
		t.Fatalf("rollbackBoundVolumeReservation() error = %v", err)
	}
	if bound.refCount != 1 {
		t.Fatalf("refCount after rollback = %d, want 1", bound.refCount)
	}
}

func TestReserveBoundVolumeRejectsSecondRWOWithPortalPath(t *testing.T) {
	mgr := &Manager{
		portals: map[string]*portalMount{
			"other": {mountPath: "/workspace", volumeID: "volume-a"},
		},
		boundVolumes: map[string]*boundVolume{
			"volume-a": {
				volumeID: "volume-a",
				teamID:   "team-a",
				access:   volume.AccessModeRWO,
				refCount: 1,
				session:  unboundSession{},
			},
		},
		volumes: newLocalVolumeManager(),
	}
	req := ctldapi.BindVolumePortalRequest{SandboxVolumeID: "volume-a", TeamID: "team-a"}
	_, _, err := mgr.reserveBoundVolume(context.Background(), req, nil, volume.AccessModeRWO, "new")
	if err == nil || !strings.Contains(err.Error(), "/workspace") {
		t.Fatalf("reserveBoundVolume() error = %v, want conflict path", err)
	}
	if mgr.boundVolumes["volume-a"].refCount != 1 {
		t.Fatalf("refCount after rejected reservation = %d, want 1", mgr.boundVolumes["volume-a"].refCount)
	}
}

func TestLegacyRebindRestoresSessionAfterCleanupFailure(t *testing.T) {
	mgr, pm, bound := cleanupFailedS0FSBinding(t)
	req := ctldapi.BindVolumePortalRequest{SandboxVolumeID: bound.volumeID, TeamID: bound.teamID}
	reserved, created, err := mgr.reserveBoundVolume(context.Background(), req, nil, volume.AccessModeRWO, portalKey(pm.podUID, pm.name))
	if err != nil {
		t.Fatalf("reserveBoundVolume() error = %v", err)
	}
	if reserved != bound || created || bound.session == nil {
		t.Fatalf("reservation = (%p, %v), session=%T", reserved, created, bound.session)
	}
	mgr.mu.Lock()
	mgr.attachPortalLocked(pm, bound, time.Now().UTC())
	mgr.mu.Unlock()
	if pm.volumeID != bound.volumeID || pm.fs == nil {
		t.Fatalf("legacy portal after rebind = %+v", pm)
	}
	if _, err := bound.session.GetAttr(context.Background(), &pb.GetAttrRequest{Inode: s0fs.RootInode}); err != nil {
		t.Fatalf("restored legacy session GetAttr() error = %v", err)
	}
}

func TestNodeFSRebindRestoresSessionAfterCleanupFailure(t *testing.T) {
	mgr, pm, bound := cleanupFailedS0FSBinding(t)
	mux := nodefs.NewSessionMux()
	if err := mux.RegisterPortal(nodefs.PortalSpec{
		Name:       "p00001",
		Slot:       1,
		Generation: 1,
		VolumeID:   portalKey(pm.podUID, pm.name),
		RootInode:  1,
		Session:    unboundSession{},
	}); err != nil {
		t.Fatal(err)
	}
	req := ctldapi.BindVolumePortalRequest{SandboxVolumeID: bound.volumeID, TeamID: bound.teamID}
	reserved, created, err := mgr.reserveBoundVolume(context.Background(), req, nil, volume.AccessModeRWO, portalKey(pm.podUID, pm.name))
	if err != nil {
		t.Fatalf("reserveBoundVolume() error = %v", err)
	}
	if reserved != bound || created || bound.session == nil {
		t.Fatalf("reservation = (%p, %v), session=%T", reserved, created, bound.session)
	}
	if _, err := mux.UpdatePortalSession("p00001", bound.volumeID, 2, bound.session); err != nil {
		t.Fatalf("UpdatePortalSession() error = %v", err)
	}
	root, err := nodefs.EncodeNodeID(1, s0fs.RootInode)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := mux.GetAttr(context.Background(), &pb.GetAttrRequest{Inode: root}); err != nil {
		t.Fatalf("restored nodefs session GetAttr() error = %v", err)
	}
}

func TestReserveBoundVolumeDoesNotRebuildS3Session(t *testing.T) {
	mgr := &Manager{
		portals:      make(map[string]*portalMount),
		boundVolumes: make(map[string]*boundVolume),
		volumes:      newLocalVolumeManager(),
	}
	bound := &boundVolume{
		volumeID: "volume-a",
		teamID:   "team-a",
		access:   volume.AccessModeRWO,
		volCtx:   &volume.VolumeContext{VolumeID: "volume-a", TeamID: "team-a", Backend: volume.BackendS3},
	}
	mgr.boundVolumes[bound.volumeID] = bound
	mgr.volumes.add(bound.volCtx)
	req := ctldapi.BindVolumePortalRequest{SandboxVolumeID: bound.volumeID, TeamID: bound.teamID}
	if _, _, err := mgr.reserveBoundVolume(context.Background(), req, nil, volume.AccessModeRWO, "portal"); err == nil {
		t.Fatal("reserveBoundVolume() error = nil for missing S3 session")
	}
}

func TestReserveBoundVolumeDoesNotRebuildClosedS0FSSession(t *testing.T) {
	engine, _ := newDirtyConflictS0FSEngine(t, "volume-a")
	if err := engine.Close(); err != nil {
		t.Fatal(err)
	}
	mgr := &Manager{
		portals:      make(map[string]*portalMount),
		boundVolumes: make(map[string]*boundVolume),
		volumes:      newLocalVolumeManager(),
	}
	bound := &boundVolume{
		volumeID: "volume-a",
		teamID:   "team-a",
		access:   volume.AccessModeRWO,
		volCtx: &volume.VolumeContext{
			VolumeID:  "volume-a",
			TeamID:    "team-a",
			Backend:   volume.BackendS0FS,
			S0FS:      engine,
			RootInode: 1,
		},
	}
	mgr.boundVolumes[bound.volumeID] = bound
	mgr.volumes.add(bound.volCtx)
	req := ctldapi.BindVolumePortalRequest{SandboxVolumeID: bound.volumeID, TeamID: bound.teamID}
	if _, _, err := mgr.reserveBoundVolume(context.Background(), req, nil, volume.AccessModeRWO, "portal"); err == nil {
		t.Fatal("reserveBoundVolume() error = nil for closed S0FS context")
	}
}

func cleanupFailedS0FSBinding(t *testing.T) (*Manager, *portalMount, *boundVolume) {
	t.Helper()
	engine, cacheDir := newDirtyConflictS0FSEngine(t, "volume-a")
	t.Cleanup(func() { _ = engine.Close() })
	mgr := &Manager{
		portals:         make(map[string]*portalMount),
		portalsByTarget: make(map[string]*portalMount),
		boundVolumes:    make(map[string]*boundVolume),
		volumes:         newLocalVolumeManager(),
	}
	volCtx := &volume.VolumeContext{
		VolumeID:  "volume-a",
		TeamID:    "team-a",
		Backend:   volume.BackendS0FS,
		S0FS:      engine,
		Access:    volume.AccessModeRWO,
		MountedAt: time.Now().UTC(),
		RootInode: 1,
		RootPath:  "/",
		CacheDir:  cacheDir,
	}
	bound := &boundVolume{
		volumeID: "volume-a",
		teamID:   "team-a",
		access:   volume.AccessModeRWO,
		refCount: 1,
		volCtx:   volCtx,
		session:  newLocalSession("volume-a", mgr.volumes, nil),
	}
	pm := &portalMount{
		podUID:        "pod-a",
		name:          "workspace",
		mountPath:     "/workspace",
		targetPath:    "/target",
		fs:            volumefuse.New("portal", time.Second, unboundSession{}),
		rootfsSession: unboundSession{},
		volumeID:      bound.volumeID,
		teamID:        bound.teamID,
		mountedAt:     time.Now().UTC(),
	}
	key := portalKey(pm.podUID, pm.name)
	mgr.portals[key] = pm
	mgr.portalsByTarget[pm.targetPath] = pm
	mgr.boundVolumes[bound.volumeID] = bound
	mgr.volumes.add(volCtx)

	mgr.mu.Lock()
	cleanup := mgr.unbindLockedSnapshot(pm)
	mgr.mu.Unlock()
	if err := mgr.finishBoundVolumeCleanup(context.Background(), cleanup); err == nil {
		t.Fatal("finishBoundVolumeCleanup() error = nil, want materialize conflict")
	}
	if bound.closing || bound.refCount != 0 || bound.session == nil || bound.materializeCancel == nil || bound.materializeDone == nil {
		t.Fatalf("bound after failed cleanup = %+v", bound)
	}
	return mgr, pm, bound
}

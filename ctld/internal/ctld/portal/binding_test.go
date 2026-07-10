package portal

import (
	"context"
	"strings"
	"testing"

	"github.com/sandbox0-ai/sandbox0/pkg/ctldapi"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/volume"
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

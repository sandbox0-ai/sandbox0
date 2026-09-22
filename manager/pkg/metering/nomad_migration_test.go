package metering

import (
	"testing"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	meteringpkg "github.com/sandbox0-ai/sandbox0/pkg/metering"
)

func TestNomadMigrationRetainsOneContinuousUserRuntimeWindow(t *testing.T) {
	projector := &NomadLifecycleProjector{regionID: "region-1"}
	claimedAt, activeAt := nomadMeteringTestTime(0), nomadMeteringTestTime(1)
	source := nomadMeteringTestSource(claimedAt, &activeAt)
	prior, err := projector.project(source, nil, nil, 1)
	if err != nil {
		t.Fatal(err)
	}
	abortedAt, committedAt, pausedAt := nomadMeteringTestTime(2), nomadMeteringTestTime(3), nomadMeteringTestTime(4)
	transitions := []nomadMeteringLifecycleTransition{
		{ID: "expired-reservation", Kind: sandboxstore.SandboxLifecycleKindMigrate, Source: sandboxstore.SandboxLifecycleSourceAuto,
			Phase: sandboxstore.SandboxLifecyclePhaseAborted, Epoch: 1, AbortedAt: &abortedAt},
		{ID: "completed-migration", Kind: sandboxstore.SandboxLifecycleKindMigrate, Source: sandboxstore.SandboxLifecycleSourceAuto,
			Phase: sandboxstore.SandboxLifecyclePhaseCommitted, Epoch: 2, CommittedAt: &committedAt},
	}
	migrated, err := projector.project(source, prior.state, transitions, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(migrated.events) != 0 || len(migrated.windows) != 0 || migrated.state.ActiveSince == nil ||
		!migrated.state.ActiveSince.Equal(activeAt) || migrated.state.SourceLifecycleEpoch != 2 {
		t.Fatalf("migration must not duplicate user compute usage: %+v", migrated)
	}
	transitions = append(transitions, nomadMeteringLifecycleTransition{ID: "pause", Kind: sandboxstore.SandboxLifecycleKindPause,
		Phase: sandboxstore.SandboxLifecyclePhaseCommitted, Epoch: 3, CommittedAt: &pausedAt})
	source.DesiredState = sandboxstore.SandboxDesiredStatePaused
	paused, err := projector.project(source, migrated.state, transitions, 3)
	if err != nil {
		t.Fatal(err)
	}
	if len(paused.events) != 1 || paused.events[0].EventType != meteringpkg.EventTypeSandboxPaused || len(paused.windows) != 1 {
		t.Fatalf("pause must close one uninterrupted runtime window: %+v", paused)
	}
	assertNomadRuntimeWindow(t, paused.windows[0], activeAt, pausedAt, 1024)
}

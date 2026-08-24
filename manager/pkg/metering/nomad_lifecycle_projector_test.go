package metering

import (
	"math"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	meteringpkg "github.com/sandbox0-ai/sandbox0/pkg/metering"
)

func TestNomadLifecycleProjectionReplaysTransitionsBetweenPasses(t *testing.T) {
	projector := &NomadLifecycleProjector{regionID: "region-1", clusterID: "cluster-default"}
	claimedAt := nomadMeteringTestTime(0)
	activeAt := nomadMeteringTestTime(1)
	pauseOneAt := nomadMeteringTestTime(2)
	resumeAt := nomadMeteringTestTime(3)
	pauseTwoAt := nomadMeteringTestTime(4)
	source := nomadMeteringTestSource(claimedAt, &activeAt)
	source.DesiredState = sandboxstore.SandboxDesiredStatePaused
	source.ObservedAt = nomadMeteringTestTime(5)

	mutations, err := projector.project(source, nil, []nomadMeteringLifecycleTransition{
		{ID: "pause-1", Kind: sandboxstore.SandboxLifecycleKindPause, Phase: sandboxstore.SandboxLifecyclePhaseCommitted, Epoch: 1, CommittedAt: &pauseOneAt},
		{ID: "resume-1", Kind: sandboxstore.SandboxLifecycleKindResume, Phase: sandboxstore.SandboxLifecyclePhaseCommitted, Epoch: 2, CommittedAt: &resumeAt},
		{ID: "pause-2", Kind: sandboxstore.SandboxLifecycleKindPause, Phase: sandboxstore.SandboxLifecyclePhaseCommitted, Epoch: 3, CommittedAt: &pauseTwoAt},
	}, 17)
	if err != nil {
		t.Fatal(err)
	}
	if got := eventTypes(mutations.events); !equalStrings(got, []string{
		meteringpkg.EventTypeSandboxClaimed,
		meteringpkg.EventTypeSandboxPaused,
		meteringpkg.EventTypeSandboxResumed,
		meteringpkg.EventTypeSandboxPaused,
	}) {
		t.Fatalf("event types = %#v", got)
	}
	if len(mutations.windows) != 2 {
		t.Fatalf("windows = %#v, want two active intervals", mutations.windows)
	}
	assertNomadRuntimeWindow(t, mutations.windows[0], activeAt, pauseOneAt, 1024)
	assertNomadRuntimeWindow(t, mutations.windows[1], resumeAt, pauseTwoAt, 1024)
	state := mutations.state
	if !state.Paused || state.ActiveSince != nil || state.PausedAt == nil || !state.PausedAt.Equal(pauseTwoAt) {
		t.Fatalf("paused state = %#v", state)
	}
	if state.SourceRevision != 17 || state.SourceLifecycleEpoch != 3 || state.LastResourceVer != "nomad/17/3" {
		t.Fatalf("source cursor = %#v", state)
	}
}

func TestNomadLifecycleProjectionPausedForkHasNoRuntimeWindow(t *testing.T) {
	projector := &NomadLifecycleProjector{regionID: "region-1"}
	claimedAt := nomadMeteringTestTime(0)
	source := nomadMeteringTestSource(claimedAt, nil)
	source.DesiredState = sandboxstore.SandboxDesiredStatePaused

	mutations, err := projector.project(source, nil, nil, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(mutations.events) != 1 || mutations.events[0].EventType != meteringpkg.EventTypeSandboxClaimed {
		t.Fatalf("events = %#v, want claimed only", mutations.events)
	}
	if len(mutations.windows) != 0 || !mutations.state.Paused || mutations.state.ActiveSince != nil {
		t.Fatalf("paused fork projection = %#v, windows=%#v", mutations.state, mutations.windows)
	}
}

func TestNomadLifecycleProjectionStartsRuntimeAtDelayedCommandReady(t *testing.T) {
	projector := &NomadLifecycleProjector{regionID: "region-1"}
	claimedAt := nomadMeteringTestTime(0)
	initial := nomadMeteringTestSource(claimedAt, nil)

	claimed, err := projector.project(initial, nil, nil, 1)
	if err != nil {
		t.Fatal(err)
	}
	if claimed.state.Paused || claimed.state.ActiveSince != nil || len(claimed.events) != 1 {
		t.Fatalf("pre-ready projection = %#v", claimed)
	}

	activeAt := nomadMeteringTestTime(1)
	ready := nomadMeteringTestSource(claimedAt, &activeAt)
	activated, err := projector.project(ready, claimed.state, nil, 2)
	if err != nil {
		t.Fatal(err)
	}
	if activated.state.ActiveSince == nil || !activated.state.ActiveSince.Equal(activeAt) {
		t.Fatalf("active_since = %v, want %s", activated.state.ActiveSince, activeAt)
	}
	if len(activated.events) != 0 || len(activated.windows) != 0 {
		t.Fatalf("command-ready repair emitted history: %#v", activated)
	}
}

func TestNomadLifecycleProjectionIgnoresDuplicateTerminalStateTransitions(t *testing.T) {
	projector := &NomadLifecycleProjector{regionID: "region-1"}
	claimedAt := nomadMeteringTestTime(0)
	activeAt := nomadMeteringTestTime(1)
	pauseAt := nomadMeteringTestTime(2)
	duplicatePauseAt := pauseAt.Add(10 * time.Second)
	resumeAt := nomadMeteringTestTime(3)
	duplicateResumeAt := resumeAt.Add(10 * time.Second)
	finalPauseAt := nomadMeteringTestTime(4)
	source := nomadMeteringTestSource(claimedAt, &activeAt)
	source.DesiredState = sandboxstore.SandboxDesiredStatePaused

	mutations, err := projector.project(source, nil, []nomadMeteringLifecycleTransition{
		{ID: "pause", Kind: sandboxstore.SandboxLifecycleKindPause, Phase: sandboxstore.SandboxLifecyclePhaseCommitted, Epoch: 1, CommittedAt: &pauseAt},
		{ID: "duplicate-pause", Kind: sandboxstore.SandboxLifecycleKindPause, Phase: sandboxstore.SandboxLifecyclePhaseCommitted, Epoch: 2, CommittedAt: &duplicatePauseAt},
		{ID: "resume", Kind: sandboxstore.SandboxLifecycleKindResume, Phase: sandboxstore.SandboxLifecyclePhaseCommitted, Epoch: 3, CommittedAt: &resumeAt},
		{ID: "duplicate-resume", Kind: sandboxstore.SandboxLifecycleKindResume, Phase: sandboxstore.SandboxLifecyclePhaseCommitted, Epoch: 4, CommittedAt: &duplicateResumeAt},
		{ID: "final-pause", Kind: sandboxstore.SandboxLifecycleKindPause, Phase: sandboxstore.SandboxLifecyclePhaseCommitted, Epoch: 5, CommittedAt: &finalPauseAt},
	}, 6)
	if err != nil {
		t.Fatal(err)
	}
	if got := eventTypes(mutations.events); !equalStrings(got, []string{
		meteringpkg.EventTypeSandboxClaimed,
		meteringpkg.EventTypeSandboxPaused,
		meteringpkg.EventTypeSandboxResumed,
		meteringpkg.EventTypeSandboxPaused,
	}) {
		t.Fatalf("event types = %#v", got)
	}
	if len(mutations.windows) != 2 {
		t.Fatalf("windows = %#v", mutations.windows)
	}
	assertNomadRuntimeWindow(t, mutations.windows[0], activeAt, pauseAt, 1024)
	assertNomadRuntimeWindow(t, mutations.windows[1], resumeAt, finalPauseAt, 1024)
	if mutations.state.SourceLifecycleEpoch != 5 {
		t.Fatalf("lifecycle cursor = %d, want 5", mutations.state.SourceLifecycleEpoch)
	}
}

func TestNomadLifecycleProjectionTreatsExactCrashAbandonAsPause(t *testing.T) {
	projector := &NomadLifecycleProjector{regionID: "region-1"}
	claimedAt := nomadMeteringTestTime(0)
	activeAt := nomadMeteringTestTime(1)
	crashedAt := nomadMeteringTestTime(2)
	source := nomadMeteringTestSource(claimedAt, &activeAt)
	source.DesiredState = sandboxstore.SandboxDesiredStatePaused

	mutations, err := projector.project(source, nil, []nomadMeteringLifecycleTransition{{
		ID: "crash-abandon", Kind: sandboxstore.SandboxLifecycleKindPause,
		Phase: sandboxstore.SandboxLifecyclePhaseAborted, Source: sandboxstore.SandboxLifecycleSourceCrash,
		Epoch: 1, Error: sandboxstore.RootFSWriterCrashAbandonReason, AbortedAt: &crashedAt,
	}}, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(mutations.windows) != 1 || !mutations.state.Paused {
		t.Fatalf("crash projection = state %#v, windows %#v", mutations.state, mutations.windows)
	}
	if got := eventTypes(mutations.events); !equalStrings(got, []string{
		meteringpkg.EventTypeSandboxClaimed, meteringpkg.EventTypeSandboxPaused,
	}) {
		t.Fatalf("events = %#v", got)
	}

	abortedAt := nomadMeteringTestTime(3)
	activeSource := nomadMeteringTestSource(claimedAt, &activeAt)
	ignored, err := projector.project(activeSource, nil, []nomadMeteringLifecycleTransition{{
		ID: "canceled-pause", Kind: sandboxstore.SandboxLifecycleKindPause,
		Phase: sandboxstore.SandboxLifecyclePhaseAborted, Source: sandboxstore.SandboxLifecycleSourceManual,
		Epoch: 1, Error: "canceled", AbortedAt: &abortedAt,
	}}, 3)
	if err != nil {
		t.Fatal(err)
	}
	if ignored.state.Paused || ignored.state.ActiveSince == nil || len(ignored.windows) != 0 || len(ignored.events) != 1 {
		t.Fatalf("ordinary abort changed runtime state: %#v", ignored)
	}
	if ignored.state.SourceLifecycleEpoch != 1 {
		t.Fatalf("ordinary abort cursor = %d, want 1", ignored.state.SourceLifecycleEpoch)
	}
}

func TestNomadLifecycleProjectionTerminatesActiveAndPausedSandboxes(t *testing.T) {
	projector := &NomadLifecycleProjector{regionID: "region-1"}
	claimedAt := nomadMeteringTestTime(0)
	activeAt := nomadMeteringTestTime(1)
	pauseAt := nomadMeteringTestTime(2)
	deletedAt := nomadMeteringTestTime(4)

	t.Run("active", func(t *testing.T) {
		source := nomadMeteringTestSource(claimedAt, &activeAt)
		source.DesiredState = sandboxstore.SandboxDesiredStateDeleted
		source.DeletedAt = &deletedAt
		mutations, err := projector.project(source, nil, nil, 4)
		if err != nil {
			t.Fatal(err)
		}
		if len(mutations.windows) != 1 {
			t.Fatalf("windows = %#v", mutations.windows)
		}
		assertNomadRuntimeWindow(t, mutations.windows[0], activeAt, deletedAt, 1024)
		if got := eventTypes(mutations.events); !equalStrings(got, []string{
			meteringpkg.EventTypeSandboxClaimed, meteringpkg.EventTypeSandboxTerminated,
		}) {
			t.Fatalf("events = %#v", got)
		}
	})

	t.Run("paused", func(t *testing.T) {
		source := nomadMeteringTestSource(claimedAt, &activeAt)
		source.DesiredState = sandboxstore.SandboxDesiredStateDeleted
		source.DeletedAt = &deletedAt
		mutations, err := projector.project(source, nil, []nomadMeteringLifecycleTransition{{
			ID: "pause", Kind: sandboxstore.SandboxLifecycleKindPause,
			Phase: sandboxstore.SandboxLifecyclePhaseCommitted, Epoch: 1, CommittedAt: &pauseAt,
		}}, 5)
		if err != nil {
			t.Fatal(err)
		}
		if len(mutations.windows) != 1 {
			t.Fatalf("paused termination windows = %#v", mutations.windows)
		}
		assertNomadRuntimeWindow(t, mutations.windows[0], activeAt, pauseAt, 1024)
		if got := eventTypes(mutations.events); !equalStrings(got, []string{
			meteringpkg.EventTypeSandboxClaimed,
			meteringpkg.EventTypeSandboxPaused,
			meteringpkg.EventTypeSandboxTerminated,
		}) {
			t.Fatalf("events = %#v", got)
		}
	})
}

func TestNomadLifecycleProjectionRejectsUnmeterableResourcesAndChronology(t *testing.T) {
	projector := &NomadLifecycleProjector{regionID: "region-1"}
	claimedAt := nomadMeteringTestTime(0)
	activeAt := nomadMeteringTestTime(2)
	source := nomadMeteringTestSource(claimedAt, &activeAt)
	source.ResourceMemoryMiB = 0
	if _, err := projector.project(source, nil, nil, 1); err == nil {
		t.Fatal("zero resource projection succeeded")
	}

	source.ResourceMemoryMiB = 1024
	pauseAt := nomadMeteringTestTime(1)
	if _, err := projector.project(source, nil, []nomadMeteringLifecycleTransition{{
		ID: "backwards", Kind: sandboxstore.SandboxLifecycleKindPause,
		Phase: sandboxstore.SandboxLifecyclePhaseCommitted, Epoch: 1, CommittedAt: &pauseAt,
	}}, 2); err == nil {
		t.Fatal("backwards runtime window succeeded")
	}

	state := &meteringpkg.SandboxProjectionState{
		SandboxID: source.SandboxID, Namespace: "nomad", TeamID: source.TeamID,
		TemplateID: source.TemplateID, ClusterID: source.ClusterID,
		ResourceMillicpu: source.ResourceMillicpu, ResourceMemoryMiB: math.MaxInt64,
		ClaimedAt: &claimedAt, ActiveSince: &claimedAt, LastObservedAt: claimedAt,
	}
	source.ResourceMemoryMiB = math.MaxInt64
	deletedAt := nomadMeteringTestTime(4)
	source.DeletedAt = &deletedAt
	if _, err := projector.project(source, state, nil, 3); err == nil {
		t.Fatal("overflowing runtime window succeeded")
	}
}

func nomadMeteringTestSource(claimedAt time.Time, activeAt *time.Time) *nomadSandboxMeteringSource {
	return &nomadSandboxMeteringSource{
		SandboxID: "sandbox-1", TeamID: "team-1", UserID: "user-1",
		TemplateID: "template-1", ClusterID: "cluster-1",
		DesiredState:        sandboxstore.SandboxDesiredStateActive,
		AllocationNamespace: "default", OwnerKind: "team",
		ResourceMillicpu: 1000, ResourceMemoryMiB: 1024,
		ClaimedAt: &claimedAt, InitialActiveAt: activeAt,
		ObservedAt: nomadMeteringTestTime(10),
	}
}

func nomadMeteringTestTime(minute int) time.Time {
	return time.Date(2026, 8, 21, 1, minute, 0, 0, time.UTC)
}

func eventTypes(events []*meteringpkg.Event) []string {
	values := make([]string, 0, len(events))
	for _, event := range events {
		values = append(values, event.EventType)
	}
	return values
}

func equalStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func assertNomadRuntimeWindow(t *testing.T, window *meteringpkg.Window, start, end time.Time, memoryMiB int64) {
	t.Helper()
	if window == nil || !window.WindowStart.Equal(start) || !window.WindowEnd.Equal(end) {
		t.Fatalf("window = %#v, want %s..%s", window, start, end)
	}
	want := memoryMiB * end.Sub(start).Milliseconds()
	if window.Value != want || window.Unit != meteringpkg.WindowUnitMiBMilliseconds {
		t.Fatalf("window value = %d %s, want %d %s", window.Value, window.Unit, want, meteringpkg.WindowUnitMiBMilliseconds)
	}
}

package metering

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/controller"
	meteringpkg "github.com/sandbox0-ai/sandbox0/pkg/metering"
	obsmetrics "github.com/sandbox0-ai/sandbox0/pkg/observability/metrics"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

type fakeRecorder struct {
	events           []*meteringpkg.Event
	windows          []*meteringpkg.Window
	states           map[string]*meteringpkg.SandboxProjectionState
	getErr           error
	txErr            error
	appendErr        error
	watermarkErr     error
	stateUpsertErr   error
	watermarkHits    int
	transactionCalls int
}

type fakeTxStore struct {
	parent  *fakeRecorder
	events  []*meteringpkg.Event
	windows []*meteringpkg.Window
	states  map[string]*meteringpkg.SandboxProjectionState
}

func (f *fakeTxStore) AppendEvent(_ context.Context, event *meteringpkg.Event) error {
	if f.parent.appendErr != nil {
		return f.parent.appendErr
	}
	f.events = append(f.events, event)
	return nil
}

func (f *fakeTxStore) AppendWindow(_ context.Context, window *meteringpkg.Window) error {
	if f.parent.appendErr != nil {
		return f.parent.appendErr
	}
	f.windows = append(f.windows, window)
	return nil
}

func (f *fakeTxStore) UpsertProducerWatermark(context.Context, string, string, time.Time) error {
	if f.parent.watermarkErr != nil {
		return f.parent.watermarkErr
	}
	f.parent.watermarkHits++
	return nil
}

func (f *fakeTxStore) UpsertSandboxProjectionState(_ context.Context, state *meteringpkg.SandboxProjectionState) error {
	if f.parent.stateUpsertErr != nil {
		return f.parent.stateUpsertErr
	}
	if f.states == nil {
		f.states = map[string]*meteringpkg.SandboxProjectionState{}
	}
	copied := *state
	f.states[state.SandboxID] = &copied
	return nil
}

func (f *fakeRecorder) GetSandboxProjectionState(_ context.Context, sandboxID string) (*meteringpkg.SandboxProjectionState, error) {
	if f.getErr != nil {
		return nil, f.getErr
	}
	state := f.states[sandboxID]
	if state == nil {
		return nil, nil
	}
	copied := *state
	return &copied, nil
}

func (f *fakeRecorder) RunInTx(_ context.Context, fn func(tx txStore) error) error {
	if f.txErr != nil {
		return f.txErr
	}
	f.transactionCalls++
	stateCopy := map[string]*meteringpkg.SandboxProjectionState{}
	for key, value := range f.states {
		if value == nil {
			continue
		}
		copied := *value
		stateCopy[key] = &copied
	}
	tx := &fakeTxStore{
		parent: f,
		states: stateCopy,
	}
	if err := fn(tx); err != nil {
		return err
	}
	f.events = append(f.events, tx.events...)
	f.windows = append(f.windows, tx.windows...)
	f.states = tx.states
	return nil
}

func TestLifecycleProjectorRecordsClaimPauseResumeTerminate(t *testing.T) {
	recorder := &fakeRecorder{states: map[string]*meteringpkg.SandboxProjectionState{}}
	projector := NewLifecycleProjector(recorder, "aws-us-east-1", "cluster-a")

	now := time.Date(2026, 3, 12, 10, 0, 0, 0, time.UTC)
	projector.now = func() time.Time { return now }

	claimedAt := now.Add(-10 * time.Minute)
	pod := withSandboxResources(buildSandboxPod(claimedAt, false, "", "1"), "2", "1Gi")
	projector.handleUpsert(pod)
	if len(recorder.events) != 1 || recorder.events[0].EventType != meteringpkg.EventTypeSandboxClaimed {
		t.Fatalf("expected claim event, got %#v", recorder.events)
	}
	if len(recorder.windows) != 0 {
		t.Fatalf("claim should not emit windows, got %#v", recorder.windows)
	}

	pausedAt := now.Add(-2 * time.Minute)
	pausedPod := buildSandboxPod(claimedAt, true, pausedAt.Format(time.RFC3339), "2")
	projector.handleUpsert(pausedPod)
	if len(recorder.events) != 2 || recorder.events[1].EventType != meteringpkg.EventTypeSandboxPaused {
		t.Fatalf("expected pause event, got %#v", recorder.events)
	}
	if len(recorder.windows) != 1 || recorder.windows[0].WindowType != meteringpkg.WindowTypeSandboxRuntimeMiBMilliseconds {
		t.Fatalf("expected runtime window after pause, got %#v", recorder.windows)
	}
	if recorder.windows[0].Value != 491_520_000 {
		t.Fatalf("paused runtime value = %d, want 491520000", recorder.windows[0].Value)
	}

	projector.now = func() time.Time { return now.Add(time.Minute) }
	resumedPod := withSandboxResources(buildSandboxPod(claimedAt, false, "", "3"), "2", "1Gi")
	projector.handleUpsert(resumedPod)
	if len(recorder.events) != 3 || recorder.events[2].EventType != meteringpkg.EventTypeSandboxResumed {
		t.Fatalf("expected resume event, got %#v", recorder.events)
	}
	if len(recorder.windows) != 1 {
		t.Fatalf("window count after resume = %d, want 1", len(recorder.windows))
	}

	projector.now = func() time.Time { return now.Add(2 * time.Minute) }
	projector.handleDelete(resumedPod)
	if len(recorder.events) != 4 || recorder.events[3].EventType != meteringpkg.EventTypeSandboxTerminated {
		t.Fatalf("expected terminate event, got %#v", recorder.events)
	}
	if len(recorder.windows) != 2 || recorder.windows[1].WindowType != meteringpkg.WindowTypeSandboxRuntimeMiBMilliseconds {
		t.Fatalf("expected final runtime window, got %#v", recorder.windows)
	}
	if recorder.windows[1].Value != 61_440_000 {
		t.Fatalf("final runtime value = %d, want 61440000", recorder.windows[1].Value)
	}
}

func TestLifecycleProjectorIgnoresStatusOnlySandboxUpdates(t *testing.T) {
	recorder := &fakeRecorder{states: map[string]*meteringpkg.SandboxProjectionState{}}
	projector := NewLifecycleProjector(recorder, "aws-us-east-1", "cluster-a")
	claimedAt := time.Date(2026, 3, 12, 10, 0, 0, 0, time.UTC)
	oldPod := withSandboxResources(buildSandboxPod(claimedAt, false, "", "1"), "1", "1Gi")
	projector.handleUpsert(oldPod)

	newPod := oldPod.DeepCopy()
	newPod.ResourceVersion = "2"
	newPod.Status.Phase = corev1.PodRunning
	newPod.Status.PodIP = "10.0.0.10"
	projector.handleUpdate(oldPod, newPod)

	if recorder.transactionCalls != 1 {
		t.Fatalf("transaction calls = %d, want 1 after status-only update", recorder.transactionCalls)
	}
	if recorder.watermarkHits != 1 {
		t.Fatalf("watermark writes = %d, want 1 after status-only update", recorder.watermarkHits)
	}
	if state := recorder.states["sb-1"]; state == nil || state.LastResourceVer != "1" {
		t.Fatalf("state after status-only update = %#v, want original resource version", state)
	}
}

func TestLifecycleProjectorRetriesStatusOnlyUpdateAfterFailedProjection(t *testing.T) {
	recorder := &fakeRecorder{
		states: map[string]*meteringpkg.SandboxProjectionState{},
		txErr:  errors.New("postgres unavailable"),
	}
	projector := NewLifecycleProjector(recorder, "aws-us-east-1", "cluster-a")
	claimedAt := time.Date(2026, 3, 12, 10, 0, 0, 0, time.UTC)
	oldPod := withSandboxResources(buildSandboxPod(claimedAt, false, "", "1"), "1", "1Gi")
	projector.handleUpsert(oldPod)

	recorder.txErr = nil
	newPod := oldPod.DeepCopy()
	newPod.ResourceVersion = "2"
	newPod.Status.Phase = corev1.PodRunning
	projector.handleUpdate(oldPod, newPod)

	if recorder.transactionCalls != 1 {
		t.Fatalf("successful transaction calls = %d, want 1 retry", recorder.transactionCalls)
	}
	if len(recorder.events) != 1 || recorder.events[0].EventType != meteringpkg.EventTypeSandboxClaimed {
		t.Fatalf("events after retry = %#v, want claimed event", recorder.events)
	}
	if state := recorder.states["sb-1"]; state == nil || state.LastResourceVer != "2" {
		t.Fatalf("state after retry = %#v, want resource version 2", state)
	}
}

func TestLifecycleProjectorPersistsMeteringRelevantSandboxUpdates(t *testing.T) {
	recorder := &fakeRecorder{states: map[string]*meteringpkg.SandboxProjectionState{}}
	projector := NewLifecycleProjector(recorder, "aws-us-east-1", "cluster-a")
	claimedAt := time.Date(2026, 3, 12, 10, 0, 0, 0, time.UTC)
	oldPod := withSandboxResources(buildSandboxPod(claimedAt, false, "", "1"), "1", "1Gi")
	projector.handleUpsert(oldPod)

	newPod := withSandboxResources(oldPod.DeepCopy(), "2", "2Gi")
	newPod.ResourceVersion = "2"
	projector.handleUpdate(oldPod, newPod)

	if recorder.transactionCalls != 2 {
		t.Fatalf("transaction calls = %d, want 2 after resource update", recorder.transactionCalls)
	}
	state := recorder.states["sb-1"]
	if state == nil || state.ResourceMillicpu != 2000 || state.ResourceMemoryMiB != 2048 || state.LastResourceVer != "2" {
		t.Fatalf("state after resource update = %#v", state)
	}
}

func TestLifecycleProjectorRecordsErrorsInMetrics(t *testing.T) {
	recorder := &fakeRecorder{
		states: map[string]*meteringpkg.SandboxProjectionState{},
		getErr: errors.New("boom"),
	}
	projector := NewLifecycleProjector(recorder, "aws-us-east-1", "cluster-a")
	registry := prometheus.NewRegistry()
	projector.SetMetrics(obsmetrics.NewManager(registry))

	pod := buildSandboxPod(time.Date(2026, 3, 12, 10, 0, 0, 0, time.UTC), false, "", "1")
	projector.handleUpsert(pod)

	metricFamilies, err := registry.Gather()
	if err != nil {
		t.Fatalf("gather metrics: %v", err)
	}

	var found bool
	for _, family := range metricFamilies {
		if family.GetName() != "manager_metering_errors_total" {
			continue
		}
		for _, metric := range family.GetMetric() {
			if len(metric.GetLabel()) == 1 &&
				metric.GetLabel()[0].GetName() == "operation" &&
				metric.GetLabel()[0].GetValue() == "load_state" &&
				metric.GetCounter().GetValue() == 1 {
				found = true
			}
		}
	}
	if !found {
		t.Fatal("expected manager_metering_errors_total{operation=\"load_state\"} to be incremented")
	}
}

func TestLifecycleProjectorRecordsSandboxServerlessWindows(t *testing.T) {
	recorder := &fakeRecorder{states: map[string]*meteringpkg.SandboxProjectionState{}}
	projector := NewLifecycleProjector(recorder, "aws-us-east-1", "cluster-a")

	now := time.Date(2026, 3, 12, 10, 0, 0, 0, time.UTC)
	projector.now = func() time.Time { return now }
	claimedAt := now.Add(-2 * time.Second)
	pod := withSandboxResources(buildSandboxPod(claimedAt, false, "", "1"), "2", "1Gi")

	projector.handleDelete(pod)

	if len(recorder.windows) != 1 {
		t.Fatalf("window count = %d, want 1", len(recorder.windows))
	}
	if recorder.windows[0].WindowType != meteringpkg.WindowTypeSandboxRuntimeMiBMilliseconds {
		t.Fatalf("window type = %q, want %q", recorder.windows[0].WindowType, meteringpkg.WindowTypeSandboxRuntimeMiBMilliseconds)
	}
	if recorder.windows[0].Value != 2_048_000 {
		t.Fatalf("runtime value = %d, want 2048000", recorder.windows[0].Value)
	}
	if recorder.windows[0].Unit != meteringpkg.WindowUnitMiBMilliseconds {
		t.Fatalf("runtime unit = %q, want %q", recorder.windows[0].Unit, meteringpkg.WindowUnitMiBMilliseconds)
	}
}

func TestLifecycleProjectorRecordsPauseFromDurableStatusOnPodDelete(t *testing.T) {
	recorder := &fakeRecorder{states: map[string]*meteringpkg.SandboxProjectionState{}}
	projector := NewLifecycleProjector(recorder, "aws-us-east-1", "cluster-a")
	projector.SetRuntimePauseLookup(func(_ context.Context, info RuntimeDeletionInfo) (bool, error) {
		if info.SandboxID != "sb-1" || info.Namespace != "sandbox0" || info.PodName != "sb-1" {
			t.Fatalf("runtime deletion info = %#v", info)
		}
		return true, nil
	})

	now := time.Date(2026, 3, 12, 10, 0, 0, 0, time.UTC)
	projector.now = func() time.Time { return now }
	claimedAt := now
	pod := withSandboxResources(buildSandboxPod(claimedAt, false, "", "1"), "1", "1Gi")
	projector.handleUpsert(pod)

	projector.now = func() time.Time { return now.Add(2 * time.Minute) }
	projector.handleDelete(pod)

	if len(recorder.events) != 2 {
		t.Fatalf("event count = %d, want 2", len(recorder.events))
	}
	if recorder.events[1].EventType != meteringpkg.EventTypeSandboxPaused {
		t.Fatalf("second event type = %q, want %q", recorder.events[1].EventType, meteringpkg.EventTypeSandboxPaused)
	}
	if len(recorder.windows) != 1 || recorder.windows[0].WindowType != meteringpkg.WindowTypeSandboxRuntimeMiBMilliseconds {
		t.Fatalf("expected runtime window after durable pause delete, got %#v", recorder.windows)
	}
	if recorder.windows[0].Value != 122_880_000 {
		t.Fatalf("runtime value = %d, want 122880000", recorder.windows[0].Value)
	}
	state := recorder.states["sb-1"]
	if state == nil || !state.Paused || state.TerminatedAt != nil {
		t.Fatalf("state after durable pause delete = %#v, want paused without termination", state)
	}
}

func TestLifecycleProjectorTreatsStaleRuntimeDeleteAsPause(t *testing.T) {
	recorder := &fakeRecorder{states: map[string]*meteringpkg.SandboxProjectionState{}}
	projector := NewLifecycleProjector(recorder, "aws-us-east-1", "cluster-a")
	projector.SetRuntimePauseLookup(func(_ context.Context, info RuntimeDeletionInfo) (bool, error) {
		if info.RuntimeGeneration != 1 {
			t.Fatalf("runtime generation = %d, want 1", info.RuntimeGeneration)
		}
		return true, nil
	})

	now := time.Date(2026, 3, 12, 10, 0, 0, 0, time.UTC)
	projector.now = func() time.Time { return now }
	pod := buildSandboxPod(now, false, "", "1")
	pod.Annotations[controller.AnnotationRuntimeGeneration] = "1"
	projector.handleUpsert(pod)

	projector.now = func() time.Time { return now.Add(time.Minute) }
	projector.handleDelete(pod)

	if len(recorder.events) != 2 {
		t.Fatalf("event count = %d, want claim and pause", len(recorder.events))
	}
	if recorder.events[1].EventType != meteringpkg.EventTypeSandboxPaused {
		t.Fatalf("second event type = %q, want pause", recorder.events[1].EventType)
	}
	state := recorder.states["sb-1"]
	if state == nil || !state.Paused || state.TerminatedAt != nil {
		t.Fatalf("state after stale runtime delete = %#v, want paused", state)
	}
}

func TestLifecycleProjectorTerminatesPausedSandboxWithPausedWindow(t *testing.T) {
	recorder := &fakeRecorder{states: map[string]*meteringpkg.SandboxProjectionState{}}
	projector := NewLifecycleProjector(recorder, "aws-us-east-1", "cluster-a")

	claimedAt := time.Date(2026, 3, 12, 10, 0, 0, 0, time.UTC)
	pausedAt := time.Date(2026, 3, 12, 10, 3, 0, 0, time.UTC)
	projector.now = func() time.Time { return time.Date(2026, 3, 12, 10, 5, 0, 0, time.UTC) }

	pod := withSandboxResources(buildSandboxPod(claimedAt, true, pausedAt.Format(time.RFC3339), "7"), "1", "1Gi")
	projector.handleDelete(pod)

	if len(recorder.events) != 3 {
		t.Fatalf("event count = %d, want 3", len(recorder.events))
	}
	if recorder.events[1].EventType != meteringpkg.EventTypeSandboxPaused {
		t.Fatalf("second event type = %q, want %q", recorder.events[1].EventType, meteringpkg.EventTypeSandboxPaused)
	}
	if recorder.events[2].EventType != meteringpkg.EventTypeSandboxTerminated {
		t.Fatalf("third event type = %q, want %q", recorder.events[2].EventType, meteringpkg.EventTypeSandboxTerminated)
	}
	if len(recorder.windows) != 1 {
		t.Fatalf("window count = %d, want 1", len(recorder.windows))
	}
	if recorder.windows[0].WindowType != meteringpkg.WindowTypeSandboxRuntimeMiBMilliseconds {
		t.Fatalf("window type = %q, want runtime", recorder.windows[0].WindowType)
	}
	if recorder.windows[0].Value != 184_320_000 {
		t.Fatalf("runtime value = %d, want 184320000", recorder.windows[0].Value)
	}
}

func TestLifecycleProjectorRetriesCommitWithoutDuplicatingWindows(t *testing.T) {
	recorder := &fakeRecorder{
		states:         map[string]*meteringpkg.SandboxProjectionState{},
		stateUpsertErr: errors.New("boom"),
	}
	projector := NewLifecycleProjector(recorder, "aws-us-east-1", "cluster-a")

	now := time.Date(2026, 3, 12, 10, 0, 0, 0, time.UTC)
	projector.now = func() time.Time { return now }
	claimedAt := now.Add(-10 * time.Minute)
	pausedAt := now.Add(-2 * time.Minute)
	buildPod := func(paused bool, pausedAtValue string, resourceVersion string) *corev1.Pod {
		return withSandboxResources(buildSandboxPod(claimedAt, paused, pausedAtValue, resourceVersion), "1", "1Gi")
	}

	projector.handleUpsert(buildPod(false, "", "1"))
	if len(recorder.events) != 0 || len(recorder.windows) != 0 {
		t.Fatalf("failed transaction should not persist facts")
	}

	recorder.stateUpsertErr = nil
	projector.handleUpsert(buildPod(false, "", "1"))
	projector.handleUpsert(buildPod(true, pausedAt.Format(time.RFC3339), "2"))

	recorder.stateUpsertErr = errors.New("boom")
	projector.now = func() time.Time { return now.Add(time.Minute) }
	projector.handleUpsert(buildPod(false, "", "3"))
	if len(recorder.windows) != 1 {
		t.Fatalf("window count after failed resume = %d, want 1", len(recorder.windows))
	}

	recorder.stateUpsertErr = nil
	projector.now = func() time.Time { return now.Add(2 * time.Minute) }
	projector.handleUpsert(buildPod(false, "", "3"))
	if len(recorder.events) != 3 {
		t.Fatalf("event count = %d, want 3", len(recorder.events))
	}
	if len(recorder.windows) != 1 {
		t.Fatalf("window count = %d, want 1", len(recorder.windows))
	}
	if recorder.windows[0].WindowType != meteringpkg.WindowTypeSandboxRuntimeMiBMilliseconds {
		t.Fatalf("window type = %q, want runtime", recorder.windows[0].WindowType)
	}
	if recorder.transactionCalls != 5 {
		t.Fatalf("transaction_calls = %d, want 5", recorder.transactionCalls)
	}
}

func TestLifecycleProjectorIgnoresPublicIdlePoolPods(t *testing.T) {
	recorder := &fakeRecorder{states: map[string]*meteringpkg.SandboxProjectionState{}}
	projector := NewLifecycleProjector(recorder, "aws-us-east-1", "cluster-a")

	pod := buildSandboxPod(time.Date(2026, 3, 12, 10, 0, 0, 0, time.UTC), false, "", "1")
	pod.Labels[controller.LabelPoolType] = controller.PoolTypeIdle
	delete(pod.Labels, controller.LabelSandboxID)

	projector.handleUpsert(pod)

	if len(recorder.events) != 0 || len(recorder.windows) != 0 {
		t.Fatalf("idle pool pod should not be metered, events=%#v windows=%#v", recorder.events, recorder.windows)
	}
}

func TestLifecycleProjectorMetersTeamOwnedWarmPoolMemory(t *testing.T) {
	recorder := &fakeRecorder{states: map[string]*meteringpkg.SandboxProjectionState{}}
	projector := NewLifecycleProjector(recorder, "aws-us-east-1", "cluster-a")

	createdAt := time.Date(2026, 3, 12, 10, 0, 0, 0, time.UTC)
	now := createdAt.Add(2 * time.Minute)
	projector.now = func() time.Time { return now }

	pod := withSandboxResources(buildTeamWarmPoolPod(createdAt, "1"), "2", "1Gi")
	projector.handleUpsert(pod)

	if len(recorder.events) != 0 {
		t.Fatalf("warm pool should not emit lifecycle events, got %#v", recorder.events)
	}
	if len(recorder.windows) != 1 {
		t.Fatalf("warm pool window count = %d, want 1", len(recorder.windows))
	}
	window := recorder.windows[0]
	if window.WindowType != meteringpkg.WindowTypeSandboxRuntimeMiBMilliseconds {
		t.Fatalf("window type = %q, want runtime memory", window.WindowType)
	}
	if window.SubjectType != meteringpkg.SubjectTypeTemplate || window.SubjectID != "tpl-1" {
		t.Fatalf("subject = %s/%s, want template/tpl-1", window.SubjectType, window.SubjectID)
	}
	if window.TeamID != "team-1" || window.UserID != "user-1" || window.TemplateID != "tpl-1" {
		t.Fatalf("unexpected ownership fields: team=%s user=%s template=%s", window.TeamID, window.UserID, window.TemplateID)
	}
	if window.SandboxID != "" {
		t.Fatalf("warm pool window sandbox_id = %q, want empty", window.SandboxID)
	}
	if window.Value != 122_880_000 {
		t.Fatalf("warm pool value = %d, want 122880000", window.Value)
	}
	state := recorder.states["warm-pool/pod-uid-1"]
	if state == nil || state.ActiveSince == nil || !state.ActiveSince.Equal(now) {
		t.Fatalf("warm pool state = %#v, want active_since advanced to %v", state, now)
	}

	projector.now = func() time.Time { return now.Add(time.Minute) }
	pod.ResourceVersion = "2"
	projector.handleUpsert(pod)
	if len(recorder.windows) != 2 {
		t.Fatalf("warm pool window count after resync = %d, want 2", len(recorder.windows))
	}
	if recorder.windows[1].Value != 61_440_000 {
		t.Fatalf("resync warm pool value = %d, want 61440000", recorder.windows[1].Value)
	}
}

func TestLifecycleProjectorClosesTeamWarmPoolWhenClaimed(t *testing.T) {
	recorder := &fakeRecorder{states: map[string]*meteringpkg.SandboxProjectionState{}}
	projector := NewLifecycleProjector(recorder, "aws-us-east-1", "cluster-a")

	createdAt := time.Date(2026, 3, 12, 10, 0, 0, 0, time.UTC)
	firstObservedAt := createdAt.Add(2 * time.Minute)
	projector.now = func() time.Time { return firstObservedAt }

	idlePod := withSandboxResources(buildTeamWarmPoolPod(createdAt, "1"), "2", "1Gi")
	projector.handleUpsert(idlePod)

	claimedAt := createdAt.Add(5 * time.Minute)
	projector.now = func() time.Time { return claimedAt }
	activePod := withSandboxResources(buildSandboxPod(claimedAt, false, "", "2"), "2", "1Gi")
	activePod.Name = idlePod.Name
	activePod.Namespace = idlePod.Namespace
	activePod.UID = idlePod.UID

	projector.handleUpdate(idlePod, activePod)

	if len(recorder.windows) != 2 {
		t.Fatalf("window count after claim = %d, want 2", len(recorder.windows))
	}
	finalWarmPoolWindow := recorder.windows[1]
	if finalWarmPoolWindow.SubjectType != meteringpkg.SubjectTypeTemplate || finalWarmPoolWindow.SubjectID != "tpl-1" {
		t.Fatalf("final warm pool subject = %s/%s, want template/tpl-1", finalWarmPoolWindow.SubjectType, finalWarmPoolWindow.SubjectID)
	}
	if finalWarmPoolWindow.Value != 184_320_000 {
		t.Fatalf("final warm pool value = %d, want 184320000", finalWarmPoolWindow.Value)
	}

	warmPoolState := recorder.states["warm-pool/pod-uid-1"]
	if warmPoolState == nil || warmPoolState.ActiveSince != nil || warmPoolState.TerminatedAt == nil || !warmPoolState.TerminatedAt.Equal(claimedAt) {
		t.Fatalf("warm pool state after claim = %#v, want closed at %v", warmPoolState, claimedAt)
	}
	if recorder.states["sb-1"] == nil {
		t.Fatalf("active sandbox projection state was not created")
	}
}

func TestLifecycleProjectorDoesNotReactivateDeletedTeamWarmPool(t *testing.T) {
	recorder := &fakeRecorder{states: map[string]*meteringpkg.SandboxProjectionState{}}
	projector := NewLifecycleProjector(recorder, "aws-us-east-1", "cluster-a")

	createdAt := time.Date(2026, 3, 12, 10, 0, 0, 0, time.UTC)
	firstObservedAt := createdAt.Add(time.Minute)
	projector.now = func() time.Time { return firstObservedAt }

	pod := withSandboxResources(buildTeamWarmPoolPod(createdAt, "1"), "2", "1Gi")
	projector.handleUpsert(pod)

	deletedAt := firstObservedAt.Add(time.Minute)
	projector.now = func() time.Time { return deletedAt }
	projector.handleDelete(pod)
	windowCountAfterDelete := len(recorder.windows)

	lateUpdateAt := deletedAt.Add(time.Minute)
	projector.now = func() time.Time { return lateUpdateAt }
	pod.ResourceVersion = "3"
	pod.DeletionTimestamp = ptrMetaTime(deletedAt)
	projector.handleUpsert(pod)

	if len(recorder.windows) != windowCountAfterDelete {
		t.Fatalf("late deleted warm pool update added windows: got %d, want %d", len(recorder.windows), windowCountAfterDelete)
	}
	state := recorder.states["warm-pool/pod-uid-1"]
	if state == nil || state.ActiveSince != nil || state.TerminatedAt == nil || !state.TerminatedAt.Equal(deletedAt) {
		t.Fatalf("warm pool state after late update = %#v, want closed at %v", state, deletedAt)
	}
}

func buildSandboxPod(claimedAt time.Time, paused bool, pausedAt string, resourceVersion string) *corev1.Pod {
	annotations := map[string]string{
		controller.AnnotationClaimedAt: claimedAt.Format(time.RFC3339),
		controller.AnnotationTeamID:    "team-1",
		controller.AnnotationUserID:    "user-1",
		controller.AnnotationClaimType: "hot",
	}
	if paused {
		annotations[controller.AnnotationPaused] = "true"
	}
	if pausedAt != "" {
		annotations[controller.AnnotationPausedAt] = pausedAt
	}
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "sb-1",
			Namespace:       "sandbox0",
			ResourceVersion: resourceVersion,
			Labels: map[string]string{
				controller.LabelPoolType:   controller.PoolTypeActive,
				controller.LabelSandboxID:  "sb-1",
				controller.LabelTemplateID: "tpl-1",
			},
			Annotations: annotations,
		},
	}
}

func buildTeamWarmPoolPod(createdAt time.Time, resourceVersion string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "idle-1",
			Namespace:         "tpl-team-1",
			UID:               types.UID("pod-uid-1"),
			ResourceVersion:   resourceVersion,
			CreationTimestamp: metav1.NewTime(createdAt),
			Labels: map[string]string{
				controller.LabelPoolType:   controller.PoolTypeIdle,
				controller.LabelTemplateID: "tpl-1",
				controller.LabelOwnerKind:  controller.OwnerKindTeamWarmPool,
			},
			Annotations: map[string]string{
				controller.AnnotationTeamID:    "team-1",
				controller.AnnotationUserID:    "user-1",
				controller.AnnotationOwnerKind: controller.OwnerKindTeamWarmPool,
			},
		},
	}
}

func withSandboxResources(pod *corev1.Pod, cpu, memory string) *corev1.Pod {
	pod.Spec.Containers = []corev1.Container{{
		Name: "procd",
		Resources: corev1.ResourceRequirements{
			Limits: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse(cpu),
				corev1.ResourceMemory: resource.MustParse(memory),
			},
			Requests: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("100m"),
				corev1.ResourceMemory: resource.MustParse("256Mi"),
			},
		},
	}}
	return pod
}

func ptrMetaTime(value time.Time) *metav1.Time {
	metaTime := metav1.NewTime(value)
	return &metaTime
}

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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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
	pod := buildSandboxPod(claimedAt, false, "", "1")
	projector.handleUpsert(pod)
	if len(recorder.events) != 1 || recorder.events[0].EventType != meteringpkg.EventTypeSandboxClaimed {
		t.Fatalf("expected claim event, got %#v", recorder.events)
	}

	pausedAt := now.Add(-2 * time.Minute)
	pausedPod := buildSandboxPod(claimedAt, true, pausedAt.Format(time.RFC3339), "2")
	projector.handleUpsert(pausedPod)
	if len(recorder.events) != 2 || recorder.events[1].EventType != meteringpkg.EventTypeSandboxPaused {
		t.Fatalf("expected pause event, got %#v", recorder.events)
	}
	if len(recorder.windows) != 1 || recorder.windows[0].WindowType != meteringpkg.WindowTypeSandboxActiveSeconds || recorder.windows[0].Value != int64(pausedAt.Sub(claimedAt)/time.Second) {
		t.Fatalf("expected active window, got %#v", recorder.windows)
	}

	projector.now = func() time.Time { return now.Add(time.Minute) }
	resumedPod := buildSandboxPod(claimedAt, false, "", "3")
	projector.handleUpsert(resumedPod)
	if len(recorder.events) != 3 || recorder.events[2].EventType != meteringpkg.EventTypeSandboxResumed {
		t.Fatalf("expected resume event, got %#v", recorder.events)
	}
	if len(recorder.windows) != 2 || recorder.windows[1].WindowType != meteringpkg.WindowTypeSandboxPausedSeconds || recorder.windows[1].Value != int64(now.Add(time.Minute).Sub(pausedAt)/time.Second) {
		t.Fatalf("expected paused window, got %#v", recorder.windows)
	}

	projector.now = func() time.Time { return now.Add(2 * time.Minute) }
	projector.handleDelete(resumedPod)
	if len(recorder.events) != 4 || recorder.events[3].EventType != meteringpkg.EventTypeSandboxTerminated {
		t.Fatalf("expected terminate event, got %#v", recorder.events)
	}
	if len(recorder.windows) != 3 || recorder.windows[2].WindowType != meteringpkg.WindowTypeSandboxActiveSeconds || recorder.windows[2].Value != 60 {
		t.Fatalf("expected final active window, got %#v", recorder.windows)
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

func TestLifecycleProjectorTerminatesPausedSandboxWithPausedWindow(t *testing.T) {
	recorder := &fakeRecorder{states: map[string]*meteringpkg.SandboxProjectionState{}}
	projector := NewLifecycleProjector(recorder, "aws-us-east-1", "cluster-a")

	claimedAt := time.Date(2026, 3, 12, 10, 0, 0, 0, time.UTC)
	pausedAt := time.Date(2026, 3, 12, 10, 3, 0, 0, time.UTC)
	projector.now = func() time.Time { return time.Date(2026, 3, 12, 10, 5, 0, 0, time.UTC) }

	pod := buildSandboxPod(claimedAt, true, pausedAt.Format(time.RFC3339), "7")
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
	if len(recorder.windows) != 2 {
		t.Fatalf("window count = %d, want 2", len(recorder.windows))
	}
	if recorder.windows[0].WindowType != meteringpkg.WindowTypeSandboxActiveSeconds || recorder.windows[0].Value != 180 {
		t.Fatalf("unexpected active window: %+v", recorder.windows[0])
	}
	if recorder.windows[1].WindowType != meteringpkg.WindowTypeSandboxPausedSeconds || recorder.windows[1].Value != 120 {
		t.Fatalf("unexpected paused window: %+v", recorder.windows[1])
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

	projector.handleUpsert(buildSandboxPod(claimedAt, false, "", "1"))
	if len(recorder.events) != 0 || len(recorder.windows) != 0 {
		t.Fatalf("failed transaction should not persist facts")
	}

	recorder.stateUpsertErr = nil
	projector.handleUpsert(buildSandboxPod(claimedAt, false, "", "1"))
	projector.handleUpsert(buildSandboxPod(claimedAt, true, pausedAt.Format(time.RFC3339), "2"))

	recorder.stateUpsertErr = errors.New("boom")
	projector.now = func() time.Time { return now.Add(time.Minute) }
	projector.handleUpsert(buildSandboxPod(claimedAt, false, "", "3"))
	if len(recorder.windows) != 1 {
		t.Fatalf("window count after failed resume = %d, want 1", len(recorder.windows))
	}

	recorder.stateUpsertErr = nil
	projector.now = func() time.Time { return now.Add(2 * time.Minute) }
	projector.handleUpsert(buildSandboxPod(claimedAt, false, "", "3"))
	if len(recorder.events) != 3 {
		t.Fatalf("event count = %d, want 3", len(recorder.events))
	}
	if len(recorder.windows) != 2 {
		t.Fatalf("window count = %d, want 2", len(recorder.windows))
	}
	if recorder.windows[1].WindowType != meteringpkg.WindowTypeSandboxPausedSeconds {
		t.Fatalf("window type = %q, want %q", recorder.windows[1].WindowType, meteringpkg.WindowTypeSandboxPausedSeconds)
	}
	if recorder.transactionCalls != 5 {
		t.Fatalf("transaction_calls = %d, want 5", recorder.transactionCalls)
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

package outbox

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/metering"
)

type fakeProjectionStore struct {
	batch      *Batch
	claimed    bool
	delivered  bool
	failed     int
	retryAt    time.Time
	lastError  string
	cleanupHit int
}

func (f *fakeProjectionStore) ClaimNextBatch(context.Context, string, time.Duration) (*Batch, error) {
	if f.batch == nil || f.claimed || f.delivered {
		return nil, nil
	}
	f.claimed = true
	return f.batch, nil
}

func (f *fakeProjectionStore) MarkDelivered(context.Context, int64, string) error {
	f.delivered = true
	f.claimed = false
	return nil
}

func (f *fakeProjectionStore) MarkFailed(_ context.Context, _ int64, _, message string, retryAt time.Time) error {
	f.failed++
	f.claimed = false
	f.lastError = message
	f.retryAt = retryAt
	return nil
}

func (f *fakeProjectionStore) DeleteDeliveredBefore(context.Context, time.Time, int) (int64, error) {
	f.cleanupHit++
	return 0, nil
}

func (f *fakeProjectionStore) Stats(context.Context) (*Stats, error) {
	if f.delivered || f.batch == nil {
		return &Stats{}, nil
	}
	createdAt := time.Now().UTC()
	return &Stats{Pending: int64(len(f.batch.Operations)), OldestPending: &createdAt}, nil
}

type fakeSink struct {
	events              []*metering.Event
	windows             []*metering.Window
	watermarks          []*WatermarkOperation
	sandboxStates       []*metering.SandboxProjectionState
	storageStates       []*metering.StorageProjectionState
	storageStateDeletes []*StorageStateDeleteOperation
	failWindowOnce      bool
}

func (f *fakeSink) AppendEvent(_ context.Context, value *metering.Event) error {
	f.events = append(f.events, value)
	return nil
}

func (f *fakeSink) AppendWindow(_ context.Context, value *metering.Window) error {
	if f.failWindowOnce {
		f.failWindowOnce = false
		return errors.New("clickhouse unavailable")
	}
	f.windows = append(f.windows, value)
	return nil
}

func (f *fakeSink) UpsertProducerWatermark(_ context.Context, producer, regionID string, completeBefore time.Time) error {
	f.watermarks = append(f.watermarks, &WatermarkOperation{Producer: producer, RegionID: regionID, CompleteBefore: completeBefore})
	return nil
}

func (f *fakeSink) UpsertSandboxProjectionState(_ context.Context, value *metering.SandboxProjectionState) error {
	f.sandboxStates = append(f.sandboxStates, value)
	return nil
}

func (f *fakeSink) UpsertStorageProjectionState(_ context.Context, value *metering.StorageProjectionState) error {
	f.storageStates = append(f.storageStates, value)
	return nil
}

func (f *fakeSink) DeleteStorageProjectionState(_ context.Context, state *metering.StorageProjectionState, deletedAt time.Time) error {
	f.storageStateDeletes = append(f.storageStateDeletes, &StorageStateDeleteOperation{State: state, DeletedAt: deletedAt})
	return nil
}

func TestProjectorRetriesTheExactTransactionBatch(t *testing.T) {
	now := time.Date(2026, 7, 12, 12, 0, 0, 0, time.UTC)
	event := &metering.Event{
		EventID:     "event-1",
		Producer:    "test",
		EventType:   metering.EventTypeSandboxClaimed,
		SubjectType: metering.SubjectTypeSandbox,
		SubjectID:   "sandbox-1",
		OccurredAt:  now,
		RecordedAt:  now,
	}
	window := &metering.Window{
		WindowID:    "window-1",
		Producer:    "test",
		WindowType:  metering.WindowTypeSandboxEgressBytes,
		SubjectType: metering.SubjectTypeSandbox,
		SubjectID:   "sandbox-1",
		WindowStart: now,
		WindowEnd:   now.Add(time.Second),
		Value:       10,
		Unit:        metering.WindowUnitBytes,
		RecordedAt:  now,
	}
	store := &fakeProjectionStore{batch: &Batch{
		ID: 42,
		Operations: []*Operation{
			{Sequence: 2, BatchID: 42, Type: OperationWindow, Payload: mustMarshal(t, window), Attempts: 1},
			{Sequence: 1, BatchID: 42, Type: OperationEvent, Payload: mustMarshal(t, event), Attempts: 1},
		},
	}}
	sink := &fakeSink{failWindowOnce: true}
	projector := NewProjector(store, sink, ProjectorConfig{WorkerID: "worker-1"}, nil)
	projector.now = func() time.Time { return now }

	processed, err := projector.ProjectOnce(context.Background())
	if !processed || err == nil {
		t.Fatalf("first ProjectOnce = (%v, %v), want processed failure", processed, err)
	}
	if store.failed != 1 || !store.retryAt.Equal(now.Add(time.Second)) {
		t.Fatalf("failed batch retry = (%d, %v), want one retry at %v", store.failed, store.retryAt, now.Add(time.Second))
	}
	if len(sink.events) != 1 || sink.events[0].EventID != event.EventID || sink.events[0].Sequence != 1 {
		t.Fatalf("first event applications = %#v", sink.events)
	}

	processed, err = projector.ProjectOnce(context.Background())
	if !processed || err != nil {
		t.Fatalf("second ProjectOnce = (%v, %v), want success", processed, err)
	}
	if !store.delivered {
		t.Fatal("batch was not marked delivered")
	}
	if len(sink.events) != 2 || sink.events[0].EventID != sink.events[1].EventID || sink.events[1].Sequence != 1 {
		t.Fatalf("event retry did not preserve identity: %#v", sink.events)
	}
	if len(sink.windows) != 1 || sink.windows[0].WindowID != window.WindowID || sink.windows[0].Sequence != 2 || !sink.windows[0].RecordedAt.Equal(now) {
		t.Fatalf("window retry changed payload: %#v", sink.windows)
	}
}

func TestProjectorAppliesProjectionStateOperations(t *testing.T) {
	now := time.Date(2026, 7, 12, 12, 0, 0, 0, time.UTC)
	sandboxState := &metering.SandboxProjectionState{SandboxID: "sandbox-1", Namespace: "default", LastObservedAt: now}
	storageState := &metering.StorageProjectionState{SubjectType: metering.SubjectTypeVolume, SubjectID: "volume-1", ObservedAt: now}
	deleted := &StorageStateDeleteOperation{State: storageState, DeletedAt: now.Add(time.Second)}
	watermark := &WatermarkOperation{Producer: "producer-1", RegionID: "region-1", CompleteBefore: now}
	store := &fakeProjectionStore{batch: &Batch{
		ID: 7,
		Operations: []*Operation{
			{Sequence: 1, Type: OperationSandboxState, Payload: mustMarshal(t, sandboxState)},
			{Sequence: 2, Type: OperationStorageState, Payload: mustMarshal(t, storageState)},
			{Sequence: 3, Type: OperationStorageStateDelete, Payload: mustMarshal(t, deleted)},
			{Sequence: 4, Type: OperationWatermark, Payload: mustMarshal(t, watermark)},
		},
	}}
	sink := &fakeSink{}
	projector := NewProjector(store, sink, ProjectorConfig{WorkerID: "worker-1"}, nil)

	if processed, err := projector.ProjectOnce(context.Background()); !processed || err != nil {
		t.Fatalf("ProjectOnce = (%v, %v)", processed, err)
	}
	if len(sink.sandboxStates) != 1 || len(sink.storageStates) != 1 || len(sink.storageStateDeletes) != 1 || len(sink.watermarks) != 1 {
		t.Fatalf("applied operations = sandbox:%d storage:%d delete:%d watermark:%d",
			len(sink.sandboxStates), len(sink.storageStates), len(sink.storageStateDeletes), len(sink.watermarks))
	}
}

func mustMarshal(t *testing.T, value any) []byte {
	t.Helper()
	payload, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal test payload: %v", err)
	}
	return payload
}

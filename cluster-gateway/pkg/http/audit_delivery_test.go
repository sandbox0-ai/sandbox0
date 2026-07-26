package http

import (
	"context"
	"crypto/ed25519"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
	obsmetrics "github.com/sandbox0-ai/sandbox0/pkg/observability/metrics"
	"github.com/sandbox0-ai/sandbox0/pkg/sandboxobservability"
	"go.uber.org/zap"
)

var auditDeliveryTestSigningKey = ed25519.NewKeyFromSeed(make([]byte, ed25519.SeedSize))

func testAuditDeliveryEvent(t *testing.T, eventID string) sandboxobservability.Event {
	t.Helper()
	event := sandboxobservability.Event{
		EventID:       eventID,
		SchemaVersion: sandboxobservability.CurrentEventSchemaVersion,
		TeamID:        "team-1",
		SandboxID:     "sb-1",
		RegionID:      "region-1",
		ClusterID:     "cluster-1",
		OccurredAt:    time.Date(2026, time.July, 13, 0, 0, 0, 0, time.UTC),
		IngestedAt:    time.Date(2026, time.July, 13, 0, 0, 0, 0, time.UTC),
		Source:        sandboxobservability.SourceClusterGateway,
		EventType:     sandboxobservability.EventTypeAPIAccess,
		Phase:         sandboxobservability.EventPhaseEffect,
		Outcome:       sandboxobservability.OutcomeSucceeded,
		Actor:         sandboxobservability.AuditActor{Kind: sandboxobservability.ActorKindService, ID: "cluster-gateway"},
		Action:        "audit.delivery.test",
		Resource:      sandboxobservability.AuditResource{Type: "sandbox", ID: "sb-1"},
		OperationID:   "operation-1",
		Producer:      sandboxobservability.AuditProducer{Service: "cluster-gateway"},
	}
	if err := sandboxobservability.SignEvent(&event, auditDeliveryTestSigningKey); err != nil {
		t.Fatalf("SignEvent() error = %v", err)
	}
	return event
}

type auditDeliveryWriter struct {
	mu       sync.Mutex
	events   []sandboxobservability.Event
	batches  [][]sandboxobservability.Event
	err      error
	started  chan struct{}
	block    chan struct{}
	onInsert func()
}

func (w *auditDeliveryWriter) InsertEvents(_ context.Context, events []sandboxobservability.Event) error {
	if w.started != nil {
		select {
		case w.started <- struct{}{}:
		default:
		}
	}
	if w.block != nil {
		<-w.block
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.err != nil {
		return w.err
	}
	w.batches = append(w.batches, append([]sandboxobservability.Event(nil), events...))
	w.events = append(w.events, events...)
	if w.onInsert != nil {
		w.onInsert()
	}
	return nil
}

func (w *auditDeliveryWriter) snapshotEvents() []sandboxobservability.Event {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]sandboxobservability.Event(nil), w.events...)
}

func (w *auditDeliveryWriter) snapshotBatchSizes() []int {
	w.mu.Lock()
	defer w.mu.Unlock()
	sizes := make([]int, 0, len(w.batches))
	for _, batch := range w.batches {
		sizes = append(sizes, len(batch))
	}
	return sizes
}

func TestAuditDeliveryEnqueueDurableReturnsWithClickHouseDown(t *testing.T) {
	dir := t.TempDir()
	writer := &auditDeliveryWriter{err: errors.New("clickhouse unavailable")}
	delivery, err := newAuditDelivery(dir, writer, zap.NewNop(), nil)
	if err != nil {
		t.Fatalf("newAuditDelivery() error = %v", err)
	}
	event := testAuditDeliveryEvent(t, "00000000-0000-4000-8000-000000000001")
	if err := delivery.EnqueueDurable(context.Background(), event); err != nil {
		t.Fatalf("EnqueueDurable() error = %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, event.EventID+".json")); err != nil {
		t.Fatalf("durable event is not in the spool: %v", err)
	}
	if got := writer.snapshotEvents(); len(got) != 0 {
		t.Fatalf("durable enqueue synchronously called ClickHouse: %#v", got)
	}
}

func TestAuditDeliveryEnqueueWakesBackgroundReplay(t *testing.T) {
	dir := t.TempDir()
	writer := &auditDeliveryWriter{}
	delivery, err := newAuditDelivery(dir, writer, zap.NewNop(), nil)
	if err != nil {
		t.Fatalf("newAuditDelivery() error = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	delivery.Start(ctx)
	// Let the worker finish its startup replay so this assertion exercises the
	// enqueue wake-up rather than the one-second periodic replay.
	time.Sleep(20 * time.Millisecond)

	event := testAuditDeliveryEvent(t, "00000000-0000-4000-8000-000000000002")
	if err := delivery.EnqueueDurable(context.Background(), event); err != nil {
		t.Fatalf("EnqueueDurable() error = %v", err)
	}
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if got := writer.snapshotEvents(); len(got) == 1 && got[0].EventID == event.EventID {
			if _, err := os.Stat(filepath.Join(dir, event.EventID+".json")); !os.IsNotExist(err) {
				t.Fatalf("replayed spool record still exists: %v", err)
			}
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("background replay did not receive event promptly: %#v", writer.snapshotEvents())
}

func TestAuditDeliveryPersistsCanonicalBeforeClickHouseAndReplaysAfterRestart(t *testing.T) {
	dir := t.TempDir()
	event := testAuditDeliveryEvent(t, "11111111-1111-4111-8111-111111111111")
	blocked := &auditDeliveryWriter{started: make(chan struct{}, 1), block: make(chan struct{}), err: errors.New("unavailable")}
	delivery, err := newAuditDelivery(dir, blocked, zap.NewNop(), nil)
	if err != nil {
		t.Fatalf("newAuditDelivery() error = %v", err)
	}
	done := make(chan error, 1)
	go func() { done <- delivery.PersistCanonical(context.Background(), event) }()
	select {
	case <-blocked.started:
	case <-time.After(time.Second):
		t.Fatal("ClickHouse writer was not called")
	}
	if _, err := os.Stat(filepath.Join(dir, event.EventID+".json")); err != nil {
		t.Fatalf("result was not fsynced before ClickHouse call: %v", err)
	}
	close(blocked.block)
	if err := <-done; err == nil || !errors.Is(err, errAuditDeliveryPending) {
		t.Fatalf("PersistCanonical() error = %v, want pending canonical event", err)
	}

	recovered := &auditDeliveryWriter{}
	restarted, err := newAuditDelivery(dir, recovered, zap.NewNop(), nil)
	if err != nil {
		t.Fatalf("restart delivery error = %v", err)
	}
	if err := restarted.replay(context.Background()); err != nil {
		t.Fatalf("replay() error = %v", err)
	}
	if len(recovered.events) != 1 || recovered.events[0].EventID != event.EventID {
		t.Fatalf("replayed events = %#v", recovered.events)
	}
	if _, err := os.Stat(filepath.Join(dir, event.EventID+".json")); !os.IsNotExist(err) {
		t.Fatalf("acknowledged spool record still exists: %v", err)
	}
}

func TestAuditDeliveryReplayBatchesPendingEvents(t *testing.T) {
	dir := t.TempDir()
	writer := &auditDeliveryWriter{}
	delivery, err := newAuditDelivery(dir, writer, zap.NewNop(), nil)
	if err != nil {
		t.Fatalf("newAuditDelivery() error = %v", err)
	}
	for range auditReplayBatchSize + 1 {
		event := testAuditDeliveryEvent(t, uuid.NewString())
		if err := delivery.EnqueueDurable(context.Background(), event); err != nil {
			t.Fatalf("EnqueueDurable() error = %v", err)
		}
	}

	if err := delivery.replay(context.Background()); err != nil {
		t.Fatalf("first replay() error = %v", err)
	}
	if err := delivery.replay(context.Background()); err != nil {
		t.Fatalf("second replay() error = %v", err)
	}
	if got, want := writer.snapshotBatchSizes(), []int{auditReplayBatchSize, 1}; len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("canonical batch sizes = %v, want %v", got, want)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir() error = %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("acknowledged spool entries = %d, want 0", len(entries))
	}
}

func TestAuditDeliveryCoalescesForegroundCanonicalBurst(t *testing.T) {
	dir := t.TempDir()
	writer := &auditDeliveryWriter{}
	delivery, err := newAuditDelivery(dir, writer, zap.NewNop(), nil)
	if err != nil {
		t.Fatalf("newAuditDelivery() error = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	delivery.started.Store(true)
	go delivery.runCanonicalBatches(ctx)

	const eventCount = 32
	events := make([]sandboxobservability.Event, 0, eventCount)
	for range eventCount {
		event := testAuditDeliveryEvent(t, uuid.NewString())
		if err := delivery.EnqueueDurable(context.Background(), event); err != nil {
			t.Fatalf("EnqueueDurable() error = %v", err)
		}
		events = append(events, event)
	}

	start := make(chan struct{})
	errs := make(chan error, eventCount)
	for _, event := range events {
		event := event
		go func() {
			<-start
			errs <- delivery.PersistCanonical(context.Background(), event)
		}()
	}
	close(start)
	for range eventCount {
		if err := <-errs; err != nil {
			t.Fatalf("PersistCanonical() error = %v", err)
		}
	}

	if got := len(writer.snapshotEvents()); got != eventCount {
		t.Fatalf("canonical events = %d, want %d", got, eventCount)
	}
	batched := false
	for _, size := range writer.snapshotBatchSizes() {
		if size > 1 {
			batched = true
			break
		}
	}
	if !batched {
		t.Fatalf("canonical batch sizes = %v, want at least one coalesced batch", writer.snapshotBatchSizes())
	}
}

func TestAuditDeliveryBoundsCanonicalWriters(t *testing.T) {
	dir := t.TempDir()
	writer := &concurrentAuditDeliveryWriter{
		started: make(chan struct{}, auditCanonicalWriterSlots+2),
		release: make(chan struct{}),
	}
	delivery, err := newAuditDelivery(dir, writer, zap.NewNop(), nil)
	if err != nil {
		t.Fatalf("newAuditDelivery() error = %v", err)
	}

	const batchCount = auditCanonicalWriterSlots + 2
	for range batchCount {
		event := testAuditDeliveryEvent(t, uuid.NewString())
		if err := delivery.EnqueueDurable(context.Background(), event); err != nil {
			t.Fatalf("EnqueueDurable() error = %v", err)
		}
		call, leader := delivery.joinCanonicalCall(event)
		if !leader {
			t.Fatalf("event %s unexpectedly joined an in-flight call", event.EventID)
		}
		go delivery.dispatchCanonicalBatch(context.Background(), []*auditCanonicalCall{call}, "foreground")
	}

	for range auditCanonicalWriterSlots {
		select {
		case <-writer.started:
		case <-time.After(time.Second):
			t.Fatal("bounded canonical writer did not start")
		}
	}
	select {
	case <-writer.started:
		t.Fatalf("more than %d canonical writers started concurrently", auditCanonicalWriterSlots)
	case <-time.After(30 * time.Millisecond):
	}
	close(writer.release)
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if writer.maxConcurrency() == auditCanonicalWriterSlots &&
			delivery.pendingCalls.Load() == 0 {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("max canonical writer concurrency = %d, pending calls = %d", writer.maxConcurrency(), delivery.pendingCalls.Load())
}

func TestAuditDeliveryPrioritizesForegroundOverReplay(t *testing.T) {
	dir := t.TempDir()
	writer := &auditDeliveryWriter{started: make(chan struct{}, 1), block: make(chan struct{})}
	delivery, err := newAuditDelivery(dir, writer, zap.NewNop(), nil)
	if err != nil {
		t.Fatalf("newAuditDelivery() error = %v", err)
	}
	background := testAuditDeliveryEvent(t, uuid.NewString())
	foreground := testAuditDeliveryEvent(t, uuid.NewString())
	if err := delivery.EnqueueDurable(context.Background(), background); err != nil {
		t.Fatalf("enqueue background event: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	delivery.started.Store(true)
	go delivery.runCanonicalBatches(ctx)
	foregroundDone := make(chan error, 1)
	go func() {
		foregroundDone <- delivery.PersistCanonical(context.Background(), foreground)
	}()
	select {
	case <-writer.started:
	case <-time.After(time.Second):
		t.Fatal("foreground canonical insert did not start")
	}

	if err := delivery.replay(context.Background()); err != nil {
		t.Fatalf("replay() while foreground active error = %v", err)
	}
	if got := len(writer.snapshotBatchSizes()); got != 0 {
		t.Fatalf("completed batches before foreground release = %d, want 0", got)
	}
	close(writer.block)
	if err := <-foregroundDone; err != nil {
		t.Fatalf("foreground PersistCanonical() error = %v", err)
	}
	if err := delivery.replay(context.Background()); err != nil {
		t.Fatalf("replay() after foreground error = %v", err)
	}
	batches := writer.snapshotBatchSizes()
	if len(batches) != 2 || batches[0] != 1 || batches[1] != 1 {
		t.Fatalf("canonical batch sizes = %v, want foreground then replay singletons", batches)
	}
}

func TestAuditDeliveryRecordsStageAndBatchMetrics(t *testing.T) {
	registry := prometheus.NewRegistry()
	metrics := obsmetrics.NewClusterGateway(registry)
	writer := &auditDeliveryWriter{}
	delivery, err := newAuditDelivery(t.TempDir(), writer, zap.NewNop(), nil, metrics)
	if err != nil {
		t.Fatalf("newAuditDelivery() error = %v", err)
	}
	event := testAuditDeliveryEvent(t, uuid.NewString())
	if err := delivery.PersistCanonical(context.Background(), event); err != nil {
		t.Fatalf("PersistCanonical() error = %v", err)
	}

	if got := testutil.ToFloat64(metrics.AuditCanonicalQueueDepth); got != 0 {
		t.Fatalf("canonical queue depth = %v, want 0", got)
	}
	if got := testutil.ToFloat64(metrics.AuditCanonicalInFlight); got != 0 {
		t.Fatalf("canonical in-flight = %v, want 0", got)
	}
	if got := delivery.pendingCalls.Load(); got != 0 {
		t.Fatalf("pending canonical calls = %d, want 0", got)
	}
	if got := len(delivery.canonicalSlot); got != 0 {
		t.Fatalf("occupied canonical writer slots = %d, want 0", got)
	}
	if !delivery.canonicalGate.TryLock() {
		t.Fatal("canonical gate is still held after PersistCanonical returned")
	}
	delivery.canonicalGate.Unlock()
	families, err := registry.Gather()
	if err != nil {
		t.Fatalf("Gather() error = %v", err)
	}
	if got := histogramSampleCount(
		families,
		"cluster_gateway_audit_canonical_batch_size",
		map[string]string{"source": "foreground", "result": "success"},
	); got != 1 {
		t.Fatalf("canonical batch metric samples = %d, want 1", got)
	}
	if got := histogramSampleCount(
		families,
		"cluster_gateway_audit_delivery_stage_duration_seconds",
		map[string]string{"mode": "canonical", "stage": "spool_write", "result": "success"},
	); got != 1 {
		t.Fatalf("spool stage metric samples = %d, want 1", got)
	}
}

func TestAuditDeliveryCanonicalWaitsForInFlightReplayWithoutDuplicate(t *testing.T) {
	dir := t.TempDir()
	writer := &auditDeliveryWriter{started: make(chan struct{}, 1), block: make(chan struct{})}
	delivery, err := newAuditDelivery(dir, writer, zap.NewNop(), nil)
	if err != nil {
		t.Fatalf("newAuditDelivery() error = %v", err)
	}
	event := testAuditDeliveryEvent(t, "77777777-7777-4777-8777-777777777777")
	if err := delivery.EnqueueDurable(context.Background(), event); err != nil {
		t.Fatalf("EnqueueDurable() error = %v", err)
	}
	replayDone := make(chan error, 1)
	go func() { replayDone <- delivery.replay(context.Background()) }()
	select {
	case <-writer.started:
	case <-time.After(time.Second):
		t.Fatal("background replay did not start")
	}

	canonicalDone := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		canonicalDone <- delivery.PersistCanonical(ctx, event)
	}()
	time.Sleep(20 * time.Millisecond)
	close(writer.block)
	if err := <-replayDone; err != nil {
		t.Fatalf("replay() error = %v", err)
	}
	if err := <-canonicalDone; err != nil {
		t.Fatalf("PersistCanonical() error = %v", err)
	}
	if got := writer.snapshotEvents(); len(got) != 1 || got[0].EventID != event.EventID {
		t.Fatalf("canonical events = %#v, want one copy", got)
	}
}

type concurrentAuditDeliveryWriter struct {
	mu      sync.Mutex
	active  int
	max     int
	started chan struct{}
	release chan struct{}
}

func (w *concurrentAuditDeliveryWriter) InsertEvents(_ context.Context, _ []sandboxobservability.Event) error {
	w.mu.Lock()
	w.active++
	if w.active > w.max {
		w.max = w.active
	}
	w.mu.Unlock()
	w.started <- struct{}{}
	<-w.release
	w.mu.Lock()
	w.active--
	w.mu.Unlock()
	return nil
}

func (w *concurrentAuditDeliveryWriter) maxConcurrency() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.max
}

func histogramSampleCount(families []*dto.MetricFamily, name string, labels map[string]string) uint64 {
	for _, family := range families {
		if family.GetName() != name {
			continue
		}
		for _, metric := range family.Metric {
			matched := true
			for key, value := range labels {
				found := false
				for _, pair := range metric.Label {
					if pair.GetName() == key && pair.GetValue() == value {
						found = true
						break
					}
				}
				if !found {
					matched = false
					break
				}
			}
			if matched && metric.Histogram != nil {
				return metric.Histogram.GetSampleCount()
			}
		}
	}
	return 0
}

func TestAuditDeliveryFallsBackToCanonicalInsertWhenSpoolWriteFails(t *testing.T) {
	dir := t.TempDir()
	writer := &auditDeliveryWriter{}
	delivery, err := newAuditDelivery(dir, writer, zap.NewNop(), nil)
	if err != nil {
		t.Fatalf("newAuditDelivery() error = %v", err)
	}
	replaceAuditSpoolDirectoryWithFile(t, dir)
	event := testAuditDeliveryEvent(t, "22222222-2222-4222-8222-222222222222")
	if err := delivery.EnqueueDurable(context.Background(), event); err != nil {
		t.Fatalf("EnqueueDurable() fallback error = %v", err)
	}
	if len(writer.events) != 1 || writer.events[0].EventID != event.EventID {
		t.Fatalf("canonical fallback events = %#v", writer.events)
	}
}

func TestAuditDeliveryReportsUnrecordedWhenSpoolAndCanonicalInsertFail(t *testing.T) {
	dir := t.TempDir()
	writer := &auditDeliveryWriter{err: errors.New("clickhouse unavailable")}
	delivery, err := newAuditDelivery(dir, writer, zap.NewNop(), nil)
	if err != nil {
		t.Fatalf("newAuditDelivery() error = %v", err)
	}
	replaceAuditSpoolDirectoryWithFile(t, dir)
	err = delivery.EnqueueDurable(context.Background(), testAuditDeliveryEvent(t, "33333333-3333-4333-8333-333333333333"))
	if err == nil || !errors.Is(err, errAuditUnrecorded) {
		t.Fatalf("EnqueueDurable() error = %v, want unrecorded event", err)
	}
}

func TestAuditDeliveryDoesNotDowngradeCanonicalACKWhenSpoolCleanupFails(t *testing.T) {
	dir := t.TempDir()
	writer := &auditDeliveryWriter{}
	delivery, err := newAuditDelivery(dir, writer, zap.NewNop(), nil)
	if err != nil {
		t.Fatalf("newAuditDelivery() error = %v", err)
	}
	writer.onInsert = func() {
		replaceAuditSpoolDirectoryWithFile(t, dir)
	}
	event := testAuditDeliveryEvent(t, "44444444-4444-4444-8444-444444444444")
	if err := delivery.PersistCanonical(context.Background(), event); err != nil {
		t.Fatalf("PersistCanonical() error after canonical ACK = %v", err)
	}
	if len(writer.events) != 1 || writer.events[0].EventID != event.EventID {
		t.Fatalf("canonical events = %#v", writer.events)
	}
}

func TestAuditDeliveryRejectsCorruptOrUnsafeIdentity(t *testing.T) {
	dir := t.TempDir()
	writer := &auditDeliveryWriter{}
	delivery, err := newAuditDelivery(dir, writer, zap.NewNop(), nil)
	if err != nil {
		t.Fatalf("newAuditDelivery() error = %v", err)
	}
	if err := delivery.EnqueueDurable(context.Background(), sandboxobservability.Event{EventID: "../escape"}); err == nil {
		t.Fatal("EnqueueDurable() error = nil, want unsafe event ID rejection")
	}
	if len(writer.events) != 0 {
		t.Fatalf("unsafe event reached canonical fallback: %#v", writer.events)
	}
	if err := os.WriteFile(filepath.Join(dir, "broken.json"), []byte("{"), 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}
	if _, err := newAuditDelivery(dir, writer, zap.NewNop(), nil); err == nil {
		t.Fatal("newAuditDelivery() error = nil, want corrupt spool startup failure")
	}
}

func TestAuditDeliveryRejectsInvalidSignedEventsBeforeCustody(t *testing.T) {
	dir := t.TempDir()
	writer := &auditDeliveryWriter{}
	delivery, err := newAuditDelivery(dir, writer, zap.NewNop(), auditDeliveryTestSigningKey.Public().(ed25519.PublicKey))
	if err != nil {
		t.Fatalf("newAuditDelivery() error = %v", err)
	}

	domainInvalid := testAuditDeliveryEvent(t, "55555555-5555-4555-8555-555555555555")
	domainInvalid.Action = ""
	structureInvalid := testAuditDeliveryEvent(t, "66666666-6666-4666-8666-666666666666")
	structureInvalid.Integrity.Signature = "not-an-ed25519-signature"
	cryptographicallyInvalid := testAuditDeliveryEvent(t, "88888888-8888-4888-8888-888888888888")
	cryptographicallyInvalid.Action = "audit.delivery.tampered"

	for _, tc := range []struct {
		name  string
		event sandboxobservability.Event
	}{
		{name: "invalid domain", event: domainInvalid},
		{name: "invalid signature structure", event: structureInvalid},
		{name: "invalid signature", event: cryptographicallyInvalid},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := delivery.EnqueueDurable(context.Background(), tc.event); err == nil {
				t.Fatal("EnqueueDurable() error = nil, want invalid event rejection")
			}
		})
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir() error = %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("invalid audit events reached durable spool: %v", entries)
	}
	if got := writer.snapshotEvents(); len(got) != 0 {
		t.Fatalf("invalid audit events reached canonical writer: %#v", got)
	}
}

func replaceAuditSpoolDirectoryWithFile(t *testing.T, dir string) {
	t.Helper()
	if err := os.RemoveAll(dir); err != nil {
		t.Fatalf("RemoveAll(%q) error = %v", dir, err)
	}
	if err := os.WriteFile(dir, []byte("not a directory"), 0o600); err != nil {
		t.Fatalf("WriteFile(%q) error = %v", dir, err)
	}
}

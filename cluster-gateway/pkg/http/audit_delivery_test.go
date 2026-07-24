package http

import (
	"context"
	"crypto/ed25519"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
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

func TestAuditDeliveryCoalescesDurableReplayUntilQuiet(t *testing.T) {
	dir := t.TempDir()
	writer := &auditDeliveryWriter{started: make(chan struct{}, 1)}
	delivery, err := newAuditDelivery(dir, writer, zap.NewNop(), nil)
	if err != nil {
		t.Fatalf("newAuditDelivery() error = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	delivery.Start(ctx)
	time.Sleep(20 * time.Millisecond)

	for _, eventID := range []string{
		"00000000-0000-4000-8000-000000000011",
		"00000000-0000-4000-8000-000000000012",
	} {
		if err := delivery.EnqueueDurable(context.Background(), testAuditDeliveryEvent(t, eventID)); err != nil {
			t.Fatalf("EnqueueDurable() error = %v", err)
		}
		time.Sleep(auditReplayQuietPeriod / 3)
	}
	select {
	case <-writer.started:
		t.Fatal("durable replay started before the quiet period elapsed")
	case <-time.After(auditReplayQuietPeriod / 3):
	}

	select {
	case <-writer.started:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("durable replay did not start after the quiet period")
	}
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if got := writer.snapshotBatchSizes(); len(got) == 1 && got[0] == 2 {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("durable replay batch sizes = %v, want [2]", writer.snapshotBatchSizes())
}

func TestAuditDeliveryDurableReplayYieldsToCanonicalCallers(t *testing.T) {
	dir := t.TempDir()
	writer := &auditDeliveryWriter{started: make(chan struct{}, 1)}
	delivery, err := newAuditDelivery(dir, writer, zap.NewNop(), nil)
	if err != nil {
		t.Fatalf("newAuditDelivery() error = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	delivery.Start(ctx)
	time.Sleep(20 * time.Millisecond)

	delivery.canonicalCalls.Store(1)
	event := testAuditDeliveryEvent(t, "00000000-0000-4000-8000-000000000013")
	if err := delivery.EnqueueDurable(context.Background(), event); err != nil {
		t.Fatalf("EnqueueDurable() error = %v", err)
	}
	select {
	case <-writer.started:
		t.Fatal("durable replay competed with an active canonical caller")
	case <-time.After(2 * auditReplayQuietPeriod):
	}

	delivery.canonicalCalls.Store(0)
	delivery.signalReplay()
	select {
	case <-writer.started:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("durable replay did not resume after canonical callers drained")
	}
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

func TestAuditDeliveryRetriesCanonicalEventAfterWriterFailure(t *testing.T) {
	dir := t.TempDir()
	writer := &auditDeliveryWriter{err: errors.New("clickhouse unavailable")}
	delivery, err := newAuditDelivery(dir, writer, zap.NewNop(), nil)
	if err != nil {
		t.Fatalf("newAuditDelivery() error = %v", err)
	}
	event := testAuditDeliveryEvent(t, "12121212-1212-4121-8121-121212121212")
	if err := delivery.PersistCanonical(context.Background(), event); !errors.Is(err, errAuditDeliveryPending) {
		t.Fatalf("first PersistCanonical() error = %v, want pending", err)
	}

	writer.mu.Lock()
	writer.err = nil
	writer.mu.Unlock()
	if err := delivery.PersistCanonical(context.Background(), event); err != nil {
		t.Fatalf("second PersistCanonical() error = %v", err)
	}
	events := writer.snapshotEvents()
	if len(events) != 1 || events[0].EventID != event.EventID {
		t.Fatalf("canonical events = %#v, want one retried event", events)
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

func TestAuditDeliveryCanonicalBatchIncludesTargetBeyondLimit(t *testing.T) {
	dir := t.TempDir()
	writer := &auditDeliveryWriter{}
	delivery, err := newAuditDelivery(dir, writer, zap.NewNop(), nil)
	if err != nil {
		t.Fatalf("newAuditDelivery() error = %v", err)
	}
	targetID := "ffffffff-ffff-4fff-8fff-ffffffffffff"
	for _, eventID := range []string{
		"11111111-1111-4111-8111-111111111111",
		"22222222-2222-4222-8222-222222222222",
		targetID,
	} {
		if err := delivery.EnqueueDurable(context.Background(), testAuditDeliveryEvent(t, eventID)); err != nil {
			t.Fatalf("EnqueueDurable() error = %v", err)
		}
	}

	delivery.mu.Lock()
	events, err := delivery.loadBatchContainingLocked(2, targetID)
	delivery.mu.Unlock()
	if err != nil {
		t.Fatalf("loadBatchContainingLocked() error = %v", err)
	}
	if len(events) != 2 {
		t.Fatalf("canonical batch size = %d, want 2", len(events))
	}
	if events[0].EventID != targetID {
		t.Fatalf("first canonical event = %s, want %s", events[0].EventID, targetID)
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

func TestAuditDeliveryBatchesConcurrentCanonicalWrites(t *testing.T) {
	dir := t.TempDir()
	writer := &auditDeliveryWriter{started: make(chan struct{}, 1), block: make(chan struct{})}
	delivery, err := newAuditDelivery(dir, writer, zap.NewNop(), nil)
	if err != nil {
		t.Fatalf("newAuditDelivery() error = %v", err)
	}

	const writes = 32
	errs := make(chan error, writes)
	firstEvent := testAuditDeliveryEvent(t, uuid.NewString())
	go func() {
		errs <- delivery.PersistCanonical(context.Background(), firstEvent)
	}()
	select {
	case <-writer.started:
	case <-time.After(time.Second):
		t.Fatal("first canonical write did not start")
	}
	for range writes - 1 {
		event := testAuditDeliveryEvent(t, uuid.NewString())
		go func() {
			errs <- delivery.PersistCanonical(context.Background(), event)
		}()
	}

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		entries, readErr := os.ReadDir(dir)
		if readErr != nil {
			t.Fatalf("ReadDir() error = %v", readErr)
		}
		if len(entries) == writes {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir() error = %v", err)
	}
	if len(entries) != writes {
		t.Fatalf("durably spooled events = %d, want %d", len(entries), writes)
	}

	close(writer.block)
	for range writes {
		if err := <-errs; err != nil {
			t.Fatalf("PersistCanonical() error = %v", err)
		}
	}
	if got := len(writer.snapshotEvents()); got != writes {
		t.Fatalf("canonical events = %d, want %d", got, writes)
	}
	batchSizes := writer.snapshotBatchSizes()
	if len(batchSizes) < 2 || batchSizes[0] != 1 {
		t.Fatalf("canonical batch sizes = %v, want an initial singleton followed by coalesced writes", batchSizes)
	}
	total := 0
	for _, size := range batchSizes {
		total += size
	}
	if total != writes {
		t.Fatalf("canonical batch rows = %d, want %d: %v", total, writes, batchSizes)
	}
	if got := delivery.canonicalTurns.Load(); got != int64(len(batchSizes)) {
		t.Fatalf("canonical delivery turns = %d, want %d", got, len(batchSizes))
	}
}

func TestAuditDeliveryBoundsParallelCanonicalBatchesWithoutDuplicates(t *testing.T) {
	dir := t.TempDir()
	writer := &auditDeliveryWriter{
		started: make(chan struct{}, auditCanonicalSlots+1),
		block:   make(chan struct{}),
	}
	delivery, err := newAuditDelivery(dir, writer, zap.NewNop(), nil)
	if err != nil {
		t.Fatalf("newAuditDelivery() error = %v", err)
	}

	const writes = auditCanonicalSlots + 1
	errs := make(chan error, writes)
	for i := range writes {
		event := testAuditDeliveryEvent(t, uuid.NewString())
		go func() {
			errs <- delivery.PersistCanonical(context.Background(), event)
		}()
		if i < auditCanonicalSlots {
			select {
			case <-writer.started:
			case <-time.After(time.Second):
				t.Fatalf("canonical batch %d did not start", i+1)
			}
		}
	}

	select {
	case <-writer.started:
		t.Fatalf("canonical batch exceeded the %d-slot bound", auditCanonicalSlots)
	case <-time.After(50 * time.Millisecond):
	}

	close(writer.block)
	for range writes {
		if err := <-errs; err != nil {
			t.Fatalf("PersistCanonical() error = %v", err)
		}
	}
	events := writer.snapshotEvents()
	if len(events) != writes {
		t.Fatalf("canonical events = %d, want %d", len(events), writes)
	}
	seen := make(map[string]struct{}, writes)
	for _, event := range events {
		if _, duplicate := seen[event.EventID]; duplicate {
			t.Fatalf("canonical event %s was inserted more than once", event.EventID)
		}
		seen[event.EventID] = struct{}{}
	}
}

func TestAuditDeliveryGroupsConcurrentDirectorySyncs(t *testing.T) {
	dir := t.TempDir()
	writer := &auditDeliveryWriter{}
	delivery, err := newAuditDelivery(dir, writer, zap.NewNop(), nil)
	if err != nil {
		t.Fatalf("newAuditDelivery() error = %v", err)
	}

	const writes = 32
	var syncMu sync.Mutex
	syncCalls := 0
	delivery.dirSync = func(path string) error {
		syncMu.Lock()
		syncCalls++
		syncMu.Unlock()
		return syncAuditDirectory(path)
	}
	start := make(chan struct{})
	errs := make(chan error, writes)
	for range writes {
		event := testAuditDeliveryEvent(t, uuid.NewString())
		go func() {
			<-start
			errs <- delivery.EnqueueDurable(context.Background(), event)
		}()
	}
	close(start)
	for range writes {
		if err := <-errs; err != nil {
			t.Fatalf("EnqueueDurable() error = %v", err)
		}
	}

	syncMu.Lock()
	got := syncCalls
	syncMu.Unlock()
	if got >= writes {
		t.Fatalf("directory sync calls = %d, want fewer than %d writes", got, writes)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir() error = %v", err)
	}
	if len(entries) != writes {
		t.Fatalf("durably spooled events = %d, want %d", len(entries), writes)
	}
}

func TestAuditDeliveryDirectorySyncFailureWakesConcurrentWriters(t *testing.T) {
	dir := t.TempDir()
	writer := &auditDeliveryWriter{}
	delivery, err := newAuditDelivery(dir, writer, zap.NewNop(), nil)
	if err != nil {
		t.Fatalf("newAuditDelivery() error = %v", err)
	}

	syncStarted := make(chan struct{})
	releaseSync := make(chan struct{})
	syncErr := errors.New("directory sync failed")
	delivery.dirSync = func(string) error {
		select {
		case <-syncStarted:
		default:
			close(syncStarted)
		}
		<-releaseSync
		return syncErr
	}
	const writes = 8
	errs := make(chan error, writes)
	for range writes {
		event := testAuditDeliveryEvent(t, uuid.NewString())
		go func() {
			errs <- delivery.EnqueueDurable(context.Background(), event)
		}()
	}
	select {
	case <-syncStarted:
	case <-time.After(time.Second):
		t.Fatal("directory sync did not start")
	}
	close(releaseSync)
	for range writes {
		err := <-errs
		if err != nil {
			t.Fatalf("EnqueueDurable() canonical fallback error = %v", err)
		}
	}
	if got := len(writer.snapshotEvents()); got != writes {
		t.Fatalf("canonical fallback events = %d, want %d", got, writes)
	}
}

func TestAuditDeliveryWaitsForConcurrentSpoolWritesToDrain(t *testing.T) {
	delivery := &auditDelivery{}
	delivery.canonicalCalls.Store(2)
	delivery.spoolWrites.Store(1)

	drained := make(chan struct{})
	go func() {
		time.Sleep(5 * time.Millisecond)
		delivery.spoolWrites.Store(0)
		close(drained)
	}()

	started := time.Now()
	if err := delivery.waitForConcurrentSpoolWrites(context.Background()); err != nil {
		t.Fatalf("waitForConcurrentSpoolWrites() error = %v", err)
	}
	<-drained
	if elapsed := time.Since(started); elapsed < 5*time.Millisecond {
		t.Fatalf("waitForConcurrentSpoolWrites() returned before the spool drained: %v", elapsed)
	}
}

func TestAuditDeliverySerializesConcurrentEventIDCollisions(t *testing.T) {
	dir := t.TempDir()
	writer := &auditDeliveryWriter{}
	delivery, err := newAuditDelivery(dir, writer, zap.NewNop(), nil)
	if err != nil {
		t.Fatalf("newAuditDelivery() error = %v", err)
	}

	eventID := "99999999-9999-4999-8999-999999999999"
	first := testAuditDeliveryEvent(t, eventID)
	second := first
	second.Action = "audit.delivery.collision"
	if err := sandboxobservability.SignEvent(&second, auditDeliveryTestSigningKey); err != nil {
		t.Fatalf("SignEvent() error = %v", err)
	}

	start := make(chan struct{})
	errs := make(chan error, 2)
	for _, event := range []sandboxobservability.Event{first, second} {
		event := event
		go func() {
			<-start
			errs <- delivery.EnqueueDurable(context.Background(), event)
		}()
	}
	close(start)

	successes := 0
	collisions := 0
	for range 2 {
		err := <-errs
		switch {
		case err == nil:
			successes++
		case strings.Contains(err.Error(), "event_id collision"):
			collisions++
		default:
			t.Fatalf("EnqueueDurable() unexpected error = %v", err)
		}
	}
	if successes != 1 || collisions != 1 {
		t.Fatalf("concurrent writes: successes=%d collisions=%d, want 1 each", successes, collisions)
	}
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

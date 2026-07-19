package http

import (
	"context"
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
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

func TestAuditDeliveryEnforcesPerTeamEntriesAndReleasesAfterRemoval(t *testing.T) {
	limits := testAuditDeliveryLimits()
	limits.maxEntries = 3
	limits.maxTeamEntries = 1
	writer := &auditDeliveryWriter{}
	delivery, err := newAuditDeliveryWithLimits(t.TempDir(), writer, zap.NewNop(), nil, limits)
	if err != nil {
		t.Fatalf("newAuditDeliveryWithLimits() error = %v", err)
	}

	first := testAuditDeliveryEventForTeam(t, uuid.NewString(), "team-1")
	if err := delivery.EnqueueDurable(context.Background(), first); err != nil {
		t.Fatalf("EnqueueDurable(first) error = %v", err)
	}
	sameTeam := testAuditDeliveryEventForTeam(t, uuid.NewString(), "team-1")
	if err := delivery.EnqueueDurable(context.Background(), sameTeam); !errors.Is(err, errAuditSpoolCapacity) {
		t.Fatalf("EnqueueDurable(same team) error = %v, want capacity", err)
	}
	if got := writer.snapshotEvents(); len(got) != 0 {
		t.Fatalf("capacity rejection bypassed to canonical writer: %#v", got)
	}
	otherTeam := testAuditDeliveryEventForTeam(t, uuid.NewString(), "team-2")
	if err := delivery.EnqueueDurable(context.Background(), otherTeam); err != nil {
		t.Fatalf("EnqueueDurable(other team) error = %v", err)
	}

	delivery.mu.Lock()
	err = delivery.removeLocked(first.EventID)
	delivery.mu.Unlock()
	if err != nil {
		t.Fatalf("removeLocked(first) error = %v", err)
	}
	if err := delivery.EnqueueDurable(context.Background(), sameTeam); err != nil {
		t.Fatalf("EnqueueDurable(same team after release) error = %v", err)
	}
}

func TestAuditDeliveryEnforcesGlobalAndPerTeamBytes(t *testing.T) {
	event := testAuditDeliveryEventForTeam(t, uuid.NewString(), "team-1")
	payload, err := json.Marshal(event)
	if err != nil {
		t.Fatalf("Marshal() error = %v", err)
	}
	recordBytes := int64(len(payload))

	t.Run("per team", func(t *testing.T) {
		limits := testAuditDeliveryLimits()
		limits.maxBytes = 3 * recordBytes
		limits.maxTeamBytes = recordBytes
		limits.maxRecordBytes = recordBytes
		delivery, err := newAuditDeliveryWithLimits(
			t.TempDir(),
			&auditDeliveryWriter{},
			zap.NewNop(),
			nil,
			limits,
		)
		if err != nil {
			t.Fatalf("newAuditDeliveryWithLimits() error = %v", err)
		}
		if err := delivery.EnqueueDurable(context.Background(), event); err != nil {
			t.Fatalf("EnqueueDurable(first) error = %v", err)
		}
		if err := delivery.EnqueueDurable(
			context.Background(),
			testAuditDeliveryEventForTeam(t, uuid.NewString(), "team-1"),
		); !errors.Is(err, errAuditSpoolCapacity) {
			t.Fatalf("EnqueueDurable(same team) error = %v, want capacity", err)
		}
		if err := delivery.EnqueueDurable(
			context.Background(),
			testAuditDeliveryEventForTeam(t, uuid.NewString(), "team-2"),
		); err != nil {
			t.Fatalf("EnqueueDurable(other team) error = %v", err)
		}
	})

	t.Run("global", func(t *testing.T) {
		limits := testAuditDeliveryLimits()
		limits.maxBytes = 2 * recordBytes
		limits.maxTeamBytes = 2 * recordBytes
		limits.maxRecordBytes = recordBytes
		delivery, err := newAuditDeliveryWithLimits(
			t.TempDir(),
			&auditDeliveryWriter{},
			zap.NewNop(),
			nil,
			limits,
		)
		if err != nil {
			t.Fatalf("newAuditDeliveryWithLimits() error = %v", err)
		}
		for _, teamID := range []string{"team-1", "team-2"} {
			if err := delivery.EnqueueDurable(
				context.Background(),
				testAuditDeliveryEventForTeam(t, uuid.NewString(), teamID),
			); err != nil {
				t.Fatalf("EnqueueDurable(%s) error = %v", teamID, err)
			}
		}
		if err := delivery.EnqueueDurable(
			context.Background(),
			testAuditDeliveryEventForTeam(t, uuid.NewString(), "team-3"),
		); !errors.Is(err, errAuditSpoolCapacity) {
			t.Fatalf("EnqueueDurable(over global bytes) error = %v, want capacity", err)
		}
	})
}

func TestAuditDeliveryConcurrentEnqueueHonorsExactGlobalEntryLimit(t *testing.T) {
	limits := testAuditDeliveryLimits()
	limits.maxEntries = 5
	limits.maxTeamEntries = 5
	delivery, err := newAuditDeliveryWithLimits(
		t.TempDir(),
		&auditDeliveryWriter{},
		zap.NewNop(),
		nil,
		limits,
	)
	if err != nil {
		t.Fatalf("newAuditDeliveryWithLimits() error = %v", err)
	}

	const attempts = 32
	events := make([]sandboxobservability.Event, 0, attempts)
	for range attempts {
		events = append(events, testAuditDeliveryEventForTeam(t, uuid.NewString(), "team-1"))
	}
	start := make(chan struct{})
	results := make(chan error, attempts)
	for _, event := range events {
		event := event
		go func() {
			<-start
			results <- delivery.EnqueueDurable(context.Background(), event)
		}()
	}
	close(start)

	successes := 0
	rejected := 0
	for range attempts {
		switch err := <-results; {
		case err == nil:
			successes++
		case errors.Is(err, errAuditSpoolCapacity):
			rejected++
		default:
			t.Fatalf("EnqueueDurable() unexpected error = %v", err)
		}
	}
	if successes != 5 || rejected != attempts-5 {
		t.Fatalf(
			"concurrent enqueue results = %d success, %d rejected; want 5, %d",
			successes,
			rejected,
			attempts-5,
		)
	}
}

func TestAuditDeliveryRebuildsBoundedUsageAtStartup(t *testing.T) {
	dir := t.TempDir()
	limits := testAuditDeliveryLimits()
	limits.maxEntries = 2
	limits.maxTeamEntries = 1
	first, err := newAuditDeliveryWithLimits(dir, &auditDeliveryWriter{}, zap.NewNop(), nil, limits)
	if err != nil {
		t.Fatalf("first newAuditDeliveryWithLimits() error = %v", err)
	}
	existing := testAuditDeliveryEventForTeam(t, uuid.NewString(), "team-1")
	if err := first.EnqueueDurable(context.Background(), existing); err != nil {
		t.Fatalf("EnqueueDurable(existing) error = %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, ".audit-incomplete.tmp"), []byte("partial"), 0o600); err != nil {
		t.Fatalf("WriteFile(temp) error = %v", err)
	}

	restarted, err := newAuditDeliveryWithLimits(dir, &auditDeliveryWriter{}, zap.NewNop(), nil, limits)
	if err != nil {
		t.Fatalf("restart newAuditDeliveryWithLimits() error = %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, ".audit-incomplete.tmp")); !os.IsNotExist(err) {
		t.Fatalf("incomplete temp record remains after startup: %v", err)
	}
	replacement := testAuditDeliveryEventForTeam(t, uuid.NewString(), "team-1")
	if err := restarted.EnqueueDurable(context.Background(), replacement); !errors.Is(err, errAuditSpoolCapacity) {
		t.Fatalf("EnqueueDurable before usage release error = %v, want capacity", err)
	}
	restarted.mu.Lock()
	err = restarted.removeLocked(existing.EventID)
	restarted.mu.Unlock()
	if err != nil {
		t.Fatalf("removeLocked(existing) error = %v", err)
	}
	if err := restarted.EnqueueDurable(context.Background(), replacement); err != nil {
		t.Fatalf("EnqueueDurable after usage release error = %v", err)
	}
	if err := restarted.EnqueueDurable(
		context.Background(),
		testAuditDeliveryEventForTeam(t, uuid.NewString(), "team-2"),
	); err != nil {
		t.Fatalf("EnqueueDurable(other team) error = %v", err)
	}

	tightened := limits
	tightened.maxEntries = 1
	tightened.maxTeamEntries = 1
	if _, err := newAuditDeliveryWithLimits(
		dir,
		&auditDeliveryWriter{},
		zap.NewNop(),
		nil,
		tightened,
	); !errors.Is(err, errAuditSpoolCapacity) {
		t.Fatalf("startup over-capacity error = %v, want capacity", err)
	}
}

func TestAuditDeliveryEnforcesRecordSizeAndFilesystemHeadroom(t *testing.T) {
	event := testAuditDeliveryEventForTeam(t, uuid.NewString(), "team-1")
	payload, err := json.Marshal(event)
	if err != nil {
		t.Fatalf("Marshal() error = %v", err)
	}
	recordBytes := int64(len(payload))

	limits := testAuditDeliveryLimits()
	limits.maxRecordBytes = recordBytes - 1
	delivery, err := newAuditDeliveryWithLimits(
		t.TempDir(),
		&auditDeliveryWriter{},
		zap.NewNop(),
		nil,
		limits,
	)
	if err != nil {
		t.Fatalf("newAuditDeliveryWithLimits() error = %v", err)
	}
	if err := delivery.EnqueueDurable(context.Background(), event); !errors.Is(err, errAuditSpoolCapacity) {
		t.Fatalf("EnqueueDurable(oversized) error = %v, want capacity", err)
	}

	limits = testAuditDeliveryLimits()
	limits.minFreeBytes = 100
	delivery, err = newAuditDeliveryWithLimits(
		t.TempDir(),
		&auditDeliveryWriter{},
		zap.NewNop(),
		nil,
		limits,
	)
	if err != nil {
		t.Fatalf("newAuditDeliveryWithLimits() error = %v", err)
	}
	delivery.freeBytes = func(string) (int64, error) {
		return recordBytes + limits.minFreeBytes - 1, nil
	}
	if err := delivery.EnqueueDurable(context.Background(), event); !errors.Is(err, errAuditSpoolCapacity) {
		t.Fatalf("EnqueueDurable(below free-space floor) error = %v, want capacity", err)
	}
	delivery.freeBytes = func(string) (int64, error) {
		return recordBytes + limits.minFreeBytes, nil
	}
	if err := delivery.EnqueueDurable(context.Background(), event); err != nil {
		t.Fatalf("EnqueueDurable(at free-space floor) error = %v", err)
	}
}

func TestAuditDeliveryStartupRejectsUnsafeRecordTypes(t *testing.T) {
	for _, tc := range []struct {
		name   string
		create func(*testing.T, string)
	}{
		{
			name: "symlink",
			create: func(t *testing.T, dir string) {
				target := filepath.Join(t.TempDir(), "target")
				if err := os.WriteFile(target, []byte("{}"), 0o600); err != nil {
					t.Fatalf("WriteFile(target) error = %v", err)
				}
				if err := os.Symlink(target, filepath.Join(dir, uuid.NewString()+".json")); err != nil {
					t.Fatalf("Symlink() error = %v", err)
				}
			},
		},
		{
			name: "directory",
			create: func(t *testing.T, dir string) {
				if err := os.Mkdir(filepath.Join(dir, uuid.NewString()+".json"), 0o700); err != nil {
					t.Fatalf("Mkdir() error = %v", err)
				}
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			tc.create(t, dir)
			if _, err := newAuditDeliveryWithLimits(
				dir,
				&auditDeliveryWriter{},
				zap.NewNop(),
				nil,
				testAuditDeliveryLimits(),
			); !errors.Is(err, errAuditSpoolCorrupt) {
				t.Fatalf("startup error = %v, want corrupt", err)
			}
		})
	}
}

func TestAuditDeliveryRejectsInvalidLimits(t *testing.T) {
	for _, mutate := range []func(*auditDeliveryLimits){
		func(limits *auditDeliveryLimits) { limits.maxBytes = 0 },
		func(limits *auditDeliveryLimits) { limits.maxEntries = 0 },
		func(limits *auditDeliveryLimits) { limits.maxTeamBytes = limits.maxBytes + 1 },
		func(limits *auditDeliveryLimits) { limits.maxTeamEntries = limits.maxEntries + 1 },
		func(limits *auditDeliveryLimits) { limits.minFreeBytes = -1 },
		func(limits *auditDeliveryLimits) { limits.maxRecordBytes = limits.maxTeamBytes + 1 },
	} {
		limits := testAuditDeliveryLimits()
		mutate(&limits)
		if _, err := newAuditDeliveryWithLimits(
			t.TempDir(),
			&auditDeliveryWriter{},
			zap.NewNop(),
			nil,
			limits,
		); err == nil {
			t.Fatalf("newAuditDeliveryWithLimits(%+v) error = nil", limits)
		}
	}
}

func testAuditDeliveryLimits() auditDeliveryLimits {
	return auditDeliveryLimits{
		maxBytes:       8 << 20,
		maxEntries:     100,
		maxTeamBytes:   4 << 20,
		maxTeamEntries: 50,
		minFreeBytes:   0,
		maxRecordBytes: 1 << 20,
	}
}

func testAuditDeliveryEventForTeam(
	t *testing.T,
	eventID string,
	teamID string,
) sandboxobservability.Event {
	t.Helper()
	event := testAuditDeliveryEvent(t, eventID)
	event.TeamID = teamID
	if err := sandboxobservability.SignEvent(&event, auditDeliveryTestSigningKey); err != nil {
		t.Fatalf("SignEvent() error = %v", err)
	}
	return event
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

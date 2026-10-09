package http

import (
	"context"
	"errors"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/sandbox0-ai/sandbox0/pkg/sandboxobservability"
	"go.uber.org/zap"
)

func preparedSpoolCall(t *testing.T, d *auditDelivery, event sandboxobservability.Event) *auditSpoolCall {
	t.Helper()
	payload, err := d.prepareSpoolEvent(event)
	if err != nil {
		t.Fatal(err)
	}
	return &auditSpoolCall{event: event, payload: payload, done: make(chan error, 1)}
}

func TestAuditSpoolBatchCustodyWaitsForSharedDirectorySyncAndRestarts(t *testing.T) {
	dir := t.TempDir()
	d, err := newAuditDelivery(dir, &auditDeliveryWriter{}, zap.NewNop(), nil)
	if err != nil {
		t.Fatal(err)
	}
	batch := make([]*auditSpoolCall, 32)
	for i := range batch {
		batch[i] = preparedSpoolCall(t, d, testAuditDeliveryEvent(t, uuid.NewString()))
	}
	entered, release := make(chan struct{}), make(chan struct{})
	var barriers atomic.Int64
	d.syncDir = func(path string) error {
		barriers.Add(1)
		close(entered)
		<-release
		return syncAuditDirectory(path)
	}
	done := make(chan []error, 1)
	go func() {
		d.mu.Lock()
		defer d.mu.Unlock()
		done <- d.putSpoolBatchLocked(batch)
	}()
	<-entered
	for _, call := range batch {
		if _, err := os.Stat(d.path(call.event.EventID)); err != nil {
			t.Fatal(err)
		}
	}
	select {
	case <-done:
		t.Fatal("custody returned before the directory barrier")
	default:
	}
	close(release)
	for _, err := range <-done {
		if err != nil {
			t.Fatal(err)
		}
	}
	if barriers.Load() != 1 {
		t.Fatalf("directory barriers = %d", barriers.Load())
	}
	writer := &auditDeliveryWriter{}
	restarted, err := newAuditDelivery(dir, writer, zap.NewNop(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := restarted.replay(t.Context()); err != nil {
		t.Fatal(err)
	}
	if len(writer.snapshotEvents()) != len(batch) {
		t.Fatal("batch was not recovered from the legacy per-event format")
	}
	entries, err := os.ReadDir(dir)
	if err != nil || len(entries) != 0 {
		t.Fatalf("replayed spool records = %d, err = %v", len(entries), err)
	}
}

func TestAuditSpoolBatchPartialFailureAndCollisionDoNotDiscardPeers(t *testing.T) {
	d, err := newAuditDelivery(t.TempDir(), &auditDeliveryWriter{}, zap.NewNop(), nil)
	if err != nil {
		t.Fatal(err)
	}
	good := testAuditDeliveryEvent(t, uuid.NewString())
	bad := testAuditDeliveryEvent(t, uuid.NewString())
	if err := os.Mkdir(d.path(bad.EventID), 0o700); err != nil {
		t.Fatal(err)
	}
	collision := good
	collision.Action = "collision"
	if err := sandboxobservability.SignEvent(&collision, auditDeliveryTestSigningKey); err != nil {
		t.Fatal(err)
	}
	batch := []*auditSpoolCall{
		preparedSpoolCall(t, d, good), preparedSpoolCall(t, d, bad),
		preparedSpoolCall(t, d, good), preparedSpoolCall(t, d, bad),
		preparedSpoolCall(t, d, collision),
	}
	d.mu.Lock()
	errs := d.putSpoolBatchLocked(batch)
	d.mu.Unlock()
	if errs[0] != nil || errs[2] != nil || !errors.Is(errs[1], errAuditSpoolWrite) ||
		!errors.Is(errs[3], errAuditSpoolWrite) || errs[4] == nil || errors.Is(errs[4], errAuditSpoolWrite) {
		t.Fatalf("partial batch results = %v", errs)
	}
	got, err := os.ReadFile(d.path(good.EventID))
	if err != nil || string(got) != string(batch[0].payload) {
		t.Fatal("a collision overwrote its durably buffered peer")
	}
}

func TestAuditSpoolRetryCannotSkipFailedDirectorySync(t *testing.T) {
	writer := &auditDeliveryWriter{err: errors.New("canonical unavailable")}
	d, err := newAuditDelivery(t.TempDir(), writer, zap.NewNop(), nil)
	if err != nil {
		t.Fatal(err)
	}
	var barriers int
	d.syncDir = func(path string) error {
		barriers++
		if barriers <= 2 {
			return errors.New("directory sync failed")
		}
		return syncAuditDirectory(path)
	}
	event := testAuditDeliveryEvent(t, uuid.NewString())
	for range 2 {
		if err := d.EnqueueDurable(t.Context(), event); !errors.Is(err, errAuditUnrecorded) {
			t.Fatalf("failed directory sync returned custody: %v", err)
		}
	}
	if err := d.EnqueueDurable(t.Context(), event); err != nil {
		t.Fatal(err)
	}
	if barriers != 3 || writer.attempts != 2 {
		t.Fatalf("barriers = %d, fallbacks = %d", barriers, writer.attempts)
	}
}

func TestAuditSpoolAdmittedCancellationWaitsForCustodyAndShutdownSealsQueue(t *testing.T) {
	d, err := newAuditDelivery(t.TempDir(), &auditDeliveryWriter{}, zap.NewNop(), nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	entered, release := make(chan struct{}), make(chan struct{})
	d.syncDir = func(path string) error {
		close(entered)
		<-release
		return syncAuditDirectory(path)
	}
	d.spoolCtx = ctx
	d.spoolStarted.Store(true)
	stopped := make(chan struct{})
	go func() { defer close(stopped); d.runSpoolBatches(ctx) }()
	request, stopRequest := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- d.spoolEvent(request, testAuditDeliveryEvent(t, uuid.NewString())) }()
	<-entered
	stopRequest()
	cancel()
	select {
	case err := <-done:
		t.Fatalf("admitted fsync was interrupted: %v", err)
	default:
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	<-stopped
	if err := d.spoolEvent(t.Context(), testAuditDeliveryEvent(t, uuid.NewString())); !errors.Is(err, errAuditSpoolWrite) {
		t.Fatalf("stopped queue accepted a new record: %v", err)
	}
}

func TestAuditSpoolProductionBurstStillWaitsForCanonicalACK(t *testing.T) {
	writer := &auditDeliveryWriter{started: make(chan struct{}, 8), block: make(chan struct{})}
	d, err := newAuditDelivery(t.TempDir(), writer, zap.NewNop(), nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	d.Start(ctx)
	const count = 100
	events := make([]sandboxobservability.Event, count)
	for i := range events {
		events[i] = testAuditDeliveryEvent(t, uuid.NewString())
	}
	start := make(chan struct{})
	done := make(chan error, count)
	var submitted sync.WaitGroup
	for _, event := range events {
		submitted.Add(1)
		go func() {
			defer submitted.Done()
			<-start
			done <- d.PersistCanonical(ctx, event)
		}()
	}
	close(start)
	select {
	case <-writer.started:
	case <-time.After(5 * time.Second):
		t.Fatal("canonical writer did not start")
	}
	select {
	case err := <-done:
		t.Fatalf("mutation returned before canonical ACK: %v", err)
	default:
	}
	close(writer.block)
	submitted.Wait()
	for range count {
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
	if len(writer.snapshotEvents()) != count {
		t.Fatalf("canonical events = %d", len(writer.snapshotEvents()))
	}
	if len(writer.snapshotBatchSizes()) >= count/2 {
		t.Fatalf("burst still fragmented into small inserts: %v", writer.snapshotBatchSizes())
	}
}

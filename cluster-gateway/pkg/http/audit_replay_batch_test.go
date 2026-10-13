package http

import (
	"context"
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/google/uuid"
	"github.com/sandbox0-ai/sandbox0/pkg/sandboxobservability"
	"go.uber.org/zap"
)

func TestAuditReplayContinuousArrivalsCannotPostponeFlush(t *testing.T) {
	dir := t.TempDir()
	synctest.Test(t, func(t *testing.T) {
		writer := &auditDeliveryWriter{}
		d, err := newAuditDelivery(dir, writer, zap.NewNop(), nil)
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		go func() { defer close(done); d.runReplay(ctx) }()
		defer func() { cancel(); <-done }()
		synctest.Wait() // finish the empty startup replay
		for i := range 20 {
			if err := d.EnqueueDurable(ctx, testAuditDeliveryEvent(t, uuid.NewString())); err != nil {
				t.Fatal(err)
			}
			synctest.Wait()
			if got := len(writer.snapshotEvents()); got != 0 {
				t.Fatalf("sparse wake flushed after %d arrivals: %d events", i+1, got)
			}
			time.Sleep(50 * time.Millisecond)
		}
		synctest.Wait()
		if sizes := writer.snapshotBatchSizes(); len(sizes) != 1 || sizes[0] != 20 {
			t.Fatalf("one-second batch = %v, want [20]", sizes)
		}
		if d.spoolRecords != 0 || d.spoolBytes != 0 {
			t.Fatalf("ACKed spool counters: %d records / %d bytes", d.spoolRecords, d.spoolBytes)
		}
	})
}

func TestAuditReplayRowThresholdFlushesBeforeTimer(t *testing.T) {
	dir := t.TempDir()
	synctest.Test(t, func(t *testing.T) {
		writer := &auditDeliveryWriter{}
		d, err := newAuditDelivery(dir, writer, zap.NewNop(), nil)
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		go func() { defer close(done); d.runReplay(ctx) }()
		defer func() { cancel(); <-done }()
		synctest.Wait()
		for range auditReplayBatchSize - 1 {
			if err := d.EnqueueDurable(ctx, testAuditDeliveryEvent(t, uuid.NewString())); err != nil {
				t.Fatal(err)
			}
		}
		synctest.Wait()
		if len(writer.snapshotEvents()) != 0 {
			t.Fatal("partial batch flushed before the time or size threshold")
		}
		if err := d.EnqueueDurable(ctx, testAuditDeliveryEvent(t, uuid.NewString())); err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		if sizes := writer.snapshotBatchSizes(); len(sizes) != 1 || sizes[0] != auditReplayBatchSize {
			t.Fatalf("full batch = %v", sizes)
		}
	})
}

func TestAuditReplayByteThresholdBoundsPayload(t *testing.T) {
	dir := t.TempDir()
	synctest.Test(t, func(t *testing.T) {
		writer := &auditDeliveryWriter{}
		d, err := newAuditDelivery(dir, writer, zap.NewNop(), nil)
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		go func() { defer close(done); d.runReplay(ctx) }()
		defer func() { cancel(); <-done }()
		synctest.Wait()
		for range 40 {
			event := testAuditDeliveryEvent(t, uuid.NewString())
			event.Attributes = map[string]any{"payload": strings.Repeat("x", 60<<10)}
			if err := sandboxobservability.SignEvent(&event, auditDeliveryTestSigningKey); err != nil {
				t.Fatal(err)
			}
			if err := d.EnqueueDurable(ctx, event); err != nil {
				t.Fatal(err)
			}
		}
		synctest.Wait()
		if len(writer.snapshotEvents()) == 0 {
			t.Fatal("byte threshold did not flush before the one-second timer")
		}
		time.Sleep(time.Second)
		synctest.Wait()
		writer.mu.Lock()
		defer writer.mu.Unlock()
		if len(writer.events) != 40 {
			t.Fatalf("delivered = %d, want 40", len(writer.events))
		}
		for _, batch := range writer.batches {
			size := 0
			for _, event := range batch {
				payload, err := json.Marshal(event)
				if err != nil {
					t.Fatal(err)
				}
				size += len(payload)
			}
			if size > auditReplayBatchBytes {
				t.Fatalf("batch payload %d exceeds byte budget %d", size, auditReplayBatchBytes)
			}
		}
	})
}

func TestAuditReplayDuplicateAndRestartKeepExactCustody(t *testing.T) {
	dir := t.TempDir()
	writer := &auditDeliveryWriter{}
	d, err := newAuditDelivery(dir, writer, zap.NewNop(), auditDeliveryTestSigningKey.Public().(ed25519.PublicKey))
	if err != nil {
		t.Fatal(err)
	}
	event := testAuditDeliveryEvent(t, uuid.NewString())
	for range 3 {
		if err := d.EnqueueDurable(context.Background(), event); err != nil {
			t.Fatal(err)
		}
	}
	payload, err := os.ReadFile(d.path(event.EventID))
	if err != nil {
		t.Fatal(err)
	}
	if d.spoolRecords != 1 || d.spoolBytes != int64(len(payload)) {
		t.Fatalf("duplicate changed backlog: %d records / %d bytes", d.spoolRecords, d.spoolBytes)
	}
	restarted, err := newAuditDelivery(dir, writer, zap.NewNop(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := restarted.replay(context.Background()); err != nil {
		t.Fatal(err)
	}
	events := writer.snapshotEvents()
	if len(events) != 1 || events[0].Integrity != event.Integrity {
		t.Fatalf("restart changed signed identity: %#v", events)
	}
	if restarted.spoolRecords != 0 || restarted.spoolBytes != 0 {
		t.Fatal("ACK did not clear recovered backlog counters")
	}
}

func TestAuditSpoolCapacityNeverAcceptsUnrecordedEvent(t *testing.T) {
	d, err := newAuditDelivery(t.TempDir(), &auditDeliveryWriter{err: errors.New("unavailable")}, zap.NewNop(), nil)
	if err != nil {
		t.Fatal(err)
	}
	retained := testAuditDeliveryEvent(t, uuid.NewString())
	if err := d.EnqueueDurable(context.Background(), retained); err != nil {
		t.Fatal(err)
	}
	d.spoolRecords = auditSpoolMaxRecords // simulate an exhausted persistent backlog
	newEvent := testAuditDeliveryEvent(t, uuid.NewString())
	if err := d.EnqueueDurable(context.Background(), newEvent); !errors.Is(err, errAuditUnrecorded) {
		t.Fatalf("full spool plus failed canonical fallback returned %v", err)
	}
	if _, err := os.Stat(d.path(retained.EventID)); err != nil {
		t.Fatal("capacity pressure removed an accepted record")
	}
	if _, err := os.Stat(d.path(newEvent.EventID)); !os.IsNotExist(err) {
		t.Fatalf("over-capacity record was admitted: %v", err)
	}
}

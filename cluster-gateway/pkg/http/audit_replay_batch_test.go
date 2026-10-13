package http

import (
	"bufio"
	"context"
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/google/uuid"
	"github.com/sandbox0-ai/sandbox0/pkg/sandboxobservability"
	"go.uber.org/zap"
)

func TestAuditReplayCrashAfterDurableCustody(t *testing.T) {
	const records = 64
	if dir := os.Getenv("SANDBOX0_TEST_CRASH_SPOOL"); dir != "" {
		d, err := newAuditDelivery(dir, &auditDeliveryWriter{err: errors.New("offline")}, zap.NewNop(), nil)
		if err != nil {
			t.Fatal(err)
		}
		d.Start(context.Background())
		for range records {
			if err := d.EnqueueDurable(context.Background(), testAuditDeliveryEvent(t, uuid.NewString())); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := os.Stdout.WriteString("custody-ready\n"); err != nil {
			t.Fatal(err)
		}
		<-context.Background().Done() // parent kills the process without shutdown
		return
	}
	dir := t.TempDir()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	child := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestAuditReplayCrashAfterDurableCustody$")
	child.Env = append(os.Environ(), "SANDBOX0_TEST_CRASH_SPOOL="+dir)
	output, err := child.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := child.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = child.Process.Kill() }()
	scanner := bufio.NewScanner(output)
	ready := false
	for scanner.Scan() {
		if scanner.Text() == "custody-ready" {
			ready = true
			break
		}
	}
	if err := child.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	_ = child.Wait()
	if !ready {
		t.Fatal("child did not acknowledge durable custody before crash")
	}
	writer := &auditDeliveryWriter{}
	restarted, err := newAuditDelivery(dir, writer, zap.NewNop(), auditDeliveryTestSigningKey.Public().(ed25519.PublicKey))
	if err != nil {
		t.Fatal(err)
	}
	if restarted.spoolRecords != records {
		t.Fatalf("crash retained %d records, want %d", restarted.spoolRecords, records)
	}
	if err := restarted.replay(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(writer.snapshotEvents()) != records || restarted.spoolRecords != 0 {
		t.Fatal("crash recovery did not deliver and clean every acknowledged record")
	}
}

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

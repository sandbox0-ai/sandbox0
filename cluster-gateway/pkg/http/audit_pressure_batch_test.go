package http

import (
	"context"
	"testing"
	"testing/synctest"

	"github.com/google/uuid"
	"go.uber.org/zap"
)

func TestAuditDeliveryCoalescesEventsArrivingDuringSlotWait(t *testing.T) {
	for _, tc := range []struct {
		name   string
		source string
		queued int
		want   int
	}{
		{"foreground", "foreground", 6, 7},
		{"bounded", "foreground", auditReplayBatchSize, auditReplayBatchSize},
		{"replay_ownership", "replay", 6, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				writer := &auditDeliveryWriter{started: make(chan struct{}, 1), block: make(chan struct{})}
				d, err := newAuditDelivery(t.TempDir(), writer, zap.NewNop(), nil)
				if err != nil {
					t.Fatal(err)
				}
				calls := make([]*auditCanonicalCall, tc.queued+1)
				for i := range calls {
					event := testAuditDeliveryEvent(t, uuid.NewString())
					if err := d.EnqueueDurable(context.Background(), event); err != nil {
						t.Fatal(err)
					}
					calls[i], _ = d.joinCanonicalCall(event)
				}
				for range auditCanonicalWriterSlots {
					if err := d.acquireCanonicalSlot(context.Background()); err != nil {
						t.Fatal(err)
					}
				}
				go d.dispatchCanonicalBatch(context.Background(), calls[:1], tc.source)
				synctest.Wait() // Dispatch is blocked on the occupied writer slots.
				for _, call := range calls[1:] {
					d.observeQueueDelta(1)
					d.canonicalQueue <- call
				}
				d.releaseCanonicalSlot()
				<-writer.started
				for _, call := range calls {
					select {
					case <-call.done:
						t.Fatal("canonical confirmation preceded the backend ACK")
					default:
					}
				}
				close(writer.block)
				for _, call := range calls[:tc.want] {
					<-call.done
					if call.err != nil {
						t.Fatal(call.err)
					}
				}
				synctest.Wait()
				if sizes := writer.snapshotBatchSizes(); len(sizes) != 1 || sizes[0] != tc.want {
					t.Fatalf("insert batches = %v, want one batch of %d", sizes, tc.want)
				}
				if got := len(d.canonicalQueue); got != len(calls)-tc.want {
					t.Fatalf("queued events = %d, want %d", got, len(calls)-tc.want)
				}
				for _, call := range calls[tc.want:] {
					select {
					case <-call.done:
						t.Fatal("unwritten queued event was confirmed")
					default:
					}
					if pending, err := d.pendingLocked(call.event.EventID); err != nil || !pending {
						t.Fatalf("queued event lost durable custody: pending=%v error=%v", pending, err)
					}
				}
				for range auditCanonicalWriterSlots - 1 {
					d.releaseCanonicalSlot()
				}
			})
		})
	}
}

package http

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/sandboxobservability"
)

type auditSpoolCall struct {
	event   sandboxobservability.Event
	payload []byte
	done    chan error
}

func (d *auditDelivery) spoolEvent(ctx context.Context, event sandboxobservability.Event) error {
	// Validation and signing checks do not occupy the shared filesystem lock.
	payload, err := d.prepareSpoolEvent(event)
	if err != nil {
		return err
	}
	call := &auditSpoolCall{event: event, payload: payload, done: make(chan error, 1)}
	if !d.spoolStarted.Load() {
		d.mu.Lock()
		errs := d.putSpoolBatchLocked([]*auditSpoolCall{call})
		d.mu.Unlock()
		return errs[0]
	}

	// Seal admission before draining on shutdown. Once admitted, local fsync
	// remains non-cancelable, as with the original per-event spool operation.
	d.spoolSubmitMu.Lock()
	if d.spoolStopped {
		d.spoolSubmitMu.Unlock()
		return auditSpoolWriteError("spool stopped", context.Canceled)
	}
	select {
	case d.spoolQueue <- call:
		d.spoolSubmitMu.Unlock()
	case <-ctx.Done():
		d.spoolSubmitMu.Unlock()
		return auditSpoolWriteError("spool admission canceled", ctx.Err())
	case <-d.spoolCtx.Done():
		d.spoolSubmitMu.Unlock()
		return auditSpoolWriteError("spool stopped", d.spoolCtx.Err())
	}
	return <-call.done
}

func (d *auditDelivery) runSpoolBatches(ctx context.Context) {
	defer func() {
		d.spoolSubmitMu.Lock()
		d.spoolStopped = true
		d.spoolSubmitMu.Unlock()
		for {
			select {
			case call := <-d.spoolQueue:
				call.done <- auditSpoolWriteError("spool stopped", ctx.Err())
			default:
				return
			}
		}
	}()
	for {
		var first *auditSpoolCall
		select {
		case <-ctx.Done():
			return
		case first = <-d.spoolQueue:
		}
		batch := []*auditSpoolCall{first}
		timer := time.NewTimer(auditCanonicalBatchWindow)
	collect:
		for len(batch) < auditReplayBatchSize {
			select {
			case call := <-d.spoolQueue:
				batch = append(batch, call)
			case <-timer.C:
				break collect
			case <-ctx.Done():
				break collect
			}
		}
		timer.Stop()
		d.mu.Lock()
		errs := d.putSpoolBatchLocked(batch)
		d.mu.Unlock()
		for i, call := range batch {
			call.done <- errs[i]
		}
	}
}

// Caller holds mu so replay and canonical cleanup never inspect or delete
// partially committed records. Distinct records sync with bounded parallelism;
// no caller receives custody until the shared directory sync succeeds.
func (d *auditDelivery) putSpoolBatchLocked(batch []*auditSpoolCall) []error {
	errs := make([]error, len(batch))
	owners := make(map[string]int, len(batch))
	duplicates := make(map[int]int)
	jobs := make([]int, 0, len(batch))
	for i, call := range batch {
		if owner, exists := owners[call.event.EventID]; exists {
			if string(batch[owner].payload) != string(call.payload) {
				errs[i] = fmt.Errorf("audit event_id collision")
			} else {
				duplicates[i] = owner
			}
			continue
		}
		owners[call.event.EventID] = i
		existing, err := os.ReadFile(d.path(call.event.EventID))
		switch {
		case err == nil:
			if string(existing) != string(call.payload) {
				errs[i] = fmt.Errorf("audit event_id collision")
			}
		case os.IsNotExist(err):
			jobs = append(jobs, i)
		default:
			errs[i] = auditSpoolWriteError("read existing record", err)
		}
	}

	queue := make(chan int, len(jobs))
	for _, i := range jobs {
		queue <- i
	}
	close(queue)
	var workers sync.WaitGroup
	for range min(auditSpoolWriterSlots, len(jobs)) {
		workers.Go(func() {
			for i := range queue {
				call := batch[i]
				errs[i] = d.writeSpoolRecord(call.event.EventID, call.payload)
			}
		})
	}
	workers.Wait()
	// Existing records also need this barrier: a previous rename may have
	// succeeded while its directory sync failed. A retry must not accept that
	// record as durable merely because it can be read from the page cache.
	hasRecord := false
	for _, i := range owners {
		if errs[i] == nil {
			hasRecord = true
			break
		}
	}
	if hasRecord {
		started := time.Now()
		err := d.syncDir(d.dir)
		d.observeStage("spool", "directory_sync", started, err)
		if err != nil {
			for _, i := range owners {
				if errs[i] == nil {
					errs[i] = auditSpoolWriteError("fsync directory", err)
				}
			}
		}
	}
	for duplicate, owner := range duplicates {
		errs[duplicate] = errs[owner]
	}
	if d.metrics != nil && d.metrics.AuditSpoolBatchSize != nil {
		var batchErr error
		for _, err := range errs {
			if err != nil {
				batchErr = err
				break
			}
		}
		d.metrics.AuditSpoolBatchSize.WithLabelValues(auditDeliveryMetricResult(batchErr)).Observe(float64(len(batch)))
	}
	return errs
}

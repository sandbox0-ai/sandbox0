package webhook

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

var ErrEventOwnerChanged = errors.New("webhook event belongs to an inherited runtime owner")

// PauseDelivery closes outbound admission and drains the current request and
// its acknowledgement. New events remain durably queued. A canceled wait keeps
// delivery gated, allowing an exact checkpoint retry or explicit cancellation.
func (d *Dispatcher) PauseDelivery(ctx context.Context) error {
	if err := d.ValidateCheckpoint(); err != nil {
		return err
	}
	d.deliveryMu.Lock()
	d.deliveryPaused = true
	done := d.deliveryDone
	d.deliveryMu.Unlock()
	if done != nil {
		select {
		case <-done:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	// Keep idle HTTP sockets out of the execution image as well.
	d.client.CloseIdleConnections()
	return nil
}

func (d *Dispatcher) ResumeDelivery() {
	d.deliveryMu.Lock()
	if d.awaitingOwner || d.ownerFenceError != nil {
		d.deliveryMu.Unlock()
		return
	}
	d.deliveryPaused = false
	d.deliveryMu.Unlock()
	d.wakeWorker()
}

// RebindCheckpointIdentity must run while delivery is drained. A fork leaves
// inherited records byte-for-byte intact; the worker quarantines them lazily so
// handover does not scan or rewrite an arbitrarily large outbox.
func (d *Dispatcher) RebindCheckpointIdentity(source, target, team string) error {
	d.deliveryMu.Lock()
	defer d.deliveryMu.Unlock()
	if !d.deliveryPaused || d.deliveryDone != nil {
		return errors.New("webhook delivery must be drained before identity handover")
	}
	d.enqueueMu.Lock()
	defer d.enqueueMu.Unlock()
	d.mu.Lock()
	defer d.mu.Unlock()
	if source == "" || target == "" || team == "" || d.teamID != team || (d.sandbox != source && d.sandbox != target) {
		return errors.New("webhook checkpoint owner changed")
	}
	if (d.isolateOwner || source != target) && d.options.OutboxDir != "" {
		if err := d.writeCheckpointOwner(target, team); err != nil {
			return err
		}
	}
	d.sandbox = target
	d.isolateOwner = d.isolateOwner || source != target
	return nil
}

func (d *Dispatcher) beginDelivery() bool {
	d.deliveryMu.Lock()
	defer d.deliveryMu.Unlock()
	if d.deliveryPaused || d.deliveryDone != nil {
		return false
	}
	d.deliveryDone = make(chan struct{})
	return true
}

func (d *Dispatcher) endDelivery() {
	d.deliveryMu.Lock()
	close(d.deliveryDone)
	d.deliveryDone = nil
	d.deliveryMu.Unlock()
}

func (d *Dispatcher) matchesDeliveryOwner(event Event) bool {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return !d.isolateOwner || (event.SandboxID == d.sandbox && event.TeamID == d.teamID)
}

func (d *Dispatcher) quarantineInheritedRecord(path string) error {
	dir := filepath.Join(d.options.OutboxDir, "inherited")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	if err := os.Rename(path, filepath.Join(dir, filepath.Base(path))); err != nil {
		return err
	}
	for _, name := range []string{dir, d.options.OutboxDir} {
		f, err := os.Open(name)
		if err != nil {
			return err
		}
		err = f.Sync()
		closeErr := f.Close()
		if err != nil {
			return err
		}
		if closeErr != nil {
			return closeErr
		}
	}
	return nil
}

// ValidateCheckpoint rejects volatile delivery before the runtime is gated.
func (d *Dispatcher) ValidateCheckpoint() error {
	d.enqueueMu.Lock()
	defer d.enqueueMu.Unlock()
	if d.options.OutboxDir == "" && (d.isConfigured() || len(d.queue) > 0) {
		return errors.New("memory checkpoint with webhooks requires a durable outbox")
	}
	return nil
}

type checkpointOwner struct {
	Version   int    `json:"version"`
	SandboxID string `json:"sandbox_id"`
	TeamID    string `json:"team_id"`
}

// A cold restart may inherit unprocessed records from a memory fork. Persist
// the isolation policy and wait for runtime activation before delivering any.
func (d *Dispatcher) loadCheckpointOwner() {
	if d.options.OutboxDir == "" {
		return
	}
	data, err := os.ReadFile(filepath.Join(d.options.OutboxDir, ".checkpoint-owner"))
	if errors.Is(err, os.ErrNotExist) {
		return
	}
	d.deliveryPaused = true
	d.awaitingOwner = true
	var owner checkpointOwner
	if err != nil {
		d.ownerFenceError = fmt.Errorf("read webhook checkpoint owner: %w", err)
		return
	}
	if json.Unmarshal(data, &owner) != nil || owner.Version != 1 || owner.SandboxID == "" || owner.TeamID == "" {
		d.ownerFenceError = errors.New("invalid webhook checkpoint owner")
		return
	}
	d.isolateOwner = true
}

func (d *Dispatcher) writeCheckpointOwner(sandbox, team string) error {
	if d.options.OutboxDir == "" || sandbox == "" || team == "" {
		return errors.New("durable webhook checkpoint owner is required")
	}
	if err := os.MkdirAll(d.options.OutboxDir, 0o700); err != nil {
		return err
	}
	data, err := json.Marshal(checkpointOwner{Version: 1, SandboxID: sandbox, TeamID: team})
	if err != nil {
		return err
	}
	f, err := os.CreateTemp(d.options.OutboxDir, ".checkpoint-owner-*")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	if _, err = f.Write(data); err != nil {
		_ = f.Close()
		return err
	}
	if err = f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	if err = os.Rename(f.Name(), filepath.Join(d.options.OutboxDir, ".checkpoint-owner")); err != nil {
		return err
	}
	dir, err := os.Open(d.options.OutboxDir)
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}

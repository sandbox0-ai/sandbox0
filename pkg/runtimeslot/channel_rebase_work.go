package runtimeslot

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"time"

	"github.com/containerd/errdefs"
)

type nodeChannelRebaseWork struct {
	fingerprint [32]byte
	done        chan struct{}
	result      NodeChannelResult
	completedAt time.Time
}

func (a *NodeChannelAgent) initNodeWorkLocked() {
	if a.workPermit == nil {
		limit := a.config.MaxConcurrent
		if limit <= 0 {
			limit = defaultNodeChannelMaxConcurrent
		}
		a.workPermit = make(chan struct{}, limit)
		a.rebaseWork = make(map[string]*nodeChannelRebaseWork)
	}
}

func (a *NodeChannelAgent) nodeWorkPermit() chan struct{} {
	a.workMu.Lock()
	defer a.workMu.Unlock()
	a.initNodeWorkLocked()
	return a.workPermit
}

// A transport rotation reloads credentials/capacity immediately. The already
// authorized private rebase continues under the agent lifetime and its own
// timeout, and an exact request on the successor stream joins that operation.
// A changed payload cannot reuse the request ID. Node shutdown still cancels
// and joins every retained operation before returning.
func (a *NodeChannelAgent) retainedRebase(ctx context.Context, command NodeChannelCommand) (*nodeChannelRebaseWork, error) {
	if command.Kind != NodeChannelCommandPausedRebase {
		return nil, errdefs.ErrInvalidArgument
	}
	if err := command.Validate(); err != nil {
		return nil, err
	}
	payload, err := json.Marshal(command)
	if err != nil || len(payload) > NodeChannelMaxBytes {
		return nil, fmt.Errorf("invalid retained rebase command: %w", errdefs.ErrInvalidArgument)
	}
	fingerprint := sha256.Sum256(payload)
	a.workMu.Lock()
	defer a.workMu.Unlock()
	a.initNodeWorkLocked()
	if work := a.rebaseWork[command.RequestID]; work != nil {
		if work.fingerprint != fingerprint {
			return nil, fmt.Errorf("retained rebase request ID is bound to another payload: %w", errdefs.ErrFailedPrecondition)
		}
		return work, nil
	}
	// Completed responses cover one rotation/reconnect interval. Both active
	// jobs and cached results share a fixed cardinality bound. An evicted result
	// can still be recovered through the node's durable exact-operation journal.
	retention := max(a.config.ConnectionMaxAge, defaultNodeChannelConnectionMaxAge)
	for id, work := range a.rebaseWork {
		if !work.completedAt.IsZero() && time.Since(work.completedAt) >= retention {
			delete(a.rebaseWork, id)
		}
	}
	if len(a.rebaseWork) >= cap(a.workPermit) {
		var oldestID string
		var oldest time.Time
		for id, work := range a.rebaseWork {
			if !work.completedAt.IsZero() && (oldestID == "" || work.completedAt.Before(oldest)) {
				oldestID, oldest = id, work.completedAt
			}
		}
		if oldestID == "" {
			return nil, fmt.Errorf("node rebase concurrency is exhausted: %w", errdefs.ErrResourceExhausted)
		}
		delete(a.rebaseWork, oldestID)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	work := &nodeChannelRebaseWork{fingerprint: fingerprint, done: make(chan struct{})}
	a.rebaseWork[command.RequestID] = work
	a.rebaseWG.Add(1)
	go func() {
		defer a.rebaseWG.Done()
		select {
		case a.workPermit <- struct{}{}:
			work.result = a.execute(ctx, command)
			<-a.workPermit
		case <-ctx.Done():
			work.result = NodeChannelResult{Version: NodeChannelVersion, RequestID: command.RequestID, Kind: command.Kind,
				Error: ctx.Err().Error(), ErrorClass: NodeChannelErrorUnavailable}
		}
		a.workMu.Lock()
		work.completedAt = time.Now()
		close(work.done)
		a.workMu.Unlock()
	}()
	return work, nil
}

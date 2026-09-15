// Copyright 2026 Sandbox0 Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package nomadruntime

import (
	"context"
	"fmt"
	"time"

	"github.com/containerd/errdefs"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshandoff"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/rootfswriterauthority"
)

const (
	writerRenewalBatchWindow  = 250 * time.Millisecond
	writerRenewalBatchTimeout = 2 * time.Second
	writerRenewalBatchWorkers = 2
	writerRenewalQueueSize    = 2 * protocol.MaxBatchRenewItems
)

type writerBatchAuthority interface {
	RenewWriterGrants(context.Context, []rootfshandoff.StageRequest) (protocol.BatchRenewResponse, error)
}

type writerRenewalReply struct {
	observation protocol.LeaseObservation
	err         error
}

type writerRenewalCall struct {
	ctx    context.Context
	stage  rootfshandoff.StageRequest
	result chan writerRenewalReply
}

// writerRenewalBatcher only coalesces transport work. Each writer's existing
// renewal loop remains responsible for its own deadline and fail-closed action.
// Bounded queue, batch size, workers and request timeout prevent a control-plane
// outage from accumulating unbounded node work or extending a local lease.
type writerRenewalBatcher struct {
	ctx       context.Context
	cancel    context.CancelFunc
	authority writerBatchAuthority
	queue     chan writerRenewalCall
	workers   chan struct{}
}

func newWriterRenewalBatcher(authority writerBatchAuthority) *writerRenewalBatcher {
	ctx, cancel := context.WithCancel(context.Background())
	b := &writerRenewalBatcher{
		ctx: ctx, cancel: cancel, authority: authority,
		queue:   make(chan writerRenewalCall, writerRenewalQueueSize),
		workers: make(chan struct{}, writerRenewalBatchWorkers),
	}
	go b.run()
	return b
}

func (b *writerRenewalBatcher) renew(ctx context.Context, stage rootfshandoff.StageRequest) (protocol.LeaseObservation, error) {
	if err := ctx.Err(); err != nil {
		return protocol.LeaseObservation{}, err
	}
	if err := stage.ValidateDurableBinding(); err != nil {
		return protocol.LeaseObservation{}, fmt.Errorf("invalid writer renewal binding: %w: %w", err, errdefs.ErrInvalidArgument)
	}
	call := writerRenewalCall{ctx: ctx, stage: stage.WithoutWriterGrantToken(), result: make(chan writerRenewalReply, 1)}
	select {
	case <-ctx.Done():
		return protocol.LeaseObservation{}, ctx.Err()
	case <-b.ctx.Done():
		return protocol.LeaseObservation{}, b.ctx.Err()
	case b.queue <- call:
	default:
		return protocol.LeaseObservation{}, fmt.Errorf("writer renewal queue is full: %w", errdefs.ErrUnavailable)
	}
	select {
	case <-ctx.Done():
		return protocol.LeaseObservation{}, ctx.Err()
	case <-b.ctx.Done():
		return protocol.LeaseObservation{}, b.ctx.Err()
	case reply := <-call.result:
		// A late response must never revive a canceled or expired incarnation.
		if err := ctx.Err(); err != nil {
			return protocol.LeaseObservation{}, err
		}
		return reply.observation, reply.err
	}
}

func (b *writerRenewalBatcher) run() {
	for {
		var first writerRenewalCall
		select {
		case <-b.ctx.Done():
			return
		case first = <-b.queue:
		}
		if first.ctx.Err() != nil {
			continue
		}
		batch := []writerRenewalCall{first}
		flushAt := renewalBatchFlushAt(first.ctx)
		timer := time.NewTimer(time.Until(flushAt))
	collect:
		for len(batch) < protocol.MaxBatchRenewItems {
			select {
			case <-b.ctx.Done():
				timer.Stop()
				return
			case <-timer.C:
				break collect
			case call := <-b.queue:
				if call.ctx.Err() != nil {
					continue
				}
				batch = append(batch, call)
				if deadline := renewalBatchFlushAt(call.ctx); deadline.Before(flushAt) {
					flushAt = deadline
					timer.Reset(time.Until(flushAt))
				}
			}
		}
		timer.Stop()
		select {
		case <-b.ctx.Done():
			return
		case b.workers <- struct{}{}:
		}
		go func() {
			defer func() { <-b.workers }()
			b.dispatch(batch)
		}()
	}
}

func renewalBatchFlushAt(ctx context.Context) time.Time {
	now := time.Now()
	delay := writerRenewalBatchWindow
	if deadline, ok := ctx.Deadline(); ok && deadline.Sub(now)/2 < delay {
		delay = deadline.Sub(now) / 2
	}
	return now.Add(delay)
}

func (b *writerRenewalBatcher) dispatch(batch []writerRenewalCall) {
	active := make([]writerRenewalCall, 0, len(batch))
	stages := make([]rootfshandoff.StageRequest, 0, len(batch))
	seen := make(map[string]struct{}, len(batch))
	for _, call := range batch {
		if call.ctx.Err() != nil {
			continue
		}
		id := call.stage.Identity.WriterGrantID
		if _, duplicate := seen[id]; duplicate {
			call.result <- writerRenewalReply{err: fmt.Errorf("writer renewal already queued: %w", errdefs.ErrUnavailable)}
			continue
		}
		seen[id] = struct{}{}
		active = append(active, call)
		stages = append(stages, call.stage)
	}
	if len(active) == 0 {
		return
	}
	// An individual writer's expiry cancels only its own wait. It must not
	// cancel a batch containing other live writers; their deadlines still run.
	ctx, cancel := context.WithTimeout(b.ctx, writerRenewalBatchTimeout)
	defer cancel()
	response, err := b.authority.RenewWriterGrants(ctx, stages)
	if err == nil {
		err = response.Validate(len(stages))
		if err != nil {
			err = fmt.Errorf("invalid writer renewal response: %w: %w", err, errdefs.ErrUnavailable)
		}
	}
	results := make(map[string]protocol.BatchRenewResult, len(response.Results))
	if err == nil {
		for _, result := range response.Results {
			if _, requested := seen[result.GrantID]; !requested {
				err = fmt.Errorf("writer renewal response contains an unrequested grant: %w", errdefs.ErrUnavailable)
				break
			}
			results[result.GrantID] = result
		}
	}
	for _, call := range active {
		if call.ctx.Err() != nil {
			continue
		}
		reply := writerRenewalReply{err: err}
		if err == nil {
			result := results[call.stage.Identity.WriterGrantID]
			if result.ErrorCode != "" {
				reply.err = writerBatchResultError(result.ErrorCode)
			} else {
				reply.observation = *result.Observation
			}
		}
		call.result <- reply
	}
}

func writerBatchResultError(code string) error {
	classification := error(errdefs.ErrUnavailable)
	switch code {
	case protocol.RenewErrorInvalidArgument:
		classification = errdefs.ErrInvalidArgument
	case protocol.RenewErrorPermissionDenied:
		classification = errdefs.ErrPermissionDenied
	case protocol.RenewErrorNotFound:
		classification = errdefs.ErrNotFound
	case protocol.RenewErrorFailedPrecondition:
		classification = errdefs.ErrFailedPrecondition
	case protocol.RenewErrorDeadlineExceeded:
		classification = context.DeadlineExceeded
	}
	return fmt.Errorf("writer authority batch renewal rejected: %w", classification)
}

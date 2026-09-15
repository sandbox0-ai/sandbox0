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
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/containerd/errdefs"
	"github.com/opencontainers/go-digest"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshandoff"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/rootfswriterauthority"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

type batchTestAuthority struct {
	rootFSWriterAuthority
	batch func(context.Context, []rootfshandoff.StageRequest) (protocol.BatchRenewResponse, error)
}

func (a batchTestAuthority) RenewWriterGrants(ctx context.Context, stages []rootfshandoff.StageRequest) (protocol.BatchRenewResponse, error) {
	return a.batch(ctx, stages)
}

func batchTestStage(id string) rootfshandoff.StageRequest {
	return rootfshandoff.StageRequest{
		BindingVersion: rootfshandoff.WriterBindingVersion, Parent: digest.FromString(id).String(), InitialGeneration: "generation",
		Identity: rootfshandoff.Identity{
			NodeUID: "node", BootID: "boot", RuntimeGeneration: "1", AllocationID: "allocation",
			NetworkIncarnationID: "sandbox", TaskName: "slot", SourceOCIDigest: "source",
			RootFSDriver: "driver", RuntimeClass: "sandbox0-gvisor", SlotNonce: "nonce", ClaimID: "claim",
			LaunchAttempt: "attempt", RootFSID: "rootfs", WriterEpoch: 1, WriterGrantID: id,
			WriterGrantToken: "test-token", WriterGrantTokenDigest: rootfshandoff.WriterGrantTokenDigest("test-token"),
		},
		ExpectedPolicyToken: rootfshandoff.NetworkPolicyToken{
			AllocationID: "allocation", NetworkIncarnationID: "sandbox", ClaimID: "claim", NetworkEpoch: 1,
			PolicyDigest: digest.FromString("policy").String(), SourceIP: "192.0.2.2", CtldGeneration: "ctld", NetNSIdentity: "netns",
		},
	}
}

func batchTestObservation() protocol.LeaseObservation {
	now := time.Now().UTC()
	return protocol.LeaseObservation{ServerTime: now, RenewAfter: now.Add(15 * time.Second), LeaseExpiresAt: now.Add(30 * time.Second)}
}

func batchTestResponse(stages []rootfshandoff.StageRequest, observation protocol.LeaseObservation) protocol.BatchRenewResponse {
	response := protocol.BatchRenewResponse{}
	// Reverse order so tests require exact grant matching, not positional use.
	for i := len(stages) - 1; i >= 0; i-- {
		response.Results = append(response.Results, protocol.BatchRenewResult{GrantID: stages[i].Identity.WriterGrantID, Observation: &observation})
	}
	return response
}

func TestWriterRenewalBatcherCoalescesResidentsAndKeepsFencingIndependent(t *testing.T) {
	var requests, maxConcurrent, concurrent atomic.Int32
	observation := batchTestObservation()
	authority := batchTestAuthority{batch: func(ctx context.Context, stages []rootfshandoff.StageRequest) (protocol.BatchRenewResponse, error) {
		requests.Add(1)
		n := concurrent.Add(1)
		defer concurrent.Add(-1)
		for old := maxConcurrent.Load(); n > old; old = maxConcurrent.Load() {
			if maxConcurrent.CompareAndSwap(old, n) {
				break
			}
		}
		if len(stages) > protocol.MaxBatchRenewItems {
			t.Error("batch exceeds protocol limit")
		}
		response := batchTestResponse(stages, observation)
		for i, stage := range stages {
			if stage.Identity.WriterGrantToken != "" {
				t.Error("batch retained the one-time writer token")
			}
			if stage.Identity.WriterGrantID == "grant-17" {
				response.Results[len(stages)-1-i] = protocol.BatchRenewResult{GrantID: "grant-17", ErrorCode: protocol.RenewErrorFailedPrecondition, Error: "fenced"}
			}
		}
		return response, nil
	}}
	b := newWriterRenewalBatcher(authority)
	defer b.cancel()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	var wg sync.WaitGroup
	for i := 0; i < 420; i++ {
		wg.Go(func() {
			got, err := b.renew(ctx, batchTestStage(fmt.Sprintf("grant-%d", i)))
			if i == 17 {
				if !errdefs.IsFailedPrecondition(err) {
					t.Errorf("fenced grant error=%v", err)
				}
			} else if err != nil || !got.LeaseExpiresAt.Equal(observation.LeaseExpiresAt) {
				t.Errorf("grant %d renewal error=%v expiry=%v", i, err, got.LeaseExpiresAt)
			}
		})
	}
	wg.Wait()
	require.GreaterOrEqual(t, requests.Load(), int32(2))
	require.LessOrEqual(t, requests.Load(), int32(3), "420 simultaneous renewals should coalesce")
	require.LessOrEqual(t, maxConcurrent.Load(), int32(writerRenewalBatchWorkers))
}

func TestWriterRenewalBatcherOneExpiredWriterDoesNotCancelLivePeer(t *testing.T) {
	entered := make(chan []rootfshandoff.StageRequest, 1)
	release := make(chan struct{})
	observation := batchTestObservation()
	b := newWriterRenewalBatcher(batchTestAuthority{batch: func(ctx context.Context, stages []rootfshandoff.StageRequest) (protocol.BatchRenewResponse, error) {
		entered <- stages
		select {
		case <-ctx.Done():
			return protocol.BatchRenewResponse{}, ctx.Err()
		case <-release:
			return batchTestResponse(stages, observation), nil
		}
	}})
	defer b.cancel()
	short, cancelShort := context.WithTimeout(t.Context(), time.Second)
	defer cancelShort()
	long, cancelLong := context.WithTimeout(t.Context(), 4*time.Second)
	defer cancelLong()
	shortResult, longResult := make(chan error, 1), make(chan error, 1)
	go func() { _, err := b.renew(short, batchTestStage("short")); shortResult <- err }()
	go func() { _, err := b.renew(long, batchTestStage("long")); longResult <- err }()
	select {
	case stages := <-entered:
		require.Len(t, stages, 2)
	case <-long.Done():
		t.Fatal("batch did not start")
	}
	require.ErrorIs(t, <-shortResult, context.DeadlineExceeded)
	select {
	case err := <-longResult:
		t.Fatalf("expired writer poisoned live peer: %v", err)
	default:
	}
	close(release)
	require.NoError(t, <-longResult)
}

func TestWriterRenewalBatcherRejectsWrongGrantResponse(t *testing.T) {
	b := newWriterRenewalBatcher(batchTestAuthority{batch: func(ctx context.Context, stages []rootfshandoff.StageRequest) (protocol.BatchRenewResponse, error) {
		response := batchTestResponse(stages, batchTestObservation())
		response.Results[0].GrantID = "another-writer"
		return response, nil
	}})
	defer b.cancel()
	_, err := b.renew(t.Context(), batchTestStage("expected-writer"))
	require.ErrorIs(t, err, errdefs.ErrUnavailable)
}

func TestWriterRenewalBatcherRejectsFullQueue(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	// Hold dispatch to reproduce an outage with the entire bounded queue full.
	b := &writerRenewalBatcher{ctx: ctx, cancel: cancel, queue: make(chan writerRenewalCall, writerRenewalQueueSize)}
	for i := 0; i < cap(b.queue); i++ {
		b.queue <- writerRenewalCall{}
	}
	_, err := b.renew(ctx, batchTestStage("overflow"))
	require.ErrorIs(t, err, errdefs.ErrUnavailable)
	require.Len(t, b.queue, writerRenewalQueueSize)
}

func TestRuntimeWriterRenewalUsesBatchAuthorityAndStopsOnClose(t *testing.T) {
	entered := make(chan int, 1)
	stopped := make(chan struct{})
	authority := batchTestAuthority{batch: func(ctx context.Context, stages []rootfshandoff.StageRequest) (protocol.BatchRenewResponse, error) {
		entered <- len(stages)
		<-ctx.Done()
		close(stopped)
		return protocol.BatchRenewResponse{}, ctx.Err()
	}}
	r := &rootfsRuntime{authority: authority, renewals: make(map[string]*rootfsRenewal), logger: newLogger(zap.NewNop())}
	defer r.stopAllRenewals()
	observation := batchTestObservation()
	observation.RenewAfter = observation.ServerTime
	for i := 0; i < 10; i++ {
		r.startRenewal(batchTestStage(fmt.Sprintf("grant-%d", i)), observation, time.Now(), func(err error) { t.Errorf("stopped writer lost lease: %v", err) })
	}
	select {
	case count := <-entered:
		require.Equal(t, 10, count)
	case <-time.After(3 * time.Second):
		t.Fatal("runtime did not use batch authority")
	}
	r.stopAllRenewals()
	select {
	case <-stopped:
	case <-time.After(3 * time.Second):
		t.Fatal("close did not cancel batch I/O")
	}
}

func TestWriterRenewalBatchResultErrorsPreserveClassification(t *testing.T) {
	for code, expected := range map[string]error{
		protocol.RenewErrorInvalidArgument:    errdefs.ErrInvalidArgument,
		protocol.RenewErrorPermissionDenied:   errdefs.ErrPermissionDenied,
		protocol.RenewErrorNotFound:           errdefs.ErrNotFound,
		protocol.RenewErrorFailedPrecondition: errdefs.ErrFailedPrecondition,
		protocol.RenewErrorDeadlineExceeded:   context.DeadlineExceeded,
		protocol.RenewErrorUnavailable:        errdefs.ErrUnavailable,
	} {
		require.True(t, errors.Is(writerBatchResultError(code), expected), code)
	}
}

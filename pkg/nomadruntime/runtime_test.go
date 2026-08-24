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
	"strings"
	"testing"
	"time"

	"github.com/containerd/errdefs"

	"github.com/sandbox0-ai/sandbox0/pkg/rootfshandoff"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/rootfswriterauthority"
)

func TestWriterLeaseRenewalUsesAuthorityRelativeTime(t *testing.T) {
	serverTime := time.Date(2000, time.January, 1, 0, 0, 0, 0, time.UTC)
	observation := protocol.LeaseObservation{
		ServerTime: serverTime, RenewAfter: serverTime.Add(100 * time.Millisecond),
		LeaseExpiresAt: serverTime.Add(400 * time.Millisecond),
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	started := time.Now()
	called := make(chan time.Duration, 1)
	done := make(chan error, 1)
	go func() {
		done <- runWriterLeaseRenewal(ctx, rootfshandoff.StageRequest{}, observation, func(
			context.Context,
			rootfshandoff.StageRequest,
		) (protocol.LeaseObservation, error) {
			called <- time.Since(started)
			cancel()
			return observation, nil
		})
	}()
	delay := <-called
	if delay < 75*time.Millisecond {
		t.Fatalf("renewal delay = %s, want authority-relative delay", delay)
	}
	if err := <-done; err != nil {
		t.Fatalf("renewal returned error after cancellation: %v", err)
	}
}

func TestWriterLeaseRenewalFailsClosedAtLastObservedExpiry(t *testing.T) {
	now := time.Now().UTC()
	observation := protocol.LeaseObservation{
		ServerTime: now, RenewAfter: now.Add(20 * time.Millisecond),
		LeaseExpiresAt: now.Add(150 * time.Millisecond),
	}
	started := time.Now()
	err := runWriterLeaseRenewal(t.Context(), rootfshandoff.StageRequest{}, observation, func(
		context.Context,
		rootfshandoff.StageRequest,
	) (protocol.LeaseObservation, error) {
		return protocol.LeaseObservation{}, errors.Join(errors.New("PostgreSQL unavailable"), errdefs.ErrUnavailable)
	})
	if err == nil || !errors.Is(err, errdefs.ErrUnavailable) || !containsText(err.Error(), "writer lease expired") {
		t.Fatalf("renewal error = %v, want unavailable lease expiry", err)
	}
	elapsed := time.Since(started)
	if elapsed < 100*time.Millisecond || elapsed >= time.Second {
		t.Fatalf("lease expiry elapsed = %s, want [100ms,1s)", elapsed)
	}
}

func TestWriterLeaseRenewalImmediatelyRejectsStaleWriter(t *testing.T) {
	now := time.Now().UTC()
	observation := protocol.LeaseObservation{
		ServerTime: now, RenewAfter: now.Add(20 * time.Millisecond),
		LeaseExpiresAt: now.Add(time.Second),
	}
	started := time.Now()
	err := runWriterLeaseRenewal(t.Context(), rootfshandoff.StageRequest{}, observation, func(
		context.Context,
		rootfshandoff.StageRequest,
	) (protocol.LeaseObservation, error) {
		return protocol.LeaseObservation{}, errors.Join(errors.New("stale writer epoch"), errdefs.ErrFailedPrecondition)
	})
	if err == nil || !containsText(err.Error(), "authority rejected") {
		t.Fatalf("renewal error = %v, want immediate authority rejection", err)
	}
	if elapsed := time.Since(started); elapsed >= 500*time.Millisecond {
		t.Fatalf("stale-writer rejection elapsed = %s, want <500ms", elapsed)
	}
}

func TestReleaseRejectedRunningForkClearsOnlyPermanentAuthorityRejection(t *testing.T) {
	acknowledged := 0
	permanent := errors.Join(errors.New("target was deleted"), errdefs.ErrFailedPrecondition)
	err := releaseRejectedRunningFork(permanent, func() error {
		acknowledged++
		return nil
	})
	if !errors.Is(err, errdefs.ErrFailedPrecondition) || acknowledged != 1 {
		t.Fatalf("permanent rejection = %v, acknowledgements = %d", err, acknowledged)
	}

	transient := errors.Join(errors.New("manager unavailable"), errdefs.ErrUnavailable)
	err = releaseRejectedRunningFork(transient, func() error {
		acknowledged++
		return nil
	})
	if !errors.Is(err, errdefs.ErrUnavailable) || acknowledged != 1 {
		t.Fatalf("transient rejection = %v, acknowledgements = %d", err, acknowledged)
	}

	ackErr := errors.New("session journal unavailable")
	err = releaseRejectedRunningFork(permanent, func() error { return ackErr })
	if !errors.Is(err, errdefs.ErrFailedPrecondition) || !errors.Is(err, ackErr) {
		t.Fatalf("joined release error = %v", err)
	}
}

func containsText(value, fragment string) bool {
	return strings.Contains(value, fragment)
}

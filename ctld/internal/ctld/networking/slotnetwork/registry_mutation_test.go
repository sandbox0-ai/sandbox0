package slotnetwork

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/containerd/errdefs"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
	bbolt "go.etcd.io/bbolt"
)

func claimedMutationRegistry(t *testing.T) *Registry {
	t.Helper()
	directory := t.TempDir()
	netnsRoot := filepath.Join(directory, "netns")
	if err := ensureDirectory(netnsRoot); err != nil {
		t.Fatal(err)
	}
	r := newTestRegistry(t, filepath.Join(directory, "network.db"), netnsRoot,
		&fakeNamespaceInspector{podIP: "192.0.2.8"}, time.Hour)
	t.Cleanup(func() { _ = r.Close() })
	autoAcknowledge(r)
	if err := r.Register(t.Context(), testRegistrationRequest()); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Prepare(t.Context(), testPrepareRequest()); err != nil {
		t.Fatal(err)
	}
	return r
}

func nextPolicyRequest(previous protocol.RuntimeSlotNetworkPrepareRequest, revision int64) protocol.RuntimeSlotNetworkPrepareRequest {
	next := previous
	next.Request.OperationID = fmt.Sprintf("mutation-%d", revision)
	next.Request.PolicyRevision = revision
	next.Request.ExpectedPolicyDigest = previous.Request.PolicyDigest
	if strings.Contains(previous.Request.NetworkPolicy, "block-all") {
		next.Request.NetworkPolicy = strings.ReplaceAll(previous.Request.NetworkPolicy, "block-all", "allow-all")
	} else {
		next.Request.NetworkPolicy = strings.ReplaceAll(previous.Request.NetworkPolicy, "allow-all", "block-all")
	}
	next.Request.PolicyDigest = protocol.NetworkPolicyDigest(next.Request.NetworkPolicy)
	return next
}

func TestRegistryActivePolicyMutationPersistsAndFencesABA(t *testing.T) {
	r := claimedMutationRegistry(t)
	initial := testPrepareRequest()
	initialToken, err := r.Prepare(t.Context(), initial)
	if err != nil {
		t.Fatal(err)
	}
	first := nextPolicyRequest(initial, 7)
	firstToken, err := r.Prepare(t.Context(), first)
	if err != nil || firstToken.NetworkEpoch <= initialToken.NetworkEpoch || firstToken.PolicyDigest != first.Request.PolicyDigest {
		t.Fatalf("first mutation = %+v, %v", firstToken, err)
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	reopened := newTestRegistry(t, r.config.StatePath, r.config.NetNSRoot, r.inspector, time.Hour)
	defer reopened.Close()
	autoAcknowledge(reopened)
	_, revision, err := reopened.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	reopened.Acknowledge(revision)
	if retry, err := reopened.Prepare(t.Context(), first); err != nil || retry != firstToken {
		t.Fatalf("reopened exact retry = %+v, %v", retry, err)
	}
	last := first
	for revision := int64(8); revision < 40; revision++ {
		next := nextPolicyRequest(last, revision)
		if _, err := reopened.Prepare(t.Context(), next); err != nil {
			t.Fatal(err)
		}
		last = next
	}
	for _, stale := range []protocol.RuntimeSlotNetworkPrepareRequest{initial, first, nextPolicyRequest(initial, 6)} {
		if _, err := reopened.Prepare(t.Context(), stale); !errdefs.IsAlreadyExists(err) {
			t.Fatalf("stale operation %s = %v", stale.Request.OperationID, err)
		}
	}
	if len(reopened.operationSlots) != 1 {
		t.Fatalf("operation history grew with updates: %d", len(reopened.operationSlots))
	}
	if err := reopened.db.View(func(tx *bbolt.Tx) error {
		if count := tx.Bucket(operationsBucket).Stats().KeyN; count != 1 {
			return fmt.Errorf("durable operation count = %d", count)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := reopened.Cleanup(t.Context(), testCleanupRequest()); err != nil {
		t.Fatal(err)
	}
	if _, err := reopened.Prepare(t.Context(), nextPolicyRequest(last, 40)); !errdefs.IsFailedPrecondition(err) {
		t.Fatalf("terminal mutation = %v", err)
	}
}

func TestRegistryMutatesClaimedNetstackAndKeepsLiveIPCollision(t *testing.T) {
	r := claimedMutationRegistry(t)
	inspector := r.inspector.(*fakeNamespaceInspector)
	inspector.mu.Lock()
	inspector.err = fmt.Errorf("address moved into netstack: %w", errExactNamespaceUnroutable)
	inspector.transferred = true
	inspector.mu.Unlock()
	next := nextPolicyRequest(testPrepareRequest(), 7)
	if _, err := r.Prepare(t.Context(), next); err != nil {
		t.Fatalf("active netstack mutation: %v", err)
	}
	// A newly registered carrier must not silently displace the still-live
	// claimed netstack simply because its address is absent from host netlink.
	inspector.mu.Lock()
	inspector.err = nil
	inspector.mu.Unlock()
	second := testRegistrationRequest()
	second.SlotID, second.AllocationID, second.NetNSRelativePath = "slot-2", "allocation-2", "allocation-2"
	second.NetNSIdentity = "netns-v1:1:3"
	if err := r.Register(t.Context(), second); err != nil {
		t.Fatal(err)
	}
	inspector.mu.Lock()
	inspector.errors = []error{fmt.Errorf("address moved into netstack: %w", errExactNamespaceUnroutable), nil}
	inspector.mu.Unlock()
	sandboxes, _, err := r.Snapshot()
	if err != nil || len(sandboxes) != 2 || r.Stats().Orphaned != 0 {
		t.Fatalf("live source collision = %+v, %v, stats %+v", sandboxes, err, r.Stats())
	}
	inspector.mu.Lock()
	inspector.transferred = false
	inspector.err = fmt.Errorf("carrier removed: %w", errExactNamespaceUnroutable)
	inspector.mu.Unlock()
	if _, err := r.Prepare(t.Context(), nextPolicyRequest(next, 8)); err == nil {
		t.Fatal("mutated a removed carrier")
	}
}

func TestRegistryActivePolicyMutationRejectsChangedAuthority(t *testing.T) {
	for name, change := range map[string]func(*protocol.RuntimeSlotNetworkPrepareRequest){
		"claim":           func(r *protocol.RuntimeSlotNetworkPrepareRequest) { r.Request.ClaimID = "other-claim" },
		"operation reuse": func(r *protocol.RuntimeSlotNetworkPrepareRequest) { r.Request.OperationID = "operation-1" },
		"prior digest": func(r *protocol.RuntimeSlotNetworkPrepareRequest) {
			r.Request.ExpectedPolicyDigest = protocol.NetworkPolicyDigest("stale")
		},
		"sandbox": func(r *protocol.RuntimeSlotNetworkPrepareRequest) {
			r.Request.NetworkPolicy = strings.ReplaceAll(r.Request.NetworkPolicy, "sandbox-1", "other-sandbox")
		},
		"team": func(r *protocol.RuntimeSlotNetworkPrepareRequest) {
			r.Request.NetworkPolicy = strings.ReplaceAll(r.Request.NetworkPolicy, "team-1", "other-team")
		},
		"namespace": func(r *protocol.RuntimeSlotNetworkPrepareRequest) { r.Request.NetNSIdentity = "netns-v1:1:3" },
		"path":      func(r *protocol.RuntimeSlotNetworkPrepareRequest) { r.NetNSRelativePath = "other-allocation" },
	} {
		t.Run(name, func(t *testing.T) {
			r := claimedMutationRegistry(t)
			next := nextPolicyRequest(testPrepareRequest(), 7)
			change(&next)
			next.Request.PolicyDigest = protocol.NetworkPolicyDigest(next.Request.NetworkPolicy)
			before := r.Stats().Revision
			if _, err := r.Prepare(t.Context(), next); err == nil {
				t.Fatal("accepted changed authority")
			}
			if r.Stats().Revision != before {
				t.Fatal("rejected mutation changed desired policy")
			}
		})
	}
}

func TestRegistryActivePolicyMutationWaitsForApplyAndRetries(t *testing.T) {
	r := claimedMutationRegistry(t)
	r.SetNotify(nil)
	next := nextPolicyRequest(testPrepareRequest(), 7)
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	if _, err := r.Prepare(ctx, next); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("unacknowledged mutation = %v", err)
	}
	sandboxes, revision, err := r.Snapshot()
	if err != nil || len(sandboxes) != 1 || sandboxes[0].NetworkPolicyHash != next.Request.PolicyDigest {
		t.Fatalf("pending policy = %+v, %v", sandboxes, err)
	}
	r.Acknowledge(revision)
	if _, err := r.Prepare(t.Context(), next); err != nil {
		t.Fatal(err)
	}
}

func TestRegistryConcurrentPolicyMutationsHaveOneWinner(t *testing.T) {
	r := claimedMutationRegistry(t)
	var wg sync.WaitGroup
	errors := make(chan error, 2)
	for _, revision := range []int64{7, 8} {
		wg.Go(func() {
			_, err := r.Prepare(t.Context(), nextPolicyRequest(testPrepareRequest(), revision))
			errors <- err
		})
	}
	wg.Wait()
	close(errors)
	successes := 0
	for err := range errors {
		if err == nil {
			successes++
		} else if !errdefs.IsAlreadyExists(err) {
			t.Fatalf("conflicting mutation = %v", err)
		}
	}
	if successes != 1 {
		t.Fatalf("successful conflicting mutations = %d", successes)
	}
}

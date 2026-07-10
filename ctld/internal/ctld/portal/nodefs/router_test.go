package nodefs

import (
	"context"
	"errors"
	"testing"
	"time"
)

func mustNodeID(t testing.TB, slot Slot, localID uint64) uint64 {
	t.Helper()
	id, err := EncodeNodeID(slot, localID)
	if err != nil {
		t.Fatalf("EncodeNodeID(%d, %d) error = %v", slot, localID, err)
	}
	return id
}

func mustHandleID(t testing.TB, slot Slot, localID uint64) uint64 {
	t.Helper()
	id, err := EncodeHandleID(slot, localID)
	if err != nil {
		t.Fatalf("EncodeHandleID(%d, %d) error = %v", slot, localID, err)
	}
	return id
}

func mustBindingNodeID(t testing.TB, slot Slot, generation, localID uint64) uint64 {
	t.Helper()
	id, err := EncodeBindingNodeID(slot, generation, localID)
	if err != nil {
		t.Fatalf("EncodeBindingNodeID(%d, %d, %d) error = %v", slot, generation, localID, err)
	}
	return id
}

func mustBindingHandleID(t testing.TB, slot Slot, generation, localID uint64) uint64 {
	t.Helper()
	id, err := EncodeBindingHandleID(slot, generation, localID)
	if err != nil {
		t.Fatalf("EncodeBindingHandleID(%d, %d, %d) error = %v", slot, generation, localID, err)
	}
	return id
}

func TestRouterAcquireNodeHandleDecodesOnePortalRequest(t *testing.T) {
	t.Parallel()
	router := NewRouter[string]()
	if err := router.Register(7, "portal-a"); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	lease, localNode, localHandle, err := router.AcquireNodeHandle(
		mustNodeID(t, 7, 11),
		mustHandleID(t, 7, 21),
	)
	if err != nil {
		t.Fatalf("AcquireNodeHandle() error = %v", err)
	}
	defer lease.Release()
	if lease.Slot != 7 || lease.Target != "portal-a" {
		t.Fatalf("route = (%d, %q), want (7, portal-a)", lease.Slot, lease.Target)
	}
	if localNode != 11 || localHandle != 21 {
		t.Fatalf("local IDs = (%d, %d), want (11, 21)", localNode, localHandle)
	}
}

func TestRouterAcquireNodeHandlePreservesOptionalZeroHandle(t *testing.T) {
	t.Parallel()
	router := NewRouter[string]()
	if err := router.Register(7, "portal-a"); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	lease, localNode, localHandle, err := router.AcquireNodeHandle(mustNodeID(t, 7, 11), 0)
	if err != nil {
		t.Fatalf("AcquireNodeHandle() error = %v", err)
	}
	defer lease.Release()
	if localNode != 11 || localHandle != 0 {
		t.Fatalf("local IDs = (%d, %d), want (11, 0)", localNode, localHandle)
	}
}

func TestRouterRejectsCrossPortalRequests(t *testing.T) {
	t.Parallel()
	router := NewRouter[string]()
	if err := router.Register(7, "portal-a"); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	if _, _, _, err := router.AcquireNodes(mustNodeID(t, 7, 11), mustNodeID(t, 8, 12)); !errors.Is(err, ErrCrossPortal) {
		t.Fatalf("AcquireNodes() error = %v, want %v", err, ErrCrossPortal)
	}
	if _, _, _, err := router.AcquireNodeHandle(mustNodeID(t, 7, 11), mustHandleID(t, 8, 21)); !errors.Is(err, ErrCrossPortal) {
		t.Fatalf("AcquireNodeHandle() error = %v, want %v", err, ErrCrossPortal)
	}
	if _, _, _, _, _, err := router.AcquireCopy(
		mustNodeID(t, 7, 11), mustHandleID(t, 7, 21),
		mustNodeID(t, 7, 12), mustHandleID(t, 8, 22),
	); !errors.Is(err, ErrCrossPortal) {
		t.Fatalf("AcquireCopy() error = %v, want %v", err, ErrCrossPortal)
	}
}

func TestRouterAcquireCopyDecodesAllIDs(t *testing.T) {
	t.Parallel()
	router := NewRouter[string]()
	if err := router.Register(9, "portal-a"); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	lease, inputNode, inputHandle, outputNode, outputHandle, err := router.AcquireCopy(
		mustNodeID(t, 9, 11), mustHandleID(t, 9, 21),
		mustNodeID(t, 9, 12), mustHandleID(t, 9, 22),
	)
	if err != nil {
		t.Fatalf("AcquireCopy() error = %v", err)
	}
	defer lease.Release()
	if inputNode != 11 || inputHandle != 21 || outputNode != 12 || outputHandle != 22 {
		t.Fatalf("local IDs = (%d, %d, %d, %d), want (11, 21, 12, 22)", inputNode, inputHandle, outputNode, outputHandle)
	}
}

func TestRouterRejectsSyntheticAndMissingRoutes(t *testing.T) {
	t.Parallel()
	router := NewRouter[string]()
	if _, _, err := router.AcquireNode(ShardRootNodeID); !errors.Is(err, ErrSyntheticNode) {
		t.Fatalf("AcquireNode(shard root) error = %v, want %v", err, ErrSyntheticNode)
	}
	if _, _, err := router.AcquireHandle(0); !errors.Is(err, ErrInvalidSlot) {
		t.Fatalf("AcquireHandle(0) error = %v, want %v", err, ErrInvalidSlot)
	}
	if _, _, err := router.AcquireNode(mustNodeID(t, 9, 1)); !errors.Is(err, ErrSlotNotFound) {
		t.Fatalf("AcquireNode(missing slot) error = %v, want %v", err, ErrSlotNotFound)
	}
}

func TestRouterRejectsDuplicateAndRetiredRegistration(t *testing.T) {
	t.Parallel()
	router := NewRouter[string]()
	if err := router.Register(3, "first"); err != nil {
		t.Fatalf("Register(first) error = %v", err)
	}
	if err := router.Register(3, "second"); !errors.Is(err, ErrSlotRegistered) {
		t.Fatalf("Register(second) error = %v, want %v", err, ErrSlotRegistered)
	}
	if _, err := router.Drain(context.Background(), 3); err != nil {
		t.Fatalf("Drain() error = %v", err)
	}
	if err := router.Register(3, "replacement"); !errors.Is(err, ErrSlotRetired) {
		t.Fatalf("Register(retired) error = %v, want %v", err, ErrSlotRetired)
	}
	if err := router.Retire(4); err != nil {
		t.Fatalf("Retire() error = %v", err)
	}
	if err := router.Register(4, "replacement"); !errors.Is(err, ErrSlotRetired) {
		t.Fatalf("Register(imported tombstone) error = %v, want %v", err, ErrSlotRetired)
	}
}

func TestRouterDrainWaitsForInFlightLease(t *testing.T) {
	t.Parallel()
	router := NewRouter[string]()
	if err := router.Register(5, "portal-a"); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	lease, _, err := router.AcquireNode(mustNodeID(t, 5, 1))
	if err != nil {
		t.Fatalf("AcquireNode() error = %v", err)
	}

	drained := make(chan struct{})
	var target string
	var drainErr error
	go func() {
		target, drainErr = router.Drain(context.Background(), 5)
		close(drained)
	}()

	deadline := time.Now().Add(time.Second)
	for {
		probe, _, err := router.AcquireNode(mustNodeID(t, 5, 1))
		if errors.Is(err, ErrSlotDraining) {
			break
		}
		if err == nil {
			probe.Release()
			time.Sleep(time.Millisecond)
			continue
		}
		if time.Now().After(deadline) {
			t.Fatalf("AcquireNode() did not observe draining state, last error = %v", err)
		}
		time.Sleep(time.Millisecond)
	}
	select {
	case <-drained:
		t.Fatal("Drain() returned before the in-flight lease was released")
	default:
	}

	lease.Release()
	select {
	case <-drained:
	case <-time.After(time.Second):
		t.Fatal("Drain() did not return after lease release")
	}
	if drainErr != nil || target != "portal-a" {
		t.Fatalf("Drain() = (%q, %v), want (portal-a, nil)", target, drainErr)
	}
	if _, _, err := router.AcquireNode(mustNodeID(t, 5, 1)); !errors.Is(err, ErrSlotNotFound) {
		t.Fatalf("AcquireNode(after drain) error = %v, want %v", err, ErrSlotNotFound)
	}
}

func TestRouterCanceledDrainRemainsDrainingAndCanResume(t *testing.T) {
	t.Parallel()
	router := NewRouter[string]()
	if err := router.Register(6, "portal-a"); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	lease, _, err := router.AcquireNode(mustNodeID(t, 6, 1))
	if err != nil {
		t.Fatalf("AcquireNode() error = %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := router.Drain(ctx, 6); !errors.Is(err, context.Canceled) {
		t.Fatalf("Drain(canceled) error = %v, want %v", err, context.Canceled)
	}
	if _, _, err := router.AcquireNode(mustNodeID(t, 6, 1)); !errors.Is(err, ErrSlotDraining) {
		t.Fatalf("AcquireNode(after canceled drain) error = %v, want %v", err, ErrSlotDraining)
	}
	lease.Release()
	target, err := router.Drain(context.Background(), 6)
	if err != nil || target != "portal-a" {
		t.Fatalf("Drain(resumed) = (%q, %v), want (portal-a, nil)", target, err)
	}
}

func BenchmarkRouterAcquireNodeHandle(b *testing.B) {
	router := NewRouter[*int]()
	target := 1
	if err := router.Register(1, &target); err != nil {
		b.Fatalf("Register() error = %v", err)
	}
	nodeID := mustNodeID(b, 1, 11)
	handleID := mustHandleID(b, 1, 21)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		lease, _, _, err := router.AcquireNodeHandle(nodeID, handleID)
		if err != nil {
			b.Fatal(err)
		}
		lease.Release()
	}
}

//go:build linux

package ha

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestNewCoordinatorValidatesConfig(t *testing.T) {
	if _, err := NewCoordinator(Config{Slot: "a"}); err == nil {
		t.Fatal("NewCoordinator() error = nil, want missing root directory error")
	}
	if _, err := NewCoordinator(Config{RootDir: t.TempDir()}); err == nil {
		t.Fatal("NewCoordinator() error = nil, want missing slot error")
	}
}

func TestCoordinatorPromotesStandbyAfterPrimaryCloses(t *testing.T) {
	root := t.TempDir()
	primaryCoordinator := newTestCoordinator(t, root, "a")
	primary, err := primaryCoordinator.WaitForPrimary(context.Background())
	if err != nil {
		t.Fatalf("WaitForPrimary(primary) error = %v", err)
	}
	if primary.Epoch != 1 {
		t.Fatalf("primary epoch = %d, want 1", primary.Epoch)
	}

	standbyCoordinator := newTestCoordinator(t, root, "b")
	result := make(chan primaryResult, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		lease, waitErr := standbyCoordinator.WaitForPrimary(ctx)
		result <- primaryResult{lease: lease, err: waitErr}
	}()
	waitForRole(t, standbyCoordinator, RoleStandby)
	if state := standbyCoordinator.State(); !state.Synchronized || state.Epoch != primary.Epoch {
		t.Fatalf("standby state = %#v, want synchronized epoch %d", state, primary.Epoch)
	}

	if err := primary.Close(); err != nil {
		t.Fatalf("Close(primary) error = %v", err)
	}
	select {
	case promoted := <-result:
		if promoted.err != nil {
			t.Fatalf("WaitForPrimary(standby) error = %v", promoted.err)
		}
		defer promoted.lease.Close()
		if promoted.lease.Epoch != 2 {
			t.Fatalf("promoted epoch = %d, want 2", promoted.lease.Epoch)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("standby was not promoted")
	}
}

func TestCoordinatorStandbyWaitHonorsCancellation(t *testing.T) {
	root := t.TempDir()
	primaryCoordinator := newTestCoordinator(t, root, "a")
	primary, err := primaryCoordinator.WaitForPrimary(context.Background())
	if err != nil {
		t.Fatalf("WaitForPrimary(primary) error = %v", err)
	}
	defer primary.Close()

	standbyCoordinator := newTestCoordinator(t, root, "b")
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		_, waitErr := standbyCoordinator.WaitForPrimary(ctx)
		result <- waitErr
	}()
	waitForRole(t, standbyCoordinator, RoleStandby)
	cancel()
	select {
	case waitErr := <-result:
		if !errors.Is(waitErr, context.Canceled) {
			t.Fatalf("WaitForPrimary() error = %v, want context canceled", waitErr)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("standby did not exit after cancellation")
	}
}

func TestCoordinatorMetricsTrackRoleAndLock(t *testing.T) {
	coordinator := newTestCoordinator(t, t.TempDir(), "a")
	lease, err := coordinator.WaitForPrimary(context.Background())
	if err != nil {
		t.Fatalf("WaitForPrimary() error = %v", err)
	}
	snapshot := coordinator.MetricsSnapshot()
	if snapshot.State.Role != RolePrimary || !snapshot.State.Synchronized {
		t.Fatalf("state = %#v, want synchronized primary", snapshot.State)
	}
	if !snapshot.LockIdentity.Known {
		t.Fatal("lock identity is not known")
	}
	if snapshot.Transitions[RoleTransition{From: RoleStarting, To: RolePrimary}] != 1 {
		t.Fatalf("transitions = %#v, want starting-to-primary transition", snapshot.Transitions)
	}
	if err := lease.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if coordinator.State().Role != RoleStarting {
		t.Fatalf("role after close = %q, want starting", coordinator.State().Role)
	}
}

type primaryResult struct {
	lease *PrimaryLease
	err   error
}

func newTestCoordinator(t *testing.T, root, slot string) *Coordinator {
	t.Helper()
	coordinator, err := NewCoordinator(Config{RootDir: root, Slot: slot})
	if err != nil {
		t.Fatalf("NewCoordinator() error = %v", err)
	}
	return coordinator
}

func waitForRole(t *testing.T, coordinator *Coordinator, role Role) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for coordinator.State().Role != role {
		if time.Now().After(deadline) {
			t.Fatalf("coordinator role = %q, want %q", coordinator.State().Role, role)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

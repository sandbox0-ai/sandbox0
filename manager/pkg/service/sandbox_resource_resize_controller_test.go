package service

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	"github.com/sandbox0-ai/sandbox0/pkg/apierror"
)

func TestSandboxResourceResizeControllerDiscoversAndRetriesPendingOperation(t *testing.T) {
	store := &sandboxResourceResizeControllerTestStore{mutations: []*sandboxstore.SandboxResourceResize{{
		SandboxID: "sandbox-a", Phase: sandboxstore.SandboxResourceResizePausing,
	}}}
	reconciler := &sandboxResourceResizeControllerTestReconciler{
		errors: []error{errors.New("transient node channel failure"), nil},
		calls:  make(chan string, 4),
	}
	controller := NewSandboxResourceResizeController(store, reconciler, nil)
	controller.resyncInterval = time.Hour
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- controller.Run(ctx, 1) }()

	for index := 0; index < 2; index++ {
		select {
		case sandboxID := <-reconciler.calls:
			if sandboxID != "sandbox-a" {
				t.Fatalf("reconciled sandbox = %q", sandboxID)
			}
		case <-time.After(3 * time.Second):
			t.Fatalf("timed out waiting for reconcile call %d", index+1)
		}
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("Run() error = %v", err)
	}
	if store.listCalls != 1 {
		t.Fatalf("pending scans = %d", store.listCalls)
	}
}

func TestSandboxResourceResizeControllerForgetsPreemptedOperation(t *testing.T) {
	store := &sandboxResourceResizeControllerTestStore{}
	reconciler := &sandboxResourceResizeControllerTestReconciler{
		errors: []error{apierror.NewConflict(
			"sandbox", "sandbox-a", errors.New("preempted"),
		)},
		calls: make(chan string, 2),
	}
	controller := NewSandboxResourceResizeController(store, reconciler, nil)
	controller.EnqueueSandboxResourceResize("sandbox-a")
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- controller.Run(ctx, 1) }()
	select {
	case <-reconciler.calls:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for preempted reconcile")
	}
	time.Sleep(50 * time.Millisecond)
	reconciler.mu.Lock()
	calls := reconciler.callCount
	reconciler.mu.Unlock()
	if calls != 1 {
		t.Fatalf("preempted mutation reconciled %d times", calls)
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("Run() error = %v", err)
	}
}

type sandboxResourceResizeControllerTestStore struct {
	mutations []*sandboxstore.SandboxResourceResize
	listCalls int
}

func (s *sandboxResourceResizeControllerTestStore) ListPendingSandboxResourceResizes(
	_ context.Context,
	_ int,
) ([]*sandboxstore.SandboxResourceResize, error) {
	s.listCalls++
	return s.mutations, nil
}

type sandboxResourceResizeControllerTestReconciler struct {
	mu        sync.Mutex
	errors    []error
	callCount int
	calls     chan string
}

func (r *sandboxResourceResizeControllerTestReconciler) CompleteSandboxResourceResize(
	_ context.Context,
	sandboxID string,
) error {
	r.mu.Lock()
	index := r.callCount
	r.callCount++
	var err error
	if index < len(r.errors) {
		err = r.errors[index]
	}
	r.mu.Unlock()
	r.calls <- sandboxID
	return err
}

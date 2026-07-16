//go:build linux

package activeguard

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"
)

func TestGuardSerializesActiveNetdAndTransfersOwnership(t *testing.T) {
	path := filepath.Join(t.TempDir(), "netd.lock")
	first, err := Acquire(context.Background(), path)
	if err != nil {
		t.Fatalf("Acquire(first) error = %v", err)
	}

	secondResult := make(chan *Guard, 1)
	secondErrors := make(chan error, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	go func() {
		second, acquireErr := Acquire(ctx, path)
		if acquireErr != nil {
			secondErrors <- acquireErr
			return
		}
		secondResult <- second
	}()
	select {
	case second := <-secondResult:
		_ = second.Close()
		t.Fatal("second guard acquired before first released")
	case err := <-secondErrors:
		t.Fatalf("Acquire(second) early error = %v", err)
	case <-time.After(150 * time.Millisecond):
	}
	if err := first.Close(); err != nil {
		t.Fatalf("Close(first) error = %v", err)
	}
	select {
	case second := <-secondResult:
		defer second.Close()
	case err := <-secondErrors:
		t.Fatalf("Acquire(second) error = %v", err)
	case <-time.After(time.Second):
		t.Fatal("second guard did not acquire after release")
	}
}

func TestGuardWaitHonorsCancellation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "netd.lock")
	first, err := Acquire(context.Background(), path)
	if err != nil {
		t.Fatalf("Acquire(first) error = %v", err)
	}
	defer first.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := Acquire(ctx, path); !errors.Is(err, context.Canceled) {
		t.Fatalf("Acquire(canceled) error = %v, want context canceled", err)
	}
}

func TestGuardDoesNotAcquireAnAvailableLockAfterCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := Acquire(ctx, filepath.Join(t.TempDir(), "netd.lock")); !errors.Is(err, context.Canceled) {
		t.Fatalf("Acquire(canceled, available) error = %v, want context canceled", err)
	}
}

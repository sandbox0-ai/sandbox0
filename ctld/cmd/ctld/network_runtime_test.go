//go:build linux

package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	ctldha "github.com/sandbox0-ai/sandbox0/ctld/internal/ctld/ha"
	ctldportal "github.com/sandbox0-ai/sandbox0/ctld/internal/ctld/portal"
)

type fakePrimaryService struct {
	started         chan struct{}
	fail            chan error
	shutdownStarted chan struct{}
	finishShutdown  chan struct{}
	ready           atomic.Bool
}

func newFakePrimaryService() *fakePrimaryService {
	return &fakePrimaryService{
		started:         make(chan struct{}),
		fail:            make(chan error, 1),
		shutdownStarted: make(chan struct{}),
		finishShutdown:  make(chan struct{}),
	}
}

func (s *fakePrimaryService) Run(ctx context.Context) error {
	close(s.started)
	select {
	case err := <-s.fail:
		return err
	case <-ctx.Done():
		close(s.shutdownStarted)
		<-s.finishShutdown
		return nil
	}
}

func (s *fakePrimaryService) Ready() bool {
	return s.ready.Load()
}

func TestPrimaryServiceHandlePropagatesFailureAndReadiness(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	service := newFakePrimaryService()
	handle := startPrimaryService(ctx, service)
	<-service.started
	if handle.Ready() {
		t.Fatal("service is ready before its runtime synchronized")
	}
	service.ready.Store(true)
	if !handle.Ready() {
		t.Fatal("service readiness was not propagated")
	}

	wantErr := errors.New("netd proxy failed")
	service.fail <- wantErr
	select {
	case err := <-handle.Errors():
		if !errors.Is(err, wantErr) {
			t.Fatalf("Errors() = %v, want %v", err, wantErr)
		}
	case <-time.After(time.Second):
		t.Fatal("service failure was not propagated")
	}
	waitCtx, waitCancel := context.WithTimeout(context.Background(), time.Second)
	defer waitCancel()
	if err := handle.Wait(waitCtx); !errors.Is(err, wantErr) {
		t.Fatalf("Wait() = %v, want %v", err, wantErr)
	}
}

func TestPrimaryServiceHandleWaitsForGracefulShutdown(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	service := newFakePrimaryService()
	handle := startPrimaryService(ctx, service)
	<-service.started
	cancel()
	<-service.shutdownStarted

	shortCtx, shortCancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer shortCancel()
	if err := handle.Wait(shortCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Wait() before shutdown completed = %v, want deadline exceeded", err)
	}
	close(service.finishShutdown)
	waitCtx, waitCancel := context.WithTimeout(context.Background(), time.Second)
	defer waitCancel()
	if err := handle.Wait(waitCtx); err != nil {
		t.Fatalf("Wait() after shutdown completed = %v", err)
	}
}

func TestNetworkRuntimeExitErrorTreatsCanceledParentAsGraceful(t *testing.T) {
	for _, networkErr := range []error{nil, context.Canceled} {
		if err, failed := networkRuntimeExitError(context.Canceled, networkErr); err != nil || failed {
			t.Fatalf("networkRuntimeExitError(parent canceled, %v) = (%v, %t), want (nil, false)", networkErr, err, failed)
		}
	}
	if err, failed := networkRuntimeExitError(nil, nil); err == nil || !failed {
		t.Fatalf("networkRuntimeExitError(active parent, nil) = (%v, %t), want failure", err, failed)
	}
}

func TestConfiguredNetworkRuntimeFactoryValidatesBeforePrimaryElection(t *testing.T) {
	factory, err := configuredNetworkRuntimeFactory("", ":8095")
	if err != nil || factory != nil {
		t.Fatalf("configuredNetworkRuntimeFactory(empty) = (%v, %v), want (nil, nil)", factory, err)
	}

	if _, err := configuredNetworkRuntimeFactory(t.TempDir()+"/missing.yaml", ":8095"); err == nil {
		t.Fatal("configuredNetworkRuntimeFactory(missing) succeeded, want validation error")
	}

	configPath := filepath.Join(t.TempDir(), "netd.yaml")
	if err := os.WriteFile(configPath, []byte("node_name: node-a\nhealth_port: 8095\n"), 0o600); err != nil {
		t.Fatalf("write network runtime config: %v", err)
	}
	if _, err := configuredNetworkRuntimeFactory(configPath, ":8095"); err == nil {
		t.Fatal("configuredNetworkRuntimeFactory accepted a ctld port collision")
	}
}

func TestRunHAPrimaryReleasesLeaseAfterNetworkRuntimeFailure(t *testing.T) {
	root := t.TempDir()
	primaryCoordinator, err := ctldha.NewCoordinator(ctldha.Config{RootDir: root, Slot: "a"})
	if err != nil {
		t.Fatalf("NewCoordinator(primary) error = %v", err)
	}
	standbyCoordinator, err := ctldha.NewCoordinator(ctldha.Config{RootDir: root, Slot: "b"})
	if err != nil {
		t.Fatalf("NewCoordinator(standby) error = %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	replicatorReady := make(chan *ctldha.Replicator, 1)
	failPrimary := make(chan struct{})
	wantErr := errors.New("network runtime failed")
	primaryResult := make(chan error, 1)
	go func() {
		primaryResult <- runHAPrimary(ctx, primaryCoordinator, nil, nil, func(_ context.Context, options primaryRunOptions) error {
			options.replicator.SetSnapshotProvider(func(context.Context, ctldportal.PortalReplicator) error { return nil })
			replicatorReady <- options.replicator
			<-failPrimary
			return wantErr
		})
	}()
	replicator := <-replicatorReady

	standbyResult := make(chan *ctldha.PrimaryLease, 1)
	standbyErrors := make(chan error, 1)
	go func() {
		lease, waitErr := standbyCoordinator.WaitForPrimary(ctx)
		if waitErr != nil {
			standbyErrors <- waitErr
			return
		}
		standbyResult <- lease
	}()
	waitForReplicatorReady(t, replicator)
	close(failPrimary)

	select {
	case err := <-primaryResult:
		if !errors.Is(err, wantErr) {
			t.Fatalf("runHAPrimary() error = %v, want %v", err, wantErr)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("failed primary did not exit")
	}
	select {
	case promoted := <-standbyResult:
		defer promoted.Close()
		if promoted.Epoch != 2 {
			t.Fatalf("promoted epoch = %d, want 2", promoted.Epoch)
		}
	case err := <-standbyErrors:
		t.Fatalf("standby promotion error = %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("standby was not promoted after primary network runtime failure")
	}
}

func waitForReplicatorReady(t *testing.T, replicator *ctldha.Replicator) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for !replicator.Ready() {
		if time.Now().After(deadline) {
			t.Fatal("standby did not synchronize with the primary")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

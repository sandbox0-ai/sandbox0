package portal

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func TestPortalRecoveryWriterCoalescesLatestManifest(t *testing.T) {
	store := newPortalRecoveryStore(t.TempDir())
	writer := newPortalRecoveryWriter(store, nil, nil)
	t.Cleanup(func() { _ = writer.Close(context.Background()) })
	manifest := recoveryWriterTestManifest("pod-a\x00workspace")
	for i := 0; i < 100; i++ {
		manifest.MountPath = filepath.Join("/workspace", time.Unix(int64(i), 0).Format("150405"))
		if err := writer.EnqueuePut(manifest); err != nil {
			t.Fatalf("EnqueuePut() error = %v", err)
		}
	}
	if err := writer.Flush(context.Background()); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}
	manifests, err := store.List()
	if err != nil {
		t.Fatalf("List() error = %v", err)
	}
	if len(manifests) != 1 {
		t.Fatalf("manifest count = %d, want 1", len(manifests))
	}
	if manifests[0].MountPath != manifest.MountPath {
		t.Fatalf("stored mount path = %q, want latest %q", manifests[0].MountPath, manifest.MountPath)
	}
}

func TestPortalRecoveryWriterOrdersDeleteAfterPendingPut(t *testing.T) {
	store := newPortalRecoveryStore(t.TempDir())
	writer := newPortalRecoveryWriter(store, nil, nil)
	t.Cleanup(func() { _ = writer.Close(context.Background()) })
	manifest := recoveryWriterTestManifest("pod-a\x00workspace")
	if err := writer.EnqueuePut(manifest); err != nil {
		t.Fatal(err)
	}
	if err := writer.DeleteAndWait(context.Background(), manifest.Key); err != nil {
		t.Fatalf("DeleteAndWait() error = %v", err)
	}
	manifests, err := store.List()
	if err != nil {
		t.Fatal(err)
	}
	if len(manifests) != 0 {
		t.Fatalf("manifest count = %d after delete, want 0", len(manifests))
	}
}

func TestPortalRecoveryWriterRetriesFailedGroupCommit(t *testing.T) {
	store := &flakyRecoveryPersistence{
		base:       newPortalRecoveryStore(t.TempDir()),
		failures:   1,
		failedOnce: make(chan struct{}),
	}
	writer := newPortalRecoveryWriter(store, nil, nil)
	manifest := recoveryWriterTestManifest("pod-a\x00workspace")
	if err := writer.EnqueuePut(manifest); err != nil {
		t.Fatal(err)
	}
	select {
	case <-store.failedOnce:
	case <-time.After(time.Second):
		t.Fatal("group commit failure was not observed")
	}
	if writer.Error() == nil {
		t.Fatal("writer did not expose unresolved local commit failure")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := writer.Flush(ctx); err != nil {
		t.Fatalf("Flush() after retry error = %v", err)
	}
	if writer.Error() != nil {
		t.Fatalf("writer error remained after successful retry: %v", writer.Error())
	}
	if err := writer.Close(ctx); err != nil {
		t.Fatal(err)
	}
	manifests, err := store.base.List()
	if err != nil {
		t.Fatal(err)
	}
	if len(manifests) != 1 || manifests[0].Key != manifest.Key {
		t.Fatalf("manifests after retry = %+v", manifests)
	}
}

func TestStandbyAcknowledgedRecoveryUpdateDoesNotWaitForLocalDisk(t *testing.T) {
	store := &blockingRecoveryPersistence{
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	writer := newPortalRecoveryWriter(store, nil, nil)
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(store.release) }) }
	t.Cleanup(func() {
		unblock()
		_ = writer.Close(context.Background())
	})
	mgr := &Manager{
		replicator:     readinessPortalReplicator(true),
		recoveryWriter: writer,
		observer:       NewObserver(nil, nil),
	}
	done := make(chan error, 1)
	go func() {
		done <- mgr.updateRecoveryManifest(context.Background(), recoveryWriterTestManifest("pod-a\x00workspace"))
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("updateRecoveryManifest() error = %v", err)
		}
	case <-time.After(time.Second):
		unblock()
		t.Fatal("standby-acknowledged update waited for blocked local persistence")
	}
	select {
	case <-store.started:
	case <-time.After(time.Second):
		unblock()
		t.Fatal("asynchronous local persistence did not start")
	}
	unblock()
	if err := writer.Close(context.Background()); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
}

func TestLocalRecoverySyncRequired(t *testing.T) {
	tests := []struct {
		name       string
		manager    *Manager
		syncNeeded bool
	}{
		{name: "nil manager", syncNeeded: true},
		{name: "no standby", manager: &Manager{}, syncNeeded: true},
		{name: "standby not ready", manager: &Manager{replicator: readinessPortalReplicator(false)}, syncNeeded: true},
		{name: "standby ready", manager: &Manager{replicator: readinessPortalReplicator(true)}, syncNeeded: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := test.manager.localRecoverySyncRequired(); got != test.syncNeeded {
				t.Fatalf("localRecoverySyncRequired() = %t, want %t", got, test.syncNeeded)
			}
		})
	}
}

type readinessPortalReplicator bool

func (r readinessPortalReplicator) Ready() bool { return bool(r) }

func (readinessPortalReplicator) Publish(context.Context, RecoveryManifest, *os.File) error {
	return nil
}

func (readinessPortalReplicator) Update(context.Context, RecoveryManifest) error { return nil }

func (readinessPortalReplicator) Remove(context.Context, string) error { return nil }

type flakyRecoveryPersistence struct {
	base       *portalRecoveryStore
	mu         sync.Mutex
	failures   int
	failedOnce chan struct{}
	once       sync.Once
}

type blockingRecoveryPersistence struct {
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (s *blockingRecoveryPersistence) put(RecoveryManifest) error {
	s.once.Do(func() { close(s.started) })
	<-s.release
	return nil
}

func (*blockingRecoveryPersistence) delete(string) error { return nil }

func (*blockingRecoveryPersistence) syncDirectory() error { return nil }

func (s *flakyRecoveryPersistence) put(manifest RecoveryManifest) error {
	return s.base.put(manifest)
}

func (s *flakyRecoveryPersistence) delete(key string) error {
	return s.base.delete(key)
}

func (s *flakyRecoveryPersistence) syncDirectory() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failures > 0 {
		s.failures--
		s.once.Do(func() { close(s.failedOnce) })
		return errors.New("injected directory sync failure")
	}
	return s.base.syncDirectory()
}

func recoveryWriterTestManifest(key string) RecoveryManifest {
	return RecoveryManifest{
		Version:           portalRecoveryVersion,
		Key:               key,
		PodUID:            "pod-a",
		Name:              "workspace",
		TargetPath:        "/target",
		RootFSBackingPath: "/rootfs",
		InitRequest:       []byte{1},
		VolumeID:          "vol-1",
	}
}

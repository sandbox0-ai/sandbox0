//go:build linux

package ha

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	ctldportal "github.com/sandbox0-ai/sandbox0/ctld/internal/ctld/portal"
)

func TestNewCoordinatorRequiresSlot(t *testing.T) {
	if _, err := NewCoordinator(Config{RootDir: t.TempDir()}); err == nil {
		t.Fatal("NewCoordinator() error = nil, want missing slot error")
	}
}

func TestCoordinatorTransfersPortalAndPromotesStandby(t *testing.T) {
	root := t.TempDir()
	primaryCoordinator := newTestCoordinator(t, root, "a")
	primary, err := primaryCoordinator.WaitForPrimary(context.Background())
	if err != nil {
		t.Fatalf("WaitForPrimary(primary) error = %v", err)
	}
	t.Cleanup(func() { _ = primary.Close() })
	capabilityDuringSnapshot := make(chan bool, 1)
	primary.Replicator.SetSnapshotProvider(func(_ context.Context, target ctldportal.PortalReplicator) error {
		provider, ok := target.(ctldportal.PortalRecoveryCapabilityProvider)
		capabilityDuringSnapshot <- ok && provider.SupportsRecoveryCapability(ctldportal.RecoveryCapabilityS0FSHandleJournal)
		return nil
	})

	standbyCoordinator := newTestCoordinator(t, root, "b")
	standbyCtx, standbyCancel := context.WithCancel(context.Background())
	defer standbyCancel()
	standbyResult := waitForPrimaryAsync(standbyCtx, standbyCoordinator)
	waitForStandbys(t, primary.Replicator, 1)
	if supported := <-capabilityDuringSnapshot; !supported {
		t.Fatal("standby capability was not negotiated before snapshot synchronization")
	}
	if !primary.Replicator.SupportsRecoveryCapability(ctldportal.RecoveryCapabilityS0FSHandleJournal) {
		t.Fatal("standby did not advertise S0FS handle journal recovery")
	}

	channel := writeTestChannel(t, "portal-channel")
	manifest := testManifest("pod-1\x00workspace")
	if err := primary.Replicator.Publish(context.Background(), manifest, channel); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
	manifest.VolumeID = "volume-1"
	if err := primary.Replicator.Update(context.Background(), manifest); err != nil {
		t.Fatalf("Update() error = %v", err)
	}
	_ = channel.Close()

	if err := primary.Close(); err != nil {
		t.Fatalf("Close(primary) error = %v", err)
	}
	promoted := receivePrimary(t, standbyResult)
	defer promoted.Close()
	if promoted.Epoch != primary.Epoch+1 {
		t.Fatalf("promoted epoch = %d, want %d", promoted.Epoch, primary.Epoch+1)
	}
	if len(promoted.Recovery) != 1 {
		t.Fatalf("promoted recovery count = %d, want 1", len(promoted.Recovery))
	}
	recovered := promoted.Recovery[0]
	if recovered.Manifest.VolumeID != "volume-1" {
		t.Fatalf("recovered volume = %q, want volume-1", recovered.Manifest.VolumeID)
	}
	payload, err := io.ReadAll(recovered.Channel)
	if err != nil {
		t.Fatalf("read recovered channel: %v", err)
	}
	if string(payload) != "portal-channel" {
		t.Fatalf("recovered channel = %q, want portal-channel", string(payload))
	}
}

func TestStandbyResynchronizesAfterTransportDisconnect(t *testing.T) {
	root := t.TempDir()
	primaryCoordinator := newTestCoordinator(t, root, "a")
	primary, err := primaryCoordinator.WaitForPrimary(context.Background())
	if err != nil {
		t.Fatalf("WaitForPrimary(primary) error = %v", err)
	}
	defer primary.Close()
	channel := writeTestChannel(t, "reconnected-channel")
	defer channel.Close()
	manifest := testManifest("pod-reconnect\x00workspace")
	primary.Replicator.SetSnapshotProvider(func(ctx context.Context, target ctldportal.PortalReplicator) error {
		return target.Publish(ctx, manifest, channel)
	})

	standbyCoordinator := newTestCoordinator(t, root, "b")
	standbyCtx, standbyCancel := context.WithCancel(context.Background())
	defer standbyCancel()
	standbyResult := waitForPrimaryAsync(standbyCtx, standbyCoordinator)
	waitForStandbys(t, primary.Replicator, 1)
	initial := onlyReadyClient(t, primary.Replicator)
	if err := initial.conn.Close(); err != nil {
		t.Fatalf("close initial standby transport: %v", err)
	}
	waitForReplacementStandby(t, primary.Replicator, initial)

	if err := primary.Close(); err != nil {
		t.Fatalf("Close(primary) error = %v", err)
	}
	promoted := receivePrimary(t, standbyResult)
	defer promoted.Close()
	if len(promoted.Recovery) != 1 {
		t.Fatalf("promoted recovery count = %d, want 1", len(promoted.Recovery))
	}
	if promoted.Recovery[0].Manifest.Key != manifest.Key {
		t.Fatalf("promoted manifest key = %q, want %q", promoted.Recovery[0].Manifest.Key, manifest.Key)
	}
}

func TestReplicatorRetainsReadyStandbyWhenAnotherExits(t *testing.T) {
	root := t.TempDir()
	primaryCoordinator := newTestCoordinator(t, root, "a")
	primary, err := primaryCoordinator.WaitForPrimary(context.Background())
	if err != nil {
		t.Fatalf("WaitForPrimary(primary) error = %v", err)
	}
	defer primary.Close()
	primary.Replicator.SetSnapshotProvider(func(context.Context, ctldportal.PortalReplicator) error { return nil })

	firstCoordinator := newTestCoordinator(t, root, "b")
	firstCtx, firstCancel := context.WithCancel(context.Background())
	defer firstCancel()
	firstResult := waitForPrimaryAsync(firstCtx, firstCoordinator)

	secondCoordinator := newTestCoordinator(t, root, "c")
	secondCtx, secondCancel := context.WithCancel(context.Background())
	secondResult := waitForPrimaryAsync(secondCtx, secondCoordinator)
	waitForStandbys(t, primary.Replicator, 2)

	secondCancel()
	select {
	case result := <-secondResult:
		if !errors.Is(result.err, context.Canceled) {
			t.Fatalf("second standby error = %v, want context canceled", result.err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("second standby did not exit")
	}
	waitForStandbys(t, primary.Replicator, 1)

	channel := writeTestChannel(t, "remaining-standby")
	if err := primary.Replicator.Publish(context.Background(), testManifest("pod-2\x00workspace"), channel); err != nil {
		t.Fatalf("Publish() with one remaining standby error = %v", err)
	}
	_ = channel.Close()

	if err := primary.Close(); err != nil {
		t.Fatalf("Close(primary) error = %v", err)
	}
	promoted := receivePrimary(t, firstResult)
	defer promoted.Close()
	if len(promoted.Recovery) != 1 {
		t.Fatalf("promoted recovery count = %d, want 1", len(promoted.Recovery))
	}
}

func TestReplicatorRequiresSynchronizedStandby(t *testing.T) {
	coordinator := newTestCoordinator(t, t.TempDir(), "a")
	primary, err := coordinator.WaitForPrimary(context.Background())
	if err != nil {
		t.Fatalf("WaitForPrimary() error = %v", err)
	}
	defer primary.Close()
	channel := writeTestChannel(t, "unavailable")
	defer channel.Close()
	if err := primary.Replicator.Publish(context.Background(), testManifest("pod-3\x00workspace"), channel); !errors.Is(err, ErrStandbyUnavailable) {
		t.Fatalf("Publish() error = %v, want %v", err, ErrStandbyUnavailable)
	}
}

func TestOnlyOneStandbyPromotesAndOtherRejoins(t *testing.T) {
	root := t.TempDir()
	primaryCoordinator := newTestCoordinator(t, root, "a")
	primary, err := primaryCoordinator.WaitForPrimary(context.Background())
	if err != nil {
		t.Fatalf("WaitForPrimary(primary) error = %v", err)
	}
	primary.Replicator.SetSnapshotProvider(func(context.Context, ctldportal.PortalReplicator) error { return nil })

	firstCoordinator := newTestCoordinator(t, root, "b")
	firstCtx, firstCancel := context.WithCancel(context.Background())
	defer firstCancel()
	firstResult := waitForPrimaryAsync(firstCtx, firstCoordinator)
	secondCoordinator := newTestCoordinator(t, root, "c")
	secondCtx, secondCancel := context.WithCancel(context.Background())
	defer secondCancel()
	secondResult := waitForPrimaryAsync(secondCtx, secondCoordinator)
	waitForStandbys(t, primary.Replicator, 2)
	if err := primary.Close(); err != nil {
		t.Fatalf("Close(primary) error = %v", err)
	}

	var promoted *PrimaryLease
	var remaining *Coordinator
	var remainingCancel context.CancelFunc
	var remainingResult <-chan primaryResult
	select {
	case result := <-firstResult:
		if result.err != nil {
			t.Fatalf("first promotion error = %v", result.err)
		}
		promoted = result.lease
		remaining = secondCoordinator
		remainingCancel = secondCancel
		remainingResult = secondResult
	case result := <-secondResult:
		if result.err != nil {
			t.Fatalf("second promotion error = %v", result.err)
		}
		promoted = result.lease
		remaining = firstCoordinator
		remainingCancel = firstCancel
		remainingResult = firstResult
	case <-time.After(5 * time.Second):
		t.Fatal("neither standby was promoted")
	}
	t.Cleanup(func() { _ = promoted.Close() })
	promoted.Replicator.SetSnapshotProvider(func(context.Context, ctldportal.PortalReplicator) error { return nil })
	waitForStandbys(t, promoted.Replicator, 1)
	state := remaining.State()
	if state.Role != RoleStandby || !state.Synchronized || state.Epoch != promoted.Epoch {
		t.Fatalf("remaining coordinator state = %#v, want synchronized standby at epoch %d", state, promoted.Epoch)
	}
	remainingCancel()
	select {
	case result := <-remainingResult:
		if !errors.Is(result.err, context.Canceled) {
			t.Fatalf("remaining standby error = %v, want context canceled", result.err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("remaining standby did not stop")
	}
	if err := promoted.Close(); err != nil {
		t.Fatalf("Close(promoted) error = %v", err)
	}
}

type primaryResult struct {
	lease *PrimaryLease
	err   error
}

func waitForPrimaryAsync(ctx context.Context, coordinator *Coordinator) <-chan primaryResult {
	result := make(chan primaryResult, 1)
	go func() {
		lease, err := coordinator.WaitForPrimary(ctx)
		result <- primaryResult{lease: lease, err: err}
	}()
	return result
}

func receivePrimary(t *testing.T, result <-chan primaryResult) *PrimaryLease {
	t.Helper()
	select {
	case got := <-result:
		if got.err != nil {
			t.Fatalf("standby promotion error = %v", got.err)
		}
		return got.lease
	case <-time.After(5 * time.Second):
		t.Fatal("standby was not promoted")
		return nil
	}
}

func waitForStandbys(t *testing.T, replicator *Replicator, want int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if replicator.StandbyCount() == want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("standby count = %d, want %d", replicator.StandbyCount(), want)
}

func TestReplicatorRecoveryCapabilityRequiresEveryConnectedStandby(t *testing.T) {
	replicator := &Replicator{clients: make(map[*standbyClient]struct{})}
	if !replicator.SupportsRecoveryCapability(ctldportal.RecoveryCapabilityS0FSHandleJournal) {
		t.Fatal("SupportsRecoveryCapability() = false without a standby, want true")
	}

	current := &standbyClient{capabilities: map[string]struct{}{
		ctldportal.RecoveryCapabilityS0FSHandleJournal: {},
	}}
	replicator.clients[current] = struct{}{}
	if !replicator.SupportsRecoveryCapability(ctldportal.RecoveryCapabilityS0FSHandleJournal) {
		t.Fatal("SupportsRecoveryCapability() = false for current standby, want true")
	}

	legacy := &standbyClient{}
	replicator.clients[legacy] = struct{}{}
	if replicator.SupportsRecoveryCapability(ctldportal.RecoveryCapabilityS0FSHandleJournal) {
		t.Fatal("SupportsRecoveryCapability() = true with legacy standby, want false")
	}
}

func onlyReadyClient(t *testing.T, replicator *Replicator) *standbyClient {
	t.Helper()
	replicator.mu.RLock()
	defer replicator.mu.RUnlock()
	var found *standbyClient
	for client := range replicator.clients {
		if !client.ready {
			continue
		}
		if found != nil {
			t.Fatal("found more than one ready standby")
		}
		found = client
	}
	if found == nil {
		t.Fatal("ready standby not found")
	}
	return found
}

func waitForReplacementStandby(t *testing.T, replicator *Replicator, previous *standbyClient) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		replicator.mu.RLock()
		_, previousPresent := replicator.clients[previous]
		replacementReady := false
		for client := range replicator.clients {
			if client != previous && client.ready {
				replacementReady = true
				break
			}
		}
		replicator.mu.RUnlock()
		if !previousPresent && replacementReady {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("standby did not resynchronize after transport disconnect")
}

func newTestCoordinator(t *testing.T, root, slot string) *Coordinator {
	t.Helper()
	coordinator, err := NewCoordinator(Config{RootDir: root, Slot: slot})
	if err != nil {
		t.Fatalf("NewCoordinator() error = %v", err)
	}
	return coordinator
}

func writeTestChannel(t *testing.T, payload string) *os.File {
	t.Helper()
	channel, err := os.OpenFile(filepath.Join(t.TempDir(), "channel"), os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		t.Fatalf("open test channel: %v", err)
	}
	if _, err := channel.WriteString(payload); err != nil {
		t.Fatalf("write test channel: %v", err)
	}
	if _, err := channel.Seek(0, io.SeekStart); err != nil {
		t.Fatalf("seek test channel: %v", err)
	}
	return channel
}

func testManifest(key string) ctldportal.RecoveryManifest {
	return ctldportal.RecoveryManifest{
		Version:           1,
		Key:               key,
		PodUID:            "pod-1",
		Name:              "workspace",
		TargetPath:        "/var/lib/kubelet/pods/pod-1/workspace",
		RootFSBackingPath: "/var/lib/sandbox0/ctld/rootfs/pod-1/workspace",
		RootFSStatePath:   "/var/lib/sandbox0/ctld/ha/rootfs/pod-1.jsonl",
		InitRequest:       []byte("init"),
	}
}

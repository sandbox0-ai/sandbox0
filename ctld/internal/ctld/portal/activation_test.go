package portal

import (
	"context"
	"testing"
	"time"
)

func TestVolumeActivationSerializesSameVolume(t *testing.T) {
	mgr := &Manager{activations: make(map[string]*volumeActivation)}
	releaseFirst, err := mgr.acquireVolumeActivation(context.Background(), "vol-1")
	if err != nil {
		t.Fatal(err)
	}

	acquired := make(chan func(), 1)
	go func() {
		release, acquireErr := mgr.acquireVolumeActivation(context.Background(), "vol-1")
		if acquireErr == nil {
			acquired <- release
		}
	}()
	select {
	case <-acquired:
		t.Fatal("second activation acquired before the first released")
	case <-time.After(20 * time.Millisecond):
	}

	releaseFirst()
	select {
	case releaseSecond := <-acquired:
		releaseSecond()
	case <-time.After(time.Second):
		t.Fatal("second activation did not acquire after release")
	}
	if len(mgr.activations) != 0 {
		t.Fatalf("activation entries = %d, want 0", len(mgr.activations))
	}
}

func TestVolumeActivationWaitHonorsContext(t *testing.T) {
	mgr := &Manager{activations: make(map[string]*volumeActivation)}
	release, err := mgr.acquireVolumeActivation(context.Background(), "vol-1")
	if err != nil {
		t.Fatal(err)
	}
	defer release()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if _, err := mgr.acquireVolumeActivation(ctx, "vol-1"); err == nil {
		t.Fatal("blocked activation ignored context deadline")
	}
	if got := mgr.activations["vol-1"].users; got != 1 {
		t.Fatalf("activation users = %d, want 1", got)
	}
}

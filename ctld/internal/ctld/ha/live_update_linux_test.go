//go:build linux

package ha

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestLivePrimaryLockRemainsHeldAcrossRelinquishAndAdoption(t *testing.T) {
	root := t.TempDir()
	source := newTestCoordinator(t, root, "a")
	lease, err := source.WaitForPrimary(t.Context())
	require.NoError(t, err)
	inherited, err := lease.ExportFile()
	require.NoError(t, err)
	defer inherited.Close()
	standby := newTestCoordinator(t, root, "b")
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	promoted := make(chan primaryResult, 1)
	go func() { next, err := standby.WaitForPrimary(ctx); promoted <- primaryResult{lease: next, err: err} }()
	waitForRole(t, standby, RoleStandby)
	require.NoError(t, lease.Relinquish())
	require.NoError(t, lease.Close(), "late normal cleanup must not issue LOCK_UN")
	require.Equal(t, RoleDraining, source.State().Role)
	select {
	case result := <-promoted:
		t.Fatalf("standby entered the transfer gap: %#v", result)
	case <-time.After(100 * time.Millisecond):
	}
	candidate := newTestCoordinator(t, root, "release-next")
	next, err := candidate.AdoptTransferred(inherited, lease.Epoch)
	require.NoError(t, err)
	require.Equal(t, lease.Epoch+1, next.Epoch)
	select {
	case result := <-promoted:
		t.Fatalf("standby overlapped successor: %#v", result)
	case <-time.After(100 * time.Millisecond):
	}
	require.NoError(t, next.Close())
	select {
	case result := <-promoted:
		require.NoError(t, result.err)
		require.Equal(t, next.Epoch+1, result.lease.Epoch)
		require.NoError(t, result.lease.Close())
	case <-time.After(3 * time.Second):
		t.Fatal("standby did not promote after successor shutdown")
	}
}

func TestLivePrimaryRejectsForeignLockAndStaleEpoch(t *testing.T) {
	root := t.TempDir()
	coordinator := newTestCoordinator(t, root, "source")
	lease, err := coordinator.WaitForPrimary(t.Context())
	require.NoError(t, err)
	defer lease.Close()
	inherited, err := lease.ExportFile()
	require.NoError(t, err)
	defer inherited.Close()
	other, err := os.Create(filepath.Join(t.TempDir(), "foreign"))
	require.NoError(t, err)
	defer other.Close()
	candidate := newTestCoordinator(t, root, "candidate")
	_, err = candidate.AdoptTransferred(other, lease.Epoch)
	require.Error(t, err)
	_, err = candidate.AdoptTransferred(inherited, lease.Epoch+1)
	require.Error(t, err)
	require.Equal(t, RoleStarting, candidate.State().Role)
}

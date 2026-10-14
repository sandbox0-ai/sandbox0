package session

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/containerd/errdefs"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsblock"
	"github.com/stretchr/testify/require"
)

// A stopped Sentry can still leave buffered filesystem writes to be drained
// by FIFREEZE. The amount is not bounded by the retirement WAL reserve.
func TestMigrationFreezeDrainsBeyondRetirementReserve(t *testing.T) {
	m, host, authority, stage, branch := newLiveGrowthFixture(t, "migration-flush-reserve")
	authority.lostPublish, authority.lostInstall = true, true
	stage = stage.WithoutWriterGrantToken()
	_, err := branch.WriteAt(bytes.Repeat([]byte{23}, rootfsblock.LogicalBlockSize), 0)
	require.NoError(t, err)
	m.runtime = &liveFreezeHost{fakeHostRuntime: host.fakeHostRuntime, freeze: func() error {
		for _, offset := range []int64{rootfsblock.LogicalBlockSize, 2 * rootfsblock.LogicalBlockSize} {
			if _, err := branch.WriteAt([]byte("buffered metadata"), offset); err != nil {
				return err
			}
		}
		return nil
	}}
	request := migrationCutRequest(t, stage)
	cut, err := m.CaptureMigrationRootFS(t.Context(), stage, request)
	require.NoError(t, err)
	require.NoError(t, cut.ValidateFor(stage, request))
	descriptor, err := rootfsblock.DecodeDescriptor(cut.Generation.Descriptor)
	require.NoError(t, err)
	reader, err := rootfsblock.NewReader(m.source, descriptor, rootfsblock.DefaultReadCacheBytes)
	require.NoError(t, err)
	data := make([]byte, rootfsblock.LogicalBlockSize)
	_, err = reader.ReadAt(data, 0)
	require.NoError(t, err)
	require.Equal(t, bytes.Repeat([]byte{23}, len(data)), data)
	for _, offset := range []int64{rootfsblock.LogicalBlockSize, 2 * rootfsblock.LogicalBlockSize} {
		_, err = reader.ReadAt(data[:17], offset)
		require.NoError(t, err)
		require.Equal(t, "buffered metadata", string(data[:17]))
	}
	stored, err := m.load(stage.Parent)
	require.NoError(t, err)
	require.Positive(t, stored.BranchSequenceBase)
	require.Nil(t, stored.LiveCommit.Pending)
	require.Equal(t, stage.InitialGeneration, stored.GenerationID)
	require.Equal(t, cut.Sequence, *stored.Migration.Sequence)
	require.Empty(t, m.freezes)
}

func TestMigrationFreezeCancellationRetainsUnconfirmedCustody(t *testing.T) {
	m, host, authority, stage, branch := newLiveGrowthFixture(t, "migration-flush-cancel")
	stage = stage.WithoutWriterGrantToken()
	_, err := branch.WriteAt([]byte("original"), 0)
	require.NoError(t, err)
	authority.failObserve = true
	m.runtime = &liveFreezeHost{fakeHostRuntime: host.fakeHostRuntime, freeze: func() error {
		_, err := branch.WriteAt([]byte("buffered metadata"), rootfsblock.LogicalBlockSize)
		return err
	}}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := m.CaptureMigrationRootFS(ctx, stage, migrationCutRequest(t, stage)); done <- err }()
	require.Eventually(t, func() bool { return branch.DirtyTailPressure() != nil }, time.Second, time.Millisecond)
	cancel()
	authority.mu.Lock()
	authority.failObserve = false
	authority.mu.Unlock()
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(5 * time.Second):
		t.Fatal("cancelled migration stranded its kernel drain")
	}
	stored, err := m.load(stage.Parent)
	require.NoError(t, err)
	require.Nil(t, stored.Migration.Sequence)
	require.Nil(t, stored.Migration.Result)
	require.NoError(t, validateMigrationCutRecord(stored))
	require.ErrorIs(t, m.CommitLive(t.Context(), stage), errdefs.ErrUnavailable)
	require.NoError(t, m.ReconcileFreezes(t.Context()))
	require.Zero(t, host.count("thaw-xfs"))
}

func TestMigrationFreezeOwnerCloseWakesBlockedDrain(t *testing.T) {
	m, host, authority, stage, branch := newLiveGrowthFixture(t, "migration-flush-owner-close")
	stage = stage.WithoutWriterGrantToken()
	_, err := branch.WriteAt([]byte("original"), 0)
	require.NoError(t, err)
	authority.failObserve = true
	m.runtime = &liveFreezeHost{fakeHostRuntime: host.fakeHostRuntime, freeze: func() error {
		_, err := branch.WriteAt([]byte("buffered metadata"), rootfsblock.LogicalBlockSize)
		return err
	}}
	done := make(chan error, 1)
	go func() {
		_, err := m.CaptureMigrationRootFS(t.Context(), stage, migrationCutRequest(t, stage))
		done <- err
	}()
	require.Eventually(t, func() bool { return branch.DirtyTailPressure() != nil }, time.Second, time.Millisecond)
	closed := make(chan error, 1)
	go func() { closed <- m.Close() }()
	select {
	case err := <-closed:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("owner shutdown joined a blocked migration drain")
	}
	require.Error(t, <-done)
	require.Zero(t, host.count("thaw-xfs"))
}

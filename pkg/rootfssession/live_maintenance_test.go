package session

import (
	"testing"
	"time"

	"github.com/containerd/errdefs"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsblock"
	"github.com/stretchr/testify/require"
)

func TestLiveCommitSchedulingUsesInstalledTimeAndIncludesUnconfirmedEmptySuffix(t *testing.T) {
	m, a, stage, branch := newNodeLiveFixture(t, "live-scheduling")
	stored, err := m.load(stage.Parent)
	require.NoError(t, err)
	created, err := time.Parse(time.RFC3339Nano, stored.CreatedAt)
	require.NoError(t, err)
	_, err = branch.WriteAt([]byte("cut"), 0)
	require.NoError(t, err)
	sessions, err := m.LiveCommitSessions(created.Add(LiveCommitInterval - time.Second))
	require.NoError(t, err)
	require.Empty(t, sessions)
	sessions, err = m.LiveCommitSessions(created.Add(LiveCommitInterval))
	require.NoError(t, err)
	require.Len(t, sessions, 1)
	require.Empty(t, sessions[0].Stage.Identity.WriterGrantToken)
	a.lostInstall = true
	require.ErrorIs(t, m.CommitLive(t.Context(), stage), errdefs.ErrUnavailable)
	sessions, err = m.LiveCommitSessions(created)
	require.NoError(t, err)
	require.Len(t, sessions, 1)
	require.True(t, sessions[0].Pending)
	require.Zero(t, sessions[0].DirtyBytes)
	_, err = branch.WriteAt([]byte("suffix"), rootfsblock.LogicalBlockSize)
	require.NoError(t, err)
	require.NoError(t, m.SettleLiveCommit(t.Context(), stage))
	require.Len(t, a.published, 1, "settling must not capture the newer suffix")
	stored, err = m.load(stage.Parent)
	require.NoError(t, err)
	installed, err := time.Parse(time.RFC3339Nano, stored.LiveCommit.LastInstalledAt)
	require.NoError(t, err)
	stored.UpdatedAt = installed.Add(10 * time.Second).Format(time.RFC3339Nano)
	require.NoError(t, m.save(stored))
	sessions, err = m.LiveCommitSessions(installed.Add(LiveCommitInterval - time.Second))
	require.NoError(t, err)
	require.Empty(t, sessions)
	sessions, err = m.LiveCommitSessions(installed.Add(LiveCommitInterval))
	require.NoError(t, err)
	require.Len(t, sessions, 1)
	require.False(t, sessions[0].Pending)
	require.Equal(t, int64(rootfsblock.LogicalBlockSize), sessions[0].DirtyBytes)
}

func TestLiveCommitSettlementDoesNotStartCutOrAcceptDifferentWriter(t *testing.T) {
	m, a, stage, branch := newNodeLiveFixture(t, "live-no-new-cut")
	_, err := branch.WriteAt([]byte("unpublished"), 0)
	require.NoError(t, err)
	require.NoError(t, m.SettleLiveCommit(t.Context(), stage))
	require.Empty(t, a.published)
	stored, err := m.load(stage.Parent)
	require.NoError(t, err)
	require.Nil(t, stored.LiveCommit)
	other := stage
	other.Identity.WriterGrantID = "foreign-grant"
	require.ErrorIs(t, m.SettleLiveCommit(t.Context(), other), errdefs.ErrFailedPrecondition)
	m.mu.Lock()
	m.captures[stage.Parent] = true
	m.mu.Unlock()
	sessions, err := m.LiveCommitSessions(time.Now().Add(time.Hour))
	require.NoError(t, err)
	require.Empty(t, sessions)
	m.mu.Lock()
	delete(m.captures, stage.Parent)
	m.mu.Unlock()
}

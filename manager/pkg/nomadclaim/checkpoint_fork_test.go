package nomadclaim

import (
	"context"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/service"
	"github.com/sandbox0-ai/sandbox0/pkg/runtimecontrol"
	"github.com/stretchr/testify/require"
)

type memoryForkServiceStore struct {
	*fakeClaimStore
	intent   *sandboxstore.NomadRunningMemoryFork
	requests []*sandboxstore.NomadSandboxForkRequest
}

func (s *memoryForkServiceStore) GetNomadSandboxRunningMemoryFork(context.Context, string) (*sandboxstore.NomadRunningMemoryFork, error) {
	return s.intent, nil
}
func (s *memoryForkServiceStore) RequestNomadSandboxRunningMemoryFork(_ context.Context, r *sandboxstore.NomadSandboxForkRequest) (*sandboxstore.NomadRunningMemoryFork, error) {
	copy := *r
	copy.Target = cloneClaimRecord(r.Target)
	s.requests = append(s.requests, &copy)
	if s.intent == nil {
		s.intent = &sandboxstore.NomadRunningMemoryFork{OperationID: r.OperationID, SourceSandboxID: r.SourceSandboxID, CaptureOperationID: "capture-fork", Target: cloneClaimRecord(r.Target)}
	}
	return s.intent, nil
}

func TestMemoryPausedForkExplicitModeAndRetryAfterParentResume(t *testing.T) {
	f, id, _ := memoryServiceFixture(t, runtimecontrol.CheckpointResume)
	store := &memoryForkServiceStore{fakeClaimStore: f.store}
	f.service.store = store
	source := f.store.records[id]
	request := &service.ForkSandboxRequest{OperationID: "paused-memory-fork"}
	response, err := f.service.ForkMemorySandbox(t.Context(), id, source.TeamID, source.UserID, request)
	require.NoError(t, err)
	require.NotNil(t, response)
	require.True(t, store.pausedForkRequests[0].Memory)
	require.Empty(t, store.requests)
	source.DesiredState = sandboxstore.SandboxDesiredStateActive
	source.RuntimeID = "new-parent-allocation"
	source.RuntimeNamespace = "default"
	_, err = f.service.ForkMemorySandbox(t.Context(), id, source.TeamID, source.UserID, request)
	require.NoError(t, err)
	require.Len(t, store.pausedForkRequests, 2)
	require.Empty(t, store.requests, "retry of paused fork must not capture its now-running parent")
}

func TestMemoryRunningForkPreservesIntentAndWaitsForParentCommit(t *testing.T) {
	f, id, p := memoryServiceFixture(t, runtimecontrol.CheckpointResume)
	store := &memoryForkServiceStore{fakeClaimStore: f.store}
	f.service.store = store
	source := f.store.records[id]
	source.DesiredState = sandboxstore.SandboxDesiredStateActive
	source.RuntimeID = "source-allocation"
	source.RuntimeNamespace = "default"
	request := &service.ForkSandboxRequest{OperationID: "running-memory-fork", StartedAt: f.now}
	_, err := f.service.ForkMemorySandbox(t.Context(), id, source.TeamID, source.UserID, request)
	require.ErrorIs(t, err, service.ErrSandboxLifecycleUnavailable)
	require.True(t, store.requests[0].Memory)
	original := cloneClaimRecord(store.intent.Target)
	f.service.now = func() time.Time { return f.now.Add(time.Hour) }
	request.StartedAt = f.now.Add(time.Second) // Each signed ingress retry has a fresh timestamp.
	source.DesiredState = sandboxstore.SandboxDesiredStatePaused
	source.RuntimeID = ""
	source.RuntimeNamespace = ""
	source.ExpiresAt = time.Time{}
	_, err = f.service.ForkMemorySandbox(t.Context(), id, source.TeamID, source.UserID, request)
	require.ErrorIs(t, err, service.ErrSandboxLifecycleUnavailable)
	require.Equal(t, original, store.requests[1].Target, "retry must not reconstruct target timestamps from the paused source")
	require.Empty(t, store.pausedForkRequests, "pending capture must not fall through to a filesystem fork")
	store.intent.ParentResumeOperationID = store.resumeCandidate.OperationID
	store.resumeRequested = true
	store.pausedForkCompleted = map[string]*sandboxstore.SandboxRecord{request.OperationID: original}
	store.records[original.ID] = cloneClaimRecord(original)
	_, err = f.service.ForkMemorySandbox(t.Context(), id, source.TeamID, source.UserID, request)
	require.ErrorIs(t, err, service.ErrSandboxLifecycleUnavailable, "local restore is insufficient without regional lifecycle commit")
	require.Len(t, p.authorities, 1)
	store.lifecyclesByID = map[string]*sandboxstore.SandboxLifecycleTxn{store.intent.ParentResumeOperationID: {ID: store.intent.ParentResumeOperationID, SandboxID: id, Kind: sandboxstore.SandboxLifecycleKindResume, Phase: sandboxstore.SandboxLifecyclePhaseCommitted}}
	response, err := f.service.ForkMemorySandbox(t.Context(), id, source.TeamID, source.UserID, request)
	require.NoError(t, err)
	require.NotNil(t, response)
	require.Len(t, p.authorities, 1, "completed parent cannot be restored again")
	require.Empty(t, store.resumeRequests, "composed operation already admitted its parent restore")
	require.Zero(t, p.coldCalls)
	require.True(t, store.pausedForkRequests[0].Memory)
	ttl := int32(12)
	request.Config = &service.ForkSandboxConfig{TTL: &ttl}
	_, err = f.service.ForkMemorySandbox(t.Context(), id, source.TeamID, source.UserID, request)
	require.Error(t, err, "retry cannot replace accepted TTL")
}

func TestMemoryRunningForkReportsCanceledCaptureWithoutRestart(t *testing.T) {
	f, id, planner := memoryServiceFixture(t, runtimecontrol.CheckpointResume)
	store := &memoryForkServiceStore{fakeClaimStore: f.store}
	f.service.store = store
	source := f.store.records[id]
	source.DesiredState = sandboxstore.SandboxDesiredStateActive
	source.RuntimeID = "source-allocation"
	source.RuntimeNamespace = "default"
	request := &service.ForkSandboxRequest{OperationID: "canceled-running-fork", Memory: true}
	_, err := f.service.ForkSandbox(t.Context(), id, source.TeamID, source.UserID, request)
	require.ErrorIs(t, err, service.ErrSandboxLifecycleUnavailable)
	store.lifecyclesByID = map[string]*sandboxstore.SandboxLifecycleTxn{store.intent.CaptureOperationID: {ID: store.intent.CaptureOperationID, SandboxID: id, Kind: sandboxstore.SandboxLifecycleKindPause, Phase: sandboxstore.SandboxLifecyclePhaseAborted}}
	_, err = f.service.ForkSandbox(t.Context(), id, source.TeamID, source.UserID, request)
	require.ErrorContains(t, err, "memory fork capture was canceled")
	require.Zero(t, planner.coldCalls)
	require.Empty(t, planner.authorities)
	require.Empty(t, store.pausedForkRequests)
}

func TestMemoryRunningForkReportsFailedParentRestoreWithoutRestart(t *testing.T) {
	f, id, planner := memoryServiceFixture(t, runtimecontrol.CheckpointResume)
	store := &memoryForkServiceStore{fakeClaimStore: f.store}
	f.service.store = store
	source := f.store.records[id]
	source.DesiredState = sandboxstore.SandboxDesiredStateActive
	source.RuntimeID = "source-allocation"
	source.RuntimeNamespace = "default"
	request := &service.ForkSandboxRequest{OperationID: "failed-parent-running-fork", Memory: true}
	_, err := f.service.ForkSandbox(t.Context(), id, source.TeamID, source.UserID, request)
	require.ErrorIs(t, err, service.ErrSandboxLifecycleUnavailable)
	store.intent.ParentResumeOperationID = store.resumeCandidate.OperationID
	store.lifecyclesByID = map[string]*sandboxstore.SandboxLifecycleTxn{store.intent.ParentResumeOperationID: {
		ID: store.intent.ParentResumeOperationID, SandboxID: id,
		Kind: sandboxstore.SandboxLifecycleKindResume, Phase: sandboxstore.SandboxLifecyclePhaseAborted,
	}}
	source.DesiredState = sandboxstore.SandboxDesiredStatePaused
	source.RuntimeID = ""
	source.RuntimeNamespace = ""
	for range 2 {
		_, err = f.service.ForkSandbox(t.Context(), id, source.TeamID, source.UserID, request)
		require.ErrorContains(t, err, "memory fork parent restore failed")
	}
	require.Zero(t, planner.coldCalls)
	require.Empty(t, planner.authorities)
	require.Empty(t, store.resumeRequests)
	require.Empty(t, store.pausedForkRequests)
}

package nomadclaim

import (
	"context"
	"errors"
	"maps"
	"strings"
	"testing"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/runtimeslotclaim"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/service"
	"github.com/sandbox0-ai/sandbox0/pkg/apierror"
	"github.com/sandbox0-ai/sandbox0/pkg/managerapi"
	"github.com/sandbox0-ai/sandbox0/pkg/runtimecontrol"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
	"github.com/stretchr/testify/require"
)

type memoryServicePlanner struct {
	*fakePlanner
	authorities  []protocol.CheckpointRestoreAuthority
	coldCalls    int
	missingProof bool
}

func (p *memoryServicePlanner) Claim(ctx context.Context, r runtimeslotclaim.Request) (*runtimeslotclaim.Result, error) {
	p.coldCalls++
	return p.fakePlanner.Claim(ctx, r)
}
func (p *memoryServicePlanner) ClaimCheckpoint(ctx context.Context, r runtimeslotclaim.Request, a protocol.CheckpointRestoreAuthority) (*runtimeslotclaim.Result, error) {
	p.authorities = append(p.authorities, a)
	result, err := p.fakePlanner.Claim(ctx, r)
	if err != nil || p.missingProof {
		return result, err
	}
	result.ProcdInstanceID = "preserved-procd"
	result.CommandProof = protocol.CommandReadyProof{Version: protocol.CommandReadyProofVersion, SlotID: result.Slot.ID,
		OperationID: r.OperationID, ClaimID: "memory-claim", LaunchAttempt: "memory-launch", RunscContainerID: protocol.NomadRunscContainerID(result.Slot.ID),
		ProcdInstanceID: result.ProcdInstanceID, ProcdAddress: result.ProcdAddress, RequestMethod: "PUT", RequestPath: protocol.ProcdCommandReadyProbePath, ResponseStatus: 200, ResponseBodyDigest: strings.Repeat("a", 64)}
	return result, nil
}
func memoryServiceFixture(t *testing.T, kind runtimecontrol.CheckpointRestoreKind) (claimServiceFixture, string, *memoryServicePlanner) {
	t.Helper()
	f := newClaimServiceFixture(t)
	id := preparePausedNomadResume(t, f)
	candidate := f.store.resumeCandidate
	if kind == runtimecontrol.CheckpointFork {
		f.store.records[id].RuntimeGeneration = 0
		candidate.Record.RuntimeGeneration = 0
		candidate.RuntimeGeneration = 1
	}
	plan, err := f.service.prepareNomadResumePlan(t.Context(), f.store.records[id])
	require.NoError(t, err)
	target := plan.assignment
	target.RuntimeGeneration = candidate.RuntimeGeneration
	// A captured runtime may itself have been activated from a filesystem fork.
	target.ResetCopiedSessionState = true
	source := target
	source.EnvVars = maps.Clone(target.EnvVars)
	source.RuntimeGeneration--
	if kind == runtimecontrol.CheckpointFork {
		source.SandboxID = "memory-parent"
		source.RuntimeGeneration = 8
		source.EnvVars[runtimecontrol.EnvSandboxID] = source.SandboxID
	}
	capture, err := runtimecontrol.NewCheckpointCaptureAssignment("memory-capture", source)
	require.NoError(t, err)
	candidate.Checkpoint = &protocol.CheckpointRestoreAuthority{Assignment: runtimecontrol.CheckpointRestoreAssignment{
		OperationID: candidate.OperationID, Kind: kind, Capture: capture, Target: target}, LifecycleEpoch: 9}
	require.NoError(t, candidate.Checkpoint.Assignment.Validate())
	candidate.CheckpointCompatibilityDigest = f.runtimeClass.CompatibilityDigest
	planner := &memoryServicePlanner{fakePlanner: f.planner}
	f.service.planner = planner
	return f, id, planner
}
func TestMemoryResumeServiceUsesCapturedAssignmentAndOrdinaryRegionalCommit(t *testing.T) {
	for _, kind := range []runtimecontrol.CheckpointRestoreKind{runtimecontrol.CheckpointResume, runtimecontrol.CheckpointFork} {
		t.Run(string(kind), func(t *testing.T) {
			f, id, p := memoryServiceFixture(t, kind)
			response, err := f.service.ResumeMemorySandboxAndWait(t.Context(), id)
			require.NoError(t, err)
			require.True(t, response.Resumed)
			require.Equal(t, id, response.SandboxID)
			require.Zero(t, p.coldCalls)
			require.Len(t, p.authorities, 1)
			require.Equal(t, *f.store.resumeCandidate.Checkpoint, p.authorities[0])
			require.Equal(t, p.authorities[0].Assignment.Target, p.requests[0].Runtime)
			require.True(t, p.requests[0].Runtime.ResetCopiedSessionState, "memory restore keeps the captured flag without running activation")
			require.True(t, f.store.resumeRequests[0].Memory)
			require.True(t, f.store.resumeRetryRequests[0].Memory)
			require.Len(t, f.store.resumeCompleteCalls, 1)
			require.Empty(t, f.store.resumeAbortCalls)
			_, err = f.service.ResumeMemorySandboxAndWait(t.Context(), id)
			require.NoError(t, err)
			require.Len(t, p.authorities, 1, "already-active retry cannot restore twice")
		})
	}
}

func TestAutomaticResumeSelectsCommittedMemoryOrFilesystemMode(t *testing.T) {
	t.Run("retained-memory", func(t *testing.T) {
		f, id, planner := memoryServiceFixture(t, runtimecontrol.CheckpointResume)
		response, err := f.service.ResumeSandboxAutomaticallyAndWait(t.Context(), id)
		require.NoError(t, err)
		require.True(t, response.Resumed)
		require.Zero(t, planner.coldCalls)
		require.Len(t, planner.authorities, 1)
		require.Len(t, f.store.resumeRequests, 1)
		require.True(t, f.store.resumeRequests[0].Memory)
	})
	t.Run("filesystem-only", func(t *testing.T) {
		f, id, planner := memoryServiceFixture(t, runtimecontrol.CheckpointResume)
		f.store.memoryResumeErr = sandboxstore.ErrNomadCheckpointNotRetained
		f.store.resumeCandidate.Checkpoint = nil
		response, err := f.service.ResumeSandboxAutomaticallyAndWait(t.Context(), id)
		require.NoError(t, err)
		require.True(t, response.Resumed)
		require.Equal(t, 1, planner.coldCalls)
		require.Empty(t, planner.authorities)
		require.Len(t, f.store.resumeRequests, 2)
		require.True(t, f.store.resumeRequests[0].Memory)
		require.False(t, f.store.resumeRequests[1].Memory)
	})
	t.Run("invalid-memory-never-starts-cold", func(t *testing.T) {
		f, id, planner := memoryServiceFixture(t, runtimecontrol.CheckpointResume)
		f.store.resumeCandidate.Checkpoint.Assignment.Target.EnvVars["MAIN"] = "changed"
		_, err := f.service.ResumeSandboxAutomaticallyAndWait(t.Context(), id)
		require.Error(t, err)
		require.Zero(t, planner.coldCalls)
		require.Len(t, f.store.resumeRequests, 1)
		require.True(t, f.store.resumeRequests[0].Memory)
	})
}
func TestMemoryResumeServicePreservesUncertainExecutionForExactRetry(t *testing.T) {
	for _, failure := range []string{"execution", "commit", "readiness"} {
		t.Run(failure, func(t *testing.T) {
			f, id, p := memoryServiceFixture(t, runtimecontrol.CheckpointResume)
			switch failure {
			case "execution":
				p.err = errors.New("lost restore reply")
			case "commit":
				f.store.resumeCompleteErr = errors.New("lost regional reply")
			case "readiness":
				p.missingProof = true
			}
			_, err := f.service.ResumeMemorySandboxAndWait(t.Context(), id)
			require.Error(t, err)
			require.Empty(t, f.store.resumeAbortCalls)
			require.True(t, f.store.resumeRequested)
			first := p.authorities[0]
			p.err = nil
			p.missingProof = false
			f.store.resumeCompleteErr = nil
			_, err = f.service.ResumeMemorySandboxAndWait(t.Context(), id)
			require.NoError(t, err)
			require.Len(t, f.store.resumeRequests, 1, "retry must not reserve another quota slot")
			require.Equal(t, first, p.authorities[1])
			require.Zero(t, p.coldCalls)
		})
	}
}
func TestMemoryResumeServiceRejectsMissingImageOrDriftBeforeExecution(t *testing.T) {
	for _, failure := range []string{"missing", "compatibility", "config", "authority", "unsupported", "filesystem-mode"} {
		t.Run(failure, func(t *testing.T) {
			f, id, p := memoryServiceFixture(t, runtimecontrol.CheckpointResume)
			switch failure {
			case "missing":
				f.store.resumeCandidate.Checkpoint = nil
			case "compatibility":
				f.store.resumeCandidate.CheckpointCompatibilityDigest = "another-runtime"
			case "config":
				f.store.resumeCandidate.Checkpoint.Assignment.Target.EnvVars["MAIN"] = "changed"
			case "authority":
				f.store.resumeCandidate.Checkpoint.Assignment.OperationID = "another-operation"
			case "unsupported":
				f.service.planner = f.planner
			}
			var err error
			if failure == "filesystem-mode" {
				_, err = f.service.ResumeSandboxAndWait(t.Context(), id)
			} else {
				_, err = f.service.ResumeMemorySandboxAndWait(t.Context(), id)
			}
			require.Error(t, err)
			require.Empty(t, p.authorities)
			require.Zero(t, p.coldCalls)
			require.Empty(t, f.store.resumeCompleteCalls)
			if failure == "unsupported" {
				require.ErrorIs(t, err, service.ErrSandboxLifecycleUnavailable)
				require.Empty(t, f.store.resumeRequests)
			}
		})
	}
}

type memoryPauseServiceStore struct {
	*fakeClaimStore
	candidate *sandboxstore.NomadSandboxMemoryPauseCandidate
	err       error
	requests  []string
}

func (s *memoryPauseServiceStore) RequestNomadSandboxMemoryPause(_ context.Context, id string) (*sandboxstore.NomadSandboxMemoryPauseCandidate, error) {
	s.requests = append(s.requests, id)
	return s.candidate, s.err
}

func TestMemoryPauseServiceNeverStopsSourceBeforeCapture(t *testing.T) {
	for _, paused := range []bool{false, true} {
		f := newClaimServiceFixture(t)
		store := &memoryPauseServiceStore{fakeClaimStore: f.store, candidate: &sandboxstore.NomadSandboxMemoryPauseCandidate{
			SandboxID: "memory-owner", OperationID: "memory-pause", AlreadyPaused: paused}}
		f.service.store = store
		response, err := f.service.PauseMemorySandboxAndWait(t.Context(), " memory-owner ")
		require.NoError(t, err)
		require.Equal(t, paused, response.Paused)
		require.Equal(t, []string{"memory-owner"}, store.requests)
		require.Empty(t, f.allocation.requests)
		require.Empty(t, f.plannedRetire.requests)
		require.Empty(t, *f.pauseOrder)
		if paused {
			require.Equal(t, managerapi.SandboxStatusPaused, response.Status)
		} else {
			require.Equal(t, managerapi.SandboxStatusStarting, response.Status)
		}
	}
}

func TestMemoryPauseServiceRejectsUnsupportedOrChangedAuthority(t *testing.T) {
	f := newClaimServiceFixture(t)
	_, err := f.service.PauseMemorySandboxAndWait(t.Context(), "owner")
	require.ErrorIs(t, err, service.ErrSandboxLifecycleUnavailable)
	store := &memoryPauseServiceStore{fakeClaimStore: f.store}
	f.service.store = store
	for _, candidate := range []*sandboxstore.NomadSandboxMemoryPauseCandidate{
		nil, {SandboxID: "other", OperationID: "op"}, {SandboxID: "owner"},
	} {
		store.candidate = candidate
		_, err = f.service.PauseMemorySandboxAndWait(t.Context(), "owner")
		require.ErrorIs(t, err, service.ErrSandboxLifecycleUnavailable)
	}
	require.Empty(t, f.allocation.requests)
	require.Empty(t, f.plannedRetire.requests)
}

func TestMemoryPauseServiceMapsUnavailableSourceToConflict(t *testing.T) {
	for _, reason := range []error{sandboxstore.ErrNomadCheckpointConflict, sandboxstore.ErrNomadSandboxForkNotReady, sandboxstore.ErrNomadSandboxResumeNotReady} {
		f := newClaimServiceFixture(t)
		f.service.store = &memoryPauseServiceStore{fakeClaimStore: f.store, err: reason}
		_, err := f.service.PauseMemorySandboxAndWait(t.Context(), "owner")
		require.True(t, apierror.IsConflict(err), "%v", err)
		require.Empty(t, f.allocation.requests)
	}
}

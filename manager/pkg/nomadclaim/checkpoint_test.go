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
	"github.com/sandbox0-ai/sandbox0/pkg/procdartifact"
	"github.com/sandbox0-ai/sandbox0/pkg/runtimecontrol"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
	v1alpha1 "github.com/sandbox0-ai/sandbox0/pkg/sandboxspec"
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

type memoryResumeFallbackStore struct {
	*fakeClaimStore
	pending bool
}

func (s *memoryResumeFallbackStore) ResolveNomadCheckpointResumeFallback(ctx context.Context, id, operation, reason string, limit *int64, quotaResolved bool) (string, bool, error) {
	if reason == "" {
		return "", false, nil
	}
	if s.pending {
		return "", true, nil
	}
	if !quotaResolved {
		return "", true, sandboxstore.ErrNomadCheckpointFallbackQuotaRequired
	}
	if _, err := s.AbortNomadSandboxResume(ctx, id, operation, reason); err != nil {
		return "", false, err
	}
	s.resumeCandidate.Checkpoint = nil
	s.resumeCandidate.OperationID += "-rootfs"
	candidate, err := s.RequestNomadSandboxResume(ctx, &sandboxstore.RequestNomadSandboxResumeRequest{
		SandboxID: id, ExpectedTeamID: s.records[id].TeamID, ActiveSandboxLimit: limit,
	})
	if err != nil {
		return "", false, err
	}
	return candidate.OperationID, true, nil
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
	f.service.store = &memoryResumeFallbackStore{fakeClaimStore: f.store}
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

func TestMemoryResumeKeepsCapturedProcdAfterImporterUpgrade(t *testing.T) {
	for _, protocol := range []string{"sandbox0.procd.v1", "sandbox0.procd.v2"} {
		t.Run(protocol, func(t *testing.T) {
			f, id, planner := memoryServiceFixture(t, runtimecontrol.CheckpointResume)
			assignment := &f.store.resumeCandidate.Checkpoint.Assignment
			captured := procdartifact.Artifact{Digest: "sha256:" + strings.Repeat("a", 64), Protocol: "sandbox0.procd.v1"}
			assignment.Target.Procd = &captured
			source := assignment.Target
			source.RuntimeGeneration = assignment.Capture.RuntimeGeneration
			capture, err := runtimecontrol.NewCheckpointCaptureAssignment(assignment.Capture.OperationID, source)
			require.NoError(t, err)
			assignment.Capture = capture
			selected := procdartifact.Artifact{Digest: "sha256:" + strings.Repeat("b", 64), Protocol: protocol}
			f.service.runtimeProcd = &selected

			_, err = f.service.ResumeMemorySandboxAndWait(t.Context(), id)
			if protocol != captured.Protocol {
				require.NoError(t, err)
				require.Empty(t, planner.authorities)
				require.Equal(t, 1, planner.coldCalls)
				return
			}
			require.NoError(t, err)
			require.Len(t, planner.authorities, 1)
			require.Equal(t, captured, *planner.requests[0].Runtime.Procd)
		})
	}
}

func TestMemoryResumeKeepsCapturedInheritedNPMRegistry(t *testing.T) {
	for _, change := range []struct{ captured, current string }{
		{"", "https://npm-cache.example.com"},
	} {
		t.Run(change.captured+"->"+change.current, func(t *testing.T) {
			f, id, planner := memoryServiceFixture(t, runtimecontrol.CheckpointResume)
			assignment := &f.store.resumeCandidate.Checkpoint.Assignment
			if change.captured != "" {
				assignment.Target.EnvVars["NPM_CONFIG_REGISTRY"] = change.captured
			}
			source := assignment.Target
			source.RuntimeGeneration = assignment.Capture.RuntimeGeneration
			capture, err := runtimecontrol.NewCheckpointCaptureAssignment(assignment.Capture.OperationID, source)
			require.NoError(t, err)
			assignment.Capture = capture
			f.service.defaultNPMRegistryURL = change.current

			_, err = f.service.ResumeMemorySandboxAndWait(t.Context(), id)
			require.NoError(t, err)
			require.Zero(t, planner.coldCalls)
			require.Equal(t, assignment.Target.EnvVars, planner.requests[0].Runtime.EnvVars)
		})
	}
}

func TestMemoryResumeFallsBackForCapturedNPMRegistryDriftWithoutProvenance(t *testing.T) {
	f, id, planner := memoryServiceFixture(t, runtimecontrol.CheckpointResume)
	assignment := &f.store.resumeCandidate.Checkpoint.Assignment
	assignment.Target.EnvVars["NPM_CONFIG_REGISTRY"] = "https://custom.example.com/npm/"
	source := assignment.Target
	source.RuntimeGeneration = assignment.Capture.RuntimeGeneration
	capture, err := runtimecontrol.NewCheckpointCaptureAssignment(assignment.Capture.OperationID, source)
	require.NoError(t, err)
	assignment.Capture = capture
	f.service.defaultNPMRegistryURL = "https://npm-cache.example.com"
	_, err = f.service.ResumeMemorySandboxAndWait(t.Context(), id)
	require.NoError(t, err)
	require.Empty(t, planner.authorities)
	require.Equal(t, 1, planner.coldCalls)
}

func TestMemoryResumeFallsBackForExplicitNPMRegistryDrift(t *testing.T) {
	for _, source := range []string{"sandbox", "template", "container"} {
		for _, key := range []string{"NPM_CONFIG_REGISTRY", "npm_config_registry"} {
			t.Run(source+"/"+key, func(t *testing.T) {
				f, id, planner := memoryServiceFixture(t, runtimecontrol.CheckpointResume)
				record := f.store.records[id]
				const registry = "https://custom.example.com/npm/"
				switch source {
				case "sandbox":
					record.Config.EnvVars = map[string]string{key: registry}
				case "template":
					record.TemplateSpec.EnvVars[key] = registry
				case "container":
					record.TemplateSpec.MainContainer.Env = append(record.TemplateSpec.MainContainer.Env,
						v1alpha1.EnvVar{Name: key, Value: registry})
				}
				f.service.defaultNPMRegistryURL = "https://npm-cache.example.com"
				_, err := f.service.ResumeMemorySandboxAndWait(t.Context(), id)
				require.NoError(t, err)
				require.Empty(t, planner.authorities)
				require.Equal(t, 1, planner.coldCalls)
			})
		}
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
	t.Run("invalid-memory-falls-back", func(t *testing.T) {
		f, id, planner := memoryServiceFixture(t, runtimecontrol.CheckpointResume)
		f.store.resumeCandidate.Checkpoint.Assignment.Target.EnvVars["MAIN"] = "changed"
		response, err := f.service.ResumeSandboxAutomaticallyAndWait(t.Context(), id)
		require.NoError(t, err)
		require.True(t, response.Resumed)
		require.Equal(t, 1, planner.coldCalls)
		require.Empty(t, planner.authorities)
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
func TestMemoryResumeServiceFallsBackBeforeExecution(t *testing.T) {
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
			if failure == "filesystem-mode" {
				_, err := f.service.ResumeSandboxAndWait(t.Context(), id)
				require.Error(t, err)
				require.Zero(t, p.coldCalls)
				return
			}
			response, err := f.service.ResumeMemorySandboxAndWait(t.Context(), id)
			require.NoError(t, err)
			require.True(t, response.Resumed)
			require.Empty(t, p.authorities)
			require.Len(t, f.store.resumeAbortCalls, 1)
			require.Len(t, f.store.resumeCompleteCalls, 1)
			require.Equal(t, f.store.resumeCandidate.RuntimeGeneration, f.planner.requests[0].Runtime.RuntimeGeneration)
		})
	}
}

func TestMemoryResumeFallbackWaitsForPhysicalCleanup(t *testing.T) {
	f, id, p := memoryServiceFixture(t, runtimecontrol.CheckpointResume)
	f.service.store.(*memoryResumeFallbackStore).pending = true
	f.store.resumeCandidate.CheckpointCompatibilityDigest = "another-runtime"
	response, err := f.service.ResumeMemorySandboxAndWait(t.Context(), id)
	require.NoError(t, err)
	require.False(t, response.Resumed, "durable fallback was accepted, not yet completed")
	_, err = f.service.ResumeSandboxAutomaticallyAndWait(t.Context(), id)
	require.ErrorIs(t, err, service.ErrSandboxLifecycleUnavailable)
	require.Empty(t, p.authorities)
	require.Zero(t, p.coldCalls)
	require.Empty(t, f.store.resumeCompleteCalls)
}

func TestMemoryResumePreservesCapturedProcdAcrossPlatformUpgrade(t *testing.T) {
	f, id, p := memoryServiceFixture(t, runtimecontrol.CheckpointResume)
	captured := f.store.resumeCandidate.Checkpoint.Assignment.Target.Procd
	f.service.runtimeProcd = &procdartifact.Artifact{Digest: "sha256:" + strings.Repeat("b", 64), Protocol: f.config.RootFSProcdProtocol}
	_, err := f.service.ResumeMemorySandboxAndWait(t.Context(), id)
	require.NoError(t, err)
	require.Zero(t, p.coldCalls)
	require.Equal(t, captured, p.authorities[0].Assignment.Target.Procd)
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

func TestMemoryResumeKeepsStoredTemplateAfterPublishedTemplateUpgrade(t *testing.T) {
	for _, fallback := range []bool{false, true} {
		t.Run(map[bool]string{false: "memory", true: "rootfs"}[fallback], func(t *testing.T) {
			f, id, p := memoryServiceFixture(t, runtimecontrol.CheckpointResume)
			template := f.config.Templates.(*fakeTemplateStore).template
			template.Spec.EnvVars = map[string]string{"TEMPLATE": "upgraded"}
			template.Spec.MainContainer.Image = "upgraded-image:latest"
			if fallback {
				f.store.resumeCandidate.CheckpointCompatibilityDigest = "old-runsc"
			}
			response, err := f.service.ResumeMemorySandboxAndWait(t.Context(), id)
			require.NoError(t, err)
			require.True(t, response.Resumed)
			require.Equal(t, "yes", f.planner.requests[0].Runtime.EnvVars["TEMPLATE"])
			require.Equal(t, fallback, p.coldCalls == 1)
			require.Equal(t, "generation-paused-1", f.store.resumeCandidate.SourceGenerationID)
		})
	}
}

func TestExplicitMemoryResumeMissingOrMismatchedCheckpointUsesRootFS(t *testing.T) {
	for _, failure := range []error{sandboxstore.ErrNomadCheckpointNotRetained, sandboxstore.ErrNomadCheckpointConflict} {
		f, id, p := memoryServiceFixture(t, runtimecontrol.CheckpointResume)
		f.store.memoryResumeErr = failure
		f.store.resumeCandidate.Checkpoint = nil
		response, err := f.service.ResumeMemorySandboxAndWait(t.Context(), id)
		require.NoError(t, err)
		require.True(t, response.Resumed)
		require.Empty(t, p.authorities)
		require.Equal(t, 1, p.coldCalls)
	}
}

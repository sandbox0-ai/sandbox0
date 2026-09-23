package runtimeslotclaim

import (
	"context"
	"errors"
	"testing"

	"github.com/sandbox0-ai/sandbox0/pkg/procdapi"
	"github.com/sandbox0-ai/sandbox0/pkg/runtimecontrol"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
	"github.com/stretchr/testify/require"
)

type checkpointHandoverPlannerStore struct {
	*checkpointPlannerStore
	commands      []procdapi.RuntimeCheckpointRequest
	committed     bool
	failCommit    bool
	changeCommand bool
}

func (s *checkpointHandoverPlannerStore) AuthorizeNomadCheckpointHandover(_ context.Context, r protocol.MigrationRestoreObservation) (*procdapi.RuntimeCheckpointRequest, error) {
	a := r.Request.Image.Checkpoint
	command := procdapi.RuntimeCheckpointRequest{Action: procdapi.MigrationRestore, InstanceID: r.Request.Image.Publication.Capture.Request.ProcdInstanceID,
		CaptureEpoch: r.Request.Image.Publication.Capture.Request.LifecycleEpoch, LifecycleEpoch: a.LifecycleEpoch, Capture: a.Assignment.Capture, Restore: &a.Assignment}
	if s.changeCommand {
		command.CaptureEpoch++
	}
	s.commands = append(s.commands, command)
	return &command, nil
}
func (s *checkpointHandoverPlannerStore) CommitNomadCheckpointHandover(_ context.Context, command procdapi.RuntimeCheckpointRequest, receipt procdapi.RuntimeCheckpointResponse) error {
	if err := receipt.ValidateFor(command); err != nil {
		return err
	}
	s.committed = true
	if s.failCommit {
		s.failCommit = false
		return errors.New("lost handover commit reply")
	}
	return nil
}

type checkpointHandoverPlannerProcd struct {
	*fakeProber
	store        *checkpointHandoverPlannerStore
	delivered    []procdapi.RuntimeCheckpointRequest
	lost         bool
	wrongReceipt bool
	wrongProbe   bool
}

func (p *checkpointHandoverPlannerProcd) CheckpointRuntime(_ context.Context, _ string, command procdapi.RuntimeCheckpointRequest, token string) (*procdapi.RuntimeCheckpointResponse, error) {
	if token != "checkpoint-token" {
		return nil, errors.New("unscoped checkpoint command")
	}
	p.delivered = append(p.delivered, command)
	d, err := command.Digest()
	if err != nil {
		return nil, err
	}
	receipt := &procdapi.RuntimeCheckpointResponse{InstanceID: command.InstanceID, RequestDigest: d, RuntimeGeneration: command.Restore.Target.RuntimeGeneration, State: "ready"}
	if p.lost {
		p.lost = false
		return nil, errors.New("lost procd handover reply")
	}
	if p.wrongReceipt {
		receipt.RuntimeGeneration++
	}
	return receipt, nil
}
func (p *checkpointHandoverPlannerProcd) ProbeCommandReady(ctx context.Context, address, token string) (*procdapi.CommandReadyProbeResult, error) {
	if !p.store.committed {
		return nil, errors.New("readiness preceded durable handover")
	}
	probe, err := p.fakeProber.ProbeCommandReady(ctx, address, token)
	if err == nil && !p.wrongProbe {
		probe.InstanceID = p.store.image.Publication.Capture.Request.ProcdInstanceID
	}
	return probe, err
}

type checkpointHandoverPlannerTokens struct{ *fakeTokenGenerator }

func (p *checkpointHandoverPlannerTokens) GenerateCheckpointToken(command procdapi.RuntimeCheckpointRequest) (string, error) {
	_, err := command.Permission()
	return "checkpoint-token", err
}

func checkpointHandoverFixture(t *testing.T, kind runtimecontrol.CheckpointRestoreKind) (*plannerFixture, *checkpointHandoverPlannerStore, *checkpointHandoverPlannerProcd, *checkpointPlannerNode) {
	t.Helper()
	f, base, node := checkpointPlannerFixture(t, kind)
	store := &checkpointHandoverPlannerStore{checkpointPlannerStore: base}
	procd := &checkpointHandoverPlannerProcd{fakeProber: f.prober, store: store}
	f.planner.store = store
	f.planner.prober = procd
	f.planner.tokenGenerator = &checkpointHandoverPlannerTokens{f.tokens}
	return f, store, procd, node
}

func TestCheckpointClaimHandoverRetriesPreserveProcessesAndGateReadiness(t *testing.T) {
	for _, kind := range []runtimecontrol.CheckpointRestoreKind{runtimecontrol.CheckpointResume, runtimecontrol.CheckpointFork} {
		t.Run(string(kind), func(t *testing.T) {
			f, s, p, n := checkpointHandoverFixture(t, kind)
			p.lost = true
			_, err := f.planner.ClaimCheckpoint(t.Context(), f.request, *s.image.Checkpoint)
			require.ErrorContains(t, err, "lost procd handover reply")
			require.Empty(t, f.prober.addresses)
			require.Empty(t, n.commands)
			s.failCommit = true
			_, err = f.planner.ClaimCheckpoint(t.Context(), f.request, *s.image.Checkpoint)
			require.ErrorContains(t, err, "lost handover commit reply")
			require.Empty(t, f.prober.addresses)
			require.Empty(t, n.commands)
			n.commandErrors = []error{errors.New("lost ready reply")}
			_, err = f.planner.ClaimCheckpoint(t.Context(), f.request, *s.image.Checkpoint)
			require.ErrorContains(t, err, "lost ready reply")
			result, err := f.planner.ClaimCheckpoint(t.Context(), f.request, *s.image.Checkpoint)
			require.NoError(t, err)
			require.Equal(t, s.image.Publication.Capture.Request.ProcdInstanceID, result.ProcdInstanceID)
			require.NoError(t, result.CommandProof.Validate())
			require.False(t, result.WithinSLO)
			require.Equal(t, PhaseCommandReadyCommit, result.Phases[len(result.Phases)-1].Phase)
			require.Len(t, p.delivered, 4)
			for _, command := range p.delivered {
				require.Equal(t, p.delivered[0], command)
			}
			require.Equal(t, 1, n.downloads)
			require.Equal(t, 1, n.preflights)
			require.Equal(t, n.commands[0], n.commands[1])
			require.Equal(t, int64(8), s.filesystem.WriterEpoch)
			require.Equal(t, [3]string{f.request.TeamID, f.request.UserID, f.request.SandboxID}, f.tokens.requests[0])
		})
	}
}
func TestCheckpointClaimHandoverRejectsChangedProofsAndUnsupportedDependencies(t *testing.T) {
	for _, failure := range []string{"command", "receipt", "probe", "tokens", "procd", "store"} {
		t.Run(failure, func(t *testing.T) {
			f, s, p, n := checkpointHandoverFixture(t, runtimecontrol.CheckpointResume)
			switch failure {
			case "command":
				s.changeCommand = true
			case "receipt":
				p.wrongReceipt = true
			case "probe":
				p.wrongProbe = true
			case "tokens":
				f.planner.tokenGenerator = f.tokens
			case "procd":
				f.planner.prober = f.prober
			case "store":
				f.planner.store = s.checkpointPlannerStore
			}
			_, err := f.planner.ClaimCheckpoint(t.Context(), f.request, *s.image.Checkpoint)
			require.Error(t, err)
			require.Empty(t, n.commands)
			if failure == "tokens" || failure == "procd" || failure == "store" {
				require.Empty(t, s.acquires)
				require.Empty(t, n.claims)
			}
		})
	}
}

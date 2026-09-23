package runtimeslotclaim

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	"github.com/sandbox0-ai/sandbox0/pkg/procdapi"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
)

const PhaseCheckpointHandover = "checkpoint_handover"

type checkpointHandoverStore interface {
	AuthorizeNomadCheckpointHandover(context.Context, protocol.MigrationRestoreObservation) (*procdapi.RuntimeCheckpointRequest, error)
	CommitNomadCheckpointHandover(context.Context, procdapi.RuntimeCheckpointRequest, procdapi.RuntimeCheckpointResponse) error
}
type checkpointProcd interface {
	CheckpointRuntime(context.Context, string, procdapi.RuntimeCheckpointRequest, string) (*procdapi.RuntimeCheckpointResponse, error)
}
type checkpointTokens interface {
	GenerateCheckpointToken(procdapi.RuntimeCheckpointRequest) (string, error)
}

var _ checkpointHandoverStore = (*sandboxstore.PGSandboxStore)(nil)

// ClaimCheckpoint restores execution, rebinds procd and verifies its first
// command using the same durable operation. It does not publish sandbox routing:
// the service must commit the existing regional resume transaction afterwards.
func (p *Planner) ClaimCheckpoint(ctx context.Context, request Request, authority protocol.CheckpointRestoreAuthority) (*Result, error) {
	if _, ok := p.store.(checkpointHandoverStore); !ok {
		return nil, errors.New("checkpoint handover authority is unavailable")
	}
	if _, ok := p.prober.(checkpointProcd); !ok {
		return nil, errors.New("checkpoint procd transport is unavailable")
	}
	if _, ok := p.tokenGenerator.(checkpointTokens); !ok {
		return nil, errors.New("checkpoint scoped tokens are unavailable")
	}
	started := p.now().UTC()
	if !request.StartedAt.IsZero() && request.StartedAt.Before(started) {
		started = request.StartedAt
	}
	restored, err := p.RestoreCheckpoint(ctx, request, authority)
	if err != nil {
		return nil, err
	}
	if err := p.readyCheckpoint(ctx, request.UserID, restored); err != nil {
		return nil, err
	}
	restored.Duration = p.now().UTC().Sub(started)
	restored.WithinSLO = false
	return restored, nil
}

// readyCheckpoint never uses ordinary activation: the restored process must
// acknowledge its exact captured instance, source epoch and independent target
// epoch before a readiness probe can authorize the regional slot transition.
func (p *Planner) readyCheckpoint(ctx context.Context, userID string, result *Result) error {
	store := p.store.(checkpointHandoverStore)
	restored := result.MigrationRestore
	if restored == nil || restored.Validate() != nil || restored.State != protocol.MigrationRestoreComplete || restored.Request.Image.Checkpoint == nil {
		return errors.New("checkpoint handover requires completed execution evidence")
	}
	image := restored.Request.Image
	phaseStart := time.Now()
	command, err := store.AuthorizeNomadCheckpointHandover(ctx, *restored)
	if err != nil {
		return fmt.Errorf("authorize checkpoint handover: %w", err)
	}
	if command == nil || command.Action != procdapi.MigrationRestore || !reflect.DeepEqual(command.Restore, &image.Checkpoint.Assignment) ||
		command.InstanceID != image.Publication.Capture.Request.ProcdInstanceID || command.CaptureEpoch != image.Publication.Capture.Request.LifecycleEpoch || command.LifecycleEpoch != image.Checkpoint.LifecycleEpoch {
		return errors.New("checkpoint handover changed restored execution")
	}
	if _, err := command.Digest(); err != nil {
		return err
	}
	token, err := p.tokenGenerator.(checkpointTokens).GenerateCheckpointToken(*command)
	if err != nil {
		return err
	}
	if token == "" {
		return errors.New("checkpoint handover token is empty")
	}
	receipt, err := p.prober.(checkpointProcd).CheckpointRuntime(ctx, result.ProcdAddress, *command, token)
	if err != nil {
		return fmt.Errorf("deliver checkpoint handover: %w", err)
	}
	if receipt == nil || receipt.ValidateFor(*command) != nil {
		return errors.New("checkpoint handover acknowledgement changed command")
	}
	if err := store.CommitNomadCheckpointHandover(ctx, *command, *receipt); err != nil {
		return fmt.Errorf("commit checkpoint handover: %w", err)
	}
	result.Phases = append(result.Phases, PhaseObservation{Phase: PhaseCheckpointHandover, Duration: time.Since(phaseStart), Succeeded: true})
	phaseStart = time.Now()
	assignment := image.RuntimeAssignment()
	token, err = p.tokenGenerator.GenerateToken(assignment.TeamID, userID, assignment.SandboxID)
	if err != nil {
		return err
	}
	if token == "" {
		return errors.New("checkpoint readiness token is empty")
	}
	deadline := result.Slot.ClaimLeaseExpiresAt
	if result.Slot.State == sandboxstore.RuntimeSlotStateActive {
		// An acknowledged slot may retry the final region commit after its original
		// startup deadline. Current ownership and hard TTL remain regional checks.
		deadline = p.now().Add(p.claimTTL)
	}
	probe, err := p.probeCommandReady(ctx, result.ProcdAddress, token, deadline, deadline)
	if err != nil {
		return fmt.Errorf("probe checkpoint command readiness: %w", err)
	}
	if probe == nil || probe.Status != "ready" || probe.InstanceID != command.InstanceID {
		return errors.New("checkpoint readiness changed preserved procd instance")
	}
	identity := restored.Request.Stage.Identity
	proof := protocol.CommandReadyProof{Version: protocol.CommandReadyProofVersion, SlotID: image.Target.SlotID,
		OperationID: image.OperationID(), ClaimID: image.Resources.ClaimID, LaunchAttempt: identity.LaunchAttempt,
		RunscContainerID: protocol.NomadRunscContainerID(image.Target.SlotID), ProcdInstanceID: probe.InstanceID, ProcdAddress: result.ProcdAddress,
		RequestMethod: "PUT", RequestPath: protocol.ProcdCommandReadyProbePath, ResponseStatus: 200, ResponseBodyDigest: probe.ResponseBodyDigest}
	if err := proof.Validate(); err != nil {
		return err
	}
	result.Phases = append(result.Phases, PhaseObservation{Phase: PhaseProcdProbe, Duration: time.Since(phaseStart), Succeeded: true})
	phaseStart = time.Now()
	ready := protocol.CommandReadyControlRequest{Proof: proof}
	response, err := p.node.CommandReady(ctx, migrationNodeTarget(image.Target), ready)
	if err != nil {
		return fmt.Errorf("commit checkpoint command readiness: %w", err)
	}
	if err := response.ValidateCommandReadyResult(ready); err != nil {
		return err
	}
	if response.MigrationAdoption != nil && response.MigrationAdoption.Request.ValidateFor(*restored) != nil {
		return errors.New("checkpoint readiness changed image adoption")
	}
	result.ProcdInstanceID = probe.InstanceID
	result.CommandProof = proof
	result.Phases = append(result.Phases, PhaseObservation{Phase: PhaseCommandReadyCommit, Duration: time.Since(phaseStart), Succeeded: true})
	return nil
}

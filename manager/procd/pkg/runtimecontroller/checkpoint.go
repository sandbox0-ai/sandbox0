package runtimecontroller

import (
	"context"
	"errors"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/runtimecontrol"
)

// PrepareCheckpoint gates API work without preselecting a restore destination.
// This is not a guest freeze: the node must capture execution and its matching
// RootFS after the HTTP layer drains admitted mutations.
func (c *Controller) PrepareCheckpoint(request runtimecontrol.CheckpointCaptureAssignment) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	return c.PrepareCheckpointContext(ctx, request)
}

// PrepareCheckpointContext carries the transport deadline through webhook drain.
func (c *Controller) PrepareCheckpointContext(ctx context.Context, request runtimecontrol.CheckpointCaptureAssignment) error {
	digest, err := request.Digest()
	if err != nil {
		return err
	}
	c.applyMu.Lock()
	defer c.applyMu.Unlock()
	state := c.State()
	if c.checkpointDigest == digest && c.checkpointCanceled {
		return errors.New("checkpoint preparation was canceled")
	}
	if c.checkpointDigest == digest && state.Phase == PhaseCheckpointing && c.checkpointRestore == "" {
		return c.pauseCheckpointDelivery(ctx)
	}
	if state.Phase != PhaseReady || state.Revision != request.Revision ||
		state.RuntimeGeneration != request.RuntimeGeneration {
		return errors.New("checkpoint source does not match the active assignment")
	}
	if c.dispatcher != nil {
		if err := c.dispatcher.ValidateCheckpoint(); err != nil {
			return err
		}
	}
	c.checkpointDigest = digest
	c.checkpointRestore = ""
	c.checkpointCanceled = false
	c.setState(PhaseCheckpointing, state.Revision, state.RuntimeGeneration, "runtime checkpoint is in progress")
	return c.pauseCheckpointDelivery(ctx)
}

// RestoreCheckpoint changes only the identity of an already-restored process.
// The regional authority must authorize execution before runsc restore, not
// merely before this API call. No activation or session restart is performed.
func (c *Controller) RestoreCheckpoint(request runtimecontrol.CheckpointRestoreAssignment) error {
	digest, err := request.Digest()
	if err != nil {
		return err
	}
	captureDigest, _ := request.Capture.Digest()
	targetRevision, _ := request.Target.Revision()
	c.applyMu.Lock()
	defer c.applyMu.Unlock()
	if c.checkpointDigest != captureDigest || c.checkpointCanceled {
		return errors.New("restore does not match the prepared checkpoint")
	}
	if c.checkpointRestore != "" && c.checkpointRestore != digest {
		return errors.New("another restore owns the captured process")
	}
	state := c.State()
	if state.Phase == PhaseReady && state.Revision == targetRevision &&
		state.RuntimeGeneration == request.Target.RuntimeGeneration && c.checkpointRestore == digest {
		return nil
	}
	if state.Phase != PhaseCheckpointing || state.Revision != request.Capture.Revision ||
		state.RuntimeGeneration != request.Capture.RuntimeGeneration {
		return errors.New("checkpoint restore source assignment changed")
	}
	// Record the exact restore before any mutation. Even a partial persistence
	// failure cannot authorize a different fork or reopen the old source.
	c.checkpointRestore = digest
	if c.sessionSupervisor != nil {
		if err := c.sessionSupervisor.RebindCheckpoint(request); err != nil {
			return err
		}
	}
	if c.dispatcher != nil {
		if err := c.dispatcher.RebindCheckpointIdentity(request.Capture.SandboxID, request.Target.SandboxID, request.Target.TeamID); err != nil {
			return err
		}
	}
	if c.contextManager != nil {
		c.contextManager.SetSandboxEnvVars(request.Target.EnvVars)
	}
	c.setState(PhaseReady, targetRevision, request.Target.RuntimeGeneration, "")
	if c.dispatcher != nil {
		c.dispatcher.ResumeDelivery()
	}
	return nil
}

// CancelPreparedCheckpoint only reopens the unchanged source. The caller must
// prove capture has not begun; cancellation is never a way to replay an image.
func (c *Controller) CancelPreparedCheckpoint(request runtimecontrol.CheckpointCaptureAssignment) error {
	digest, err := request.Digest()
	if err != nil {
		return err
	}
	c.applyMu.Lock()
	defer c.applyMu.Unlock()
	state := c.State()
	if state.Revision != request.Revision || state.RuntimeGeneration != request.RuntimeGeneration ||
		(state.Phase != PhaseReady && state.Phase != PhaseCheckpointing) {
		return errors.New("checkpoint cannot reopen the source assignment")
	}
	if c.checkpointDigest != digest {
		if state.Phase != PhaseReady {
			return errors.New("another operation owns the source")
		}
		c.checkpointDigest = digest
		c.checkpointRestore = ""
	} else if c.checkpointRestore != "" {
		return errors.New("checkpoint restore has already started")
	}
	c.checkpointCanceled = true
	c.setState(PhaseReady, state.Revision, state.RuntimeGeneration, "")
	if c.dispatcher != nil {
		c.dispatcher.ResumeDelivery()
	}
	return nil
}

func (c *Controller) pauseCheckpointDelivery(ctx context.Context) error {
	if c.dispatcher != nil {
		return c.dispatcher.PauseDelivery(ctx)
	}
	return nil
}

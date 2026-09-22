package runtimecontroller

import (
	"errors"

	"github.com/sandbox0-ai/sandbox0/pkg/runtimecontrol"
)

// PrepareMigration gates new API work before the source execution cut. This
// local gate is not a process freeze; ctld must still stop guest execution and
// capture a matching RootFS. Only the authenticated regional migration path may
// call these methods; no public runtime activation route is provided.
func (c *Controller) PrepareMigration(request runtimecontrol.MigrationAssignment) error {
	digest, err := request.Digest()
	if err != nil {
		return err
	}
	c.applyMu.Lock()
	defer c.applyMu.Unlock()
	state := c.State()
	if c.migrationDigest == digest && c.migrationCanceled {
		return errors.New("migration preparation was canceled")
	}
	if c.migrationDigest == digest && state.Phase == PhaseMigrating {
		return nil
	}
	if state.Phase != PhaseReady || state.Revision != request.SourceRevision ||
		state.RuntimeGeneration != request.SourceGeneration {
		return errors.New("migration source does not match the active assignment")
	}
	c.migrationDigest = digest
	c.migrationRebinding = false
	c.migrationCanceled = false
	c.setState(PhaseMigrating, state.Revision, state.RuntimeGeneration, "runtime migration is in progress")
	return nil
}

// RestoreMigration rebinds the preserved procd process after the destination
// has sole execution authorization. It deliberately does not call Activate:
// live process handles, REPLs and supervised attempts belong to this restored
// execution state and must survive. A failed session save keeps readiness gated.
func (c *Controller) RestoreMigration(request runtimecontrol.MigrationAssignment) error {
	digest, err := request.Digest()
	if err != nil {
		return err
	}
	c.applyMu.Lock()
	defer c.applyMu.Unlock()
	state := c.State()
	targetRevision, _ := request.Target.Revision()
	if c.migrationDigest != digest {
		return errors.New("restored migration does not match the prepared operation")
	}
	if state.Phase == PhaseReady && state.Revision == targetRevision &&
		state.RuntimeGeneration == request.Target.RuntimeGeneration {
		return nil
	}
	if state.Phase != PhaseMigrating || state.Revision != request.SourceRevision ||
		state.RuntimeGeneration != request.SourceGeneration {
		return errors.New("restored migration source assignment changed")
	}
	c.migrationRebinding = true
	if c.sessionSupervisor != nil {
		if err := c.sessionSupervisor.RebindRuntimeGeneration(request.OperationID,
			request.Target.SandboxID, request.SourceGeneration, request.Target.RuntimeGeneration); err != nil {
			return err
		}
	}
	c.setState(PhaseReady, targetRevision, request.Target.RuntimeGeneration, "")
	return nil
}

// CancelPreparedMigration only reopens the unchanged source API gate. Manager
// must independently prove the destination never received execution authority
// and source execution is safe to resume before issuing this cancellation.
func (c *Controller) CancelPreparedMigration(request runtimecontrol.MigrationAssignment) error {
	digest, err := request.Digest()
	if err != nil {
		return err
	}
	c.applyMu.Lock()
	defer c.applyMu.Unlock()
	state := c.State()
	if state.Revision != request.SourceRevision || state.RuntimeGeneration != request.SourceGeneration ||
		(state.Phase != PhaseMigrating && state.Phase != PhaseReady) {
		return errors.New("migration cannot reopen the source assignment")
	}
	if c.migrationDigest != digest {
		// Cancellation may win delivery ordering before preparation. The HTTP
		// authority checks monotonic lifecycle epochs; retain this tombstone
		// without first closing a ready source's admission.
		if state.Phase != PhaseReady {
			return errors.New("another migration owns the source assignment")
		}
		c.migrationDigest, c.migrationRebinding = digest, false
	} else if c.migrationRebinding {
		return errors.New("migration cannot cancel a destination rebind")
	}
	c.migrationCanceled = true
	c.setState(PhaseReady, state.Revision, state.RuntimeGeneration, "")
	return nil
}

package session

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"

	"github.com/containerd/errdefs"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshandoff"
)

// ErrMigrationCutOwnerLost means filesystem caches disappeared before a durable
// freeze/sync boundary. Neither a memory image nor the remaining WAL can recover
// that cut. It does not itself authorize physical cleanup or writer retirement.
var ErrMigrationCutOwnerLost = errors.New("migration filesystem cut lost its owner before durable sync")

type abandonedMigrationCut struct {
	Request           rootfshandoff.MigrationRootFSCutRequest `json:"request"`
	WriterOperationID string                                  `json:"writer_operation_id"`
}

func validateAbandonedMigrationCut(current record) error {
	a := current.AbandonedMigration
	if a == nil {
		return nil
	}
	if current.Version < 12 || current.Stage == nil || current.Migration != nil ||
		current.RetireOperationID != "" || current.RunningForkRequest != nil || current.DirtyTailPressure != nil ||
		a.Request.Validate() != nil || a.Request.SourceBindingDigest != current.BindingDigest ||
		a.Request.GenerationID == current.GenerationID || strings.TrimSpace(a.WriterOperationID) != a.WriterOperationID ||
		a.WriterOperationID == "" || len(a.WriterOperationID) > 512 ||
		!containsSessionState(current.State, stateFailed, stateReleasing, stateTombstoned) ||
		(current.FreezeOperationID != "" && current.FreezeOperationID != a.Request.OperationID) ||
		(current.CrashFence != nil && (!current.CrashFence.External || current.CrashFence.OperationID != a.WriterOperationID)) {
		return fmt.Errorf("invalid abandoned migration cut: %w", errdefs.ErrFailedPrecondition)
	}
	return nil
}

// AbandonLostMigrationCut changes only local custody after the node has durably
// accepted exact regional capture-failure authority. It requires a dead local
// owner and no synced sequence, result or handoff. The original cut identity is
// retained until normal physical proof, regional writer retirement and artifact
// reclamation complete. It never promotes the remaining WAL or publishes a head.
func (m *Manager) AbandonLostMigrationCut(ctx context.Context, stage rootfshandoff.StageRequest, request rootfshandoff.MigrationRootFSCutRequest, writerOperationID string) error {
	if ctx == nil {
		return errdefs.ErrInvalidArgument
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if stage.ValidateDurableBinding() != nil || stage.Identity.WriterGrantToken != "" || request.Validate() != nil {
		return errdefs.ErrInvalidArgument
	}
	binding, err := stage.BindingDigest()
	if err != nil || request.SourceBindingDigest != hex.EncodeToString(binding[:]) {
		return errdefs.ErrFailedPrecondition
	}
	unlock := m.lock(stage.Parent)
	defer unlock()
	current, err := m.load(stage.Parent)
	if err != nil {
		return err
	}
	if !sameBinding(current, stage, request.SourceBindingDigest) {
		return errdefs.ErrFailedPrecondition
	}
	want := abandonedMigrationCut{Request: request, WriterOperationID: writerOperationID}
	if current.AbandonedMigration != nil {
		if *current.AbandonedMigration != want {
			return errdefs.ErrAlreadyExists
		}
		return validateAbandonedMigrationCut(current)
	}
	if err := validateMigrationCutRecord(current); err != nil {
		return err
	}
	cut := current.Migration
	if cut.Request != request || cut.Sequence != nil || cut.Result != nil || cut.DetachRequest != nil || cut.FinalizeRequest != nil {
		return errdefs.ErrFailedPrecondition
	}
	m.mu.Lock()
	owned := m.live[stage.Parent] != nil || m.captures[stage.Parent]
	m.mu.Unlock()
	if owned {
		return fmt.Errorf("migration cut still has a local owner: %w", errdefs.ErrFailedPrecondition)
	}
	current.AbandonedMigration, current.Migration = &want, nil
	current.Version, current.State = sessionSchemaVersion, stateFailed
	if err := validateAbandonedMigrationCut(current); err != nil {
		return err
	}
	return m.save(current)
}

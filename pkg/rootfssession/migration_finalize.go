package session

import (
	"context"
	"encoding/hex"
	"fmt"

	"github.com/containerd/errdefs"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshandoff"
)

// FinalizeMigrationRootFS consumes ctld's explicit regional handoff command.
// Intent survives interruption while the shared terminal-artifact reclaimer
// removes the WAL and mount directories. It never reopens or publishes a WAL.
func (m *Manager) FinalizeMigrationRootFS(ctx context.Context, stage rootfshandoff.StageRequest, request rootfshandoff.MigrationRootFSFinalizeRequest) (rootfshandoff.MigrationRootFSFinalizeProof, error) {
	var zero rootfshandoff.MigrationRootFSFinalizeProof
	if err := request.Validate(); err != nil {
		return zero, err
	}
	if err := stage.ValidateDurableBinding(); err != nil {
		return zero, err
	}
	binding, err := stage.BindingDigest()
	if err != nil {
		return zero, err
	}
	unlock := m.lock(stage.Parent)
	defer unlock()
	current, err := m.load(stage.Parent)
	if err != nil {
		return zero, err
	}
	if !sameBinding(current, stage, hex.EncodeToString(binding[:])) || current.Migration == nil {
		return zero, errdefs.ErrFailedPrecondition
	}
	if err := validateMigrationCutRecord(current); err != nil {
		return zero, err
	}
	migration := current.Migration
	if migration.DetachProof == nil || request.ValidateFor(stage, *migration.DetachProof) != nil {
		return zero, errdefs.ErrFailedPrecondition
	}
	if migration.FinalizeRequest != nil && *migration.FinalizeRequest != request {
		return zero, errdefs.ErrAlreadyExists
	}
	if migration.FinalizeProof != nil {
		return *migration.FinalizeProof, nil
	}
	m.mu.Lock()
	_, live := m.live[stage.Parent]
	capturing := m.captures[stage.Parent]
	m.mu.Unlock()
	if live || capturing || !current.DeviceReservationReleased {
		return zero, fmt.Errorf("migration source still owns physical resources: %w", errdefs.ErrFailedPrecondition)
	}
	if err := ctx.Err(); err != nil {
		return zero, err
	}
	if migration.FinalizeRequest == nil {
		migration.FinalizeRequest = &request
		current.Version = sessionSchemaVersion
		if err := m.save(current); err != nil {
			return zero, err
		}
	}
	if err := m.reclaimTerminalArtifactsLocked(&current); err != nil {
		return zero, err
	}
	if err := m.validateTerminalArtifactAbsence(current); err != nil {
		return zero, err
	}
	proof := rootfshandoff.MigrationRootFSFinalizeProof{Request: request, Parent: current.Parent, BindingDigest: current.BindingDigest, BranchAbsent: true, MountDirectoriesAbsent: true}
	proof.Digest, err = proof.ProofDigest()
	if err != nil {
		return zero, err
	}
	if err := proof.ValidateFor(stage, *migration.DetachProof, request); err != nil {
		return zero, err
	}
	current.Migration.FinalizeProof = &proof
	if err := m.save(current); err != nil {
		return zero, err
	}
	return proof, nil
}

// ForgetFinalizedMigrationRootFS is called only after ctld has durably stored
// its final slot cleanup proof. That node journal then owns response replay;
// deleting the compact session cannot lose physical cleanup evidence.
func (m *Manager) ForgetFinalizedMigrationRootFS(stage rootfshandoff.StageRequest, request rootfshandoff.MigrationRootFSFinalizeRequest) error {
	if err := stage.ValidateDurableBinding(); err != nil {
		return err
	}
	if err := request.Validate(); err != nil {
		return err
	}
	_, err := m.forgetTerminalWithMigration(stage.Parent, stage.Identity, func(current record) (bool, error) {
		binding, err := stage.BindingDigest()
		if err != nil {
			return false, err
		}
		if !sameBinding(current, stage, hex.EncodeToString(binding[:])) || validateMigrationCutRecord(current) != nil ||
			current.Migration == nil || current.Migration.FinalizeProof == nil || current.Migration.DetachProof == nil ||
			current.Migration.FinalizeProof.ValidateFor(stage, *current.Migration.DetachProof, request) != nil {
			return false, errdefs.ErrFailedPrecondition
		}
		return true, nil
	}, true)
	return err
}

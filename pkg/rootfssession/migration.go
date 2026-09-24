package session

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/containerd/errdefs"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsblock"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshandoff"
)

// Sequence is written only after successful XFS freeze and branch WAL sync.
// An intent without it cannot recover a matching filesystem from a dead owner.
type migrationCutRecord struct {
	FinalizeRequest *rootfshandoff.MigrationRootFSFinalizeRequest `json:"finalize_request,omitempty"`
	FinalizeProof   *rootfshandoff.MigrationRootFSFinalizeProof   `json:"finalize_proof,omitempty"`
	DetachRequest   *rootfshandoff.MigrationRootFSDetachRequest   `json:"detach_request,omitempty"`
	DetachProof     *rootfshandoff.MigrationRootFSDetachProof     `json:"detach_proof,omitempty"`
	Request         rootfshandoff.MigrationRootFSCutRequest       `json:"request"`
	Sequence        *uint64                                       `json:"sequence,omitempty"`
	Result          *rootfshandoff.MigrationRootFSCut             `json:"result,omitempty"`
}

func validateMigrationCutRecord(current record) error {
	migration := current.Migration
	if migration == nil || current.Stage == nil ||
		current.RetireOperationID != "" || current.RunningForkRequest != nil || current.CrashFence != nil ||
		migration.Request.Validate() != nil || migration.Request.SourceBindingDigest != current.BindingDigest ||
		migration.Request.GenerationID == current.GenerationID || migration.Sequence != nil && *migration.Sequence > math.MaxInt64 {
		return fmt.Errorf("invalid migration RootFS custody: %w", errdefs.ErrFailedPrecondition)
	}
	if migration.DetachRequest == nil {
		sealedAndThawed := current.Version >= 11 && migration.Result != nil && current.FreezeOperationID == ""
		if current.State != stateMigration || (!sealedAndThawed && current.FreezeOperationID != migration.Request.OperationID) || migration.DetachProof != nil {
			return fmt.Errorf("invalid migration freeze custody: %w", errdefs.ErrFailedPrecondition)
		}
	} else {
		request := *migration.DetachRequest
		if request.Validate() != nil || migration.Result == nil || request.CutDigest != migration.Result.Digest ||
			request.OperationID != migration.Request.OperationID || !containsSessionState(current.State, stateMigration, stateReleasing, stateTombstoned) ||
			(current.FreezeOperationID != "" && current.FreezeOperationID != migration.Request.OperationID) {
			return fmt.Errorf("invalid migration detach intent: %w", errdefs.ErrFailedPrecondition)
		}
		if migration.DetachProof != nil {
			if current.State != stateTombstoned || current.FreezeOperationID != "" {
				return errdefs.ErrFailedPrecondition
			}
			if err := migration.DetachProof.ValidateFor(*current.Stage, *migration.Result, request); err != nil {
				return err
			}
		}
	}
	if migration.FinalizeRequest != nil {
		if current.Version < 10 || migration.DetachProof == nil || !current.DeviceReservationReleased ||
			migration.FinalizeRequest.ValidateFor(*current.Stage, *migration.DetachProof) != nil {
			return fmt.Errorf("invalid migration artifact reclamation intent: %w", errdefs.ErrFailedPrecondition)
		}
		if migration.FinalizeProof != nil && (!current.BranchRemoved || migration.FinalizeProof.ValidateFor(*current.Stage, *migration.DetachProof, *migration.FinalizeRequest) != nil) {
			return fmt.Errorf("invalid migration artifact reclamation proof: %w", errdefs.ErrFailedPrecondition)
		}
	} else if migration.FinalizeProof != nil || current.BranchRemoved {
		return fmt.Errorf("migration artifact reclamation lacks explicit intent: %w", errdefs.ErrFailedPrecondition)
	}
	if migration.Result != nil {
		if migration.Sequence == nil || migration.Result.Sequence != *migration.Sequence {
			return fmt.Errorf("migration RootFS result lacks its synced WAL boundary: %w", errdefs.ErrFailedPrecondition)
		}
		return migration.Result.ValidateFor(*current.Stage, migration.Request)
	}
	return nil
}

// CaptureMigrationRootFS seals the filesystem after the node has independently
// verified successful memory capture and stopped source execution. XFS stays
// frozen until the immutable cut is durable, then thaws while source execution
// remains stopped under migration custody. This method cannot publish a regional
// head or claim source resources are released.
func (m *Manager) CaptureMigrationRootFS(ctx context.Context, stage rootfshandoff.StageRequest, request rootfshandoff.MigrationRootFSCutRequest) (rootfshandoff.MigrationRootFSCut, error) {
	var zero rootfshandoff.MigrationRootFSCut
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
	if stage.Identity.WriterGrantToken != "" || stage.Generation == nil || stage.Generation.LocatorVersion == math.MaxInt64 ||
		request.SourceBindingDigest != hex.EncodeToString(binding[:]) || request.GenerationID == stage.InitialGeneration {
		return zero, errdefs.ErrFailedPrecondition
	}
	unlock := m.lock(stage.Parent)
	defer func() {
		if unlock != nil {
			unlock()
		}
	}()
	current, err := m.load(stage.Parent)
	if err != nil {
		return zero, err
	}
	if !sameBinding(current, stage, request.SourceBindingDigest) {
		return zero, errdefs.ErrFailedPrecondition
	}
	m.mu.Lock()
	live := m.live[stage.Parent]
	m.mu.Unlock()
	initial := current.Migration == nil
	m.mu.Lock()
	if m.captures[stage.Parent] {
		m.mu.Unlock()
		return zero, errdefs.ErrUnavailable
	}
	m.captures[stage.Parent] = true
	m.mu.Unlock()
	defer func() { m.mu.Lock(); delete(m.captures, stage.Parent); m.mu.Unlock() }()
	if initial {
		if current.State != stateReady || live == nil || current.RetireOperationID != "" || current.RunningForkRequest != nil ||
			current.DirtyTailPressure != nil || current.FreezeOperationID != "" || current.CrashFence != nil {
			return zero, fmt.Errorf("migration requires the exact live and unowned RootFS session: %w", errdefs.ErrFailedPrecondition)
		}
		if err := ctx.Err(); err != nil {
			return zero, err
		}
		// A competing retirement is retryable until the filesystem barrier
		// begins. Reserve its bounded sync capacity before persisting cut intent;
		// an intent without a synced sequence must remain fail-closed.
		if err := live.branch.BeginRetirement(); err != nil {
			return zero, err
		}
		current.Migration = &migrationCutRecord{Request: request}
		current.State, current.Version = stateMigration, sessionSchemaVersion
		current.FreezeOperationID = request.OperationID
		if err := m.save(current); err != nil {
			return zero, err
		}
	} else {
		if err := validateMigrationCutRecord(current); err != nil {
			return zero, err
		}
		if current.Migration.DetachRequest != nil {
			return zero, errdefs.ErrFailedPrecondition
		}
		if current.Migration.Request != request {
			return zero, errdefs.ErrAlreadyExists
		}
		if current.Migration.Result != nil {
			return m.finishMigrationCutLocked(current)
		}
		if current.Migration.Sequence == nil {
			if live == nil {
				return zero, errors.Join(ErrMigrationCutOwnerLost, errdefs.ErrUnavailable)
			}
			return zero, fmt.Errorf("migration freeze/sync outcome is uncertain; a WAL tail cannot prove a filesystem cut: %w", errdefs.ErrUnavailable)
		}
	}
	if err := ctx.Err(); err != nil {
		return zero, err
	}
	var branch *rootfsblock.Branch
	if live != nil {
		branch = live.branch
	} else {
		// Only an already-synced sequence permits replay after owner death.
		// Reopening a WAL never recreates or flushes lost filesystem caches.
		branch, err = m.reopenBranch(current)
		if err != nil {
			return zero, err
		}
		defer branch.Close()
	}
	if !initial {
		if err := branch.BeginRetirement(); err != nil {
			return zero, err
		}
	}
	if initial {
		if err := m.runtime.FreezeXFS(current.XFSRoot); err != nil {
			return zero, fmt.Errorf("freeze migration RootFS: %w", err)
		}
	}
	checkpoint, err := branch.Checkpoint()
	if err != nil {
		return zero, err
	}
	defer checkpoint.Close()
	sequence := checkpoint.Sequence()
	if current.Migration.Sequence != nil && *current.Migration.Sequence != sequence {
		return zero, fmt.Errorf("migration WAL changed after its synced filesystem cut: %w", errdefs.ErrFailedPrecondition)
	}
	if initial {
		current.Migration.Sequence = &sequence
		if err := m.save(current); err != nil {
			return zero, err
		}
	}
	// Immutable publication can outlive a consumer lease interval. Release the
	// session metadata lock so the exact carrier can renew while XFS stays
	// frozen; the per-parent capture marker excludes a second publisher.
	unlock()
	unlock = nil
	base, err := rootfsblock.DecodeDescriptor(current.BaseDescriptor)
	if err != nil {
		return zero, err
	}
	// Execution images must be paired with immutable regional blocks, not an
	// inline dirty tail whose durability would depend on a later SQL commit.
	built, err := rootfsblock.BuildIncrementalGenerationFromBlockReader(ctx, m.source, base, checkpoint, m.publisher, rootfsblock.BuildOptions{})
	err = errors.Join(err, checkpoint.Close())
	if err != nil {
		return zero, err
	}
	generation := *stage.Generation
	generation.GenerationID = request.GenerationID
	generation.CurrentBlockHead = built.Descriptor.MappingRoot.RootDigest
	generation.WriterEpoch = stage.Identity.WriterEpoch
	generation.LocatorVersion++
	generation.Descriptor, generation.DurabilityState = built.Payload, rootfsblock.DurabilityS3
	result, err := rootfshandoff.NewMigrationRootFSCut(request, generation, sequence)
	if err != nil {
		return zero, err
	}
	if err := result.ValidateFor(stage, request); err != nil {
		return zero, err
	}
	unlock = m.lock(stage.Parent)
	current, err = m.load(stage.Parent)
	if err != nil {
		return zero, err
	}
	if err := validateMigrationCutRecord(current); err != nil {
		return zero, err
	}
	if current.Migration.Request != request || current.Migration.Sequence == nil || *current.Migration.Sequence != sequence {
		return zero, errdefs.ErrFailedPrecondition
	}
	current.Migration.Result = &result
	if err := m.save(current); err != nil {
		return zero, err
	}
	return m.finishMigrationCutLocked(current)
}

// The immutable cut is durable before releasing the filesystem barrier. Guest
// execution remains stopped under migration custody; later XFS housekeeping
// writes on the old branch are discarded by detach, never republished. Keeping
// XFS frozen through image transfer would pin its superblock across NBD owner
// death and prevent physical cleanup. A lost thaw reply retries the same cut.
func (m *Manager) finishMigrationCutLocked(current record) (rootfshandoff.MigrationRootFSCut, error) {
	if err := validateMigrationCutRecord(current); err != nil || current.Migration.Result == nil {
		return rootfshandoff.MigrationRootFSCut{}, errdefs.ErrFailedPrecondition
	}
	if current.FreezeOperationID != "" {
		if err := m.runtime.ThawXFS(current.XFSRoot); err != nil {
			return rootfshandoff.MigrationRootFSCut{}, fmt.Errorf("thaw sealed migration RootFS: %w", err)
		}
		current.FreezeOperationID = ""
		current.Version = sessionSchemaVersion
		if err := m.save(current); err != nil {
			return rootfshandoff.MigrationRootFSCut{}, err
		}
	}
	return cloneMigrationRootFSCut(*current.Migration.Result), nil
}

func cloneMigrationRootFSCut(value rootfshandoff.MigrationRootFSCut) rootfshandoff.MigrationRootFSCut {
	value.Generation.Descriptor = append([]byte(nil), value.Generation.Descriptor...)
	return value
}

// DetachMigrationRootFS consumes an explicit handoff authorization only after
// ctld has removed the exact source execution instance and its stable mount.
// It preserves the cut, journal and WAL as custody evidence. Normal crash and
// release paths cannot initiate or reinterpret this transition.
func (m *Manager) DetachMigrationRootFS(ctx context.Context, stage rootfshandoff.StageRequest, request rootfshandoff.MigrationRootFSDetachRequest) (rootfshandoff.MigrationRootFSDetachProof, error) {
	var zero rootfshandoff.MigrationRootFSDetachProof
	if err := request.Validate(); err != nil {
		return zero, err
	}
	if err := stage.ValidateDurableBinding(); err != nil {
		return zero, err
	}
	if stage.Identity.WriterGrantToken != "" {
		return zero, errdefs.ErrFailedPrecondition
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
	if !sameBinding(current, stage, hex.EncodeToString(binding[:])) {
		return zero, errdefs.ErrFailedPrecondition
	}
	if err := validateMigrationCutRecord(current); err != nil {
		return zero, err
	}
	migration := current.Migration
	if migration.Result == nil || request.OperationID != migration.Request.OperationID || request.CutDigest != migration.Result.Digest {
		return zero, errdefs.ErrFailedPrecondition
	}
	if migration.DetachRequest != nil && *migration.DetachRequest != request {
		return zero, errdefs.ErrAlreadyExists
	}
	m.mu.Lock()
	capturing := m.captures[stage.Parent]
	m.mu.Unlock()
	if capturing {
		return zero, errdefs.ErrUnavailable
	}
	if migration.DetachProof != nil {
		if err := m.releaseDeviceReservation(&current); err != nil {
			return zero, err
		}
		return *migration.DetachProof, nil
	}
	if migration.DetachRequest == nil {
		migration.DetachRequest = &request
		current.Version = sessionSchemaVersion
		if err := m.save(current); err != nil {
			return zero, err
		}
	}
	if current.State != stateTombstoned {
		// This shared teardown does not create another checkpoint: the already
		// published memory/filesystem cut remains the only migration output.
		if err := m.releasePhysicalLocked(ctx, current); err != nil {
			return zero, err
		}
		current, err = m.load(stage.Parent)
		if err != nil {
			return zero, err
		}
	}
	m.mu.Lock()
	_, live := m.live[stage.Parent]
	m.mu.Unlock()
	if live || current.DevicePath == "" || current.DeviceAllocationID == "" {
		return zero, errdefs.ErrFailedPrecondition
	}
	inspector, ok := m.runtime.(CrashFenceHostInspector)
	if !ok {
		return zero, errdefs.ErrUnavailable
	}
	host, err := inspector.InspectCrashFence(current.DevicePath, current.XFSRoot, current.MergedRoot)
	if err != nil {
		return zero, err
	}
	observation := rootfshandoff.CrashFenceSessionObservation{
		Parent: current.Parent, RootFSID: current.RootFSID, WriterEpoch: current.WriterEpoch,
		OperationID: request.OperationID, BindingDigest: current.BindingDigest, SessionState: current.State,
		BranchPath: current.BranchPath, DeviceBound: true, DevicePath: current.DevicePath, NBDPoolAbsent: host.NBDPoolAbsent,
		NBDPID: host.NBDPID, NBDHolders: append([]string(nil), host.NBDHolders...), LiveSessionAbsent: true,
		MergedMountAbsent: host.MergedMountAbsent, XFSMountAbsent: host.XFSMountAbsent, ObservedAt: time.Now().UTC().Format(time.RFC3339Nano),
	}
	proof, err := rootfshandoff.NewMigrationRootFSDetachProof(request, observation)
	if err != nil {
		return zero, fmt.Errorf("migration writer remains attached: %w: %w", err, errdefs.ErrFailedPrecondition)
	}
	if err := proof.ValidateFor(stage, *current.Migration.Result, request); err != nil {
		return zero, err
	}
	current.Migration.DetachProof = &proof
	if err := m.save(current); err != nil {
		return zero, err
	}
	if err := m.releaseDeviceReservation(&current); err != nil {
		return zero, err
	}
	return proof, nil
}

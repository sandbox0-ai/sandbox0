package session

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/containerd/errdefs"
	"github.com/opencontainers/go-digest"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsblock"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshandoff"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/rootfswriterauthority"
	bolt "go.etcd.io/bbolt"
)

// LiveCommitAuthority keeps remote publication and local base installation
// separate. Neither acknowledgement changes the immutable launch binding.
type LiveCommitAuthority interface {
	PublishLiveRootFS(context.Context, rootfshandoff.StageRequest, LiveCommitPublication) (protocol.LiveCommitProgress, error)
	InstallLiveRootFS(context.Context, rootfshandoff.StageRequest, LiveCommitInstallation) (protocol.LiveCommitProgress, error)
	ObserveLiveCommit(context.Context, rootfshandoff.StageRequest) (protocol.LiveCommitObservation, error)
}

type LiveCommitPublication struct {
	OperationID                   string
	ExpectedCommittedGenerationID string
	BaseGenerationID              string
	CutSequence                   uint64
	Generation                    rootfshandoff.GenerationDescriptor
}

type LiveCommitInstallation struct {
	OperationID  string
	GenerationID string
	CutSequence  uint64
}

type liveCommitRecord struct {
	CommittedGenerationID string             `json:"committed_generation_id"`
	CommittedSequence     uint64             `json:"committed_sequence"`
	InstalledGenerationID string             `json:"installed_generation_id"`
	LastInstalledAt       string             `json:"last_installed_at,omitempty"`
	Pending               *pendingLiveCommit `json:"pending,omitempty"`
	ObsoleteBranch        *obsoleteBranch    `json:"obsolete_branch,omitempty"`
}

type obsoleteBranch struct {
	Path         string `json:"path"`
	SequenceBase uint64 `json:"sequence_base"`
}

type pendingLiveCommit struct {
	OperationID                   string                              `json:"operation_id"`
	ExpectedCommittedGenerationID string                              `json:"expected_committed_generation_id"`
	BaseGenerationID              string                              `json:"base_generation_id"`
	Cut                           rootfsblock.BranchCut               `json:"cut"`
	GenerationID                  string                              `json:"generation_id"`
	Generation                    *rootfshandoff.GenerationDescriptor `json:"generation,omitempty"`
	RemoteConfirmed               bool                                `json:"remote_confirmed,omitempty"`
}

func (m *Manager) requireLiveCommitSettled(current record) error {
	m.mu.Lock()
	busy := m.captures[current.Parent] || m.growing[current.Parent]
	m.mu.Unlock()
	if busy || current.LiveCommit != nil && (current.LiveCommit.Pending != nil || current.LiveCommit.ObsoleteBranch != nil) {
		return fmt.Errorf("RootFS checkpoint or live installation is pending: %w", errdefs.ErrUnavailable)
	}
	return nil
}

func sessionBranchPath(branchRoot, mountRoot, parent string, sequenceBase uint64) string {
	initial := sessionPaths(branchRoot, mountRoot, parent).branch
	if sequenceBase == 0 {
		return initial
	}
	return strings.TrimSuffix(initial, filepath.Ext(initial)) + "." + strconv.FormatUint(sequenceBase, 10) + filepath.Ext(initial)
}

func (m *Manager) branchPath(current record) string {
	return sessionBranchPath(m.branchRoot, m.mountRoot, current.Parent, current.BranchSequenceBase)
}

// CommitLive uploads a flushed prefix while guest I/O continues, then installs
// its acknowledged immutable base under the same NBD backend. Every external
// side effect is preceded by a durable intent and can be retried verbatim.
// The owner must invoke it regularly; it does not start an implicit goroutine.
func (m *Manager) CommitLive(ctx context.Context, stage rootfshandoff.StageRequest) error {
	return m.runLiveCommit(ctx, stage, true)
}

// SettleLiveCommit resolves existing publication/installation intent without
// starting a new cut. Lifecycle callers invoke it before taking storage custody.
func (m *Manager) SettleLiveCommit(ctx context.Context, stage rootfshandoff.StageRequest) error {
	return m.runLiveCommit(ctx, stage, false)
}

func (m *Manager) runLiveCommit(ctx context.Context, stage rootfshandoff.StageRequest, allowNewCut bool) error {
	stage = stage.WithoutWriterGrantToken()
	if err := stage.ValidateDurableBinding(); err != nil {
		return err
	}
	binding, err := stage.BindingDigest()
	if err != nil {
		return err
	}
	unlock := m.lock(stage.Parent)
	current, err := m.load(stage.Parent)
	if err != nil {
		unlock()
		return err
	}
	if !sameBinding(current, stage, hex.EncodeToString(binding[:])) || current.Stage == nil {
		unlock()
		return errdefs.ErrFailedPrecondition
	}
	if !allowNewCut && (current.LiveCommit == nil || current.LiveCommit.Pending == nil && current.LiveCommit.ObsoleteBranch == nil) {
		unlock()
		return nil
	}
	if m.liveCommitAuthority == nil {
		unlock()
		return fmt.Errorf("live commit authority is unavailable: %w", errdefs.ErrUnavailable)
	}
	m.mu.Lock()
	freeze, freezing := m.freezes[stage.Parent]
	freezeOwned := freezing && freeze.matches(current)
	if current.State != stateReady && !freezeOwned || current.Stage == nil ||
		(current.RunningForkRequest != nil || current.FreezeOperationID != "") && !freezeOwned ||
		freezeOwned && allowNewCut && !freeze.draining || current.Migration != nil && !freezeOwned || current.CrashFence != nil {
		m.mu.Unlock()
		unlock()
		return fmt.Errorf("RootFS writer cannot commit in its current custody: %w", errdefs.ErrUnavailable)
	}
	live := m.live[stage.Parent]
	if live == nil || m.liveCommitting[stage.Parent] || m.captures[stage.Parent] && !freezeOwned || m.closing || m.handoff {
		m.mu.Unlock()
		unlock()
		return errdefs.ErrUnavailable
	}
	select {
	case m.liveCommitSlots <- struct{}{}:
	default:
		m.mu.Unlock()
		unlock()
		return fmt.Errorf("live commit workers are occupied: %w", errdefs.ErrUnavailable)
	}
	m.captures[stage.Parent] = true
	m.liveCommitting[stage.Parent] = true
	done := make(chan struct{})
	m.liveCommitDone[stage.Parent] = done
	m.liveCommitWG.Add(1)
	m.mu.Unlock()
	unlock()
	defer func() {
		m.mu.Lock()
		if !freezeOwned {
			delete(m.captures, stage.Parent)
		}
		delete(m.liveCommitting, stage.Parent)
		delete(m.liveCommitDone, stage.Parent)
		close(done)
		<-m.liveCommitSlots
		m.mu.Unlock()
		m.liveCommitWG.Done()
	}()
	ctx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(m.lifetime, cancel)
	defer func() { stop(); cancel() }()
	return m.commitLive(ctx, current, live, allowNewCut)
}

func (m *Manager) commitLive(ctx context.Context, current record, live *liveSession, allowNewCut bool) error {
	stage := *current.Stage
	if current.LiveCommit == nil {
		current.LiveCommit = &liveCommitRecord{CommittedGenerationID: current.GenerationID, InstalledGenerationID: current.GenerationID}
		current.Version = sessionSchemaVersion
	}
	if err := m.validateLiveCommit(current); err != nil {
		return err
	}
	if live.branch.RotationFailure() != nil {
		if err := m.recoverLiveRotation(current, live); err != nil {
			return err
		}
	}
	if err := m.reapObsoleteLiveBranch(&current); err != nil {
		return err
	}
	cursor := current.LiveCommit
	if cursor.Pending == nil {
		if !allowNewCut {
			return nil
		}
		checkpoint, err := live.branch.Checkpoint()
		if err != nil {
			return err
		}
		cut := checkpoint.Cut()
		if err := checkpoint.Close(); err != nil {
			return err
		}
		if cut.Sequence <= cursor.CommittedSequence {
			return nil
		}
		if cut.Sequence > math.MaxInt64 {
			return fmt.Errorf("live cut exceeds regional sequence limit")
		}
		operation := protocol.LiveCommitOperationID(stage.Identity.WriterGrantID, cut.Sequence)
		identity := sha256.Sum256([]byte(current.BindingDigest + "\x00" + operation))
		cursor.Pending = &pendingLiveCommit{OperationID: operation, ExpectedCommittedGenerationID: cursor.CommittedGenerationID,
			BaseGenerationID: cursor.InstalledGenerationID, Cut: cut, GenerationID: "live-" + hex.EncodeToString(identity[:])}
		current.Version = sessionSchemaVersion
		if err := m.saveLiveCommit(current); err != nil {
			return err
		}
	}
	pending := cursor.Pending
	observation, err := m.liveCommitAuthority.ObserveLiveCommit(ctx, stage)
	if err != nil {
		return err
	}
	if err := observation.Validate(stage.Identity.WriterGrantID, stage.InitialGeneration); err != nil {
		return err
	}
	p := observation.Progress
	if p.CommittedSequence == pending.Cut.Sequence && p.CommittedGenerationID == pending.GenerationID && p.OperationID == pending.OperationID {
		previousInstalled := p.InstalledSequence == pending.Cut.Identity.SequenceBase && p.InstalledGenerationID == pending.BaseGenerationID
		currentInstalled := p.InstalledSequence == pending.Cut.Sequence && p.InstalledGenerationID == pending.GenerationID && current.BranchSequenceBase == pending.Cut.Sequence
		if !previousInstalled && !currentInstalled {
			return fmt.Errorf("regional installation differs from the durable WAL pointer: %w", errdefs.ErrFailedPrecondition)
		}
		if pending.Generation == nil {
			return fmt.Errorf("regional receipt has no durable local publication")
		}
		if (observation.WriterState == "consumed" || observation.WriterState == "retiring") && observation.CommittedDescriptorDigest != digest.FromBytes(pending.Generation.Descriptor).String() {
			return fmt.Errorf("regional receipt refers to another immutable descriptor: %w", errdefs.ErrFailedPrecondition)
		}
		pending.RemoteConfirmed = true
		cursor.CommittedGenerationID, cursor.CommittedSequence = p.CommittedGenerationID, p.CommittedSequence
		if err := m.saveLiveCommit(current); err != nil {
			return err
		}
	} else if pending.RemoteConfirmed || p.CommittedGenerationID != pending.ExpectedCommittedGenerationID || p.CommittedSequence != cursor.CommittedSequence || p.InstalledGenerationID != pending.BaseGenerationID || p.InstalledSequence != pending.Cut.Identity.SequenceBase {
		return fmt.Errorf("regional live cursor differs from durable local intent: %w", errdefs.ErrFailedPrecondition)
	} else if observation.WriterState == "retiring" {
		// The region fenced the pending cut before accepting it. All writes
		// still exist in the selected WAL; ordinary final sealing can proceed.
		cursor.Pending = nil
		return m.saveLiveCommit(current)
	}
	if observation.WriterState != "consumed" && observation.WriterState != "retiring" {
		return fmt.Errorf("live writer is terminal: %w", errdefs.ErrFailedPrecondition)
	}
	if !pending.RemoteConfirmed {
		if pending.Generation == nil {
			checkpoint, err := live.branch.CheckpointAtCut(pending.Cut)
			if err != nil {
				return err
			}
			base, err := rootfsblock.DecodeDescriptor(current.BaseDescriptor)
			if err != nil {
				_ = checkpoint.Close()
				return err
			}
			built, buildErr := rootfsblock.BuildIncrementalGenerationFromBlockReader(ctx, m.source, base, checkpoint, m.operationPublisher(stage, pending.OperationID), rootfsblock.BuildOptions{})
			if err := errors.Join(buildErr, checkpoint.Close()); err != nil {
				return err
			}
			pending.Generation = &rootfshandoff.GenerationDescriptor{Version: rootfshandoff.GenerationDescriptorVersion, GenerationID: pending.GenerationID,
				FilesystemID: current.RootFSID, SourceOCIDigest: stage.Generation.SourceOCIDigest, BaseArtifactDigest: stage.Generation.BaseArtifactDigest,
				BaseBlockRoot: stage.Generation.BaseBlockRoot, CurrentBlockHead: built.Descriptor.MappingRoot.RootDigest, WriterEpoch: current.WriterEpoch,
				FormatGeneration: stage.Generation.FormatGeneration, DurabilityState: rootfsblock.DurabilityS3, LocatorVersion: stage.Generation.LocatorVersion, Descriptor: built.Payload}
			if err := pending.Generation.Validate(); err != nil {
				return err
			}
			if err := m.saveLiveCommit(current); err != nil {
				return err
			}
		}
		progress, err := m.liveCommitAuthority.PublishLiveRootFS(ctx, stage, LiveCommitPublication{OperationID: pending.OperationID,
			ExpectedCommittedGenerationID: pending.ExpectedCommittedGenerationID, BaseGenerationID: pending.BaseGenerationID, CutSequence: pending.Cut.Sequence, Generation: *pending.Generation})
		if err != nil {
			return err
		}
		if progress.CommittedGenerationID != pending.GenerationID || progress.CommittedSequence != pending.Cut.Sequence || progress.OperationID != pending.OperationID ||
			progress.InstalledGenerationID != pending.BaseGenerationID || progress.InstalledSequence != pending.Cut.Identity.SequenceBase {
			return fmt.Errorf("live publication returned a different cut: %w", errdefs.ErrUnavailable)
		}
		pending.RemoteConfirmed = true
		cursor.CommittedGenerationID, cursor.CommittedSequence = pending.GenerationID, pending.Cut.Sequence
		if err := m.saveLiveCommit(current); err != nil {
			return err
		}
	}
	if current.BranchSequenceBase != pending.Cut.Sequence {
		if observation.WriterState == "retiring" {
			// Accepted but uninstalled cuts need no local rotation for final
			// sealing: its complete WAL and prior installed base remain intact.
			cursor.Pending = nil
			return m.saveLiveCommit(current)
		}
		if err := m.installLiveRotation(&current, live); err != nil {
			var capacity *rootfsblock.DirtyTailCapacityError
			if errors.As(err, &capacity) && live.branch.RotationFailure() == nil && current.BranchSequenceBase != pending.Cut.Sequence {
				// Keep the acknowledged remote cursor and complete selected WAL.
				// The next attempt can cut a newer prefix with a smaller suffix;
				// at admission pressure that suffix can be empty.
				current.LiveCommit.Pending = nil
				return errors.Join(err, m.saveLiveCommit(current))
			}
			return err
		}
	}
	progress, err := m.liveCommitAuthority.InstallLiveRootFS(ctx, stage, LiveCommitInstallation{OperationID: pending.OperationID, GenerationID: pending.GenerationID, CutSequence: pending.Cut.Sequence})
	if err != nil {
		return err
	}
	if progress.OperationID != pending.OperationID || progress.CommittedGenerationID != pending.GenerationID || progress.CommittedSequence != pending.Cut.Sequence || progress.InstalledGenerationID != pending.GenerationID || progress.InstalledSequence != pending.Cut.Sequence {
		return fmt.Errorf("live installation returned a different cut: %w", errdefs.ErrUnavailable)
	}
	if err := m.reapObsoleteLiveBranch(&current); err != nil {
		return err
	}
	current.LiveCommit.Pending = nil
	current.LiveCommit.LastInstalledAt = time.Now().UTC().Format(time.RFC3339Nano)
	return m.saveLiveCommit(current)
}

func (m *Manager) installLiveRotation(current *record, live *liveSession) error {
	unlock := m.lock(current.Parent)
	defer unlock()
	pending := current.LiveCommit.Pending
	descriptor, err := rootfsblock.DecodeDescriptor(pending.Generation.Descriptor)
	if err != nil {
		return err
	}
	reader, err := rootfsblock.NewReaderWithCacheContext(m.lifetime, m.source, descriptor, m.readCache)
	if err != nil {
		return err
	}
	destination := sessionBranchPath(m.branchRoot, m.mountRoot, current.Parent, pending.Cut.Sequence)
	// A previously ambiguous callback selected the old WAL. Its inactive
	// successor is disposable only after that exact durable selection is read.
	if _, err := os.Lstat(destination); err == nil {
		if err := m.removeInactiveLiveBranch(destination); err != nil {
			return err
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	err = live.branch.RotateCommitted(pending.Cut, reader, descriptor.MappingRoot.RootDigest, destination, func(identity rootfsblock.BranchIdentity) error {
		next := *current
		nextCursor := *current.LiveCommit
		next.LiveCommit = &nextCursor
		nextCursor.ObsoleteBranch = &obsoleteBranch{Path: current.BranchPath, SequenceBase: current.BranchSequenceBase}
		next.BaseDescriptor = append([]byte(nil), pending.Generation.Descriptor...)
		next.BranchPath, next.BranchSequenceBase = destination, identity.SequenceBase
		nextCursor.InstalledGenerationID = pending.GenerationID
		return m.saveLiveCommitLocked(next)
	})
	// Always reload: a failed fsync/transaction acknowledgement may have
	// selected either record. Never infer durable installation from an error.
	durable, loadErr := m.load(current.Parent)
	if loadErr != nil {
		return errors.Join(err, loadErr)
	}
	*current = durable
	if live.branch.RotationFailure() != nil {
		if recoveryErr := m.recoverLiveRotation(durable, live); recoveryErr != nil {
			return errors.Join(err, recoveryErr)
		}
	}
	if current.BranchSequenceBase == pending.Cut.Sequence && live.branch.Identity().SequenceBase == pending.Cut.Sequence {
		return nil // Reap outside this parent lock after the installation ack.
	}
	return err
}

func (m *Manager) recoverLiveRotation(current record, live *liveSession) error {
	if err := m.validateLiveCommit(current); err != nil {
		return err
	}
	descriptor, err := rootfsblock.DecodeDescriptor(current.BaseDescriptor)
	if err != nil {
		return err
	}
	reader, err := rootfsblock.NewReaderWithCacheContext(m.lifetime, m.source, descriptor, m.readCache)
	if err != nil {
		return err
	}
	return live.branch.RecoverRotation(current.BranchPath, rootfsblock.BranchIdentity{Version: rootfsblock.BranchFormatVersion, RootFSID: current.RootFSID,
		GenerationID: current.GenerationID, WriterEpoch: current.WriterEpoch, LogicalSizeBytes: descriptor.LogicalSizeBytes,
		BaseRootDigest: descriptor.MappingRoot.RootDigest, SequenceBase: current.BranchSequenceBase}, reader)
}

func (m *Manager) removeInactiveLiveBranch(path string) error {
	if err := m.nodeDirty.ValidateOwnerDetached(path); err != nil {
		return err
	}
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err := syncTerminalDirectory(filepath.Dir(path)); err != nil {
		return err
	}
	return m.nodeDirty.ReleaseOwner(path)
}

func (m *Manager) reapObsoleteLiveBranch(current *record) error {
	if current.LiveCommit == nil || current.LiveCommit.ObsoleteBranch == nil {
		return nil
	}
	old := current.LiveCommit.ObsoleteBranch
	if old.SequenceBase >= current.BranchSequenceBase || old.Path != sessionBranchPath(m.branchRoot, m.mountRoot, current.Parent, old.SequenceBase) {
		return errdefs.ErrFailedPrecondition
	}
	if err := m.removeInactiveLiveBranch(old.Path); err != nil {
		return err
	}
	current.LiveCommit.ObsoleteBranch = nil
	return m.saveLiveCommit(*current)
}

func (m *Manager) validateLiveCommit(current record) error {
	if current.BranchPath != m.branchPath(current) {
		return fmt.Errorf("live branch path differs from its durable sequence")
	}
	c := current.LiveCommit
	if c == nil {
		if current.BranchSequenceBase != 0 {
			return errdefs.ErrFailedPrecondition
		}
		return nil
	}
	if current.Version != sessionSchemaVersion || current.Stage == nil || c.CommittedGenerationID == "" || c.InstalledGenerationID == "" || current.BranchSequenceBase > c.CommittedSequence || c.CommittedSequence > math.MaxInt64 ||
		current.BranchSequenceBase == 0 && c.InstalledGenerationID != current.GenerationID || c.CommittedSequence == 0 && c.CommittedGenerationID != current.GenerationID {
		return errdefs.ErrFailedPrecondition
	}
	p := c.Pending
	if old := c.ObsoleteBranch; old != nil && (old.SequenceBase >= current.BranchSequenceBase || old.Path != sessionBranchPath(m.branchRoot, m.mountRoot, current.Parent, old.SequenceBase)) {
		return errdefs.ErrFailedPrecondition
	}
	if p == nil {
		return nil
	}
	sequence, err := protocol.LiveCommitSequence(current.Stage.Identity.WriterGrantID, p.OperationID)
	if err != nil || sequence != p.Cut.Sequence || p.GenerationID == "" || p.ExpectedCommittedGenerationID == "" || p.BaseGenerationID == "" || p.Cut.Identity.RootFSID != current.RootFSID || p.Cut.Identity.GenerationID != current.GenerationID || p.Cut.Identity.WriterEpoch != current.WriterEpoch || p.Cut.Sequence <= p.Cut.Identity.SequenceBase || p.Cut.Identity.Validate() != nil {
		return errdefs.ErrFailedPrecondition
	}
	if p.RemoteConfirmed && (p.Generation == nil || c.CommittedGenerationID != p.GenerationID || c.CommittedSequence != p.Cut.Sequence) {
		return errdefs.ErrFailedPrecondition
	}
	if current.BranchSequenceBase == p.Cut.Sequence {
		if !p.RemoteConfirmed || c.InstalledGenerationID != p.GenerationID || !bytes.Equal(current.BaseDescriptor, p.Generation.Descriptor) {
			return errdefs.ErrFailedPrecondition
		}
	} else {
		base, err := rootfsblock.DecodeDescriptor(current.BaseDescriptor)
		if err != nil || current.BranchSequenceBase != p.Cut.Identity.SequenceBase || c.InstalledGenerationID != p.BaseGenerationID ||
			base.MappingRoot.RootDigest != p.Cut.Identity.BaseRootDigest || base.LogicalSizeBytes != p.Cut.Identity.LogicalSizeBytes {
			return errdefs.ErrFailedPrecondition
		}
	}
	if p.Generation != nil {
		launch := current.Stage.Generation
		if launch == nil || p.Generation.GenerationID != p.GenerationID || p.Generation.FilesystemID != current.RootFSID || p.Generation.WriterEpoch != current.WriterEpoch || p.Generation.Validate() != nil ||
			p.Generation.SourceOCIDigest != launch.SourceOCIDigest || p.Generation.BaseArtifactDigest != launch.BaseArtifactDigest || p.Generation.BaseBlockRoot != launch.BaseBlockRoot || p.Generation.FormatGeneration != launch.FormatGeneration || p.Generation.DurabilityState != rootfsblock.DurabilityS3 {
			return errdefs.ErrFailedPrecondition
		}
		base, err := rootfsblock.DecodeDescriptor(p.Generation.Descriptor)
		if err != nil || base.LogicalSizeBytes != p.Cut.Identity.LogicalSizeBytes {
			return errdefs.ErrFailedPrecondition
		}
	}
	return nil
}

// Only explicitly journaled paths with the exact parent/sequence naming rule
// may be reclaimed. A terminal proof for one session never authorizes a glob.
func (m *Manager) ownedSessionBranches(current record) ([]string, error) {
	if err := m.validateLiveCommit(current); err != nil {
		return nil, err
	}
	paths := []string{current.BranchPath}
	if current.LiveCommit != nil {
		if old := current.LiveCommit.ObsoleteBranch; old != nil {
			paths = append(paths, old.Path)
		}
		if pending := current.LiveCommit.Pending; pending != nil {
			candidate := sessionBranchPath(m.branchRoot, m.mountRoot, current.Parent, pending.Cut.Sequence)
			if candidate != current.BranchPath {
				paths = append(paths, candidate)
			}
		}
	}
	return paths, nil
}

func (m *Manager) saveLiveCommit(current record) error {
	unlock := m.lock(current.Parent)
	defer unlock()
	return m.saveLiveCommitLocked(current)
}

// Merge only storage progress in the write transaction. Consumer lease renewals
// and pressure observations may legitimately arrive during the remote upload.
func (m *Manager) saveLiveCommitLocked(current record) error {
	if err := m.validateLiveCommit(current); err != nil {
		return err
	}
	m.mu.Lock()
	freeze := m.freezes[current.Parent]
	m.mu.Unlock()
	err := m.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(sessionBucket)
		payload := bucket.Get([]byte(current.Parent))
		if payload == nil {
			return errdefs.ErrNotFound
		}
		var stored record
		if err := json.Unmarshal(payload, &stored); err != nil {
			return err
		}
		if stored.Parent != current.Parent || stored.RootFSID != current.RootFSID || stored.WriterEpoch != current.WriterEpoch ||
			stored.BindingDigest != current.BindingDigest || stored.GenerationID != current.GenerationID || stored.State != stateReady && !freeze.matches(stored) ||
			stored.Stage == nil || stored.Migration != nil && !freeze.matches(stored) || stored.CrashFence != nil ||
			(stored.RunningForkRequest != nil || stored.FreezeOperationID != "") && !freeze.matches(stored) {
			return fmt.Errorf("RootFS custody changed during live publication: %w", errdefs.ErrFailedPrecondition)
		}
		if current.BranchSequenceBase > stored.BranchSequenceBase {
			stored.DirtyTailPressure = nil
		}
		stored.Version, stored.LiveCommit = current.Version, current.LiveCommit
		stored.BaseDescriptor, stored.BranchPath, stored.BranchSequenceBase = current.BaseDescriptor, current.BranchPath, current.BranchSequenceBase
		stored.UpdatedAt = time.Now().UTC().Format(time.RFC3339Nano)
		return putRecord(bucket, stored)
	})
	if err == nil {
		m.setRecoveryIndex(current.Parent, time.Time{}, false)
	}
	return err
}

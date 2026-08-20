package session

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/containerd/errdefs"
	"github.com/opencontainers/go-digest"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsblock"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsrebase"
	bolt "go.etcd.io/bbolt"
)

const (
	rebaseRecordVersion          = 1
	rebaseStatePreparing         = "preparing"
	rebaseStateApplying          = "applying"
	rebaseStateBuilding          = "building"
	rebaseStateResult            = "result"
	defaultRebaseCleanupTimeout  = 30 * time.Second
	rebaseAcknowledgementVersion = 1
	rebaseAcknowledgementBytes   = 1 + 8 + sha256.Size + sha256.Size
	maxRebaseAcknowledgements    = 4096
	rebaseAcknowledgementTTL     = 24 * time.Hour
)

var rebaseRoles = [...]string{"old", "source", "target"}

type rebaseRecord struct {
	Version       int                        `json:"version"`
	OperationID   string                     `json:"operation_id"`
	RequestDigest string                     `json:"request_digest"`
	Request       rootfsrebase.WorkerRequest `json:"request"`
	State         string                     `json:"state"`
	Resources     []rebaseResource           `json:"resources"`
	Result        *rootfsrebase.WorkerResult `json:"result,omitempty"`
	Clean         bool                       `json:"clean,omitempty"`
	Failure       string                     `json:"failure,omitempty"`
	CreatedAt     string                     `json:"created_at"`
	UpdatedAt     string                     `json:"updated_at"`
}

type rebaseResource struct {
	Role                      string `json:"role"`
	BranchPath                string `json:"branch_path"`
	XFSRoot                   string `json:"xfs_root"`
	MergedRoot                string `json:"merged_root"`
	DeviceAllocationID        string `json:"device_allocation_id"`
	DevicePath                string `json:"device_path,omitempty"`
	XFSMountIntent            bool   `json:"xfs_mount_intent,omitempty"`
	OverlayMountIntent        bool   `json:"overlay_mount_intent,omitempty"`
	DeviceClosed              bool   `json:"device_closed,omitempty"`
	DeviceReservationReleased bool   `json:"device_reservation_released,omitempty"`
}

type rebaseAcknowledgement struct {
	AcknowledgedAt int64
	RequestDigest  [sha256.Size]byte
	ProofDigest    [sha256.Size]byte
}

type liveRebaseResources struct {
	branches   map[string]*rootfsblock.Branch
	devices    map[string]Device
	checkpoint *rootfsblock.BranchCheckpoint
}

type filesystemRebaseEngine struct{}

func (filesystemRebaseEngine) Apply(
	ctx context.Context,
	request rootfsrebase.WorkerRequest,
	oldRoot, sourceRoot, targetRoot string,
	dirtyBlocks []uint64,
) (*rootfsrebase.ApplyResult, error) {
	lineageID := request.FilesystemID + ":" + request.SourceBaseArtifactDigest
	oldManifest, err := rootfsrebase.ScanWithOptions(oldRoot, rootfsrebase.ScanOptions{LineageID: lineageID})
	if err != nil {
		return nil, fmt.Errorf("scan old RootFS: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	sourceManifest, err := rootfsrebase.ScanWithOptions(sourceRoot, rootfsrebase.ScanOptions{LineageID: lineageID})
	if err != nil {
		return nil, fmt.Errorf("scan source RootFS: %w", err)
	}
	dirtyRanges, err := rootfsrebase.DirtyFileRanges(
		*sourceManifest, dirtyBlocks, rootfsblock.LogicalBlockSize,
	)
	if err != nil {
		return nil, fmt.Errorf("attribute source dirty blocks: %w", err)
	}
	diff, err := rootfsrebase.Diff(*oldManifest, *sourceManifest, dirtyRanges)
	if err != nil {
		return nil, fmt.Errorf("diff source RootFS: %w", err)
	}
	result, err := rootfsrebase.Apply(ctx, rootfsrebase.ApplyRequest{
		OldRoot: oldRoot, SourceRoot: sourceRoot, TargetRoot: targetRoot,
		Old: *oldManifest, Source: *sourceManifest, Diff: *diff,
	})
	if err != nil {
		return nil, err
	}
	if err := result.Validate(); err != nil {
		return nil, fmt.Errorf("validate RootFS rebase apply result: %w", err)
	}
	return result, nil
}

// ExecuteRebase mounts three isolated branches, applies the file-aware merge,
// publishes an S3-materialized target generation, and durably caches the exact
// result until the regional authority acknowledges it.
func (m *Manager) ExecuteRebase(
	ctx context.Context,
	request rootfsrebase.WorkerRequest,
) (result rootfsrebase.WorkerResult, returnErr error) {
	if err := request.Validate(); err != nil {
		return result, fmt.Errorf("validate RootFS rebase request: %w: %w", err, errdefs.ErrInvalidArgument)
	}
	requestDigest, err := request.Digest()
	if err != nil {
		return result, err
	}
	requestDigestBytes, _ := parseRebaseSHA256Digest(requestDigest)
	m.mu.Lock()
	if m.closing {
		m.mu.Unlock()
		return result, fmt.Errorf("RootFS session manager is closing: %w", errdefs.ErrUnavailable)
	}
	m.rebaseWG.Add(1)
	m.mu.Unlock()
	defer m.rebaseWG.Done()
	select {
	case m.rebaseAdmission <- struct{}{}:
		defer func() { <-m.rebaseAdmission }()
	case <-ctx.Done():
		return result, ctx.Err()
	case <-m.lifetime.Done():
		return result, fmt.Errorf("RootFS session manager stopped: %w", errdefs.ErrUnavailable)
	}
	operationCtx, operationCancel := context.WithCancel(ctx)
	stopLifetimeCancel := context.AfterFunc(m.lifetime, operationCancel)
	defer func() {
		stopLifetimeCancel()
		operationCancel()
	}()

	unlock := m.lock("rebase:" + request.OperationID)
	defer unlock()
	current, err := m.loadRebase(request.OperationID)
	if err == nil {
		if current.RequestDigest != requestDigest {
			return result, fmt.Errorf("rebase operation is bound to another request: %w", errdefs.ErrAlreadyExists)
		}
		if current.Result != nil {
			if !current.Clean {
				if err := m.cleanupRebaseRecord(operationCtx, &current, nil); err != nil {
					return result, err
				}
			}
			cached := cloneRebaseResult(*current.Result)
			if err := cached.ValidateFor(request); err != nil {
				return result, fmt.Errorf("cached RootFS rebase result is invalid: %w", errdefs.ErrFailedPrecondition)
			}
			return cached, nil
		}
		if err := m.cleanupRebaseRecord(operationCtx, &current, nil); err != nil {
			return result, fmt.Errorf("recover interrupted RootFS rebase: %w", err)
		}
		if err := m.deleteRebase(request.OperationID, requestDigest); err != nil {
			return result, err
		}
	} else if !errdefs.IsNotFound(err) {
		return result, err
	} else if acknowledged, ackErr := m.loadRebaseAcknowledgement(request.OperationID); ackErr == nil {
		if acknowledged.RequestDigest != requestDigestBytes {
			return result, fmt.Errorf("rebase operation is bound to another acknowledged request: %w", errdefs.ErrAlreadyExists)
		}
		return result, fmt.Errorf("rebase operation was already acknowledged: %w", errdefs.ErrFailedPrecondition)
	} else if !errdefs.IsNotFound(ackErr) {
		return result, ackErr
	}
	rollbackDeadline, _ := time.Parse(time.RFC3339Nano, request.RollbackExpiresAt)
	now := time.Now()
	if !rollbackDeadline.After(now) {
		return result, fmt.Errorf("RootFS rebase rollback deadline expired: %w", errdefs.ErrFailedPrecondition)
	}
	if rollbackDeadline.After(now.Add(rootfsrebase.MaxWorkerRollbackRetention)) {
		return result, fmt.Errorf("RootFS rebase rollback retention exceeds node policy: %w", errdefs.ErrInvalidArgument)
	}
	workCtx, workCancel := context.WithDeadline(operationCtx, rollbackDeadline)
	defer workCancel()
	current = newRebaseRecord(m.branchRoot, m.mountRoot, request, requestDigest)
	if err := m.saveNewRebase(current); err != nil {
		return result, err
	}
	live := &liveRebaseResources{
		branches: make(map[string]*rootfsblock.Branch), devices: make(map[string]Device),
	}
	defer func() {
		if returnErr == nil {
			return
		}
		cleanupCtx, cancel := context.WithTimeout(context.Background(), defaultRebaseCleanupTimeout)
		defer cancel()
		cleanupErr := m.cleanupRebaseRecord(cleanupCtx, &current, live)
		if cleanupErr == nil && current.Result == nil {
			cleanupErr = m.deleteRebase(current.OperationID, current.RequestDigest)
		}
		returnErr = errors.Join(returnErr, cleanupErr)
	}()

	oldBase, _ := rootfsblock.DecodeDescriptor(request.SourceBaseDescriptor)
	sourceGeneration, _ := rootfsblock.DecodeDescriptor(request.SourceGenerationDescriptor)
	targetBase, _ := rootfsblock.DecodeDescriptor(request.TargetBaseDescriptor)
	dirtyBlocks, err := rootfsblock.ChangedBlocks(
		workCtx, m.source, oldBase, sourceGeneration, request.MaxChangedBlocks,
	)
	if err != nil {
		return result, fmt.Errorf("derive RootFS rebase dirty blocks: %w", err)
	}
	descriptors := map[string]rootfsblock.Descriptor{
		"old": oldBase, "source": sourceGeneration, "target": targetBase,
	}
	for index := range current.Resources {
		resource := &current.Resources[index]
		reader, err := rootfsblock.NewReaderWithCache(m.source, descriptors[resource.Role], m.readCache)
		if err != nil {
			return result, fmt.Errorf("open %s rebase generation: %w", resource.Role, err)
		}
		rootFSID, generationID := rebaseBranchIdentity(request, resource.Role)
		branch, err := rootfsblock.OpenBranchWithOptions(resource.BranchPath, rootfsblock.BranchIdentity{
			Version:          rootfsblock.BranchFormatVersion,
			RootFSID:         rootFSID,
			GenerationID:     generationID,
			WriterEpoch:      request.TargetWriterEpoch,
			LogicalSizeBytes: reader.Size(), BaseRootDigest: descriptors[resource.Role].MappingRoot.RootDigest,
		}, reader, m.branchOptions())
		if err != nil {
			return result, fmt.Errorf("open %s rebase branch: %w", resource.Role, err)
		}
		live.branches[resource.Role] = branch
		devicePath, err := m.runtime.ReserveDevice(resource.DeviceAllocationID)
		if err != nil {
			return result, fmt.Errorf("reserve %s rebase NBD device: %w", resource.Role, err)
		}
		resource.DevicePath = devicePath
		if err := m.saveRebase(current); err != nil {
			return result, err
		}
		device, err := m.runtime.AttachDevice(m.lifetime, workCtx, devicePath, resource.DeviceAllocationID, branch)
		if err != nil {
			return result, fmt.Errorf("attach %s rebase NBD device: %w", resource.Role, err)
		}
		if device.Path() != devicePath {
			_ = device.Close()
			return result, fmt.Errorf("%s rebase NBD path changed after attach", resource.Role)
		}
		live.devices[resource.Role] = device
		resource.XFSMountIntent = true
		if err := m.saveRebase(current); err != nil {
			return result, err
		}
		if err := m.runtime.MountXFS(devicePath, resource.XFSRoot); err != nil {
			return result, fmt.Errorf("mount %s rebase XFS: %w", resource.Role, err)
		}
		resource.OverlayMountIntent = true
		if err := m.saveRebase(current); err != nil {
			return result, err
		}
		if err := m.runtime.MountOverlay(resource.XFSRoot, resource.MergedRoot); err != nil {
			return result, fmt.Errorf("mount %s rebase OverlayFS: %w", resource.Role, err)
		}
	}
	current.State = rebaseStateApplying
	if err := m.saveRebase(current); err != nil {
		return result, err
	}
	oldResource, sourceResource, targetResource := current.resource("old"), current.resource("source"), current.resource("target")
	if oldResource == nil || sourceResource == nil || targetResource == nil {
		return result, fmt.Errorf("RootFS rebase journal lacks a required resource")
	}
	applyResult, err := m.rebaseEngine.Apply(
		workCtx, request, oldResource.MergedRoot, sourceResource.MergedRoot,
		targetResource.MergedRoot, dirtyBlocks,
	)
	if err != nil {
		return result, fmt.Errorf("apply RootFS rebase: %w", err)
	}
	if applyResult == nil || applyResult.Validate() != nil {
		return result, fmt.Errorf("RootFS rebase engine returned an invalid result")
	}
	if err := m.unmountRebaseMounts(&current, true); err != nil {
		return result, err
	}
	targetBranch := live.branches["target"]
	live.checkpoint, err = targetBranch.Checkpoint()
	if err != nil {
		return result, fmt.Errorf("checkpoint rebased target branch: %w", err)
	}
	if err := m.closeRebaseDevicesAndBranches(workCtx, &current, live); err != nil {
		return result, err
	}
	current.State = rebaseStateBuilding
	if err := m.saveRebase(current); err != nil {
		return result, err
	}
	built, err := rootfsblock.BuildIncrementalGenerationFromBlockReader(
		workCtx, m.source, targetBase, live.checkpoint, m.publisher, rootfsblock.BuildOptions{},
	)
	if err != nil {
		return result, fmt.Errorf("publish rebased target generation: %w", err)
	}
	if err := live.checkpoint.Close(); err != nil {
		return result, fmt.Errorf("close rebased target checkpoint: %w", err)
	}
	live.checkpoint = nil
	health, err := applyResult.HealthProofBytes()
	if err != nil {
		return result, err
	}
	result = rootfsrebase.WorkerResult{
		Version: rootfsrebase.WorkerProtocolVersion, RequestDigest: requestDigest,
		GenerationID: request.TargetGenerationID, FilesystemID: request.FilesystemID,
		ParentGenerationID: request.SourceGenerationID, SourceOCIDigest: request.TargetSourceOCIDigest,
		BaseArtifactDigest: request.TargetBaseArtifactDigest, BaseBlockRoot: request.TargetBaseBlockRoot,
		CurrentBlockHead: built.Descriptor.MappingRoot.RootDigest, WriterEpoch: request.TargetWriterEpoch,
		FormatGeneration: request.TargetFormatGeneration, DurabilityState: rootfsblock.DurabilityS3,
		LocatorVersion: request.SourceLocatorVersion + 1, Descriptor: built.Payload,
		HealthCheckDigest: health, DirtyBlocks: len(dirtyBlocks),
		PublishedObjects: built.Objects, PublishedBytes: built.Bytes, Apply: *applyResult,
	}
	if err := result.SealProof(); err != nil {
		return result, err
	}
	if err := result.ValidateFor(request); err != nil {
		return result, err
	}
	current.Result = ptrRebaseResult(result)
	current.State = rebaseStateResult
	if err := m.saveRebase(current); err != nil {
		return result, err
	}
	if err := m.removeRebaseArtifacts(&current); err != nil {
		return result, err
	}
	current.Clean = true
	current.Failure = ""
	if err := m.saveRebase(current); err != nil {
		return result, err
	}
	return cloneRebaseResult(result), nil
}

// ReconcileRebases removes interrupted unpublished branches and physical
// attachments while preserving exact completed results for regional replay.
func (m *Manager) ReconcileRebases(ctx context.Context) error {
	if err := m.reconcileDeviceReservations(); err != nil {
		return fmt.Errorf("reconcile RootFS device reservations before rebases: %w", err)
	}
	if err := m.pruneRebaseAcknowledgements(time.Now().UTC()); err != nil {
		return fmt.Errorf("prune RootFS rebase acknowledgements: %w", err)
	}
	var operationIDs []string
	if err := m.db.View(func(tx *bolt.Tx) error {
		return tx.Bucket(rebaseBucket).ForEach(func(key, payload []byte) error {
			if payload != nil {
				operationIDs = append(operationIDs, string(key))
			}
			return nil
		})
	}); err != nil {
		return err
	}
	var result error
	for _, operationID := range operationIDs {
		if err := ctx.Err(); err != nil {
			return errors.Join(result, err)
		}
		unlock := m.lock("rebase:" + operationID)
		current, err := m.loadRebase(operationID)
		if err == nil && !current.Clean {
			err = m.cleanupRebaseRecord(ctx, &current, nil)
		}
		if err == nil && current.Result == nil {
			err = m.deleteRebase(current.OperationID, current.RequestDigest)
		}
		unlock()
		if err != nil {
			result = errors.Join(result, fmt.Errorf("reconcile RootFS rebase %q: %w", operationID, err))
		}
	}
	return result
}

// RejectRebase serializes with execution and returns a durable exact proof.
// If execution already produced an output, the output remains cached until
// the regional authority persists the rejection and acknowledges its proof.
func (m *Manager) RejectRebase(
	ctx context.Context,
	request rootfsrebase.WorkerRequest,
) (rootfsrebase.WorkerRejection, error) {
	if err := request.Validate(); err != nil {
		return rootfsrebase.WorkerRejection{},
			fmt.Errorf("validate RootFS rebase rejection: %w: %w", err, errdefs.ErrInvalidArgument)
	}
	requestDigest, err := request.Digest()
	if err != nil {
		return rootfsrebase.WorkerRejection{}, err
	}
	m.mu.Lock()
	if m.closing {
		m.mu.Unlock()
		return rootfsrebase.WorkerRejection{},
			fmt.Errorf("RootFS session manager is closing: %w", errdefs.ErrUnavailable)
	}
	m.rebaseWG.Add(1)
	m.mu.Unlock()
	defer m.rebaseWG.Done()

	unlock := m.lock("rebase:" + request.OperationID)
	defer unlock()
	if err := ctx.Err(); err != nil {
		return rootfsrebase.WorkerRejection{}, err
	}
	current, err := m.loadRebase(request.OperationID)
	if err == nil {
		if current.RequestDigest != requestDigest {
			return rootfsrebase.WorkerRejection{},
				fmt.Errorf("rebase operation is bound to another request: %w", errdefs.ErrAlreadyExists)
		}
		if !current.Clean {
			if err := m.cleanupRebaseRecord(ctx, &current, nil); err != nil {
				return rootfsrebase.WorkerRejection{}, err
			}
		}
		if current.Result != nil {
			return rootfsrebase.RejectWithResult(request, *current.Result)
		}
		rejection, err := rootfsrebase.RejectWithoutResult(request)
		if err != nil {
			return rootfsrebase.WorkerRejection{}, err
		}
		if err := m.commitRebaseRejection(current, rejection.ProofDigest, time.Now().UTC()); err != nil {
			return rootfsrebase.WorkerRejection{}, err
		}
		return rejection, nil
	}
	if !errdefs.IsNotFound(err) {
		return rootfsrebase.WorkerRejection{}, err
	}
	rejection, err := rootfsrebase.RejectWithoutResult(request)
	if err != nil {
		return rootfsrebase.WorkerRejection{}, err
	}
	acknowledged, ackErr := m.loadRebaseAcknowledgement(request.OperationID)
	if ackErr == nil {
		requestBytes, _ := parseRebaseSHA256Digest(requestDigest)
		proofBytes, _ := parseRebaseSHA256Digest(rejection.ProofDigest)
		if acknowledged.RequestDigest != requestBytes || acknowledged.ProofDigest != proofBytes {
			return rootfsrebase.WorkerRejection{},
				fmt.Errorf("rebase operation already has another permanent outcome: %w", errdefs.ErrFailedPrecondition)
		}
		return rejection, nil
	}
	if !errdefs.IsNotFound(ackErr) {
		return rootfsrebase.WorkerRejection{}, ackErr
	}
	if err := m.commitAbsentRebaseRejection(request, rejection.ProofDigest, time.Now().UTC()); err != nil {
		return rootfsrebase.WorkerRejection{}, err
	}
	return rejection, nil
}

// AcknowledgeRebase removes one clean cached result only after the regional
// controller has either committed it or permanently rejected the operation.
func (m *Manager) AcknowledgeRebase(request rootfsrebase.WorkerRequest, proofDigest string) error {
	if err := request.Validate(); err != nil {
		return fmt.Errorf("validate rebase acknowledgement request: %w: %w", err, errdefs.ErrInvalidArgument)
	}
	requestDigest, err := request.Digest()
	if err != nil {
		return err
	}
	operationID := request.OperationID
	proofDigest = strings.TrimSpace(proofDigest)
	if proofDigest == "" {
		return fmt.Errorf("rebase operation and proof digest are required: %w", errdefs.ErrInvalidArgument)
	}
	requestDigestBytes, err := parseRebaseSHA256Digest(requestDigest)
	if err != nil {
		return err
	}
	proofDigestBytes, err := parseRebaseSHA256Digest(proofDigest)
	if err != nil {
		return fmt.Errorf("rebase proof digest is invalid: %w: %w", err, errdefs.ErrInvalidArgument)
	}
	unlock := m.lock("rebase:" + operationID)
	defer unlock()
	current, err := m.loadRebase(operationID)
	if err != nil {
		if errdefs.IsNotFound(err) {
			acknowledged, ackErr := m.loadRebaseAcknowledgement(operationID)
			if ackErr != nil {
				if errdefs.IsNotFound(ackErr) {
					return nil
				}
				return ackErr
			}
			if acknowledged.RequestDigest != requestDigestBytes || acknowledged.ProofDigest != proofDigestBytes {
				return fmt.Errorf("rebase acknowledgement does not match cached tombstone: %w", errdefs.ErrFailedPrecondition)
			}
			return nil
		}
		return err
	}
	if current.RequestDigest != requestDigest || current.Result == nil || !current.Clean ||
		current.Result.ProofDigest != proofDigest {
		return fmt.Errorf("rebase result proof does not match cached output: %w", errdefs.ErrFailedPrecondition)
	}
	return m.commitRebaseAcknowledgement(current, proofDigest, time.Now().UTC())
}

func (m *Manager) cleanupRebaseRecord(
	ctx context.Context,
	current *rebaseRecord,
	live *liveRebaseResources,
) error {
	if current == nil {
		return nil
	}
	var result error
	result = errors.Join(result, m.unmountRebaseMounts(current, false))
	result = errors.Join(result, m.closeRebaseDevicesAndBranches(ctx, current, live))
	if live != nil && live.checkpoint != nil {
		result = errors.Join(result, live.checkpoint.Close())
		live.checkpoint = nil
	}
	if result != nil {
		current.Failure = result.Error()
		_ = m.saveRebase(*current)
		return result
	}
	if err := m.removeRebaseArtifacts(current); err != nil {
		current.Failure = err.Error()
		_ = m.saveRebase(*current)
		return err
	}
	current.Clean = true
	current.Failure = ""
	return m.saveRebase(*current)
}

func (m *Manager) unmountRebaseMounts(current *rebaseRecord, syncTarget bool) error {
	var result error
	for index := len(current.Resources) - 1; index >= 0; index-- {
		resource := &current.Resources[index]
		overlayDetached := !resource.OverlayMountIntent
		if resource.OverlayMountIntent {
			err := m.runtime.UnmountOverlay(resource.MergedRoot, syncTarget && resource.Role == "target")
			if err == nil {
				overlayDetached = true
				resource.OverlayMountIntent = false
				err = m.saveRebase(*current)
			}
			result = errors.Join(result, err)
		}
		if resource.XFSMountIntent && overlayDetached {
			err := m.runtime.UnmountXFS(resource.XFSRoot, syncTarget && resource.Role == "target")
			if err == nil {
				resource.XFSMountIntent = false
				err = m.saveRebase(*current)
			}
			result = errors.Join(result, err)
		}
	}
	if result != nil {
		return fmt.Errorf("unmount RootFS rebase filesystems: %w", result)
	}
	return nil
}

func (m *Manager) closeRebaseDevicesAndBranches(
	ctx context.Context,
	current *rebaseRecord,
	live *liveRebaseResources,
) error {
	var result error
	for index := len(current.Resources) - 1; index >= 0; index-- {
		resource := &current.Resources[index]
		deviceDetached := resource.DeviceClosed || resource.DevicePath == ""
		if !resource.DeviceClosed && resource.DevicePath != "" {
			var err error
			if live != nil && live.devices[resource.Role] != nil {
				err = live.devices[resource.Role].Close()
				if err == nil {
					delete(live.devices, resource.Role)
				}
			} else {
				err = m.runtime.RecoverOrphanDevice(ctx, resource.DevicePath, resource.DeviceAllocationID)
			}
			if err == nil {
				deviceDetached = true
				resource.DeviceClosed = true
				err = m.saveRebase(*current)
			}
			result = errors.Join(result, err)
		}
		if deviceDetached && live != nil && live.branches[resource.Role] != nil {
			err := live.branches[resource.Role].Close()
			if err == nil {
				delete(live.branches, resource.Role)
			}
			result = errors.Join(result, err)
		}
		if deviceDetached && resource.DevicePath != "" && !resource.DeviceReservationReleased {
			resource.DeviceReservationReleased = true
			if err := m.saveRebase(*current); err != nil {
				resource.DeviceReservationReleased = false
				result = errors.Join(result, err)
			} else {
				m.runtime.ReleaseDeviceReservation(resource.DevicePath, resource.DeviceAllocationID)
			}
		}
	}
	if result != nil {
		return fmt.Errorf("close RootFS rebase devices or branches: %w", result)
	}
	return nil
}

func (m *Manager) removeRebaseArtifacts(current *rebaseRecord) error {
	branchRoot, mountRoot := rebaseOperationRoots(m.branchRoot, m.mountRoot, current.OperationID)
	for _, resource := range current.Resources {
		if filepath.Dir(resource.BranchPath) != branchRoot ||
			filepath.Dir(filepath.Dir(resource.XFSRoot)) != mountRoot ||
			filepath.Dir(filepath.Dir(resource.MergedRoot)) != mountRoot {
			return fmt.Errorf("rebase artifact path does not match operation identity")
		}
		if err := m.nodeDirty.ValidateOwnerDetached(resource.BranchPath); err != nil {
			return fmt.Errorf("validate %s rebase dirty tail release: %w", resource.Role, err)
		}
	}
	if err := os.RemoveAll(branchRoot); err != nil {
		return fmt.Errorf("remove RootFS rebase branches: %w", err)
	}
	for _, resource := range current.Resources {
		if err := m.nodeDirty.ReleaseOwner(resource.BranchPath); err != nil {
			return fmt.Errorf("release %s rebase dirty tail: %w", resource.Role, err)
		}
	}
	if err := os.RemoveAll(mountRoot); err != nil {
		return fmt.Errorf("remove RootFS rebase mounts: %w", err)
	}
	return nil
}

func newRebaseRecord(
	branchRoot, mountRoot string,
	request rootfsrebase.WorkerRequest,
	requestDigest string,
) rebaseRecord {
	operationBranchRoot, operationMountRoot := rebaseOperationRoots(branchRoot, mountRoot, request.OperationID)
	resources := make([]rebaseResource, 0, len(rebaseRoles))
	for _, role := range rebaseRoles {
		resources = append(resources, rebaseResource{
			Role: role, BranchPath: filepath.Join(operationBranchRoot, role+".wal"),
			XFSRoot:            filepath.Join(operationMountRoot, role, "xfs"),
			MergedRoot:         filepath.Join(operationMountRoot, role, "merged"),
			DeviceAllocationID: rebaseDeviceAllocationID(request.OperationID, role),
		})
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	return rebaseRecord{
		Version: rebaseRecordVersion, OperationID: request.OperationID,
		RequestDigest: requestDigest, Request: cloneRebaseRequest(request),
		State: rebaseStatePreparing, Resources: resources, CreatedAt: now, UpdatedAt: now,
	}
}

func (r *rebaseRecord) resource(role string) *rebaseResource {
	for index := range r.Resources {
		if r.Resources[index].Role == role {
			return &r.Resources[index]
		}
	}
	return nil
}

func rebaseOperationRoots(branchRoot, mountRoot, operationID string) (string, string) {
	sum := sha256.Sum256([]byte(operationID))
	name := hex.EncodeToString(sum[:])
	return filepath.Join(branchRoot, "rebases", name), filepath.Join(mountRoot, "rebases", name)
}

func rebaseDeviceAllocationID(operationID, role string) string {
	sum := sha256.Sum256([]byte(operationID + "\x00" + role))
	return "rebase-" + hex.EncodeToString(sum[:16]) + "-" + role
}

func rebaseBranchIdentity(request rootfsrebase.WorkerRequest, role string) (string, string) {
	root := sha256.Sum256([]byte(request.FilesystemID + "\x00" + request.OperationID))
	generation := sha256.Sum256([]byte(request.OperationID + "\x00" + role))
	return "rebase-rootfs-" + hex.EncodeToString(root[:16]),
		"rebase-generation-" + hex.EncodeToString(generation[:16]) + "-" + role
}

func (m *Manager) loadRebase(operationID string) (rebaseRecord, error) {
	var current rebaseRecord
	err := m.db.View(func(tx *bolt.Tx) error {
		payload := tx.Bucket(rebaseBucket).Get([]byte(operationID))
		if payload == nil {
			return errdefs.ErrNotFound
		}
		return json.Unmarshal(payload, &current)
	})
	if err != nil {
		return current, err
	}
	if err := m.validateRebaseRecord(current, operationID); err != nil {
		return current, err
	}
	return current, nil
}

func (m *Manager) saveNewRebase(current rebaseRecord) error {
	if err := m.validateRebaseRecord(current, current.OperationID); err != nil {
		return err
	}
	return m.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(rebaseBucket)
		if bucket.Get([]byte(current.OperationID)) != nil {
			return errdefs.ErrAlreadyExists
		}
		return putRebaseRecord(bucket, current)
	})
}

func (m *Manager) saveRebase(current rebaseRecord) error {
	current.UpdatedAt = time.Now().UTC().Format(time.RFC3339Nano)
	if err := m.validateRebaseRecord(current, current.OperationID); err != nil {
		return err
	}
	return m.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(rebaseBucket)
		stored := bucket.Get([]byte(current.OperationID))
		if stored == nil {
			return errdefs.ErrNotFound
		}
		var previous rebaseRecord
		if err := json.Unmarshal(stored, &previous); err != nil {
			return err
		}
		if previous.RequestDigest != current.RequestDigest {
			return fmt.Errorf("RootFS rebase request identity changed: %w", errdefs.ErrFailedPrecondition)
		}
		return putRebaseRecord(bucket, current)
	})
}

func putRebaseRecord(bucket *bolt.Bucket, current rebaseRecord) error {
	payload, err := json.Marshal(current)
	if err != nil {
		return err
	}
	return bucket.Put([]byte(current.OperationID), payload)
}

func (m *Manager) deleteRebase(operationID, requestDigest string) error {
	return m.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(rebaseBucket)
		payload := bucket.Get([]byte(operationID))
		if payload == nil {
			return nil
		}
		var current rebaseRecord
		if err := json.Unmarshal(payload, &current); err != nil {
			return err
		}
		if current.RequestDigest != requestDigest {
			return fmt.Errorf("RootFS rebase request changed before deletion: %w", errdefs.ErrFailedPrecondition)
		}
		return bucket.Delete([]byte(operationID))
	})
}

func (m *Manager) loadRebaseAcknowledgement(operationID string) (rebaseAcknowledgement, error) {
	var acknowledgement rebaseAcknowledgement
	err := m.db.View(func(tx *bolt.Tx) error {
		payload := tx.Bucket(rebaseAckBucket).Get(rebaseAcknowledgementKey(operationID))
		if payload == nil {
			return errdefs.ErrNotFound
		}
		decoded, err := decodeRebaseAcknowledgement(payload)
		if err != nil {
			return err
		}
		acknowledgement = decoded
		return nil
	})
	return acknowledgement, err
}

func (m *Manager) commitRebaseAcknowledgement(
	current rebaseRecord,
	proofDigest string,
	now time.Time,
) error {
	requestDigest, err := parseRebaseSHA256Digest(current.RequestDigest)
	if err != nil {
		return err
	}
	proof, err := parseRebaseSHA256Digest(proofDigest)
	if err != nil {
		return err
	}
	acknowledgement := rebaseAcknowledgement{
		AcknowledgedAt: now.UTC().UnixNano(), RequestDigest: requestDigest, ProofDigest: proof,
	}
	return m.db.Update(func(tx *bolt.Tx) error {
		rebases := tx.Bucket(rebaseBucket)
		payload := rebases.Get([]byte(current.OperationID))
		if payload == nil {
			return errdefs.ErrNotFound
		}
		var stored rebaseRecord
		if err := json.Unmarshal(payload, &stored); err != nil {
			return err
		}
		if stored.RequestDigest != current.RequestDigest || stored.Result == nil || !stored.Clean ||
			stored.Result.ProofDigest != proofDigest {
			return fmt.Errorf("RootFS rebase changed before acknowledgement: %w", errdefs.ErrFailedPrecondition)
		}
		acks := tx.Bucket(rebaseAckBucket)
		key := rebaseAcknowledgementKey(current.OperationID)
		if previous := acks.Get(key); previous != nil {
			decoded, err := decodeRebaseAcknowledgement(previous)
			if err != nil {
				return err
			}
			if decoded.RequestDigest != acknowledgement.RequestDigest || decoded.ProofDigest != acknowledgement.ProofDigest {
				return fmt.Errorf("RootFS rebase acknowledgement identity changed: %w", errdefs.ErrFailedPrecondition)
			}
		} else if err := acks.Put(key, encodeRebaseAcknowledgement(acknowledgement)); err != nil {
			return err
		}
		if err := rebases.Delete([]byte(current.OperationID)); err != nil {
			return err
		}
		return pruneRebaseAcknowledgementBucket(acks, now.UTC())
	})
}

func (m *Manager) commitRebaseRejection(
	current rebaseRecord,
	proofDigest string,
	now time.Time,
) error {
	requestDigest, err := parseRebaseSHA256Digest(current.RequestDigest)
	if err != nil {
		return err
	}
	proof, err := parseRebaseSHA256Digest(proofDigest)
	if err != nil {
		return err
	}
	acknowledgement := rebaseAcknowledgement{
		AcknowledgedAt: now.UTC().UnixNano(), RequestDigest: requestDigest, ProofDigest: proof,
	}
	return m.db.Update(func(tx *bolt.Tx) error {
		rebases := tx.Bucket(rebaseBucket)
		payload := rebases.Get([]byte(current.OperationID))
		if payload == nil {
			return errdefs.ErrNotFound
		}
		var stored rebaseRecord
		if err := json.Unmarshal(payload, &stored); err != nil {
			return err
		}
		if stored.RequestDigest != current.RequestDigest || stored.Result != nil || !stored.Clean {
			return fmt.Errorf("RootFS rebase changed before rejection: %w", errdefs.ErrFailedPrecondition)
		}
		if err := putExactRebaseAcknowledgement(tx.Bucket(rebaseAckBucket), current.OperationID, acknowledgement); err != nil {
			return err
		}
		if err := rebases.Delete([]byte(current.OperationID)); err != nil {
			return err
		}
		return pruneRebaseAcknowledgementBucket(tx.Bucket(rebaseAckBucket), now.UTC())
	})
}

func (m *Manager) commitAbsentRebaseRejection(
	request rootfsrebase.WorkerRequest,
	proofDigest string,
	now time.Time,
) error {
	requestDigestValue, err := request.Digest()
	if err != nil {
		return err
	}
	requestDigest, err := parseRebaseSHA256Digest(requestDigestValue)
	if err != nil {
		return err
	}
	proof, err := parseRebaseSHA256Digest(proofDigest)
	if err != nil {
		return err
	}
	acknowledgement := rebaseAcknowledgement{
		AcknowledgedAt: now.UTC().UnixNano(), RequestDigest: requestDigest, ProofDigest: proof,
	}
	return m.db.Update(func(tx *bolt.Tx) error {
		if tx.Bucket(rebaseBucket).Get([]byte(request.OperationID)) != nil {
			return fmt.Errorf("RootFS rebase appeared before rejection: %w", errdefs.ErrFailedPrecondition)
		}
		acks := tx.Bucket(rebaseAckBucket)
		if err := putExactRebaseAcknowledgement(acks, request.OperationID, acknowledgement); err != nil {
			return err
		}
		return pruneRebaseAcknowledgementBucket(acks, now.UTC())
	})
}

func putExactRebaseAcknowledgement(
	bucket *bolt.Bucket,
	operationID string,
	acknowledgement rebaseAcknowledgement,
) error {
	key := rebaseAcknowledgementKey(operationID)
	if previous := bucket.Get(key); previous != nil {
		decoded, err := decodeRebaseAcknowledgement(previous)
		if err != nil {
			return err
		}
		if decoded.RequestDigest != acknowledgement.RequestDigest || decoded.ProofDigest != acknowledgement.ProofDigest {
			return fmt.Errorf("RootFS rebase acknowledgement identity changed: %w", errdefs.ErrFailedPrecondition)
		}
		return nil
	}
	return bucket.Put(key, encodeRebaseAcknowledgement(acknowledgement))
}

func (m *Manager) pruneRebaseAcknowledgements(now time.Time) error {
	return m.db.Update(func(tx *bolt.Tx) error {
		return pruneRebaseAcknowledgementBucket(tx.Bucket(rebaseAckBucket), now.UTC())
	})
}

func pruneRebaseAcknowledgementBucket(bucket *bolt.Bucket, now time.Time) error {
	type retainedAcknowledgement struct {
		key            []byte
		acknowledgedAt int64
	}
	cutoff := now.Add(-rebaseAcknowledgementTTL).UnixNano()
	retained := make([]retainedAcknowledgement, 0, maxRebaseAcknowledgements+1)
	var expired [][]byte
	if err := bucket.ForEach(func(key, payload []byte) error {
		if payload == nil {
			return nil
		}
		acknowledgement, err := decodeRebaseAcknowledgement(payload)
		if err != nil {
			return err
		}
		copyKey := append([]byte(nil), key...)
		if acknowledgement.AcknowledgedAt <= cutoff {
			expired = append(expired, copyKey)
			return nil
		}
		retained = append(retained, retainedAcknowledgement{
			key: copyKey, acknowledgedAt: acknowledgement.AcknowledgedAt,
		})
		return nil
	}); err != nil {
		return err
	}
	for _, key := range expired {
		if err := bucket.Delete(key); err != nil {
			return err
		}
	}
	if len(retained) <= maxRebaseAcknowledgements {
		return nil
	}
	sort.Slice(retained, func(left, right int) bool {
		if retained[left].acknowledgedAt != retained[right].acknowledgedAt {
			return retained[left].acknowledgedAt < retained[right].acknowledgedAt
		}
		return string(retained[left].key) < string(retained[right].key)
	})
	for _, item := range retained[:len(retained)-maxRebaseAcknowledgements] {
		if err := bucket.Delete(item.key); err != nil {
			return err
		}
	}
	return nil
}

func rebaseAcknowledgementKey(operationID string) []byte {
	sum := sha256.Sum256([]byte(operationID))
	return sum[:]
}

func encodeRebaseAcknowledgement(acknowledgement rebaseAcknowledgement) []byte {
	payload := make([]byte, rebaseAcknowledgementBytes)
	payload[0] = rebaseAcknowledgementVersion
	binary.BigEndian.PutUint64(payload[1:9], uint64(acknowledgement.AcknowledgedAt))
	copy(payload[9:9+sha256.Size], acknowledgement.RequestDigest[:])
	copy(payload[9+sha256.Size:], acknowledgement.ProofDigest[:])
	return payload
}

func decodeRebaseAcknowledgement(payload []byte) (rebaseAcknowledgement, error) {
	var acknowledgement rebaseAcknowledgement
	if len(payload) != rebaseAcknowledgementBytes || payload[0] != rebaseAcknowledgementVersion {
		return acknowledgement, fmt.Errorf("RootFS rebase acknowledgement tombstone is invalid")
	}
	acknowledgement.AcknowledgedAt = int64(binary.BigEndian.Uint64(payload[1:9]))
	copy(acknowledgement.RequestDigest[:], payload[9:9+sha256.Size])
	copy(acknowledgement.ProofDigest[:], payload[9+sha256.Size:])
	if acknowledgement.AcknowledgedAt <= 0 {
		return rebaseAcknowledgement{}, fmt.Errorf("RootFS rebase acknowledgement timestamp is invalid")
	}
	return acknowledgement, nil
}

func parseRebaseSHA256Digest(value string) ([sha256.Size]byte, error) {
	var result [sha256.Size]byte
	parsed, err := digest.Parse(value)
	if err != nil || parsed.Algorithm() != digest.SHA256 || parsed.String() != value {
		return result, fmt.Errorf("digest must be canonical sha256")
	}
	decoded, err := hex.DecodeString(parsed.Encoded())
	if err != nil || len(decoded) != sha256.Size {
		return result, fmt.Errorf("sha256 digest payload must contain 32 bytes")
	}
	copy(result[:], decoded)
	return result, nil
}

func validateRebaseRecord(current rebaseRecord, operationID string) error {
	if current.Version != rebaseRecordVersion || current.OperationID != operationID ||
		len(current.Resources) != len(rebaseRoles) {
		return fmt.Errorf("RootFS rebase journal record is invalid")
	}
	digest, err := current.Request.Digest()
	if err != nil || digest != current.RequestDigest || current.Request.OperationID != current.OperationID {
		return fmt.Errorf("RootFS rebase journal request identity is invalid")
	}
	for index, role := range rebaseRoles {
		resource := current.Resources[index]
		if resource.Role != role || resource.BranchPath == "" || resource.XFSRoot == "" ||
			resource.MergedRoot == "" || resource.DeviceAllocationID == "" {
			return fmt.Errorf("RootFS rebase journal resource %s is invalid", role)
		}
		if resource.DevicePath == "" && (resource.DeviceClosed || resource.DeviceReservationReleased ||
			resource.XFSMountIntent || resource.OverlayMountIntent) {
			return fmt.Errorf("RootFS rebase journal resource %s has side effects without a device", role)
		}
		if resource.OverlayMountIntent && !resource.XFSMountIntent {
			return fmt.Errorf("RootFS rebase journal resource %s has OverlayFS without XFS", role)
		}
		if resource.DeviceReservationReleased && !resource.DeviceClosed {
			return fmt.Errorf("RootFS rebase journal resource %s released a live device", role)
		}
		if resource.DeviceClosed && (resource.XFSMountIntent || resource.OverlayMountIntent) {
			return fmt.Errorf("RootFS rebase journal resource %s closed a mounted device", role)
		}
		if current.Clean && (resource.XFSMountIntent || resource.OverlayMountIntent ||
			(resource.DevicePath != "" && (!resource.DeviceClosed || !resource.DeviceReservationReleased))) {
			return fmt.Errorf("clean RootFS rebase journal resource %s retains a physical side effect", role)
		}
	}
	if !containsSessionState(current.State, rebaseStatePreparing, rebaseStateApplying, rebaseStateBuilding, rebaseStateResult) {
		return fmt.Errorf("RootFS rebase journal state %q is invalid", current.State)
	}
	createdAt, createdErr := time.Parse(time.RFC3339Nano, current.CreatedAt)
	updatedAt, updatedErr := time.Parse(time.RFC3339Nano, current.UpdatedAt)
	if createdErr != nil || updatedErr != nil || createdAt.UTC().Format(time.RFC3339Nano) != current.CreatedAt ||
		updatedAt.UTC().Format(time.RFC3339Nano) != current.UpdatedAt || updatedAt.Before(createdAt) {
		return fmt.Errorf("RootFS rebase journal timestamps are invalid")
	}
	if current.Result != nil {
		if current.State != rebaseStateResult {
			return fmt.Errorf("RootFS rebase journal result has invalid state %q", current.State)
		}
		if err := current.Result.ValidateFor(current.Request); err != nil {
			return fmt.Errorf("RootFS rebase journal result is invalid: %w", err)
		}
	} else if current.State == rebaseStateResult {
		return fmt.Errorf("RootFS rebase journal result state lacks an output")
	}
	return nil
}

func (m *Manager) validateRebaseRecord(current rebaseRecord, operationID string) error {
	if err := validateRebaseRecord(current, operationID); err != nil {
		return err
	}
	expected := newRebaseRecord(m.branchRoot, m.mountRoot, current.Request, current.RequestDigest)
	for index := range expected.Resources {
		want, got := expected.Resources[index], current.Resources[index]
		if got.BranchPath != want.BranchPath || got.XFSRoot != want.XFSRoot ||
			got.MergedRoot != want.MergedRoot || got.DeviceAllocationID != want.DeviceAllocationID {
			return fmt.Errorf("RootFS rebase journal resource %s does not match its operation identity", got.Role)
		}
	}
	return nil
}

func cloneRebaseRequest(request rootfsrebase.WorkerRequest) rootfsrebase.WorkerRequest {
	request.SourceBaseDescriptor = append([]byte(nil), request.SourceBaseDescriptor...)
	request.SourceGenerationDescriptor = append([]byte(nil), request.SourceGenerationDescriptor...)
	request.TargetBaseDescriptor = append([]byte(nil), request.TargetBaseDescriptor...)
	return request
}

func cloneRebaseResult(result rootfsrebase.WorkerResult) rootfsrebase.WorkerResult {
	result.Descriptor = append([]byte(nil), result.Descriptor...)
	result.HealthCheckDigest = append([]byte(nil), result.HealthCheckDigest...)
	return result
}

func ptrRebaseResult(result rootfsrebase.WorkerResult) *rootfsrebase.WorkerResult {
	clone := cloneRebaseResult(result)
	return &clone
}

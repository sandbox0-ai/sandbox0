package service

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	"github.com/sandbox0-ai/sandbox0/pkg/managerapi"
	"github.com/sandbox0-ai/sandbox0/pkg/template"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	utilrand "k8s.io/apimachinery/pkg/util/rand"
)

var ErrSandboxRootFSStoreUnavailable = errors.New("sandbox rootfs store is unavailable")
var ErrSandboxRootFSRequiresPausedSandbox = errors.New("sandbox rootfs operation requires a paused sandbox")
var ErrSandboxRootFSSourceRequiresRunningOrPaused = errors.New("sandbox rootfs source operation requires a running or paused sandbox")
var ErrRootFSSnapshotExpired = errors.New("rootfs snapshot expires_at must be in the future")

type SandboxRootFSProductStore interface {
	CreateRootFSSnapshot(ctx context.Context, req *sandboxstore.CreateRootFSSnapshotRequest) (*sandboxstore.RootFSSnapshot, error)
	ListRootFSSnapshots(ctx context.Context, req *sandboxstore.ListRootFSSnapshotsRequest) ([]*sandboxstore.RootFSSnapshot, error)
	GetRootFSSnapshot(ctx context.Context, snapshotID, teamID string) (*sandboxstore.RootFSSnapshot, error)
	DeleteRootFSSnapshot(ctx context.Context, snapshotID, teamID string) error
	ForkRootFSFilesystem(ctx context.Context, req *sandboxstore.ForkRootFSFilesystemRequest) (*sandboxstore.RootFSFilesystem, error)
	RestoreRootFSFromSnapshot(ctx context.Context, req *sandboxstore.RestoreRootFSFromSnapshotRequest) (*sandboxstore.RootFSFilesystem, error)
}

type sandboxRootFSSnapshotCreator interface {
	CreateRootFSSnapshot(ctx context.Context, req *sandboxstore.CreateRootFSSnapshotRequest) (*sandboxstore.RootFSSnapshot, error)
}

type sandboxRootFSRestorer interface {
	RestoreRootFSFromSnapshot(ctx context.Context, req *sandboxstore.RestoreRootFSFromSnapshotRequest) (*sandboxstore.RootFSFilesystem, error)
}

type sandboxRootFSForker interface {
	ForkRootFSFilesystem(ctx context.Context, req *sandboxstore.ForkRootFSFilesystemRequest) (*sandboxstore.RootFSFilesystem, error)
}

type sandboxRecordUpserter interface {
	UpsertSandbox(ctx context.Context, record *sandboxstore.SandboxRecord) error
}

type CreateSandboxRootFSSnapshotRequest struct {
	Name        string    `json:"name,omitempty"`
	Description string    `json:"description,omitempty"`
	ExpiresAt   time.Time `json:"expires_at,omitempty"`
}

type SandboxRootFSSnapshot struct {
	ID          string    `json:"id"`
	SandboxID   string    `json:"sandbox_id"`
	Name        string    `json:"name,omitempty"`
	Description string    `json:"description,omitempty"`
	CreatedAt   time.Time `json:"created_at"`
	ExpiresAt   time.Time `json:"expires_at,omitempty"`
}

type ListSandboxRootFSSnapshotsResponse struct {
	Snapshots []*SandboxRootFSSnapshot `json:"snapshots"`
	Count     int                      `json:"count"`
}

type RestoreSandboxRootFSRequest struct {
	SnapshotID string `json:"snapshot_id"`
}

type RestoreSandboxRootFSResponse struct {
	SandboxID  string `json:"sandbox_id"`
	SnapshotID string `json:"snapshot_id"`
	Status     string `json:"status"`
}

type ForkSandboxRequest struct {
	Config *ForkSandboxConfig `json:"config,omitempty"`
}

type ForkSandboxConfig struct {
	TTL     *int32 `json:"ttl,omitempty"`
	HardTTL *int32 `json:"hard_ttl,omitempty"`
}

type ForkSandboxResponse struct {
	SourceSandboxID string              `json:"source_sandbox_id"`
	Sandbox         *managerapi.Sandbox `json:"sandbox"`
}

func (s *SandboxService) CreateSandboxRootFSSnapshot(ctx context.Context, sandboxID, teamID string, req *CreateSandboxRootFSSnapshotRequest) (*SandboxRootFSSnapshot, error) {
	if req == nil {
		req = &CreateSandboxRootFSSnapshotRequest{}
	}
	sandboxID = strings.TrimSpace(sandboxID)
	teamID = strings.TrimSpace(teamID)
	if sandboxID == "" {
		return nil, fmt.Errorf("sandbox_id is required")
	}
	if teamID == "" {
		return nil, fmt.Errorf("team_id is required")
	}
	if !req.ExpiresAt.IsZero() && !req.ExpiresAt.After(s.now().UTC()) {
		return nil, ErrRootFSSnapshotExpired
	}
	return s.createSandboxRootFSSnapshotWithID(
		ctx,
		sandboxID,
		teamID,
		generateRootFSSnapshotID(),
		strings.TrimSpace(req.Name),
		strings.TrimSpace(req.Description),
		req.ExpiresAt,
	)
}

func (s *SandboxService) createSandboxRootFSSnapshotWithID(ctx context.Context, sandboxID, teamID, snapshotID, name, description string, expiresAt time.Time) (*SandboxRootFSSnapshot, error) {
	store, err := s.rootFSProductStore()
	if err != nil {
		return nil, err
	}
	sandboxID = strings.TrimSpace(sandboxID)
	teamID = strings.TrimSpace(teamID)
	snapshotID = strings.TrimSpace(snapshotID)
	if sandboxID == "" {
		return nil, fmt.Errorf("sandbox_id is required")
	}
	if teamID == "" {
		return nil, fmt.Errorf("team_id is required")
	}
	if snapshotID == "" {
		return nil, fmt.Errorf("snapshot_id is required")
	}
	name = strings.TrimSpace(name)
	description = strings.TrimSpace(description)
	var checkpoint *rootFSSourceCheckpoint
	for {
		_, checkpoint, err = s.prepareRootFSSourceCheckpoint(ctx, sandboxID, teamID, sandboxstore.SandboxLifecycleKindSnapshot)
		if err == nil {
			break
		}
		switch {
		case errors.Is(err, errSandboxLifecyclePausing),
			errors.Is(err, errSandboxLifecycleResuming),
			errors.Is(err, errSandboxLifecycleRootFSCheckpointing):
			if waitErr := s.waitForSandboxLifecycleTxnExit(ctx, sandboxID); waitErr != nil {
				return nil, waitErr
			}
			continue
		default:
			return nil, err
		}
	}
	checkpointCommitted := false
	if checkpoint != nil {
		defer func() {
			checkpoint.close(s, checkpointCommitted)
		}()
	}
	snapshot, err := s.commitRootFSSnapshot(ctx, store, sandboxID, teamID, snapshotID, strings.TrimSpace(name), strings.TrimSpace(description), expiresAt, checkpoint)
	if err != nil {
		return nil, err
	}
	checkpointCommitted = true
	return sandboxRootFSSnapshotFromStore(snapshot), nil
}

func (s *SandboxService) ListSandboxRootFSSnapshots(ctx context.Context, sandboxID, teamID string) (*ListSandboxRootFSSnapshotsResponse, error) {
	store, err := s.rootFSProductStore()
	if err != nil {
		return nil, err
	}
	sandboxID = strings.TrimSpace(sandboxID)
	teamID = strings.TrimSpace(teamID)
	if sandboxID == "" {
		return nil, fmt.Errorf("sandbox_id is required")
	}
	if teamID == "" {
		return nil, fmt.Errorf("team_id is required")
	}
	if err := s.requireRootFSSandboxOwnership(ctx, sandboxID, teamID); err != nil {
		return nil, err
	}
	snapshots, err := store.ListRootFSSnapshots(ctx, &sandboxstore.ListRootFSSnapshotsRequest{
		SandboxID: sandboxID,
		TeamID:    teamID,
	})
	if err != nil {
		return nil, err
	}
	out := make([]*SandboxRootFSSnapshot, 0, len(snapshots))
	for _, snapshot := range snapshots {
		if snapshot == nil || template.IsBuildSnapshotID(snapshot.ID) {
			continue
		}
		out = append(out, sandboxRootFSSnapshotFromStore(snapshot))
	}
	return &ListSandboxRootFSSnapshotsResponse{
		Snapshots: out,
		Count:     len(out),
	}, nil
}

func (s *SandboxService) GetSandboxRootFSSnapshot(ctx context.Context, snapshotID, teamID string) (*SandboxRootFSSnapshot, error) {
	snapshotID = strings.TrimSpace(snapshotID)
	if template.IsBuildSnapshotID(snapshotID) {
		return nil, sandboxstore.ErrRootFSSnapshotNotFound
	}
	store, err := s.rootFSProductStore()
	if err != nil {
		return nil, err
	}
	snapshot, err := store.GetRootFSSnapshot(ctx, snapshotID, strings.TrimSpace(teamID))
	if err != nil {
		return nil, err
	}
	return sandboxRootFSSnapshotFromStore(snapshot), nil
}

func (s *SandboxService) DeleteSandboxRootFSSnapshot(ctx context.Context, snapshotID, teamID string) error {
	snapshotID = strings.TrimSpace(snapshotID)
	if template.IsBuildSnapshotID(snapshotID) {
		return sandboxstore.ErrRootFSSnapshotNotFound
	}
	store, err := s.rootFSProductStore()
	if err != nil {
		return err
	}
	return store.DeleteRootFSSnapshot(ctx, snapshotID, strings.TrimSpace(teamID))
}

func (s *SandboxService) RestoreSandboxRootFS(ctx context.Context, sandboxID, teamID string, req *RestoreSandboxRootFSRequest) (*RestoreSandboxRootFSResponse, error) {
	store, err := s.rootFSProductStore()
	if err != nil {
		return nil, err
	}
	if req == nil {
		return nil, fmt.Errorf("snapshot_id is required")
	}
	sandboxID = strings.TrimSpace(sandboxID)
	teamID = strings.TrimSpace(teamID)
	snapshotID := strings.TrimSpace(req.SnapshotID)
	if sandboxID == "" {
		return nil, fmt.Errorf("sandbox_id is required")
	}
	if teamID == "" {
		return nil, fmt.Errorf("team_id is required")
	}
	if snapshotID == "" {
		return nil, fmt.Errorf("snapshot_id is required")
	}
	if template.IsBuildSnapshotID(snapshotID) {
		return nil, sandboxstore.ErrRootFSSnapshotNotFound
	}
	if _, err := store.GetRootFSSnapshot(ctx, snapshotID, teamID); err != nil {
		return nil, err
	}
	err = s.sandboxStore.WithSandboxLock(ctx, sandboxID, func(lockCtx context.Context, tx sandboxstore.SandboxStoreTx, record *sandboxstore.SandboxRecord) error {
		if err := validateRootFSSandboxRecord(record, sandboxID, teamID, true); err != nil {
			return err
		}
		restorer := sandboxRootFSRestorer(store)
		if txRestorer, ok := tx.(sandboxRootFSRestorer); ok {
			restorer = txRestorer
		}
		_, restoreErr := restorer.RestoreRootFSFromSnapshot(lockCtx, &sandboxstore.RestoreRootFSFromSnapshotRequest{
			SandboxID:  sandboxID,
			SnapshotID: snapshotID,
			TeamID:     teamID,
		})
		return restoreErr
	})
	if err != nil {
		return nil, err
	}
	return &RestoreSandboxRootFSResponse{
		SandboxID:  sandboxID,
		SnapshotID: snapshotID,
		Status:     managerapi.SandboxStatusPaused,
	}, nil
}

func (s *SandboxService) ForkSandbox(ctx context.Context, sourceSandboxID, teamID, userID string, req *ForkSandboxRequest) (*ForkSandboxResponse, error) {
	store, err := s.rootFSProductStore()
	if err != nil {
		return nil, err
	}
	if req == nil {
		req = &ForkSandboxRequest{}
	}
	sourceSandboxID = strings.TrimSpace(sourceSandboxID)
	teamID = strings.TrimSpace(teamID)
	userID = strings.TrimSpace(userID)
	if sourceSandboxID == "" {
		return nil, fmt.Errorf("sandbox_id is required")
	}
	if teamID == "" {
		return nil, fmt.Errorf("team_id is required")
	}

	var source *sandboxstore.SandboxRecord
	var checkpoint *rootFSSourceCheckpoint
	for {
		source, checkpoint, err = s.prepareRootFSSourceCheckpoint(ctx, sourceSandboxID, teamID, sandboxstore.SandboxLifecycleKindFork)
		if err == nil {
			break
		}
		switch {
		case errors.Is(err, errSandboxLifecyclePausing),
			errors.Is(err, errSandboxLifecycleResuming),
			errors.Is(err, errSandboxLifecycleRootFSCheckpointing):
			if waitErr := s.waitForSandboxLifecycleTxnExit(ctx, sourceSandboxID); waitErr != nil {
				return nil, waitErr
			}
			continue
		default:
			return nil, err
		}
	}
	template, err := s.templateForSandboxRecord(source)
	if err != nil {
		if checkpoint != nil {
			checkpoint.close(s, false)
		}
		return nil, err
	}
	targetID, err := s.generateAvailableForkSandboxID(ctx, template)
	if err != nil {
		if checkpoint != nil {
			checkpoint.close(s, false)
		}
		return nil, err
	}
	now := s.now().UTC()
	targetConfig := cloneSandboxConfigValue(source.Config)
	if req.Config != nil {
		if req.Config.TTL != nil {
			targetConfig.TTL = cloneInt32Ptr(req.Config.TTL)
		}
		if req.Config.HardTTL != nil {
			targetConfig.HardTTL = cloneInt32Ptr(req.Config.HardTTL)
		}
	}
	if err := validateSandboxConfigLifecycle(targetConfig.TTL, targetConfig.HardTTL); err != nil {
		if checkpoint != nil {
			checkpoint.close(s, false)
		}
		return nil, err
	}
	expiresAt := expirationFromTTL(now, targetConfig.TTL)
	if expiresAt.IsZero() && targetConfig.TTL == nil {
		expiresAt = source.ExpiresAt
	}
	hardExpiresAt := expirationFromTTL(now, targetConfig.HardTTL)
	if hardExpiresAt.IsZero() && targetConfig.HardTTL == nil {
		hardExpiresAt = source.HardExpiresAt
	}
	target := &sandboxstore.SandboxRecord{
		ID:                targetID,
		TeamID:            teamID,
		UserID:            userID,
		TemplateID:        source.TemplateID,
		TemplateName:      source.TemplateName,
		TemplateNamespace: source.TemplateNamespace,
		ClusterID:         source.ClusterID,
		DesiredState:      sandboxstore.SandboxDesiredStatePaused,
		Config:            targetConfig,
		TemplateSpec:      *source.TemplateSpec.DeepCopy(),
		ClaimedAt:         now,
		ExpiresAt:         expiresAt,
		HardExpiresAt:     hardExpiresAt,
		CreatedAt:         now,
		UpdatedAt:         now,
	}
	checkpointCommitted := false
	if checkpoint != nil {
		defer func() {
			checkpoint.close(s, checkpointCommitted)
		}()
	}
	err = s.commitForkSandbox(ctx, store, sourceSandboxID, teamID, target, checkpoint)
	if err != nil {
		return nil, err
	}
	checkpointCommitted = true
	return &ForkSandboxResponse{
		SourceSandboxID: sourceSandboxID,
		Sandbox:         s.recordToSandbox(target),
	}, nil
}

type rootFSSourceCheckpoint struct {
	txn           *sandboxstore.SandboxLifecycleTxn
	rootFSState   *sandboxstore.SandboxRootFSState
	procdAddress  string
	internalToken string
}

func (c *rootFSSourceCheckpoint) close(s *SandboxService, committed bool) {
	if c == nil || s == nil {
		return
	}
	if !committed {
		reason := rootFSSourceCheckpointTxnReason(c.txn, "transaction did not commit")
		s.deleteUncommittedRootFSObject(c.rootFSState, reason)
		if c.txn != nil {
			_ = s.abortLifecycleTxn(context.Background(), c.txn.SandboxID, c.txn.ID, reason)
		}
	}
	s.releasePauseRuntimeBarrier(context.Background(), c.procdAddress, c.internalToken)
}

func rootFSSourceCheckpointTxnReason(txn *sandboxstore.SandboxLifecycleTxn, suffix string) string {
	kind := "rootfs checkpoint"
	if txn != nil {
		switch txn.Kind {
		case sandboxstore.SandboxLifecycleKindFork:
			kind = "fork"
		case sandboxstore.SandboxLifecycleKindSnapshot:
			kind = "snapshot"
		}
	}
	return kind + " " + suffix
}

func (s *SandboxService) prepareRootFSSourceCheckpoint(ctx context.Context, sourceSandboxID, teamID, kind string) (*sandboxstore.SandboxRecord, *rootFSSourceCheckpoint, error) {
	switch kind {
	case sandboxstore.SandboxLifecycleKindFork, sandboxstore.SandboxLifecycleKindSnapshot:
	default:
		return nil, nil, fmt.Errorf("unsupported rootfs source checkpoint kind %q", kind)
	}
	var source *sandboxstore.SandboxRecord
	var txn *sandboxstore.SandboxLifecycleTxn
	var waitErr error
	err := s.sandboxStore.WithSandboxLock(ctx, sourceSandboxID, func(lockCtx context.Context, tx sandboxstore.SandboxStoreTx, record *sandboxstore.SandboxRecord) error {
		if err := validateRootFSSourceSandboxRecord(record, sourceSandboxID, teamID, s.now()); err != nil {
			return err
		}
		activeTxn, err := tx.GetActiveLifecycleTxn(lockCtx, sourceSandboxID)
		if err != nil {
			return err
		}
		if activeTxn != nil {
			switch activeTxn.Kind {
			case sandboxstore.SandboxLifecycleKindPause:
				if sandboxLifecycleTxnCancelableAutoPause(activeTxn) {
					if _, err := tx.RequestLifecycleTxnCancel(lockCtx, activeTxn.ID, kind+" arrived during auto pause"); err != nil {
						return err
					}
				}
				waitErr = errSandboxLifecyclePausing
			case sandboxstore.SandboxLifecycleKindResume:
				waitErr = errSandboxLifecycleResuming
			default:
				waitErr = errSandboxLifecycleRootFSCheckpointing
			}
			return nil
		}
		source = cloneSandboxRecordForRootFSProduct(record)
		if record.DesiredState == sandboxstore.SandboxDesiredStatePaused {
			return nil
		}
		if !s.config.CtldEnabled || s.ctldClient == nil {
			return ErrSandboxCheckpointRequiresCtld
		}
		pod, err := s.resolveRootFSSourceRuntimePod(lockCtx, record)
		if err != nil {
			return err
		}
		txn = &sandboxstore.SandboxLifecycleTxn{
			ID:               uuid.NewString(),
			SandboxID:        sourceSandboxID,
			Kind:             kind,
			Phase:            sandboxstore.SandboxLifecyclePhasePreparing,
			Source:           sandboxstore.SandboxLifecycleSourceManual,
			FromGeneration:   runtimeGenerationFromPod(pod),
			FromPodNamespace: pod.Namespace,
			FromPodName:      pod.Name,
		}
		return tx.BeginLifecycleTxn(lockCtx, txn)
	})
	if err != nil {
		return nil, nil, err
	}
	if waitErr != nil {
		return nil, nil, waitErr
	}
	if source == nil {
		return nil, nil, fmt.Errorf("sandbox record is required")
	}
	if txn == nil {
		return source, nil, nil
	}
	checkpoint, err := s.prepareRunningRootFSSourceCheckpoint(ctx, source, txn)
	if err != nil {
		_ = s.abortLifecycleTxn(context.Background(), sourceSandboxID, txn.ID, err.Error())
		if checkpoint != nil {
			checkpoint.close(s, false)
		}
		return nil, nil, err
	}
	return source, checkpoint, nil
}

func (s *SandboxService) prepareRunningRootFSSourceCheckpoint(ctx context.Context, source *sandboxstore.SandboxRecord, txn *sandboxstore.SandboxLifecycleTxn) (*rootFSSourceCheckpoint, error) {
	if source == nil || txn == nil {
		return nil, fmt.Errorf("rootfs checkpoint source is required")
	}
	pod, err := s.getSandboxPod(ctx, source.ID)
	if err != nil {
		return nil, fmt.Errorf("get runtime pod: %w", err)
	}
	generation := runtimeGenerationFromPod(pod)
	if generation != txn.FromGeneration {
		return nil, fmt.Errorf("sandbox runtime generation changed during rootfs checkpoint: txn=%d pod=%d", txn.FromGeneration, generation)
	}
	if txn.FromPodName != "" && pod.Name != txn.FromPodName {
		return nil, apierrors.NewConflict(schema.GroupResource{Resource: "pod"}, pod.Name, fmt.Errorf("rootfs checkpoint transaction points at runtime pod %s", txn.FromPodName))
	}
	if txn.FromPodNamespace != "" && pod.Namespace != txn.FromPodNamespace {
		return nil, apierrors.NewConflict(schema.GroupResource{Resource: "pod"}, pod.Name, fmt.Errorf("rootfs checkpoint transaction points at runtime namespace %s", txn.FromPodNamespace))
	}
	if err := s.markLifecycleTxnPhase(ctx, source.ID, txn.ID, sandboxstore.SandboxLifecyclePhaseBarriered); err != nil {
		return nil, err
	}
	procdAddress, internalToken, err := s.activatePauseRuntimeBarrier(ctx, pod, source, txn)
	if err != nil {
		return nil, err
	}
	checkpoint := &rootFSSourceCheckpoint{
		txn:           txn,
		procdAddress:  procdAddress,
		internalToken: internalToken,
	}
	if err := s.markLifecycleTxnPhase(ctx, source.ID, txn.ID, sandboxstore.SandboxLifecyclePhasePublishing); err != nil {
		return checkpoint, err
	}
	rootFSState, err := s.prepareSandboxRootFSCheckpoint(ctx, pod, source)
	if err != nil {
		return checkpoint, err
	}
	if rootFSState == nil {
		return checkpoint, fmt.Errorf("rootfs checkpoint produced no state")
	}
	checkpoint.rootFSState = rootFSState
	if err := s.markLifecycleTxnPreparedHead(ctx, source.ID, txn.ID, rootFSState.LayerID); err != nil {
		return checkpoint, err
	}
	return checkpoint, nil
}

func (s *SandboxService) commitRootFSSnapshot(ctx context.Context, store SandboxRootFSProductStore, sandboxID, teamID, snapshotID, name, description string, expiresAt time.Time, checkpoint *rootFSSourceCheckpoint) (*sandboxstore.RootFSSnapshot, error) {
	var snapshot *sandboxstore.RootFSSnapshot
	err := s.sandboxStore.WithSandboxLock(ctx, sandboxID, func(lockCtx context.Context, tx sandboxstore.SandboxStoreTx, record *sandboxstore.SandboxRecord) error {
		if err := validateRootFSSourceSandboxRecord(record, sandboxID, teamID, s.now()); err != nil {
			return err
		}
		activeTxn, err := tx.GetActiveLifecycleTxn(lockCtx, sandboxID)
		if err != nil {
			return err
		}
		if checkpoint != nil {
			if activeTxn == nil || checkpoint.txn == nil || activeTxn.ID != checkpoint.txn.ID || activeTxn.Kind != sandboxstore.SandboxLifecycleKindSnapshot {
				return fmt.Errorf("snapshot lifecycle transaction is no longer active")
			}
			if err := tx.UpdateLifecycleTxnPhase(lockCtx, checkpoint.txn.ID, sandboxstore.SandboxLifecyclePhaseCommitting); err != nil {
				return err
			}
			if checkpoint.rootFSState != nil {
				if err := tx.SaveRootFSState(lockCtx, checkpoint.rootFSState); err != nil {
					return err
				}
			}
		} else {
			if activeTxn != nil {
				return errSandboxLifecycleRootFSCheckpointing
			}
			if record.DesiredState != sandboxstore.SandboxDesiredStatePaused {
				return errSandboxLifecycleRootFSCheckpointing
			}
		}

		creator := sandboxRootFSSnapshotCreator(store)
		if txCreator, ok := tx.(sandboxRootFSSnapshotCreator); ok {
			creator = txCreator
		}
		var createErr error
		snapshot, createErr = creator.CreateRootFSSnapshot(lockCtx, &sandboxstore.CreateRootFSSnapshotRequest{
			SandboxID:   sandboxID,
			SnapshotID:  snapshotID,
			Name:        name,
			Description: description,
			ExpiresAt:   expiresAt,
		})
		if createErr != nil {
			return createErr
		}
		if checkpoint != nil && checkpoint.txn != nil {
			preparedHead := ""
			if checkpoint.rootFSState != nil {
				preparedHead = checkpoint.rootFSState.LayerID
			}
			if err := tx.CommitLifecycleTxn(lockCtx, checkpoint.txn.ID, preparedHead); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return snapshot, nil
}

func (s *SandboxService) commitForkSandbox(ctx context.Context, store SandboxRootFSProductStore, sourceSandboxID, teamID string, target *sandboxstore.SandboxRecord, checkpoint *rootFSSourceCheckpoint) error {
	if target == nil {
		return fmt.Errorf("target sandbox record is required")
	}
	err := s.sandboxStore.WithSandboxLock(ctx, sourceSandboxID, func(lockCtx context.Context, tx sandboxstore.SandboxStoreTx, record *sandboxstore.SandboxRecord) error {
		if err := validateRootFSSourceSandboxRecord(record, sourceSandboxID, teamID, s.now()); err != nil {
			return err
		}
		activeTxn, err := tx.GetActiveLifecycleTxn(lockCtx, sourceSandboxID)
		if err != nil {
			return err
		}
		if checkpoint != nil {
			if activeTxn == nil || checkpoint.txn == nil || activeTxn.ID != checkpoint.txn.ID || activeTxn.Kind != sandboxstore.SandboxLifecycleKindFork {
				return fmt.Errorf("fork lifecycle transaction is no longer active")
			}
			if err := tx.UpdateLifecycleTxnPhase(lockCtx, checkpoint.txn.ID, sandboxstore.SandboxLifecyclePhaseCommitting); err != nil {
				return err
			}
			if checkpoint.rootFSState != nil {
				if err := tx.SaveRootFSState(lockCtx, checkpoint.rootFSState); err != nil {
					return err
				}
			}
		} else {
			if activeTxn != nil {
				return errSandboxLifecycleRootFSCheckpointing
			}
			if record.DesiredState != sandboxstore.SandboxDesiredStatePaused {
				return errSandboxLifecycleRootFSCheckpointing
			}
		}

		upserter := sandboxRecordUpserter(s.sandboxStore)
		txBacked := false
		if txUpserter, ok := tx.(sandboxRecordUpserter); ok {
			upserter = txUpserter
			txBacked = true
		}
		if err := upserter.UpsertSandbox(lockCtx, target); err != nil {
			return err
		}
		forker := sandboxRootFSForker(store)
		if txForker, ok := tx.(sandboxRootFSForker); ok {
			forker = txForker
			txBacked = true
		}
		if _, err := forker.ForkRootFSFilesystem(lockCtx, &sandboxstore.ForkRootFSFilesystemRequest{
			SourceSandboxID: sourceSandboxID,
			TargetSandboxID: target.ID,
			TargetTeamID:    teamID,
		}); err != nil {
			if txBacked {
				return err
			}
			if cleanupErr := s.sandboxStore.MarkSandboxDeleted(lockCtx, target.ID, s.now().UTC()); cleanupErr != nil && s.logger != nil {
				s.logger.Warn("Failed to clean up sandbox record after rootfs fork failure",
					zap.String("sandboxID", target.ID),
					zap.Error(cleanupErr),
				)
			}
			return err
		}
		if checkpoint != nil && checkpoint.txn != nil {
			preparedHead := ""
			if checkpoint.rootFSState != nil {
				preparedHead = checkpoint.rootFSState.LayerID
			}
			if err := tx.CommitLifecycleTxn(lockCtx, checkpoint.txn.ID, preparedHead); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	return nil
}

func (s *SandboxService) rootFSProductStore() (SandboxRootFSProductStore, error) {
	if s == nil || s.sandboxStore == nil {
		return nil, ErrSandboxRootFSStoreUnavailable
	}
	store, ok := s.sandboxStore.(SandboxRootFSProductStore)
	if !ok {
		return nil, ErrSandboxRootFSStoreUnavailable
	}
	return store, nil
}

func (s *SandboxService) generateAvailableForkSandboxID(ctx context.Context, template *v1alpha1.SandboxTemplate) (string, error) {
	if s == nil || s.sandboxStore == nil {
		return "", ErrSandboxRootFSStoreUnavailable
	}
	for i := 0; i < 8; i++ {
		sandboxID, err := s.generateStableSandboxID(template)
		if err != nil {
			return "", err
		}
		existing, err := s.sandboxStore.GetSandbox(ctx, sandboxID)
		if err != nil {
			if errors.Is(err, sandboxstore.ErrSandboxRecordNotFound) {
				return sandboxID, nil
			}
			return "", err
		}
		if existing == nil {
			return sandboxID, nil
		}
	}
	return "", fmt.Errorf("failed to allocate fork sandbox id")
}

func (s *SandboxService) requireRootFSSandboxOwnership(ctx context.Context, sandboxID, teamID string) error {
	if s == nil || s.sandboxStore == nil {
		return ErrSandboxRootFSStoreUnavailable
	}
	record, err := s.sandboxStore.GetSandbox(ctx, sandboxID)
	if err != nil {
		return err
	}
	return validateRootFSSandboxRecord(record, sandboxID, teamID, false)
}

func validateRootFSSandboxRecord(record *sandboxstore.SandboxRecord, sandboxID, teamID string, requirePaused bool) error {
	if record == nil || record.DesiredState == sandboxstore.SandboxDesiredStateDeleted {
		return apierrors.NewNotFound(schema.GroupResource{Resource: "sandbox"}, sandboxID)
	}
	if record.TeamID != teamID {
		return apierrors.NewForbidden(schema.GroupResource{Resource: "sandbox"}, sandboxID, fmt.Errorf("sandbox belongs to a different team"))
	}
	if requirePaused && record.DesiredState != sandboxstore.SandboxDesiredStatePaused {
		return fmt.Errorf("%w: current desired state is %s", ErrSandboxRootFSRequiresPausedSandbox, record.DesiredState)
	}
	return nil
}

func validateRootFSSourceSandboxRecord(record *sandboxstore.SandboxRecord, sandboxID, teamID string, now time.Time) error {
	if err := validateRootFSSandboxRecord(record, sandboxID, teamID, false); err != nil {
		return err
	}
	if record.DesiredState != sandboxstore.SandboxDesiredStatePaused && record.DesiredState != sandboxstore.SandboxDesiredStateActive {
		return fmt.Errorf("%w: current desired state is %s", ErrSandboxRootFSSourceRequiresRunningOrPaused, record.DesiredState)
	}
	if sandboxHardExpired(record.HardExpiresAt, now) {
		return apierrors.NewNotFound(schema.GroupResource{Resource: "sandbox"}, sandboxID)
	}
	return nil
}

// resolveRootFSSourceRuntimePod projects active source readiness exclusively
// from the shared informer cache. Paused sources have no runtime pod by design.
func (s *SandboxService) resolveRootFSSourceRuntimePod(ctx context.Context, record *sandboxstore.SandboxRecord) (*corev1.Pod, error) {
	if record == nil {
		return nil, fmt.Errorf("%w: current status is %s", ErrSandboxRootFSSourceRequiresRunningOrPaused, managerapi.SandboxStatusStarting)
	}
	if record.DesiredState == sandboxstore.SandboxDesiredStatePaused {
		return nil, nil
	}
	if record.DesiredState != sandboxstore.SandboxDesiredStateActive {
		return nil, fmt.Errorf("%w: current desired state is %s", ErrSandboxRootFSSourceRequiresRunningOrPaused, record.DesiredState)
	}
	if s == nil || s.podLister == nil {
		return nil, fmt.Errorf("%w: current status is %s", ErrSandboxRootFSSourceRequiresRunningOrPaused, managerapi.SandboxStatusStarting)
	}
	pod, err := s.getSandboxPod(ctx, record.ID)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil, fmt.Errorf("%w: current status is %s", ErrSandboxRootFSSourceRequiresRunningOrPaused, managerapi.SandboxStatusStarting)
		}
		return nil, fmt.Errorf("get cached runtime pod: %w", err)
	}
	status := s.podToSandboxStatus(pod)
	if status != managerapi.SandboxStatusRunning {
		return nil, fmt.Errorf("%w: current status is %s", ErrSandboxRootFSSourceRequiresRunningOrPaused, status)
	}
	return pod, nil
}

func sandboxRootFSSnapshotFromStore(snapshot *sandboxstore.RootFSSnapshot) *SandboxRootFSSnapshot {
	if snapshot == nil {
		return nil
	}
	return &SandboxRootFSSnapshot{
		ID:          snapshot.ID,
		SandboxID:   snapshot.SourceSandboxID,
		Name:        snapshot.Name,
		Description: snapshot.Description,
		CreatedAt:   snapshot.CreatedAt,
		ExpiresAt:   snapshot.ExpiresAt,
	}
}

func generateRootFSSnapshotID() string {
	return "rootfs-snapshot-" + utilrand.String(10)
}

func cloneSandboxRecordForRootFSProduct(record *sandboxstore.SandboxRecord) *sandboxstore.SandboxRecord {
	if record == nil {
		return nil
	}
	clone := *record
	clone.Config = cloneSandboxConfigValue(record.Config)
	clone.Mounts = append([]managerapi.ClaimMount(nil), record.Mounts...)
	clone.TemplateSpec = *record.TemplateSpec.DeepCopy()
	return &clone
}

func cloneSandboxConfigValue(cfg sandboxstore.SandboxConfig) sandboxstore.SandboxConfig {
	cloned := cloneSandboxConfig(&cfg)
	if cloned == nil {
		return sandboxstore.SandboxConfig{}
	}
	return *cloned
}

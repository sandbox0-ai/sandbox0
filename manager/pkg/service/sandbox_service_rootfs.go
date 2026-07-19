package service

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"path"
	"strings"
	"time"

	"github.com/google/uuid"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/controller"
	"github.com/sandbox0-ai/sandbox0/pkg/ctldapi"
	"github.com/sandbox0-ai/sandbox0/pkg/teamquota"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
)

const sandboxRootFSContainerName = "procd"

const sandboxRootFSOperationTimeout = 5 * time.Minute
const sandboxRootFSSourceCheckpointLifecycleStaleAfter = sandboxRootFSOperationTimeout + time.Minute
const sandboxRootFSUncommittedObjectDeleteDelay = 15 * time.Minute
const sandboxRootFSUncommittedObjectDeleteTimeout = 30 * time.Second
const rootFSPublishStageTTL = 10 * time.Minute
const rootFSPublishStageReleaseGrace = 2 * time.Minute

func (s *SandboxService) saveSandboxRootFSCheckpoint(ctx context.Context, pod *corev1.Pod, record *SandboxRecord, tx SandboxStoreTx) error {
	state, err := s.prepareSandboxRootFSCheckpoint(ctx, pod, record)
	if err != nil {
		return err
	}
	if state == nil {
		return nil
	}
	if tx != nil {
		return tx.SaveRootFSState(ctx, state)
	}
	if s.sandboxStore != nil {
		return s.sandboxStore.SaveRootFSState(ctx, state)
	}
	return nil
}

func (s *SandboxService) prepareSandboxRootFSCheckpoint(ctx context.Context, pod *corev1.Pod, record *SandboxRecord) (*SandboxRootFSState, error) {
	if s == nil || !s.config.CtldEnabled || s.ctldClient == nil || pod == nil {
		return nil, nil
	}
	if record == nil {
		return nil, nil
	}
	sandboxID := sandboxIDFromPod(pod)
	if sandboxID == "" {
		sandboxID = record.ID
	}
	teamID := record.TeamID
	if sandboxID == "" {
		sandboxID = pod.Name
	}
	if teamID == "" && pod.Annotations != nil {
		teamID = pod.Annotations[controller.AnnotationTeamID]
	}
	if strings.TrimSpace(teamID) == "" {
		return nil, fmt.Errorf("team_id is required to save sandbox rootfs checkpoint")
	}

	ctldAddress, err := s.ctldAddressForPod(ctx, pod)
	if err != nil {
		return nil, err
	}
	generation := runtimeGenerationFromPod(pod)
	parentLayerID := ""
	expectedHeadLayerID := ""
	var parentState *SandboxRootFSState
	if parentState, err = s.latestRootFSState(ctx, sandboxID); err != nil {
		return nil, fmt.Errorf("load current rootfs head: %w", err)
	} else if parentState != nil {
		expectedHeadLayerID = strings.TrimSpace(parentState.LayerID)
		if squash, reason := s.shouldSquashSandboxRootFSCheckpoint(parentState); squash {
			if s.logger != nil {
				s.logger.Info("Squashing sandbox rootfs checkpoint",
					zap.String("sandboxID", sandboxID),
					zap.String("headLayerID", expectedHeadLayerID),
					zap.String("reason", reason),
				)
			}
		} else {
			parentLayerID = expectedHeadLayerID
		}
	}
	layerID := uuid.NewString()
	prepareReq := ctldapi.PrepareRootFSSnapshotRequest{
		Target:                    rootFSTargetForPod(pod),
		StageID:                   layerID,
		TeamID:                    teamID,
		SandboxID:                 sandboxID,
		ExpectedRuntimeGeneration: generation,
		ExpiresAt:                 s.now().Add(rootFSPublishStageTTL),
		ParentLayerID:             parentLayerID,
		ExcludedPaths:             rootFSExcludedPathsForPod(pod),
	}
	resp, err := s.prepareAndPublishSandboxRootFSSnapshot(ctx, ctldAddress, prepareReq, sandboxID, teamID, generation, layerID)
	if err != nil && parentLayerID != "" && rootFSBaselineMissing(err, resp) {
		parentLayerID = ""
		prepareReq.ParentLayerID = ""
		resp, err = s.prepareAndPublishSandboxRootFSSnapshot(ctx, ctldAddress, prepareReq, sandboxID, teamID, generation, layerID)
	}
	if err != nil {
		return nil, fmt.Errorf("save sandbox rootfs checkpoint: %w", rootFSResponseError(err, publishRootFSError(resp)))
	}
	state, err := rootFSStateFromPublishResponse(sandboxID, teamID, generation, resp)
	if err != nil {
		return nil, err
	}
	state.LayerID = layerID
	state.ParentLayerID = parentLayerID
	state.ExpectedHeadLayerID = expectedHeadLayerID
	platform := s.rootFSPlatformForPod(pod)
	if platform.OS == "" && parentState != nil {
		platform.OS = parentState.PlatformOS
	}
	if platform.Architecture == "" && parentState != nil {
		platform.Architecture = parentState.PlatformArchitecture
	}
	if platform.Variant == "" && parentState != nil {
		platform.Variant = parentState.PlatformVariant
	}
	state.PlatformOS = platform.OS
	state.PlatformArchitecture = platform.Architecture
	state.PlatformVariant = platform.Variant
	return state, nil
}

const rootFSPlatformVariantLabel = "sandbox0.ai/platform-variant"

// rootFSPlatformForPod captures the platform of the node that actually ran the
// sandbox. It deliberately does not fall back to manager's own GOOS/GOARCH.
func (s *SandboxService) rootFSPlatformForPod(pod *corev1.Pod) ocispec.Platform {
	if pod == nil {
		return ocispec.Platform{}
	}
	platform := ocispec.Platform{
		OS:           strings.TrimSpace(pod.Spec.NodeSelector[corev1.LabelOSStable]),
		Architecture: strings.TrimSpace(pod.Spec.NodeSelector[corev1.LabelArchStable]),
		Variant:      strings.TrimSpace(pod.Spec.NodeSelector[rootFSPlatformVariantLabel]),
	}
	if s == nil || s.nodeLister == nil || strings.TrimSpace(pod.Spec.NodeName) == "" {
		return platform
	}
	node, err := s.nodeLister.Get(pod.Spec.NodeName)
	if err != nil || node == nil {
		return platform
	}
	if value := strings.TrimSpace(node.Labels[corev1.LabelOSStable]); value != "" {
		platform.OS = value
	} else if value := strings.TrimSpace(node.Status.NodeInfo.OperatingSystem); value != "" {
		platform.OS = value
	}
	if value := strings.TrimSpace(node.Labels[corev1.LabelArchStable]); value != "" {
		platform.Architecture = value
	} else if value := strings.TrimSpace(node.Status.NodeInfo.Architecture); value != "" {
		platform.Architecture = value
	}
	if value := strings.TrimSpace(node.Labels[rootFSPlatformVariantLabel]); value != "" {
		platform.Variant = value
	}
	return platform
}

func (s *SandboxService) prepareAndPublishSandboxRootFSSnapshot(ctx context.Context, ctldAddress string, prepareReq ctldapi.PrepareRootFSSnapshotRequest, sandboxID, teamID string, generation int64, layerID string) (*ctldapi.PublishRootFSSnapshotResponse, error) {
	if prepareReq.ExpiresAt.IsZero() {
		prepareReq.ExpiresAt = s.now().Add(rootFSPublishStageTTL)
	}
	prepareReq.StageID = strings.TrimSpace(layerID)
	prepareReq.TeamID = strings.TrimSpace(teamID)
	prepareReq.SandboxID = strings.TrimSpace(sandboxID)
	prepareReq.ExpectedRuntimeGeneration = generation
	if err := s.prepareRootFSPublishStage(ctx, RootFSPublishStage{
		StageID:           prepareReq.StageID,
		TeamID:            prepareReq.TeamID,
		SandboxID:         prepareReq.SandboxID,
		CtldAddress:       ctldAddress,
		RuntimeGeneration: generation,
		ExpiresAt:         prepareReq.ExpiresAt,
		ReleaseAfter:      prepareReq.ExpiresAt.Add(rootFSPublishStageReleaseGrace),
	}); err != nil {
		return &ctldapi.PublishRootFSSnapshotResponse{}, err
	}
	prepared, err := s.ctldClient.PrepareRootFSSnapshotWithTimeout(ctx, ctldAddress, prepareReq, sandboxRootFSOperationTimeout)
	if err != nil {
		resp := &ctldapi.PublishRootFSSnapshotResponse{}
		if prepared != nil {
			resp.Info = prepared.Info
			resp.Descriptor = prepared.Descriptor
			resp.Error = prepared.Error
		}
		return resp, err
	}
	objectKey, err := defaultSandboxRootFSObjectKey(teamID, sandboxID, generation, prepared.Descriptor.Digest)
	if err != nil {
		_, _ = s.ctldClient.AbortRootFSSnapshotWithTimeout(context.Background(), ctldAddress, rootFSAbortRequest(prepared.Handle, teamID, sandboxID, generation), sandboxRootFSOperationTimeout)
		return &ctldapi.PublishRootFSSnapshotResponse{Info: prepared.Info, Descriptor: prepared.Descriptor}, err
	}
	pendingState := rootFSStateFromPreparedSnapshot(sandboxID, teamID, generation, layerID, objectKey, prepared)
	if err := s.prepareRootFSObjectPublish(ctx, prepareReq.StageID, pendingState, s.now().Add(sandboxRootFSUncommittedObjectDeleteDelay)); err != nil {
		_, _ = s.ctldClient.AbortRootFSSnapshotWithTimeout(context.Background(), ctldAddress, rootFSAbortRequest(prepared.Handle, teamID, sandboxID, generation), sandboxRootFSOperationTimeout)
		return &ctldapi.PublishRootFSSnapshotResponse{Info: prepared.Info, Descriptor: prepared.Descriptor}, err
	}
	published, err := s.ctldClient.PublishRootFSSnapshotWithTimeout(ctx, ctldAddress, ctldapi.PublishRootFSSnapshotRequest{
		Handle:                    prepared.Handle,
		SandboxID:                 sandboxID,
		TeamID:                    teamID,
		ExpectedRuntimeGeneration: generation,
		ObjectKey:                 objectKey,
	}, sandboxRootFSOperationTimeout)
	if err != nil {
		_, _ = s.ctldClient.AbortRootFSSnapshotWithTimeout(context.Background(), ctldAddress, rootFSAbortRequest(prepared.Handle, teamID, sandboxID, generation), sandboxRootFSOperationTimeout)
		s.deleteUncommittedRootFSObject(pendingState, "rootfs snapshot publish failed")
		resp := &ctldapi.PublishRootFSSnapshotResponse{Info: prepared.Info, Descriptor: prepared.Descriptor}
		if published != nil {
			resp.Info = published.Info
			resp.Descriptor = published.Descriptor
			resp.Error = published.Error
		}
		return resp, err
	}
	return published, nil
}

func rootFSAbortRequest(handle, teamID, sandboxID string, generation int64) ctldapi.AbortRootFSSnapshotRequest {
	return ctldapi.AbortRootFSSnapshotRequest{
		Handle:                    handle,
		TeamID:                    strings.TrimSpace(teamID),
		SandboxID:                 strings.TrimSpace(sandboxID),
		ExpectedRuntimeGeneration: generation,
	}
}

func (s *SandboxService) shouldSquashSandboxRootFSCheckpoint(state *SandboxRootFSState) (bool, string) {
	if s == nil || s.config.RootFSSquashDisabled || state == nil {
		return false, ""
	}
	depth := len(state.LayerChain)
	if depth == 0 && strings.TrimSpace(state.LayerID) != "" {
		depth = 1
	}
	if maxDepth := s.config.RootFSSquashMaxChainDepth; maxDepth > 0 && depth >= maxDepth {
		return true, fmt.Sprintf("chain_depth:%d", depth)
	}
	if maxBytes := s.config.RootFSSquashMaxChainBytes; maxBytes > 0 {
		var totalBytes int64
		for _, layer := range state.LayerChain {
			if layer != nil && layer.DiffSize > 0 {
				totalBytes += layer.DiffSize
			}
		}
		if totalBytes == 0 && state.DiffSize > 0 {
			totalBytes = state.DiffSize
		}
		if totalBytes >= maxBytes {
			return true, fmt.Sprintf("chain_bytes:%d", totalBytes)
		}
	}
	return false, ""
}

func (s *SandboxService) applySandboxRootFSCheckpoint(ctx context.Context, pod *corev1.Pod, state *SandboxRootFSState) error {
	if state == nil {
		return nil
	}
	if s == nil || !s.config.CtldEnabled || s.ctldClient == nil {
		return fmt.Errorf("ctld is required to restore sandbox rootfs checkpoint")
	}
	if pod == nil {
		return fmt.Errorf("pod is nil")
	}
	ctldAddress, err := s.ctldAddressForPod(ctx, pod)
	if err != nil {
		return err
	}
	req := ctldapi.ApplyRootFSRequest{
		Target:                      rootFSTargetForPod(pod),
		TeamID:                      state.TeamID,
		SandboxID:                   state.SandboxID,
		ExpectedRuntime:             state.Runtime,
		ExpectedRuntimeHandler:      state.RuntimeHandler,
		ExpectedSnapshotter:         state.Snapshotter,
		ExpectedBaseImageDigest:     state.BaseImageDigest,
		ExpectedSnapshotParent:      state.SnapshotParent,
		ExpectedSnapshotParentChain: append([]string(nil), state.SnapshotParentChain...),
		Descriptor: ctldapi.RootFSDiffDescriptor{
			MediaType: state.DiffMediaType,
			Digest:    state.DiffDigest,
			DiffID:    state.DiffID,
			Size:      state.DiffSize,
			ObjectKey: state.DiffObjectKey,
		},
		ExcludedPaths: rootFSExcludedPathsForPod(pod),
	}
	if layers := rootFSLayerDescriptors(state); len(layers) > 0 {
		req.Layers = layers
		req.BaselineLayerID = state.LayerID
		req.Descriptor = ctldapi.RootFSDiffDescriptor{}
	}
	resp, err := s.ctldClient.ApplyRootFSWithTimeout(ctx, ctldAddress, req, sandboxRootFSOperationTimeout)
	if err != nil {
		return fmt.Errorf("apply sandbox rootfs checkpoint: %w", rootFSResponseError(err, applyRootFSError(resp)))
	}
	if resp == nil || !resp.Applied {
		return fmt.Errorf("apply sandbox rootfs checkpoint: ctld did not report applied")
	}
	return nil
}

func rootFSExcludedPathsForPod(pod *corev1.Pod) []string {
	if pod == nil {
		return nil
	}
	var mounts []ClaimMount
	if pod.Annotations != nil {
		mounts = parseClaimMounts(pod.Annotations[controller.AnnotationMounts])
	}
	seen := make(map[string]struct{}, len(mounts)+1)
	out := make([]string, 0, len(mounts)+1)
	add := func(raw string) {
		if raw == "" || !strings.HasPrefix(raw, "/") {
			return
		}
		mountPath := path.Clean(raw)
		if mountPath == "/" {
			return
		}
		if _, ok := seen[mountPath]; ok {
			return
		}
		seen[mountPath] = struct{}{}
		out = append(out, mountPath)
	}
	for _, mount := range mounts {
		add(strings.TrimSpace(mount.MountPoint))
	}
	if pod.Annotations != nil && strings.TrimSpace(pod.Annotations[controller.AnnotationWebhookStateVolumeID]) != "" {
		add(webhookStateMountPoint)
	}
	return out
}

func rootFSLayerDescriptors(state *SandboxRootFSState) []ctldapi.RootFSLayerDescriptor {
	if state == nil {
		return nil
	}
	if len(state.LayerChain) > 0 {
		out := make([]ctldapi.RootFSLayerDescriptor, 0, len(state.LayerChain))
		for _, layer := range state.LayerChain {
			if layer == nil || strings.TrimSpace(layer.ID) == "" {
				continue
			}
			out = append(out, ctldapi.RootFSLayerDescriptor{
				LayerID:       layer.ID,
				ParentLayerID: layer.ParentLayerID,
				Descriptor: ctldapi.RootFSDiffDescriptor{
					MediaType: layer.DiffMediaType,
					Digest:    layer.DiffDigest,
					DiffID:    layer.DiffID,
					Size:      layer.DiffSize,
					ObjectKey: layer.DiffObjectKey,
				},
			})
		}
		if len(out) > 0 {
			return out
		}
	}
	if strings.TrimSpace(state.LayerID) == "" {
		return nil
	}
	return []ctldapi.RootFSLayerDescriptor{{
		LayerID:       state.LayerID,
		ParentLayerID: state.ParentLayerID,
		Descriptor: ctldapi.RootFSDiffDescriptor{
			MediaType: state.DiffMediaType,
			Digest:    state.DiffDigest,
			DiffID:    state.DiffID,
			Size:      state.DiffSize,
			ObjectKey: state.DiffObjectKey,
		},
	}}
}

func (s *SandboxService) latestRootFSState(ctx context.Context, sandboxID string) (*SandboxRootFSState, error) {
	if s == nil || s.sandboxStore == nil {
		return nil, nil
	}
	return s.sandboxStore.GetLatestRootFSState(ctx, sandboxID)
}

func rootFSTargetForPod(pod *corev1.Pod) ctldapi.RootFSContainerRef {
	if pod == nil {
		return ctldapi.RootFSContainerRef{ContainerName: sandboxRootFSContainerName}
	}
	return ctldapi.RootFSContainerRef{
		Namespace:     pod.Namespace,
		PodName:       pod.Name,
		PodUID:        string(pod.UID),
		ContainerName: sandboxRootFSContainerName,
	}
}

func rootFSStateFromPublishResponse(sandboxID, teamID string, generation int64, resp *ctldapi.PublishRootFSSnapshotResponse) (*SandboxRootFSState, error) {
	if resp == nil {
		return nil, fmt.Errorf("save sandbox rootfs checkpoint: empty ctld response")
	}
	if strings.TrimSpace(resp.Descriptor.Digest) == "" {
		return nil, fmt.Errorf("save sandbox rootfs checkpoint: diff digest is empty")
	}
	if strings.TrimSpace(resp.Descriptor.ObjectKey) == "" {
		return nil, fmt.Errorf("save sandbox rootfs checkpoint: diff object key is empty")
	}
	return &SandboxRootFSState{
		SandboxID:           sandboxID,
		TeamID:              teamID,
		RuntimeGeneration:   generation,
		Runtime:             resp.Info.Runtime,
		RuntimeHandler:      resp.Info.RuntimeHandler,
		BaseImageRef:        resp.Info.BaseImageRef,
		BaseImageDigest:     resp.Info.BaseImageDigest,
		Snapshotter:         resp.Info.Snapshotter,
		SnapshotParent:      resp.Info.SnapshotParent,
		SnapshotParentChain: append([]string(nil), resp.Info.SnapshotParentChain...),
		DiffDigest:          resp.Descriptor.Digest,
		DiffID:              resp.Descriptor.DiffID,
		DiffMediaType:       resp.Descriptor.MediaType,
		DiffSize:            resp.Descriptor.Size,
		DiffObjectKey:       resp.Descriptor.ObjectKey,
	}, nil
}

func rootFSStateFromPreparedSnapshot(sandboxID, teamID string, generation int64, layerID, objectKey string, prepared *ctldapi.PrepareRootFSSnapshotResponse) *SandboxRootFSState {
	if prepared == nil {
		return nil
	}
	return &SandboxRootFSState{
		LayerID:             layerID,
		SandboxID:           sandboxID,
		TeamID:              teamID,
		RuntimeGeneration:   generation,
		Runtime:             prepared.Info.Runtime,
		RuntimeHandler:      prepared.Info.RuntimeHandler,
		BaseImageRef:        prepared.Info.BaseImageRef,
		BaseImageDigest:     prepared.Info.BaseImageDigest,
		Snapshotter:         prepared.Info.Snapshotter,
		SnapshotParent:      prepared.Info.SnapshotParent,
		SnapshotParentChain: append([]string(nil), prepared.Info.SnapshotParentChain...),
		DiffDigest:          prepared.Descriptor.Digest,
		DiffID:              prepared.Descriptor.DiffID,
		DiffMediaType:       prepared.Descriptor.MediaType,
		DiffSize:            prepared.Descriptor.Size,
		DiffObjectKey:       objectKey,
	}
}

func defaultSandboxRootFSObjectKey(teamID, sandboxID string, generation int64, digest string) (string, error) {
	teamID = strings.TrimSpace(teamID)
	sandboxID = strings.TrimSpace(sandboxID)
	if teamID == "" {
		return "", fmt.Errorf("team_id is required when rootfs object key is omitted")
	}
	if sandboxID == "" {
		return "", fmt.Errorf("sandbox_id is required when rootfs object key is omitted")
	}
	if strings.Contains(teamID, "/") || strings.Contains(sandboxID, "/") {
		return "", fmt.Errorf("team_id and sandbox_id cannot contain '/'")
	}
	parts := strings.SplitN(strings.TrimSpace(digest), ":", 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return "", fmt.Errorf("invalid rootfs diff digest %q", digest)
	}
	return path.Join("sandbox-rootfs", teamID, sandboxID, fmt.Sprintf("%d", generation), parts[0], parts[1]+".tar"), nil
}

func (s *SandboxService) queueUncommittedRootFSObjectDeletion(ctx context.Context, state *SandboxRootFSState, notBefore time.Time) error {
	if state == nil || strings.TrimSpace(state.DiffObjectKey) == "" {
		return nil
	}
	store, ok := s.sandboxStore.(interface {
		QueueUncommittedRootFSObjectDeletion(context.Context, *SandboxRootFSState, time.Time) error
	})
	if !ok || store == nil {
		return nil
	}
	return store.QueueUncommittedRootFSObjectDeletion(ctx, state, notBefore)
}

func (s *SandboxService) prepareRootFSObjectPublish(
	ctx context.Context,
	stageID string,
	state *SandboxRootFSState,
	notBefore time.Time,
) error {
	if s == nil {
		return fmt.Errorf("sandbox service is required for rootfs publish")
	}
	if s.teamQuotaStore == nil {
		return fmt.Errorf("%w: capacity store is not configured for rootfs publish", ErrTeamQuotaUnavailable)
	}
	quotaStore, ok := s.teamQuotaStore.(teamquota.CapacityTxStore)
	if !ok || quotaStore == nil {
		return fmt.Errorf("%w: transactional capacity store is required for rootfs publish", ErrTeamQuotaUnavailable)
	}
	store, ok := s.sandboxStore.(interface {
		PrepareRootFSObjectPublish(
			context.Context,
			string,
			*SandboxRootFSState,
			time.Time,
			teamquota.CapacityTxStore,
		) error
	})
	if !ok || store == nil {
		return fmt.Errorf("%w: postgres rootfs object publish store is not configured", ErrTeamQuotaUnavailable)
	}
	return store.PrepareRootFSObjectPublish(ctx, stageID, state, notBefore, quotaStore)
}

func (s *SandboxService) prepareRootFSPublishStage(
	ctx context.Context,
	stage RootFSPublishStage,
) error {
	if s == nil {
		return fmt.Errorf("sandbox service is required for rootfs publish staging")
	}
	quotaStore, ok := s.teamQuotaStore.(teamquota.CapacityTxStore)
	if !ok || quotaStore == nil {
		return fmt.Errorf("%w: transactional capacity store is required for rootfs publish staging", ErrTeamQuotaUnavailable)
	}
	store, ok := s.sandboxStore.(interface {
		PrepareRootFSPublishStage(
			context.Context,
			RootFSPublishStage,
			teamquota.CapacityTxStore,
		) error
	})
	if !ok || store == nil {
		return fmt.Errorf("%w: postgres rootfs publish stage store is not configured", ErrTeamQuotaUnavailable)
	}
	return store.PrepareRootFSPublishStage(ctx, stage, quotaStore)
}

func (s *SandboxService) recoverDueRootFSPublishStages(ctx context.Context, limit int) error {
	if s == nil {
		return fmt.Errorf("sandbox service is required for rootfs publish stage recovery")
	}
	store, ok := s.sandboxStore.(interface {
		ListDueRootFSPublishStages(context.Context, int) ([]RootFSPublishStage, error)
		ReleaseRootFSPublishStage(context.Context, string, teamquota.CapacityTxStore) (bool, error)
	})
	if !ok || store == nil {
		return fmt.Errorf("%w: postgres rootfs publish stage recovery store is not configured", ErrTeamQuotaUnavailable)
	}
	stages, err := store.ListDueRootFSPublishStages(ctx, limit)
	if err != nil {
		return err
	}
	if len(stages) == 0 {
		return nil
	}
	quotaStore, ok := s.teamQuotaStore.(teamquota.CapacityTxStore)
	if !ok || quotaStore == nil {
		return fmt.Errorf("%w: transactional capacity store is required for rootfs publish stage recovery", ErrTeamQuotaUnavailable)
	}
	if s.ctldClient == nil {
		return fmt.Errorf("ctld client is required for rootfs publish stage recovery")
	}
	var recoveryErrs []error
	for _, stage := range stages {
		abortResp, abortErr := s.ctldClient.AbortRootFSSnapshotWithTimeout(
			ctx,
			stage.CtldAddress,
			rootFSAbortRequest(stage.StageID, stage.TeamID, stage.SandboxID, stage.RuntimeGeneration),
			sandboxRootFSOperationTimeout,
		)
		if abortErr != nil || abortResp == nil || !abortResp.Aborted {
			if abortErr == nil {
				abortErr = fmt.Errorf("ctld did not confirm rootfs stage removal")
			}
			recoveryErrs = append(recoveryErrs, fmt.Errorf(
				"confirm rootfs publish stage %s absent on ctld: %w",
				stage.StageID,
				abortErr,
			))
			continue
		}
		if _, err := store.ReleaseRootFSPublishStage(ctx, stage.StageID, quotaStore); err != nil {
			recoveryErrs = append(recoveryErrs, fmt.Errorf(
				"release confirmed rootfs publish stage %s: %w",
				stage.StageID,
				err,
			))
		}
	}
	return errors.Join(recoveryErrs...)
}

func (s *SandboxService) deleteUncommittedRootFSObject(state *SandboxRootFSState, reason string) {
	if state == nil || strings.TrimSpace(state.DiffObjectKey) == "" {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), sandboxRootFSUncommittedObjectDeleteTimeout)
	defer cancel()
	if err := s.queueUncommittedRootFSObjectDeletion(ctx, state, time.Now()); err != nil && s.logger != nil {
		s.logger.Warn("Failed to queue uncommitted rootfs object deletion",
			zap.String("sandboxID", state.SandboxID),
			zap.String("objectKey", state.DiffObjectKey),
			zap.String("reason", reason),
			zap.Error(err),
		)
	}
}

func publishRootFSError(resp *ctldapi.PublishRootFSSnapshotResponse) string {
	if resp == nil {
		return ""
	}
	return strings.TrimSpace(resp.Error)
}

func applyRootFSError(resp *ctldapi.ApplyRootFSResponse) string {
	if resp == nil {
		return ""
	}
	return strings.TrimSpace(resp.Error)
}

func rootFSBaselineMissing(err error, resp *ctldapi.PublishRootFSSnapshotResponse) bool {
	var reqErr *ctldapi.RequestError
	if !errors.As(err, &reqErr) || reqErr == nil || reqErr.StatusCode != http.StatusNotFound {
		return false
	}
	message := strings.ToLower(publishRootFSError(resp))
	return strings.Contains(message, "baseline") && strings.Contains(message, "not captured")
}

func rootFSResponseError(err error, message string) error {
	if err == nil {
		return nil
	}
	if strings.TrimSpace(message) == "" {
		return err
	}
	return fmt.Errorf("%w: %s", err, message)
}

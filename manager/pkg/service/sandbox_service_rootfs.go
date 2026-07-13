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
	godigest "github.com/opencontainers/go-digest"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/controller"
	"github.com/sandbox0-ai/sandbox0/pkg/ctldapi"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
)

const sandboxRootFSContainerName = "procd"

const sandboxRootFSOperationTimeout = 5 * time.Minute
const sandboxRootFSAbortTimeout = 5 * time.Second
const sandboxRootFSSourceCheckpointLifecycleStaleAfter = sandboxRootFSOperationTimeout + time.Minute
const sandboxRootFSUncommittedObjectDeleteDelay = 15 * time.Minute
const sandboxRootFSUncommittedObjectDeleteTimeout = 30 * time.Second

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
	if loadedParentState, err := s.latestRootFSState(ctx, sandboxID); err != nil {
		return nil, fmt.Errorf("load current rootfs head: %w", err)
	} else if loadedParentState != nil {
		parentState = loadedParentState
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
	prepareReq := ctldapi.PrepareRootFSSnapshotRequest{
		Target:        rootFSTargetForPod(pod),
		ParentLayerID: parentLayerID,
		ExcludedPaths: rootFSExcludedPathsForPod(pod),
	}
	layerID := uuid.NewString()
	prepared, err := s.ctldClient.PrepareRootFSSnapshotWithTimeout(ctx, ctldAddress, prepareReq, sandboxRootFSOperationTimeout)
	if err != nil && parentLayerID != "" && rootFSBaselineMissing(err, prepareRootFSError(prepared)) {
		parentLayerID = ""
		prepareReq.ParentLayerID = ""
		prepared, err = s.ctldClient.PrepareRootFSSnapshotWithTimeout(ctx, ctldAddress, prepareReq, sandboxRootFSOperationTimeout)
	}
	if err != nil {
		return nil, fmt.Errorf("save sandbox rootfs checkpoint: %w", rootFSResponseError(err, prepareRootFSError(prepared)))
	}
	var discardedPrepared *ctldapi.PrepareRootFSSnapshotResponse
	if parentLayerID != "" {
		if squash, reason := s.shouldSquashSandboxRootFSDeletion(parentState, prepared.DiffStats); squash {
			squashReq := prepareReq
			squashReq.ParentLayerID = ""
			squashedPrepared, squashErr := s.ctldClient.PrepareRootFSSnapshotWithTimeout(ctx, ctldAddress, squashReq, sandboxRootFSOperationTimeout)
			if squashErr != nil {
				if s.logger != nil {
					s.logger.Warn("Skipping sandbox rootfs deletion squash because the replacement snapshot could not be prepared",
						zap.String("sandboxID", sandboxID),
						zap.String("headLayerID", expectedHeadLayerID),
						zap.String("reason", reason),
						zap.Error(rootFSResponseError(squashErr, prepareRootFSError(squashedPrepared))),
					)
				}
			} else {
				if s.logger != nil {
					s.logger.Info("Squashing sandbox rootfs checkpoint after a large deletion",
						zap.String("sandboxID", sandboxID),
						zap.String("headLayerID", expectedHeadLayerID),
						zap.String("reason", reason),
					)
				}
				discardedPrepared = prepared
				prepared = squashedPrepared
				parentLayerID = ""
			}
		}
	}
	resp, err := s.publishPreparedSandboxRootFSSnapshot(ctx, ctldAddress, prepared, sandboxID, teamID, generation, layerID)
	if discardedPrepared != nil {
		if cleanupErr := s.abortPreparedSandboxRootFSSnapshot(ctldAddress, discardedPrepared); cleanupErr != nil && s.logger != nil {
			s.logger.Warn("Failed to clean up discarded incremental sandbox rootfs snapshot",
				zap.String("sandboxID", sandboxID),
				zap.String("headLayerID", expectedHeadLayerID),
				zap.Error(cleanupErr),
			)
		}
	}
	if err != nil {
		return nil, fmt.Errorf("save sandbox rootfs checkpoint: %w", rootFSResponseError(err, saveRootFSError(resp)))
	}
	state, err := rootFSStateFromSaveResponse(sandboxID, teamID, generation, resp)
	if err != nil {
		return nil, err
	}
	state.LayerID = layerID
	state.ParentLayerID = parentLayerID
	state.ExpectedHeadLayerID = expectedHeadLayerID
	return state, nil
}

func (s *SandboxService) publishPreparedSandboxRootFSSnapshot(ctx context.Context, ctldAddress string, prepared *ctldapi.PrepareRootFSSnapshotResponse, sandboxID, teamID string, generation int64, layerID string) (*ctldapi.SaveRootFSResponse, error) {
	if prepared == nil {
		return nil, fmt.Errorf("prepared rootfs snapshot is nil")
	}
	objectKey, err := defaultSandboxRootFSObjectKey(teamID, sandboxID, generation, prepared.Descriptor.Digest)
	if err != nil {
		_ = s.abortPreparedSandboxRootFSSnapshot(ctldAddress, prepared)
		return &ctldapi.SaveRootFSResponse{Info: prepared.Info, Descriptor: prepared.Descriptor}, err
	}
	pendingState := rootFSStateFromPreparedSnapshot(sandboxID, teamID, generation, layerID, objectKey, prepared)
	if err := s.queueUncommittedRootFSObjectDeletion(ctx, pendingState, s.now().Add(sandboxRootFSUncommittedObjectDeleteDelay)); err != nil {
		_ = s.abortPreparedSandboxRootFSSnapshot(ctldAddress, prepared)
		return &ctldapi.SaveRootFSResponse{Info: prepared.Info, Descriptor: prepared.Descriptor}, err
	}
	published, err := s.ctldClient.PublishRootFSSnapshotWithTimeout(ctx, ctldAddress, ctldapi.PublishRootFSSnapshotRequest{
		Handle:                    prepared.Handle,
		SandboxID:                 sandboxID,
		TeamID:                    teamID,
		ExpectedRuntimeGeneration: generation,
		ObjectKey:                 objectKey,
	}, sandboxRootFSOperationTimeout)
	if err != nil {
		_ = s.abortPreparedSandboxRootFSSnapshot(ctldAddress, prepared)
		s.deleteUncommittedRootFSObject(pendingState, "rootfs snapshot publish failed")
		resp := &ctldapi.SaveRootFSResponse{Info: prepared.Info, Descriptor: prepared.Descriptor}
		if published != nil {
			resp.Info = published.Info
			resp.Descriptor = published.Descriptor
			resp.Error = published.Error
		}
		return resp, err
	}
	return &ctldapi.SaveRootFSResponse{
		Info:       published.Info,
		Descriptor: published.Descriptor,
		Error:      published.Error,
	}, nil
}

func (s *SandboxService) abortPreparedSandboxRootFSSnapshot(ctldAddress string, prepared *ctldapi.PrepareRootFSSnapshotResponse) error {
	if prepared == nil || strings.TrimSpace(prepared.Handle) == "" {
		return fmt.Errorf("prepared rootfs snapshot handle is empty")
	}
	ctx, cancel := context.WithTimeout(context.Background(), sandboxRootFSAbortTimeout)
	defer cancel()
	resp, err := s.ctldClient.AbortRootFSSnapshotWithTimeout(ctx, ctldAddress, ctldapi.AbortRootFSSnapshotRequest{Handle: prepared.Handle}, sandboxRootFSAbortTimeout)
	if err != nil {
		return rootFSResponseError(err, abortRootFSError(resp))
	}
	if resp == nil || !resp.Aborted {
		return fmt.Errorf("ctld did not report the prepared rootfs snapshot as aborted")
	}
	return nil
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
		totalBytes := sandboxRootFSChainBytes(state)
		if totalBytes >= maxBytes {
			return true, fmt.Sprintf("chain_bytes:%d", totalBytes)
		}
	}
	return false, ""
}

// shouldSquashSandboxRootFSDeletion decides whether the current deletion delta
// is large enough to justify replacing the reachable chain with a full layer.
func (s *SandboxService) shouldSquashSandboxRootFSDeletion(state *SandboxRootFSState, stats *ctldapi.RootFSDiffStats) (bool, string) {
	if s == nil || s.config.RootFSSquashDisabled || state == nil || stats == nil || stats.DeletedBytes <= 0 {
		return false, ""
	}
	if minBytes := s.config.RootFSSquashMinDeletedBytes; minBytes <= 0 || stats.DeletedBytes < minBytes {
		return false, ""
	}
	chainBytes := sandboxRootFSChainBytes(state)
	if chainBytes <= 0 {
		return false, ""
	}
	ratio := float64(stats.DeletedBytes) / float64(chainBytes)
	if minRatio := s.config.RootFSSquashMinDeletedRatio; minRatio <= 0 || ratio < minRatio {
		return false, ""
	}
	return true, fmt.Sprintf("deleted_bytes:%d,chain_bytes:%d,ratio:%.4f", stats.DeletedBytes, chainBytes, ratio)
}

func sandboxRootFSChainBytes(state *SandboxRootFSState) int64 {
	if state == nil {
		return 0
	}
	var totalBytes int64
	for _, layer := range state.LayerChain {
		if layer != nil && layer.DiffSize > 0 {
			totalBytes += layer.DiffSize
		}
	}
	if totalBytes == 0 && state.DiffSize > 0 {
		totalBytes = state.DiffSize
	}
	return totalBytes
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
		ExpectedRuntime:             state.Runtime,
		ExpectedRuntimeHandler:      state.RuntimeHandler,
		ExpectedSnapshotter:         state.Snapshotter,
		ExpectedBaseImageDigest:     state.BaseImageDigest,
		ExpectedSnapshotParent:      state.SnapshotParent,
		ExpectedSnapshotParentChain: append([]string(nil), state.SnapshotParentChain...),
		Descriptor: ctldapi.RootFSDiffDescriptor{
			MediaType: state.DiffMediaType,
			Digest:    state.DiffDigest,
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

func (s *SandboxService) applySandboxRootFSCheckpointWithFallback(ctx context.Context, pod *corev1.Pod, record *SandboxRecord, template *v1alpha1.SandboxTemplate, req *ClaimRequest, state *SandboxRootFSState, fallbackStatus string) (*corev1.Pod, error) {
	if state == nil {
		return pod, nil
	}
	err := s.applySandboxRootFSCheckpoint(ctx, pod, state)
	if err == nil {
		return pod, nil
	}
	fallbackTemplate, fallbackErr := templateWithCheckpointBaseImage(template, state)
	if fallbackErr != nil {
		return pod, fmt.Errorf("%w; checkpoint base image fallback unavailable: %v", err, fallbackErr)
	}
	if s != nil && s.logger != nil {
		s.logger.Warn("Rootfs force-apply failed; retrying with checkpoint base image",
			zap.String("sandboxID", state.SandboxID),
			zap.String("baseImageRef", state.BaseImageRef),
			zap.String("baseImageDigest", state.BaseImageDigest),
			zap.Error(err),
		)
	}
	s.requestSandboxDeletionAfterClaimFailure(pod, "rootfs force-apply failed")

	fallbackPod, fallbackErr := s.createNewPod(ctx, fallbackTemplate, req)
	if fallbackErr != nil {
		return pod, fmt.Errorf("%w; create checkpoint base image runtime: %v", err, fallbackErr)
	}
	networkPod, fallbackErr := s.waitForColdPodNetworkPolicy(ctx, fallbackPod, req.TeamID)
	if fallbackErr != nil {
		s.requestSandboxDeletionAfterClaimFailure(fallbackPod, "checkpoint base image network policy failed")
		return fallbackPod, fmt.Errorf("%w; prepare checkpoint base image network policy: %v", err, fallbackErr)
	}
	fallbackPod = networkPod
	readyPod, fallbackErr := s.waitForPodClaimReady(ctx, fallbackPod.Namespace, fallbackPod.Name)
	if fallbackErr != nil {
		s.requestSandboxDeletionAfterClaimFailure(fallbackPod, "checkpoint base image runtime readiness failed")
		return fallbackPod, fmt.Errorf("%w; wait for checkpoint base image runtime: %v", err, fallbackErr)
	}
	if fallbackErr := s.applySandboxRootFSCheckpoint(ctx, readyPod, state); fallbackErr != nil {
		s.requestSandboxDeletionAfterClaimFailure(readyPod, "checkpoint base image rootfs apply failed")
		return readyPod, fmt.Errorf("%w; checkpoint base image retry failed: %v", err, fallbackErr)
	}
	if strings.TrimSpace(fallbackStatus) != "" {
		if fallbackErr := s.saveRestoredRuntimePod(ctx, readyPod, record, fallbackStatus); fallbackErr != nil {
			s.requestSandboxDeletionAfterClaimFailure(readyPod, "checkpoint base image runtime persistence failed")
			return readyPod, fallbackErr
		}
	}
	return readyPod, nil
}

func (s *SandboxService) saveRestoredRuntimePod(ctx context.Context, pod *corev1.Pod, record *SandboxRecord, status string) error {
	if s == nil || s.sandboxStore == nil || pod == nil || record == nil {
		return nil
	}
	sandboxID := strings.TrimSpace(record.ID)
	if sandboxID == "" {
		sandboxID = sandboxIDFromPod(pod)
	}
	if sandboxID == "" {
		return fmt.Errorf("sandbox_id is required")
	}
	return s.sandboxStore.WithSandboxLock(ctx, sandboxID, func(lockCtx context.Context, tx SandboxStoreTx, _ *SandboxRecord) error {
		return tx.SaveRuntime(lockCtx, sandboxID, pod.Namespace, pod.Name, status, runtimeGenerationFromPod(pod), parseRFC3339AnnotationTime(pod.Annotations, controller.AnnotationExpiresAt), parseRFC3339AnnotationTime(pod.Annotations, controller.AnnotationHardExpiresAt), sandboxRuntimeMetadataFromPod(pod))
	})
}

func templateWithCheckpointBaseImage(template *v1alpha1.SandboxTemplate, state *SandboxRootFSState) (*v1alpha1.SandboxTemplate, error) {
	if template == nil {
		return nil, fmt.Errorf("template is required")
	}
	image, err := checkpointBaseImageRef(state)
	if err != nil {
		return nil, err
	}
	clone := template.DeepCopy()
	clone.Spec.MainContainer.Image = image
	clone.Spec.MainContainer.ImagePullPolicy = string(corev1.PullIfNotPresent)
	return clone, nil
}

func checkpointBaseImageRef(state *SandboxRootFSState) (string, error) {
	if state == nil {
		return "", fmt.Errorf("rootfs state is required")
	}
	repo := imageRepositoryFromRef(state.BaseImageRef)
	if repo == "" {
		return "", fmt.Errorf("base image ref is required")
	}
	digestValue := strings.TrimSpace(state.BaseImageDigest)
	if digestValue == "" {
		return "", fmt.Errorf("base image digest is required")
	}
	parsed, err := godigest.Parse(digestValue)
	if err != nil {
		return "", fmt.Errorf("base image digest %q is invalid: %w", digestValue, err)
	}
	return repo + "@" + parsed.String(), nil
}

func imageRepositoryFromRef(ref string) string {
	ref = strings.TrimSpace(ref)
	if ref == "" {
		return ""
	}
	if idx := strings.LastIndex(ref, "@"); idx >= 0 {
		ref = ref[:idx]
	}
	lastSlash := strings.LastIndex(ref, "/")
	lastColon := strings.LastIndex(ref, ":")
	if lastColon > lastSlash {
		ref = ref[:lastColon]
	}
	return strings.TrimSpace(ref)
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

func rootFSStateFromSaveResponse(sandboxID, teamID string, generation int64, resp *ctldapi.SaveRootFSResponse) (*SandboxRootFSState, error) {
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
	if s.rootFSObjectDeleter == nil {
		if s.logger != nil {
			s.logger.Warn("Uncommitted rootfs object deletion deferred; object deleter is not configured",
				zap.String("sandboxID", state.SandboxID),
				zap.String("objectKey", state.DiffObjectKey),
				zap.String("reason", reason),
			)
		}
		return
	}
	if err := s.rootFSObjectDeleter.Delete(state.DiffObjectKey); err != nil {
		if s.logger != nil {
			s.logger.Warn("Failed to delete uncommitted rootfs object",
				zap.String("sandboxID", state.SandboxID),
				zap.String("objectKey", state.DiffObjectKey),
				zap.String("reason", reason),
				zap.Error(err),
			)
		}
		return
	}
	store, ok := s.sandboxStore.(interface {
		CompleteRootFSObjectDeletion(context.Context, string) error
	})
	if ok && store != nil {
		if err := store.CompleteRootFSObjectDeletion(ctx, state.DiffObjectKey); err != nil && s.logger != nil {
			s.logger.Warn("Failed to mark uncommitted rootfs object deleted",
				zap.String("sandboxID", state.SandboxID),
				zap.String("objectKey", state.DiffObjectKey),
				zap.Error(err),
			)
		}
	}
}

func saveRootFSError(resp *ctldapi.SaveRootFSResponse) string {
	if resp == nil {
		return ""
	}
	return strings.TrimSpace(resp.Error)
}

func prepareRootFSError(resp *ctldapi.PrepareRootFSSnapshotResponse) string {
	if resp == nil {
		return ""
	}
	return strings.TrimSpace(resp.Error)
}

func abortRootFSError(resp *ctldapi.AbortRootFSSnapshotResponse) string {
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

func rootFSBaselineMissing(err error, message string) bool {
	var reqErr *ctldapi.RequestError
	if !errors.As(err, &reqErr) || reqErr == nil || reqErr.StatusCode != http.StatusNotFound {
		return false
	}
	message = strings.ToLower(strings.TrimSpace(message))
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

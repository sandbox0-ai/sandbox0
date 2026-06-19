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

func (s *SandboxService) saveSandboxRootFSCheckpoint(ctx context.Context, pod *corev1.Pod, record *SandboxRecord, tx SandboxStoreTx) error {
	if s == nil || !s.config.CtldEnabled || s.ctldClient == nil || pod == nil {
		return nil
	}
	if record == nil {
		return nil
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
		return fmt.Errorf("team_id is required to save sandbox rootfs checkpoint")
	}

	ctldAddress, err := s.ctldAddressForPod(ctx, pod)
	if err != nil {
		return err
	}
	generation := runtimeGenerationFromPod(pod)
	parentLayerID := ""
	expectedHeadLayerID := ""
	if parentState, err := s.latestRootFSState(ctx, sandboxID); err != nil {
		return fmt.Errorf("load current rootfs head: %w", err)
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
	saveReq := ctldapi.SaveRootFSRequest{
		Target:                    rootFSTargetForPod(pod),
		SandboxID:                 sandboxID,
		TeamID:                    teamID,
		ExpectedRuntimeGeneration: generation,
		ParentLayerID:             parentLayerID,
		ExcludedPaths:             rootFSExcludedPathsForBoundMounts(pod, rootFSBoundMountsForCheckpoint(pod, record)),
	}
	resp, err := s.ctldClient.SaveRootFSWithTimeout(ctx, ctldAddress, saveReq, sandboxRootFSOperationTimeout)
	if err != nil && parentLayerID != "" && rootFSBaselineMissing(err, resp) {
		parentLayerID = ""
		saveReq.ParentLayerID = ""
		resp, err = s.ctldClient.SaveRootFSWithTimeout(ctx, ctldAddress, saveReq, sandboxRootFSOperationTimeout)
	}
	if err != nil {
		return fmt.Errorf("save sandbox rootfs checkpoint: %w", rootFSResponseError(err, saveRootFSError(resp)))
	}
	state, err := rootFSStateFromSaveResponse(sandboxID, teamID, generation, resp)
	if err != nil {
		return err
	}
	state.LayerID = uuid.NewString()
	state.ParentLayerID = parentLayerID
	state.ExpectedHeadLayerID = expectedHeadLayerID
	if tx != nil {
		return tx.SaveRootFSState(ctx, state)
	}
	if s.sandboxStore != nil {
		return s.sandboxStore.SaveRootFSState(ctx, state)
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

func (s *SandboxService) applySandboxRootFSCheckpoint(ctx context.Context, pod *corev1.Pod, state *SandboxRootFSState, mounts []ClaimMount) error {
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
		ExcludedPaths: rootFSExcludedPathsForBoundMounts(pod, mounts),
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

func rootFSBoundMountsForCheckpoint(pod *corev1.Pod, record *SandboxRecord) []ClaimMount {
	if record != nil && len(record.Mounts) > 0 {
		return record.Mounts
	}
	return parseClaimMountsFromPod(pod)
}

func parseClaimMountsFromPod(pod *corev1.Pod) []ClaimMount {
	if pod == nil || pod.Annotations == nil {
		return nil
	}
	return parseClaimMounts(pod.Annotations[controller.AnnotationMounts])
}

func rootFSExcludedPathsForBoundMounts(pod *corev1.Pod, mounts []ClaimMount) []string {
	seen := make(map[string]struct{}, len(mounts)+1)
	out := make([]string, 0, len(mounts)+1)
	for _, mount := range mounts {
		out = appendRootFSExcludedPath(out, seen, mount.MountPoint)
	}
	if pod != nil && pod.Annotations != nil && strings.TrimSpace(pod.Annotations[controller.AnnotationWebhookStateVolumeID]) != "" {
		out = appendRootFSExcludedPath(out, seen, webhookStateMountPoint)
	}
	return out
}

func appendRootFSExcludedPath(out []string, seen map[string]struct{}, raw string) []string {
	raw = strings.TrimSpace(raw)
	if raw == "" || !strings.HasPrefix(raw, "/") {
		return out
	}
	mountPath := path.Clean(raw)
	if mountPath == "/" {
		return out
	}
	if _, ok := seen[mountPath]; ok {
		return out
	}
	seen[mountPath] = struct{}{}
	return append(out, mountPath)
}

func (s *SandboxService) applySandboxRootFSCheckpointWithFallback(ctx context.Context, pod *corev1.Pod, record *SandboxRecord, template *v1alpha1.SandboxTemplate, req *ClaimRequest, state *SandboxRootFSState, fallbackStatus string) (*corev1.Pod, error) {
	if state == nil {
		return pod, nil
	}
	if strings.TrimSpace(fallbackStatus) == "" {
		fallbackStatus = SandboxStatusResuming
	}
	mounts := claimMountsFromRequest(req)
	err := s.applySandboxRootFSCheckpoint(ctx, pod, state, mounts)
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
	readyPod, fallbackErr := s.waitForPodClaimReady(ctx, fallbackPod.Namespace, fallbackPod.Name)
	if fallbackErr != nil {
		s.requestSandboxDeletionAfterClaimFailure(fallbackPod, "checkpoint base image runtime readiness failed")
		return fallbackPod, fmt.Errorf("%w; wait for checkpoint base image runtime: %v", err, fallbackErr)
	}
	if fallbackErr := s.saveRestoredRuntimePod(ctx, readyPod, record, fallbackStatus); fallbackErr != nil {
		s.requestSandboxDeletionAfterClaimFailure(readyPod, "checkpoint base image runtime persistence failed")
		return readyPod, fmt.Errorf("%w; save checkpoint base image runtime: %v", err, fallbackErr)
	}
	if fallbackErr := s.applySandboxRootFSCheckpoint(ctx, readyPod, state, mounts); fallbackErr != nil {
		s.requestSandboxDeletionAfterClaimFailure(readyPod, "checkpoint base image rootfs apply failed")
		return readyPod, fmt.Errorf("%w; checkpoint base image retry failed: %v", err, fallbackErr)
	}
	return readyPod, nil
}

func claimMountsFromRequest(req *ClaimRequest) []ClaimMount {
	if req == nil {
		return nil
	}
	return req.Mounts
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
		return tx.SaveRuntime(lockCtx, sandboxID, pod.Namespace, pod.Name, status, runtimeGenerationFromPod(pod), parseRFC3339AnnotationTime(pod.Annotations, controller.AnnotationExpiresAt), parseRFC3339AnnotationTime(pod.Annotations, controller.AnnotationHardExpiresAt))
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

func saveRootFSError(resp *ctldapi.SaveRootFSResponse) string {
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

func rootFSBaselineMissing(err error, resp *ctldapi.SaveRootFSResponse) bool {
	var reqErr *ctldapi.RequestError
	if !errors.As(err, &reqErr) || reqErr == nil || reqErr.StatusCode != http.StatusNotFound {
		return false
	}
	message := strings.ToLower(saveRootFSError(resp))
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

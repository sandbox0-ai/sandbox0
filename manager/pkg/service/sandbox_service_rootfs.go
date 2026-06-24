package service

import (
	"context"
	"encoding/json"
	"fmt"
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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

const sandboxRootFSContainerName = "procd"

const sandboxRootFSOperationTimeout = 5 * time.Minute
const sandboxRootFSMountPathAnnotation = "sandbox0.ai/rootfs-mount-path"

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
	filesystemID := sandboxID
	var parentState *SandboxRootFSState
	if latest, err := s.latestRootFSState(ctx, sandboxID); err != nil {
		return fmt.Errorf("load current rootfs head: %w", err)
	} else if latest != nil {
		parentState = latest
		if rootFSStorageEngine(parentState.StorageEngine) != ctldapi.RootFSStorageEngineS0FS {
			return fmt.Errorf("legacy rootfs checkpoint %s uses unsupported storage engine %q", parentState.LayerID, parentState.StorageEngine)
		}
		if strings.TrimSpace(parentState.S0FSVolumeID) != "" {
			filesystemID = parentState.S0FSVolumeID
		}
		expectedHeadLayerID = strings.TrimSpace(parentState.LayerID)
		parentLayerID = expectedHeadLayerID
	}
	if getter, ok := s.sandboxStore.(interface {
		GetRootFSFilesystem(context.Context, string) (*RootFSFilesystem, error)
	}); ok {
		filesystem, err := getter.GetRootFSFilesystem(ctx, sandboxID)
		if err != nil {
			return fmt.Errorf("load rootfs filesystem: %w", err)
		}
		if filesystem != nil && strings.TrimSpace(filesystem.ID) != "" {
			filesystemID = filesystem.ID
		}
	}
	saveReq := ctldapi.SaveRootFSRequest{
		Target:                    rootFSTargetForPod(pod),
		SandboxID:                 sandboxID,
		TeamID:                    teamID,
		FilesystemID:              filesystemID,
		ExpectedRuntimeGeneration: generation,
		ParentHead:                rootFSHeadDescriptorFromState(parentState),
		ExcludedPaths:             rootFSExcludedPathsForPod(pod),
	}
	resp, err := s.ctldClient.SaveRootFSWithTimeout(ctx, ctldAddress, saveReq, sandboxRootFSOperationTimeout)
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
	filesystemID := strings.TrimSpace(state.S0FSVolumeID)
	if filesystemID == "" {
		filesystemID = sandboxIDFromPod(pod)
	}
	if getter, ok := s.sandboxStore.(interface {
		GetRootFSFilesystem(context.Context, string) (*RootFSFilesystem, error)
	}); ok {
		sandboxID := sandboxIDFromPod(pod)
		if sandboxID != "" {
			filesystem, err := getter.GetRootFSFilesystem(ctx, sandboxID)
			if err != nil {
				return fmt.Errorf("load rootfs filesystem: %w", err)
			}
			if filesystem != nil && strings.TrimSpace(filesystem.ID) != "" {
				filesystemID = filesystem.ID
			}
		}
	}
	head := rootFSHeadDescriptorFromState(state)
	if strings.TrimSpace(head.ManifestKey) == "" {
		return fmt.Errorf("rootfs checkpoint %s uses unsupported storage engine %q", state.LayerID, state.StorageEngine)
	}
	req := ctldapi.ApplyRootFSRequest{
		Target:                      rootFSTargetForPod(pod),
		TeamID:                      state.TeamID,
		FilesystemID:                filesystemID,
		ExpectedRuntime:             state.Runtime,
		ExpectedRuntimeHandler:      state.RuntimeHandler,
		ExpectedSnapshotter:         state.Snapshotter,
		ExpectedBaseImageDigest:     state.BaseImageDigest,
		ExpectedSnapshotParent:      state.SnapshotParent,
		ExpectedSnapshotParentChain: append([]string(nil), state.SnapshotParentChain...),
		Head:                        head,
		ExcludedPaths:               rootFSExcludedPathsForPod(pod),
	}
	resp, err := s.ctldClient.ApplyRootFSWithTimeout(ctx, ctldAddress, req, sandboxRootFSOperationTimeout)
	if err != nil {
		return fmt.Errorf("apply sandbox rootfs checkpoint: %w", rootFSResponseError(err, applyRootFSError(resp)))
	}
	if resp == nil || !resp.Applied {
		return fmt.Errorf("apply sandbox rootfs checkpoint: ctld did not report applied")
	}
	if len(resp.Warnings) > 0 && s.logger != nil {
		s.logger.Warn("Rootfs checkpoint applied with compatibility warnings",
			zap.String("sandboxID", state.SandboxID),
			zap.Strings("warnings", resp.Warnings),
		)
	}
	if strings.TrimSpace(resp.MountPath) != "" {
		if err := s.setSandboxRootFSMountPathAnnotation(ctx, pod, strings.TrimSpace(resp.MountPath)); err != nil {
			return err
		}
	}
	return nil
}

func (s *SandboxService) setSandboxRootFSMountPathAnnotation(ctx context.Context, pod *corev1.Pod, mountPath string) error {
	mountPath = strings.TrimSpace(mountPath)
	if pod == nil || mountPath == "" {
		return nil
	}
	if pod.Annotations == nil {
		pod.Annotations = make(map[string]string)
	}
	pod.Annotations[sandboxRootFSMountPathAnnotation] = mountPath
	if s == nil || s.k8sClient == nil || pod.Namespace == "" || pod.Name == "" {
		return nil
	}
	patch := map[string]any{
		"metadata": map[string]any{
			"annotations": map[string]string{
				sandboxRootFSMountPathAnnotation: mountPath,
			},
		},
	}
	raw, err := json.Marshal(patch)
	if err != nil {
		return fmt.Errorf("marshal rootfs mount annotation patch: %w", err)
	}
	updated, err := s.k8sClient.CoreV1().Pods(pod.Namespace).Patch(ctx, pod.Name, types.MergePatchType, raw, metav1.PatchOptions{})
	if err != nil {
		return fmt.Errorf("patch rootfs mount annotation: %w", err)
	}
	if updated != nil {
		pod.ResourceVersion = updated.ResourceVersion
		if updated.Annotations != nil {
			if pod.Annotations == nil {
				pod.Annotations = make(map[string]string)
			}
			for key, value := range updated.Annotations {
				pod.Annotations[key] = value
			}
		}
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
	if strings.TrimSpace(fallbackStatus) == "" {
		fallbackStatus = SandboxStatusResuming
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
	readyPod, fallbackErr := s.waitForPodClaimReady(ctx, fallbackPod.Namespace, fallbackPod.Name)
	if fallbackErr != nil {
		s.requestSandboxDeletionAfterClaimFailure(fallbackPod, "checkpoint base image runtime readiness failed")
		return fallbackPod, fmt.Errorf("%w; wait for checkpoint base image runtime: %v", err, fallbackErr)
	}
	if fallbackErr := s.saveRestoredRuntimePod(ctx, readyPod, record, fallbackStatus); fallbackErr != nil {
		s.requestSandboxDeletionAfterClaimFailure(readyPod, "checkpoint base image runtime persistence failed")
		return readyPod, fmt.Errorf("%w; save checkpoint base image runtime: %v", err, fallbackErr)
	}
	if fallbackErr := s.applySandboxRootFSCheckpoint(ctx, readyPod, state); fallbackErr != nil {
		s.requestSandboxDeletionAfterClaimFailure(readyPod, "checkpoint base image rootfs apply failed")
		return readyPod, fmt.Errorf("%w; checkpoint base image retry failed: %v", err, fallbackErr)
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
	if strings.TrimSpace(resp.Head.VolumeID) == "" {
		return nil, fmt.Errorf("save sandbox rootfs checkpoint: s0fs volume id is empty")
	}
	if strings.TrimSpace(resp.Head.ManifestKey) == "" {
		return nil, fmt.Errorf("save sandbox rootfs checkpoint: s0fs manifest key is empty")
	}
	if resp.Head.ManifestSeq == 0 {
		return nil, fmt.Errorf("save sandbox rootfs checkpoint: s0fs manifest seq is empty")
	}
	return &SandboxRootFSState{
		SandboxID:         sandboxID,
		TeamID:            teamID,
		RuntimeGeneration: generation,
		Runtime:           resp.Info.Runtime,
		RuntimeHandler:    resp.Info.RuntimeHandler,
		BaseImageRef:      resp.Info.BaseImageRef,
		BaseImageDigest:   resp.Info.BaseImageDigest,
		Snapshotter:       resp.Info.Snapshotter,
		SnapshotParent:    resp.Info.SnapshotParent,
		SnapshotParentChain: append([]string(nil),
			resp.Info.SnapshotParentChain...),
		StorageEngine:     ctldapi.RootFSStorageEngineS0FS,
		S0FSVolumeID:      resp.Head.VolumeID,
		S0FSManifestKey:   resp.Head.ManifestKey,
		S0FSManifestSeq:   resp.Head.ManifestSeq,
		S0FSCheckpointSeq: resp.Head.CheckpointSeq,
	}, nil
}

func rootFSHeadDescriptorFromState(state *SandboxRootFSState) ctldapi.RootFSHeadDescriptor {
	if state == nil || rootFSStorageEngine(state.StorageEngine) != ctldapi.RootFSStorageEngineS0FS {
		return ctldapi.RootFSHeadDescriptor{}
	}
	return ctldapi.RootFSHeadDescriptor{
		Engine:        ctldapi.RootFSStorageEngineS0FS,
		TeamID:        state.TeamID,
		FilesystemID:  state.S0FSVolumeID,
		VolumeID:      state.S0FSVolumeID,
		ManifestKey:   state.S0FSManifestKey,
		ManifestSeq:   state.S0FSManifestSeq,
		CheckpointSeq: state.S0FSCheckpointSeq,
	}
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

func rootFSResponseError(err error, message string) error {
	if err == nil {
		return nil
	}
	if strings.TrimSpace(message) == "" {
		return err
	}
	return fmt.Errorf("%w: %s", err, message)
}

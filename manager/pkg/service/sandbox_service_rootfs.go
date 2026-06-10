package service

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	godigest "github.com/opencontainers/go-digest"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/controller"
	"github.com/sandbox0-ai/sandbox0/pkg/ctldapi"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
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
	resp, err := s.ctldClient.SaveRootFSWithTimeout(ctx, ctldAddress, ctldapi.SaveRootFSRequest{
		Target:                    rootFSTargetForPod(pod),
		SandboxID:                 sandboxID,
		TeamID:                    teamID,
		ExpectedRuntimeGeneration: generation,
	}, sandboxRootFSOperationTimeout)
	if err != nil {
		return fmt.Errorf("save sandbox rootfs checkpoint: %w", rootFSResponseError(err, saveRootFSError(resp)))
	}
	state, err := rootFSStateFromSaveResponse(sandboxID, teamID, pod.Spec.NodeName, generation, resp)
	if err != nil {
		return err
	}
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
	resp, err := s.ctldClient.ApplyRootFSWithTimeout(ctx, ctldAddress, ctldapi.ApplyRootFSRequest{
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
	}, sandboxRootFSOperationTimeout)
	if err != nil {
		return fmt.Errorf("apply sandbox rootfs checkpoint: %w", rootFSResponseError(err, applyRootFSError(resp)))
	}
	if resp == nil || !resp.Applied {
		return fmt.Errorf("apply sandbox rootfs checkpoint: ctld did not report applied")
	}
	return nil
}

func (s *SandboxService) applySandboxRootFSCheckpointWithFallback(ctx context.Context, pod *corev1.Pod, record *SandboxRecord, template *v1alpha1.SandboxTemplate, req *ClaimRequest, state *SandboxRootFSState, claimType string) (*corev1.Pod, error) {
	if state == nil {
		return pod, nil
	}
	if rootFSRequiresCheckpointImageRestore(state) {
		if claimType == "checkpoint-image" {
			return pod, nil
		}
		return nil, fmt.Errorf("runtime %q rootfs checkpoint restore requires a checkpoint image runtime", state.Runtime)
	}
	err := s.applySandboxRootFSCheckpoint(ctx, pod, state)
	if err == nil {
		return pod, nil
	}
	fallbackTemplate, fallbackErr := templateWithCheckpointBaseImage(template, state)
	if fallbackErr != nil {
		return nil, fmt.Errorf("%w; checkpoint base image fallback unavailable: %v", err, fallbackErr)
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
		return nil, fmt.Errorf("%w; create checkpoint base image runtime: %v", err, fallbackErr)
	}
	readyPod, fallbackErr := s.waitForPodClaimReady(ctx, fallbackPod.Namespace, fallbackPod.Name)
	if fallbackErr != nil {
		s.requestSandboxDeletionAfterClaimFailure(fallbackPod, "checkpoint base image runtime readiness failed")
		return nil, fmt.Errorf("%w; wait for checkpoint base image runtime: %v", err, fallbackErr)
	}
	if fallbackErr := s.saveRestoredRuntimePod(ctx, readyPod, record, SandboxStatusResuming); fallbackErr != nil {
		s.requestSandboxDeletionAfterClaimFailure(readyPod, "checkpoint base image runtime persistence failed")
		return nil, fmt.Errorf("%w; save checkpoint base image runtime: %v", err, fallbackErr)
	}
	if fallbackErr := s.applySandboxRootFSCheckpoint(ctx, readyPod, state); fallbackErr != nil {
		s.requestSandboxDeletionAfterClaimFailure(readyPod, "checkpoint base image rootfs apply failed")
		return nil, fmt.Errorf("%w; checkpoint base image retry failed: %v", err, fallbackErr)
	}
	return readyPod, nil
}

type preparedRootFSCheckpointImage struct {
	state *SandboxRootFSState
	image *RootFSCheckpointImage
}

func (p *preparedRootFSCheckpointImage) matches(state *SandboxRootFSState) bool {
	if p == nil || p.state == nil || p.image == nil || state == nil {
		return false
	}
	return p.state.SandboxID == state.SandboxID &&
		p.state.RuntimeGeneration == state.RuntimeGeneration &&
		p.state.DiffDigest == state.DiffDigest &&
		p.state.DiffObjectKey == state.DiffObjectKey &&
		p.state.BaseImageDigest == state.BaseImageDigest
}

func (s *SandboxService) prepublishRootFSCheckpointImage(ctx context.Context, sandboxID string) (*preparedRootFSCheckpointImage, error) {
	if s == nil || s.sandboxStore == nil {
		return nil, nil
	}
	record, err := s.sandboxStore.GetSandbox(ctx, sandboxID)
	if err != nil {
		if errors.Is(err, ErrSandboxRecordNotFound) {
			return nil, nil
		}
		return nil, err
	}
	if record == nil || record.Status != SandboxStatusPaused {
		return nil, nil
	}
	state, err := s.latestRootFSState(ctx, sandboxID)
	if err != nil {
		return nil, err
	}
	if !rootFSRequiresCheckpointImageRestore(state) {
		return nil, nil
	}
	image, err := s.publishRootFSCheckpointImage(ctx, state)
	if err != nil {
		return nil, err
	}
	return &preparedRootFSCheckpointImage{state: state, image: image}, nil
}

func (s *SandboxService) publishRootFSCheckpointImage(ctx context.Context, state *SandboxRootFSState) (*RootFSCheckpointImage, error) {
	if state == nil {
		return nil, fmt.Errorf("rootfs state is required")
	}
	if s == nil || s.rootFSImagePublisher == nil {
		return nil, fmt.Errorf("rootfs checkpoint image publisher is not configured")
	}
	ctldAddress, err := s.rootFSDiffCtldAddress(ctx, state)
	if err != nil {
		return nil, err
	}
	image, err := s.rootFSImagePublisher.Publish(ctx, RootFSCheckpointImagePublishRequest{
		TeamID:      state.TeamID,
		CtldAddress: ctldAddress,
		State:       state,
	})
	if err != nil {
		return nil, err
	}
	if image == nil || strings.TrimSpace(image.PullRef) == "" {
		return nil, fmt.Errorf("rootfs checkpoint image publisher returned no pull ref")
	}
	return image, nil
}

func templateWithRootFSCheckpointImage(template *v1alpha1.SandboxTemplate, image *RootFSCheckpointImage) (*v1alpha1.SandboxTemplate, error) {
	if template == nil {
		return nil, fmt.Errorf("template is required")
	}
	if image == nil || strings.TrimSpace(image.PullRef) == "" {
		return nil, fmt.Errorf("rootfs checkpoint image pull ref is required")
	}
	clone := template.DeepCopy()
	clone.Spec.MainContainer.Image = image.PullRef
	clone.Spec.MainContainer.ImagePullPolicy = string(corev1.PullIfNotPresent)
	return clone, nil
}

func (s *SandboxService) rootFSDiffCtldAddress(ctx context.Context, state *SandboxRootFSState) (string, error) {
	if state != nil && strings.TrimSpace(state.NodeName) != "" {
		addr, err := s.ctldAddressForNodeName(ctx, strings.TrimSpace(state.NodeName))
		if err == nil {
			return addr, nil
		}
		if s != nil && s.logger != nil {
			s.logger.Warn("Failed to resolve rootfs checkpoint source node ctld; trying another node",
				zap.String("sandboxID", state.SandboxID),
				zap.String("node", state.NodeName),
				zap.Error(err),
			)
		}
	}
	if s == nil {
		return "", fmt.Errorf("sandbox service is nil")
	}
	if s.nodeLister != nil {
		nodes, err := s.nodeLister.List(labels.Everything())
		if err == nil {
			for _, node := range nodes {
				addr, addrErr := ctldAddressForNode(node, s.config.CtldPort)
				if addrErr == nil {
					return addr, nil
				}
			}
		}
	}
	if s.k8sClient == nil {
		return "", fmt.Errorf("kubernetes client is not configured")
	}
	nodes, err := s.k8sClient.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return "", fmt.Errorf("list nodes for rootfs checkpoint diff: %w", err)
	}
	for i := range nodes.Items {
		addr, addrErr := ctldAddressForNode(&nodes.Items[i], s.config.CtldPort)
		if addrErr == nil {
			return addr, nil
		}
	}
	return "", fmt.Errorf("no node with an internal ip is available for rootfs checkpoint diff")
}

func rootFSRequiresCheckpointImageRestore(state *SandboxRootFSState) bool {
	if state == nil {
		return false
	}
	runtime := strings.ToLower(strings.TrimSpace(state.Runtime))
	handler := strings.ToLower(strings.TrimSpace(state.RuntimeHandler))
	return runtime == "gvisor" || strings.Contains(handler, "gvisor") || strings.Contains(handler, "runsc")
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

func rootFSStateFromSaveResponse(sandboxID, teamID, nodeName string, generation int64, resp *ctldapi.SaveRootFSResponse) (*SandboxRootFSState, error) {
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
		NodeName:            nodeName,
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

func rootFSResponseError(err error, message string) error {
	if err == nil {
		return nil
	}
	if strings.TrimSpace(message) == "" {
		return err
	}
	return fmt.Errorf("%w: %s", err, message)
}

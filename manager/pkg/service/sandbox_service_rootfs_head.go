package service

import (
	"context"
	"errors"
	"fmt"
	"strings"

	distref "github.com/distribution/reference"
	godigest "github.com/opencontainers/go-digest"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/controller"
	"github.com/sandbox0-ai/sandbox0/pkg/ctldapi"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshead"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/util/retry"
)

var ErrRootFSHeadMigrationRequired = errors.New("rootfs checkpoint requires metadata-head migration")

type rootFSLayerChainByHeadReader interface {
	GetRootFSLayerChainByHead(context.Context, string, string) ([]*SandboxRootFSLayer, error)
}

func (s *SandboxService) prepareClaimRootFSHead(ctx context.Context, req *ClaimRequest) error {
	if req == nil || strings.TrimSpace(req.SnapshotID) == "" {
		return nil
	}
	store, err := s.rootFSProductStore()
	if err != nil {
		return err
	}
	snapshot, err := store.GetRootFSSnapshot(ctx, strings.TrimSpace(req.SnapshotID), strings.TrimSpace(req.TeamID))
	if err != nil {
		return err
	}
	reader, ok := s.sandboxStore.(rootFSLayerChainByHeadReader)
	if !ok || reader == nil {
		return ErrSandboxRootFSStoreUnavailable
	}
	chain, err := reader.GetRootFSLayerChainByHead(ctx, snapshot.TeamID, snapshot.HeadLayerID)
	if err != nil {
		return err
	}
	state := rootFSStateFromLayerChain(req.SandboxID, chain)
	if state == nil {
		return fmt.Errorf("%w: snapshot %s has no rootfs head", ErrRootFSFilesystemNotFound, snapshot.ID)
	}
	return configureClaimRootFSHead(req, state)
}

func configureClaimRootFSHead(req *ClaimRequest, state *SandboxRootFSState) error {
	if req == nil || state == nil {
		return nil
	}
	image := strings.TrimSpace(state.HeadImageRef)
	if image == "" || strings.TrimSpace(state.HeadImageDigest) == "" ||
		strings.TrimSpace(state.HeadObjectKey) == "" ||
		strings.TrimSpace(state.HeadObjectDigest) == "" ||
		state.HeadObjectSize <= 0 || state.HeadObjectMediaType != rootfshead.HeadMediaType {
		return fmt.Errorf("%w: sandbox %s head %s has no complete metadata-only head", ErrRootFSHeadMigrationRequired, state.SandboxID, state.LayerID)
	}
	digestValue, err := digestFromPinnedImage(image)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrRootFSHeadMigrationRequired, err)
	}
	if digestValue.String() != strings.TrimSpace(state.HeadImageDigest) {
		return fmt.Errorf("%w: head image digest %s does not match %s", ErrRootFSHeadMigrationRequired, digestValue, state.HeadImageDigest)
	}
	if _, err := rootFSHeadReferenceFromState(state); err != nil {
		return fmt.Errorf("%w: rootfs head %s is invalid: %v", ErrRootFSHeadMigrationRequired, state.LayerID, err)
	}
	baseImageRef, err := digestPinnedBaseImage(state.BaseImageRef, state.BaseImageDigest)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrRootFSHeadMigrationRequired, err)
	}
	req.RootFSHeadImageRef = image
	req.RootFSHeadLayerID = strings.TrimSpace(state.LayerID)
	req.RootFSBaseImageRef = baseImageRef
	req.RootFSHeadReference, _ = rootFSHeadReferenceFromState(state)
	req.RootFSHeadImage = rootfshead.ImageReference{
		Name:           image,
		ManifestDigest: state.HeadImageDigest,
		Platform:       rootFSPlatformFromState(state),
	}
	if err := req.RootFSHeadImage.Validate(); err != nil {
		return fmt.Errorf("%w: %v", ErrRootFSHeadMigrationRequired, err)
	}
	return nil
}

func digestPinnedBaseImage(imageRef, imageDigest string) (string, error) {
	named, err := distref.ParseNormalizedNamed(strings.TrimSpace(imageRef))
	if err != nil {
		return "", fmt.Errorf("parse rootfs base image %q: %w", imageRef, err)
	}
	digestValue, err := godigest.Parse(strings.TrimSpace(imageDigest))
	if err != nil {
		return "", fmt.Errorf("parse rootfs base image digest %q: %w", imageDigest, err)
	}
	pinned, err := distref.WithDigest(distref.TrimNamed(named), digestValue)
	if err != nil {
		return "", fmt.Errorf("pin rootfs base image: %w", err)
	}
	return pinned.String(), nil
}

func digestFromPinnedImage(image string) (godigest.Digest, error) {
	image = strings.TrimSpace(image)
	separator := strings.LastIndex(image, "@")
	if separator <= 0 || separator == len(image)-1 {
		return "", fmt.Errorf("rootfs head image %q is not digest-pinned", image)
	}
	digestValue, err := godigest.Parse(image[separator+1:])
	if err != nil {
		return "", fmt.Errorf("rootfs head image %q has an invalid digest: %w", image, err)
	}
	return digestValue, nil
}

func applyClaimRootFSHeadToPod(pod *corev1.Pod, req *ClaimRequest) error {
	if pod == nil || req == nil || strings.TrimSpace(req.RootFSHeadImageRef) == "" {
		return nil
	}
	if strings.TrimSpace(req.RootFSHeadLayerID) == "" {
		return fmt.Errorf("rootfs head layer id is required")
	}
	if _, err := digestFromPinnedImage(req.RootFSHeadImageRef); err != nil {
		return err
	}
	containerIndex := procdContainerIndex(pod.Spec.Containers)
	if containerIndex < 0 {
		return fmt.Errorf("pod %s/%s has no %s container", pod.Namespace, pod.Name, sandboxRootFSContainerName)
	}
	pod.Spec.Containers[containerIndex].Image = strings.TrimSpace(req.RootFSHeadImageRef)
	// The image has already been materialized and confirmed through CRI on this
	// node. Never lets kubelet resolve it locally without a registry fallback.
	pod.Spec.Containers[containerIndex].ImagePullPolicy = corev1.PullNever
	if pod.Annotations == nil {
		pod.Annotations = make(map[string]string)
	}
	pod.Annotations[controller.AnnotationRootFSHeadImage] = strings.TrimSpace(req.RootFSHeadImageRef)
	pod.Annotations[controller.AnnotationRootFSHeadLayerID] = strings.TrimSpace(req.RootFSHeadLayerID)
	return nil
}

// applyColdRootFSBaseImageToPod starts a cold carrier with the canonical base
// image. Once Kubernetes has selected a node and unpacked that base, manager
// materializes and activates the node-local metadata head on the same Pod.
func applyColdRootFSBaseImageToPod(pod *corev1.Pod, req *ClaimRequest) error {
	if pod == nil || req == nil || strings.TrimSpace(req.RootFSHeadImageRef) == "" {
		return nil
	}
	baseImageRef := strings.TrimSpace(req.RootFSBaseImageRef)
	if baseImageRef == "" {
		return fmt.Errorf("rootfs base image is required for cold resume")
	}
	containerIndex := procdContainerIndex(pod.Spec.Containers)
	if containerIndex < 0 {
		return fmt.Errorf("pod %s/%s has no %s container", pod.Namespace, pod.Name, sandboxRootFSContainerName)
	}
	pod.Spec.Containers[containerIndex].Image = baseImageRef
	pod.Spec.Containers[containerIndex].ImagePullPolicy = corev1.PullIfNotPresent
	return nil
}

func (s *SandboxService) materializeClaimRootFSHead(ctx context.Context, pod *corev1.Pod, req *ClaimRequest) error {
	if pod == nil || req == nil || strings.TrimSpace(req.RootFSHeadImageRef) == "" {
		return nil
	}
	if s == nil || !s.config.CtldEnabled || s.ctldClient == nil {
		return fmt.Errorf("ctld rootfs head materialization is not configured")
	}
	if err := req.RootFSHeadReference.Validate(); err != nil {
		return err
	}
	if err := req.RootFSHeadImage.Validate(); err != nil {
		return err
	}
	ctldAddress, err := s.ctldAddressForPod(ctx, pod)
	if err != nil {
		return err
	}
	response, err := s.ctldClient.MaterializeRootFSHeadWithTimeout(ctx, ctldAddress, ctldapi.MaterializeRootFSHeadRequest{
		Head: req.RootFSHeadReference, Image: req.RootFSHeadImage,
	}, sandboxRootFSOperationTimeout)
	if err != nil {
		return rootFSResponseError(err, materializeRootFSHeadError(response))
	}
	if response == nil || !response.Materialized || strings.TrimSpace(response.Image) != strings.TrimSpace(req.RootFSHeadImageRef) {
		return fmt.Errorf("ctld did not materialize rootfs head image %s", req.RootFSHeadImageRef)
	}
	return nil
}

func materializeRootFSHeadError(response *ctldapi.MaterializeRootFSHeadResponse) string {
	if response == nil {
		return ""
	}
	return strings.TrimSpace(response.Error)
}

func (s *SandboxService) activateClaimRootFSHead(ctx context.Context, pod *corev1.Pod, req *ClaimRequest) (*corev1.Pod, error) {
	if pod == nil || req == nil || strings.TrimSpace(req.RootFSHeadImageRef) == "" {
		return pod, nil
	}
	containerIndex := procdContainerIndex(pod.Spec.Containers)
	if containerIndex < 0 {
		return pod, fmt.Errorf("pod %s/%s has no %s container", pod.Namespace, pod.Name, sandboxRootFSContainerName)
	}
	if pod.Spec.Containers[containerIndex].Image != req.RootFSHeadImageRef {
		if err := s.materializeClaimRootFSHead(ctx, pod, req); err != nil {
			return pod, err
		}
		updated, err := s.patchPodRootFSHead(ctx, pod.Namespace, pod.Name, req)
		if err != nil {
			return pod, err
		}
		pod = updated
	}
	return s.waitForPodRootFSHeadReady(ctx, pod.Namespace, pod.Name, req.RootFSHeadImageRef, req.RootFSHeadLayerID)
}

func (s *SandboxService) patchPodRootFSHead(ctx context.Context, namespace, name string, req *ClaimRequest) (*corev1.Pod, error) {
	var updated *corev1.Pod
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		current, err := s.k8sClient.CoreV1().Pods(namespace).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return err
		}
		desired := current.DeepCopy()
		if err := applyClaimRootFSHeadToPod(desired, req); err != nil {
			return err
		}
		updated, err = s.k8sClient.CoreV1().Pods(namespace).Update(ctx, desired, metav1.UpdateOptions{})
		return err
	})
	if err != nil {
		return nil, fmt.Errorf("activate node-local rootfs head on pod %s/%s: %w", namespace, name, err)
	}
	return updated, nil
}

func procdContainerIndex(containers []corev1.Container) int {
	for index := range containers {
		if containers[index].Name == sandboxRootFSContainerName {
			return index
		}
	}
	return -1
}

func (s *SandboxService) waitForPodRootFSHeadReady(ctx context.Context, namespace, name, desiredImage, desiredLayerID string) (*corev1.Pod, error) {
	if strings.TrimSpace(desiredImage) == "" {
		if s == nil || s.podLister == nil {
			return nil, fmt.Errorf("pod lister is not configured")
		}
		return s.podLister.Pods(namespace).Get(name)
	}
	timeout := s.config.RuntimeReadyTimeout
	if timeout < defaultPodClaimReadyTimeout {
		timeout = defaultPodClaimReadyTimeout
	}
	readyCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	lastReason := "rootfs head container is not ready"
	evaluate := func() (*corev1.Pod, bool, error) {
		if s.podLister == nil {
			return nil, false, fmt.Errorf("pod lister is not configured")
		}
		pod, err := s.podLister.Pods(namespace).Get(name)
		if err != nil {
			if k8serrors.IsNotFound(err) {
				lastReason = fmt.Sprintf("pod %s/%s is not visible", namespace, name)
				return nil, false, nil
			}
			return nil, false, err
		}
		ready, reason := podRootFSHeadReady(pod, desiredImage, desiredLayerID)
		if reason != "" {
			lastReason = reason
		}
		return pod, ready, nil
	}
	if pod, ready, err := evaluate(); err != nil || ready {
		return pod, err
	}
	events, unregister := s.ensurePodEventWaiter().register(namespace, name)
	defer unregister()
	if pod, ready, err := evaluate(); err != nil || ready {
		return pod, err
	}
	for {
		select {
		case <-readyCtx.Done():
			return nil, fmt.Errorf("pod %s/%s rootfs head not ready after %s: %s: %w", namespace, name, timeout, lastReason, readyCtx.Err())
		case event := <-events:
			if event.deleted {
				return nil, fmt.Errorf("pod %s/%s rootfs head not ready: pod is deleting", namespace, name)
			}
			pod, ready, err := evaluate()
			if err != nil || ready {
				return pod, err
			}
		}
	}
}

func podRootFSHeadReady(pod *corev1.Pod, desiredImage, desiredLayerID string) (bool, string) {
	if pod == nil {
		return false, "pod is nil"
	}
	if pod.Annotations[controller.AnnotationRootFSHeadImage] != desiredImage ||
		pod.Annotations[controller.AnnotationRootFSHeadLayerID] != desiredLayerID {
		return false, "rootfs head annotations have not been observed"
	}
	containerIndex := procdContainerIndex(pod.Spec.Containers)
	if containerIndex < 0 || pod.Spec.Containers[containerIndex].Image != desiredImage {
		return false, "procd spec image has not been updated"
	}
	desiredDigest, err := digestFromPinnedImage(desiredImage)
	if err != nil {
		return false, err.Error()
	}
	for index := range pod.Status.ContainerStatuses {
		status := &pod.Status.ContainerStatuses[index]
		if status.Name != sandboxRootFSContainerName {
			continue
		}
		if status.State.Running == nil {
			return false, "procd container is not running"
		}
		if status.Image == desiredImage || strings.Contains(status.ImageID, desiredDigest.String()) {
			return true, ""
		}
		return false, "procd is still running the previous image"
	}
	return false, "procd container status is missing"
}

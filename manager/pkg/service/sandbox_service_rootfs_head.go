package service

import (
	"context"
	"errors"
	"fmt"
	"strings"

	distref "github.com/distribution/reference"
	godigest "github.com/opencontainers/go-digest"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/controller"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshead"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
)

var ErrRootFSHeadMigrationRequired = errors.New("rootfs checkpoint requires metadata-head migration")

const (
	rootFSBaseImageVolumeName = "rootfs-base-image"
	rootFSBaseImageMountPath  = "/run/sandbox0/rootfs-base-image"
)

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
	if pod.Annotations == nil {
		pod.Annotations = make(map[string]string)
	}
	pod.Annotations[controller.AnnotationRootFSHeadImage] = strings.TrimSpace(req.RootFSHeadImageRef)
	pod.Annotations[controller.AnnotationRootFSHeadLayerID] = strings.TrimSpace(req.RootFSHeadLayerID)
	return nil
}

// applyColdRootFSBaseImageVolume makes kubelet unpack the canonical template
// base before a marker-only head is pulled on a newly created Pod. Hot resume
// does not call this function because the warm Pod already owns that snapshot
// and Pod volumes are immutable.
func applyColdRootFSBaseImageVolume(pod *corev1.Pod, req *ClaimRequest) error {
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
	for index := range pod.Spec.Volumes {
		if pod.Spec.Volumes[index].Name != rootFSBaseImageVolumeName {
			continue
		}
		image := pod.Spec.Volumes[index].Image
		if image == nil || image.Reference != baseImageRef {
			return fmt.Errorf("pod %s/%s has conflicting rootfs base image volume", pod.Namespace, pod.Name)
		}
		return ensureRootFSBaseImageMount(&pod.Spec.Containers[containerIndex])
	}
	pod.Spec.Volumes = append(pod.Spec.Volumes, corev1.Volume{
		Name: rootFSBaseImageVolumeName,
		VolumeSource: corev1.VolumeSource{Image: &corev1.ImageVolumeSource{
			Reference:  baseImageRef,
			PullPolicy: corev1.PullIfNotPresent,
		}},
	})
	return ensureRootFSBaseImageMount(&pod.Spec.Containers[containerIndex])
}

func ensureRootFSBaseImageMount(container *corev1.Container) error {
	if container == nil {
		return fmt.Errorf("procd container is required")
	}
	for index := range container.VolumeMounts {
		mount := &container.VolumeMounts[index]
		if mount.Name != rootFSBaseImageVolumeName && mount.MountPath != rootFSBaseImageMountPath {
			continue
		}
		if mount.Name != rootFSBaseImageVolumeName || mount.MountPath != rootFSBaseImageMountPath {
			return fmt.Errorf("procd container has conflicting rootfs base image mount")
		}
		return nil
	}
	container.VolumeMounts = append(container.VolumeMounts, corev1.VolumeMount{
		Name:      rootFSBaseImageVolumeName,
		MountPath: rootFSBaseImageMountPath,
		ReadOnly:  true,
	})
	return nil
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

package service

import (
	"context"
	"fmt"
	"strings"

	managerconfig "github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/controller"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	"github.com/sandbox0-ai/sandbox0/pkg/ctldapi"
	"github.com/sandbox0-ai/sandbox0/pkg/dataplane"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/util/retry"
)

// activateRuntimeWithRootFSHead activates a materialized Head on the claimed
// Pod. Reusing a warm Pod preserves its sandbox, network identity, and mounts.
// Cold Pods are replaced because kubelet can defer an in-place image update
// until its next sync; there is no published runtime identity to preserve yet.
func (s *SandboxService) activateRuntimeWithRootFSHead(
	ctx context.Context,
	current *corev1.Pod,
	template *v1alpha1.SandboxTemplate,
	req *ClaimRequest,
	head *sandboxstore.SandboxRootFSHead,
	replaceColdPod bool,
) (*corev1.Pod, bool, error) {
	if s == nil || s.ctldClient == nil || current == nil || template == nil || req == nil || head == nil {
		return current, false, fmt.Errorf("rootfs Head runtime activation inputs are required")
	}
	if err := head.Reference.Validate(); err != nil {
		return current, false, fmt.Errorf("validate rootfs Head reference: %w", err)
	}
	if err := head.Image.Validate(); err != nil {
		return current, false, fmt.Errorf("validate rootfs Head image: %w", err)
	}
	nodeName := strings.TrimSpace(current.Spec.NodeName)
	if nodeName == "" {
		return current, false, fmt.Errorf("rootfs Head runtime source pod is not scheduled")
	}
	containerIndex := procdContainerIndex(current.Spec.Containers)
	if containerIndex < 0 {
		return current, false, fmt.Errorf("pod %s/%s has no %s container", current.Namespace, current.Name, sandboxRootFSContainerName)
	}

	snapshotterInstance, err := s.rootFSSnapshotterInstanceForNode(nodeName)
	if err != nil {
		return current, false, err
	}
	if err := s.materializeRootFSHead(ctx, current, head); err != nil {
		return current, false, err
	}

	// imagePullPolicy is immutable on an existing Pod. Always would make
	// kubelet contact sandbox0.local even though ctld has installed the image.
	// A cold Pod also has no externally visible identity worth retaining, and
	// replacing it avoids waiting for kubelet's periodic image-change sync.
	if replaceColdPod || current.Spec.Containers[containerIndex].ImagePullPolicy == corev1.PullAlways {
		if s.logger != nil {
			reason := "procd imagePullPolicy is Always"
			if replaceColdPod {
				reason = "cold rootfs Head activation"
			}
			s.logger.Warn("Falling back to a replacement Pod for rootfs Head activation",
				zap.String("pod", current.Namespace+"/"+current.Name),
				zap.String("reason", reason),
			)
		}
		replacement, err := s.createRootFSHeadReplacementPod(ctx, current, template, req, head, snapshotterInstance, replaceColdPod)
		return replacement, true, err
	}

	updated, err := s.patchPodRootFSHead(ctx, current, head, snapshotterInstance)
	if err != nil {
		return current, false, err
	}
	ready, err := s.waitForPodRootFSHeadReady(ctx, updated.Namespace, updated.Name, head)
	if err != nil {
		return updated, false, err
	}
	return ready, false, nil
}

func (s *SandboxService) materializeRootFSHead(ctx context.Context, pod *corev1.Pod, head *sandboxstore.SandboxRootFSHead) error {
	ctldAddress, err := s.ctldAddressForPod(ctx, pod)
	if err != nil {
		return err
	}
	materialized, err := s.ctldClient.MaterializeRootFSHead(ctx, ctldAddress, ctldapi.MaterializeRootFSHeadRequest{
		Reference: head.Reference,
		Image:     head.Image,
	}, sandboxRootFSOperationTimeout)
	if err != nil {
		message := ""
		if materialized != nil {
			message = materialized.Error
		}
		return fmt.Errorf("materialize sandbox rootfs Head: %w", rootFSResponseError(err, message))
	}
	if materialized == nil || !materialized.Materialized || materialized.ImageName != head.Image.Name {
		return fmt.Errorf("materialize sandbox rootfs Head: ctld did not confirm image %s", head.Image.Name)
	}
	return nil
}

func (s *SandboxService) rootFSSnapshotterInstanceForNode(nodeName string) (string, error) {
	if s.nodeLister == nil {
		return "", fmt.Errorf("resolve rootfs snapshotter instance: node cache is not configured")
	}
	node, err := s.nodeLister.Get(nodeName)
	if err != nil {
		return "", fmt.Errorf("resolve rootfs snapshotter instance on node %s: %w", nodeName, err)
	}
	instance := strings.TrimSpace(node.Annotations[dataplane.NodeRootFSSnapshotterInstanceAnnotation])
	if instance == "" {
		return "", fmt.Errorf("rootfs snapshotter instance is not published on node %s", nodeName)
	}
	return instance, nil
}

func (s *SandboxService) patchPodRootFSHead(
	ctx context.Context,
	expected *corev1.Pod,
	head *sandboxstore.SandboxRootFSHead,
	snapshotterInstance string,
) (*corev1.Pod, error) {
	var updated *corev1.Pod
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		current, err := s.k8sClient.CoreV1().Pods(expected.Namespace).Get(ctx, expected.Name, metav1.GetOptions{})
		if err != nil {
			return err
		}
		if expected.UID != "" && current.UID != expected.UID {
			return fmt.Errorf("pod %s/%s identity changed from %s to %s", expected.Namespace, expected.Name, expected.UID, current.UID)
		}
		containerIndex := procdContainerIndex(current.Spec.Containers)
		if containerIndex < 0 {
			return fmt.Errorf("pod %s/%s has no %s container", current.Namespace, current.Name, sandboxRootFSContainerName)
		}
		if current.Spec.Containers[containerIndex].ImagePullPolicy == corev1.PullAlways {
			return fmt.Errorf("pod %s/%s rootfs Head image cannot be updated in place with imagePullPolicy Always", current.Namespace, current.Name)
		}
		desired := current.DeepCopy()
		desired.Spec.Containers[containerIndex].Image = head.Image.Name
		if desired.Annotations == nil {
			desired.Annotations = make(map[string]string)
		}
		desired.Annotations[controller.AnnotationRootFSHeadID] = head.Reference.HeadID
		desired.Annotations[controller.AnnotationRootFSHeadImage] = head.Image.Name
		desired.Annotations[controller.AnnotationRootFSSnapshotterInstance] = snapshotterInstance
		updated, err = s.k8sClient.CoreV1().Pods(expected.Namespace).Update(ctx, desired, metav1.UpdateOptions{})
		return err
	})
	if err != nil {
		return expected, fmt.Errorf("activate rootfs Head image on pod %s/%s: %w", expected.Namespace, expected.Name, err)
	}
	return updated, nil
}

func (s *SandboxService) waitForPodRootFSHeadReady(
	ctx context.Context,
	namespace string,
	name string,
	head *sandboxstore.SandboxRootFSHead,
) (*corev1.Pod, error) {
	timeout := managerconfig.EffectiveRuntimeReadyTimeout(s.config.RuntimeReadyTimeout)
	readyCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	lastReason := "rootfs Head container is not ready"
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
		ready, reason := podRootFSHeadReady(pod, head)
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
			return nil, fmt.Errorf("pod %s/%s rootfs Head not ready after %s: %s: %w", namespace, name, timeout, lastReason, readyCtx.Err())
		case event := <-events:
			if event.deleted {
				return nil, fmt.Errorf("pod %s/%s rootfs Head not ready: pod is deleting", namespace, name)
			}
			pod, ready, err := evaluate()
			if err != nil || ready {
				return pod, err
			}
		}
	}
}

func podRootFSHeadReady(pod *corev1.Pod, head *sandboxstore.SandboxRootFSHead) (bool, string) {
	if pod == nil || head == nil {
		return false, "pod or rootfs Head is nil"
	}
	if pod.Annotations[controller.AnnotationRootFSHeadID] != head.Reference.HeadID ||
		pod.Annotations[controller.AnnotationRootFSHeadImage] != head.Image.Name {
		return false, "rootfs Head annotations have not been observed"
	}
	containerIndex := procdContainerIndex(pod.Spec.Containers)
	if containerIndex < 0 || pod.Spec.Containers[containerIndex].Image != head.Image.Name {
		return false, "procd spec image has not been updated"
	}
	for index := range pod.Status.ContainerStatuses {
		status := &pod.Status.ContainerStatuses[index]
		if status.Name != sandboxRootFSContainerName {
			continue
		}
		if status.State.Running == nil {
			return false, "procd container is not running"
		}
		if status.Image == head.Image.Name || strings.Contains(status.ImageID, head.Image.ManifestDigest) {
			return true, ""
		}
		return false, "procd is still running the previous image"
	}
	return false, "procd container status is missing"
}

func procdContainerIndex(containers []corev1.Container) int {
	for index := range containers {
		if containers[index].Name == sandboxRootFSContainerName {
			return index
		}
	}
	return -1
}

func (s *SandboxService) createRootFSHeadReplacementPod(
	ctx context.Context,
	current *corev1.Pod,
	template *v1alpha1.SandboxTemplate,
	req *ClaimRequest,
	head *sandboxstore.SandboxRootFSHead,
	snapshotterInstance string,
	immediate bool,
) (*corev1.Pod, error) {
	rootFSTemplate := template.DeepCopy()
	rootFSTemplate.Spec.MainContainer.Image = head.Image.Name
	rootFSTemplate.Spec.MainContainer.ImagePullPolicy = string(corev1.PullNever)
	deleteOptions := metav1.DeleteOptions{}
	if immediate {
		deleteOptions = immediatePodDeletionOptions()
	}
	if err := s.k8sClient.CoreV1().Pods(current.Namespace).Delete(ctx, current.Name, deleteOptions); err != nil && !k8serrors.IsNotFound(err) {
		return current, fmt.Errorf("delete template runtime before rootfs Head attach: %w", err)
	}
	if err := s.waitForSandboxRuntimePodDeletion(ctx, current.Namespace, current.Name); err != nil {
		return current, err
	}
	replacementRequest := *req
	replacementRequest.PreferredNodeName = current.Spec.NodeName
	replacementRequest.RootFSSnapshotterInstance = snapshotterInstance
	replacement, err := s.createNewPod(ctx, rootFSTemplate, &replacementRequest)
	if err != nil {
		return current, fmt.Errorf("create rootfs Head runtime: %w", err)
	}
	return replacement, nil
}

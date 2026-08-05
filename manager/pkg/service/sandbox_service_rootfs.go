package service

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"path"
	"strings"
	"time"

	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/controller"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	"github.com/sandbox0-ai/sandbox0/pkg/ctldapi"
	"github.com/sandbox0-ai/sandbox0/pkg/dataplane"
	"github.com/sandbox0-ai/sandbox0/pkg/managerapi"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshead"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const sandboxRootFSContainerName = "procd"

const sandboxRootFSOperationTimeout = 5 * time.Minute
const sandboxRootFSSourceCheckpointLifecycleStaleAfter = sandboxRootFSOperationTimeout + time.Minute

func (s *SandboxService) prepareSandboxRootFSHeadCheckpoint(ctx context.Context, pod *corev1.Pod, record *sandboxstore.SandboxRecord, headID string) (*sandboxstore.SandboxRootFSHead, error) {
	if s == nil || !s.config.CtldEnabled || s.ctldClient == nil || pod == nil || record == nil {
		return nil, nil
	}
	sandboxID := strings.TrimSpace(record.ID)
	if sandboxID == "" {
		sandboxID = sandboxPodID(pod)
	}
	teamID := strings.TrimSpace(record.TeamID)
	if sandboxID == "" || teamID == "" {
		return nil, fmt.Errorf("sandbox_id and team_id are required to seal sandbox rootfs Head")
	}
	headID = strings.TrimSpace(headID)
	if headID == "" {
		return nil, fmt.Errorf("rootfs Head idempotency key is required")
	}
	parent, err := s.latestRootFSHead(ctx, sandboxID)
	if err != nil {
		return nil, fmt.Errorf("load current rootfs Head: %w", err)
	}
	ctldAddress, err := s.ctldAddressForPod(ctx, pod)
	if err != nil {
		return nil, err
	}
	generation := runtimeGenerationFromPod(pod)
	request := ctldapi.SealRootFSHeadRequest{
		SandboxID:                 sandboxID,
		TeamID:                    teamID,
		HeadID:                    headID,
		ExpectedRuntimeGeneration: generation,
	}
	if parent != nil {
		request.ExpectedParent = cloneRootFSHeadReference(&parent.Reference)
	}
	response, err := s.ctldClient.SealRootFSHead(ctx, ctldAddress, request, sandboxRootFSOperationTimeout)
	if err != nil {
		sealErr := fmt.Errorf("seal sandbox rootfs Head: %w", rootFSResponseError(err, sealRootFSError(response)))
		if response != nil && response.Reference.HeadID == headID {
			ackErr := s.acknowledgeSandboxRootFSHead(ctx, ctldAddress, sandboxID, teamID, headID, generation, false, true)
			return nil, errors.Join(sealErr, ackErr)
		}
		return nil, sealErr
	}
	if response == nil {
		return nil, fmt.Errorf("seal sandbox rootfs Head: empty ctld response")
	}
	if response.Reference.HeadID != headID || response.Head.HeadID != headID {
		return nil, fmt.Errorf("seal sandbox rootfs Head: ctld returned Head %q/%q, expected %q", response.Reference.HeadID, response.Head.HeadID, headID)
	}
	if err := response.Reference.Validate(); err != nil {
		return nil, fmt.Errorf("seal sandbox rootfs Head reference: %w", err)
	}
	if err := response.Head.Validate(); err != nil {
		return nil, fmt.Errorf("seal sandbox rootfs Head: %w", err)
	}
	if err := response.Image.Validate(); err != nil {
		return nil, fmt.Errorf("seal sandbox rootfs Head image: %w", err)
	}
	head := &sandboxstore.SandboxRootFSHead{
		SandboxID:         sandboxID,
		SourceSandboxID:   sandboxID,
		TeamID:            teamID,
		RuntimeGeneration: generation,
		Parent:            request.ExpectedParent,
		Reference:         response.Reference,
		Base:              response.Head.Base,
		Image:             response.Image,
		CreatedAt:         s.now().UTC(),
	}
	if err := s.sandboxStore.StageRootFSHead(ctx, head); err != nil {
		return nil, fmt.Errorf("stage sandbox rootfs Head: %w", err)
	}
	return head, nil
}

func (s *SandboxService) acknowledgeSandboxRootFSHead(ctx context.Context, ctldAddress, sandboxID, teamID, headID string, generation int64, published, runtimeContinues bool) error {
	if s == nil || s.ctldClient == nil {
		return nil
	}
	response, err := s.ctldClient.AcknowledgeRootFSHead(ctx, ctldAddress, ctldapi.AcknowledgeRootFSHeadRequest{
		SandboxID:         sandboxID,
		TeamID:            teamID,
		RuntimeGeneration: generation,
		HeadID:            headID,
		Published:         published,
		RuntimeContinues:  runtimeContinues,
	}, sandboxRootFSOperationTimeout)
	if err != nil {
		message := ""
		if response != nil {
			message = response.Error
		}
		return fmt.Errorf("acknowledge sandbox rootfs Head: %w", rootFSResponseError(err, message))
	}
	if response == nil || !response.Acknowledged {
		return fmt.Errorf("acknowledge sandbox rootfs Head: ctld did not confirm Head %s", headID)
	}
	return nil
}

func (s *SandboxService) saveSandboxRootFSCheckpoint(ctx context.Context, pod *corev1.Pod, record *sandboxstore.SandboxRecord, tx sandboxstore.SandboxStoreTx, headID string) error {
	head, err := s.prepareSandboxRootFSHeadCheckpoint(ctx, pod, record, headID)
	if err != nil || head == nil {
		return err
	}
	if tx != nil {
		return tx.SaveRootFSHead(ctx, head)
	}
	if s.sandboxStore != nil {
		return s.sandboxStore.SaveRootFSHead(ctx, head)
	}
	return nil
}

func (s *SandboxService) bindSandboxRootFSSync(ctx context.Context, pod *corev1.Pod, record *sandboxstore.SandboxRecord) error {
	if s == nil || !s.config.CtldEnabled || s.ctldClient == nil || pod == nil || record == nil {
		return nil
	}
	parent, err := s.latestRootFSHead(ctx, record.ID)
	if err != nil {
		return err
	}
	ctldAddress, err := s.ctldAddressForPod(ctx, pod)
	if err != nil {
		return err
	}
	request := ctldapi.BindRootFSSyncRequest{
		Target:            rootFSTargetForPod(pod),
		SandboxID:         record.ID,
		TeamID:            record.TeamID,
		RuntimeGeneration: runtimeGenerationFromPod(pod),
		ExcludedPaths:     rootFSExcludedPathsForPod(pod),
	}
	if parent != nil {
		request.Parent = cloneRootFSHeadReference(&parent.Reference)
		expectedBase := parent.Base
		request.ExpectedBase = &expectedBase
	}
	waitCtx, cancel := context.WithTimeout(ctx, sandboxRootFSOperationTimeout)
	defer cancel()
	bound, err := s.ctldClient.BindRootFSSync(waitCtx, ctldAddress, request)
	if err != nil {
		return fmt.Errorf("bind sandbox rootfs sync: %w", rootFSResponseError(err, bindRootFSSyncError(bound)))
	}
	if bound != nil && bound.Status.SealedReference != nil {
		return s.reconcilePendingRootFSSeal(waitCtx, ctldAddress, record, bound.Status)
	}
	if bound != nil && bound.Status.InitialScanComplete && bound.Status.LastError == "" {
		return nil
	}
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-waitCtx.Done():
			return fmt.Errorf("wait for sandbox rootfs initial sync: %w", waitCtx.Err())
		case <-ticker.C:
			status, err := s.ctldClient.GetRootFSSyncStatus(waitCtx, ctldAddress, ctldapi.GetRootFSSyncStatusRequest{
				SandboxID:         record.ID,
				RuntimeGeneration: runtimeGenerationFromPod(pod),
			})
			if err != nil {
				return fmt.Errorf("get sandbox rootfs sync status: %w", rootFSResponseError(err, rootFSSyncStatusError(status)))
			}
			if status == nil {
				continue
			}
			if status.Status.InitialScanComplete {
				if status.Status.LastError != "" {
					return fmt.Errorf("sandbox rootfs initial sync failed: %s", status.Status.LastError)
				}
				return nil
			}
		}
	}
}

// reconcilePendingRootFSSeal repairs a lost manager acknowledgement after the
// durable lifecycle transaction has committed or aborted. An active lifecycle
// transaction remains the only authority allowed to decide that outcome.
func (s *SandboxService) reconcilePendingRootFSSeal(ctx context.Context, ctldAddress string, record *sandboxstore.SandboxRecord, status ctldapi.RootFSSyncStatus) error {
	if s == nil || s.sandboxStore == nil || record == nil || status.SealedReference == nil {
		return nil
	}
	activeTxn, err := s.sandboxStore.GetActiveLifecycleTxn(ctx, record.ID)
	if err != nil {
		return fmt.Errorf("load lifecycle transaction for pending rootfs Head: %w", err)
	}
	if activeTxn != nil {
		return fmt.Errorf("rootfs Head %s is awaiting active lifecycle transaction %s", status.SealedReference.HeadID, activeTxn.ID)
	}
	current, err := s.latestRootFSHead(ctx, record.ID)
	if err != nil {
		return fmt.Errorf("load published rootfs Head for acknowledgement recovery: %w", err)
	}
	published := current != nil && sameRootFSHeadReference(current.Reference, *status.SealedReference)
	if err := s.acknowledgeSandboxRootFSHead(
		ctx,
		ctldAddress,
		record.ID,
		record.TeamID,
		status.SealedReference.HeadID,
		status.RuntimeGeneration,
		published,
		true,
	); err != nil {
		return fmt.Errorf("recover pending rootfs Head acknowledgement: %w", err)
	}
	return nil
}

func (s *SandboxService) latestRootFSHead(ctx context.Context, sandboxID string) (*sandboxstore.SandboxRootFSHead, error) {
	if s == nil || s.sandboxStore == nil || strings.TrimSpace(sandboxID) == "" {
		return nil, nil
	}
	return s.sandboxStore.GetRootFSHead(ctx, sandboxID)
}

func (s *SandboxService) EnsureSandboxRootFSSync(ctx context.Context, sandboxID string) error {
	if s == nil || s.sandboxStore == nil || !s.config.CtldEnabled || s.ctldClient == nil {
		return nil
	}
	record, err := s.sandboxStore.GetSandbox(ctx, strings.TrimSpace(sandboxID))
	if err != nil || record == nil {
		return err
	}
	if record.DesiredState != sandboxstore.SandboxDesiredStateActive || !record.DeletedAt.IsZero() {
		return nil
	}
	pod, err := s.getSandboxPod(ctx, record.ID)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return err
	}
	if s.podToSandboxStatus(pod) != managerapi.SandboxStatusRunning {
		return nil
	}
	return s.bindSandboxRootFSSync(ctx, pod, record)
}

func cloneRootFSHeadReference(reference *rootfshead.HeadReference) *rootfshead.HeadReference {
	if reference == nil {
		return nil
	}
	cloned := *reference
	return &cloned
}

func sameRootFSHeadReference(left, right rootfshead.HeadReference) bool {
	return left.Version == right.Version && left.HeadID == right.HeadID && left.Manifest == right.Manifest
}

func currentRootFSHeadID(ctx context.Context, tx sandboxstore.SandboxStoreTx, sandboxID string) (string, error) {
	if tx == nil {
		return "", nil
	}
	head, err := tx.GetRootFSHead(ctx, sandboxID)
	if err != nil || head == nil {
		return "", err
	}
	return head.Reference.HeadID, nil
}

func (s *SandboxService) replaceRuntimeWithRootFSHead(ctx context.Context, current *corev1.Pod, template *v1alpha1.SandboxTemplate, req *ClaimRequest, head *sandboxstore.SandboxRootFSHead) (*corev1.Pod, error) {
	if s == nil || s.ctldClient == nil || current == nil || template == nil || req == nil || head == nil {
		return current, fmt.Errorf("rootfs Head runtime replacement inputs are required")
	}
	nodeName := strings.TrimSpace(current.Spec.NodeName)
	if nodeName == "" {
		return current, fmt.Errorf("rootfs Head runtime source pod is not scheduled")
	}
	ctldAddress, err := s.ctldAddressForPod(ctx, current)
	if err != nil {
		return current, err
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
		return current, fmt.Errorf("materialize sandbox rootfs Head: %w", rootFSResponseError(err, message))
	}
	if materialized == nil || !materialized.Materialized || materialized.ImageName != head.Image.Name {
		return current, fmt.Errorf("materialize sandbox rootfs Head: ctld did not confirm image %s", head.Image.Name)
	}
	if s.nodeLister == nil {
		return current, fmt.Errorf("resolve rootfs snapshotter instance: node cache is not configured")
	}
	node, err := s.nodeLister.Get(nodeName)
	if err != nil {
		return current, fmt.Errorf("resolve rootfs snapshotter instance on node %s: %w", nodeName, err)
	}
	snapshotterInstance := strings.TrimSpace(node.Annotations[dataplane.NodeRootFSSnapshotterInstanceAnnotation])
	if snapshotterInstance == "" {
		return current, fmt.Errorf("rootfs snapshotter instance is not published on node %s", nodeName)
	}

	rootFSTemplate := template.DeepCopy()
	rootFSTemplate.Spec.MainContainer.Image = head.Image.Name
	rootFSTemplate.Spec.MainContainer.ImagePullPolicy = string(corev1.PullNever)
	if err := s.k8sClient.CoreV1().Pods(current.Namespace).Delete(ctx, current.Name, metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
		return current, fmt.Errorf("delete template runtime before rootfs Head attach: %w", err)
	}
	if err := s.waitForSandboxRuntimePodDeletion(ctx, current.Namespace, current.Name); err != nil {
		return current, err
	}
	replacementRequest := *req
	replacementRequest.PreferredNodeName = nodeName
	replacementRequest.RootFSSnapshotterInstance = snapshotterInstance
	replacement, err := s.createNewPod(ctx, rootFSTemplate, &replacementRequest)
	if err != nil {
		return current, fmt.Errorf("create rootfs Head runtime: %w", err)
	}
	return replacement, nil
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

func rootFSExcludedPathsForPod(pod *corev1.Pod) []string {
	if pod == nil {
		return nil
	}
	var mounts []managerapi.ClaimMount
	if pod.Annotations != nil {
		mounts = parseClaimMounts(pod.Annotations[controller.AnnotationMounts])
	}
	seen := make(map[string]struct{}, len(mounts)+8)
	out := make([]string, 0, len(mounts)+8)
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
	// These paths are ephemeral or runtime-owned even when a particular image
	// does not declare a Kubernetes mount for them.
	add("/tmp")
	add("/procd")
	add("/procd-image")
	for _, container := range pod.Spec.Containers {
		if container.Name != sandboxRootFSContainerName {
			continue
		}
		for _, mount := range container.VolumeMounts {
			add(strings.TrimSpace(mount.MountPath))
		}
		break
	}
	for _, mount := range mounts {
		add(strings.TrimSpace(mount.MountPoint))
	}
	if pod.Annotations != nil && strings.TrimSpace(pod.Annotations[controller.AnnotationWebhookStateVolumeID]) != "" {
		add(webhookStateMountPoint)
	}
	return out
}

func (s *SandboxService) saveRestoredRuntimePod(ctx context.Context, pod *corev1.Pod, record *sandboxstore.SandboxRecord) error {
	if s == nil || s.sandboxStore == nil || pod == nil || record == nil {
		return nil
	}
	sandboxID := strings.TrimSpace(record.ID)
	if sandboxID == "" {
		sandboxID = sandboxPodID(pod)
	}
	if sandboxID == "" {
		return fmt.Errorf("sandbox_id is required")
	}
	return s.sandboxStore.WithSandboxLock(ctx, sandboxID, func(lockCtx context.Context, tx sandboxstore.SandboxStoreTx, locked *sandboxstore.SandboxRecord) error {
		if locked == nil || locked.DesiredState == sandboxstore.SandboxDesiredStateTerminating || locked.DesiredState == sandboxstore.SandboxDesiredStateDeleted || !locked.DeletedAt.IsZero() {
			return nil
		}
		if runtimeGenerationFromPod(pod) < locked.RuntimeGeneration {
			return nil
		}
		return tx.SaveRuntime(lockCtx, sandboxID, pod.Namespace, pod.Name, runtimeGenerationFromPod(pod), parseRFC3339AnnotationTime(pod.Annotations, controller.AnnotationExpiresAt), parseRFC3339AnnotationTime(pod.Annotations, controller.AnnotationHardExpiresAt), sandboxRuntimeMetadataFromPod(pod))
	})
}

func rootFSTargetForPod(pod *corev1.Pod) ctldapi.RootFSContainerRef {
	if pod == nil {
		return ctldapi.RootFSContainerRef{ContainerName: sandboxRootFSContainerName}
	}
	containerID := ""
	if status := procdContainerStatus(pod); status != nil {
		containerID = status.ContainerID
	}
	return ctldapi.RootFSContainerRef{
		Namespace:     pod.Namespace,
		PodName:       pod.Name,
		PodUID:        string(pod.UID),
		ContainerName: sandboxRootFSContainerName,
		ContainerID:   containerID,
	}
}

func bindRootFSSyncError(resp *ctldapi.BindRootFSSyncResponse) string {
	if resp == nil {
		return ""
	}
	return strings.TrimSpace(resp.Error)
}

func rootFSSyncStatusError(resp *ctldapi.GetRootFSSyncStatusResponse) string {
	if resp == nil {
		return ""
	}
	return strings.TrimSpace(resp.Error)
}

func sealRootFSError(resp *ctldapi.SealRootFSHeadResponse) string {
	if resp == nil {
		return ""
	}
	return strings.TrimSpace(resp.Error)
}

func rootFSTerminatedSnapshotMissing(err error) bool {
	var reqErr *ctldapi.RequestError
	return errors.As(err, &reqErr) && reqErr != nil && reqErr.StatusCode == http.StatusNotFound
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

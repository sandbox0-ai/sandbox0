package service

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	api "github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/carrierpool"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/controller"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	"github.com/sandbox0-ai/sandbox0/pkg/carrier"
	"github.com/sandbox0-ai/sandbox0/pkg/ctldapi"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
)

type rootFSHeadByIDStore interface {
	GetRootFSHeadByID(context.Context, string, string) (*sandboxstore.SandboxRootFSHead, error)
	BindSandboxToRootFSHead(context.Context, string, string, string) error
}

// claimS0FSCarrier handles revisions that have completed OCI-to-ImageFS import.
// It returns handled=false while the feature is disabled or a revision is not
// ready, leaving the legacy cohort untouched before any carrier side effect.
func (s *SandboxService) claimS0FSCarrier(ctx context.Context, template *api.SandboxTemplate, req *ClaimRequest) (*ClaimResponse, bool, error) {
	if s == nil || s.sharedCarrierPool == nil || template == nil || req == nil || strings.TrimSpace(req.SnapshotID) != "" {
		return nil, false, nil
	}
	revision := template.Status.ImageRevision
	if revision == nil || revision.State != api.TemplateImageRevisionStateReady || strings.TrimSpace(revision.ImageFSHeadID) == "" {
		return nil, false, nil
	}
	store, ok := s.sandboxStore.(rootFSHeadByIDStore)
	if !ok {
		return nil, true, fmt.Errorf("sandbox store does not support S0FS ImageFS binding")
	}
	pod, _, err := s.allocateS0FSCarrier(ctx, template, req)
	if err != nil {
		return nil, true, err
	}
	fail := func(reason string, err error) (*ClaimResponse, bool, error) {
		s.deleteFailedCarrier(pod)
		if strings.TrimSpace(req.SandboxID) != "" {
			s.markSandboxDeletedAfterClaimFailure(req.SandboxID, reason)
		}
		return nil, true, err
	}
	pod, runtimeRevision, err := s.publishRuntimeAssignment(ctx, pod, false)
	if err != nil {
		return fail("carrier runtime assignment failed", err)
	}
	record := sandboxRecordForClaimedPod(s, pod, template, req)
	record.RootFSRuntimeVersion = sandboxstore.RootFSRuntimeS0FSV2
	if err := s.sandboxStore.UpsertSandbox(ctx, record); err != nil {
		return fail("carrier persistence failed", err)
	}
	if err := store.BindSandboxToRootFSHead(ctx, record.ID, req.TeamID, revision.ImageFSHeadID); err != nil {
		return fail("ImageFS binding failed", err)
	}
	head, err := store.GetRootFSHeadByID(ctx, revision.ImageFSHeadID, req.TeamID)
	if err != nil || head == nil {
		if err == nil {
			err = fmt.Errorf("ImageFS Head %s is missing", revision.ImageFSHeadID)
		}
		return fail("ImageFS lookup failed", err)
	}
	pod, err = s.activateS0FSCarrierHead(ctx, pod, head)
	if err != nil {
		return fail("carrier rootfs activation failed", err)
	}
	if err := s.bindSandboxRootFSSync(ctx, pod, record); err != nil {
		return fail("carrier rootfs sync failed", err)
	}
	pod, err = s.activateRuntimeAssignment(ctx, pod, runtimeRevision)
	if err != nil {
		return fail("carrier procd readiness failed", err)
	}
	if err := s.persistUpdatedSandboxPod(ctx, pod); err != nil {
		return fail("carrier readiness persistence failed", err)
	}
	procdAddress, err := s.prodAddress(ctx, pod)
	if err != nil {
		return fail("carrier procd address failed", err)
	}
	return &ClaimResponse{
		SandboxID: req.SandboxID, Status: s.podToSandboxStatus(pod), ProcdAddress: procdAddress,
		PodName: pod.Name, Template: req.Template, ClusterId: template.Spec.ClusterId,
	}, true, nil
}

func (s *SandboxService) allocateS0FSCarrier(ctx context.Context, template *api.SandboxTemplate, req *ClaimRequest) (*corev1.Pod, string, error) {
	if s == nil || s.sharedCarrierPool == nil || template == nil || req == nil {
		return nil, "", fmt.Errorf("S0FS carrier allocator is not configured")
	}
	compatible, _ := carrierpool.Compatible(template)
	var pod *corev1.Pod
	var err error
	claimType := "shared"
	if compatible {
		pod, err = s.sharedCarrierPool.Reserve(ctx)
	}
	if err != nil {
		return nil, "", fmt.Errorf("reserve shared carrier: %w", err)
	}
	if pod == nil {
		claimType = "cold-s0fs"
		if err := controller.EnsureProcdConfigSecret(ctx, s.k8sClient, s.secretLister, template); err != nil {
			return nil, "", fmt.Errorf("ensure cold carrier procd config: %w", err)
		}
		pod, err = s.sharedCarrierPool.CreateCold(ctx, template)
		if err != nil {
			return nil, "", fmt.Errorf("create cold S0FS carrier: %w", err)
		}
		pod, err = s.waitForCarrierGate(ctx, pod.Namespace, pod.Name, pod.Labels[carrier.LabelGeneration])
		if err != nil {
			s.deleteFailedCarrier(pod)
			return nil, "", err
		}
	}
	fail := func(reason string, err error) (*corev1.Pod, string, error) {
		s.deleteFailedCarrier(pod)
		return nil, "", fmt.Errorf("%s: %w", reason, err)
	}
	pod, err = s.assignCarrier(ctx, pod, template, req, claimType)
	if err != nil {
		return fail("carrier assignment failed", err)
	}
	resourceQuota, err := s.effectiveSandboxResourceQuota(template, req.Config)
	if err != nil {
		return fail("carrier resource validation failed", err)
	}
	if sandboxPodNeedsResourceResize(pod, resourceQuota) {
		pod, err = s.resizeSandboxPodResourcesWithClient(ctx, s.hotClaimClient(), pod, resourceQuota)
		if err != nil {
			return fail("carrier resize failed", fmt.Errorf("resize S0FS carrier: %w", err))
		}
	}
	if err := s.applyNetworkProviderFromPod(ctx, pod, req.TeamID); err != nil {
		return fail("carrier network policy failed", err)
	}
	return pod, claimType, nil
}

func (s *SandboxService) activateS0FSCarrierHead(ctx context.Context, pod *corev1.Pod, head *sandboxstore.SandboxRootFSHead) (*corev1.Pod, error) {
	if pod == nil || head == nil {
		return pod, fmt.Errorf("carrier Pod and rootfs Head are required")
	}
	ctldAddress, err := s.ctldAddressForPod(ctx, pod)
	if err != nil {
		return pod, fmt.Errorf("resolve carrier ctld: %w", err)
	}
	slot := pod.Annotations[carrier.AnnotationSlot]
	markerImage, err := carrier.MarkerImage(slot)
	if err != nil {
		return pod, err
	}
	materialized, err := s.ctldClient.MaterializeRootFSHead(ctx, ctldAddress, ctldapi.MaterializeRootFSHeadRequest{
		Reference: head.Reference, Image: head.Image, CarrierSlot: slot, TargetImageName: markerImage,
	}, sandboxRootFSOperationTimeout)
	if err != nil || materialized == nil || !materialized.Materialized || materialized.ImageName != markerImage {
		if err == nil {
			err = fmt.Errorf("ctld did not confirm carrier marker %s", markerImage)
		}
		return pod, fmt.Errorf("materialize carrier rootfs: %w", err)
	}
	released, err := s.ctldClient.ReleaseCarrierGate(ctx, ctldAddress, ctldapi.ReleaseCarrierGateRequest{
		Namespace: pod.Namespace, PodName: pod.Name, PodUID: string(pod.UID), Slot: slot,
	}, sandboxRootFSOperationTimeout)
	if err != nil || released == nil || !released.Released {
		if err == nil {
			err = fmt.Errorf("ctld did not confirm carrier gate release")
		}
		return pod, fmt.Errorf("release carrier gate: %w", err)
	}
	pod, err = s.waitForCarrierRuntimeStarted(ctx, pod.Namespace, pod.Name)
	if err != nil {
		return pod, fmt.Errorf("wait for carrier runtime: %w", err)
	}
	return pod, nil
}

// waitForCarrierRuntimeStarted waits only for the gated main container. The
// sandbox readiness gate depends on the runtime assignment, which is activated
// after rootfs binding, so waiting for full Pod readiness here would deadlock.
func (s *SandboxService) waitForCarrierRuntimeStarted(ctx context.Context, namespace, name string) (*corev1.Pod, error) {
	timeout := s.config.RuntimeReadyTimeout
	if timeout <= 0 {
		timeout = 5 * time.Minute
	}
	var started *corev1.Pod
	err := wait.PollUntilContextTimeout(ctx, 50*time.Millisecond, timeout, true, func(ctx context.Context) (bool, error) {
		pod, err := s.k8sClient.CoreV1().Pods(namespace).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return false, err
		}
		if pod.DeletionTimestamp != nil || pod.Status.Phase == corev1.PodFailed || pod.Status.Phase == corev1.PodSucceeded {
			return false, fmt.Errorf("carrier Pod entered terminal state %s", pod.Status.Phase)
		}
		for i := range pod.Status.ContainerStatuses {
			status := &pod.Status.ContainerStatuses[i]
			if status.Name != "procd" {
				continue
			}
			if status.State.Terminated != nil {
				return false, fmt.Errorf("carrier runtime terminated: %s", status.State.Terminated.Reason)
			}
			if status.State.Running != nil && strings.TrimSpace(pod.Status.PodIP) != "" {
				started = pod
				return true, nil
			}
		}
		return false, nil
	})
	if err != nil {
		return nil, err
	}
	return started, nil
}

func (s *SandboxService) assignCarrier(ctx context.Context, pod *corev1.Pod, template *api.SandboxTemplate, req *ClaimRequest, claimType string) (*corev1.Pod, error) {
	if pod == nil {
		return nil, fmt.Errorf("carrier Pod is required")
	}
	current, err := s.k8sClient.CoreV1().Pods(pod.Namespace).Get(ctx, pod.Name, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	updated := current.DeepCopy()
	if updated.Labels == nil {
		updated.Labels = make(map[string]string)
	}
	if updated.Annotations == nil {
		updated.Annotations = make(map[string]string)
	}
	updated.Labels[controller.LabelTemplateID] = template.Name
	updated.Labels[controller.LabelTemplateLogicalID] = controller.TemplateLogicalID(template)
	updated.Labels[controller.LabelSandboxID] = req.SandboxID
	updated.Labels[controller.LabelPoolType] = controller.PoolTypeActive
	ensureSandboxCleanupFinalizer(updated)
	updated.Annotations = controller.ClaimedSandboxPodAnnotations(updated.Annotations, s.config.AutoscalerSafeToEvictAnnotationKeys)
	updated.Annotations[controller.AnnotationSandboxID] = req.SandboxID
	updated.Annotations[controller.AnnotationRuntimeGeneration] = strconv.FormatInt(req.RuntimeGeneration, 10)
	updated.Annotations[controller.AnnotationTeamID] = req.TeamID
	updated.Annotations[controller.AnnotationUserID] = req.UserID
	updated.Annotations[controller.AnnotationClaimedAt] = s.clock.Now().UTC().Format(time.RFC3339Nano)
	updated.Annotations[controller.AnnotationClaimType] = claimType
	applyClaimMetadata(updated, req.Metadata)
	persistedConfig := s.claimConfigForPersistence(req.Config)
	var ttl, hardTTL *int32
	if persistedConfig != nil {
		ttl, hardTTL = persistedConfig.TTL, persistedConfig.HardTTL
		configJSON, err := json.Marshal(persistedConfig)
		if err != nil {
			return nil, err
		}
		updated.Annotations[controller.AnnotationConfig] = string(configJSON)
	}
	setExpirationAnnotation(updated.Annotations, s.clock.Now(), ttl)
	setClaimHardExpirationAnnotation(updated.Annotations, s.clock.Now(), hardTTL, req.HardExpiresAt)
	if err := setMountsAnnotation(updated.Annotations, req.Mounts); err != nil {
		return nil, err
	}
	networkState, err := s.applyPoliciesForPod(ctx, updated, template, req)
	if err != nil {
		return nil, err
	}
	rollback, err := s.syncCredentialBindings(ctx, updated, req.TeamID, networkState, req.mayHaveExistingCredentialBindings)
	if err != nil {
		return nil, err
	}
	updated, err = s.k8sClient.CoreV1().Pods(updated.Namespace).Update(ctx, updated, metav1.UpdateOptions{})
	if err != nil {
		_ = rollback(context.WithoutCancel(ctx))
		return nil, err
	}
	return updated, nil
}

func (s *SandboxService) waitForCarrierGate(ctx context.Context, namespace, name, generation string) (*corev1.Pod, error) {
	timeout := s.config.RuntimeReadyTimeout
	if timeout <= 0 {
		timeout = 5 * time.Minute
	}
	var ready *corev1.Pod
	err := wait.PollUntilContextTimeout(ctx, 100*time.Millisecond, timeout, true, func(ctx context.Context) (bool, error) {
		pod, err := s.k8sClient.CoreV1().Pods(namespace).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return false, err
		}
		if carrierpool.GateReady(pod, generation) {
			ready = pod
			return true, nil
		}
		return false, nil
	})
	if err != nil {
		return nil, fmt.Errorf("wait for cold carrier gate: %w", err)
	}
	return ready, nil
}

func (s *SandboxService) deleteFailedCarrier(pod *corev1.Pod) {
	if s == nil || s.sharedCarrierPool == nil || pod == nil {
		return
	}
	cleanup, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = s.sharedCarrierPool.Delete(cleanup, pod)
}

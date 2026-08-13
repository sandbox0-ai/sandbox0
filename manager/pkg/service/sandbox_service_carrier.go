package service

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	api "github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/carrierpool"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/controller"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	"github.com/sandbox0-ai/sandbox0/pkg/carrier"
	"github.com/sandbox0-ai/sandbox0/pkg/ctldapi"
	"github.com/sandbox0-ai/sandbox0/pkg/runtimecontrol"
	"golang.org/x/sync/errgroup"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	corelisters "k8s.io/client-go/listers/core/v1"
)

type rootFSHeadByIDStore interface {
	GetRootFSHeadByID(context.Context, string, string) (*sandboxstore.SandboxRootFSHead, error)
	BindSandboxToRootFSHead(context.Context, string, string, string) error
}

// claimS0FSCarrier handles an admitted revision that completed OCI-to-ImageFS
// import. Admission is decided before this method so it never falls back to a
// legacy rootfs after entering the S0FS path.
func (s *SandboxService) claimS0FSCarrier(ctx context.Context, template *api.SandboxTemplate, req *ClaimRequest) (*ClaimResponse, bool, error) {
	if s == nil || template == nil || req == nil {
		return nil, false, nil
	}
	if s.sharedCarrierPool == nil {
		return nil, true, fmt.Errorf("%w: S0FS carrier runtime is not configured", ErrDataPlaneNotReady)
	}
	revision := template.Status.ImageRevision
	if revision == nil || revision.State != api.TemplateImageRevisionStateReady || strings.TrimSpace(revision.ImageFSHeadID) == "" {
		return nil, true, fmt.Errorf("%w: admitted template image revision is not ready", ErrDataPlaneNotReady)
	}
	store, ok := s.sandboxStore.(rootFSHeadByIDStore)
	if !ok {
		return nil, true, fmt.Errorf("sandbox store does not support S0FS ImageFS binding")
	}
	phaseStarted := time.Now()
	pod, claimType, err := s.allocateS0FSCarrier(ctx, template, req, s.config.S0FSAdmission.UsesSharedCarrier())
	s.observeClaimPhase(req.Template, claimType, "allocate_s0fs_carrier", phaseStarted, err)
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
	phaseStarted = time.Now()
	pod, runtimeRevision, err := s.publishRuntimeAssignment(ctx, pod, false)
	s.observeClaimPhase(req.Template, claimType, "publish_runtime_assignment", phaseStarted, err)
	if err != nil {
		return fail("carrier runtime assignment failed", err)
	}
	record := sandboxRecordForClaimedPod(s, pod, template, req)
	record.RootFSRuntimeVersion = sandboxstore.RootFSRuntimeS0FSV2
	phaseStarted = time.Now()
	err = s.sandboxStore.UpsertSandbox(ctx, record)
	s.observeClaimPhase(req.Template, claimType, "persist_sandbox", phaseStarted, err)
	if err != nil {
		return fail("carrier persistence failed", err)
	}
	phaseStarted = time.Now()
	err = store.BindSandboxToRootFSHead(ctx, record.ID, req.TeamID, revision.ImageFSHeadID)
	s.observeClaimPhase(req.Template, claimType, "bind_imagefs_head", phaseStarted, err)
	if err != nil {
		return fail("ImageFS binding failed", err)
	}
	phaseStarted = time.Now()
	head, err := store.GetRootFSHeadByID(ctx, revision.ImageFSHeadID, req.TeamID)
	if err != nil || head == nil {
		if err == nil {
			err = fmt.Errorf("ImageFS Head %s is missing", revision.ImageFSHeadID)
		}
		s.observeClaimPhase(req.Template, claimType, "load_imagefs_head", phaseStarted, err)
		return fail("ImageFS lookup failed", err)
	}
	s.observeClaimPhase(req.Template, claimType, "load_imagefs_head", phaseStarted, nil)
	phaseStarted = time.Now()
	pod, err = s.activateS0FSCarrierHead(ctx, pod, head, req.Template, claimType)
	s.observeClaimPhase(req.Template, claimType, "materialize_rootfs_head", phaseStarted, err)
	if err != nil {
		return fail("carrier rootfs activation failed", err)
	}
	pod, err = s.waitForS0FSCarrierReady(ctx, pod, req.Template, claimType, func(readyCtx context.Context, readyPod *corev1.Pod) error {
		phaseStarted := time.Now()
		bindErr := s.bindSandboxRootFSSync(readyCtx, readyPod, record)
		s.observeClaimPhase(req.Template, claimType, "bind_rootfs_sync", phaseStarted, bindErr)
		return bindErr
	})
	if err != nil {
		return fail("carrier runtime readiness failed", err)
	}
	phaseStarted = time.Now()
	pod, err = s.activateRuntimeAssignment(ctx, pod, runtimeRevision)
	s.observeClaimPhase(req.Template, claimType, "activate_runtime", phaseStarted, err)
	if err != nil {
		return fail("carrier procd readiness failed", err)
	}
	phaseStarted = time.Now()
	err = s.persistUpdatedSandboxPod(ctx, pod)
	s.observeClaimPhase(req.Template, claimType, "persist_ready_sandbox", phaseStarted, err)
	if err != nil {
		return fail("carrier readiness persistence failed", err)
	}
	phaseStarted = time.Now()
	procdAddress, err := s.prodAddress(ctx, pod)
	s.observeClaimPhase(req.Template, claimType, "resolve_procd_address", phaseStarted, err)
	if err != nil {
		return fail("carrier procd address failed", err)
	}
	return &ClaimResponse{
		SandboxID: req.SandboxID, Status: s.podToSandboxStatus(pod), ProcdAddress: procdAddress,
		PodName: pod.Name, Template: req.Template, ClusterId: template.Spec.ClusterId,
	}, true, nil
}

func (s *SandboxService) allocateS0FSCarrier(ctx context.Context, template *api.SandboxTemplate, req *ClaimRequest, allowShared bool) (*corev1.Pod, string, error) {
	if s == nil || s.sharedCarrierPool == nil || template == nil || req == nil {
		return nil, "", fmt.Errorf("S0FS carrier allocator is not configured")
	}
	compatible, _ := carrierpool.Compatible(template)
	var pod *corev1.Pod
	var err error
	claimType := "shared"
	if allowShared && compatible {
		phaseStarted := time.Now()
		pod, err = s.sharedCarrierPool.Reserve(ctx)
		s.observeClaimPhase(req.Template, claimType, "reserve_shared_carrier", phaseStarted, err)
	}
	if err != nil {
		return nil, "", fmt.Errorf("reserve shared carrier: %w", err)
	}
	if pod == nil {
		claimType = "cold-s0fs"
		phaseStarted := time.Now()
		err = controller.EnsureProcdConfigSecret(ctx, s.k8sClient, s.secretLister, template)
		s.observeClaimPhase(req.Template, claimType, "ensure_cold_carrier_config", phaseStarted, err)
		if err != nil {
			return nil, "", fmt.Errorf("ensure cold carrier procd config: %w", err)
		}
		phaseStarted = time.Now()
		pod, err = s.sharedCarrierPool.CreateCold(ctx, template)
		s.observeClaimPhase(req.Template, claimType, "create_cold_carrier", phaseStarted, err)
		if err != nil {
			return nil, "", fmt.Errorf("create cold S0FS carrier: %w", err)
		}
		phaseStarted = time.Now()
		pod, err = s.waitForCarrierGate(ctx, pod.Namespace, pod.Name, pod.Labels[carrier.LabelGeneration])
		s.observeClaimPhase(req.Template, claimType, "wait_cold_carrier_gate", phaseStarted, err)
		if err != nil {
			s.deleteFailedCarrier(pod)
			return nil, "", err
		}
	}
	fail := func(reason string, err error) (*corev1.Pod, string, error) {
		s.deleteFailedCarrier(pod)
		return nil, "", fmt.Errorf("%s: %w", reason, err)
	}
	phaseStarted := time.Now()
	pod, err = s.assignCarrier(ctx, pod, template, req, claimType)
	s.observeClaimPhase(req.Template, claimType, "assign_carrier_metadata", phaseStarted, err)
	if err != nil {
		return fail("carrier assignment failed", err)
	}
	resourceQuota, err := s.effectiveSandboxResourceQuota(template, req.Config)
	if err != nil {
		return fail("carrier resource validation failed", err)
	}
	if sandboxPodNeedsResourceResize(pod, resourceQuota) {
		phaseStarted = time.Now()
		pod, err = s.resizeSandboxPodResourcesWithClient(ctx, s.hotClaimClient(), pod, resourceQuota)
		s.observeClaimPhase(req.Template, claimType, "resize_carrier_resources", phaseStarted, err)
		if err != nil {
			return fail("carrier resize failed", fmt.Errorf("resize S0FS carrier: %w", err))
		}
	}
	phaseStarted = time.Now()
	err = s.applyNetworkProviderFromPod(ctx, pod, req.TeamID)
	s.observeClaimPhase(req.Template, claimType, "apply_network_policy", phaseStarted, err)
	if err != nil {
		return fail("carrier network policy failed", err)
	}
	return pod, claimType, nil
}

func (s *SandboxService) activateS0FSCarrierHead(
	ctx context.Context,
	pod *corev1.Pod,
	head *sandboxstore.SandboxRootFSHead,
	template string,
	claimType string,
) (*corev1.Pod, error) {
	if pod == nil || head == nil {
		return pod, fmt.Errorf("carrier Pod and rootfs Head are required")
	}
	phaseStarted := time.Now()
	ctldAddress, err := s.ctldAddressForPod(ctx, pod)
	s.observeClaimPhase(template, claimType, "resolve_carrier_ctld", phaseStarted, err)
	if err != nil {
		return pod, fmt.Errorf("resolve carrier ctld: %w", err)
	}
	slot := pod.Annotations[carrier.AnnotationSlot]
	markerImage, err := carrier.MarkerImage(slot)
	if err != nil {
		return pod, err
	}
	phaseStarted = time.Now()
	materialized, err := s.ctldClient.MaterializeRootFSHead(ctx, ctldAddress, ctldapi.MaterializeRootFSHeadRequest{
		Reference: head.Reference, Image: head.Image, CarrierSlot: slot, TargetImageName: markerImage,
	}, sandboxRootFSOperationTimeout)
	if err != nil || materialized == nil || !materialized.Materialized || materialized.ImageName != markerImage {
		if err == nil {
			err = fmt.Errorf("ctld did not confirm carrier marker %s", markerImage)
		}
		s.observeClaimPhase(template, claimType, "materialize_carrier_image", phaseStarted, err)
		return pod, fmt.Errorf("materialize carrier rootfs: %w", err)
	}
	s.observeClaimPhase(template, claimType, "materialize_carrier_image", phaseStarted, nil)
	phaseStarted = time.Now()
	released, err := s.ctldClient.ReleaseCarrierGate(ctx, ctldAddress, ctldapi.ReleaseCarrierGateRequest{
		Namespace: pod.Namespace, PodName: pod.Name, PodUID: string(pod.UID), Slot: slot,
		SandboxID: strings.TrimSpace(pod.Annotations[controller.AnnotationSandboxID]), RuntimeGeneration: runtimeGenerationFromPod(pod),
		ContainerName: runtimecontrol.ProcdContainerName,
	}, sandboxRootFSOperationTimeout)
	if err != nil || released == nil || !released.Released {
		if err == nil {
			err = fmt.Errorf("ctld did not confirm carrier gate release")
		}
		s.observeClaimPhase(template, claimType, "release_carrier_gate", phaseStarted, err)
		return pod, fmt.Errorf("release carrier gate: %w", err)
	}
	if released.Namespace != pod.Namespace || released.PodName != pod.Name || released.PodUID != string(pod.UID) || released.Slot != slot ||
		released.SandboxID != strings.TrimSpace(pod.Annotations[controller.AnnotationSandboxID]) || released.RuntimeGeneration != runtimeGenerationFromPod(pod) ||
		released.ContainerName != runtimecontrol.ProcdContainerName || strings.TrimSpace(released.PodIP) == "" {
		err = fmt.Errorf("release carrier gate returned a conflicting runtime identity")
		s.observeClaimPhase(template, claimType, "release_carrier_gate", phaseStarted, err)
		return pod, err
	}
	s.observeClaimPhase(template, claimType, "release_carrier_gate", phaseStarted, nil)
	started := pod.DeepCopy()
	started.Status.PodIP = strings.TrimSpace(released.PodIP)
	return started, nil
}

// waitForS0FSCarrierReady overlaps the two independent direct readiness paths:
// procd must serve the exact Pod identity while ctld prepares the exact rootfs
// target. Both must complete before runtime assignment activation.
func (s *SandboxService) waitForS0FSCarrierReady(
	ctx context.Context,
	pod *corev1.Pod,
	template string,
	claimType string,
	prepareRootFS func(context.Context, *corev1.Pod) error,
) (*corev1.Pod, error) {
	if pod == nil || strings.TrimSpace(pod.Status.PodIP) == "" || prepareRootFS == nil {
		return pod, fmt.Errorf("carrier Pod IP and rootfs preparation are required")
	}
	group, readyCtx := errgroup.WithContext(ctx)
	var started *corev1.Pod
	group.Go(func() error {
		phaseStarted := time.Now()
		var err error
		started, err = s.waitForCarrierRuntimeStarted(readyCtx, pod, pod.Status.PodIP)
		s.observeClaimPhase(template, claimType, "wait_procd_startup", phaseStarted, err)
		return err
	})
	group.Go(func() error {
		return prepareRootFS(readyCtx, pod)
	})
	if err := group.Wait(); err != nil {
		return pod, err
	}
	return started, nil
}

// waitForCarrierRuntimeStarted waits for procd's own immutable Pod identity.
// Kubelet Pod status is deliberately outside the claim critical path because
// Generic PLEG can publish ContainerStatus.Running almost a second late.
func (s *SandboxService) waitForCarrierRuntimeStarted(ctx context.Context, pod *corev1.Pod, podIP string) (*corev1.Pod, error) {
	if pod == nil || s.procdClient == nil {
		return nil, fmt.Errorf("carrier Pod and procd client are required")
	}
	timeout := s.config.RuntimeReadyTimeout
	if timeout <= 0 {
		timeout = 5 * time.Minute
	}
	waitCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	address := "http://" + net.JoinHostPort(strings.TrimSpace(podIP), strconv.Itoa(s.config.ProcdPort))
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		attemptCtx, attemptCancel := context.WithTimeout(waitCtx, 200*time.Millisecond)
		startup, err := s.procdClient.Startup(attemptCtx, address)
		attemptCancel()
		if err == nil {
			if startup.Status != "started" || startup.Namespace != pod.Namespace || startup.PodName != pod.Name || startup.PodUID != string(pod.UID) {
				return nil, fmt.Errorf("procd startup identity conflicts with carrier Pod")
			}
			started := pod.DeepCopy()
			started.Status.PodIP = strings.TrimSpace(podIP)
			return started, nil
		}
		if terminal, reason := terminalPodForRuntimeWait(s.podLister, pod); terminal {
			return nil, fmt.Errorf("carrier Pod %s while waiting for procd startup", reason)
		}
		select {
		case <-waitCtx.Done():
			return nil, fmt.Errorf("wait for procd startup at %s: %w", address, waitCtx.Err())
		case <-ticker.C:
		}
	}
}

func terminalPodForRuntimeWait(podLister corelisters.PodLister, pod *corev1.Pod) (bool, string) {
	if podLister == nil || pod == nil {
		return false, ""
	}
	current, err := podLister.Pods(pod.Namespace).Get(pod.Name)
	if err != nil {
		return false, ""
	}
	if current.DeletionTimestamp != nil {
		return true, "is terminating"
	}
	if current.Status.Phase == corev1.PodFailed || current.Status.Phase == corev1.PodSucceeded {
		return true, fmt.Sprintf("entered terminal state %s", current.Status.Phase)
	}
	return false, ""
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

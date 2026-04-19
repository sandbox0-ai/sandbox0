package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/controller"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/util/retry"
)

// PauseSandboxResponse represents the response from pausing a sandbox.
type PauseSandboxResponse struct {
	SandboxID     string                `json:"sandbox_id"`
	Paused        bool                  `json:"paused"`
	PowerState    SandboxPowerState     `json:"power_state"`
	ResourceUsage *SandboxResourceUsage `json:"resource_usage,omitempty"`
	UpdatedMemory string                `json:"updated_memory,omitempty"`
	UpdatedCPU    string                `json:"updated_cpu,omitempty"`
}

// ResumeSandboxResponse represents the response from resuming a sandbox.
type ResumeSandboxResponse struct {
	SandboxID      string            `json:"sandbox_id"`
	Resumed        bool              `json:"resumed"`
	PowerState     SandboxPowerState `json:"power_state"`
	RestoredMemory string            `json:"restored_memory,omitempty"`
}

// PausedState stores the sandbox state before pause for restoration on resume.
type PausedState struct {
	// Resources stores original pod resources before pause.
	Resources map[string]ContainerResources `json:"resources"`
	// OriginalTTL stores the original TTL (in seconds) set by user or default.
	// On resume, this TTL is reused to reset the countdown.
	OriginalTTL *int32 `json:"original_ttl,omitempty"`
}

// ContainerResources stores resource requests/limits for a container.
type ContainerResources struct {
	Requests corev1.ResourceList `json:"requests,omitempty"`
	Limits   corev1.ResourceList `json:"limits,omitempty"`
}

type resumeSandboxPreparation struct {
	Pod            *corev1.Pod
	PowerState     SandboxPowerState
	RestoredMemory string
	HadPausedState bool
}

type expectedSandboxPowerState struct {
	Desired    string
	Generation int64
}

// PauseSandbox delegates sandbox pause execution to the configured power executor.
func (s *SandboxService) PauseSandbox(ctx context.Context, sandboxID string) (*PauseSandboxResponse, error) {
	return s.sandboxPowerExecutor().Pause(ctx, sandboxID)
}

// pauseSandboxLocal pauses a sandbox and reduces pod resources based on actual usage.
// This uses Kubernetes 1.35+ in-place pod update feature.
func (s *SandboxService) pauseSandboxLocal(ctx context.Context, sandboxID string) (*PauseSandboxResponse, error) {
	s.logger.Info("Pausing sandbox", zap.String("sandboxID", sandboxID))

	// Find the pod by sandbox ID
	pod, err := s.getSandboxPod(ctx, sandboxID)
	if err != nil {
		return nil, fmt.Errorf("get pod: %w", err)
	}

	// Check if already paused
	if pod.Annotations[controller.AnnotationPaused] == "true" {
		return &PauseSandboxResponse{
			SandboxID:  sandboxID,
			Paused:     true,
			PowerState: sandboxPowerStateFromAnnotations(pod.Annotations),
		}, nil
	}
	expected := currentSandboxPowerExpectation(pod.Annotations, SandboxPowerStatePaused)

	// Generate internal token for procd authentication
	if s.internalTokenGenerator == nil {
		return nil, fmt.Errorf("token generators not configured, cannot authenticate with procd")
	}
	teamID := pod.Annotations[controller.AnnotationTeamID]
	userID := pod.Annotations[controller.AnnotationUserID]

	internalToken, err := s.internalTokenGenerator.GenerateToken(teamID, userID, sandboxID)
	if err != nil {
		return nil, fmt.Errorf("generate internal token: %w", err)
	}

	// Call procd pause API
	procdAddress, err := s.prodAddress(ctx, pod)
	if err != nil {
		return nil, fmt.Errorf("get procd address: %w", err)
	}
	pauseResp, err := s.procdClient.Pause(ctx, procdAddress, internalToken)
	if err != nil {
		return nil, fmt.Errorf("call procd pause: %w", err)
	}

	if !pauseResp.Paused {
		return nil, fmt.Errorf("procd pause failed: %s", pauseResp.Error)
	}

	completedResp, err := s.completePausedSandbox(ctx, pod, sandboxID, pauseResp.ResourceUsage, expected)
	if err != nil && errors.Is(err, errSandboxPowerStateStale) && completedResp != nil && completedResp.PowerState.Desired == SandboxPowerStateActive {
		resumeResp, resumeErr := s.procdClient.Resume(ctx, procdAddress, internalToken)
		if resumeErr != nil {
			return completedResp, fmt.Errorf("resume procd after stale pause: %w", resumeErr)
		}
		if !resumeResp.Resumed {
			return completedResp, fmt.Errorf("procd resume after stale pause failed: %s", resumeResp.Error)
		}
		return &PauseSandboxResponse{SandboxID: sandboxID, Paused: false, PowerState: completedResp.PowerState, ResourceUsage: pauseResp.ResourceUsage}, nil
	}
	return completedResp, err
}

// RequestPauseSandbox records a desired paused state and reconciles it asynchronously.
func (s *SandboxService) RequestPauseSandbox(ctx context.Context, sandboxID string) (*PauseSandboxResponse, error) {
	state, err := s.requestSandboxPowerState(ctx, sandboxID, SandboxPowerStatePaused)
	if err != nil {
		return nil, err
	}
	return &PauseSandboxResponse{
		SandboxID:  sandboxID,
		Paused:     true,
		PowerState: state,
	}, nil
}

// PauseSandboxAndWait records a desired paused state and waits until the sandbox observes it.
func (s *SandboxService) PauseSandboxAndWait(ctx context.Context, sandboxID string) (*PauseSandboxResponse, error) {
	resp, err := s.RequestPauseSandbox(ctx, sandboxID)
	if err != nil {
		return nil, err
	}
	if resp.PowerState.Desired == SandboxPowerStatePaused && resp.PowerState.Observed == SandboxPowerStatePaused && resp.PowerState.Phase == SandboxPowerPhaseStable {
		return resp, nil
	}
	waitCtx, cancel := sandboxPowerTransitionContext(ctx)
	defer cancel()
	state, err := s.waitForSandboxPowerState(waitCtx, sandboxID, SandboxPowerStatePaused, resp.PowerState.DesiredGeneration)
	if err != nil {
		return &PauseSandboxResponse{SandboxID: sandboxID, Paused: state.Observed == SandboxPowerStatePaused, PowerState: state}, err
	}
	resp.PowerState = state
	resp.Paused = true
	return resp, nil
}

// ResumeSandbox delegates sandbox resume execution to the configured power executor.
func (s *SandboxService) ResumeSandbox(ctx context.Context, sandboxID string) (*ResumeSandboxResponse, error) {
	return s.sandboxPowerExecutor().Resume(ctx, sandboxID)
}

// resumeSandboxLocal resumes a paused sandbox and restores original pod resources.
func (s *SandboxService) resumeSandboxLocal(ctx context.Context, sandboxID string) (*ResumeSandboxResponse, error) {
	s.logger.Info("Resuming sandbox", zap.String("sandboxID", sandboxID))

	// Find the pod by sandbox ID
	pod, err := s.getSandboxPod(ctx, sandboxID)
	if err != nil {
		return nil, fmt.Errorf("get pods: %w", err)
	}
	expected := currentSandboxPowerExpectation(pod.Annotations, SandboxPowerStateActive)

	prep, resp, err := s.prepareSandboxResume(ctx, pod, sandboxID, expected)
	if err != nil {
		return nil, err
	}
	if resp != nil {
		return resp, nil
	}
	pod = prep.Pod

	// Generate internal token for procd authentication
	if s.internalTokenGenerator == nil {
		return nil, fmt.Errorf("token generators not configured, cannot authenticate with procd")
	}
	teamID := pod.Annotations[controller.AnnotationTeamID]
	userID := pod.Annotations[controller.AnnotationUserID]

	internalToken, err := s.internalTokenGenerator.GenerateToken(teamID, userID, sandboxID)
	if err != nil {
		return nil, fmt.Errorf("generate internal token: %w", err)
	}

	// Call procd resume API
	procdAddress, err := s.prodAddress(ctx, pod)
	if err != nil {
		return nil, fmt.Errorf("get procd address: %w", err)
	}
	resumeResp, err := s.procdClient.Resume(ctx, procdAddress, internalToken)
	if err != nil {
		return nil, fmt.Errorf("call procd resume: %w", err)
	}

	if !resumeResp.Resumed {
		return nil, fmt.Errorf("procd resume failed: %s", resumeResp.Error)
	}

	powerState, err := s.completeSandboxResume(ctx, sandboxID, expected)
	if err != nil {
		return nil, err
	}

	s.logger.Info("Sandbox resumed successfully",
		zap.String("sandboxID", sandboxID),
		zap.String("restoredMemory", prep.RestoredMemory),
	)

	return &ResumeSandboxResponse{
		SandboxID:      sandboxID,
		Resumed:        true,
		PowerState:     powerState,
		RestoredMemory: prep.RestoredMemory,
	}, nil
}

func (s *SandboxService) completePausedSandbox(ctx context.Context, pod *corev1.Pod, sandboxID string, usage *SandboxResourceUsage, expected expectedSandboxPowerState) (*PauseSandboxResponse, error) {
	if pod.Annotations[controller.AnnotationPaused] == "true" {
		return &PauseSandboxResponse{
			SandboxID:  sandboxID,
			Paused:     true,
			PowerState: sandboxPowerStateFromAnnotations(pod.Annotations),
		}, nil
	}

	pausedState := PausedState{Resources: s.extractOriginalResources(pod)}
	if configJSON := pod.Annotations[controller.AnnotationConfig]; configJSON != "" {
		var config SandboxConfig
		if err := json.Unmarshal([]byte(configJSON), &config); err == nil && config.TTL != nil {
			pausedState.OriginalTTL = config.TTL
		}
	}
	pausedStateJSON, err := json.Marshal(pausedState)
	if err != nil {
		return nil, fmt.Errorf("marshal paused state: %w", err)
	}

	var newRequestMemory resource.Quantity
	var newLimitMemory resource.Quantity
	if usage != nil && usage.ContainerMemoryWorkingSet > 0 {
		workingSet := usage.ContainerMemoryWorkingSet
		reqBytes := int64(workingSet)
		minReq, err := resource.ParseQuantity(s.config.PauseMinMemoryRequest)
		if err == nil && reqBytes < minReq.Value() {
			reqBytes = minReq.Value()
		}
		newRequestMemory = *resource.NewQuantity(reqBytes, resource.BinarySI)

		limitBytes := int64(float64(workingSet) * s.config.PauseMemoryBufferRatio)
		minLimit, err := resource.ParseQuantity(s.config.PauseMinMemoryLimit)
		if err == nil && limitBytes < minLimit.Value() {
			limitBytes = minLimit.Value()
		}
		newLimitMemory = *resource.NewQuantity(limitBytes, resource.BinarySI)
	}

	minCPU := resource.MustParse(s.config.PauseMinCPU)
	var powerState SandboxPowerState
	err = retry.RetryOnConflict(retry.DefaultRetry, func() error {
		currentPod, getErr := s.getSandboxPodForPowerState(ctx, sandboxID)
		if getErr != nil {
			return getErr
		}
		if currentPod.Annotations[controller.AnnotationPaused] == "true" {
			powerState = sandboxPowerStateFromAnnotations(currentPod.Annotations)
			pod = currentPod
			return nil
		}
		currentState, matchErr := s.matchSandboxPowerExpectation(currentPod, expected)
		if matchErr != nil {
			powerState = currentState
			return matchErr
		}
		annotatedPod := currentPod.DeepCopy()
		if annotatedPod.Annotations == nil {
			annotatedPod.Annotations = make(map[string]string)
		}
		annotatedPod.Annotations[controller.AnnotationPaused] = "true"
		annotatedPod.Annotations[controller.AnnotationPausedAt] = s.clock.Now().Format(time.RFC3339)
		annotatedPod.Annotations[controller.AnnotationPausedState] = string(pausedStateJSON)
		powerState = completedSandboxPowerState(annotatedPod.Annotations, SandboxPowerStatePaused)
		applySandboxPowerStateAnnotations(annotatedPod.Annotations, powerState)
		delete(annotatedPod.Annotations, controller.AnnotationExpiresAt)

		updatedPod, updateErr := s.k8sClient.CoreV1().Pods(currentPod.Namespace).Update(ctx, annotatedPod, metav1.UpdateOptions{})
		if updateErr != nil {
			return updateErr
		}
		pod = updatedPod
		return nil
	})
	if err != nil {
		if errors.Is(err, errSandboxPowerStateStale) {
			return &PauseSandboxResponse{
				SandboxID:     sandboxID,
				Paused:        powerState.Desired == SandboxPowerStatePaused,
				PowerState:    powerState,
				ResourceUsage: usage,
			}, err
		}
		return nil, fmt.Errorf("update pod annotations after pause: %w", err)
	}

	if !newLimitMemory.IsZero() || !minCPU.IsZero() {
		resizePod := pod.DeepCopy()
		found := s.applyPausedResourceTargets(resizePod, newRequestMemory, newLimitMemory, minCPU)
		if !found {
			s.logger.Warn("Main container 'procd' not found during pause resource update", zap.String("sandboxID", sandboxID))
		} else if _, err = s.k8sClient.CoreV1().Pods(pod.Namespace).UpdateResize(ctx, pod.Name, resizePod, metav1.UpdateOptions{}); err != nil {
			s.logger.Error("Failed to update pod resources after pause",
				zap.String("sandboxID", sandboxID),
				zap.Error(err),
			)
		}
	}

	workingSet := int64(0)
	if usage != nil {
		workingSet = usage.ContainerMemoryWorkingSet
	}
	s.logger.Info("Sandbox paused successfully",
		zap.String("sandboxID", sandboxID),
		zap.String("newRequest", newRequestMemory.String()),
		zap.String("newLimit", newLimitMemory.String()),
		zap.Int64("workingSet", workingSet),
	)

	return &PauseSandboxResponse{
		SandboxID:     sandboxID,
		Paused:        true,
		PowerState:    powerState,
		ResourceUsage: usage,
		UpdatedMemory: newLimitMemory.String(),
		UpdatedCPU:    minCPU.String(),
	}, nil
}

func (s *SandboxService) applyPausedResourceTargets(resizePod *corev1.Pod, newRequestMemory, newLimitMemory, minCPU resource.Quantity) bool {
	if resizePod == nil {
		return false
	}
	for i := range resizePod.Spec.Containers {
		container := &resizePod.Spec.Containers[i]
		if container.Name != "procd" {
			continue
		}
		if container.Resources.Requests == nil {
			container.Resources.Requests = make(corev1.ResourceList)
		}
		if !newRequestMemory.IsZero() {
			container.Resources.Requests[corev1.ResourceMemory] = newRequestMemory
		}
		container.Resources.Requests[corev1.ResourceCPU] = minCPU
		if container.Resources.Limits == nil {
			container.Resources.Limits = make(corev1.ResourceList)
		}
		if !newLimitMemory.IsZero() {
			container.Resources.Limits[corev1.ResourceMemory] = newLimitMemory
		}
		container.Resources.Limits[corev1.ResourceCPU] = minCPU
		return true
	}
	return false
}

func (s *SandboxService) prepareSandboxResume(ctx context.Context, pod *corev1.Pod, sandboxID string, expected expectedSandboxPowerState) (*resumeSandboxPreparation, *ResumeSandboxResponse, error) {
	if pod.Annotations[controller.AnnotationPaused] != "true" && !sandboxPodMayHaveFrozenCgroup(pod) {
		return nil, &ResumeSandboxResponse{
			SandboxID:  sandboxID,
			Resumed:    true,
			PowerState: sandboxPowerStateFromAnnotations(pod.Annotations),
		}, nil
	}
	if pod.Annotations[controller.AnnotationPaused] != "true" {
		return &resumeSandboxPreparation{Pod: pod, PowerState: sandboxPowerStateFromAnnotations(pod.Annotations)}, nil, nil
	}

	var restoredMemory string
	var powerState SandboxPowerState
	var hadPausedState bool
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		currentPod, getErr := s.getSandboxPodForPowerState(ctx, sandboxID)
		if getErr != nil {
			return getErr
		}
		if currentPod.Annotations[controller.AnnotationPaused] != "true" {
			powerState = sandboxPowerStateFromAnnotations(currentPod.Annotations)
			pod = currentPod
			return nil
		}
		currentState, matchErr := s.matchSandboxPowerExpectation(currentPod, expected)
		if matchErr != nil {
			powerState = currentState
			return matchErr
		}

		powerState = requestedSandboxPowerState(currentPod.Annotations, SandboxPowerStateActive)
		pod = currentPod

		pausedStateJSON := currentPod.Annotations[controller.AnnotationPausedState]
		if pausedStateJSON == "" {
			hadPausedState = false
			return nil
		}
		var pausedState PausedState
		if err := json.Unmarshal([]byte(pausedStateJSON), &pausedState); err != nil {
			hadPausedState = false
			return nil
		}
		hadPausedState = true
		resizePod := currentPod.DeepCopy()
		for i := range resizePod.Spec.Containers {
			container := &resizePod.Spec.Containers[i]
			if orig, ok := pausedState.Resources[container.Name]; ok {
				container.Resources.Requests = orig.Requests
				container.Resources.Limits = orig.Limits
				if memReq, ok := orig.Requests[corev1.ResourceMemory]; ok {
					restoredMemory = memReq.String()
				}
			}
		}
		_, updateErr := s.k8sClient.CoreV1().Pods(currentPod.Namespace).UpdateResize(ctx, currentPod.Name, resizePod, metav1.UpdateOptions{})
		return updateErr
	})
	if err != nil {
		if errors.Is(err, errSandboxPowerStateStale) {
			return nil, &ResumeSandboxResponse{
				SandboxID:  sandboxID,
				Resumed:    powerState.Desired == SandboxPowerStateActive,
				PowerState: powerState,
			}, err
		}
		return nil, nil, fmt.Errorf("restore pod resources before resume: %w", err)
	}

	if pod.Annotations[controller.AnnotationPaused] != "true" {
		return nil, &ResumeSandboxResponse{
			SandboxID:  sandboxID,
			Resumed:    true,
			PowerState: powerState,
		}, nil
	}

	return &resumeSandboxPreparation{Pod: pod, PowerState: powerState, RestoredMemory: restoredMemory, HadPausedState: hadPausedState}, nil, nil
}

func (s *SandboxService) completeSandboxResume(ctx context.Context, sandboxID string, expected expectedSandboxPowerState) (SandboxPowerState, error) {
	var powerState SandboxPowerState
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		currentPod, getErr := s.getSandboxPodForPowerState(ctx, sandboxID)
		if getErr != nil {
			return getErr
		}
		if currentPod.Annotations[controller.AnnotationPaused] != "true" {
			currentState := sandboxPowerStateFromAnnotations(currentPod.Annotations)
			if !sandboxPodMayHaveFrozenCgroup(currentPod) && currentState.Desired == SandboxPowerStateActive && currentState.Observed == SandboxPowerStateActive && currentState.Phase == SandboxPowerPhaseStable {
				powerState = currentState
				return nil
			}
			annotationPod := currentPod.DeepCopy()
			if annotationPod.Annotations == nil {
				annotationPod.Annotations = make(map[string]string)
			}
			powerState = completedSandboxPowerState(annotationPod.Annotations, SandboxPowerStateActive)
			applySandboxPowerStateAnnotations(annotationPod.Annotations, powerState)
			_, updateErr := s.k8sClient.CoreV1().Pods(currentPod.Namespace).Update(ctx, annotationPod, metav1.UpdateOptions{})
			return updateErr
		}
		currentState, matchErr := s.matchSandboxPowerExpectation(currentPod, expected)
		if matchErr != nil {
			powerState = currentState
			return matchErr
		}

		annotationPod := currentPod.DeepCopy()
		if annotationPod.Annotations == nil {
			annotationPod.Annotations = make(map[string]string)
		}
		var ttlToRestore *int32
		pausedStateJSON := currentPod.Annotations[controller.AnnotationPausedState]
		if pausedStateJSON != "" {
			var pausedState PausedState
			if err := json.Unmarshal([]byte(pausedStateJSON), &pausedState); err == nil {
				ttlToRestore = pausedState.OriginalTTL
			}
		}
		if ttlToRestore == nil && s.config.DefaultTTL > 0 {
			ttlToRestore = int32Ptr(int32(s.config.DefaultTTL.Seconds()))
		}
		setExpirationAnnotation(annotationPod.Annotations, s.clock.Now(), ttlToRestore)
		delete(annotationPod.Annotations, controller.AnnotationPaused)
		delete(annotationPod.Annotations, controller.AnnotationPausedAt)
		delete(annotationPod.Annotations, controller.AnnotationPausedState)
		powerState = completedSandboxPowerState(annotationPod.Annotations, SandboxPowerStateActive)
		applySandboxPowerStateAnnotations(annotationPod.Annotations, powerState)
		_, updateErr := s.k8sClient.CoreV1().Pods(currentPod.Namespace).Update(ctx, annotationPod, metav1.UpdateOptions{})
		return updateErr
	})
	if err != nil {
		return powerState, err
	}
	return powerState, nil
}

// RequestResumeSandbox records a desired active state and reconciles it asynchronously.
func (s *SandboxService) RequestResumeSandbox(ctx context.Context, sandboxID string) (*ResumeSandboxResponse, error) {
	state, err := s.requestSandboxPowerState(ctx, sandboxID, SandboxPowerStateActive)
	if err != nil {
		return nil, err
	}
	return &ResumeSandboxResponse{
		SandboxID:  sandboxID,
		Resumed:    true,
		PowerState: state,
	}, nil
}

// ResumeSandboxAndWait records a desired active state and waits until the sandbox observes it.
func (s *SandboxService) ResumeSandboxAndWait(ctx context.Context, sandboxID string) (*ResumeSandboxResponse, error) {
	resp, err := s.RequestResumeSandbox(ctx, sandboxID)
	if err != nil {
		return nil, err
	}
	if resp.PowerState.Desired == SandboxPowerStateActive && resp.PowerState.Observed == SandboxPowerStateActive && resp.PowerState.Phase == SandboxPowerPhaseStable {
		return resp, nil
	}
	waitCtx, cancel := sandboxPowerTransitionContext(ctx)
	defer cancel()
	state, err := s.waitForSandboxPowerState(waitCtx, sandboxID, SandboxPowerStateActive, resp.PowerState.DesiredGeneration)
	if err != nil {
		return &ResumeSandboxResponse{SandboxID: sandboxID, Resumed: state.Observed == SandboxPowerStateActive, PowerState: state}, err
	}
	resp.PowerState = state
	resp.Resumed = true
	return resp, nil
}

// RequestPauseSandboxByID records the desired paused state for controller-driven reconciliation.
func (s *SandboxService) RequestPauseSandboxByID(ctx context.Context, sandboxID string) error {
	_, err := s.RequestPauseSandbox(ctx, sandboxID)
	return err
}

// PauseSandboxByID records the desired paused state for compatibility with older controller callers.
//
// Deprecated: use RequestPauseSandboxByID for declarative pause requests.
func (s *SandboxService) PauseSandboxByID(ctx context.Context, sandboxID string) error {
	return s.RequestPauseSandboxByID(ctx, sandboxID)
}

// TerminateSandboxByID implements the SandboxTerminator interface from controller package.
// It wraps TerminateSandbox and returns only the error.
func (s *SandboxService) TerminateSandboxByID(ctx context.Context, sandboxID string) error {
	return s.TerminateSandbox(ctx, sandboxID)
}

// GetSandboxResourceUsage gets the resource usage of a sandbox.
func (s *SandboxService) GetSandboxResourceUsage(ctx context.Context, sandboxID string) (*SandboxResourceUsage, error) {
	// Find the pod by sandbox ID
	pod, err := s.getSandboxPod(ctx, sandboxID)
	if err != nil {
		return nil, fmt.Errorf("get pod: %w", err)
	}

	// Generate internal token for procd authentication
	if s.internalTokenGenerator == nil {
		return nil, fmt.Errorf("token generators not configured, cannot authenticate with procd")
	}
	teamID := pod.Annotations[controller.AnnotationTeamID]
	userID := pod.Annotations[controller.AnnotationUserID]

	internalToken, err := s.internalTokenGenerator.GenerateToken(teamID, userID, sandboxID)
	if err != nil {
		return nil, fmt.Errorf("generate internal token: %w", err)
	}

	// Call procd stats API
	procdAddress, err := s.prodAddress(ctx, pod)
	if err != nil {
		return nil, fmt.Errorf("get procd address: %w", err)
	}
	statsResp, err := s.procdClient.Stats(ctx, procdAddress, internalToken)
	if err != nil {
		return nil, fmt.Errorf("call procd stats: %w", err)
	}

	return &statsResp.SandboxResourceUsage, nil
}

// extractOriginalResources extracts current resources from pod containers.
func (s *SandboxService) extractOriginalResources(pod *corev1.Pod) map[string]ContainerResources {
	resources := make(map[string]ContainerResources)

	for _, container := range pod.Spec.Containers {
		resources[container.Name] = ContainerResources{
			Requests: container.Resources.Requests.DeepCopy(),
			Limits:   container.Resources.Limits.DeepCopy(),
		}
	}

	return resources
}

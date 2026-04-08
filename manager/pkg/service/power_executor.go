package service

import (
	"context"
	"fmt"

	"github.com/sandbox0-ai/sandbox0/pkg/ctldapi"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// SandboxPowerExecutor executes pause and resume transitions for a sandbox.
// The default implementation stays manager-local today and will be replaced by ctld later.
type SandboxPowerExecutor interface {
	Pause(ctx context.Context, sandboxID string) (*PauseSandboxResponse, error)
	Resume(ctx context.Context, sandboxID string) (*ResumeSandboxResponse, error)
}

type localSandboxPowerExecutor struct {
	service *SandboxService
}

type ctldSandboxPowerExecutor struct {
	service *SandboxService
}

func newSandboxPowerExecutor(service *SandboxService) SandboxPowerExecutor {
	if service != nil && service.config.CtldEnabled {
		return &ctldSandboxPowerExecutor{service: service}
	}
	return newLocalSandboxPowerExecutor(service)
}

func newLocalSandboxPowerExecutor(service *SandboxService) SandboxPowerExecutor {
	return &localSandboxPowerExecutor{service: service}
}

func (e *localSandboxPowerExecutor) Pause(ctx context.Context, sandboxID string) (*PauseSandboxResponse, error) {
	return e.service.pauseSandboxLocal(ctx, sandboxID)
}

func (e *localSandboxPowerExecutor) Resume(ctx context.Context, sandboxID string) (*ResumeSandboxResponse, error) {
	return e.service.resumeSandboxLocal(ctx, sandboxID)
}

func (e *ctldSandboxPowerExecutor) Pause(ctx context.Context, sandboxID string) (*PauseSandboxResponse, error) {
	pod, err := e.service.getSandboxPodForPowerState(ctx, sandboxID)
	if err != nil {
		return nil, fmt.Errorf("get pod: %w", err)
	}
	ctldAddress, err := e.service.ctldAddressForSandbox(ctx, sandboxID)
	if err != nil {
		return nil, err
	}
	resp, err := e.service.ctldClient.Pause(ctx, ctldAddress, sandboxID)
	if err != nil {
		return nil, err
	}
	if !resp.Paused {
		return nil, fmt.Errorf("ctld pause failed: %s", resp.Error)
	}
	return e.service.completePausedSandbox(ctx, pod, sandboxID, sandboxUsageFromCtld(resp.ResourceUsage))
}

func (e *ctldSandboxPowerExecutor) Resume(ctx context.Context, sandboxID string) (*ResumeSandboxResponse, error) {
	pod, err := e.service.getSandboxPodForPowerState(ctx, sandboxID)
	if err != nil {
		return nil, fmt.Errorf("get pod: %w", err)
	}
	prep, resp, err := e.service.prepareSandboxResume(ctx, pod, sandboxID)
	if err != nil {
		return nil, err
	}
	if resp != nil {
		return resp, nil
	}
	ctldAddress, err := e.service.ctldAddressForPod(ctx, prep.Pod)
	if err != nil {
		return nil, err
	}
	ctldResp, err := e.service.ctldClient.Resume(ctx, ctldAddress, sandboxID)
	if err != nil {
		return nil, err
	}
	if !ctldResp.Resumed {
		return nil, fmt.Errorf("ctld resume failed: %s", ctldResp.Error)
	}
	return &ResumeSandboxResponse{SandboxID: sandboxID, Resumed: true, PowerState: prep.PowerState, RestoredMemory: prep.RestoredMemory}, nil
}

func sandboxUsageFromCtld(in *ctldapi.SandboxResourceUsage) *SandboxResourceUsage {
	if in == nil {
		return nil
	}
	return &SandboxResourceUsage{
		ContainerMemoryUsage:      in.ContainerMemoryUsage,
		ContainerMemoryLimit:      in.ContainerMemoryLimit,
		ContainerMemoryWorkingSet: in.ContainerMemoryWorkingSet,
		TotalMemoryRSS:            in.TotalMemoryRSS,
		TotalMemoryVMS:            in.TotalMemoryVMS,
		TotalOpenFiles:            in.TotalOpenFiles,
		TotalThreadCount:          in.TotalThreadCount,
		TotalIOReadBytes:          in.TotalIOReadBytes,
		TotalIOWriteBytes:         in.TotalIOWriteBytes,
		ContextCount:              in.ContextCount,
		RunningContextCount:       in.RunningContextCount,
		PausedContextCount:        in.PausedContextCount,
	}
}

func (s *SandboxService) ctldAddressForSandbox(ctx context.Context, sandboxID string) (string, error) {
	pod, err := s.getSandboxPodForPowerState(ctx, sandboxID)
	if err != nil {
		return "", fmt.Errorf("get pod: %w", err)
	}
	return s.ctldAddressForPod(ctx, pod)
}

func (s *SandboxService) ctldAddressForPod(ctx context.Context, pod *corev1.Pod) (string, error) {
	if pod == nil {
		return "", fmt.Errorf("pod is nil")
	}
	if s.k8sClient == nil {
		return "", fmt.Errorf("kubernetes client is not configured")
	}
	if pod.Spec.NodeName == "" {
		return "", fmt.Errorf("sandbox pod %s/%s is not scheduled", pod.Namespace, pod.Name)
	}
	node, err := s.k8sClient.CoreV1().Nodes().Get(ctx, pod.Spec.NodeName, metav1.GetOptions{})
	if err != nil {
		return "", fmt.Errorf("get node %s: %w", pod.Spec.NodeName, err)
	}
	for _, address := range node.Status.Addresses {
		if address.Type == corev1.NodeInternalIP && address.Address != "" {
			return fmt.Sprintf("http://%s:%d", address.Address, s.config.CtldPort), nil
		}
	}
	return "", fmt.Errorf("node %s has no internal ip", node.Name)
}

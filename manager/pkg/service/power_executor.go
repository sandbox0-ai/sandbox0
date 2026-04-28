package service

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// SandboxPowerExecutor executes pause and resume transitions for a sandbox.
// The manager-local implementation executes transitions directly; ctld mode records desired state for node-local reconciliation.
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
	return e.service.RequestPauseSandbox(ctx, sandboxID)
}

func (e *ctldSandboxPowerExecutor) Resume(ctx context.Context, sandboxID string) (*ResumeSandboxResponse, error) {
	return e.service.RequestResumeSandbox(ctx, sandboxID)
}

func (s *SandboxService) ctldAddressForPod(ctx context.Context, pod *corev1.Pod) (string, error) {
	if pod == nil {
		return "", fmt.Errorf("pod is nil")
	}
	if pod.Status.HostIP != "" {
		return fmt.Sprintf("http://%s:%d", pod.Status.HostIP, s.config.CtldPort), nil
	}
	if pod.Spec.NodeName == "" {
		return "", fmt.Errorf("sandbox pod %s/%s is not scheduled", pod.Namespace, pod.Name)
	}

	if s.nodeLister != nil {
		node, err := s.nodeLister.Get(pod.Spec.NodeName)
		if err == nil {
			return ctldAddressForNode(node, s.config.CtldPort)
		}
	}

	if s.k8sClient == nil {
		return "", fmt.Errorf("kubernetes client is not configured")
	}
	node, err := s.k8sClient.CoreV1().Nodes().Get(ctx, pod.Spec.NodeName, metav1.GetOptions{})
	if err != nil {
		return "", fmt.Errorf("get node %s: %w", pod.Spec.NodeName, err)
	}
	return ctldAddressForNode(node, s.config.CtldPort)
}

func ctldAddressForNode(node *corev1.Node, port int) (string, error) {
	if node == nil {
		return "", fmt.Errorf("node is nil")
	}
	for _, address := range node.Status.Addresses {
		if address.Type == corev1.NodeInternalIP && address.Address != "" {
			return fmt.Sprintf("http://%s:%d", address.Address, port), nil
		}
	}
	return "", fmt.Errorf("node %s has no internal ip", node.Name)
}

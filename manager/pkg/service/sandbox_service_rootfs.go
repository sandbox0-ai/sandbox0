package service

import (
	"context"
	"fmt"
	"strings"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/pkg/ctldapi"
	corev1 "k8s.io/api/core/v1"
)

const sandboxRootFSMountPath = v1alpha1.SandboxRootFSMountPath

func (s *SandboxService) bindSandboxRootFS(ctx context.Context, pod *corev1.Pod, req *ClaimRequest) error {
	if s == nil || !s.config.CtldEnabled || req == nil || strings.TrimSpace(req.FilesystemID) == "" {
		return nil
	}
	if pod == nil {
		return fmt.Errorf("sandbox pod is required")
	}
	if s.ctldClient == nil {
		return fmt.Errorf("ctld client is not configured")
	}
	ctldAddress, err := s.ctldAddressForPod(ctx, pod)
	if err != nil {
		return err
	}
	resp, err := s.ctldClient.BindSandboxRootFS(ctx, ctldAddress, ctldapi.BindSandboxRootFSRequest{
		Namespace:         pod.Namespace,
		PodName:           pod.Name,
		PodUID:            string(pod.UID),
		ContainerID:       procdContainerID(pod),
		SandboxID:         req.SandboxID,
		TeamID:            req.TeamID,
		FilesystemID:      req.FilesystemID,
		RuntimeGeneration: req.RuntimeGeneration,
		BaseImageRef:      req.FilesystemBaseImageRef,
		BaseImageDigest:   req.FilesystemBaseImageDigest,
		TargetPath:        sandboxRootFSMountPath,
		RootFSVolumeName:  v1alpha1.SandboxRootFSVolumeName,
	})
	if err != nil {
		return err
	}
	if resp == nil || !rootFSBindResponseMatches(resp, req.FilesystemID) {
		return fmt.Errorf("ctld sandbox rootfs bind did not return filesystem %s", req.FilesystemID)
	}
	return nil
}

func (s *SandboxService) flushSandboxRootFSForPod(ctx context.Context, pod *corev1.Pod, filesystemID, teamID, sandboxID string, runtimeGeneration int64) error {
	if s == nil || !s.config.CtldEnabled || strings.TrimSpace(filesystemID) == "" {
		return nil
	}
	if pod == nil {
		return fmt.Errorf("sandbox pod is required")
	}
	if s.ctldClient == nil {
		return fmt.Errorf("ctld client is not configured")
	}
	ctldAddress, err := s.ctldAddressForPod(ctx, pod)
	if err != nil {
		return err
	}
	resp, err := s.ctldClient.FlushSandboxRootFS(ctx, ctldAddress, ctldapi.FlushSandboxRootFSRequest{
		Namespace:         pod.Namespace,
		PodName:           pod.Name,
		PodUID:            string(pod.UID),
		SandboxID:         sandboxID,
		TeamID:            teamID,
		FilesystemID:      filesystemID,
		RuntimeGeneration: runtimeGeneration,
	})
	if err != nil {
		return err
	}
	if resp == nil || !resp.Flushed {
		return fmt.Errorf("ctld sandbox rootfs flush did not complete for filesystem %s", filesystemID)
	}
	return nil
}

func (s *SandboxService) releaseSandboxRootFSForPod(ctx context.Context, pod *corev1.Pod, req *ClaimRequest) error {
	if req == nil {
		return nil
	}
	return s.releaseSandboxRootFSForLifecycle(ctx, SandboxLifecycleInfo{
		Namespace:         podNamespace(pod),
		PodName:           podName(pod),
		SandboxID:         req.SandboxID,
		TeamID:            req.TeamID,
		FilesystemID:      req.FilesystemID,
		PodUID:            podUID(pod),
		NodeName:          podNodeName(pod),
		HostIP:            podHostIP(pod),
		RuntimeGeneration: req.RuntimeGeneration,
	})
}

func (s *SandboxService) releaseSandboxRootFSForLifecycle(ctx context.Context, info SandboxLifecycleInfo) error {
	if s == nil || !s.config.CtldEnabled || strings.TrimSpace(info.FilesystemID) == "" {
		return nil
	}
	if s.ctldClient == nil {
		return fmt.Errorf("ctld client is not configured")
	}
	if strings.TrimSpace(info.HostIP) == "" && strings.TrimSpace(info.NodeName) == "" {
		return nil
	}
	ctldAddress, err := s.ctldAddressForLifecycleInfo(ctx, info)
	if err != nil {
		return err
	}
	resp, err := s.ctldClient.ReleaseSandboxRootFS(ctx, ctldAddress, ctldapi.ReleaseSandboxRootFSRequest{
		Namespace:         info.Namespace,
		PodName:           info.PodName,
		PodUID:            info.PodUID,
		SandboxID:         info.SandboxID,
		TeamID:            info.TeamID,
		FilesystemID:      info.FilesystemID,
		RuntimeGeneration: info.RuntimeGeneration,
	})
	if err != nil {
		return err
	}
	if resp == nil || !resp.Released {
		return fmt.Errorf("ctld sandbox rootfs release did not complete for filesystem %s", info.FilesystemID)
	}
	return nil
}

func rootFSBindResponseMatches(resp *ctldapi.BindSandboxRootFSResponse, filesystemID string) bool {
	if resp == nil {
		return false
	}
	return strings.TrimSpace(resp.FilesystemID) == strings.TrimSpace(filesystemID)
}

func podNamespace(pod *corev1.Pod) string {
	if pod == nil {
		return ""
	}
	return pod.Namespace
}

func podName(pod *corev1.Pod) string {
	if pod == nil {
		return ""
	}
	return pod.Name
}

func podUID(pod *corev1.Pod) string {
	if pod == nil {
		return ""
	}
	return string(pod.UID)
}

func podNodeName(pod *corev1.Pod) string {
	if pod == nil {
		return ""
	}
	return pod.Spec.NodeName
}

func podHostIP(pod *corev1.Pod) string {
	if pod == nil {
		return ""
	}
	return pod.Status.HostIP
}

func procdContainerID(pod *corev1.Pod) string {
	if pod == nil {
		return ""
	}
	for _, status := range pod.Status.ContainerStatuses {
		if status.Name == "procd" {
			return strings.TrimSpace(status.ContainerID)
		}
	}
	return ""
}

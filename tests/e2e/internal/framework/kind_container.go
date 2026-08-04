package framework

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	baseframework "github.com/sandbox0-ai/sandbox0/internal/framework"
)

// StopKindPodContainer stops a Pod container through the CRI on its Kind node.
func StopKindPodContainer(ctx context.Context, kubeconfig, namespace, podName, containerName string) error {
	if strings.TrimSpace(podName) == "" {
		return fmt.Errorf("pod name is required")
	}
	if strings.TrimSpace(containerName) == "" {
		return fmt.Errorf("container name is required")
	}

	args := []string{"get", "pod", podName, "-o", "json"}
	if namespace != "" {
		args = append(args, "--namespace", namespace)
	}
	output, err := baseframework.KubectlOutput(ctx, kubeconfig, args...)
	if err != nil {
		return fmt.Errorf("get pod %q: %w", podName, err)
	}

	target, err := extractKindPodContainerTarget([]byte(output), containerName)
	if err != nil {
		return fmt.Errorf("resolve container %q in pod %q: %w", containerName, podName, err)
	}

	return baseframework.RunCommand(ctx, "docker", "exec", target.NodeName, "crictl", "stop", "-t", "0", target.ContainerID)
}

type kindPodContainerTarget struct {
	NodeName    string
	ContainerID string
}

func extractKindPodContainerTarget(data []byte, containerName string) (kindPodContainerTarget, error) {
	if strings.TrimSpace(containerName) == "" {
		return kindPodContainerTarget{}, fmt.Errorf("container name is required")
	}

	var pod struct {
		Spec struct {
			NodeName string `json:"nodeName"`
		} `json:"spec"`
		Status struct {
			ContainerStatuses []kindContainerStatus `json:"containerStatuses"`
		} `json:"status"`
	}
	if err := json.Unmarshal(data, &pod); err != nil {
		return kindPodContainerTarget{}, fmt.Errorf("decode pod: %w", err)
	}

	nodeName := strings.TrimSpace(pod.Spec.NodeName)
	if nodeName == "" {
		return kindPodContainerTarget{}, fmt.Errorf("pod node name is empty")
	}
	status, found := kindContainerStatusByName(pod.Status.ContainerStatuses, containerName)
	if !found {
		return kindPodContainerTarget{}, fmt.Errorf("container status is missing")
	}

	containerID := strings.TrimSpace(status.ContainerID)
	if _, rawID, hasScheme := strings.Cut(containerID, "://"); hasScheme {
		containerID = strings.TrimSpace(rawID)
	}
	if containerID == "" {
		return kindPodContainerTarget{}, fmt.Errorf("container ID is empty")
	}

	return kindPodContainerTarget{NodeName: nodeName, ContainerID: containerID}, nil
}

type kindContainerStatus struct {
	Name        string `json:"name"`
	ContainerID string `json:"containerID"`
}

func kindContainerStatusByName(statuses []kindContainerStatus, name string) (kindContainerStatus, bool) {
	for _, status := range statuses {
		if status.Name == name {
			return status, true
		}
	}
	return kindContainerStatus{}, false
}

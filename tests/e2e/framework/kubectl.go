package framework

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// Kubectl runs a kubectl command with optional kubeconfig.
func Kubectl(ctx context.Context, kubeconfig string, args ...string) error {
	fmt.Printf("Running kubectl command: %s %v\n", "kubectl", args)
	if kubeconfig != "" {
		args = append(args, "--kubeconfig", kubeconfig)
	}
	return RunCommand(ctx, "kubectl", args...)
}

// KubectlOutput runs a kubectl command and returns output.
func KubectlOutput(ctx context.Context, kubeconfig string, args ...string) (string, error) {
	fmt.Printf("Running kubectl command: %s %v\n", "kubectl", args)
	if kubeconfig != "" {
		args = append(args, "--kubeconfig", kubeconfig)
	}
	return RunCommandOutput(ctx, "kubectl", args...)
}

// KubectlWaitForCondition waits for a condition on a resource.
func KubectlWaitForCondition(ctx context.Context, kubeconfig, namespace, resource, name, condition, timeout string) error {
	fmt.Printf("Waiting for condition %q on resource %q/%q in namespace %q with timeout %q...\n", condition, resource, name, namespace, timeout)
	if resource == "" || name == "" || condition == "" {
		return fmt.Errorf("resource, name, and condition are required")
	}

	timeoutDuration, err := time.ParseDuration(timeout)
	if err != nil {
		args := []string{
			"wait",
			fmt.Sprintf("--for=condition=%s", condition),
			fmt.Sprintf("%s/%s", resource, name),
			"--timeout",
			timeout,
		}
		args = append(args, withNamespace(namespace)...)
		return Kubectl(ctx, kubeconfig, args...)
	}

	deadline := time.Now().Add(timeoutDuration)
	for attempt := 1; ; attempt++ {
		waitArgs := []string{
			"wait",
			fmt.Sprintf("--for=condition=%s", condition),
			fmt.Sprintf("%s/%s", resource, name),
			"--timeout",
			"10s",
		}
		waitArgs = append(waitArgs, withNamespace(namespace)...)
		if err := Kubectl(ctx, kubeconfig, waitArgs...); err == nil {
			return nil
		}

		if err := checkForPodFailures(ctx, kubeconfig, namespace); err != nil {
			return err
		}
		logResourceStatus(ctx, kubeconfig, namespace, resource, name)
		if time.Now().After(deadline) {
			return fmt.Errorf("timeout waiting for condition %q on %s/%s in namespace %q", condition, resource, name, namespace)
		}
		time.Sleep(10 * time.Second)
	}
}

// KubectlWaitForDelete waits for a resource to be deleted.
func KubectlWaitForDelete(ctx context.Context, kubeconfig, namespace, resource, name, timeout string) error {
	fmt.Printf("Waiting for resource %q/%q to be deleted in namespace %q with timeout %q...\n", resource, name, namespace, timeout)
	if resource == "" || name == "" {
		return fmt.Errorf("resource and name are required")
	}

	args := []string{
		"wait",
		"--for=delete",
		fmt.Sprintf("%s/%s", resource, name),
		"--timeout",
		timeout,
	}
	if namespace != "" {
		args = append(args, "--namespace", namespace)
	}

	return Kubectl(ctx, kubeconfig, args...)
}

// KubectlRolloutStatus waits for a rollout to finish.
func KubectlRolloutStatus(ctx context.Context, kubeconfig, namespace, resource, timeout string) error {
	fmt.Printf("Waiting for rollout status of resource %q in namespace %q with timeout %q...\n", resource, namespace, timeout)
	if resource == "" {
		return fmt.Errorf("resource is required")
	}

	timeoutDuration, err := time.ParseDuration(timeout)
	if err != nil {
		args := []string{"rollout", "status", resource, "--timeout", timeout}
		args = append(args, withNamespace(namespace)...)
		return Kubectl(ctx, kubeconfig, args...)
	}

	deadline := time.Now().Add(timeoutDuration)
	for attempt := 1; ; attempt++ {
		args := []string{"rollout", "status", resource, "--timeout", "10s"}
		args = append(args, withNamespace(namespace)...)
		if err := Kubectl(ctx, kubeconfig, args...); err == nil {
			return nil
		}

		if err := checkForPodFailures(ctx, kubeconfig, namespace); err != nil {
			return err
		}
		logRolloutStatus(ctx, kubeconfig, namespace, resource)
		if time.Now().After(deadline) {
			return fmt.Errorf("timeout waiting for rollout of %q in namespace %q", resource, namespace)
		}
		time.Sleep(10 * time.Second)
	}
}

// KubectlGetJSONPath fetches a jsonpath value for a resource.
func KubectlGetJSONPath(ctx context.Context, kubeconfig, namespace, resource, name, jsonPath string) (string, error) {
	fmt.Printf("Getting JSON path %q for resource %q/%q in namespace %q...\n", jsonPath, resource, name, namespace)
	if resource == "" || name == "" || jsonPath == "" {
		return "", fmt.Errorf("resource, name, and jsonPath are required")
	}

	args := []string{"get", resource, name, "-o", fmt.Sprintf("jsonpath=%s", jsonPath)}
	if namespace != "" {
		args = append(args, "--namespace", namespace)
	}

	output, err := KubectlOutput(ctx, kubeconfig, args...)
	return strings.TrimSpace(output), err
}

// KubectlPatch applies a merge patch to a resource.
func KubectlPatch(ctx context.Context, kubeconfig, namespace, resource, name, patch string) error {
	fmt.Printf("Patching resource %q/%q in namespace %q with patch %q...\n", resource, name, namespace, patch)
	if resource == "" || name == "" || patch == "" {
		return fmt.Errorf("resource, name, and patch are required")
	}

	args := []string{
		"patch",
		resource,
		name,
		"--type",
		"merge",
		"-p",
		patch,
	}
	if namespace != "" {
		args = append(args, "--namespace", namespace)
	}

	return Kubectl(ctx, kubeconfig, args...)
}

// KubectlDeleteManifest deletes resources from a manifest file.
func KubectlDeleteManifest(ctx context.Context, kubeconfig, manifestPath string) error {
	fmt.Printf("Deleting resources from manifest file %q...\n", manifestPath)
	if manifestPath == "" {
		return fmt.Errorf("manifest path is required")
	}
	return Kubectl(ctx, kubeconfig, "delete", "-f", manifestPath, "--ignore-not-found=true")
}

func withNamespace(namespace string) []string {
	if namespace == "" {
		return nil
	}
	return []string{"--namespace", namespace}
}

func logResourceStatus(ctx context.Context, kubeconfig, namespace, resource, name string) {
	args := []string{"get", resource, name, "-o", "wide"}
	args = append(args, withNamespace(namespace)...)
	if output, err := KubectlOutput(ctx, kubeconfig, args...); err == nil {
		fmt.Printf("Resource status: %s\n", strings.TrimSpace(output))
	}

	conditionsArgs := []string{"get", resource, name, "-o", "jsonpath={.status.conditions}"}
	conditionsArgs = append(conditionsArgs, withNamespace(namespace)...)
	if output, err := KubectlOutput(ctx, kubeconfig, conditionsArgs...); err == nil && strings.TrimSpace(output) != "" {
		fmt.Printf("Resource conditions: %s\n", strings.TrimSpace(output))
	}
}

func logRolloutStatus(ctx context.Context, kubeconfig, namespace, resource string) {
	args := []string{"rollout", "status", resource, "--timeout", "5s"}
	args = append(args, withNamespace(namespace)...)
	if output, err := KubectlOutput(ctx, kubeconfig, args...); err == nil {
		fmt.Printf("Rollout status: %s\n", strings.TrimSpace(output))
	}
}

type podList struct {
	Items []pod `json:"items"`
}

type pod struct {
	Metadata podMetadata `json:"metadata"`
	Status   podStatus   `json:"status"`
}

type podMetadata struct {
	Name string `json:"name"`
}

type podStatus struct {
	Phase                 string            `json:"phase"`
	Reason                string            `json:"reason"`
	Message               string            `json:"message"`
	ContainerStatuses     []containerStatus `json:"containerStatuses"`
	InitContainerStatuses []containerStatus `json:"initContainerStatuses"`
}

type containerStatus struct {
	Name      string         `json:"name"`
	State     containerState `json:"state"`
	LastState containerState `json:"lastState"`
}

type containerState struct {
	Waiting    *containerStateWaiting    `json:"waiting"`
	Terminated *containerStateTerminated `json:"terminated"`
}

type containerStateWaiting struct {
	Reason  string `json:"reason"`
	Message string `json:"message"`
}

type containerStateTerminated struct {
	Reason   string `json:"reason"`
	Message  string `json:"message"`
	ExitCode int32  `json:"exitCode"`
}

var podFailureReasons = map[string]struct{}{
	"CrashLoopBackOff":           {},
	"ImagePullBackOff":           {},
	"ErrImagePull":               {},
	"CreateContainerConfigError": {},
	"CreateContainerError":       {},
	"RunContainerError":          {},
	"ContainerCannotRun":         {},
	"InvalidImageName":           {},
	"ErrImageNeverPull":          {},
	"BackOff":                    {},
}

func checkForPodFailures(ctx context.Context, kubeconfig, namespace string) error {
	if namespace == "" {
		return nil
	}
	output, err := KubectlOutput(ctx, kubeconfig, "get", "pods", "--namespace", namespace, "-o", "json")
	if err != nil {
		return nil
	}
	var list podList
	if err := json.Unmarshal([]byte(output), &list); err != nil {
		return nil
	}
	for _, item := range list.Items {
		if err := detectPodFailure(item); err != nil {
			return err
		}
	}
	return nil
}

func detectPodFailure(item pod) error {
	if item.Status.Phase == "Failed" {
		return fmt.Errorf("pod %q failed: %s %s", item.Metadata.Name, item.Status.Reason, item.Status.Message)
	}
	if err := detectContainerFailures(item.Metadata.Name, item.Status.InitContainerStatuses); err != nil {
		return err
	}
	if err := detectContainerFailures(item.Metadata.Name, item.Status.ContainerStatuses); err != nil {
		return err
	}
	return nil
}

func detectContainerFailures(podName string, statuses []containerStatus) error {
	for _, status := range statuses {
		if status.State.Waiting != nil {
			if _, ok := podFailureReasons[status.State.Waiting.Reason]; ok {
				return fmt.Errorf("pod %q container %q waiting: %s %s", podName, status.Name, status.State.Waiting.Reason, status.State.Waiting.Message)
			}
		}
		if status.State.Terminated != nil {
			if status.State.Terminated.ExitCode != 0 && status.State.Terminated.Reason != "Completed" {
				return fmt.Errorf("pod %q container %q terminated: %s %s (exit %d)", podName, status.Name, status.State.Terminated.Reason, status.State.Terminated.Message, status.State.Terminated.ExitCode)
			}
		}
		if status.LastState.Terminated != nil {
			if status.LastState.Terminated.ExitCode != 0 && status.LastState.Terminated.Reason != "Completed" {
				return fmt.Errorf("pod %q container %q last terminated: %s %s (exit %d)", podName, status.Name, status.LastState.Terminated.Reason, status.LastState.Terminated.Message, status.LastState.Terminated.ExitCode)
			}
		}
	}
	return nil
}

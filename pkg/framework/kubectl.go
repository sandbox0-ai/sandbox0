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

// KubectlExecOutput runs kubectl exec and returns output.
func KubectlExecOutput(ctx context.Context, kubeconfig, namespace, pod string, command ...string) (string, error) {
	if pod == "" {
		return "", fmt.Errorf("pod name is required")
	}
	if len(command) == 0 {
		return "", fmt.Errorf("command is required")
	}

	args := []string{"exec"}
	args = append(args, withNamespace(namespace)...)
	args = append(args, pod, "--")
	args = append(args, command...)
	return KubectlOutput(ctx, kubeconfig, args...)
}

// KubectlExecContainerOutput runs kubectl exec against a specific container and returns output.
func KubectlExecContainerOutput(ctx context.Context, kubeconfig, namespace, pod, container string, command ...string) (string, error) {
	if pod == "" {
		return "", fmt.Errorf("pod name is required")
	}
	if container == "" {
		return "", fmt.Errorf("container name is required")
	}
	if len(command) == 0 {
		return "", fmt.Errorf("command is required")
	}

	args := []string{"exec"}
	args = append(args, withNamespace(namespace)...)
	args = append(args, pod, "-c", container, "--")
	args = append(args, command...)
	return KubectlOutput(ctx, kubeconfig, args...)
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
		time.Sleep(5 * time.Second)
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
	return Kubectl(ctx, kubeconfig, "delete", "-f", manifestPath, "--ignore-not-found=true", "--wait=false")
}

// KubectlDeleteNamespacesByLabel requests deletion for namespaces matching a label selector.
func KubectlDeleteNamespacesByLabel(ctx context.Context, kubeconfig, labelSelector string) error {
	if labelSelector == "" {
		return fmt.Errorf("label selector is required")
	}
	return Kubectl(ctx, kubeconfig, "delete", "namespaces", "-l", labelSelector, "--ignore-not-found=true", "--wait=false")
}

// KubectlDeleteNamespace requests deletion for a namespace without waiting in kubectl.
func KubectlDeleteNamespace(ctx context.Context, kubeconfig, namespace string) error {
	if namespace == "" {
		return fmt.Errorf("namespace is required")
	}
	return Kubectl(ctx, kubeconfig, "delete", "namespace", namespace, "--ignore-not-found=true", "--wait=false")
}

const (
	managerTemplateReconcilerQuiesceSupportedMarkerPath = "/tmp/sandbox0-manager-template-reconciler-quiesce-supported"
	managerTemplateReconcilerQuiescedMarkerPath         = "/tmp/sandbox0-manager-template-reconciler-quiesced"
)

// KubectlQuiesceManagerTemplateReconcilers stops background template syncing in
// every running manager Pod while leaving manager-hosted storage available.
func KubectlQuiesceManagerTemplateReconcilers(ctx context.Context, kubeconfig, namespace, timeout string) error {
	if ctx == nil {
		return fmt.Errorf("context is required")
	}
	if namespace == "" {
		return fmt.Errorf("namespace is required")
	}
	timeoutDuration, err := time.ParseDuration(timeout)
	if err != nil {
		return fmt.Errorf("parse timeout %q: %w", timeout, err)
	}
	namespaceResource, err := KubectlOutput(
		ctx,
		kubeconfig,
		"get",
		"namespace",
		namespace,
		"--ignore-not-found=true",
		"-o",
		"name",
	)
	if err != nil {
		return err
	}
	if strings.TrimSpace(namespaceResource) == "" {
		return nil
	}

	deadline := time.Now().Add(timeoutDuration)
	quiescedPods := make(map[string]struct{})
	for {
		runningPods, ready, err := kubectlReadyManagerPods(ctx, kubeconfig, namespace)
		if err != nil {
			return err
		}
		if !ready {
			if err := waitForManagerQuiesceRetry(ctx, deadline, namespace); err != nil {
				return err
			}
			continue
		}

		newPods := make([]managerPodRef, 0, len(runningPods))
		for _, pod := range runningPods {
			if _, ok := quiescedPods[pod.Identity]; ok {
				continue
			}
			if _, err := KubectlExecContainerOutput(
				ctx,
				kubeconfig,
				namespace,
				pod.Name,
				"manager",
				"/bin/sh",
				"-c",
				"test -f "+managerTemplateReconcilerQuiesceSupportedMarkerPath,
			); err != nil {
				return fmt.Errorf("manager Pod %s/%s does not advertise template reconciliation quiesce support", namespace, pod.Name)
			}
			newPods = append(newPods, pod)
		}
		for _, pod := range newPods {
			if _, err := KubectlExecContainerOutput(
				ctx,
				kubeconfig,
				namespace,
				pod.Name,
				"manager",
				"/bin/sh",
				"-c",
				"kill -USR1 1",
			); err != nil {
				return fmt.Errorf("quiesce manager Pod %s/%s: %w", namespace, pod.Name, err)
			}
		}
		for _, pod := range newPods {
			if err := kubectlWaitForManagerQuiesceAck(ctx, kubeconfig, namespace, pod.Name, deadline); err != nil {
				return err
			}
			quiescedPods[pod.Identity] = struct{}{}
		}

		confirmedPods, ready, err := kubectlReadyManagerPods(ctx, kubeconfig, namespace)
		if err != nil {
			return err
		}
		if ready && sameManagerPodSet(runningPods, confirmedPods) {
			return nil
		}
		if err := waitForManagerQuiesceRetry(ctx, deadline, namespace); err != nil {
			return err
		}
	}
}

type managerPodRef struct {
	Name     string
	Identity string
}

func kubectlReadyManagerPods(ctx context.Context, kubeconfig, namespace string) ([]managerPodRef, bool, error) {
	output, err := KubectlOutput(
		ctx,
		kubeconfig,
		"get",
		"pods",
		"--namespace",
		namespace,
		"-l",
		"app.kubernetes.io/component=manager",
		"-o",
		"json",
	)
	if err != nil {
		return nil, false, err
	}
	var pods podList
	if err := json.Unmarshal([]byte(output), &pods); err != nil {
		return nil, false, fmt.Errorf("decode manager Pods: %w", err)
	}
	deploymentsOutput, err := KubectlOutput(
		ctx,
		kubeconfig,
		"get",
		"deployments",
		"--namespace",
		namespace,
		"-l",
		"app.kubernetes.io/component=manager",
		"-o",
		"json",
	)
	if err != nil {
		return nil, false, err
	}
	var deployments deploymentList
	if err := json.Unmarshal([]byte(deploymentsOutput), &deployments); err != nil {
		return nil, false, fmt.Errorf("decode manager Deployments: %w", err)
	}
	desiredReplicas := int32(0)
	for _, deployment := range deployments.Items {
		if deployment.Spec.Replicas == nil {
			desiredReplicas++
		} else {
			desiredReplicas += *deployment.Spec.Replicas
		}
	}

	runningPods := make([]managerPodRef, 0, len(pods.Items))
	unreadyPods := make([]string, 0)
	for _, pod := range pods.Items {
		managerStatus, found := containerStatusByName(pod.Status.ContainerStatuses, "manager")
		if pod.Status.Phase != "Running" || pod.Metadata.DeletionTimestamp != "" || !found || !managerStatus.Ready {
			unreadyPods = append(unreadyPods, pod.Metadata.Name+"("+pod.Status.Phase+")")
			continue
		}
		runningPods = append(runningPods, managerPodRef{
			Name: pod.Metadata.Name,
			Identity: fmt.Sprintf(
				"%s/%s/%s/%d",
				pod.Metadata.UID,
				pod.Metadata.Name,
				managerStatus.ContainerID,
				managerStatus.RestartCount,
			),
		})
	}
	if len(unreadyPods) > 0 || int32(len(runningPods)) < desiredReplicas {
		fmt.Printf(
			"Waiting for manager Pods to stabilize before quiescing: running=%d desired=%d unready=%s\n",
			len(runningPods),
			desiredReplicas,
			strings.Join(unreadyPods, ", "),
		)
		return runningPods, false, nil
	}
	return runningPods, true, nil
}

func kubectlWaitForManagerQuiesceAck(ctx context.Context, kubeconfig, namespace, pod string, deadline time.Time) error {
	var lastErr error
	for {
		_, err := KubectlExecContainerOutput(
			ctx,
			kubeconfig,
			namespace,
			pod,
			"manager",
			"/bin/sh",
			"-c",
			"test -f "+managerTemplateReconcilerQuiescedMarkerPath,
		)
		if err == nil {
			return nil
		}
		lastErr = err
		if time.Now().After(deadline) {
			if lastErr != nil {
				return fmt.Errorf("wait for manager Pod %s/%s to quiesce: %w", namespace, pod, lastErr)
			}
			return fmt.Errorf("timeout waiting for manager Pod %s/%s to quiesce", namespace, pod)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}
}

func waitForManagerQuiesceRetry(ctx context.Context, deadline time.Time, namespace string) error {
	if time.Now().After(deadline) {
		return fmt.Errorf("timeout waiting for manager Pods in namespace %s to stabilize", namespace)
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(500 * time.Millisecond):
		return nil
	}
}

func sameManagerPodSet(left, right []managerPodRef) bool {
	if len(left) != len(right) {
		return false
	}
	values := make(map[string]struct{}, len(left))
	for _, value := range left {
		values[value.Identity] = struct{}{}
	}
	for _, value := range right {
		if _, ok := values[value.Identity]; !ok {
			return false
		}
	}
	return true
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
	Name              string `json:"name"`
	UID               string `json:"uid"`
	DeletionTimestamp string `json:"deletionTimestamp"`
}

type podStatus struct {
	Phase                 string            `json:"phase"`
	Reason                string            `json:"reason"`
	Message               string            `json:"message"`
	ContainerStatuses     []containerStatus `json:"containerStatuses"`
	InitContainerStatuses []containerStatus `json:"initContainerStatuses"`
}

type deploymentList struct {
	Items []deployment `json:"items"`
}

type deployment struct {
	Spec deploymentSpec `json:"spec"`
}

type deploymentSpec struct {
	Replicas *int32 `json:"replicas"`
}

type namespaceList struct {
	Items []namespace `json:"items"`
}

type namespace struct {
	Metadata namespaceMetadata `json:"metadata"`
	Status   namespaceStatus   `json:"status"`
}

type namespaceMetadata struct {
	Name string `json:"name"`
}

type namespaceStatus struct {
	Phase string `json:"phase"`
}

// KubectlWaitForNamespacesDeletedByLabel waits for all namespaces matching a label selector to disappear.
func KubectlWaitForNamespacesDeletedByLabel(ctx context.Context, kubeconfig, labelSelector, timeout string) error {
	if labelSelector == "" {
		return fmt.Errorf("label selector is required")
	}

	timeoutDuration, err := time.ParseDuration(timeout)
	if err != nil {
		return fmt.Errorf("parse timeout %q: %w", timeout, err)
	}

	deadline := time.Now().Add(timeoutDuration)
	for {
		output, err := KubectlOutput(ctx, kubeconfig, "get", "namespaces", "-l", labelSelector, "-o", "json")
		if err != nil {
			return err
		}

		var namespaces namespaceList
		if err := json.Unmarshal([]byte(output), &namespaces); err != nil {
			return fmt.Errorf("decode namespaces for selector %q: %w", labelSelector, err)
		}
		if len(namespaces.Items) == 0 {
			return nil
		}

		names := namespacePhaseDescriptions(namespaces.Items)
		fmt.Printf("Namespaces still present for selector %q: %s\n", labelSelector, strings.Join(names, ", "))

		if time.Now().After(deadline) {
			return fmt.Errorf("timeout waiting for namespaces with selector %q to be deleted: %s", labelSelector, strings.Join(names, ", "))
		}
		time.Sleep(2 * time.Second)
	}
}

// KubectlWaitForNamespaceDeleted waits for a namespace to disappear.
func KubectlWaitForNamespaceDeleted(ctx context.Context, kubeconfig, namespaceName, timeout string) error {
	if namespaceName == "" {
		return fmt.Errorf("namespace is required")
	}

	timeoutDuration, err := time.ParseDuration(timeout)
	if err != nil {
		return fmt.Errorf("parse timeout %q: %w", timeout, err)
	}

	deadline := time.Now().Add(timeoutDuration)
	for {
		output, err := KubectlOutput(ctx, kubeconfig, "get", "namespace", namespaceName, "--ignore-not-found=true", "-o", "json")
		if err != nil {
			return err
		}
		if strings.TrimSpace(output) == "" {
			return nil
		}

		var item namespace
		if err := json.Unmarshal([]byte(output), &item); err != nil {
			return fmt.Errorf("decode namespace %q: %w", namespaceName, err)
		}

		description := strings.Join(namespacePhaseDescriptions([]namespace{item}), ", ")
		fmt.Printf("Namespace still present: %s\n", description)
		if time.Now().After(deadline) {
			return fmt.Errorf("timeout waiting for namespace %q to be deleted: %s", namespaceName, description)
		}
		time.Sleep(2 * time.Second)
	}
}

func namespacePhaseDescriptions(items []namespace) []string {
	names := make([]string, 0, len(items))
	for _, item := range items {
		phase := strings.TrimSpace(item.Status.Phase)
		if phase == "" {
			phase = "Unknown"
		}
		names = append(names, fmt.Sprintf("%s(%s)", item.Metadata.Name, phase))
	}
	return names
}

type containerStatus struct {
	Name         string         `json:"name"`
	Ready        bool           `json:"ready"`
	RestartCount int32          `json:"restartCount"`
	ContainerID  string         `json:"containerID"`
	State        containerState `json:"state"`
	LastState    containerState `json:"lastState"`
}

func containerStatusByName(statuses []containerStatus, name string) (containerStatus, bool) {
	for _, status := range statuses {
		if status.Name == name {
			return status, true
		}
	}
	return containerStatus{}, false
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

type podFailure struct {
	PodName       string
	ContainerName string
	State         string
	Reason        string
	Message       string
}

func (p *podFailure) Error() string {
	if p == nil {
		return ""
	}
	if p.ContainerName == "" {
		return fmt.Sprintf("pod %q failed: %s %s", p.PodName, p.Reason, p.Message)
	}
	return fmt.Sprintf("pod %q container %q %s: %s %s", p.PodName, p.ContainerName, p.State, p.Reason, p.Message)
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
		if failure := detectPodFailure(item); failure != nil {
			logPodFailure(ctx, kubeconfig, namespace, failure)
			return failure
		}
	}
	return nil
}

func detectPodFailure(item pod) *podFailure {
	if item.Status.Phase == "Failed" {
		return &podFailure{
			PodName: item.Metadata.Name,
			Reason:  item.Status.Reason,
			Message: item.Status.Message,
		}
	}
	if failure := detectContainerFailures(item.Metadata.Name, item.Status.InitContainerStatuses); failure != nil {
		return failure
	}
	if failure := detectContainerFailures(item.Metadata.Name, item.Status.ContainerStatuses); failure != nil {
		return failure
	}
	return nil
}

func detectContainerFailures(podName string, statuses []containerStatus) *podFailure {
	for _, status := range statuses {
		if status.State.Waiting != nil {
			if _, ok := podFailureReasons[status.State.Waiting.Reason]; ok {
				return &podFailure{
					PodName:       podName,
					ContainerName: status.Name,
					State:         "waiting",
					Reason:        status.State.Waiting.Reason,
					Message:       status.State.Waiting.Message,
				}
			}
		}
		if status.State.Terminated != nil {
			if status.State.Terminated.ExitCode != 0 && status.State.Terminated.Reason != "Completed" {
				return &podFailure{
					PodName:       podName,
					ContainerName: status.Name,
					State:         "terminated",
					Reason:        status.State.Terminated.Reason,
					Message: fmt.Sprintf(
						"%s (exit %d)",
						status.State.Terminated.Message,
						status.State.Terminated.ExitCode,
					),
				}
			}
		}
		if status.LastState.Terminated != nil {
			if status.LastState.Terminated.ExitCode != 0 && status.LastState.Terminated.Reason != "Completed" {
				return &podFailure{
					PodName:       podName,
					ContainerName: status.Name,
					State:         "last terminated",
					Reason:        status.LastState.Terminated.Reason,
					Message: fmt.Sprintf(
						"%s (exit %d)",
						status.LastState.Terminated.Message,
						status.LastState.Terminated.ExitCode,
					),
				}
			}
		}
	}
	return nil
}

func logPodFailure(ctx context.Context, kubeconfig, namespace string, failure *podFailure) {
	if failure == nil || namespace == "" || failure.ContainerName == "" {
		return
	}
	logContainerLogs(ctx, kubeconfig, namespace, failure.PodName, failure.ContainerName, false)
	logContainerLogs(ctx, kubeconfig, namespace, failure.PodName, failure.ContainerName, true)
}

func logContainerLogs(ctx context.Context, kubeconfig, namespace, podName, containerName string, previous bool) {
	args := []string{"logs", podName, "--namespace", namespace, "-c", containerName}
	if previous {
		args = append(args, "--previous")
	}
	fmt.Printf("Fetching logs for pod %q container %q (previous=%t)...\n", podName, containerName, previous)
	output, err := KubectlOutput(ctx, kubeconfig, args...)
	if err != nil {
		fmt.Printf("Failed to fetch logs for pod %q container %q (previous=%t): %v\n", podName, containerName, previous, err)
		return
	}
	if strings.TrimSpace(output) == "" {
		fmt.Printf("No logs for pod %q container %q (previous=%t).\n", podName, containerName, previous)
		return
	}
	fmt.Printf("Logs for pod %q container %q (previous=%t):\n%s\n", podName, containerName, previous, output)
}

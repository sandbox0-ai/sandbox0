package rootfs

import (
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/sandbox0-ai/sandbox0/pkg/carrier"
	"github.com/sandbox0-ai/sandbox0/pkg/ctldapi"
	"github.com/sandbox0-ai/sandbox0/pkg/runtimecontrol"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ReleaseCarrierGate commits the node-local init-to-main transition after all
// security and rootfs assignments are ready. Repeated releases are idempotent.
func (c *Controller) ReleaseCarrierGate(r *http.Request, req ctldapi.ReleaseCarrierGateRequest) (ctldapi.ReleaseCarrierGateResponse, int) {
	if c == nil || c.k8sClient == nil || strings.TrimSpace(c.nodeName) == "" || strings.TrimSpace(c.kubeletPodsRoot) == "" {
		return ctldapi.ReleaseCarrierGateResponse{Error: "carrier gate runtime is not configured"}, http.StatusNotImplemented
	}
	req.Namespace = strings.TrimSpace(req.Namespace)
	req.PodName = strings.TrimSpace(req.PodName)
	req.PodUID = strings.TrimSpace(req.PodUID)
	req.Slot = strings.TrimSpace(req.Slot)
	req.SandboxID = strings.TrimSpace(req.SandboxID)
	req.ContainerName = strings.TrimSpace(req.ContainerName)
	if req.Namespace == "" || req.PodName == "" || req.PodUID == "" || req.SandboxID == "" || req.RuntimeGeneration <= 0 || req.ContainerName == "" {
		return ctldapi.ReleaseCarrierGateResponse{Error: "namespace, pod_name, pod_uid, sandbox_id, positive runtime_generation, and container_name are required"}, http.StatusBadRequest
	}
	if err := carrier.ValidateSlot(req.Slot); err != nil {
		return ctldapi.ReleaseCarrierGateResponse{Error: err.Error()}, http.StatusBadRequest
	}
	pod, err := c.k8sClient.CoreV1().Pods(req.Namespace).Get(requestContext(r), req.PodName, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return ctldapi.ReleaseCarrierGateResponse{Error: "carrier Pod not found"}, http.StatusNotFound
	}
	if err != nil {
		return ctldapi.ReleaseCarrierGateResponse{Error: err.Error()}, http.StatusServiceUnavailable
	}
	generation, generationErr := strconv.ParseInt(strings.TrimSpace(pod.Annotations[runtimecontrol.AnnotationRuntimeGeneration]), 10, 64)
	containerFound := false
	for i := range pod.Spec.Containers {
		if pod.Spec.Containers[i].Name == req.ContainerName {
			containerFound = true
			break
		}
	}
	if string(pod.UID) != req.PodUID || pod.Spec.NodeName != c.nodeName || pod.Annotations[carrier.AnnotationSlot] != req.Slot ||
		strings.TrimSpace(pod.Annotations[runtimecontrol.AnnotationSandboxID]) != req.SandboxID || generationErr != nil || generation != req.RuntimeGeneration || !containerFound {
		return ctldapi.ReleaseCarrierGateResponse{Error: "carrier Pod identity, node, slot, sandbox, generation, or container conflict"}, http.StatusConflict
	}
	podIP := strings.TrimSpace(pod.Status.PodIP)
	if podIP == "" {
		return ctldapi.ReleaseCarrierGateResponse{Error: "carrier Pod has no IP"}, http.StatusConflict
	}
	if !hasCarrierGateVolume(pod) {
		return ctldapi.ReleaseCarrierGateResponse{Error: "carrier Pod has no gate volume"}, http.StatusConflict
	}
	releasePath := filepath.Join(c.kubeletPodsRoot, req.PodUID, "volumes", "kubernetes.io~empty-dir", carrier.GateVolumeName, carrier.GateReleaseFile)
	root := filepath.Clean(c.kubeletPodsRoot) + string(os.PathSeparator)
	if !strings.HasPrefix(filepath.Clean(releasePath)+string(os.PathSeparator), root) {
		return ctldapi.ReleaseCarrierGateResponse{Error: "carrier gate path escapes kubelet root"}, http.StatusBadRequest
	}
	if exists, err := validateExistingCarrierRelease(releasePath, req.Slot); err != nil {
		return ctldapi.ReleaseCarrierGateResponse{Error: err.Error()}, http.StatusConflict
	} else if exists {
		return carrierGateReleasedResponse(req, podIP), http.StatusOK
	}
	file, err := os.CreateTemp(filepath.Dir(releasePath), "."+carrier.GateReleaseFile+"."+req.Slot+".")
	if err != nil {
		return ctldapi.ReleaseCarrierGateResponse{Error: fmt.Sprintf("create carrier gate release: %v", err)}, http.StatusInternalServerError
	}
	tempPath := file.Name()
	removeTemp := true
	defer func() {
		if removeTemp {
			_ = os.Remove(tempPath)
		}
	}()
	if _, err := file.WriteString(req.Slot + "\n"); err != nil {
		_ = file.Close()
		return ctldapi.ReleaseCarrierGateResponse{Error: fmt.Sprintf("write carrier gate: %v", err)}, http.StatusInternalServerError
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return ctldapi.ReleaseCarrierGateResponse{Error: fmt.Sprintf("sync carrier gate: %v", err)}, http.StatusInternalServerError
	}
	if err := file.Close(); err != nil {
		return ctldapi.ReleaseCarrierGateResponse{Error: fmt.Sprintf("close carrier gate: %v", err)}, http.StatusInternalServerError
	}
	if err := os.Link(tempPath, releasePath); err != nil {
		if errors.Is(err, os.ErrExist) {
			if exists, validationErr := validateExistingCarrierRelease(releasePath, req.Slot); validationErr != nil {
				return ctldapi.ReleaseCarrierGateResponse{Error: validationErr.Error()}, http.StatusConflict
			} else if exists {
				return carrierGateReleasedResponse(req, podIP), http.StatusOK
			}
		}
		return ctldapi.ReleaseCarrierGateResponse{Error: fmt.Sprintf("publish carrier gate release: %v", err)}, http.StatusInternalServerError
	}
	if err := os.Remove(tempPath); err != nil {
		return ctldapi.ReleaseCarrierGateResponse{Error: fmt.Sprintf("remove carrier gate temporary file: %v", err)}, http.StatusInternalServerError
	}
	removeTemp = false
	if err := syncCarrierGateDirectory(filepath.Dir(releasePath)); err != nil {
		return ctldapi.ReleaseCarrierGateResponse{Error: fmt.Sprintf("sync carrier gate directory: %v", err)}, http.StatusInternalServerError
	}
	return carrierGateReleasedResponse(req, podIP), http.StatusOK
}

func carrierGateReleasedResponse(req ctldapi.ReleaseCarrierGateRequest, podIP string) ctldapi.ReleaseCarrierGateResponse {
	return ctldapi.ReleaseCarrierGateResponse{
		Released: true, Namespace: req.Namespace, PodName: req.PodName, PodUID: req.PodUID, PodIP: podIP,
		Slot: req.Slot, SandboxID: req.SandboxID, RuntimeGeneration: req.RuntimeGeneration, ContainerName: req.ContainerName,
	}
}

func validateExistingCarrierRelease(path, slot string) (bool, error) {
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("inspect carrier gate release: %w", err)
	}
	if !info.Mode().IsRegular() {
		return true, fmt.Errorf("carrier gate release is not a regular file")
	}
	payload, err := os.ReadFile(path)
	if err != nil {
		return true, fmt.Errorf("read carrier gate release: %w", err)
	}
	if string(payload) != slot+"\n" {
		return true, fmt.Errorf("carrier gate release belongs to a different slot")
	}
	return true, nil
}

func syncCarrierGateDirectory(path string) error {
	directory, err := os.Open(path)
	if err != nil {
		return err
	}
	return errors.Join(directory.Sync(), directory.Close())
}

func hasCarrierGateVolume(pod *corev1.Pod) bool {
	if pod == nil {
		return false
	}
	for _, volume := range pod.Spec.Volumes {
		if volume.Name == carrier.GateVolumeName && volume.EmptyDir != nil {
			return true
		}
	}
	return false
}

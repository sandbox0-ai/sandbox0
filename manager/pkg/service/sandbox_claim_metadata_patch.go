package service

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/controller"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

type claimMetadataPatchOperation struct {
	Operation string `json:"op"`
	Path      string `json:"path"`
	Value     any    `json:"value,omitempty"`
}

// patchClaimedPodMetadata atomically moves one idle pod to claimed metadata
// without replacing spec or status fields owned by other Kubernetes actors.
func (s *SandboxService) patchClaimedPodMetadata(
	ctx context.Context,
	originalPod *corev1.Pod,
	claimedPod *corev1.Pod,
) (*corev1.Pod, error) {
	patch, err := buildClaimMetadataPatch(originalPod, claimedPod)
	if err != nil {
		return nil, err
	}
	return s.k8sClient.CoreV1().Pods(originalPod.Namespace).Patch(
		ctx,
		originalPod.Name,
		types.JSONPatchType,
		patch,
		metav1.PatchOptions{},
	)
}

// buildClaimMetadataPatch creates the compare-and-swap JSON patch used by hot
// claims. UID, resource version, and pool state prevent stale managers from
// claiming the same pod.
func buildClaimMetadataPatch(originalPod, claimedPod *corev1.Pod) ([]byte, error) {
	if originalPod == nil || claimedPod == nil {
		return nil, fmt.Errorf("build claim metadata patch: pod is nil")
	}
	if originalPod.Namespace != claimedPod.Namespace || originalPod.Name != claimedPod.Name {
		return nil, fmt.Errorf(
			"build claim metadata patch: pod identity changed from %s/%s to %s/%s",
			originalPod.Namespace,
			originalPod.Name,
			claimedPod.Namespace,
			claimedPod.Name,
		)
	}

	operations := make([]claimMetadataPatchOperation, 0, 16)
	if originalPod.UID != "" {
		operations = append(operations, claimMetadataPatchOperation{
			Operation: "test",
			Path:      "/metadata/uid",
			Value:     originalPod.UID,
		})
	}
	if originalPod.ResourceVersion != "" {
		operations = append(operations, claimMetadataPatchOperation{
			Operation: "test",
			Path:      "/metadata/resourceVersion",
			Value:     originalPod.ResourceVersion,
		})
	}
	operations = append(operations, claimMetadataPatchOperation{
		Operation: "test",
		Path:      metadataMapPath("labels", controller.LabelPoolType),
		Value:     controller.PoolTypeIdle,
	})

	operations = appendMetadataMapPatch(operations, "labels", originalPod.Labels, claimedPod.Labels)
	operations = appendMetadataMapPatch(operations, "annotations", originalPod.Annotations, claimedPod.Annotations)
	operations = appendFinalizerPatch(operations, originalPod.Finalizers, claimedPod.Finalizers)
	operations = appendReplicaSetOwnerRemovalPatch(operations, originalPod.OwnerReferences)

	patch, err := json.Marshal(operations)
	if err != nil {
		return nil, fmt.Errorf("marshal claim metadata patch: %w", err)
	}
	return patch, nil
}

func appendMetadataMapPatch(
	operations []claimMetadataPatchOperation,
	field string,
	original map[string]string,
	desired map[string]string,
) []claimMetadataPatchOperation {
	if original == nil && len(desired) > 0 {
		return append(operations, claimMetadataPatchOperation{
			Operation: "add",
			Path:      "/metadata/" + field,
			Value:     desired,
		})
	}

	removedKeys := make([]string, 0)
	for key := range original {
		if _, exists := desired[key]; !exists {
			removedKeys = append(removedKeys, key)
		}
	}
	sort.Strings(removedKeys)
	for _, key := range removedKeys {
		operations = append(operations, claimMetadataPatchOperation{
			Operation: "remove",
			Path:      metadataMapPath(field, key),
		})
	}

	changedKeys := make([]string, 0)
	for key, desiredValue := range desired {
		if originalValue, exists := original[key]; !exists || originalValue != desiredValue {
			changedKeys = append(changedKeys, key)
		}
	}
	sort.Strings(changedKeys)
	for _, key := range changedKeys {
		operation := "add"
		if _, exists := original[key]; exists {
			operation = "replace"
		}
		operations = append(operations, claimMetadataPatchOperation{
			Operation: operation,
			Path:      metadataMapPath(field, key),
			Value:     desired[key],
		})
	}
	return operations
}

func appendFinalizerPatch(
	operations []claimMetadataPatchOperation,
	original []string,
	desired []string,
) []claimMetadataPatchOperation {
	existing := make(map[string]struct{}, len(original))
	for _, finalizer := range original {
		existing[finalizer] = struct{}{}
	}
	for _, finalizer := range desired {
		if _, exists := existing[finalizer]; exists {
			continue
		}
		if len(original) == 0 {
			operations = append(operations, claimMetadataPatchOperation{
				Operation: "add",
				Path:      "/metadata/finalizers",
				Value:     []string{finalizer},
			})
			original = append(original, finalizer)
		} else {
			operations = append(operations, claimMetadataPatchOperation{
				Operation: "add",
				Path:      "/metadata/finalizers/-",
				Value:     finalizer,
			})
		}
		existing[finalizer] = struct{}{}
	}
	return operations
}

func appendReplicaSetOwnerRemovalPatch(
	operations []claimMetadataPatchOperation,
	ownerReferences []metav1.OwnerReference,
) []claimMetadataPatchOperation {
	for index := len(ownerReferences) - 1; index >= 0; index-- {
		owner := ownerReferences[index]
		if owner.Kind != "ReplicaSet" || owner.Controller == nil || !*owner.Controller {
			continue
		}
		path := "/metadata/ownerReferences/" + strconv.Itoa(index)
		if owner.UID != "" {
			operations = append(operations, claimMetadataPatchOperation{
				Operation: "test",
				Path:      path + "/uid",
				Value:     owner.UID,
			})
		}
		operations = append(operations, claimMetadataPatchOperation{
			Operation: "remove",
			Path:      path,
		})
	}
	return operations
}

func removeReplicaSetControllerOwnerReferences(ownerReferences []metav1.OwnerReference) []metav1.OwnerReference {
	filtered := make([]metav1.OwnerReference, 0, len(ownerReferences))
	for _, owner := range ownerReferences {
		if owner.Kind == "ReplicaSet" && owner.Controller != nil && *owner.Controller {
			continue
		}
		filtered = append(filtered, owner)
	}
	return filtered
}

func metadataMapPath(field, key string) string {
	return "/metadata/" + field + "/" + escapeJSONPointerToken(key)
}

func escapeJSONPointerToken(value string) string {
	value = strings.ReplaceAll(value, "~", "~0")
	return strings.ReplaceAll(value, "/", "~1")
}

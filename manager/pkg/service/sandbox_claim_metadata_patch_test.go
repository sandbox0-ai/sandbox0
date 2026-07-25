package service

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/controller"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

func TestPatchClaimedPodMetadataChangesOnlyManagerOwnedMetadata(t *testing.T) {
	controllerOwner := true
	original := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:       "sandbox-a",
			Name:            "idle-a",
			UID:             types.UID("pod-uid"),
			ResourceVersion: "42",
			Labels: map[string]string{
				controller.LabelPoolType:   controller.PoolTypeIdle,
				controller.LabelTemplateID: "default",
				"example.com/unrelated":    "keep",
			},
			Annotations: map[string]string{
				controller.AnnotationTemplateSpecHash: "template-hash",
				"example.com/unrelated":               "keep",
			},
			Finalizers: []string{"example.com/unrelated"},
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion: "apps/v1",
					Kind:       "ReplicaSet",
					Name:       "warm-pool",
					UID:        types.UID("replicaset-uid"),
					Controller: &controllerOwner,
				},
				{
					APIVersion: "example.com/v1",
					Kind:       "ExternalOwner",
					Name:       "external-owner",
					UID:        types.UID("external-owner-uid"),
				},
			},
		},
		Spec: corev1.PodSpec{
			NodeName: "node-a",
			Containers: []corev1.Container{{
				Name:  "procd",
				Image: "example.com/procd:test",
			}},
		},
		Status: corev1.PodStatus{Phase: corev1.PodRunning, PodIP: "10.0.0.1"},
	}
	claimed := original.DeepCopy()
	claimed.Labels[controller.LabelPoolType] = controller.PoolTypeActive
	claimed.Labels[controller.LabelSandboxID] = "sandbox-id"
	claimed.Annotations[controller.AnnotationSandboxID] = "sandbox-id"
	ensureSandboxCleanupFinalizer(claimed)
	claimed.OwnerReferences = removeReplicaSetControllerOwnerReferences(claimed.OwnerReferences)

	client := fake.NewSimpleClientset(original.DeepCopy())
	service := &SandboxService{k8sClient: client}
	patched, err := service.patchClaimedPodMetadata(context.Background(), original, claimed)
	if err != nil {
		t.Fatalf("patchClaimedPodMetadata() error = %v", err)
	}

	if got := patched.Labels["example.com/unrelated"]; got != "keep" {
		t.Fatalf("unrelated label = %q, want keep", got)
	}
	if got := patched.Annotations["example.com/unrelated"]; got != "keep" {
		t.Fatalf("unrelated annotation = %q, want keep", got)
	}
	if got := patched.Spec.NodeName; got != original.Spec.NodeName {
		t.Fatalf("node name = %q, want %q", got, original.Spec.NodeName)
	}
	if got := patched.Status.PodIP; got != original.Status.PodIP {
		t.Fatalf("pod IP = %q, want %q", got, original.Status.PodIP)
	}
	if len(patched.Finalizers) != 2 ||
		patched.Finalizers[0] != "example.com/unrelated" ||
		patched.Finalizers[1] != sandboxCleanupFinalizer {
		t.Fatalf("finalizers = %v, want unrelated and sandbox cleanup", patched.Finalizers)
	}
	if len(patched.OwnerReferences) != 1 || patched.OwnerReferences[0].UID != types.UID("external-owner-uid") {
		t.Fatalf("owner references = %#v, want only external owner", patched.OwnerReferences)
	}

	var patchAction k8stesting.PatchAction
	for _, action := range client.Actions() {
		if action.GetVerb() == "update" {
			t.Fatalf("claim issued full pod update: %#v", action)
		}
		if action.GetVerb() == "patch" {
			patchAction = action.(k8stesting.PatchAction)
		}
	}
	if patchAction == nil {
		t.Fatal("claim did not issue a pod patch")
	}
	if patchAction.GetPatchType() != types.JSONPatchType {
		t.Fatalf("patch type = %q, want %q", patchAction.GetPatchType(), types.JSONPatchType)
	}

	var operations []claimMetadataPatchOperation
	if err := json.Unmarshal(patchAction.GetPatch(), &operations); err != nil {
		t.Fatalf("unmarshal patch: %v", err)
	}
	assertClaimPatchOperation(t, operations, "test", "/metadata/uid")
	assertClaimPatchOperation(t, operations, "test", "/metadata/resourceVersion")
	assertClaimPatchOperation(t, operations, "test", metadataMapPath("labels", controller.LabelPoolType))
	assertClaimPatchOperation(t, operations, "remove", "/metadata/ownerReferences/0")
}

func TestPatchClaimedPodMetadataRejectsAlreadyClaimedPod(t *testing.T) {
	original := newClaimTestPod("sandbox-a", "idle-a", "default", true)
	original.UID = types.UID("pod-uid")
	claimed := original.DeepCopy()
	claimed.Labels[controller.LabelPoolType] = controller.PoolTypeActive
	claimed.Labels[controller.LabelSandboxID] = "sandbox-id"
	ensureSandboxCleanupFinalizer(claimed)

	client := fake.NewSimpleClientset(original.DeepCopy())
	service := &SandboxService{k8sClient: client}
	if _, err := service.patchClaimedPodMetadata(context.Background(), original, claimed); err != nil {
		t.Fatalf("first patchClaimedPodMetadata() error = %v", err)
	}
	if _, err := service.patchClaimedPodMetadata(context.Background(), original, claimed); err == nil {
		t.Fatal("second patchClaimedPodMetadata() error = nil, want stale idle-label precondition failure")
	}
}

func TestIdlePodLostDuringClaimRecognizesMetadataPatchPreconditionFailure(t *testing.T) {
	err := &apierrors.StatusError{ErrStatus: metav1.Status{
		Reason:  metav1.StatusReasonInvalid,
		Message: "testing value /metadata/resourceVersion failed: test failed",
	}}
	if !isIdlePodLostDuringClaim(err) {
		t.Fatalf("isIdlePodLostDuringClaim(%v) = false, want true", err)
	}
}

func assertClaimPatchOperation(
	t *testing.T,
	operations []claimMetadataPatchOperation,
	operation string,
	path string,
) {
	t.Helper()
	for _, candidate := range operations {
		if candidate.Operation == operation && candidate.Path == path {
			return
		}
	}
	t.Fatalf("patch does not contain %s %s: %#v", operation, path, operations)
}

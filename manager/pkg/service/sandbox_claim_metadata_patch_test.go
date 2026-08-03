package service

import (
	"context"
	"encoding/json"
	"strings"
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
	patched, err := service.patchClaimedPod(context.Background(), original, claimed)
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

func TestPatchClaimedPodReplacesOnlyProcdImageForMetadataHead(t *testing.T) {
	original := newClaimTestPod("sandbox-a", "idle-a", "default", true)
	original.UID = types.UID("pod-uid")
	original.Spec.Containers[0].Image = "example.com/procd:base"
	original.Spec.Containers[0].ImagePullPolicy = corev1.PullIfNotPresent
	original.Spec.Containers = append(original.Spec.Containers, corev1.Container{
		Name:  "sidecar",
		Image: "example.com/sidecar:v1",
	})
	claimed := original.DeepCopy()
	claimed.Labels[controller.LabelPoolType] = controller.PoolTypeActive
	claimed.Spec.Containers[procdContainerIndex(claimed.Spec.Containers)].Image =
		"example.com/rootfs/head@sha256:" + strings.Repeat("a", 64)
	// Even an accidental desired-policy change must not enter the Pod patch;
	// Kubernetes treats imagePullPolicy as immutable.
	claimed.Spec.Containers[procdContainerIndex(claimed.Spec.Containers)].ImagePullPolicy = corev1.PullNever
	claimed.Spec.Containers[1].Image = "example.com/sidecar:v2"

	client := fake.NewSimpleClientset(original.DeepCopy())
	service := &SandboxService{k8sClient: client}
	patched, err := service.patchClaimedPod(context.Background(), original, claimed)
	if err != nil {
		t.Fatalf("patchClaimedPod() error = %v", err)
	}
	if got := patched.Spec.Containers[0].Image; got != claimed.Spec.Containers[0].Image {
		t.Fatalf("procd image = %q, want %q", got, claimed.Spec.Containers[0].Image)
	}
	if got := patched.Spec.Containers[0].ImagePullPolicy; got != corev1.PullIfNotPresent {
		t.Fatalf("procd image pull policy = %q, want preserved %q", got, corev1.PullIfNotPresent)
	}
	if got := patched.Spec.Containers[1].Image; got != original.Spec.Containers[1].Image {
		t.Fatalf("sidecar image = %q, want unchanged %q", got, original.Spec.Containers[1].Image)
	}

	var patchAction k8stesting.PatchAction
	for _, action := range client.Actions() {
		if action.GetVerb() == "patch" {
			patchAction = action.(k8stesting.PatchAction)
		}
	}
	if patchAction == nil {
		t.Fatal("claim did not issue a pod patch")
	}
	var operations []claimMetadataPatchOperation
	if err := json.Unmarshal(patchAction.GetPatch(), &operations); err != nil {
		t.Fatalf("unmarshal patch: %v", err)
	}
	for _, operation := range operations {
		if strings.HasPrefix(operation.Path, "/spec/") &&
			operation.Path != "/spec/containers/0/image" {
			t.Fatalf("unexpected spec patch operation: %#v", operation)
		}
		if strings.Contains(operation.Path, "imagePullPolicy") {
			t.Fatalf("claim attempted immutable image pull policy patch: %#v", operation)
		}
	}
	assertClaimPatchOperation(t, operations, "test", "/spec/containers/0/image")
	assertClaimPatchOperation(t, operations, "replace", "/spec/containers/0/image")
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
	if _, err := service.patchClaimedPod(context.Background(), original, claimed); err != nil {
		t.Fatalf("first patchClaimedPodMetadata() error = %v", err)
	}
	if _, err := service.patchClaimedPod(context.Background(), original, claimed); err == nil {
		t.Fatal("second patchClaimedPodMetadata() error = nil, want stale idle-label precondition failure")
	}
}

func TestPatchClaimedPodMetadataUsesHotClaimClient(t *testing.T) {
	original := newClaimTestPod("sandbox-a", "idle-a", "default", true)
	original.UID = types.UID("pod-uid")
	claimed := original.DeepCopy()
	claimed.Labels[controller.LabelSandboxID] = "sandbox-id"
	claimed.Annotations[controller.AnnotationSandboxID] = "sandbox-id"
	ensureSandboxCleanupFinalizer(claimed)

	sharedClient := fake.NewSimpleClientset(original.DeepCopy())
	hotClaimClient := fake.NewSimpleClientset(original.DeepCopy())
	service := &SandboxService{
		k8sClient:         sharedClient,
		hotClaimK8sClient: hotClaimClient,
	}
	if _, err := service.patchClaimedPod(context.Background(), original, claimed); err != nil {
		t.Fatalf("patchClaimedPodMetadata() error = %v", err)
	}
	if actions := sharedClient.Actions(); len(actions) != 0 {
		t.Fatalf("shared client actions = %v, want none", actions)
	}
	if actions := hotClaimClient.Actions(); len(actions) != 1 || actions[0].GetVerb() != "patch" {
		t.Fatalf("hot claim client actions = %v, want one patch", actions)
	}
}

func TestClaimMetadataPatchPreconditionFailureRecognizesJSONPatchTest(t *testing.T) {
	err := &apierrors.StatusError{ErrStatus: metav1.Status{
		Reason:  metav1.StatusReasonInvalid,
		Message: "testing value /metadata/resourceVersion failed: test failed",
	}}
	if !isClaimMetadataPatchPreconditionFailure(err) {
		t.Fatalf("isClaimMetadataPatchPreconditionFailure(%v) = false, want true", err)
	}
}

func TestClaimMetadataPatchPreconditionFailureDoesNotHideUnchangedValidationError(t *testing.T) {
	original := newClaimTestPod("sandbox-a", "idle-a", "default", true)
	client := fake.NewSimpleClientset(original.DeepCopy())
	service := &SandboxService{k8sClient: client}
	err := &apierrors.StatusError{ErrStatus: metav1.Status{
		Reason:  metav1.StatusReasonInvalid,
		Message: "the server rejected our request due to an error in our request",
	}}

	if service.claimMetadataPatchPreconditionFailed(context.Background(), original, err) {
		t.Fatal("unchanged pod was treated as a lost claim candidate")
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

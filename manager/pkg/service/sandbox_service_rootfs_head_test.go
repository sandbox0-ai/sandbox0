package service

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/controller"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshead"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestConfigureClaimRootFSHeadAcceptsCompleteDigestPinnedHead(t *testing.T) {
	imageDigest := "sha256:" + strings.Repeat("a", 64)
	state := completeRootFSHeadTestState(imageDigest)
	req := &ClaimRequest{SandboxID: state.SandboxID}

	if err := configureClaimRootFSHead(req, state); err != nil {
		t.Fatalf("configureClaimRootFSHead() error = %v", err)
	}
	if req.RootFSHeadImageRef != state.HeadImageRef {
		t.Fatalf("RootFSHeadImageRef = %q, want %q", req.RootFSHeadImageRef, state.HeadImageRef)
	}
	if req.RootFSHeadLayerID != state.LayerID {
		t.Fatalf("RootFSHeadLayerID = %q, want %q", req.RootFSHeadLayerID, state.LayerID)
	}
	if req.RootFSBaseImageRef != "docker.io/library/debian@"+state.BaseImageDigest {
		t.Fatalf("RootFSBaseImageRef = %q", req.RootFSBaseImageRef)
	}
}

func TestConfigureClaimRootFSHeadRejectsLegacyReplayCheckpoint(t *testing.T) {
	state := &SandboxRootFSState{
		SandboxID:     "sandbox-1",
		LayerID:       "legacy-layer",
		DiffObjectKey: "rootfs/legacy.tar.gz",
	}

	err := configureClaimRootFSHead(&ClaimRequest{SandboxID: state.SandboxID}, state)
	if !errors.Is(err, ErrRootFSHeadMigrationRequired) {
		t.Fatalf("configureClaimRootFSHead() error = %v, want ErrRootFSHeadMigrationRequired", err)
	}
}

func TestOptionalRootFSHeadReferenceTreatsLegacyCheckpointAsMigrationBase(t *testing.T) {
	state := &SandboxRootFSState{
		LayerID:       "legacy-layer",
		DiffObjectKey: "rootfs/legacy.tar",
		DiffDigest:    "sha256:" + strings.Repeat("c", 64),
	}

	reference, err := optionalRootFSHeadReference(state)
	if err != nil {
		t.Fatalf("optionalRootFSHeadReference() error = %v", err)
	}
	if reference != nil {
		t.Fatalf("optionalRootFSHeadReference() = %#v, want nil legacy parent", reference)
	}
}

func TestOptionalRootFSHeadReferenceRejectsPartialMetadataHead(t *testing.T) {
	state := &SandboxRootFSState{LayerID: "partial", HeadObjectKey: "rootfs/head"}

	if _, err := optionalRootFSHeadReference(state); err == nil {
		t.Fatal("optionalRootFSHeadReference() error = nil, want invalid partial head")
	}
}

func TestApplyClaimRootFSHeadChangesOnlyProcdImageAndAnnotations(t *testing.T) {
	imageDigest := "sha256:" + strings.Repeat("b", 64)
	req := &ClaimRequest{
		RootFSHeadImageRef: "registry.example.com/rootfs/head@" + imageDigest,
		RootFSHeadLayerID:  "layer-2",
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:   "sandbox",
			Name:        "warm-pod",
			Annotations: map[string]string{"example.com/keep": "true"},
		},
		Spec: corev1.PodSpec{
			NodeName: "node-1",
			Containers: []corev1.Container{
				{Name: sandboxRootFSContainerName, Image: "registry.example.com/template:base"},
				{Name: "sidecar", Image: "registry.example.com/sidecar:v1"},
			},
		},
	}

	if err := applyClaimRootFSHeadToPod(pod, req); err != nil {
		t.Fatalf("applyClaimRootFSHeadToPod() error = %v", err)
	}
	if got := pod.Spec.Containers[0].Image; got != req.RootFSHeadImageRef {
		t.Fatalf("procd image = %q, want %q", got, req.RootFSHeadImageRef)
	}
	if got := pod.Spec.Containers[1].Image; got != "registry.example.com/sidecar:v1" {
		t.Fatalf("sidecar image = %q, want unchanged", got)
	}
	if got := pod.Spec.NodeName; got != "node-1" {
		t.Fatalf("node name = %q, want unchanged", got)
	}
	if got := pod.Annotations[controller.AnnotationRootFSHeadImage]; got != req.RootFSHeadImageRef {
		t.Fatalf("head image annotation = %q, want %q", got, req.RootFSHeadImageRef)
	}
	if got := pod.Annotations[controller.AnnotationRootFSHeadLayerID]; got != req.RootFSHeadLayerID {
		t.Fatalf("head layer annotation = %q, want %q", got, req.RootFSHeadLayerID)
	}
	if got := pod.Annotations["example.com/keep"]; got != "true" {
		t.Fatalf("unrelated annotation = %q, want unchanged", got)
	}
}

func TestApplyColdRootFSBaseImageVolumePinsBaseWithoutChangingSidecars(t *testing.T) {
	req := &ClaimRequest{
		RootFSHeadImageRef: "registry.example.com/rootfs/head@sha256:" + strings.Repeat("b", 64),
		RootFSBaseImageRef: "docker.io/library/debian@sha256:" + strings.Repeat("a", 64),
	}
	pod := &corev1.Pod{Spec: corev1.PodSpec{Containers: []corev1.Container{
		{Name: sandboxRootFSContainerName},
		{Name: "sidecar", VolumeMounts: []corev1.VolumeMount{{Name: "keep", MountPath: "/keep"}}},
	}}}

	if err := applyColdRootFSBaseImageVolume(pod, req); err != nil {
		t.Fatalf("applyColdRootFSBaseImageVolume() error = %v", err)
	}
	if len(pod.Spec.Volumes) != 1 || pod.Spec.Volumes[0].Image == nil {
		t.Fatalf("rootfs base image volume = %#v", pod.Spec.Volumes)
	}
	if got := pod.Spec.Volumes[0].Image.Reference; got != req.RootFSBaseImageRef {
		t.Fatalf("base image reference = %q, want %q", got, req.RootFSBaseImageRef)
	}
	if len(pod.Spec.Containers[0].VolumeMounts) != 1 || pod.Spec.Containers[0].VolumeMounts[0].MountPath != rootFSBaseImageMountPath {
		t.Fatalf("procd base mount = %#v", pod.Spec.Containers[0].VolumeMounts)
	}
	if got := pod.Spec.Containers[1].VolumeMounts; len(got) != 1 || got[0].Name != "keep" {
		t.Fatalf("sidecar mounts changed: %#v", got)
	}
}

func TestPodRootFSHeadReadyRequiresRunningDesiredDigest(t *testing.T) {
	imageDigest := "sha256:" + strings.Repeat("c", 64)
	desiredImage := "registry.example.com/rootfs/head@" + imageDigest
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{
			controller.AnnotationRootFSHeadImage:   desiredImage,
			controller.AnnotationRootFSHeadLayerID: "layer-3",
		}},
		Spec: corev1.PodSpec{Containers: []corev1.Container{{
			Name:  sandboxRootFSContainerName,
			Image: desiredImage,
		}}},
		Status: corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{{
			Name:    sandboxRootFSContainerName,
			Image:   desiredImage,
			ImageID: "registry.example.com/rootfs/head@" + imageDigest,
			State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{
				StartedAt: metav1.NewTime(time.Now()),
			}},
		}}},
	}

	if ready, reason := podRootFSHeadReady(pod, desiredImage, "layer-3"); !ready {
		t.Fatalf("podRootFSHeadReady() = false, reason %q", reason)
	}
	pod.Status.ContainerStatuses[0].ImageID = "registry.example.com/rootfs/head@sha256:" + strings.Repeat("d", 64)
	pod.Status.ContainerStatuses[0].Image = "registry.example.com/rootfs/head:old"
	if ready, _ := podRootFSHeadReady(pod, desiredImage, "layer-3"); ready {
		t.Fatal("podRootFSHeadReady() = true for previous image")
	}
}

func completeRootFSHeadTestState(imageDigest string) *SandboxRootFSState {
	return &SandboxRootFSState{
		SandboxID:           "sandbox-1",
		LayerID:             "layer-1",
		BaseImageRef:        "debian:bookworm-slim",
		BaseImageDigest:     imageDigest,
		HeadObjectKey:       "rootfs/heads/head.json.gz",
		HeadObjectDigest:    "sha256:" + strings.Repeat("e", 64),
		HeadObjectSize:      128,
		HeadObjectMediaType: rootfshead.HeadMediaType,
		HeadImageRef:        "registry.example.com/rootfs/head@" + imageDigest,
		HeadImageDigest:     imageDigest,
		LayerChain: []*SandboxRootFSLayer{{
			ID:             "layer-1",
			SnapshotParent: "sha256:base-snapshot",
		}},
	}
}

package service

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/controller"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
)

func TestClaimIdlePodRequiresPodReady(t *testing.T) {
	template := &v1alpha1.SandboxTemplate{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "template-a",
			Namespace: "ns-a",
		},
	}
	notReadyPod := newClaimTestPod("ns-a", "idle-not-ready", "template-a", false)

	client := fake.NewSimpleClientset(notReadyPod.DeepCopy())
	svc := &SandboxService{
		k8sClient: client,
		podLister: newClaimTestPodLister(t, notReadyPod),
		clock:     systemTime{},
		logger:    zap.NewNop(),
	}

	pod, err := svc.claimIdlePod(context.Background(), template, &ClaimRequest{
		TeamID: "team-a",
		UserID: "user-a",
	})
	if err != nil {
		t.Fatalf("claimIdlePod() error = %v", err)
	}
	if pod != nil {
		t.Fatalf("claimIdlePod() = %s, want nil for running but not-ready pod", pod.Name)
	}
}

func TestClaimIdlePodClaimsReadyPod(t *testing.T) {
	template := &v1alpha1.SandboxTemplate{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "template-a",
			Namespace: "ns-a",
		},
	}
	readyPod := newClaimTestPod("ns-a", "idle-ready", "template-a", true)

	client := fake.NewSimpleClientset(readyPod.DeepCopy())
	svc := &SandboxService{
		k8sClient: client,
		podLister: newClaimTestPodLister(t, readyPod),
		clock:     systemTime{},
		logger:    zap.NewNop(),
	}

	pod, err := svc.claimIdlePod(context.Background(), template, &ClaimRequest{
		TeamID: "team-a",
		UserID: "user-a",
	})
	if err != nil {
		t.Fatalf("claimIdlePod() error = %v", err)
	}
	if pod == nil {
		t.Fatal("claimIdlePod() = nil, want ready pod")
	}
	if pod.Name != "idle-ready" {
		t.Fatalf("claimIdlePod() selected %q, want %q", pod.Name, "idle-ready")
	}
	if got := pod.Labels[controller.LabelPoolType]; got != controller.PoolTypeActive {
		t.Fatalf("pool-type = %q, want %q", got, controller.PoolTypeActive)
	}
}

func TestWaitForPodReadyWaitsUntilReady(t *testing.T) {
	pod := newClaimTestPod("ns-a", "cold-pod", "template-a", false)
	indexer := newClaimTestPodIndexer(t, pod)
	svc := &SandboxService{
		podLister: corelisters.NewPodLister(indexer),
		config: SandboxServiceConfig{
			ProcdInitTimeout: 100 * time.Millisecond,
		},
	}

	go func() {
		time.Sleep(80 * time.Millisecond)
		updated := pod.DeepCopy()
		updated.Status.Conditions[0].Status = corev1.ConditionTrue
		updated.Status.Conditions[0].LastTransitionTime = metav1.NewTime(time.Now().UTC())
		if err := indexer.Update(updated); err != nil {
			t.Errorf("update pod: %v", err)
		}
	}()

	readyPod, err := svc.waitForPodReady(context.Background(), pod.Namespace, pod.Name)
	if err != nil {
		t.Fatalf("waitForPodReady() error = %v", err)
	}
	if !controller.IsPodReady(readyPod) {
		t.Fatalf("waitForPodReady() returned pod that is not ready")
	}
}

func TestWaitForPodReadyTimesOut(t *testing.T) {
	pod := newClaimTestPod("ns-a", "cold-pod", "template-a", false)
	svc := &SandboxService{
		podLister: newClaimTestPodLister(t, pod),
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := svc.waitForPodReady(ctx, pod.Namespace, pod.Name)
	if err == nil {
		t.Fatal("waitForPodReady() error = nil, want timeout")
	}
}

func TestValidateClaimMountsRejectsDuplicateVolume(t *testing.T) {
	req := &ClaimRequest{
		Mounts: []ClaimMount{
			{SandboxVolumeID: "vol-1", MountPoint: "/workspace/a"},
			{SandboxVolumeID: "vol-1", MountPoint: "/workspace/b"},
		},
	}

	err := validateClaimMounts(req)
	if err == nil {
		t.Fatal("expected duplicate volume validation error")
	}
	if !errors.Is(err, ErrInvalidClaimRequest) {
		t.Fatalf("expected ErrInvalidClaimRequest, got %v", err)
	}
}

func TestValidateClaimMountsRejectsDuplicateMountPoint(t *testing.T) {
	req := &ClaimRequest{
		Mounts: []ClaimMount{
			{SandboxVolumeID: "vol-1", MountPoint: "/workspace/data"},
			{SandboxVolumeID: "vol-2", MountPoint: "/workspace/data"},
		},
	}

	err := validateClaimMounts(req)
	if err == nil {
		t.Fatal("expected duplicate mount point validation error")
	}
	if !errors.Is(err, ErrInvalidClaimRequest) {
		t.Fatalf("expected ErrInvalidClaimRequest, got %v", err)
	}
}

func TestValidateClaimMountsNormalizesMountPoint(t *testing.T) {
	req := &ClaimRequest{
		Mounts: []ClaimMount{{SandboxVolumeID: "vol-1", MountPoint: "/workspace/project/../data"}},
	}

	if err := validateClaimMounts(req); err != nil {
		t.Fatalf("validateClaimMounts() error = %v", err)
	}
	if got := req.Mounts[0].MountPoint; got != "/workspace/data" {
		t.Fatalf("mount point = %q, want %q", got, "/workspace/data")
	}
}

func TestMergeTemplateSharedVolumeMountsUsesClaimBindingForOptionalSharedVolume(t *testing.T) {
	claimWriteback := true
	claimPrefetch := int32(128)
	got, err := mergeTemplateSharedVolumeMounts(
		[]ClaimMount{{
			SandboxVolumeID: "vol-claim",
			MountPoint:      "/workspace/shared",
			VolumeConfig: &MountVolumeConfig{
				Writeback: &claimWriteback,
				Prefetch:  &claimPrefetch,
			},
		}},
		[]v1alpha1.SharedVolumeSpec{{
			Name:      "workspace",
			MountPath: "/workspace/shared",
			CacheSize: "4Gi",
		}},
	)
	if err != nil {
		t.Fatalf("mergeTemplateSharedVolumeMounts() error = %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("mount count = %d, want 1", len(got))
	}
	if got[0].SandboxVolumeID != "vol-claim" {
		t.Fatalf("sandbox volume id = %q, want %q", got[0].SandboxVolumeID, "vol-claim")
	}
	if got[0].MountPoint != "/workspace/shared" {
		t.Fatalf("mount point = %q, want %q", got[0].MountPoint, "/workspace/shared")
	}
	if got[0].VolumeConfig == nil {
		t.Fatal("expected merged volume config")
	}
	if got[0].VolumeConfig.CacheSize != "4Gi" {
		t.Fatalf("cache size = %q, want %q", got[0].VolumeConfig.CacheSize, "4Gi")
	}
	if got[0].VolumeConfig.Prefetch == nil || *got[0].VolumeConfig.Prefetch != claimPrefetch {
		t.Fatalf("prefetch = %v, want %d", got[0].VolumeConfig.Prefetch, claimPrefetch)
	}
	if got[0].VolumeConfig.Writeback == nil || !*got[0].VolumeConfig.Writeback {
		t.Fatalf("writeback = %v, want true", got[0].VolumeConfig.Writeback)
	}
}

func TestMergeTemplateSharedVolumeMountsRejectsMissingClaimBindingForOptionalSharedVolume(t *testing.T) {
	_, err := mergeTemplateSharedVolumeMounts(nil, []v1alpha1.SharedVolumeSpec{{
		Name:      "workspace",
		MountPath: "/workspace/shared",
	}})
	if err == nil {
		t.Fatal("expected missing claim binding error")
	}
	if !errors.Is(err, ErrInvalidClaimRequest) {
		t.Fatalf("expected ErrInvalidClaimRequest, got %v", err)
	}
	if !strings.Contains(err.Error(), `shared volume "workspace" requires a claim mount for "/workspace/shared"`) {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestMergeTemplateSharedVolumeMountsRejectsClaimConflictForPinnedSharedVolume(t *testing.T) {
	_, err := mergeTemplateSharedVolumeMounts(
		[]ClaimMount{{SandboxVolumeID: "vol-claim", MountPoint: "/workspace/shared"}},
		[]v1alpha1.SharedVolumeSpec{{
			Name:            "workspace",
			SandboxVolumeID: "vol-template",
			MountPath:       "/workspace/shared",
		}},
	)
	if err == nil {
		t.Fatal("expected claim conflict error")
	}
	if !errors.Is(err, ErrInvalidClaimRequest) {
		t.Fatalf("expected ErrInvalidClaimRequest, got %v", err)
	}
	if !strings.Contains(err.Error(), `claim mount "/workspace/shared" conflicts with template shared volume "workspace"`) {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestClaimMountWaitTimeoutDefaultsWhenEnabled(t *testing.T) {
	got := claimMountWaitTimeout(&ClaimRequest{WaitForMounts: true})
	if got != 30*time.Second {
		t.Fatalf("claimMountWaitTimeout() = %s, want 30s", got)
	}
	custom := int32(2500)
	got = claimMountWaitTimeout(&ClaimRequest{WaitForMounts: true, MountWaitTimeoutMs: &custom})
	if got != 2500*time.Millisecond {
		t.Fatalf("claimMountWaitTimeout() with override = %s, want 2500ms", got)
	}
}

func newClaimTestPodLister(t *testing.T, pods ...*corev1.Pod) corelisters.PodLister {
	t.Helper()
	return corelisters.NewPodLister(newClaimTestPodIndexer(t, pods...))
}

func newClaimTestPodIndexer(t *testing.T, pods ...*corev1.Pod) cache.Indexer {
	t.Helper()
	indexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{
		cache.NamespaceIndex: cache.MetaNamespaceIndexFunc,
	})
	for _, pod := range pods {
		if err := indexer.Add(pod); err != nil {
			t.Fatalf("add pod: %v", err)
		}
	}
	return indexer
}

func newClaimTestPod(namespace, name, templateID string, ready bool) *corev1.Pod {
	status := corev1.ConditionFalse
	if ready {
		status = corev1.ConditionTrue
	}
	now := time.Now().UTC()
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      name,
			Labels: map[string]string{
				controller.LabelTemplateID: templateID,
				controller.LabelPoolType:   controller.PoolTypeIdle,
			},
			ResourceVersion: "1",
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			Conditions: []corev1.PodCondition{
				{
					Type:               corev1.PodReady,
					Status:             status,
					LastTransitionTime: metav1.NewTime(now),
				},
			},
		},
	}
}

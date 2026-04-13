package service

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/controller"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/network"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"github.com/sandbox0-ai/sandbox0/pkg/naming"
	"github.com/sandbox0-ai/sandbox0/pkg/sandboxprobe"
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

func TestWaitForPodReadyWaitsForPodToAppear(t *testing.T) {
	indexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc})
	svc := &SandboxService{
		podLister: corelisters.NewPodLister(indexer),
		config: SandboxServiceConfig{
			ProcdInitTimeout: 100 * time.Millisecond,
		},
	}

	pod := newClaimTestPod("ns-a", "cold-pod", "template-a", true)
	go func() {
		time.Sleep(80 * time.Millisecond)
		if err := indexer.Add(pod); err != nil {
			t.Errorf("add pod: %v", err)
		}
	}()

	readyPod, err := svc.waitForPodReady(context.Background(), pod.Namespace, pod.Name)
	if err != nil {
		t.Fatalf("waitForPodReady() error = %v", err)
	}
	if readyPod.Name != pod.Name {
		t.Fatalf("waitForPodReady() returned %q, want %q", readyPod.Name, pod.Name)
	}
}

func TestWaitForPodClaimReadyUsesSandboxReadinessWithoutPodReady(t *testing.T) {
	pod := newClaimReadyTestPod("ns-a", "cold-pod", "template-a")
	indexer := newClaimTestPodIndexer(t, pod)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Fatalf("method = %s, want POST", r.Method)
		}
		if r.URL.Path != "/api/v1/pods/ns-a/cold-pod/probes/readiness" {
			t.Fatalf("path = %s", r.URL.Path)
		}
		_ = json.NewEncoder(w).Encode(sandboxprobe.Passed(sandboxprobe.KindReadiness, "SandboxProbePassed", "sandbox probe passed", nil))
	}))
	defer server.Close()
	host, port := splitTestServerAddress(t, server)

	svc := &SandboxService{
		k8sClient:  fake.NewSimpleClientset(pod.DeepCopy(), newClaimTestNode("node-a", host)),
		podLister:  corelisters.NewPodLister(indexer),
		ctldClient: NewCtldClient(CtldClientConfig{Timeout: time.Second}),
		config: SandboxServiceConfig{
			CtldPort: port,
		},
		logger: zap.NewNop(),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	readyPod, err := svc.waitForPodClaimReady(ctx, pod.Namespace, pod.Name)
	if err != nil {
		t.Fatalf("waitForPodClaimReady() error = %v", err)
	}
	if controller.IsPodReady(readyPod) {
		t.Fatal("waitForPodClaimReady() waited for Kubernetes PodReady; want sandbox claim readiness only")
	}
}

func TestCreateNewPodCleansUpAfterNetworkApplyFailure(t *testing.T) {
	withClaimTestPublicKey(t)

	template := &v1alpha1.SandboxTemplate{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "template-a",
			Namespace: "ns-a",
		},
		Spec: v1alpha1.SandboxTemplateSpec{
			MainContainer: v1alpha1.ContainerSpec{Image: "busybox"},
		},
	}
	applyErr := errors.New("apply failed")
	removed := make([]string, 0, 1)
	client := fake.NewSimpleClientset()
	svc := &SandboxService{
		k8sClient:            client,
		secretLister:         newClaimTestSecretLister(t),
		NetworkPolicyService: NewNetworkPolicyService(zap.NewNop()),
		networkProvider: &assertingNetworkProvider{
			applyErr: applyErr,
			removeFunc: func(namespace, sandboxID string) {
				removed = append(removed, namespace+"/"+sandboxID)
			},
		},
		clock:  systemTime{},
		logger: zap.NewNop(),
	}

	_, err := svc.createNewPod(context.Background(), template, &ClaimRequest{TeamID: "team-a", UserID: "user-a"})
	if err == nil {
		t.Fatal("createNewPod() error = nil, want network apply failure")
	}
	if !errors.Is(err, applyErr) {
		t.Fatalf("createNewPod() error = %v, want wrapped apply failure", err)
	}

	pods, err := client.CoreV1().Pods("ns-a").List(context.Background(), metav1.ListOptions{})
	if err != nil {
		t.Fatalf("list pods: %v", err)
	}
	if len(pods.Items) != 0 {
		t.Fatalf("pods after failed cold claim = %d, want 0", len(pods.Items))
	}
	if len(removed) != 1 {
		t.Fatalf("network policy removals = %d, want 1", len(removed))
	}
}

func TestClaimSandboxCleansColdPodWhenClaimReadinessFails(t *testing.T) {
	withClaimTestPublicKey(t)
	templateNamespace, err := naming.TemplateNamespaceForBuiltin("managed-agent-claude")
	if err != nil {
		t.Fatalf("template namespace: %v", err)
	}
	template := &v1alpha1.SandboxTemplate{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "managed-agent-claude",
			Namespace: templateNamespace,
		},
		Spec: v1alpha1.SandboxTemplateSpec{
			MainContainer: v1alpha1.ContainerSpec{Image: "busybox"},
		},
	}
	client := fake.NewSimpleClientset()
	ctx, cancel := context.WithCancel(context.Background())
	svc := &SandboxService{
		k8sClient:            client,
		podLister:            newClaimTestPodLister(t),
		secretLister:         newClaimTestSecretLister(t),
		templateLister:       staticTemplateLister{templates: []*v1alpha1.SandboxTemplate{template}},
		NetworkPolicyService: NewNetworkPolicyService(zap.NewNop()),
		networkProvider: &assertingNetworkProvider{applyFunc: func(_ network.SandboxPolicyInput) {
			cancel()
		}},
		clock:  systemTime{},
		logger: zap.NewNop(),
	}

	_, err = svc.ClaimSandbox(ctx, &ClaimRequest{Template: "managed-agent-claude", TeamID: "team-a", UserID: "user-a"})
	if err == nil {
		t.Fatal("ClaimSandbox() error = nil, want claim readiness failure")
	}

	pods, err := client.CoreV1().Pods(templateNamespace).List(context.Background(), metav1.ListOptions{})
	if err != nil {
		t.Fatalf("list pods: %v", err)
	}
	if len(pods.Items) != 0 {
		t.Fatalf("pods after failed cold claim = %d, want 0", len(pods.Items))
	}
}

func TestWaitForPodClaimReadyWaitsForProcdContainerRunning(t *testing.T) {
	pod := newClaimReadyTestPod("ns-a", "cold-pod", "template-a")
	pod.Status.ContainerStatuses = nil
	indexer := newClaimTestPodIndexer(t, pod)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(sandboxprobe.Passed(sandboxprobe.KindReadiness, "SandboxProbePassed", "sandbox probe passed", nil))
	}))
	defer server.Close()
	host, port := splitTestServerAddress(t, server)

	svc := &SandboxService{
		k8sClient:  fake.NewSimpleClientset(pod.DeepCopy(), newClaimTestNode("node-a", host)),
		podLister:  corelisters.NewPodLister(indexer),
		ctldClient: NewCtldClient(CtldClientConfig{Timeout: time.Second}),
		config: SandboxServiceConfig{
			CtldPort: port,
		},
		logger: zap.NewNop(),
	}

	go func() {
		time.Sleep(80 * time.Millisecond)
		updated := pod.DeepCopy()
		updated.Status.ContainerStatuses = []corev1.ContainerStatus{{
			Name:  "procd",
			Ready: true,
			State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: metav1.Now()}},
		}}
		if err := indexer.Update(updated); err != nil {
			t.Errorf("update pod: %v", err)
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	readyPod, err := svc.waitForPodClaimReady(ctx, pod.Namespace, pod.Name)
	if err != nil {
		t.Fatalf("waitForPodClaimReady() error = %v", err)
	}
	if !podContainerRunning(readyPod, "procd") {
		t.Fatal("waitForPodClaimReady() returned before procd container was running")
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

func withClaimTestPublicKey(t *testing.T) {
	t.Helper()
	keyPath := filepath.Join(t.TempDir(), "internal_jwt_public.key")
	if err := os.WriteFile(keyPath, []byte("test-public-key"), 0o600); err != nil {
		t.Fatalf("write public key: %v", err)
	}
	previousKeyPath := internalauth.DefaultInternalJWTPublicKeyPath
	internalauth.DefaultInternalJWTPublicKeyPath = keyPath
	t.Cleanup(func() { internalauth.DefaultInternalJWTPublicKeyPath = previousKeyPath })
}

func newClaimTestSecretLister(t *testing.T, secrets ...*corev1.Secret) corelisters.SecretLister {
	t.Helper()
	indexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{
		cache.NamespaceIndex: cache.MetaNamespaceIndexFunc,
	})
	for _, secret := range secrets {
		if err := indexer.Add(secret); err != nil {
			t.Fatalf("add secret: %v", err)
		}
	}
	return corelisters.NewSecretLister(indexer)
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

func newClaimReadyTestPod(namespace, name, templateID string) *corev1.Pod {
	pod := newClaimTestPod(namespace, name, templateID, false)
	pod.Spec.NodeName = "node-a"
	pod.Spec.ReadinessGates = []corev1.PodReadinessGate{{ConditionType: v1alpha1.SandboxPodReadinessConditionType}}
	pod.Status.PodIP = "10.244.0.10"
	pod.Status.Conditions = append(pod.Status.Conditions,
		corev1.PodCondition{Type: corev1.ContainersReady, Status: corev1.ConditionTrue},
		corev1.PodCondition{Type: v1alpha1.SandboxPodReadinessConditionType, Status: corev1.ConditionFalse},
	)
	pod.Status.ContainerStatuses = []corev1.ContainerStatus{{
		Name:  "procd",
		Ready: true,
		State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: metav1.Now()}},
	}}
	return pod
}

func newClaimTestNode(name, internalIP string) *corev1.Node {
	return &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Status: corev1.NodeStatus{Addresses: []corev1.NodeAddress{{
			Type:    corev1.NodeInternalIP,
			Address: internalIP,
		}}},
	}
}

func splitTestServerAddress(t *testing.T, server *httptest.Server) (string, int) {
	t.Helper()
	parsed, err := url.Parse(server.URL)
	if err != nil {
		t.Fatalf("parse server URL: %v", err)
	}
	host, portString, err := net.SplitHostPort(parsed.Host)
	if err != nil {
		t.Fatalf("split server host: %v", err)
	}
	port, err := strconv.Atoi(portString)
	if err != nil {
		t.Fatalf("parse server port: %v", err)
	}
	return host, port
}

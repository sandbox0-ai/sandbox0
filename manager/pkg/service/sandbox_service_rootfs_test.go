package service

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/controller"
	"github.com/sandbox0-ai/sandbox0/pkg/ctldapi"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
)

func TestPauseSandboxRuntimeSavesRootFSBeforeDeletingPod(t *testing.T) {
	saveCalled := false
	ctld := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/api/v1/rootfs/save", r.URL.Path)
		var req ctldapi.SaveRootFSRequest
		require.NoError(t, json.NewDecoder(r.Body).Decode(&req))
		assert.Equal(t, "sandbox-1", req.SandboxID)
		assert.Equal(t, "team-1", req.TeamID)
		assert.Equal(t, int64(3), req.ExpectedRuntimeGeneration)
		assert.True(t, req.Freeze)
		assert.Equal(t, ctldapi.RootFSContainerRef{
			Namespace:     "default",
			PodName:       "pod-1",
			PodUID:        "pod-uid",
			ContainerName: "procd",
		}, req.Target)
		saveCalled = true
		_ = json.NewEncoder(w).Encode(ctldapi.SaveRootFSResponse{
			Info: ctldapi.RootFSInfo{
				Runtime:             "runc",
				RuntimeHandler:      "io.containerd.runc.v2",
				BaseImageRef:        "docker.io/library/busybox:1.36",
				BaseImageDigest:     "sha256:base",
				Snapshotter:         "overlayfs",
				SnapshotParent:      "parent-1",
				SnapshotParentChain: []string{"parent-1", "parent-0"},
			},
			Descriptor: ctldapi.RootFSDiffDescriptor{
				MediaType: "application/vnd.oci.image.layer.v1.tar",
				Digest:    "sha256:diff",
				Size:      123,
				ObjectKey: "sandbox-rootfs/team-1/sandbox-1/3/sha256/diff.tar",
			},
		})
	}))
	defer ctld.Close()
	ctldURL, ctldPort := parsedTestServer(t, ctld.URL)

	pod := rootFSTestPod("pod-1", "sandbox-1", "team-1")
	pod.Status.HostIP = ctldURL.Hostname()
	k8sClient := fake.NewSimpleClientset(pod)
	k8sClient.PrependReactor("delete", "pods", func(action ktesting.Action) (bool, runtime.Object, error) {
		require.True(t, saveCalled, "pod delete must happen after rootfs checkpoint save")
		return false, nil, nil
	})
	store := &memorySandboxStore{records: map[string]*SandboxRecord{
		"sandbox-1": {
			ID:                "sandbox-1",
			TeamID:            "team-1",
			RuntimeGeneration: 3,
			Status:            SandboxStatusRunning,
		},
	}}
	svc := &SandboxService{
		k8sClient:    k8sClient,
		podLister:    newTestPodLister(t, pod),
		sandboxStore: store,
		ctldClient:   NewCtldClient(CtldClientConfig{Timeout: time.Second}),
		config:       SandboxServiceConfig{CtldEnabled: true, CtldPort: ctldPort},
		clock:        systemTime{},
		logger:       zap.NewNop(),
	}

	require.NoError(t, svc.PauseSandboxRuntime(context.Background(), "sandbox-1"))

	state := store.rootFSStates["sandbox-1"]
	require.NotNil(t, state)
	assert.Equal(t, int64(3), state.RuntimeGeneration)
	assert.Equal(t, "runc", state.Runtime)
	assert.Equal(t, "sha256:base", state.BaseImageDigest)
	assert.Equal(t, []string{"parent-1", "parent-0"}, state.SnapshotParentChain)
	assert.Equal(t, "sha256:diff", state.DiffDigest)
	assert.Equal(t, "sandbox-rootfs/team-1/sandbox-1/3/sha256/diff.tar", state.DiffObjectKey)
	assert.Equal(t, SandboxStatusPaused, store.records["sandbox-1"].Status)
}

func TestFinishRestoredSandboxRuntimeAppliesRootFSBeforeProcdInitialization(t *testing.T) {
	var calls []string
	ctld := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/api/v1/rootfs/apply", r.URL.Path)
		var req ctldapi.ApplyRootFSRequest
		require.NoError(t, json.NewDecoder(r.Body).Decode(&req))
		assert.Equal(t, "runc", req.ExpectedRuntime)
		assert.Equal(t, "io.containerd.runc.v2", req.ExpectedRuntimeHandler)
		assert.Equal(t, "overlayfs", req.ExpectedSnapshotter)
		assert.Equal(t, "sha256:base", req.ExpectedBaseImageDigest)
		assert.Equal(t, "parent-1", req.ExpectedSnapshotParent)
		assert.Equal(t, []string{"parent-1", "parent-0"}, req.ExpectedSnapshotParentChain)
		assert.Equal(t, "sha256:diff", req.Descriptor.Digest)
		assert.Equal(t, "sandbox-rootfs/team-1/sandbox-1/3/sha256/diff.tar", req.Descriptor.ObjectKey)
		assert.True(t, req.Freeze)
		calls = append(calls, "apply")
		_ = json.NewEncoder(w).Encode(ctldapi.ApplyRootFSResponse{Applied: true})
	}))
	defer ctld.Close()
	ctldURL, ctldPort := parsedTestServer(t, ctld.URL)

	procd := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/api/v1/initialize", r.URL.Path)
		require.Equal(t, []string{"apply"}, calls)
		calls = append(calls, "procd")
		require.NoError(t, spec.WriteSuccess(w, http.StatusOK, InitializeResponse{SandboxID: "sandbox-1", TeamID: "team-1"}))
	}))
	defer procd.Close()
	procdURL, procdPort := parsedTestServer(t, procd.URL)

	pod := rootFSTestPod("pod-1", "sandbox-1", "team-1")
	pod.Status.HostIP = ctldURL.Hostname()
	pod.Status.PodIP = procdURL.Hostname()
	store := &memorySandboxStore{
		records: map[string]*SandboxRecord{},
		rootFSStates: map[string]*SandboxRootFSState{
			"sandbox-1": rootFSTestState(),
		},
	}
	svc := &SandboxService{
		k8sClient:              fake.NewSimpleClientset(pod),
		podLister:              newTestPodLister(t, pod),
		sandboxStore:           store,
		ctldClient:             NewCtldClient(CtldClientConfig{Timeout: time.Second}),
		procdClient:            NewProcdClient(ProcdClientConfig{Timeout: time.Second}),
		internalTokenGenerator: staticTokenGenerator{},
		config: SandboxServiceConfig{
			CtldEnabled:      true,
			CtldPort:         ctldPort,
			ProcdPort:        procdPort,
			ProcdInitTimeout: time.Second,
		},
		clock:  systemTime{},
		logger: zap.NewNop(),
	}
	record := &SandboxRecord{
		ID:                "sandbox-1",
		TeamID:            "team-1",
		UserID:            "user-1",
		TemplateID:        "template-1",
		TemplateName:      "template-1",
		TemplateNamespace: "template-default",
		TemplateSpec:      v1alpha1.SandboxTemplateSpec{},
		RuntimeGeneration: 3,
		Status:            SandboxStatusPaused,
	}

	require.NoError(t, svc.finishRestoredSandboxRuntime(context.Background(), pod, record, "hot"))
	assert.Equal(t, []string{"apply", "procd"}, calls)
}

func TestRestoreFailureCleanupCanSkipRootFSSave(t *testing.T) {
	var saveCalled atomic.Bool
	ctld := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v1/rootfs/save" {
			saveCalled.Store(true)
		}
		_ = json.NewEncoder(w).Encode(ctldapi.SaveRootFSResponse{})
	}))
	defer ctld.Close()
	ctldURL, ctldPort := parsedTestServer(t, ctld.URL)

	pod := rootFSTestPod("pod-1", "sandbox-1", "team-1")
	pod.Status.HostIP = ctldURL.Hostname()
	originalState := rootFSTestState()
	store := &memorySandboxStore{
		records: map[string]*SandboxRecord{
			"sandbox-1": {
				ID:                "sandbox-1",
				TeamID:            "team-1",
				RuntimeGeneration: 3,
				Status:            SandboxStatusStarting,
			},
		},
		rootFSStates: map[string]*SandboxRootFSState{
			"sandbox-1": originalState,
		},
	}
	svc := &SandboxService{
		k8sClient:    fake.NewSimpleClientset(pod),
		podLister:    newTestPodLister(t, pod),
		sandboxStore: store,
		ctldClient:   NewCtldClient(CtldClientConfig{Timeout: time.Second}),
		config:       SandboxServiceConfig{CtldEnabled: true, CtldPort: ctldPort},
		clock:        systemTime{},
		logger:       zap.NewNop(),
	}

	require.NoError(t, svc.pauseSandboxRuntime(context.Background(), "sandbox-1", false))

	assert.False(t, saveCalled.Load())
	assert.Equal(t, originalState.DiffObjectKey, store.rootFSStates["sandbox-1"].DiffObjectKey)
	assert.Equal(t, SandboxStatusPaused, store.records["sandbox-1"].Status)
}

func rootFSTestPod(name, sandboxID, teamID string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1ObjectMeta(name, sandboxID, teamID),
		Spec: corev1.PodSpec{
			NodeName: "node-1",
			Containers: []corev1.Container{{
				Name: "procd",
			}},
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			ContainerStatuses: []corev1.ContainerStatus{{
				Name:  "procd",
				State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}},
			}},
		},
	}
}

func metav1ObjectMeta(name, sandboxID, teamID string) metav1.ObjectMeta {
	return metav1.ObjectMeta{
		Name:      name,
		Namespace: "default",
		UID:       types.UID("pod-uid"),
		Labels: map[string]string{
			controller.LabelSandboxID:         sandboxID,
			controller.LabelTemplateID:        "template-1",
			controller.LabelTemplateLogicalID: "template-1",
			controller.LabelPoolType:          controller.PoolTypeActive,
		},
		Annotations: map[string]string{
			controller.AnnotationSandboxID:         sandboxID,
			controller.AnnotationTeamID:            teamID,
			controller.AnnotationUserID:            "user-1",
			controller.AnnotationRuntimeGeneration: "3",
			controller.AnnotationClaimedAt:         time.Now().UTC().Format(time.RFC3339),
			controller.AnnotationClaimType:         "hot",
		},
	}
}

func rootFSTestState() *SandboxRootFSState {
	return &SandboxRootFSState{
		SandboxID:           "sandbox-1",
		TeamID:              "team-1",
		RuntimeGeneration:   3,
		Runtime:             "runc",
		RuntimeHandler:      "io.containerd.runc.v2",
		BaseImageRef:        "docker.io/library/busybox:1.36",
		BaseImageDigest:     "sha256:base",
		Snapshotter:         "overlayfs",
		SnapshotParent:      "parent-1",
		SnapshotParentChain: []string{"parent-1", "parent-0"},
		DiffDigest:          "sha256:diff",
		DiffMediaType:       "application/vnd.oci.image.layer.v1.tar",
		DiffSize:            123,
		DiffObjectKey:       "sandbox-rootfs/team-1/sandbox-1/3/sha256/diff.tar",
	}
}

func parsedTestServer(t *testing.T, rawURL string) (*url.URL, int) {
	t.Helper()
	parsed, err := url.Parse(rawURL)
	require.NoError(t, err)
	port, err := strconv.Atoi(parsed.Port())
	require.NoError(t, err)
	return parsed, port
}

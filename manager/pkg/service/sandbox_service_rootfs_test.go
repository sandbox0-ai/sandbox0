package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
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
	"github.com/sandbox0-ai/sandbox0/pkg/tokenbucket"
	"github.com/sandbox0-ai/sandbox0/pkg/volumeportal"
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

func TestPrepareRootFSObjectPublishFailsClosedWithoutTeamQuotaStore(t *testing.T) {
	service := &SandboxService{}
	err := service.prepareRootFSObjectPublish(context.Background(), "stage-a", &SandboxRootFSState{
		TeamID:        "team-a",
		DiffObjectKey: "rootfs/team-a/object.tar",
	}, time.Now())
	if !errors.Is(err, ErrTeamQuotaUnavailable) {
		t.Fatalf("prepareRootFSObjectPublish() error = %v, want ErrTeamQuotaUnavailable", err)
	}
}

func TestPauseSandboxRuntimeQueuesRootFSSaveBeforeDeletingPod(t *testing.T) {
	saveCalled := false
	ctld := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/rootfs/snapshots/prepare":
			var req ctldapi.PrepareRootFSSnapshotRequest
			require.NoError(t, json.NewDecoder(r.Body).Decode(&req))
			assert.Empty(t, req.ParentLayerID)
			assert.Equal(t, ctldapi.RootFSContainerRef{
				Namespace:     "default",
				PodName:       "pod-1",
				PodUID:        "pod-uid",
				ContainerName: "procd",
			}, req.Target)
			assert.ElementsMatch(t, []string{"/workspace/data", volumeportal.WebhookStateMountPath}, req.ExcludedPaths)
			_ = json.NewEncoder(w).Encode(ctldapi.PrepareRootFSSnapshotResponse{
				Handle: "handle-1",
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
				},
			})
		case "/api/v1/rootfs/snapshots/publish":
			var req ctldapi.PublishRootFSSnapshotRequest
			require.NoError(t, json.NewDecoder(r.Body).Decode(&req))
			assert.Equal(t, "handle-1", req.Handle)
			assert.Equal(t, "sandbox-1", req.SandboxID)
			assert.Equal(t, "team-1", req.TeamID)
			assert.Equal(t, int64(3), req.ExpectedRuntimeGeneration)
			saveCalled = true
			_ = json.NewEncoder(w).Encode(ctldapi.PublishRootFSSnapshotResponse{
				Published: true,
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
		default:
			t.Fatalf("unexpected ctld path %s", r.URL.Path)
		}
	}))
	defer ctld.Close()
	ctldURL, ctldPort := parsedTestServer(t, ctld.URL)

	pod := rootFSTestPod("pod-1", "sandbox-1", "team-1")
	addRootFSTestVolumePortal(pod, "data", "/workspace/data")
	addRootFSTestVolumePortal(pod, volumeportal.WebhookStatePortalName, volumeportal.WebhookStateMountPath)
	setRootFSTestClaimMounts(t, pod, []ClaimMount{{SandboxVolumeID: "vol-1", MountPoint: "/workspace/data"}})
	pod.Annotations[controller.AnnotationWebhookStateVolumeID] = "webhook-state-vol-1"
	pod.Status.HostIP = ctldURL.Hostname()
	k8sClient := fake.NewSimpleClientset(pod)
	deleteCalled := false
	k8sClient.PrependReactor("delete", "pods", func(action ktesting.Action) (bool, runtime.Object, error) {
		require.True(t, saveCalled, "pod delete must happen after rootfs checkpoint save")
		deleteCalled = true
		return true, nil, nil
	})
	k8sClient.PrependReactor("update", "pods", func(action ktesting.Action) (bool, runtime.Object, error) {
		updated := action.(ktesting.UpdateAction).GetObject().(*corev1.Pod)
		assert.NotContains(t, updated.Annotations, "sandbox0.ai/runtime-deletion-reason")
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
	enqueuer := &recordingPauseEnqueuer{}
	svc := &SandboxService{
		k8sClient:      k8sClient,
		podLister:      newTestPodLister(t, pod),
		sandboxStore:   store,
		teamQuotaStore: &permissiveTeamQuotaCapacityStore{},
		ctldClient:     NewCtldClient(CtldClientConfig{Timeout: time.Second}),
		config:         SandboxServiceConfig{CtldEnabled: true, CtldPort: ctldPort},
		clock:          systemTime{},
		logger:         zap.NewNop(),
		pauseEnqueuer:  enqueuer,
	}
	var procdCalls []string
	defer attachRootFSTestProcd(t, pod, svc, &procdCalls)()

	resp, err := svc.PauseSandbox(context.Background(), "sandbox-1")
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.False(t, resp.Paused)
	assert.Equal(t, SandboxStatusRunning, resp.Status)
	assert.False(t, saveCalled, "pause request must not synchronously save rootfs")
	assert.Equal(t, []string{"sandbox-1"}, enqueuer.calls)
	assert.Equal(t, SandboxStatusRunning, store.records["sandbox-1"].Status)
	require.Len(t, store.lifecycleTxns, 1)

	require.NoError(t, svc.CompletePausingSandboxRuntime(context.Background(), "sandbox-1"))

	assert.True(t, deleteCalled)
	assert.Contains(t, procdCalls, "barrier:true")
	assert.Contains(t, procdCalls, "pause")
	_, err = k8sClient.CoreV1().Pods(pod.Namespace).Get(context.Background(), pod.Name, metav1.GetOptions{})
	require.NoError(t, err, "pause completion should not wait for the pod to disappear after delete is accepted")
	state := store.rootFSStates["sandbox-1"]
	require.NotNil(t, state)
	assert.Equal(t, int64(3), state.RuntimeGeneration)
	assert.Equal(t, "runc", state.Runtime)
	assert.Equal(t, "sha256:base", state.BaseImageDigest)
	assert.Equal(t, []string{"parent-1", "parent-0"}, state.SnapshotParentChain)
	assert.Equal(t, "sha256:diff", state.DiffDigest)
	assert.Equal(t, "sandbox-rootfs/team-1/sandbox-1/3/sha256/diff.tar", state.DiffObjectKey)
	assert.NotEmpty(t, state.LayerID)
	assert.Equal(t, SandboxStatusPaused, store.records["sandbox-1"].Status)
}

func TestPauseSandboxRuntimeSavesChildLayerFromParentHead(t *testing.T) {
	var savedReq ctldapi.PrepareRootFSSnapshotRequest
	ctld := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/rootfs/snapshots/prepare":
			require.NoError(t, json.NewDecoder(r.Body).Decode(&savedReq))
			_ = json.NewEncoder(w).Encode(ctldapi.PrepareRootFSSnapshotResponse{
				Handle: "handle-1",
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
					Digest:    "sha256:child",
					Size:      123,
				},
			})
		case "/api/v1/rootfs/snapshots/publish":
			_ = json.NewEncoder(w).Encode(ctldapi.PublishRootFSSnapshotResponse{
				Published: true,
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
					Digest:    "sha256:child",
					Size:      123,
					ObjectKey: "sandbox-rootfs/team-1/sandbox-1/4/sha256/child.tar",
				},
			})
		default:
			t.Fatalf("unexpected ctld path %s", r.URL.Path)
		}
	}))
	defer ctld.Close()
	ctldURL, ctldPort := parsedTestServer(t, ctld.URL)

	pod := rootFSTestPod("pod-1", "sandbox-1", "team-1")
	pod.Status.HostIP = ctldURL.Hostname()
	store := &memorySandboxStore{
		records: map[string]*SandboxRecord{
			"sandbox-1": {
				ID:                "sandbox-1",
				TeamID:            "team-1",
				RuntimeGeneration: 3,
				Status:            SandboxStatusRunning,
			},
		},
		rootFSStates: map[string]*SandboxRootFSState{
			"sandbox-1": {
				LayerID:           "layer-parent",
				SandboxID:         "sandbox-1",
				TeamID:            "team-1",
				RuntimeGeneration: 3,
				DiffDigest:        "sha256:parent",
				DiffObjectKey:     "sandbox-rootfs/team-1/sandbox-1/3/sha256/parent.tar",
			},
		},
	}
	svc := &SandboxService{
		k8sClient:      fake.NewSimpleClientset(pod),
		podLister:      newTestPodLister(t, pod),
		sandboxStore:   store,
		teamQuotaStore: &permissiveTeamQuotaCapacityStore{},
		ctldClient:     NewCtldClient(CtldClientConfig{Timeout: time.Second}),
		config:         SandboxServiceConfig{CtldEnabled: true, CtldPort: ctldPort},
		clock:          systemTime{},
		logger:         zap.NewNop(),
	}
	defer attachRootFSTestProcd(t, pod, svc, nil)()
	addRootFSTestPauseTxn(store, pod, SandboxLifecyclePhasePreparing)

	require.NoError(t, svc.CompletePausingSandboxRuntime(context.Background(), "sandbox-1"))

	assert.Equal(t, "layer-parent", savedReq.ParentLayerID)
	state := store.rootFSStates["sandbox-1"]
	require.NotNil(t, state)
	assert.NotEmpty(t, state.LayerID)
	assert.Equal(t, "layer-parent", state.ParentLayerID)
	assert.Equal(t, "sha256:child", state.DiffDigest)
}

func TestCompletePausingSandboxRuntimeDoesNotCommitStaleCheckpoint(t *testing.T) {
	store := &memorySandboxStore{
		records: map[string]*SandboxRecord{
			"sandbox-1": {
				ID:                "sandbox-1",
				TeamID:            "team-1",
				RuntimeGeneration: 3,
				Status:            SandboxStatusRunning,
			},
		},
	}
	txnID := ""
	ctld := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/rootfs/snapshots/prepare":
			store.mu.Lock()
			store.lifecycleTxns[txnID].Phase = SandboxLifecyclePhaseAborted
			store.mu.Unlock()
			_ = json.NewEncoder(w).Encode(ctldapi.PrepareRootFSSnapshotResponse{
				Handle: "handle-1",
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
					Digest:    "sha256:stale",
					Size:      123,
				},
			})
		case "/api/v1/rootfs/snapshots/publish":
			_ = json.NewEncoder(w).Encode(ctldapi.PublishRootFSSnapshotResponse{
				Published: true,
				Descriptor: ctldapi.RootFSDiffDescriptor{
					MediaType: "application/vnd.oci.image.layer.v1.tar",
					Digest:    "sha256:stale",
					Size:      123,
					ObjectKey: "sandbox-rootfs/team-1/sandbox-1/3/sha256/stale.tar",
				},
			})
		default:
			t.Fatalf("unexpected ctld path %s", r.URL.Path)
		}
	}))
	defer ctld.Close()
	ctldURL, ctldPort := parsedTestServer(t, ctld.URL)

	pod := rootFSTestPod("pod-1", "sandbox-1", "team-1")
	pod.Status.HostIP = ctldURL.Hostname()
	k8sClient := fake.NewSimpleClientset(pod)
	deleteCalled := false
	k8sClient.PrependReactor("delete", "pods", func(action ktesting.Action) (bool, runtime.Object, error) {
		deleteCalled = true
		return true, nil, nil
	})
	svc := &SandboxService{
		k8sClient:           k8sClient,
		podLister:           newTestPodLister(t, pod),
		sandboxStore:        store,
		teamQuotaStore:      &permissiveTeamQuotaCapacityStore{},
		ctldClient:          NewCtldClient(CtldClientConfig{Timeout: time.Second}),
		config:              SandboxServiceConfig{CtldEnabled: true, CtldPort: ctldPort},
		clock:               systemTime{},
		logger:              zap.NewNop(),
		rootFSObjectDeleter: &recordingRootFSObjectDeleter{},
	}
	defer attachRootFSTestProcd(t, pod, svc, nil)()
	txnID = addRootFSTestPauseTxn(store, pod, SandboxLifecyclePhasePreparing)

	require.NoError(t, svc.CompletePausingSandboxRuntime(context.Background(), "sandbox-1"))
	assert.False(t, deleteCalled)
	assert.Nil(t, store.rootFSStates["sandbox-1"])
	assert.Equal(t, SandboxStatusRunning, store.records["sandbox-1"].Status)
	deleter := svc.rootFSObjectDeleter.(*recordingRootFSObjectDeleter)
	assert.Empty(t, deleter.keys, "request path must not race durable rootfs GC")
}

func TestPauseSandboxRuntimeSquashesRootFSWhenChainIsTooDeep(t *testing.T) {
	var savedReq ctldapi.PrepareRootFSSnapshotRequest
	ctld := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/rootfs/snapshots/prepare":
			require.NoError(t, json.NewDecoder(r.Body).Decode(&savedReq))
			_ = json.NewEncoder(w).Encode(ctldapi.PrepareRootFSSnapshotResponse{
				Handle: "handle-1",
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
					Digest:    "sha256:squashed",
					Size:      456,
				},
			})
		case "/api/v1/rootfs/snapshots/publish":
			_ = json.NewEncoder(w).Encode(ctldapi.PublishRootFSSnapshotResponse{
				Published: true,
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
					Digest:    "sha256:squashed",
					Size:      456,
					ObjectKey: "sandbox-rootfs/team-1/sandbox-1/4/sha256/squashed.tar",
				},
			})
		default:
			t.Fatalf("unexpected ctld path %s", r.URL.Path)
		}
	}))
	defer ctld.Close()
	ctldURL, ctldPort := parsedTestServer(t, ctld.URL)

	pod := rootFSTestPod("pod-1", "sandbox-1", "team-1")
	pod.Status.HostIP = ctldURL.Hostname()
	parentState := &SandboxRootFSState{
		LayerID:           "layer-8",
		SandboxID:         "sandbox-1",
		TeamID:            "team-1",
		RuntimeGeneration: 3,
		DiffDigest:        "sha256:parent",
		DiffObjectKey:     "sandbox-rootfs/team-1/sandbox-1/3/sha256/parent.tar",
	}
	for i := 1; i <= 8; i++ {
		layer := &SandboxRootFSLayer{
			ID:            "layer-" + strconv.Itoa(i),
			TeamID:        "team-1",
			DiffDigest:    "sha256:layer",
			DiffObjectKey: "rootfs/layer.tar",
			DiffSize:      1,
		}
		if i > 1 {
			layer.ParentLayerID = "layer-" + strconv.Itoa(i-1)
		}
		parentState.LayerChain = append(parentState.LayerChain, layer)
	}
	store := &memorySandboxStore{
		records: map[string]*SandboxRecord{
			"sandbox-1": {
				ID:                "sandbox-1",
				TeamID:            "team-1",
				RuntimeGeneration: 3,
				Status:            SandboxStatusRunning,
			},
		},
		rootFSStates: map[string]*SandboxRootFSState{
			"sandbox-1": parentState,
		},
	}
	svc := &SandboxService{
		k8sClient:      fake.NewSimpleClientset(pod),
		podLister:      newTestPodLister(t, pod),
		sandboxStore:   store,
		teamQuotaStore: &permissiveTeamQuotaCapacityStore{},
		ctldClient:     NewCtldClient(CtldClientConfig{Timeout: time.Second}),
		config: SandboxServiceConfig{
			CtldEnabled:               true,
			CtldPort:                  ctldPort,
			RootFSSquashMaxChainDepth: 8,
		},
		clock:  systemTime{},
		logger: zap.NewNop(),
	}
	defer attachRootFSTestProcd(t, pod, svc, nil)()
	addRootFSTestPauseTxn(store, pod, SandboxLifecyclePhasePreparing)

	require.NoError(t, svc.CompletePausingSandboxRuntime(context.Background(), "sandbox-1"))

	assert.Empty(t, savedReq.ParentLayerID)
	state := store.rootFSStates["sandbox-1"]
	require.NotNil(t, state)
	assert.NotEmpty(t, state.LayerID)
	assert.Empty(t, state.ParentLayerID)
	assert.Equal(t, "layer-8", state.ExpectedHeadLayerID)
	assert.Equal(t, "sha256:squashed", state.DiffDigest)
}

func TestPauseSandboxRuntimeFallsBackToRootLayerWhenBaselineIsMissing(t *testing.T) {
	var saveRequests []ctldapi.PrepareRootFSSnapshotRequest
	ctld := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/rootfs/snapshots/prepare":
			var req ctldapi.PrepareRootFSSnapshotRequest
			require.NoError(t, json.NewDecoder(r.Body).Decode(&req))
			saveRequests = append(saveRequests, req)
			if req.ParentLayerID != "" {
				w.WriteHeader(http.StatusNotFound)
				_ = json.NewEncoder(w).Encode(ctldapi.PrepareRootFSSnapshotResponse{Error: "create rootfs diff: rootfs baseline layer-parent is not captured"})
				return
			}
			_ = json.NewEncoder(w).Encode(ctldapi.PrepareRootFSSnapshotResponse{
				Handle: "handle-2",
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
					Digest:    "sha256:full",
					Size:      456,
				},
			})
		case "/api/v1/rootfs/snapshots/publish":
			_ = json.NewEncoder(w).Encode(ctldapi.PublishRootFSSnapshotResponse{
				Published: true,
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
					Digest:    "sha256:full",
					Size:      456,
					ObjectKey: "sandbox-rootfs/team-1/sandbox-1/3/sha256/full.tar",
				},
			})
		default:
			t.Fatalf("unexpected ctld path %s", r.URL.Path)
		}
	}))
	defer ctld.Close()
	ctldURL, ctldPort := parsedTestServer(t, ctld.URL)

	pod := rootFSTestPod("pod-1", "sandbox-1", "team-1")
	pod.Status.HostIP = ctldURL.Hostname()
	store := &memorySandboxStore{
		records: map[string]*SandboxRecord{
			"sandbox-1": {
				ID:                "sandbox-1",
				TeamID:            "team-1",
				RuntimeGeneration: 3,
				Status:            SandboxStatusRunning,
			},
		},
		rootFSStates: map[string]*SandboxRootFSState{
			"sandbox-1": {
				LayerID:           "layer-parent",
				SandboxID:         "sandbox-1",
				TeamID:            "team-1",
				RuntimeGeneration: 2,
				DiffDigest:        "sha256:parent",
				DiffObjectKey:     "sandbox-rootfs/team-1/sandbox-1/2/sha256/parent.tar",
			},
		},
	}
	svc := &SandboxService{
		k8sClient:      fake.NewSimpleClientset(pod),
		podLister:      newTestPodLister(t, pod),
		sandboxStore:   store,
		teamQuotaStore: &permissiveTeamQuotaCapacityStore{},
		ctldClient:     NewCtldClient(CtldClientConfig{Timeout: time.Second}),
		config:         SandboxServiceConfig{CtldEnabled: true, CtldPort: ctldPort},
		clock:          systemTime{},
		logger:         zap.NewNop(),
	}
	defer attachRootFSTestProcd(t, pod, svc, nil)()
	addRootFSTestPauseTxn(store, pod, SandboxLifecyclePhasePreparing)

	require.NoError(t, svc.CompletePausingSandboxRuntime(context.Background(), "sandbox-1"))

	require.Len(t, saveRequests, 2)
	assert.Equal(t, "layer-parent", saveRequests[0].ParentLayerID)
	assert.Empty(t, saveRequests[1].ParentLayerID)
	state := store.rootFSStates["sandbox-1"]
	require.NotNil(t, state)
	assert.NotEmpty(t, state.LayerID)
	assert.Empty(t, state.ParentLayerID)
	assert.Equal(t, "layer-parent", state.ExpectedHeadLayerID)
	assert.Equal(t, "sha256:full", state.DiffDigest)
}

func TestGetSandboxHidesRuntimeAfterPauseBarrier(t *testing.T) {
	pod := rootFSTestPod("pod-1", "sandbox-1", "team-1")
	pod.Status.PodIP = "10.0.0.10"
	store := &memorySandboxStore{
		records: map[string]*SandboxRecord{
			"sandbox-1": {
				ID:                  "sandbox-1",
				TeamID:              "team-1",
				UserID:              "user-1",
				TemplateID:          "template-1",
				CurrentPodName:      "pod-1",
				CurrentPodNamespace: "default",
				RuntimeGeneration:   3,
				Status:              SandboxStatusRunning,
			},
		},
	}
	addRootFSTestPauseTxn(store, pod, SandboxLifecyclePhaseBarriered)
	svc := &SandboxService{
		k8sClient:    fake.NewSimpleClientset(pod),
		podLister:    newTestPodLister(t, pod),
		sandboxStore: store,
		config:       SandboxServiceConfig{ProcdPort: 49983},
		clock:        systemTime{},
		logger:       zap.NewNop(),
	}

	sandbox, err := svc.GetSandbox(context.Background(), "sandbox-1")
	require.NoError(t, err)
	require.NotNil(t, sandbox)
	assert.Equal(t, SandboxStatusRunning, sandbox.Status)
	assert.False(t, sandbox.Paused)
	assert.Empty(t, sandbox.InternalAddr)
	assert.Equal(t, "pod-1", sandbox.PodName)
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
		assert.ElementsMatch(t, []string{"/workspace/data"}, req.ExcludedPaths)
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
	addRootFSTestVolumePortal(pod, "data", "/workspace/data")
	setRootFSTestClaimMounts(t, pod, []ClaimMount{{SandboxVolumeID: "vol-1", MountPoint: "/workspace/data"}})
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

	_, err := svc.finishRestoredSandboxRuntime(context.Background(), pod, record, "hot")
	require.NoError(t, err)
	assert.Equal(t, []string{"apply", "procd"}, calls)
}

func TestFinishRestoredSandboxRuntimeAppliesRootFSLayerChain(t *testing.T) {
	var applyReq ctldapi.ApplyRootFSRequest
	ctld := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/api/v1/rootfs/apply", r.URL.Path)
		require.NoError(t, json.NewDecoder(r.Body).Decode(&applyReq))
		_ = json.NewEncoder(w).Encode(ctldapi.ApplyRootFSResponse{Applied: true})
	}))
	defer ctld.Close()
	ctldURL, ctldPort := parsedTestServer(t, ctld.URL)

	procd := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/api/v1/initialize", r.URL.Path)
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
			"sandbox-1": rootFSTestLayerState(),
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

	_, err := svc.finishRestoredSandboxRuntime(context.Background(), pod, record, "hot")
	require.NoError(t, err)

	assert.Empty(t, applyReq.Descriptor.Digest)
	assert.Equal(t, "layer-child", applyReq.BaselineLayerID)
	require.Len(t, applyReq.Layers, 2)
	assert.Equal(t, "layer-parent", applyReq.Layers[0].LayerID)
	assert.Empty(t, applyReq.Layers[0].ParentLayerID)
	assert.Equal(t, "rootfs/parent.tar", applyReq.Layers[0].Descriptor.ObjectKey)
	assert.Equal(t, "layer-child", applyReq.Layers[1].LayerID)
	assert.Equal(t, "layer-parent", applyReq.Layers[1].ParentLayerID)
	assert.Equal(t, "rootfs/child.tar", applyReq.Layers[1].Descriptor.ObjectKey)
}

func TestRootFSCheckpointApplyFailureDoesNotCreateFallbackRuntime(t *testing.T) {
	ctld := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/api/v1/rootfs/apply", r.URL.Path)
		w.WriteHeader(http.StatusInternalServerError)
		require.NoError(t, json.NewEncoder(w).Encode(ctldapi.ApplyRootFSResponse{
			Error: "apply rootfs diff: simulated conflict",
		}))
	}))
	defer ctld.Close()
	ctldURL, ctldPort := parsedTestServer(t, ctld.URL)

	oldPod := rootFSTestPod("pod-current", "sandbox-1", "team-1")
	oldPod.Status.HostIP = ctldURL.Hostname()
	oldPod.Finalizers = []string{"sandbox0.ai/test-finalizer"}
	k8sClient := fake.NewSimpleClientset(oldPod)
	var deleteCalls atomic.Int64
	var createCalls atomic.Int64
	k8sClient.PrependReactor("delete", "pods", func(ktesting.Action) (bool, runtime.Object, error) {
		deleteCalls.Add(1)
		return false, nil, nil
	})
	k8sClient.PrependReactor("create", "pods", func(ktesting.Action) (bool, runtime.Object, error) {
		createCalls.Add(1)
		return false, nil, nil
	})
	rateLimiter := &recordingSandboxStartRateLimiter{
		decision: tokenbucket.Decision{Allowed: true},
	}
	state := &SandboxRootFSState{
		SandboxID:         "sandbox-1",
		TeamID:            "team-1",
		BaseImageRef:      "docker.io/library/busybox:1.36",
		BaseImageDigest:   "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		DiffDigest:        "sha256:diff",
		DiffMediaType:     "application/vnd.oci.image.layer.v1.tar",
		DiffObjectKey:     "sandbox-rootfs/team-1/sandbox-1/3/sha256/diff.tar",
		RuntimeGeneration: 3,
	}
	record := &SandboxRecord{
		ID:                "sandbox-1",
		TeamID:            "team-1",
		UserID:            "user-1",
		TemplateID:        "template-1",
		TemplateName:      "template-1",
		TemplateNamespace: "default",
		TemplateSpec: v1alpha1.SandboxTemplateSpec{
			MainContainer: v1alpha1.ContainerSpec{Image: "docker.io/library/busybox:1.37"},
		},
		RuntimeGeneration: 3,
		Status:            SandboxStatusPaused,
	}
	store := &memorySandboxStore{
		records:      map[string]*SandboxRecord{record.ID: record},
		rootFSStates: map[string]*SandboxRootFSState{record.ID: state},
	}
	svc := &SandboxService{
		k8sClient:            k8sClient,
		teamQuotaRateLimiter: rateLimiter,
		sandboxStore:         store,
		ctldClient:           NewCtldClient(CtldClientConfig{Timeout: time.Second}),
		config: SandboxServiceConfig{
			CtldEnabled: true,
			CtldPort:    ctldPort,
		},
		clock:  systemTime{},
		logger: zap.NewNop(),
	}

	restoredPod, err := svc.finishRestoredSandboxRuntime(
		context.Background(),
		oldPod,
		record,
		"hot",
	)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "simulated conflict")
	assert.Equal(t, oldPod.UID, restoredPod.UID)
	assert.Zero(t, deleteCalls.Load(), "apply failure cleanup belongs to the outer caller")
	assert.Zero(t, createCalls.Load(), "apply failure must not create a second runtime")
	assert.Zero(t, rateLimiter.calls, "no hidden replacement start may consume rate quota")
	stillPresent, getErr := k8sClient.CoreV1().Pods(oldPod.Namespace).Get(
		context.Background(),
		oldPod.Name,
		metav1.GetOptions{},
	)
	require.NoError(t, getErr)
	assert.Equal(t, oldPod.UID, stillPresent.UID)
}

func TestRestoreFailureCleanupCanSkipRootFSCheckpointPublish(t *testing.T) {
	var checkpointCalled atomic.Bool
	ctld := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v1/rootfs/snapshots/prepare" ||
			r.URL.Path == "/api/v1/rootfs/snapshots/publish" {
			checkpointCalled.Store(true)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{})
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

	assert.False(t, checkpointCalled.Load())
	assert.Equal(t, originalState.DiffObjectKey, store.rootFSStates["sandbox-1"].DiffObjectKey)
	assert.Equal(t, SandboxStatusPaused, store.records["sandbox-1"].Status)
}

func TestRootFSExcludedPathsForPodUsesBoundClaimMountPaths(t *testing.T) {
	pod := rootFSTestPod("pod-1", "sandbox-1", "team-1")
	addRootFSTestVolumePortal(pod, "data", "/workspace/data/")
	addRootFSTestVolumePortal(pod, "data-duplicate", "/workspace/data")
	addRootFSTestVolumePortal(pod, "database", "/workspace/database")
	addRootFSTestVolumePortal(pod, "ignored-root", "/")
	setRootFSTestClaimMounts(t, pod, []ClaimMount{
		{SandboxVolumeID: "vol-1", MountPoint: "/workspace/data/"},
		{SandboxVolumeID: "vol-2", MountPoint: "/workspace/database"},
	})
	pod.Annotations[controller.AnnotationWebhookStateVolumeID] = "webhook-state-vol-1"
	pod.Spec.Volumes = append(pod.Spec.Volumes, corev1.Volume{
		Name: "ignored-relative",
		VolumeSource: corev1.VolumeSource{
			CSI: &corev1.CSIVolumeSource{
				Driver: volumeportal.DriverName,
				VolumeAttributes: map[string]string{
					volumeportal.AttributePortalName: "ignored-relative",
					volumeportal.AttributeMountPath:  "workspace/relative",
				},
			},
		},
	})

	got := rootFSExcludedPathsForPod(pod)

	assert.ElementsMatch(t, []string{"/workspace/data", "/workspace/database", volumeportal.WebhookStateMountPath}, got)
}

func TestRootFSExcludedPathsForPodIgnoresUnboundVolumePortals(t *testing.T) {
	pod := rootFSTestPod("pod-1", "sandbox-1", "team-1")
	addRootFSTestVolumePortal(pod, "data", "/workspace/data")

	got := rootFSExcludedPathsForPod(pod)

	assert.Empty(t, got)
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

func addRootFSTestVolumePortal(pod *corev1.Pod, name, mountPath string) {
	if pod == nil {
		return
	}
	portalName := volumeportal.NormalizePortalName(name, mountPath)
	volumeName := "volume-" + portalName
	pod.Spec.Volumes = append(pod.Spec.Volumes, corev1.Volume{
		Name: volumeName,
		VolumeSource: corev1.VolumeSource{
			CSI: &corev1.CSIVolumeSource{
				Driver: volumeportal.DriverName,
				VolumeAttributes: map[string]string{
					volumeportal.AttributePortalName: portalName,
					volumeportal.AttributeMountPath:  mountPath,
				},
			},
		},
	})
	for i := range pod.Spec.Containers {
		pod.Spec.Containers[i].VolumeMounts = append(pod.Spec.Containers[i].VolumeMounts, corev1.VolumeMount{
			Name:      volumeName,
			MountPath: mountPath,
		})
	}
}

func setRootFSTestClaimMounts(t *testing.T, pod *corev1.Pod, mounts []ClaimMount) {
	t.Helper()
	if pod.Annotations == nil {
		pod.Annotations = make(map[string]string)
	}
	require.NoError(t, setMountsAnnotation(pod.Annotations, mounts))
}

func attachRootFSTestProcd(t *testing.T, pod *corev1.Pod, svc *SandboxService, calls *[]string) func() {
	t.Helper()
	procd := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/lifecycle/barrier":
			require.Equal(t, http.MethodPut, r.Method)
			var req ProcdLifecycleBarrierRequest
			require.NoError(t, json.NewDecoder(r.Body).Decode(&req))
			if calls != nil {
				*calls = append(*calls, fmt.Sprintf("barrier:%t", req.Active))
			}
			require.NoError(t, spec.WriteSuccess(w, http.StatusOK, ProcdLifecycleBarrierResponse{
				Active:            req.Active,
				Epoch:             req.Epoch,
				RuntimeGeneration: req.RuntimeGeneration,
			}))
		case "/api/v1/sandbox/pause":
			require.Equal(t, http.MethodPost, r.Method)
			if calls != nil {
				*calls = append(*calls, "pause")
			}
			require.NoError(t, spec.WriteSuccess(w, http.StatusOK, ProcdPauseResponse{Paused: true}))
		case "/api/v1/sandbox/resume":
			require.Equal(t, http.MethodPost, r.Method)
			if calls != nil {
				*calls = append(*calls, "resume")
			}
			require.NoError(t, spec.WriteSuccess(w, http.StatusOK, ProcdResumeResponse{Resumed: true}))
		default:
			t.Fatalf("unexpected procd path %s", r.URL.Path)
		}
	}))
	procdURL, procdPort := parsedTestServer(t, procd.URL)
	pod.Status.PodIP = procdURL.Hostname()
	svc.procdClient = NewProcdClientWithHTTPClient(procd.Client())
	svc.internalTokenGenerator = staticTokenGenerator{}
	svc.config.ProcdPort = procdPort
	return procd.Close
}

func addRootFSTestPauseTxn(store *memorySandboxStore, pod *corev1.Pod, phase string) string {
	if phase == "" {
		phase = SandboxLifecyclePhasePreparing
	}
	sandboxID := sandboxIDFromPod(pod)
	txnID := "pause-txn-" + sandboxID
	if store.lifecycleTxns == nil {
		store.lifecycleTxns = make(map[string]*SandboxLifecycleTxn)
	}
	store.lifecycleTxns[txnID] = &SandboxLifecycleTxn{
		ID:               txnID,
		SandboxID:        sandboxID,
		Kind:             SandboxLifecycleKindPause,
		Phase:            phase,
		Epoch:            1,
		FromGeneration:   runtimeGenerationFromPod(pod),
		FromPodNamespace: pod.Namespace,
		FromPodName:      pod.Name,
	}
	if record := store.records[sandboxID]; record != nil {
		record.Status = SandboxStatusRunning
		record.CurrentPodNamespace = pod.Namespace
		record.CurrentPodName = pod.Name
		record.RuntimeGeneration = runtimeGenerationFromPod(pod)
		record.LifecycleEpoch = 1
	}
	return txnID
}

type recordingPauseEnqueuer struct {
	calls []string
}

func (r *recordingPauseEnqueuer) EnqueueSandboxPause(sandboxID string) {
	r.calls = append(r.calls, sandboxID)
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
		DiffID:              "sha256:diff",
		DiffMediaType:       "application/vnd.oci.image.layer.v1.tar",
		DiffSize:            123,
		DiffObjectKey:       "sandbox-rootfs/team-1/sandbox-1/3/sha256/diff.tar",
	}
}

func rootFSTestLayerState() *SandboxRootFSState {
	state := rootFSTestState()
	state.LayerID = "layer-child"
	state.ParentLayerID = "layer-parent"
	state.DiffDigest = "sha256:child"
	state.DiffObjectKey = "rootfs/child.tar"
	state.LayerChain = []*SandboxRootFSLayer{
		{
			ID:                  "layer-parent",
			SourceSandboxID:     "sandbox-1",
			TeamID:              "team-1",
			RuntimeGeneration:   2,
			Runtime:             "runc",
			RuntimeHandler:      "io.containerd.runc.v2",
			BaseImageRef:        "docker.io/library/busybox:1.36",
			BaseImageDigest:     "sha256:base",
			Snapshotter:         "overlayfs",
			SnapshotParent:      "parent-1",
			SnapshotParentChain: []string{"parent-1", "parent-0"},
			DiffDigest:          "sha256:parent",
			DiffMediaType:       "application/vnd.oci.image.layer.v1.tar",
			DiffSize:            100,
			DiffObjectKey:       "rootfs/parent.tar",
		},
		{
			ID:                  "layer-child",
			ParentLayerID:       "layer-parent",
			SourceSandboxID:     "sandbox-1",
			TeamID:              "team-1",
			RuntimeGeneration:   3,
			Runtime:             "runc",
			RuntimeHandler:      "io.containerd.runc.v2",
			BaseImageRef:        "docker.io/library/busybox:1.36",
			BaseImageDigest:     "sha256:base",
			Snapshotter:         "overlayfs",
			SnapshotParent:      "parent-1",
			SnapshotParentChain: []string{"parent-1", "parent-0"},
			DiffDigest:          "sha256:child",
			DiffMediaType:       "application/vnd.oci.image.layer.v1.tar",
			DiffSize:            123,
			DiffObjectKey:       "rootfs/child.tar",
		},
	}
	return state
}

func parsedTestServer(t *testing.T, rawURL string) (*url.URL, int) {
	t.Helper()
	parsed, err := url.Parse(rawURL)
	require.NoError(t, err)
	port, err := strconv.Atoi(parsed.Port())
	require.NoError(t, err)
	return parsed, port
}

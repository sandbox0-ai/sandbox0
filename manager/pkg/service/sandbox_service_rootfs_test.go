package service

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/controller"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	"github.com/sandbox0-ai/sandbox0/pkg/ctldapi"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	"github.com/sandbox0-ai/sandbox0/pkg/managerapi"
	"github.com/sandbox0-ai/sandbox0/pkg/naming"
	"github.com/sandbox0-ai/sandbox0/pkg/procdapi"
	"github.com/sandbox0-ai/sandbox0/pkg/runtimecontrol"
	"github.com/sandbox0-ai/sandbox0/pkg/sandboxprobe"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	corelisters "k8s.io/client-go/listers/core/v1"
	ktesting "k8s.io/client-go/testing"
)

func rootFSSnapshotTestInfo() ctldapi.RootFSInfo {
	return ctldapi.RootFSInfo{
		Runtime:             "runc",
		RuntimeHandler:      "io.containerd.runc.v2",
		BaseImageRef:        "docker.io/library/busybox:1.36",
		BaseImageDigest:     "sha256:base",
		Snapshotter:         "overlayfs",
		SnapshotParent:      "parent-1",
		SnapshotParentChain: []string{"parent-1", "parent-0"},
	}
}

func newRootFSSnapshotCTLDServer(
	t *testing.T,
	prepareResponse ctldapi.PrepareRootFSSnapshotResponse,
	publishResponse ctldapi.PublishRootFSSnapshotResponse,
	onPrepare func(ctldapi.PrepareRootFSSnapshotRequest),
	onPublish func(ctldapi.PublishRootFSSnapshotRequest),
) *httptest.Server {
	t.Helper()

	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/rootfs/snapshots/prepare":
			if onPrepare != nil {
				var req ctldapi.PrepareRootFSSnapshotRequest
				require.NoError(t, json.NewDecoder(r.Body).Decode(&req))
				onPrepare(req)
			}
			require.NoError(t, json.NewEncoder(w).Encode(prepareResponse))
		case "/api/v1/rootfs/snapshots/publish":
			if onPublish != nil {
				var req ctldapi.PublishRootFSSnapshotRequest
				require.NoError(t, json.NewDecoder(r.Body).Decode(&req))
				onPublish(req)
			}
			require.NoError(t, json.NewEncoder(w).Encode(publishResponse))
		default:
			t.Fatalf("unexpected ctld path %s", r.URL.Path)
		}
	}))
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
			assert.Empty(t, req.ExcludedPaths)
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
	markRuntimeIdentityPodReady(t, pod)
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
	store := &memorySandboxStore{records: map[string]*sandboxstore.SandboxRecord{
		"sandbox-1": {
			ID:                "sandbox-1",
			TeamID:            "team-1",
			RuntimeGeneration: 3,
			DesiredState:      sandboxstore.SandboxDesiredStateActive,
		},
	}}
	enqueuer := &recordingPauseEnqueuer{}
	svc := &SandboxService{
		k8sClient:     k8sClient,
		podLister:     newTestPodLister(t, pod),
		sandboxStore:  store,
		ctldClient:    ctldapi.NewClientWithTimeout(time.Second),
		config:        SandboxServiceConfig{CtldEnabled: true, CtldPort: ctldPort},
		clock:         systemTime{},
		logger:        zap.NewNop(),
		pauseEnqueuer: enqueuer,
	}
	var procdCalls []string
	defer attachRootFSTestProcd(t, pod, svc, &procdCalls)()

	resp, err := svc.PauseSandbox(context.Background(), "sandbox-1")
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.False(t, resp.Paused)
	assert.Equal(t, managerapi.SandboxStatusRunning, resp.Status)
	assert.False(t, saveCalled, "pause request must not synchronously save rootfs")
	assert.Equal(t, []string{"sandbox-1"}, enqueuer.calls)
	assert.Equal(t, sandboxstore.SandboxDesiredStateActive, store.records["sandbox-1"].DesiredState)
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
	assert.Equal(t, sandboxstore.SandboxDesiredStatePaused, store.records["sandbox-1"].DesiredState)
}

func TestPauseSandboxRuntimeSavesChildLayerFromParentHead(t *testing.T) {
	var savedReq ctldapi.PrepareRootFSSnapshotRequest
	ctld := newRootFSSnapshotCTLDServer(t,
		ctldapi.PrepareRootFSSnapshotResponse{
			Handle: "handle-1",
			Info:   rootFSSnapshotTestInfo(),
			Descriptor: ctldapi.RootFSDiffDescriptor{
				MediaType: "application/vnd.oci.image.layer.v1.tar",
				Digest:    "sha256:child",
				Size:      123,
			},
		},
		ctldapi.PublishRootFSSnapshotResponse{
			Published: true,
			Info:      rootFSSnapshotTestInfo(),
			Descriptor: ctldapi.RootFSDiffDescriptor{
				MediaType: "application/vnd.oci.image.layer.v1.tar",
				Digest:    "sha256:child",
				Size:      123,
				ObjectKey: "sandbox-rootfs/team-1/sandbox-1/4/sha256/child.tar",
			},
		},
		func(req ctldapi.PrepareRootFSSnapshotRequest) { savedReq = req },
		nil,
	)
	defer ctld.Close()
	ctldURL, ctldPort := parsedTestServer(t, ctld.URL)

	pod := rootFSTestPod("pod-1", "sandbox-1", "team-1")
	pod.Status.HostIP = ctldURL.Hostname()
	store := &memorySandboxStore{
		records: map[string]*sandboxstore.SandboxRecord{
			"sandbox-1": {
				ID:                "sandbox-1",
				TeamID:            "team-1",
				RuntimeGeneration: 3,
				DesiredState:      sandboxstore.SandboxDesiredStateActive,
			},
		},
		rootFSStates: map[string]*sandboxstore.SandboxRootFSState{
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
		k8sClient:    fake.NewSimpleClientset(pod),
		podLister:    newTestPodLister(t, pod),
		sandboxStore: store,
		ctldClient:   ctldapi.NewClientWithTimeout(time.Second),
		config:       SandboxServiceConfig{CtldEnabled: true, CtldPort: ctldPort},
		clock:        systemTime{},
		logger:       zap.NewNop(),
	}
	defer attachRootFSTestProcd(t, pod, svc, nil)()
	addRootFSTestPauseTxn(store, pod, sandboxstore.SandboxLifecyclePhasePreparing)

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
		records: map[string]*sandboxstore.SandboxRecord{
			"sandbox-1": {
				ID:                "sandbox-1",
				TeamID:            "team-1",
				RuntimeGeneration: 3,
				DesiredState:      sandboxstore.SandboxDesiredStateActive,
			},
		},
	}
	txnID := ""
	ctld := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/rootfs/snapshots/prepare":
			store.mu.Lock()
			store.lifecycleTxns[txnID].Phase = sandboxstore.SandboxLifecyclePhaseAborted
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
		ctldClient:          ctldapi.NewClientWithTimeout(time.Second),
		config:              SandboxServiceConfig{CtldEnabled: true, CtldPort: ctldPort},
		clock:               systemTime{},
		logger:              zap.NewNop(),
		rootFSObjectDeleter: &recordingSandboxRootFSObjectDeleter{},
	}
	defer attachRootFSTestProcd(t, pod, svc, nil)()
	txnID = addRootFSTestPauseTxn(store, pod, sandboxstore.SandboxLifecyclePhasePreparing)

	require.NoError(t, svc.CompletePausingSandboxRuntime(context.Background(), "sandbox-1"))
	assert.False(t, deleteCalled)
	assert.Nil(t, store.rootFSStates["sandbox-1"])
	assert.Equal(t, sandboxstore.SandboxDesiredStateActive, store.records["sandbox-1"].DesiredState)
	deleter := svc.rootFSObjectDeleter.(*recordingSandboxRootFSObjectDeleter)
	assert.Equal(t, []string{"sandbox-rootfs/team-1/sandbox-1/3/sha256/stale.tar"}, deleter.keys)
}

func TestPauseSandboxRuntimeSquashesRootFSWhenChainIsTooDeep(t *testing.T) {
	var savedReq ctldapi.PrepareRootFSSnapshotRequest
	ctld := newRootFSSnapshotCTLDServer(t,
		ctldapi.PrepareRootFSSnapshotResponse{
			Handle: "handle-1",
			Info:   rootFSSnapshotTestInfo(),
			Descriptor: ctldapi.RootFSDiffDescriptor{
				MediaType: "application/vnd.oci.image.layer.v1.tar",
				Digest:    "sha256:squashed",
				Size:      456,
			},
		},
		ctldapi.PublishRootFSSnapshotResponse{
			Published: true,
			Info:      rootFSSnapshotTestInfo(),
			Descriptor: ctldapi.RootFSDiffDescriptor{
				MediaType: "application/vnd.oci.image.layer.v1.tar",
				Digest:    "sha256:squashed",
				Size:      456,
				ObjectKey: "sandbox-rootfs/team-1/sandbox-1/4/sha256/squashed.tar",
			},
		},
		func(req ctldapi.PrepareRootFSSnapshotRequest) { savedReq = req },
		nil,
	)
	defer ctld.Close()
	ctldURL, ctldPort := parsedTestServer(t, ctld.URL)

	pod := rootFSTestPod("pod-1", "sandbox-1", "team-1")
	pod.Status.HostIP = ctldURL.Hostname()
	parentState := &sandboxstore.SandboxRootFSState{
		LayerID:           "layer-8",
		SandboxID:         "sandbox-1",
		TeamID:            "team-1",
		RuntimeGeneration: 3,
		DiffDigest:        "sha256:parent",
		DiffObjectKey:     "sandbox-rootfs/team-1/sandbox-1/3/sha256/parent.tar",
	}
	for i := 1; i <= 8; i++ {
		layer := &sandboxstore.SandboxRootFSLayer{
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
		records: map[string]*sandboxstore.SandboxRecord{
			"sandbox-1": {
				ID:                "sandbox-1",
				TeamID:            "team-1",
				RuntimeGeneration: 3,
				DesiredState:      sandboxstore.SandboxDesiredStateActive,
			},
		},
		rootFSStates: map[string]*sandboxstore.SandboxRootFSState{
			"sandbox-1": parentState,
		},
	}
	svc := &SandboxService{
		k8sClient:    fake.NewSimpleClientset(pod),
		podLister:    newTestPodLister(t, pod),
		sandboxStore: store,
		ctldClient:   ctldapi.NewClientWithTimeout(time.Second),
		config: SandboxServiceConfig{
			CtldEnabled:               true,
			CtldPort:                  ctldPort,
			RootFSSquashMaxChainDepth: 8,
		},
		clock:  systemTime{},
		logger: zap.NewNop(),
	}
	defer attachRootFSTestProcd(t, pod, svc, nil)()
	addRootFSTestPauseTxn(store, pod, sandboxstore.SandboxLifecyclePhasePreparing)

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
		records: map[string]*sandboxstore.SandboxRecord{
			"sandbox-1": {
				ID:                "sandbox-1",
				TeamID:            "team-1",
				RuntimeGeneration: 3,
				DesiredState:      sandboxstore.SandboxDesiredStateActive,
			},
		},
		rootFSStates: map[string]*sandboxstore.SandboxRootFSState{
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
		k8sClient:    fake.NewSimpleClientset(pod),
		podLister:    newTestPodLister(t, pod),
		sandboxStore: store,
		ctldClient:   ctldapi.NewClientWithTimeout(time.Second),
		config:       SandboxServiceConfig{CtldEnabled: true, CtldPort: ctldPort},
		clock:        systemTime{},
		logger:       zap.NewNop(),
	}
	defer attachRootFSTestProcd(t, pod, svc, nil)()
	addRootFSTestPauseTxn(store, pod, sandboxstore.SandboxLifecyclePhasePreparing)

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
	markRuntimeIdentityPodReady(t, pod)
	pod.Status.PodIP = "10.0.0.10"
	store := &memorySandboxStore{
		records: map[string]*sandboxstore.SandboxRecord{
			"sandbox-1": {
				ID:                  "sandbox-1",
				TeamID:              "team-1",
				UserID:              "user-1",
				TemplateID:          "template-1",
				CurrentPodName:      "pod-1",
				CurrentPodNamespace: "default",
				RuntimeGeneration:   3,
				DesiredState:        sandboxstore.SandboxDesiredStateActive,
			},
		},
	}
	addRootFSTestPauseTxn(store, pod, sandboxstore.SandboxLifecyclePhaseBarriered)
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
	assert.Equal(t, managerapi.SandboxStatusRunning, sandbox.Status)
	assert.False(t, sandbox.Paused)
	assert.Empty(t, sandbox.InternalAddr)
	assert.Equal(t, "pod-1", sandbox.PodName)
}

func TestFinishRestoredSandboxRuntimeAppliesRootFSBeforeRuntimeActivation(t *testing.T) {
	var calls []string
	ctld := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/rootfs/apply":
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
			assert.Empty(t, req.ExcludedPaths)
			calls = append(calls, "apply")
			_ = json.NewEncoder(w).Encode(ctldapi.ApplyRootFSResponse{Applied: true})
		default:
			t.Fatalf("unexpected CTLD path %s", r.URL.Path)
		}
	}))
	defer ctld.Close()
	ctldURL, ctldPort := parsedTestServer(t, ctld.URL)

	pod := rootFSTestPod("pod-1", "sandbox-1", "team-1")
	pod.Status.HostIP = ctldURL.Hostname()
	pod.Status.PodIP = "10.0.0.10"
	store := &memorySandboxStore{
		records: map[string]*sandboxstore.SandboxRecord{},
		rootFSStates: map[string]*sandboxstore.SandboxRootFSState{
			"sandbox-1": rootFSTestState(),
		},
	}
	indexer := newClaimTestPodIndexer(t, pod)
	client := fake.NewSimpleClientset(pod)
	installRuntimeObservationReactor(t, client, indexer, runtimecontrol.ObservedReady, func(*corev1.Pod) {
		require.Equal(t, []string{"apply"}, calls)
		calls = append(calls, "runtime")
	})
	svc := &SandboxService{
		k8sClient:    client,
		podLister:    corelisters.NewPodLister(indexer),
		sandboxStore: store,
		ctldClient:   ctldapi.NewClientWithTimeout(time.Second),
		config: SandboxServiceConfig{
			CtldEnabled:         true,
			CtldPort:            ctldPort,
			RuntimeReadyTimeout: time.Second,
		},
		clock:  systemTime{},
		logger: zap.NewNop(),
	}
	record := &sandboxstore.SandboxRecord{
		ID:                "sandbox-1",
		TeamID:            "team-1",
		UserID:            "user-1",
		TemplateID:        "template-1",
		TemplateName:      "template-1",
		TemplateNamespace: "template-default",
		TemplateSpec:      v1alpha1.SandboxTemplateSpec{},
		RuntimeGeneration: 3,
		DesiredState:      sandboxstore.SandboxDesiredStatePaused,
	}

	_, err := svc.finishRestoredSandboxRuntime(context.Background(), pod, record, "hot")
	require.NoError(t, err)
	assert.Equal(t, []string{"apply", "runtime"}, calls)
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

	pod := rootFSTestPod("pod-1", "sandbox-1", "team-1")
	pod.Status.HostIP = ctldURL.Hostname()
	pod.Status.PodIP = "10.0.0.10"
	store := &memorySandboxStore{
		records: map[string]*sandboxstore.SandboxRecord{},
		rootFSStates: map[string]*sandboxstore.SandboxRootFSState{
			"sandbox-1": rootFSTestLayerState(),
		},
	}
	indexer := newClaimTestPodIndexer(t, pod)
	client := fake.NewSimpleClientset(pod)
	installRuntimeObservationReactor(t, client, indexer, runtimecontrol.ObservedReady, nil)
	svc := &SandboxService{
		k8sClient:    client,
		podLister:    corelisters.NewPodLister(indexer),
		sandboxStore: store,
		ctldClient:   ctldapi.NewClientWithTimeout(time.Second),
		config: SandboxServiceConfig{
			CtldEnabled:         true,
			CtldPort:            ctldPort,
			RuntimeReadyTimeout: time.Second,
		},
		clock:  systemTime{},
		logger: zap.NewNop(),
	}
	record := &sandboxstore.SandboxRecord{
		ID:                "sandbox-1",
		TeamID:            "team-1",
		UserID:            "user-1",
		TemplateID:        "template-1",
		TemplateName:      "template-1",
		TemplateNamespace: "template-default",
		TemplateSpec:      v1alpha1.SandboxTemplateSpec{},
		RuntimeGeneration: 3,
		DesiredState:      sandboxstore.SandboxDesiredStatePaused,
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

func TestFinishRestoredSandboxRuntimeResetsSessionStateCopiedByFork(t *testing.T) {
	ctld := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/api/v1/rootfs/apply", r.URL.Path)
		_ = json.NewEncoder(w).Encode(ctldapi.ApplyRootFSResponse{Applied: true})
	}))
	defer ctld.Close()
	ctldURL, ctldPort := parsedTestServer(t, ctld.URL)

	const sandboxID = "forked-sandbox"
	pod := rootFSTestPod("pod-1", sandboxID, "team-1")
	pod.Status.HostIP = ctldURL.Hostname()
	pod.Status.PodIP = "10.0.0.10"
	state := rootFSTestLayerState()
	state.SandboxID = sandboxID
	for _, layer := range state.LayerChain {
		layer.SourceSandboxID = "source-sandbox"
	}
	store := &memorySandboxStore{
		records: map[string]*sandboxstore.SandboxRecord{},
		rootFSStates: map[string]*sandboxstore.SandboxRootFSState{
			sandboxID: state,
		},
	}
	indexer := newClaimTestPodIndexer(t, pod)
	client := fake.NewSimpleClientset(pod)
	installRuntimeObservationReactor(t, client, indexer, runtimecontrol.ObservedReady, func(activePod *corev1.Pod) {
		assert.Equal(t, "true", activePod.Annotations[runtimecontrol.AnnotationResetCopiedState])
	})
	svc := &SandboxService{
		k8sClient:    client,
		podLister:    corelisters.NewPodLister(indexer),
		sandboxStore: store,
		ctldClient:   ctldapi.NewClientWithTimeout(time.Second),
		config: SandboxServiceConfig{
			CtldEnabled:         true,
			CtldPort:            ctldPort,
			RuntimeReadyTimeout: time.Second,
		},
		clock:  systemTime{},
		logger: zap.NewNop(),
	}
	record := &sandboxstore.SandboxRecord{
		ID:                sandboxID,
		TeamID:            "team-1",
		UserID:            "user-1",
		TemplateID:        "template-1",
		TemplateName:      "template-1",
		TemplateNamespace: "template-default",
		TemplateSpec:      v1alpha1.SandboxTemplateSpec{},
		DesiredState:      sandboxstore.SandboxDesiredStatePaused,
	}

	_, err := svc.finishRestoredSandboxRuntime(context.Background(), pod, record, "hot")
	require.NoError(t, err)
}

func TestCopiedSessionStateRequiresResetUsesRootFSHeadProvenance(t *testing.T) {
	tests := []struct {
		name      string
		sandboxID string
		state     *sandboxstore.SandboxRootFSState
		want      bool
	}{
		{name: "missing state", sandboxID: "sandbox-1"},
		{
			name:      "own head",
			sandboxID: "sandbox-1",
			state: &sandboxstore.SandboxRootFSState{LayerChain: []*sandboxstore.SandboxRootFSLayer{
				{SourceSandboxID: "source-sandbox"},
				{SourceSandboxID: "sandbox-1"},
			}},
		},
		{
			name:      "copied head",
			sandboxID: "sandbox-1",
			state: &sandboxstore.SandboxRootFSState{LayerChain: []*sandboxstore.SandboxRootFSLayer{
				{SourceSandboxID: "source-sandbox"},
			}},
			want: true,
		},
		{
			name:      "legacy head without provenance fails closed",
			sandboxID: "sandbox-1",
			state:     &sandboxstore.SandboxRootFSState{LayerChain: []*sandboxstore.SandboxRootFSLayer{{}}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := copiedSessionStateRequiresReset(tt.sandboxID, tt.state); got != tt.want {
				t.Fatalf("copiedSessionStateRequiresReset() = %t, want %t", got, tt.want)
			}
		})
	}
}

func TestFinishRestoredSandboxRuntimeRetriesWithCheckpointBaseImage(t *testing.T) {
	withClaimTestPublicKey(t)

	const checkpointDigest = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	templateNamespace, err := naming.TemplateNamespaceForTeam("team-1")
	require.NoError(t, err)

	var applyTargets []string
	ctld := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/api/v1/rootfs/apply":
			var req ctldapi.ApplyRootFSRequest
			require.NoError(t, json.NewDecoder(r.Body).Decode(&req))
			applyTargets = append(applyTargets, req.Target.PodName)
			assert.Equal(t, checkpointDigest, req.ExpectedBaseImageDigest)
			if req.Target.PodName == "pod-current" {
				w.WriteHeader(http.StatusInternalServerError)
				_ = json.NewEncoder(w).Encode(ctldapi.ApplyRootFSResponse{Error: "apply rootfs diff: simulated conflict"})
				return
			}
			_ = json.NewEncoder(w).Encode(ctldapi.ApplyRootFSResponse{Applied: true})
		case strings.HasSuffix(r.URL.Path, "/probes/readiness"):
			_ = json.NewEncoder(w).Encode(sandboxprobe.Passed(sandboxprobe.KindReadiness, "SandboxProbePassed", "sandbox probe passed", nil))
		default:
			t.Fatalf("unexpected ctld path: %s", r.URL.Path)
		}
	}))
	defer ctld.Close()
	ctldURL, ctldPort := parsedTestServer(t, ctld.URL)

	currentPod := rootFSTestPod("pod-current", "sandbox-1", "team-1")
	currentPod.Namespace = templateNamespace
	currentPod.Status.HostIP = ctldURL.Hostname()
	currentPod.Status.PodIP = "10.0.0.10"
	indexer := newClaimTestPodIndexer(t, currentPod)
	k8sClient := fake.NewSimpleClientset(currentPod)
	var fallbackImage string
	k8sClient.PrependReactor("create", "pods", func(action ktesting.Action) (bool, runtime.Object, error) {
		pod := action.(ktesting.CreateAction).GetObject().(*corev1.Pod)
		require.Len(t, pod.Spec.Containers, 1)
		fallbackImage = pod.Spec.Containers[0].Image

		pod.UID = types.UID("fallback-uid")
		pod.Status.Phase = corev1.PodRunning
		pod.Status.HostIP = ctldURL.Hostname()
		pod.Status.PodIP = "10.0.0.11"
		pod.Status.ContainerStatuses = []corev1.ContainerStatus{{
			Name:  "procd",
			State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}},
		}}
		pod.Status.Conditions = []corev1.PodCondition{
			{Type: corev1.PodReady, Status: corev1.ConditionTrue},
			{Type: v1alpha1.SandboxPodReadinessConditionType, Status: corev1.ConditionTrue},
		}
		require.NoError(t, indexer.Add(pod.DeepCopy()))
		return false, nil, nil
	})
	installRuntimeObservationReactor(t, k8sClient, indexer, runtimecontrol.ObservedReady, func(*corev1.Pod) {
		require.Len(t, applyTargets, 2)
	})
	template := &v1alpha1.SandboxTemplate{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "template-1",
			Namespace: templateNamespace,
		},
		Spec: v1alpha1.SandboxTemplateSpec{
			MainContainer: v1alpha1.ContainerSpec{Image: "docker.io/library/busybox:1.37"},
		},
	}
	store := &memorySandboxStore{
		records: map[string]*sandboxstore.SandboxRecord{
			"sandbox-1": {
				ID:                "sandbox-1",
				TeamID:            "team-1",
				UserID:            "user-1",
				TemplateID:        "template-1",
				TemplateName:      "template-1",
				TemplateNamespace: templateNamespace,
				TemplateSpec:      template.Spec,
				RuntimeGeneration: 3,
				DesiredState:      sandboxstore.SandboxDesiredStatePaused,
			},
		},
		rootFSStates: map[string]*sandboxstore.SandboxRootFSState{
			"sandbox-1": {
				SandboxID:           "sandbox-1",
				TeamID:              "team-1",
				RuntimeGeneration:   3,
				Runtime:             "runc",
				RuntimeHandler:      "io.containerd.runc.v2",
				BaseImageRef:        "docker.io/library/busybox:1.36",
				BaseImageDigest:     checkpointDigest,
				Snapshotter:         "overlayfs",
				SnapshotParent:      "parent-1",
				SnapshotParentChain: []string{"parent-1", "parent-0"},
				DiffDigest:          "sha256:diff",
				DiffMediaType:       "application/vnd.oci.image.layer.v1.tar",
				DiffSize:            123,
				DiffObjectKey:       "sandbox-rootfs/team-1/sandbox-1/3/sha256/diff.tar",
			},
		},
	}
	svc := &SandboxService{
		k8sClient:      k8sClient,
		podLister:      corelisters.NewPodLister(indexer),
		secretLister:   newClaimTestSecretLister(t),
		templateLister: staticTemplateLister{templates: []*v1alpha1.SandboxTemplate{template}},
		sandboxStore:   store,
		ctldClient:     ctldapi.NewClientWithTimeout(time.Second),
		config: SandboxServiceConfig{
			CtldEnabled:         true,
			CtldPort:            ctldPort,
			RuntimeReadyTimeout: time.Second,
		},
		clock:  systemTime{},
		logger: zap.NewNop(),
	}
	record := store.records["sandbox-1"]

	restoredPod, err := svc.finishRestoredSandboxRuntime(context.Background(), currentPod, record, "hot")

	require.NoError(t, err)
	txn := &sandboxstore.SandboxLifecycleTxn{
		ID:             "resume-txn-sandbox-1",
		SandboxID:      "sandbox-1",
		Kind:           sandboxstore.SandboxLifecycleKindResume,
		Phase:          sandboxstore.SandboxLifecyclePhasePreparing,
		FromGeneration: 3,
		ToGeneration:   runtimeGenerationFromPod(restoredPod),
		ToPodNamespace: restoredPod.Namespace,
		ToPodName:      restoredPod.Name,
	}
	store.lifecycleTxns = map[string]*sandboxstore.SandboxLifecycleTxn{txn.ID: txn}
	require.NoError(t, svc.commitResumedSandboxRuntime(context.Background(), restoredPod, record, txn))
	require.Len(t, applyTargets, 2)
	assert.Equal(t, "pod-current", applyTargets[0])
	assert.NotEqual(t, "pod-current", applyTargets[1])
	assert.Equal(t, "docker.io/library/busybox@"+checkpointDigest, fallbackImage)
	assert.Equal(t, applyTargets[1], store.records["sandbox-1"].CurrentPodName)
	assert.Equal(t, sandboxstore.SandboxDesiredStateActive, store.records["sandbox-1"].DesiredState)
}

func TestCheckpointBaseImageRefPinsDigest(t *testing.T) {
	ref, err := checkpointBaseImageRef(&sandboxstore.SandboxRootFSState{
		BaseImageRef:    "registry.example.com:5000/team/image:old-tag",
		BaseImageDigest: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
	})

	require.NoError(t, err)
	assert.Equal(t, "registry.example.com:5000/team/image@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", ref)
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
		records: map[string]*sandboxstore.SandboxRecord{
			"sandbox-1": {
				ID:                "sandbox-1",
				TeamID:            "team-1",
				RuntimeGeneration: 3,
				DesiredState:      sandboxstore.SandboxDesiredStateActive,
			},
		},
		rootFSStates: map[string]*sandboxstore.SandboxRootFSState{
			"sandbox-1": originalState,
		},
	}
	svc := &SandboxService{
		k8sClient:    fake.NewSimpleClientset(pod),
		podLister:    newTestPodLister(t, pod),
		sandboxStore: store,
		ctldClient:   ctldapi.NewClientWithTimeout(time.Second),
		config:       SandboxServiceConfig{CtldEnabled: true, CtldPort: ctldPort},
		clock:        systemTime{},
		logger:       zap.NewNop(),
	}

	require.NoError(t, svc.pauseSandboxRuntime(context.Background(), "sandbox-1", false))

	assert.False(t, saveCalled.Load())
	assert.Equal(t, originalState.DiffObjectKey, store.rootFSStates["sandbox-1"].DiffObjectKey)
	assert.Equal(t, sandboxstore.SandboxDesiredStatePaused, store.records["sandbox-1"].DesiredState)
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

func attachRootFSTestProcd(t *testing.T, pod *corev1.Pod, svc *SandboxService, calls *[]string) func() {
	t.Helper()
	procd := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/lifecycle/barrier":
			require.Equal(t, http.MethodPut, r.Method)
			var req procdapi.ProcdLifecycleBarrierRequest
			require.NoError(t, json.NewDecoder(r.Body).Decode(&req))
			if calls != nil {
				*calls = append(*calls, fmt.Sprintf("barrier:%t", req.Active))
			}
			require.NoError(t, spec.WriteSuccess(w, http.StatusOK, procdapi.ProcdLifecycleBarrierResponse{
				Active:            req.Active,
				Epoch:             req.Epoch,
				RuntimeGeneration: req.RuntimeGeneration,
			}))
		case "/api/v1/sandbox/pause":
			require.Equal(t, http.MethodPost, r.Method)
			if calls != nil {
				*calls = append(*calls, "pause")
			}
			require.NoError(t, spec.WriteSuccess(w, http.StatusOK, procdapi.ProcdPauseResponse{Paused: true}))
		case "/api/v1/sandbox/resume":
			require.Equal(t, http.MethodPost, r.Method)
			if calls != nil {
				*calls = append(*calls, "resume")
			}
			require.NoError(t, spec.WriteSuccess(w, http.StatusOK, procdapi.ProcdResumeResponse{Resumed: true}))
		default:
			t.Fatalf("unexpected procd path %s", r.URL.Path)
		}
	}))
	procdURL, procdPort := parsedTestServer(t, procd.URL)
	pod.Status.PodIP = procdURL.Hostname()
	svc.procdClient = procdapi.NewProcdClientWithHTTPClient(procd.Client())
	svc.internalTokenGenerator = staticTokenGenerator{}
	svc.config.ProcdPort = procdPort
	return procd.Close
}

func addRootFSTestPauseTxn(store *memorySandboxStore, pod *corev1.Pod, phase string) string {
	if phase == "" {
		phase = sandboxstore.SandboxLifecyclePhasePreparing
	}
	sandboxID := sandboxPodID(pod)
	txnID := "pause-txn-" + sandboxID
	if store.lifecycleTxns == nil {
		store.lifecycleTxns = make(map[string]*sandboxstore.SandboxLifecycleTxn)
	}
	store.lifecycleTxns[txnID] = &sandboxstore.SandboxLifecycleTxn{
		ID:               txnID,
		SandboxID:        sandboxID,
		Kind:             sandboxstore.SandboxLifecycleKindPause,
		Phase:            phase,
		Epoch:            1,
		FromGeneration:   runtimeGenerationFromPod(pod),
		FromPodNamespace: pod.Namespace,
		FromPodName:      pod.Name,
	}
	if record := store.records[sandboxID]; record != nil {
		record.DesiredState = sandboxstore.SandboxDesiredStateActive
		record.CurrentPodNamespace = pod.Namespace
		record.CurrentPodName = pod.Name
		record.RuntimeGeneration = runtimeGenerationFromPod(pod)
		record.LifecycleEpoch = 1
	}
	return txnID
}

type recordingPauseEnqueuer struct {
	calls         []string
	recoveryCalls []string
}

func (r *recordingPauseEnqueuer) EnqueueSandboxPause(sandboxID string) {
	r.calls = append(r.calls, sandboxID)
}

func (r *recordingPauseEnqueuer) EnqueueSandboxRecovery(sandboxID string) {
	r.recoveryCalls = append(r.recoveryCalls, sandboxID)
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

func rootFSTestState() *sandboxstore.SandboxRootFSState {
	return &sandboxstore.SandboxRootFSState{
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

type recordingSandboxRootFSObjectDeleter struct {
	keys []string
}

func (d *recordingSandboxRootFSObjectDeleter) Delete(key string) error {
	d.keys = append(d.keys, key)
	return nil
}

func rootFSTestLayerState() *sandboxstore.SandboxRootFSState {
	state := rootFSTestState()
	state.LayerID = "layer-child"
	state.ParentLayerID = "layer-parent"
	state.DiffDigest = "sha256:child"
	state.DiffObjectKey = "rootfs/child.tar"
	state.LayerChain = []*sandboxstore.SandboxRootFSLayer{
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

package service

import (
	"context"
	"encoding/json"
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
	"github.com/sandbox0-ai/sandbox0/pkg/ctldapi"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	"github.com/sandbox0-ai/sandbox0/pkg/naming"
	"github.com/sandbox0-ai/sandbox0/pkg/sandboxprobe"
	"github.com/sandbox0-ai/sandbox0/pkg/volumeportal"
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

func TestPauseSandboxRuntimeQueuesRootFSSaveBeforeDeletingPod(t *testing.T) {
	saveCalled := false
	ctld := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/api/v1/rootfs/save", r.URL.Path)
		var req ctldapi.SaveRootFSRequest
		require.NoError(t, json.NewDecoder(r.Body).Decode(&req))
		assert.Equal(t, "sandbox-1", req.SandboxID)
		assert.Equal(t, "team-1", req.TeamID)
		assert.Equal(t, int64(3), req.ExpectedRuntimeGeneration)
		assert.Equal(t, ctldapi.RootFSContainerRef{
			Namespace:     "default",
			PodName:       "pod-1",
			PodUID:        "pod-uid",
			ContainerName: "procd",
		}, req.Target)
		assert.ElementsMatch(t, []string{"/workspace/data", volumeportal.RootFSMountPath, volumeportal.WebhookStateMountPath}, req.ExcludedPaths)
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
			Head: ctldapi.RootFSHeadDescriptor{
				Engine:        ctldapi.RootFSStorageEngineS0FS,
				TeamID:        "team-1",
				FilesystemID:  "sandbox-1",
				VolumeID:      "sandbox-1",
				ManifestKey:   "manifests/00000000000000000003.json",
				ManifestSeq:   3,
				CheckpointSeq: 1,
			},
		})
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
	enqueuer := &recordingPauseEnqueuer{}
	svc := &SandboxService{
		k8sClient:     k8sClient,
		podLister:     newTestPodLister(t, pod),
		sandboxStore:  store,
		ctldClient:    NewCtldClient(CtldClientConfig{Timeout: time.Second}),
		config:        SandboxServiceConfig{CtldEnabled: true, CtldPort: ctldPort},
		clock:         systemTime{},
		logger:        zap.NewNop(),
		pauseEnqueuer: enqueuer,
	}

	resp, err := svc.PauseSandbox(context.Background(), "sandbox-1")
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.False(t, resp.Paused)
	assert.Equal(t, SandboxStatusPausing, resp.Status)
	assert.False(t, saveCalled, "pause request must not synchronously save rootfs")
	assert.Equal(t, []string{"sandbox-1"}, enqueuer.calls)
	assert.Equal(t, SandboxStatusPausing, store.records["sandbox-1"].Status)

	require.NoError(t, svc.CompletePausingSandboxRuntime(context.Background(), "sandbox-1"))

	state := store.rootFSStates["sandbox-1"]
	require.NotNil(t, state)
	assert.Equal(t, int64(3), state.RuntimeGeneration)
	assert.Equal(t, "runc", state.Runtime)
	assert.Equal(t, "sha256:base", state.BaseImageDigest)
	assert.Equal(t, []string{"parent-1", "parent-0"}, state.SnapshotParentChain)
	assert.Equal(t, "manifests/00000000000000000003.json", state.S0FSManifestKey)
	assert.NotEmpty(t, state.LayerID)
	assert.Equal(t, SandboxStatusPaused, store.records["sandbox-1"].Status)
}

func TestPauseSandboxRuntimeSavesChildLayerFromParentHead(t *testing.T) {
	var savedReq ctldapi.SaveRootFSRequest
	ctld := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/api/v1/rootfs/save", r.URL.Path)
		require.NoError(t, json.NewDecoder(r.Body).Decode(&savedReq))
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
			Head: ctldapi.RootFSHeadDescriptor{
				Engine:        ctldapi.RootFSStorageEngineS0FS,
				TeamID:        "team-1",
				FilesystemID:  "sandbox-1",
				VolumeID:      "sandbox-1",
				ManifestKey:   "manifests/00000000000000000004.json",
				ManifestSeq:   4,
				CheckpointSeq: 2,
			},
		})
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
				Status:            SandboxStatusPausing,
			},
		},
		rootFSStates: map[string]*SandboxRootFSState{
			"sandbox-1": {
				LayerID:           "layer-parent",
				SandboxID:         "sandbox-1",
				TeamID:            "team-1",
				RuntimeGeneration: 3,
				StorageEngine:     ctldapi.RootFSStorageEngineS0FS,
				S0FSVolumeID:      "sandbox-1",
				S0FSManifestKey:   "manifests/00000000000000000003.json",
				S0FSManifestSeq:   3,
				S0FSCheckpointSeq: 1,
			},
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

	require.NoError(t, svc.CompletePausingSandboxRuntime(context.Background(), "sandbox-1"))

	assert.Equal(t, "manifests/00000000000000000003.json", savedReq.ParentHead.ManifestKey)
	state := store.rootFSStates["sandbox-1"]
	require.NotNil(t, state)
	assert.NotEmpty(t, state.LayerID)
	assert.Equal(t, "layer-parent", state.ParentLayerID)
	assert.Equal(t, "manifests/00000000000000000004.json", state.S0FSManifestKey)
}

func TestGetSandboxReportsPausingRecordWhileRuntimePodStillRunning(t *testing.T) {
	pod := rootFSTestPod("pod-1", "sandbox-1", "team-1")
	pod.Status.PodIP = "10.0.0.10"
	store := &memorySandboxStore{records: map[string]*SandboxRecord{
		"sandbox-1": {
			ID:                  "sandbox-1",
			TeamID:              "team-1",
			UserID:              "user-1",
			TemplateID:          "template-1",
			CurrentPodName:      "pod-1",
			CurrentPodNamespace: "default",
			RuntimeGeneration:   3,
			Status:              SandboxStatusPausing,
		},
	}}
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
	assert.Equal(t, SandboxStatusPausing, sandbox.Status)
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
		assert.Equal(t, ctldapi.RootFSStorageEngineS0FS, req.Head.Engine)
		assert.Equal(t, "fs-1", req.Head.VolumeID)
		assert.Equal(t, "manifests/00000000000000000007.json", req.Head.ManifestKey)
		assert.ElementsMatch(t, []string{"/workspace/data", volumeportal.RootFSMountPath}, req.ExcludedPaths)
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

func TestFinishRestoredSandboxRuntimeAppliesS0FSHead(t *testing.T) {
	var applyReq ctldapi.ApplyRootFSRequest
	var initReq InitializeRequest
	ctld := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/api/v1/rootfs/apply", r.URL.Path)
		require.NoError(t, json.NewDecoder(r.Body).Decode(&applyReq))
		_ = json.NewEncoder(w).Encode(ctldapi.ApplyRootFSResponse{
			Applied:   true,
			MountPath: "/sandbox0/rootfs",
			Head: ctldapi.RootFSHeadDescriptor{
				Engine:        ctldapi.RootFSStorageEngineS0FS,
				TeamID:        "team-1",
				FilesystemID:  "fs-1",
				VolumeID:      "fs-1",
				ManifestKey:   "manifests/00000000000000000007.json",
				ManifestSeq:   7,
				CheckpointSeq: 3,
			},
		})
	}))
	defer ctld.Close()
	ctldURL, ctldPort := parsedTestServer(t, ctld.URL)

	procd := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/api/v1/initialize", r.URL.Path)
		require.NoError(t, json.NewDecoder(r.Body).Decode(&initReq))
		require.NoError(t, spec.WriteSuccess(w, http.StatusOK, InitializeResponse{SandboxID: "sandbox-1", TeamID: "team-1"}))
	}))
	defer procd.Close()
	procdURL, procdPort := parsedTestServer(t, procd.URL)

	pod := rootFSTestPod("pod-1", "sandbox-1", "team-1")
	pod.Status.HostIP = ctldURL.Hostname()
	pod.Status.PodIP = procdURL.Hostname()
	k8sClient := fake.NewSimpleClientset(pod)
	store := &memorySandboxStore{
		records: map[string]*SandboxRecord{},
		rootFSStates: map[string]*SandboxRootFSState{
			"sandbox-1": rootFSTestS0FSState(),
		},
	}
	svc := &SandboxService{
		k8sClient:              k8sClient,
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

	assert.Equal(t, "fs-1", applyReq.FilesystemID)
	assert.Equal(t, ctldapi.RootFSStorageEngineS0FS, applyReq.Head.Engine)
	assert.Equal(t, "team-1", applyReq.Head.TeamID)
	assert.Equal(t, "fs-1", applyReq.Head.VolumeID)
	assert.Equal(t, "manifests/00000000000000000007.json", applyReq.Head.ManifestKey)
	assert.Equal(t, uint64(7), applyReq.Head.ManifestSeq)
	assert.Equal(t, uint64(3), applyReq.Head.CheckpointSeq)
	assert.Equal(t, "/sandbox0/rootfs", initReq.RootFSMountPath)

	updated, err := k8sClient.CoreV1().Pods(pod.Namespace).Get(context.Background(), pod.Name, metav1.GetOptions{})
	require.NoError(t, err)
	assert.Equal(t, "/sandbox0/rootfs", updated.Annotations[sandboxRootFSMountPathAnnotation])
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
				_ = json.NewEncoder(w).Encode(ctldapi.ApplyRootFSResponse{Error: "attach s0fs rootfs: simulated conflict"})
				return
			}
			_ = json.NewEncoder(w).Encode(ctldapi.ApplyRootFSResponse{Applied: true})
		case strings.HasSuffix(r.URL.Path, "/probes/readiness"):
			_ = json.NewEncoder(w).Encode(sandboxprobe.Passed(sandboxprobe.KindReadiness, "SandboxProbePassed", "sandbox probe passed", nil))
		case r.URL.Path == "/api/v1/volume-portals/check":
			_ = json.NewEncoder(w).Encode(ctldapi.CheckVolumePortalsResponse{Ready: true})
		default:
			t.Fatalf("unexpected ctld path: %s", r.URL.Path)
		}
	}))
	defer ctld.Close()
	ctldURL, ctldPort := parsedTestServer(t, ctld.URL)

	procd := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/api/v1/initialize", r.URL.Path)
		require.Len(t, applyTargets, 2)
		require.NoError(t, spec.WriteSuccess(w, http.StatusOK, InitializeResponse{SandboxID: "sandbox-1", TeamID: "team-1"}))
	}))
	defer procd.Close()
	procdURL, procdPort := parsedTestServer(t, procd.URL)

	currentPod := rootFSTestPod("pod-current", "sandbox-1", "team-1")
	currentPod.Namespace = templateNamespace
	currentPod.Status.HostIP = ctldURL.Hostname()
	currentPod.Status.PodIP = procdURL.Hostname()
	indexer := newClaimTestPodIndexer(t, currentPod)
	k8sClient := fake.NewSimpleClientset(currentPod)
	var fallbackImage string
	k8sClient.PrependReactor("create", "pods", func(action ktesting.Action) (bool, runtime.Object, error) {
		pod := action.(ktesting.CreateAction).GetObject().(*corev1.Pod).DeepCopy()
		require.Len(t, pod.Spec.Containers, 1)
		fallbackImage = pod.Spec.Containers[0].Image

		readyPod := pod.DeepCopy()
		readyPod.UID = types.UID("fallback-uid")
		readyPod.Status.Phase = corev1.PodRunning
		readyPod.Status.HostIP = ctldURL.Hostname()
		readyPod.Status.PodIP = procdURL.Hostname()
		readyPod.Status.ContainerStatuses = []corev1.ContainerStatus{{
			Name:  "procd",
			State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}},
		}}
		require.NoError(t, indexer.Add(readyPod))
		return false, nil, nil
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
		records: map[string]*SandboxRecord{
			"sandbox-1": {
				ID:                  "sandbox-1",
				TeamID:              "team-1",
				UserID:              "user-1",
				TemplateID:          "template-1",
				TemplateName:        "template-1",
				TemplateNamespace:   templateNamespace,
				TemplateSpec:        template.Spec,
				CurrentPodName:      "pod-current",
				CurrentPodNamespace: templateNamespace,
				RuntimeGeneration:   3,
				Status:              SandboxStatusResuming,
			},
		},
		rootFSStates: map[string]*SandboxRootFSState{
			"sandbox-1": rootFSTestS0FSStateWithBaseDigest(checkpointDigest),
		},
	}
	svc := &SandboxService{
		k8sClient:              k8sClient,
		podLister:              corelisters.NewPodLister(indexer),
		secretLister:           newClaimTestSecretLister(t),
		templateLister:         staticTemplateLister{templates: []*v1alpha1.SandboxTemplate{template}},
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
	record := store.records["sandbox-1"]

	_, err = svc.finishRestoredSandboxRuntime(context.Background(), currentPod, record, "hot")

	require.NoError(t, err)
	require.Len(t, applyTargets, 2)
	assert.Equal(t, "pod-current", applyTargets[0])
	assert.NotEqual(t, "pod-current", applyTargets[1])
	assert.Equal(t, "docker.io/library/busybox@"+checkpointDigest, fallbackImage)
	assert.Equal(t, applyTargets[1], store.records["sandbox-1"].CurrentPodName)
	assert.Equal(t, SandboxStatusRunning, store.records["sandbox-1"].Status)
}

func TestCheckpointBaseImageRefPinsDigest(t *testing.T) {
	ref, err := checkpointBaseImageRef(&SandboxRootFSState{
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
	assert.Equal(t, originalState.S0FSManifestKey, store.rootFSStates["sandbox-1"].S0FSManifestKey)
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

	assert.ElementsMatch(t, []string{"/workspace/data", "/workspace/database", volumeportal.RootFSMountPath, volumeportal.WebhookStateMountPath}, got)
}

func TestRootFSExcludedPathsForPodIgnoresUnboundVolumePortals(t *testing.T) {
	pod := rootFSTestPod("pod-1", "sandbox-1", "team-1")
	addRootFSTestVolumePortal(pod, "data", "/workspace/data")

	got := rootFSExcludedPathsForPod(pod)

	assert.Equal(t, []string{volumeportal.RootFSMountPath}, got)
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
		LayerID:             "layer-s0fs",
		RuntimeGeneration:   3,
		Runtime:             "runc",
		RuntimeHandler:      "io.containerd.runc.v2",
		BaseImageRef:        "docker.io/library/busybox:1.36",
		BaseImageDigest:     "sha256:base",
		Snapshotter:         "overlayfs",
		SnapshotParent:      "parent-1",
		SnapshotParentChain: []string{"parent-1", "parent-0"},
		StorageEngine:       ctldapi.RootFSStorageEngineS0FS,
		S0FSVolumeID:        "fs-1",
		S0FSManifestKey:     "manifests/00000000000000000007.json",
		S0FSManifestSeq:     7,
		S0FSCheckpointSeq:   3,
	}
}

func rootFSTestS0FSState() *SandboxRootFSState {
	return rootFSTestState()
}

func rootFSTestS0FSStateWithBaseDigest(baseDigest string) *SandboxRootFSState {
	state := rootFSTestState()
	state.BaseImageDigest = baseDigest
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

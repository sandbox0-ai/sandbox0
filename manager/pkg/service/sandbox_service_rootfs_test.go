package service

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	godigest "github.com/opencontainers/go-digest"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/controller"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/templateimage"
	"github.com/sandbox0-ai/sandbox0/pkg/ctldapi"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshead"
	"github.com/sandbox0-ai/sandbox0/pkg/volumeportal"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/objectstore"
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

const (
	rootFSTestBaseDigest     = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	rootFSTestDiffDigest     = "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	rootFSTestIndexDigest    = "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
	rootFSTestManifestDigest = "sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
	rootFSTestBaseSnapshot   = "sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
)

func TestPauseSandboxRuntimePublishesMetadataHeadBeforeDeletingPod(t *testing.T) {
	checkpointPublished := false
	headStore := objectstore.NewMemoryStore(t.Name())
	var checkpoint ctldapi.RootFSCheckpointDescriptor
	ctld := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/rootfs/snapshots/prepare":
			var req ctldapi.PrepareRootFSSnapshotRequest
			require.NoError(t, json.NewDecoder(r.Body).Decode(&req))
			assert.Equal(t, ctldapi.RootFSContainerRef{
				Namespace:     "default",
				PodName:       "pod-1",
				PodUID:        "pod-uid",
				ContainerName: "procd",
			}, req.Target)
			assert.ElementsMatch(t, []string{"/workspace/data", volumeportal.WebhookStateMountPath}, req.ExcludedPaths)
			var headPayload []byte
			checkpoint, headPayload = rootFSTestCheckpoint(t, req.HeadID)
			require.NoError(t, headStore.Put(checkpoint.Reference.Manifest.Key, bytes.NewReader(headPayload)))
			_ = json.NewEncoder(w).Encode(ctldapi.PrepareRootFSSnapshotResponse{
				Handle: "handle-1",
				Info: ctldapi.RootFSInfo{
					Runtime:         "runc",
					RuntimeHandler:  "io.containerd.runc.v2",
					BaseImageRef:    "docker.io/library/busybox:1.36",
					BaseImageDigest: rootFSTestBaseDigest,
					Snapshotter:     rootfshead.SnapshotterName,
					SnapshotParent:  rootFSTestBaseSnapshot,
				},
				Checkpoint: checkpoint,
			})
		case "/api/v1/rootfs/snapshots/publish":
			var req ctldapi.PublishRootFSSnapshotRequest
			require.NoError(t, json.NewDecoder(r.Body).Decode(&req))
			assert.Equal(t, "handle-1", req.Handle)
			checkpointPublished = true
			_ = json.NewEncoder(w).Encode(ctldapi.PublishRootFSSnapshotResponse{
				Published: true,
				Info: ctldapi.RootFSInfo{
					Runtime:         "runc",
					RuntimeHandler:  "io.containerd.runc.v2",
					BaseImageRef:    "docker.io/library/busybox:1.36",
					BaseImageDigest: rootFSTestBaseDigest,
					Snapshotter:     rootfshead.SnapshotterName,
					SnapshotParent:  rootFSTestBaseSnapshot,
				},
				Checkpoint: checkpoint,
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
	publisher := &recordingRootFSHeadPublisher{}
	deleteCalled := false
	k8sClient.PrependReactor("delete", "pods", func(action ktesting.Action) (bool, runtime.Object, error) {
		require.True(t, checkpointPublished, "pod delete must happen after checkpoint publication")
		require.Len(t, publisher.requests, 1, "pod delete must happen after metadata head publication")
		deleteCalled = true
		return true, nil, nil
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
		k8sClient:           k8sClient,
		podLister:           newTestPodLister(t, pod),
		sandboxStore:        store,
		ctldClient:          NewCtldClient(CtldClientConfig{Timeout: time.Second}),
		config:              SandboxServiceConfig{CtldEnabled: true, CtldPort: ctldPort},
		clock:               systemTime{},
		logger:              zap.NewNop(),
		pauseEnqueuer:       enqueuer,
		rootFSHeadPublisher: publisher,
		rootFSHeadStore:     headStore,
	}
	defer attachRootFSTestProcd(t, pod, svc, nil)()

	resp, err := svc.PauseSandbox(context.Background(), "sandbox-1")
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.False(t, resp.Paused)
	assert.Equal(t, []string{"sandbox-1"}, enqueuer.calls)

	require.NoError(t, svc.CompletePausingSandboxRuntime(context.Background(), "sandbox-1"))
	assert.True(t, deleteCalled)
	state := store.rootFSStates["sandbox-1"]
	require.NotNil(t, state)
	assert.Empty(t, state.DiffDigest)
	assert.Equal(t, checkpoint.Reference.Manifest.Digest, state.HeadObjectDigest)
	assert.NotEmpty(t, state.HeadObjectKey)
	assert.Equal(t, rootfshead.HeadMediaType, state.HeadObjectMediaType)
	assert.Equal(t, "registry.example/rootfs-heads@"+rootFSTestManifestDigest, state.HeadImageRef)
	assert.Equal(t, rootFSTestManifestDigest, state.HeadImageDigest)
	assert.Equal(t, SandboxStatusPaused, store.records["sandbox-1"].Status)

	reader, err := headStore.Get(state.HeadObjectKey, 0, -1)
	require.NoError(t, err)
	defer reader.Close()
	head, err := rootfshead.DecodeHead(reader)
	require.NoError(t, err)
	assert.Equal(t, state.LayerID, head.HeadID)
	assert.Equal(t, rootFSTestBaseSnapshot, head.BaseSnapshotKey)
	require.Len(t, publisher.requests, 1)
	assert.Equal(t, state.LayerID, publisher.requests[0].Reference.HeadID)
	markerObject, _, err := rootfshead.MarkerObject(publisher.requests[0].Reference)
	require.NoError(t, err)
	markerReader, err := headStore.Get(markerObject.Key, 0, -1)
	require.NoError(t, err)
	defer markerReader.Close()
	markerReference, err := rootfshead.DecodeMarker(markerReader)
	require.NoError(t, err)
	assert.Equal(t, publisher.requests[0].Reference, markerReference)
}

func TestAppendRootFSCheckpointLayerPreservesImmutableChain(t *testing.T) {
	parent := rootFSTestState()
	parent.LayerID = "layer-parent"
	parent.LayerChain = []*SandboxRootFSLayer{rootFSLayerFromState(parent)}
	child := rootFSTestState()
	child.LayerID = "layer-child"
	child.ParentLayerID = "layer-parent"

	chain := appendRootFSCheckpointLayer(parent, child)

	require.Len(t, chain, 2)
	assert.Equal(t, "layer-parent", chain[0].ID)
	assert.Equal(t, "layer-child", chain[1].ID)
	assert.Equal(t, "layer-parent", chain[1].ParentLayerID)
	parent.LayerChain[0].ID = "mutated"
	assert.Equal(t, "layer-parent", chain[0].ID)
}

func TestAppendRootFSCheckpointLayerCutsLegacyMigrationLineage(t *testing.T) {
	legacy := &SandboxRootFSState{
		LayerID:       "legacy-layer",
		DiffObjectKey: "rootfs/legacy.tar",
	}
	head := rootFSTestState()
	head.LayerID = "metadata-head"
	head.ParentLayerID = ""

	chain := appendRootFSCheckpointLayer(legacy, head)

	require.Len(t, chain, 1)
	assert.Equal(t, "metadata-head", chain[0].ID)
}

func TestInheritCanonicalRootFSBaseDoesNotNestMetadataHeads(t *testing.T) {
	parent := &SandboxRootFSState{
		BaseImageRef:    "docker.io/library/busybox:1.36",
		BaseImageDigest: rootFSTestBaseDigest,
	}
	next := &SandboxRootFSState{
		BaseImageRef:    "registry.example/rootfs-heads@" + rootFSTestManifestDigest,
		BaseImageDigest: rootFSTestManifestDigest,
	}

	inheritCanonicalRootFSBase(next, parent)

	assert.Equal(t, parent.BaseImageRef, next.BaseImageRef)
	assert.Equal(t, parent.BaseImageDigest, next.BaseImageDigest)
}

func TestGetSandboxHidesRuntimeAfterPauseBarrier(t *testing.T) {
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
			Status:              SandboxStatusRunning,
		},
	}}
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

func TestCopiedSessionStateRequiresResetUsesRootFSHeadProvenance(t *testing.T) {
	tests := []struct {
		name      string
		sandboxID string
		state     *SandboxRootFSState
		want      bool
	}{
		{name: "missing state", sandboxID: "sandbox-1"},
		{
			name:      "own head",
			sandboxID: "sandbox-1",
			state: &SandboxRootFSState{LayerChain: []*SandboxRootFSLayer{
				{SourceSandboxID: "source-sandbox"},
				{SourceSandboxID: "sandbox-1"},
			}},
		},
		{
			name:      "copied head",
			sandboxID: "sandbox-1",
			state:     &SandboxRootFSState{LayerChain: []*SandboxRootFSLayer{{SourceSandboxID: "source-sandbox"}}},
			want:      true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, copiedSessionStateRequiresReset(tt.sandboxID, tt.state))
		})
	}
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
			"sandbox-1": {ID: "sandbox-1", TeamID: "team-1", RuntimeGeneration: 3, Status: SandboxStatusStarting},
		},
		rootFSStates: map[string]*SandboxRootFSState{"sandbox-1": originalState},
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

func TestRootFSExcludedPathsForPodUsesBoundClaimMountPaths(t *testing.T) {
	pod := rootFSTestPod("pod-1", "sandbox-1", "team-1")
	addRootFSTestVolumePortal(pod, "data", "/workspace/data/")
	addRootFSTestVolumePortal(pod, "data-duplicate", "/workspace/data")
	addRootFSTestVolumePortal(pod, "database", "/workspace/database")
	addRootFSTestVolumePortal(pod, "tmp-volume", "/tmp/sandbox0-volume")
	setRootFSTestClaimMounts(t, pod, []ClaimMount{
		{SandboxVolumeID: "vol-1", MountPoint: "/workspace/data/"},
		{SandboxVolumeID: "vol-2", MountPoint: "/workspace/database"},
		{SandboxVolumeID: "vol-3", MountPoint: "/tmp/sandbox0-volume"},
	})
	pod.Annotations[controller.AnnotationWebhookStateVolumeID] = "webhook-state-vol-1"

	got := rootFSExcludedPathsForPod(pod)

	assert.ElementsMatch(t, []string{"/workspace/data", "/workspace/database", "/tmp/sandbox0-volume", volumeportal.WebhookStateMountPath}, got)
}

func TestRootFSExcludedPathsForPodIgnoresUnboundVolumePortals(t *testing.T) {
	pod := rootFSTestPod("pod-1", "sandbox-1", "team-1")
	addRootFSTestVolumePortal(pod, "data", "/workspace/data")
	assert.Empty(t, rootFSExcludedPathsForPod(pod))
}

type recordingRootFSHeadPublisher struct {
	requests []templateimage.HeadRequest
	err      error
}

func configureRootFSHeadTestDependencies(t *testing.T, svc *SandboxService) *recordingRootFSHeadPublisher {
	t.Helper()
	publisher := &recordingRootFSHeadPublisher{}
	svc.rootFSHeadPublisher = publisher
	svc.rootFSHeadStore = objectstore.NewMemoryStore(t.Name())
	return publisher
}

func (p *recordingRootFSHeadPublisher) PublishHead(_ context.Context, req templateimage.HeadRequest) (*templateimage.Result, error) {
	p.requests = append(p.requests, req)
	if p.err != nil {
		return nil, p.err
	}
	digest := godigest.Digest(rootFSTestManifestDigest)
	return &templateimage.Result{
		PullReference:  "registry.example/rootfs-heads@" + digest.String(),
		PushReference:  "registry.internal/rootfs-heads@" + digest.String(),
		ManifestDigest: digest,
		Platform:       req.Platform,
	}, nil
}

func rootFSTestPod(name, sandboxID, teamID string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1ObjectMeta(name, sandboxID, teamID),
		Spec: corev1.PodSpec{
			NodeName: "node-1",
			Containers: []corev1.Container{{
				Name:  "procd",
				Image: "docker.io/sandbox0/procd:latest",
			}},
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			ContainerStatuses: []corev1.ContainerStatus{{
				Name:  "procd",
				Image: "docker.io/sandbox0/procd:latest",
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
		VolumeSource: corev1.VolumeSource{CSI: &corev1.CSIVolumeSource{
			Driver: volumeportal.DriverName,
			VolumeAttributes: map[string]string{
				volumeportal.AttributePortalName: portalName,
				volumeportal.AttributeMountPath:  mountPath,
			},
		}},
	})
	for i := range pod.Spec.Containers {
		pod.Spec.Containers[i].VolumeMounts = append(pod.Spec.Containers[i].VolumeMounts, corev1.VolumeMount{Name: volumeName, MountPath: mountPath})
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
			var req ProcdLifecycleBarrierRequest
			require.NoError(t, json.NewDecoder(r.Body).Decode(&req))
			if calls != nil {
				*calls = append(*calls, fmt.Sprintf("barrier:%t", req.Active))
			}
			require.NoError(t, spec.WriteSuccess(w, http.StatusOK, ProcdLifecycleBarrierResponse{Active: req.Active, Epoch: req.Epoch, RuntimeGeneration: req.RuntimeGeneration}))
		case "/api/v1/sandbox/pause":
			if calls != nil {
				*calls = append(*calls, "pause")
			}
			require.NoError(t, spec.WriteSuccess(w, http.StatusOK, ProcdPauseResponse{Paused: true}))
		case "/api/v1/sandbox/resume":
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

func rootFSTestState() *SandboxRootFSState {
	return &SandboxRootFSState{
		SandboxID:           "sandbox-1",
		TeamID:              "team-1",
		RuntimeGeneration:   3,
		Runtime:             "runc",
		RuntimeHandler:      "io.containerd.runc.v2",
		BaseImageRef:        "docker.io/library/busybox:1.36",
		BaseImageDigest:     rootFSTestBaseDigest,
		Snapshotter:         rootfshead.SnapshotterName,
		SnapshotParent:      rootFSTestBaseSnapshot,
		HeadObjectDigest:    rootFSTestDiffDigest,
		HeadObjectMediaType: rootfshead.HeadMediaType,
		HeadObjectSize:      123,
		HeadObjectKey:       "sandbox-rootfs/cow-v2/teams/team-1/filesystems/sandbox-1/heads/sha256/head",
	}
}

func rootFSTestMetadataHeadState(sandboxID, teamID string) *SandboxRootFSState {
	state := rootFSTestState()
	state.LayerID = "layer-v1"
	state.SandboxID = sandboxID
	state.TeamID = teamID
	state.HeadObjectDigest = rootFSTestDiffDigest
	state.HeadObjectMediaType = rootfshead.HeadMediaType
	state.HeadObjectSize = 64
	state.HeadObjectKey = "sandbox-rootfs/" + teamID + "/" + sandboxID + "/heads/layer-v1/sha256/head.json.gz"
	state.HeadImageDigest = rootFSTestManifestDigest
	state.HeadImageRef = "registry.example/rootfs-heads@" + rootFSTestManifestDigest
	state.LayerChain = []*SandboxRootFSLayer{rootFSLayerFromState(state)}
	return state
}

func rootFSTestCheckpoint(t *testing.T, headID string) (ctldapi.RootFSCheckpointDescriptor, []byte) {
	t.Helper()
	directory := rootfshead.Object{
		Key:       "sandbox-rootfs/cow-v2/teams/team-1/filesystems/sandbox-1/directories/sha256/root",
		Digest:    rootFSTestIndexDigest,
		Size:      42,
		MediaType: rootfshead.DirectoryIndexMediaType,
	}
	payload, err := rootfshead.EncodeHead(rootfshead.Head{
		Version:         rootfshead.Version,
		HeadID:          headID,
		BaseImageDigest: rootFSTestBaseDigest,
		BaseSnapshotKey: rootFSTestBaseSnapshot,
		Root: rootfshead.Entry{
			Inode:     "root",
			Kind:      rootfshead.EntryDirectory,
			Mode:      0o755,
			Directory: &directory,
		},
	})
	require.NoError(t, err)
	digest := godigest.FromBytes(payload)
	manifest := rootfshead.Object{
		Key:       fmt.Sprintf("sandbox-rootfs/cow-v2/teams/team-1/filesystems/sandbox-1/heads/sha256/%s", digest.Encoded()),
		Digest:    digest.String(),
		Size:      int64(len(payload)),
		MediaType: rootfshead.HeadMediaType,
	}
	return ctldapi.RootFSCheckpointDescriptor{
		Reference: rootfshead.HeadReference{
			Version:  rootfshead.Version,
			HeadID:   headID,
			Manifest: manifest,
		},
		Objects:            []rootfshead.Object{manifest},
		CreatedBytes:       manifest.Size,
		CreatedObjectCount: 1,
	}, payload
}

func parsedTestServer(t *testing.T, rawURL string) (*url.URL, int) {
	t.Helper()
	parsed, err := url.Parse(rawURL)
	require.NoError(t, err)
	port, err := strconv.Atoi(parsed.Port())
	require.NoError(t, err)
	return parsed, port
}

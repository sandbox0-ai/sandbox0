package service

import (
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

	"github.com/opencontainers/go-digest"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/controller"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	"github.com/sandbox0-ai/sandbox0/pkg/ctldapi"
	"github.com/sandbox0-ai/sandbox0/pkg/dataplane"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	"github.com/sandbox0-ai/sandbox0/pkg/managerapi"
	"github.com/sandbox0-ai/sandbox0/pkg/procdapi"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshead"
	"github.com/sandbox0-ai/sandbox0/pkg/runtimecontrol"
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

func newRootFSHeadCTLDServer(t *testing.T, onSeal func(ctldapi.SealRootFSHeadRequest)) *httptest.Server {
	t.Helper()

	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/rootfs/heads/seal":
			var req ctldapi.SealRootFSHeadRequest
			require.NoError(t, json.NewDecoder(r.Body).Decode(&req))
			if onSeal != nil {
				onSeal(req)
			}
			require.NoError(t, json.NewEncoder(w).Encode(rootFSHeadTestSealResponse(t, req)))
		case "/api/v1/rootfs/heads/acknowledge":
			var req ctldapi.AcknowledgeRootFSHeadRequest
			require.NoError(t, json.NewDecoder(r.Body).Decode(&req))
			require.NoError(t, json.NewEncoder(w).Encode(ctldapi.AcknowledgeRootFSHeadResponse{Acknowledged: true}))
		default:
			t.Fatalf("unexpected ctld path %s", r.URL.Path)
		}
	}))
}

func rootFSHeadTestSealResponse(t *testing.T, req ctldapi.SealRootFSHeadRequest) ctldapi.SealRootFSHeadResponse {
	t.Helper()
	prefix, err := rootfshead.TeamObjectPrefix(req.TeamID)
	require.NoError(t, err)
	object := func(mediaType, payload string) rootfshead.Object {
		digestValue := digest.FromString(payload)
		key, keyErr := rootfshead.ObjectKey(prefix, mediaType, digestValue.String())
		require.NoError(t, keyErr)
		return rootfshead.Object{Key: key, Digest: digestValue.String(), Size: int64(len(payload)), MediaType: mediaType}
	}
	directory := object(rootfshead.DirectoryIndexMediaType, "directory:"+req.HeadID)
	manifest := object(rootfshead.HeadMediaType, "head:"+req.HeadID)
	base := rootfshead.BaseIdentity{
		ImageReference: "docker.io/library/busybox@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		ManifestDigest: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		ChainID:        "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		OS:             "linux", Architecture: "amd64",
	}
	reference := rootfshead.HeadReference{Version: rootfshead.Version, HeadID: req.HeadID, Manifest: manifest}
	composed, err := rootfshead.ComposeImage(prefix, reference, []byte(`{"architecture":"amd64","os":"linux","rootfs":{"type":"layers","diff_ids":[]}}`))
	require.NoError(t, err)
	return ctldapi.SealRootFSHeadResponse{
		Reference: reference,
		Head: rootfshead.Head{
			Version: rootfshead.Version,
			HeadID:  req.HeadID,
			Base:    base,
			Root:    rootfshead.Entry{Inode: "root", Kind: rootfshead.EntryDirectory, Mode: 0o040755, Nlink: 2, Directory: &directory},
		},
		Image: composed.Reference,
	}
}

func rootFSHeadTestFixture(t *testing.T, sandboxID, teamID, headID string, generation int64) *sandboxstore.SandboxRootFSHead {
	t.Helper()
	response := rootFSHeadTestSealResponse(t, ctldapi.SealRootFSHeadRequest{SandboxID: sandboxID, TeamID: teamID, HeadID: headID})
	return &sandboxstore.SandboxRootFSHead{
		SandboxID: sandboxID, SourceSandboxID: sandboxID, TeamID: teamID, RuntimeGeneration: generation,
		Reference: response.Reference, Base: response.Head.Base, Image: response.Image,
	}
}

func TestEnsureSandboxRootFSSyncRecoversLostSealAcknowledgement(t *testing.T) {
	for _, test := range []struct {
		name          string
		published     bool
		activeTxn     bool
		wantErr       bool
		wantPublished bool
	}{
		{name: "published Head", published: true, wantPublished: true},
		{name: "abandoned Head"},
		{name: "active transaction retains authority", activeTxn: true, wantErr: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			const sandboxID = "sandbox-1"
			const teamID = "team-1"
			sealed := rootFSHeadTestFixture(t, sandboxID, teamID, "sealed-head", 3)
			parent := rootFSHeadTestFixture(t, sandboxID, teamID, "parent-head", 2)
			var acknowledged *ctldapi.AcknowledgeRootFSHeadRequest
			ctld := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/api/v1/rootfs/sync/bind":
					require.Equal(t, http.MethodPut, r.Method)
					require.NoError(t, json.NewEncoder(w).Encode(ctldapi.BindRootFSSyncResponse{Status: ctldapi.RootFSSyncStatus{
						SandboxID: sandboxID, RuntimeGeneration: 3, InitialScanComplete: true,
						Sealed: true, SealedReference: &sealed.Reference,
					}}))
				case "/api/v1/rootfs/heads/acknowledge":
					require.Equal(t, http.MethodPut, r.Method)
					var req ctldapi.AcknowledgeRootFSHeadRequest
					require.NoError(t, json.NewDecoder(r.Body).Decode(&req))
					acknowledged = &req
					require.NoError(t, json.NewEncoder(w).Encode(ctldapi.AcknowledgeRootFSHeadResponse{Acknowledged: true}))
				default:
					t.Fatalf("unexpected ctld path %s", r.URL.Path)
				}
			}))
			defer ctld.Close()
			ctldURL, ctldPort := parsedTestServer(t, ctld.URL)

			pod := rootFSTestPod("pod-1", sandboxID, teamID)
			markRuntimeIdentityPodReady(t, pod)
			pod.Status.HostIP = ctldURL.Hostname()
			record := &sandboxstore.SandboxRecord{
				ID: sandboxID, TeamID: teamID, RuntimeGeneration: 3,
				CurrentPodNamespace: pod.Namespace, CurrentPodName: pod.Name,
				DesiredState: sandboxstore.SandboxDesiredStateActive,
			}
			current := parent
			if test.published {
				current = sealed
			}
			store := &memorySandboxStore{
				records:     map[string]*sandboxstore.SandboxRecord{sandboxID: record},
				rootFSHeads: map[string]*sandboxstore.SandboxRootFSHead{sandboxID: current},
			}
			if test.activeTxn {
				addRootFSTestPauseTxn(store, pod, sandboxstore.SandboxLifecyclePhasePublishing)
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

			err := svc.EnsureSandboxRootFSSync(context.Background(), sandboxID)
			if test.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), "active lifecycle transaction")
				assert.Nil(t, acknowledged)
				return
			}
			require.NoError(t, err)
			require.NotNil(t, acknowledged)
			assert.Equal(t, sealed.Reference.HeadID, acknowledged.HeadID)
			assert.Equal(t, test.wantPublished, acknowledged.Published)
			assert.True(t, acknowledged.RuntimeContinues)
		})
	}
}

func TestBindSandboxRootFSSyncWaitsThroughTransientInitialError(t *testing.T) {
	var statusCalls atomic.Int32
	ctld := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/rootfs/sync/bind":
			require.NoError(t, json.NewEncoder(w).Encode(ctldapi.BindRootFSSyncResponse{
				Status: ctldapi.RootFSSyncStatus{LastError: "reconcile: transient upper scan failure"},
			}))
		case "/api/v1/rootfs/sync/status":
			call := statusCalls.Add(1)
			status := ctldapi.RootFSSyncStatus{LastError: "reconcile: transient upper scan failure"}
			if call >= 2 {
				status = ctldapi.RootFSSyncStatus{InitialScanComplete: true}
			}
			require.NoError(t, json.NewEncoder(w).Encode(ctldapi.GetRootFSSyncStatusResponse{Status: status}))
		default:
			t.Fatalf("unexpected ctld path %s", r.URL.Path)
		}
	}))
	defer ctld.Close()
	ctldURL, ctldPort := parsedTestServer(t, ctld.URL)
	pod := rootFSTestPod("pod-initial-retry", "sandbox-1", "team-1")
	pod.Status.HostIP = ctldURL.Hostname()
	record := &sandboxstore.SandboxRecord{ID: "sandbox-1", TeamID: "team-1", RuntimeGeneration: 1}
	svc := &SandboxService{
		ctldClient: ctldapi.NewClientWithTimeout(time.Second),
		config:     SandboxServiceConfig{CtldEnabled: true, CtldPort: ctldPort},
	}

	require.NoError(t, svc.bindSandboxRootFSSync(context.Background(), pod, record))
	assert.GreaterOrEqual(t, statusCalls.Load(), int32(2))
}

func TestPrepareRootFSCheckpointAbandonsPartialSeal(t *testing.T) {
	const sandboxID = "sandbox-1"
	const teamID = "team-1"
	const headID = "head-partial"
	var acknowledged *ctldapi.AcknowledgeRootFSHeadRequest
	ctld := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/rootfs/heads/seal":
			response := rootFSHeadTestSealResponse(t, ctldapi.SealRootFSHeadRequest{SandboxID: sandboxID, TeamID: teamID, HeadID: headID})
			response.Image = rootfshead.ImageReference{}
			response.Error = "injected marker upload failure"
			w.WriteHeader(http.StatusInternalServerError)
			require.NoError(t, json.NewEncoder(w).Encode(response))
		case "/api/v1/rootfs/heads/acknowledge":
			var req ctldapi.AcknowledgeRootFSHeadRequest
			require.NoError(t, json.NewDecoder(r.Body).Decode(&req))
			acknowledged = &req
			require.NoError(t, json.NewEncoder(w).Encode(ctldapi.AcknowledgeRootFSHeadResponse{Acknowledged: true}))
		default:
			t.Fatalf("unexpected ctld path %s", r.URL.Path)
		}
	}))
	defer ctld.Close()
	ctldURL, ctldPort := parsedTestServer(t, ctld.URL)
	pod := rootFSTestPod("pod-1", sandboxID, teamID)
	pod.Status.HostIP = ctldURL.Hostname()
	record := &sandboxstore.SandboxRecord{ID: sandboxID, TeamID: teamID, RuntimeGeneration: 3}
	svc := &SandboxService{
		sandboxStore: &memorySandboxStore{records: map[string]*sandboxstore.SandboxRecord{sandboxID: record}},
		ctldClient:   ctldapi.NewClientWithTimeout(time.Second),
		config:       SandboxServiceConfig{CtldEnabled: true, CtldPort: ctldPort},
		clock:        systemTime{},
	}
	_, err := svc.prepareSandboxRootFSHeadCheckpoint(context.Background(), pod, record, headID)
	require.Error(t, err)
	require.NotNil(t, acknowledged)
	assert.Equal(t, headID, acknowledged.HeadID)
	assert.False(t, acknowledged.Published)
	assert.True(t, acknowledged.RuntimeContinues)
}

func TestPauseSandboxRuntimeQueuesRootFSSaveBeforeDeletingPod(t *testing.T) {
	saveCalled := false
	ctld := newRootFSHeadCTLDServer(t, func(req ctldapi.SealRootFSHeadRequest) {
		assert.Equal(t, "sandbox-1", req.SandboxID)
		assert.Equal(t, "team-1", req.TeamID)
		assert.Equal(t, int64(3), req.ExpectedRuntimeGeneration)
		assert.Nil(t, req.ExpectedParent)
		saveCalled = true
	})
	defer ctld.Close()
	ctldURL, ctldPort := parsedTestServer(t, ctld.URL)

	pod := rootFSTestPod("pod-1", "sandbox-1", "team-1")
	addRootFSTestVolumePortal(pod, "data", "/workspace/data")
	addRootFSTestVolumePortal(pod, volumeportal.WebhookStatePortalName, volumeportal.WebhookStateMountPath)
	setRootFSTestClaimMounts(t, pod, []managerapi.ClaimMount{{SandboxVolumeID: "vol-1", MountPoint: "/workspace/data"}})
	pod.Annotations[controller.AnnotationWebhookStateVolumeID] = "webhook-state-vol-1"
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
	head := store.rootFSHeads["sandbox-1"]
	require.NotNil(t, head)
	assert.Equal(t, int64(3), head.RuntimeGeneration)
	assert.Equal(t, "team-1", head.TeamID)
	assert.NotEmpty(t, head.Reference.HeadID)
	assert.Nil(t, head.Parent)
	assert.Equal(t, sandboxstore.SandboxDesiredStatePaused, store.records["sandbox-1"].DesiredState)
}

func TestPauseSandboxRuntimeSealsFromExpectedParentHead(t *testing.T) {
	parent := rootFSHeadTestFixture(t, "sandbox-1", "team-1", "head-parent", 2)
	var savedReq ctldapi.SealRootFSHeadRequest
	ctld := newRootFSHeadCTLDServer(t, func(req ctldapi.SealRootFSHeadRequest) { savedReq = req })
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
		rootFSHeads: map[string]*sandboxstore.SandboxRootFSHead{"sandbox-1": parent},
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

	require.NotNil(t, savedReq.ExpectedParent)
	assert.Equal(t, parent.Reference, *savedReq.ExpectedParent)
	head := store.rootFSHeads["sandbox-1"]
	require.NotNil(t, head)
	require.NotNil(t, head.Parent)
	assert.Equal(t, parent.Reference, *head.Parent)
	assert.NotEqual(t, parent.Reference.HeadID, head.Reference.HeadID)
}

func TestRootFSSnapshotterRecoveryFreezesRuntimeBeforeSeal(t *testing.T) {
	var sealed bool
	ctld := newRootFSHeadCTLDServer(t, func(ctldapi.SealRootFSHeadRequest) { sealed = true })
	defer ctld.Close()
	ctldURL, ctldPort := parsedTestServer(t, ctld.URL)

	pod := rootFSTestPod("pod-1", "sandbox-1", "team-1")
	pod.Status.HostIP = ctldURL.Hostname()
	store := &memorySandboxStore{records: map[string]*sandboxstore.SandboxRecord{
		"sandbox-1": {
			ID:                "sandbox-1",
			TeamID:            "team-1",
			UserID:            "user-1",
			RuntimeGeneration: 3,
			DesiredState:      sandboxstore.SandboxDesiredStateActive,
		},
	}}
	txnID := addRootFSTestPauseTxn(store, pod, sandboxstore.SandboxLifecyclePhasePreparing)
	store.lifecycleTxns[txnID].Source = sandboxstore.SandboxLifecycleSourceRootFS
	client := fake.NewSimpleClientset(pod.DeepCopy())
	svc := &SandboxService{
		k8sClient:    client,
		podLister:    newTestPodLister(t, pod),
		sandboxStore: store,
		ctldClient:   ctldapi.NewClientWithTimeout(time.Second),
		config:       SandboxServiceConfig{CtldEnabled: true, CtldPort: ctldPort},
		clock:        systemTime{},
		logger:       zap.NewNop(),
	}
	var procdCalls []string
	defer attachRootFSTestProcd(t, pod, svc, &procdCalls)()

	require.NoError(t, svc.CompletePausingSandboxRuntime(context.Background(), "sandbox-1"))

	assert.True(t, sealed)
	assert.Contains(t, procdCalls, "barrier:true")
	assert.Contains(t, procdCalls, "pause")
	assert.Equal(t, sandboxstore.SandboxDesiredStatePaused, store.records["sandbox-1"].DesiredState)
	assert.Equal(t, sandboxstore.SandboxLifecyclePhaseCommitted, store.lifecycleTxns[txnID].Phase)
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
	ctld := newRootFSHeadCTLDServer(t, func(ctldapi.SealRootFSHeadRequest) {
		store.mu.Lock()
		store.lifecycleTxns[txnID].Phase = sandboxstore.SandboxLifecyclePhaseAborted
		store.mu.Unlock()
	})
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
		k8sClient:    k8sClient,
		podLister:    newTestPodLister(t, pod),
		sandboxStore: store,
		ctldClient:   ctldapi.NewClientWithTimeout(time.Second),
		config:       SandboxServiceConfig{CtldEnabled: true, CtldPort: ctldPort},
		clock:        systemTime{},
		logger:       zap.NewNop(),
	}
	defer attachRootFSTestProcd(t, pod, svc, nil)()
	txnID = addRootFSTestPauseTxn(store, pod, sandboxstore.SandboxLifecyclePhasePreparing)

	require.NoError(t, svc.CompletePausingSandboxRuntime(context.Background(), "sandbox-1"))
	assert.False(t, deleteCalled)
	assert.Nil(t, store.rootFSHeads["sandbox-1"])
	assert.Equal(t, sandboxstore.SandboxDesiredStateActive, store.records["sandbox-1"].DesiredState)
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

func TestFinishRestoredSandboxRuntimeMaterializesHeadBeforeRuntimeActivation(t *testing.T) {
	withClaimTestPublicKey(t)

	for _, test := range []struct {
		name            string
		sourceSandboxID string
		wantReset       bool
	}{
		{name: "own Head", sourceSandboxID: "sandbox-1"},
		{name: "forked Head", sourceSandboxID: "source-sandbox", wantReset: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			var calls []string
			var materializeReq ctldapi.MaterializeRootFSHeadRequest
			ctld := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/api/v1/rootfs/heads/materialize":
					require.NoError(t, json.NewDecoder(r.Body).Decode(&materializeReq))
					calls = append(calls, "materialize")
					require.NoError(t, json.NewEncoder(w).Encode(ctldapi.MaterializeRootFSHeadResponse{
						Materialized: true,
						ImageName:    materializeReq.Image.Name,
					}))
				case "/api/v1/rootfs/sync/bind":
					calls = append(calls, "bind")
					require.NoError(t, json.NewEncoder(w).Encode(ctldapi.BindRootFSSyncResponse{
						Status: ctldapi.RootFSSyncStatus{InitialScanComplete: true},
					}))
				case "/api/v1/volume-portals/check":
					require.NoError(t, json.NewEncoder(w).Encode(ctldapi.CheckVolumePortalsResponse{Ready: true}))
				default:
					t.Fatalf("unexpected CTLD path %s", r.URL.Path)
				}
			}))
			defer ctld.Close()
			ctldURL, ctldPort := parsedTestServer(t, ctld.URL)

			const sandboxID = "sandbox-1"
			currentPod := rootFSTestPod("pod-current", sandboxID, "team-1")
			currentPod.Status.HostIP = ctldURL.Hostname()
			currentPod.Status.PodIP = "10.0.0.10"
			head := rootFSHeadTestFixture(t, sandboxID, "team-1", "head-v1", 3)
			head.SourceSandboxID = test.sourceSandboxID
			store := &memorySandboxStore{
				records: map[string]*sandboxstore.SandboxRecord{},
				rootFSHeads: map[string]*sandboxstore.SandboxRootFSHead{
					sandboxID: head,
				},
				rootFSHeadVersions: map[string]*sandboxstore.SandboxRootFSHead{
					head.Reference.HeadID: head,
				},
			}
			indexer := newClaimTestPodIndexer(t, currentPod)
			client := fake.NewSimpleClientset(currentPod.DeepCopy())
			snapshotterInstance := "snapshotter-pod/0/containerd://snapshotter"
			node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{
				Name: currentPod.Spec.NodeName,
				Annotations: map[string]string{
					dataplane.NodeRootFSSnapshotterInstanceAnnotation: snapshotterInstance,
				},
			}}
			client.PrependReactor("delete", "pods", func(action ktesting.Action) (bool, runtime.Object, error) {
				deleted, exists, err := indexer.GetByKey(currentPod.Namespace + "/" + action.(ktesting.DeleteAction).GetName())
				if err == nil && exists {
					_ = indexer.Delete(deleted)
				}
				return false, nil, nil
			})
			scheduleCreatedClaimPodInIndexer(t, client, indexer, func(pod *corev1.Pod) {
				require.Len(t, pod.Spec.Containers, 1)
				assert.Equal(t, head.Image.Name, pod.Spec.Containers[0].Image)
				assert.Equal(t, corev1.PullNever, pod.Spec.Containers[0].ImagePullPolicy)
				assert.Equal(t, snapshotterInstance, pod.Annotations[controller.AnnotationRootFSSnapshotterInstance])
				pod.UID = types.UID("rootfs-runtime-uid")
				pod.Status.HostIP = ctldURL.Hostname()
				pod.Status.PodIP = "10.0.0.11"
				pod.Status.ContainerStatuses = []corev1.ContainerStatus{{
					Name: "procd", Ready: true, State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}},
				}}
				pod.Status.Conditions = []corev1.PodCondition{
					{Type: corev1.PodReady, Status: corev1.ConditionTrue},
					{Type: v1alpha1.SandboxPodReadinessConditionType, Status: corev1.ConditionTrue},
				}
			})
			installRuntimeObservationReactor(t, client, indexer, runtimecontrol.ObservedReady, func(activePod *corev1.Pod) {
				require.Equal(t, []string{"materialize", "bind"}, calls)
				assert.Equal(t, test.wantReset, activePod.Annotations[runtimecontrol.AnnotationResetCopiedState] == "true")
				calls = append(calls, "runtime")
			})
			svc := &SandboxService{
				k8sClient:              client,
				podLister:              corelisters.NewPodLister(indexer),
				nodeLister:             newClaimTestNodeLister(t, node),
				secretLister:           newClaimTestSecretLister(t),
				sandboxStore:           store,
				ctldClient:             ctldapi.NewClientWithTimeout(time.Second),
				internalTokenGenerator: staticTokenGenerator{},
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
				RuntimeGeneration: 3,
				DesiredState:      sandboxstore.SandboxDesiredStatePaused,
			}

			restoredPod, err := svc.finishRestoredSandboxRuntime(context.Background(), currentPod, record, "hot")

			require.NoError(t, err)
			assert.NotEqual(t, currentPod.Name, restoredPod.Name)
			assert.Equal(t, []string{"materialize", "bind", "runtime"}, calls)
			assert.Equal(t, head.Reference.HeadID, materializeReq.Reference.HeadID)
		})
	}
}

func TestFinishRestoredSandboxRuntimeUsesTemplateBaselineWithoutPublishedHead(t *testing.T) {
	withClaimTestPublicKey(t)

	var calls []string
	ctld := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/rootfs/sync/bind":
			calls = append(calls, "bind")
			require.NoError(t, json.NewEncoder(w).Encode(ctldapi.BindRootFSSyncResponse{
				Status: ctldapi.RootFSSyncStatus{InitialScanComplete: true},
			}))
		case "/api/v1/volume-portals/check":
			require.NoError(t, json.NewEncoder(w).Encode(ctldapi.CheckVolumePortalsResponse{Ready: true}))
		case "/api/v1/rootfs/heads/materialize":
			t.Fatal("a sandbox without a published Head must resume from the template baseline")
		default:
			t.Fatalf("unexpected CTLD path %s", r.URL.Path)
		}
	}))
	defer ctld.Close()
	ctldURL, ctldPort := parsedTestServer(t, ctld.URL)

	const sandboxID = "sandbox-1"
	currentPod := rootFSTestPod("pod-current", sandboxID, "team-1")
	currentPod.Status.HostIP = ctldURL.Hostname()
	currentPod.Status.PodIP = "10.0.0.10"
	indexer := newClaimTestPodIndexer(t, currentPod)
	client := fake.NewSimpleClientset(currentPod.DeepCopy())
	installRuntimeObservationReactor(t, client, indexer, runtimecontrol.ObservedReady, func(activePod *corev1.Pod) {
		require.Equal(t, []string{"bind"}, calls)
		assert.NotEqual(t, "true", activePod.Annotations[runtimecontrol.AnnotationResetCopiedState])
		calls = append(calls, "runtime")
	})
	store := &memorySandboxStore{records: map[string]*sandboxstore.SandboxRecord{}}
	svc := &SandboxService{
		k8sClient:              client,
		podLister:              corelisters.NewPodLister(indexer),
		secretLister:           newClaimTestSecretLister(t),
		sandboxStore:           store,
		ctldClient:             ctldapi.NewClientWithTimeout(time.Second),
		internalTokenGenerator: staticTokenGenerator{},
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
		RuntimeGeneration: 3,
		DesiredState:      sandboxstore.SandboxDesiredStatePaused,
	}

	restoredPod, err := svc.finishRestoredSandboxRuntime(context.Background(), currentPod, record, "hot")

	require.NoError(t, err)
	assert.Equal(t, currentPod.Name, restoredPod.Name)
	assert.Equal(t, currentPod.UID, restoredPod.UID)
	assert.Equal(t, []string{"bind", "runtime"}, calls)
	for _, action := range client.Actions() {
		if action.GetResource().Resource == "pods" && (action.GetVerb() == "create" || action.GetVerb() == "delete") {
			t.Fatalf("unexpected pod replacement action: %s", action.GetVerb())
		}
	}
}

func TestRestoreFailureCleanupCanSkipRootFSSave(t *testing.T) {
	var sealCalled atomic.Bool
	ctld := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v1/rootfs/heads/seal" {
			sealCalled.Store(true)
		}
		_ = json.NewEncoder(w).Encode(ctldapi.SealRootFSHeadResponse{})
	}))
	defer ctld.Close()
	ctldURL, ctldPort := parsedTestServer(t, ctld.URL)

	pod := rootFSTestPod("pod-1", "sandbox-1", "team-1")
	pod.Status.HostIP = ctldURL.Hostname()
	originalHead := rootFSHeadTestFixture(t, "sandbox-1", "team-1", "head-previous", 2)
	store := &memorySandboxStore{
		records: map[string]*sandboxstore.SandboxRecord{
			"sandbox-1": {
				ID:                "sandbox-1",
				TeamID:            "team-1",
				RuntimeGeneration: 3,
				DesiredState:      sandboxstore.SandboxDesiredStateActive,
			},
		},
		rootFSHeads: map[string]*sandboxstore.SandboxRootFSHead{
			"sandbox-1": originalHead,
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

	assert.False(t, sealCalled.Load())
	assert.Equal(t, originalHead.Reference.HeadID, store.rootFSHeads["sandbox-1"].Reference.HeadID)
	assert.Equal(t, sandboxstore.SandboxDesiredStatePaused, store.records["sandbox-1"].DesiredState)
}

func TestRootFSExcludedPathsForPodUsesBoundClaimMountPaths(t *testing.T) {
	pod := rootFSTestPod("pod-1", "sandbox-1", "team-1")
	addRootFSTestVolumePortal(pod, "data", "/workspace/data/")
	addRootFSTestVolumePortal(pod, "data-duplicate", "/workspace/data")
	addRootFSTestVolumePortal(pod, "database", "/workspace/database")
	addRootFSTestVolumePortal(pod, "tmp-volume", "/tmp/sandbox0-volume")
	addRootFSTestVolumePortal(pod, "ignored-root", "/")
	setRootFSTestClaimMounts(t, pod, []managerapi.ClaimMount{
		{SandboxVolumeID: "vol-1", MountPoint: "/workspace/data/"},
		{SandboxVolumeID: "vol-2", MountPoint: "/workspace/database"},
		{SandboxVolumeID: "vol-3", MountPoint: "/tmp/sandbox0-volume"},
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

	assert.ElementsMatch(t, []string{
		"/tmp", "/procd", "/procd-image",
		"/workspace/data", "/workspace/database", "/tmp/sandbox0-volume", volumeportal.WebhookStateMountPath,
	}, got)
}

func TestRootFSExcludedPathsForPodIncludesRuntimeMountsButNotUnboundPortals(t *testing.T) {
	pod := rootFSTestPod("pod-1", "sandbox-1", "team-1")
	addRootFSTestVolumePortal(pod, "data", "/workspace/data")
	pod.Spec.Containers[0].VolumeMounts = append(pod.Spec.Containers[0].VolumeMounts, corev1.VolumeMount{
		Name: "runtime-config", MountPath: "/config",
	})

	got := rootFSExcludedPathsForPod(pod)

	assert.ElementsMatch(t, []string{"/tmp", "/procd", "/procd-image", "/config"}, got)
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

func setRootFSTestClaimMounts(t *testing.T, pod *corev1.Pod, mounts []managerapi.ClaimMount) {
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
	txn := &sandboxstore.SandboxLifecycleTxn{
		ID:               txnID,
		SandboxID:        sandboxID,
		Kind:             sandboxstore.SandboxLifecycleKindPause,
		Phase:            phase,
		Epoch:            1,
		FromGeneration:   runtimeGenerationFromPod(pod),
		FromPodNamespace: pod.Namespace,
		FromPodName:      pod.Name,
	}
	if head := store.rootFSHeads[sandboxID]; head != nil {
		txn.ExpectedHeadID = head.Reference.HeadID
	}
	store.lifecycleTxns[txnID] = txn
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

func parsedTestServer(t *testing.T, rawURL string) (*url.URL, int) {
	t.Helper()
	parsed, err := url.Parse(rawURL)
	require.NoError(t, err)
	port, err := strconv.Atoi(parsed.Port())
	require.NoError(t, err)
	return parsed, port
}

package service

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/pkg/ctldapi"
	"github.com/sandbox0-ai/sandbox0/pkg/teamquota"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/client-go/kubernetes/fake"
)

func TestSandboxRootFSProductRequiresPausedSandboxForRestore(t *testing.T) {
	now := time.Now().UTC()
	store := &memorySandboxStore{
		records: map[string]*SandboxRecord{
			"sandbox-1": rootFSProductTestRecord("sandbox-1", "team-1", SandboxStatusRunning, now),
		},
		rootFSStates: map[string]*SandboxRootFSState{
			"sandbox-1": rootFSProductTestState("sandbox-1", "team-1", "layer-1"),
		},
		rootFSSnapshots: map[string]*RootFSSnapshot{
			"snapshot-1": {
				ID:              "snapshot-1",
				FilesystemID:    "sandbox-1",
				TeamID:          "team-1",
				SourceSandboxID: "sandbox-1",
				HeadLayerID:     "layer-1",
				CreatedAt:       now,
			},
		},
	}
	svc := rootFSProductTestService(store)

	_, err := svc.CreateSandboxRootFSSnapshot(context.Background(), "sandbox-1", "team-1", nil)
	require.ErrorIs(t, err, ErrSandboxCheckpointRequiresCtld)

	_, err = svc.RestoreSandboxRootFS(context.Background(), "sandbox-1", "team-1", &RestoreSandboxRootFSRequest{SnapshotID: "snapshot-1"})
	require.ErrorIs(t, err, ErrSandboxRootFSRequiresPausedSandbox)

	_, err = svc.ForkSandbox(context.Background(), "sandbox-1", "team-1", "user-1", nil)
	require.ErrorIs(t, err, ErrSandboxCheckpointRequiresCtld)
}

func TestSandboxRootFSProductHidesInternalTemplateBuildSnapshots(t *testing.T) {
	now := time.Now().UTC()
	internalSnapshotID := "template-build-123456"
	store := &memorySandboxStore{
		records: map[string]*SandboxRecord{
			"sandbox-1": rootFSProductTestRecord("sandbox-1", "team-1", SandboxStatusPaused, now),
		},
		rootFSStates: map[string]*SandboxRootFSState{
			"sandbox-1": rootFSProductTestState("sandbox-1", "team-1", "layer-1"),
		},
		rootFSSnapshots: map[string]*RootFSSnapshot{
			"snapshot-public": {
				ID:              "snapshot-public",
				FilesystemID:    "sandbox-1",
				TeamID:          "team-1",
				SourceSandboxID: "sandbox-1",
				HeadLayerID:     "layer-1",
				CreatedAt:       now,
			},
			internalSnapshotID: {
				ID:              internalSnapshotID,
				FilesystemID:    "sandbox-1",
				TeamID:          "team-1",
				SourceSandboxID: "sandbox-1",
				HeadLayerID:     "layer-1",
				CreatedAt:       now,
			},
		},
	}
	svc := rootFSProductTestService(store)

	list, err := svc.ListSandboxRootFSSnapshots(context.Background(), "sandbox-1", "team-1")
	require.NoError(t, err)
	require.Len(t, list.Snapshots, 1)
	assert.Equal(t, "snapshot-public", list.Snapshots[0].ID)
	assert.Equal(t, 1, list.Count)

	_, err = svc.GetSandboxRootFSSnapshot(context.Background(), internalSnapshotID, "team-1")
	require.ErrorIs(t, err, ErrRootFSSnapshotNotFound)
	err = svc.DeleteSandboxRootFSSnapshot(context.Background(), internalSnapshotID, "team-1")
	require.ErrorIs(t, err, ErrRootFSSnapshotNotFound)
	_, err = svc.RestoreSandboxRootFS(
		context.Background(),
		"sandbox-1",
		"team-1",
		&RestoreSandboxRootFSRequest{SnapshotID: internalSnapshotID},
	)
	require.ErrorIs(t, err, ErrRootFSSnapshotNotFound)
	assert.Contains(t, store.rootFSSnapshots, internalSnapshotID, "public delete must retain internal build snapshot")

	internal, err := store.GetRootFSSnapshot(context.Background(), internalSnapshotID, "team-1")
	require.NoError(t, err)
	assert.Equal(t, internalSnapshotID, internal.ID, "internal store access remains available to the build worker")
}

func TestSandboxRootFSProductSnapshotsRestoresAndForksPausedSandbox(t *testing.T) {
	now := time.Now().UTC()
	autoResume := true
	store := &memorySandboxStore{
		records: map[string]*SandboxRecord{
			"sandbox-1": rootFSProductTestRecord("sandbox-1", "team-1", SandboxStatusPaused, now),
		},
		rootFSStates: map[string]*SandboxRootFSState{
			"sandbox-1": rootFSProductTestState("sandbox-1", "team-1", "layer-v1"),
		},
	}
	store.records["sandbox-1"].Config = SandboxConfig{
		AutoResume: &autoResume,
		Services: []SandboxAppService{{
			ID:      "web",
			Port:    8080,
			Ingress: SandboxAppServiceIngress{Public: true},
		}},
	}
	store.records["sandbox-1"].Mounts = []ClaimMount{{
		SandboxVolumeID: "volume-1",
		MountPoint:      "/workspace/data",
	}}
	svc := rootFSProductTestService(store)

	snapshot, err := svc.CreateSandboxRootFSSnapshot(context.Background(), "sandbox-1", "team-1", &CreateSandboxRootFSSnapshotRequest{
		Name:        "before-edit",
		Description: "state before edit",
	})
	require.NoError(t, err)
	require.NotNil(t, snapshot)
	assert.Equal(t, "sandbox-1", snapshot.SandboxID)
	assert.Equal(t, "before-edit", snapshot.Name)

	list, err := svc.ListSandboxRootFSSnapshots(context.Background(), "sandbox-1", "team-1")
	require.NoError(t, err)
	require.NotNil(t, list)
	require.Len(t, list.Snapshots, 1)
	assert.Equal(t, snapshot.ID, list.Snapshots[0].ID)

	loaded, err := svc.GetSandboxRootFSSnapshot(context.Background(), snapshot.ID, "team-1")
	require.NoError(t, err)
	assert.Equal(t, snapshot.ID, loaded.ID)

	store.rootFSStates["sandbox-1"] = rootFSProductTestState("sandbox-1", "team-1", "layer-v2")
	restoreResp, err := svc.RestoreSandboxRootFS(context.Background(), "sandbox-1", "team-1", &RestoreSandboxRootFSRequest{SnapshotID: snapshot.ID})
	require.NoError(t, err)
	assert.Equal(t, SandboxStatusPaused, restoreResp.Status)
	assert.Equal(t, "layer-v1", store.rootFSStates["sandbox-1"].LayerID)

	forkResp, err := svc.ForkSandbox(context.Background(), "sandbox-1", "team-1", "user-2", nil)
	require.NoError(t, err)
	require.NotNil(t, forkResp)
	require.NotNil(t, forkResp.Sandbox)
	assert.Equal(t, "sandbox-1", forkResp.SourceSandboxID)
	assert.NotEqual(t, "sandbox-1", forkResp.Sandbox.ID)
	assert.Equal(t, "team-1", forkResp.Sandbox.TeamID)
	assert.Equal(t, "user-2", forkResp.Sandbox.UserID)
	assert.Equal(t, SandboxStatusPaused, forkResp.Sandbox.Status)
	assert.Empty(t, forkResp.Sandbox.Mounts)
	assert.Len(t, forkResp.Sandbox.Services, 1)
	assert.Equal(t, "layer-v1", store.rootFSStates[forkResp.Sandbox.ID].LayerID)

	require.NoError(t, svc.DeleteSandboxRootFSSnapshot(context.Background(), snapshot.ID, "team-1"))
	_, err = svc.GetSandboxRootFSSnapshot(context.Background(), snapshot.ID, "team-1")
	require.ErrorIs(t, err, ErrRootFSSnapshotNotFound)
}

func TestSandboxRootFSProductSnapshotRunningSandboxCheckpointsWithoutPausingSource(t *testing.T) {
	now := time.Now().UTC()
	var prepareReq ctldapi.PrepareRootFSSnapshotRequest
	var publishReq ctldapi.PublishRootFSSnapshotRequest
	ctld := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/rootfs/snapshots/prepare":
			require.NoError(t, json.NewDecoder(r.Body).Decode(&prepareReq))
			require.NoError(t, json.NewEncoder(w).Encode(ctldapi.PrepareRootFSSnapshotResponse{
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
					Digest:    "sha256:diff-v2",
					Size:      456,
				},
			}))
		case "/api/v1/rootfs/snapshots/publish":
			require.NoError(t, json.NewDecoder(r.Body).Decode(&publishReq))
			require.NoError(t, json.NewEncoder(w).Encode(ctldapi.PublishRootFSSnapshotResponse{
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
					Digest:    "sha256:diff-v2",
					Size:      456,
					ObjectKey: "sandbox-rootfs/team-1/sandbox-1/3/sha256/diff-v2.tar",
				},
			}))
		default:
			t.Fatalf("unexpected ctld path %s", r.URL.Path)
		}
	}))
	defer ctld.Close()
	ctldURL, ctldPort := parsedTestServer(t, ctld.URL)

	pod := rootFSTestPod("pod-1", "sandbox-1", "team-1")
	pod.Status.HostIP = ctldURL.Hostname()
	source := rootFSProductTestRecord("sandbox-1", "team-1", SandboxStatusRunning, now)
	source.CurrentPodNamespace = pod.Namespace
	source.CurrentPodName = pod.Name
	source.RuntimeGeneration = runtimeGenerationFromPod(pod)
	store := &memorySandboxStore{
		records: map[string]*SandboxRecord{
			"sandbox-1": source,
		},
		rootFSStates: map[string]*SandboxRootFSState{
			"sandbox-1": rootFSProductTestState("sandbox-1", "team-1", "layer-v1"),
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
	var procdCalls []string
	defer attachRootFSTestProcd(t, pod, svc, &procdCalls)()

	snapshot, err := svc.CreateSandboxRootFSSnapshot(context.Background(), "sandbox-1", "team-1", &CreateSandboxRootFSSnapshotRequest{
		Name: "running-state",
	})

	require.NoError(t, err)
	require.NotNil(t, snapshot)
	assert.Equal(t, "sandbox-1", snapshot.SandboxID)
	assert.Equal(t, "running-state", snapshot.Name)
	assert.Equal(t, "layer-v1", prepareReq.ParentLayerID)
	assert.Equal(t, "sandbox-1", publishReq.SandboxID)
	assert.Equal(t, int64(3), publishReq.ExpectedRuntimeGeneration)
	assert.Equal(t, []string{"barrier:true", "pause", "resume", "barrier:false"}, procdCalls)

	sourceRecord := store.records["sandbox-1"]
	require.NotNil(t, sourceRecord)
	assert.Equal(t, SandboxStatusRunning, sourceRecord.Status)
	assert.Equal(t, pod.Name, sourceRecord.CurrentPodName)
	assert.Equal(t, pod.Namespace, sourceRecord.CurrentPodNamespace)
	assert.Equal(t, 0, store.pauses)

	sourceState := store.rootFSStates["sandbox-1"]
	require.NotNil(t, sourceState)
	assert.NotEqual(t, "layer-v1", sourceState.LayerID)
	assert.Equal(t, int64(3), sourceState.RuntimeGeneration)
	assert.Equal(t, "sha256:diff-v2", sourceState.DiffDigest)
	storedSnapshot := store.rootFSSnapshots[snapshot.ID]
	require.NotNil(t, storedSnapshot)
	assert.Equal(t, sourceState.LayerID, storedSnapshot.HeadLayerID)

	activeTxn, err := store.GetActiveLifecycleTxn(context.Background(), "sandbox-1")
	require.NoError(t, err)
	assert.Nil(t, activeTxn)
}

func TestSandboxRootFSProductForkRunningSandboxCheckpointsWithoutPausingSource(t *testing.T) {
	now := time.Now().UTC()
	var prepareReq ctldapi.PrepareRootFSSnapshotRequest
	var publishReq ctldapi.PublishRootFSSnapshotRequest
	ctld := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/rootfs/snapshots/prepare":
			require.NoError(t, json.NewDecoder(r.Body).Decode(&prepareReq))
			require.NoError(t, json.NewEncoder(w).Encode(ctldapi.PrepareRootFSSnapshotResponse{
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
					Digest:    "sha256:diff-v2",
					Size:      456,
				},
			}))
		case "/api/v1/rootfs/snapshots/publish":
			require.NoError(t, json.NewDecoder(r.Body).Decode(&publishReq))
			require.NoError(t, json.NewEncoder(w).Encode(ctldapi.PublishRootFSSnapshotResponse{
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
					Digest:    "sha256:diff-v2",
					Size:      456,
					ObjectKey: "sandbox-rootfs/team-1/sandbox-1/3/sha256/diff-v2.tar",
				},
			}))
		default:
			t.Fatalf("unexpected ctld path %s", r.URL.Path)
		}
	}))
	defer ctld.Close()
	ctldURL, ctldPort := parsedTestServer(t, ctld.URL)

	pod := rootFSTestPod("pod-1", "sandbox-1", "team-1")
	pod.Status.HostIP = ctldURL.Hostname()
	source := rootFSProductTestRecord("sandbox-1", "team-1", SandboxStatusRunning, now)
	source.CurrentPodNamespace = pod.Namespace
	source.CurrentPodName = pod.Name
	source.RuntimeGeneration = runtimeGenerationFromPod(pod)
	store := &memorySandboxStore{
		records: map[string]*SandboxRecord{
			"sandbox-1": source,
		},
		rootFSStates: map[string]*SandboxRootFSState{
			"sandbox-1": rootFSProductTestState("sandbox-1", "team-1", "layer-v1"),
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
	var procdCalls []string
	defer attachRootFSTestProcd(t, pod, svc, &procdCalls)()

	forkResp, err := svc.ForkSandbox(context.Background(), "sandbox-1", "team-1", "user-2", nil)

	require.NoError(t, err)
	require.NotNil(t, forkResp)
	require.NotNil(t, forkResp.Sandbox)
	assert.Equal(t, "sandbox-1", forkResp.SourceSandboxID)
	assert.Equal(t, SandboxStatusPaused, forkResp.Sandbox.Status)
	assert.Equal(t, "user-2", forkResp.Sandbox.UserID)
	assert.Equal(t, "layer-v1", prepareReq.ParentLayerID)
	assert.Equal(t, "sandbox-1", publishReq.SandboxID)
	assert.Equal(t, int64(3), publishReq.ExpectedRuntimeGeneration)
	assert.Equal(t, []string{"barrier:true", "pause", "resume", "barrier:false"}, procdCalls)

	sourceRecord := store.records["sandbox-1"]
	require.NotNil(t, sourceRecord)
	assert.Equal(t, SandboxStatusRunning, sourceRecord.Status)
	assert.Equal(t, pod.Name, sourceRecord.CurrentPodName)
	assert.Equal(t, pod.Namespace, sourceRecord.CurrentPodNamespace)
	assert.Equal(t, 0, store.pauses)

	sourceState := store.rootFSStates["sandbox-1"]
	require.NotNil(t, sourceState)
	assert.NotEqual(t, "layer-v1", sourceState.LayerID)
	assert.Equal(t, int64(3), sourceState.RuntimeGeneration)
	assert.Equal(t, "sha256:diff-v2", sourceState.DiffDigest)
	forkState := store.rootFSStates[forkResp.Sandbox.ID]
	require.NotNil(t, forkState)
	assert.Equal(t, sourceState.LayerID, forkState.LayerID)
	assert.Equal(t, "team-1", forkState.TeamID)

	activeTxn, err := store.GetActiveLifecycleTxn(context.Background(), "sandbox-1")
	require.NoError(t, err)
	assert.Nil(t, activeTxn)
}

func TestSandboxRootFSProductWaitAbortsStaleForkTxn(t *testing.T) {
	now := time.Date(2026, 7, 8, 12, 0, 0, 0, time.UTC)
	store := &memorySandboxStore{
		records: map[string]*SandboxRecord{
			"sandbox-1": rootFSProductTestRecord("sandbox-1", "team-1", SandboxStatusRunning, now),
		},
		lifecycleTxns: map[string]*SandboxLifecycleTxn{
			"txn-1": {
				ID:        "txn-1",
				SandboxID: "sandbox-1",
				Kind:      SandboxLifecycleKindFork,
				Phase:     SandboxLifecyclePhasePublishing,
				UpdatedAt: now.Add(-sandboxRootFSSourceCheckpointLifecycleStaleAfter - time.Second),
			},
		},
	}
	svc := rootFSProductTestService(store)
	svc.clock = fixedClock{now: now}

	err := svc.waitForSandboxLifecycleTxnExit(context.Background(), "sandbox-1")

	require.NoError(t, err)
	txn := store.lifecycleTxns["txn-1"]
	require.NotNil(t, txn)
	assert.Equal(t, SandboxLifecyclePhaseAborted, txn.Phase)
	assert.Equal(t, "stale fork transaction", txn.Error)
}

func TestSandboxRootFSProductWaitAbortsStaleSnapshotTxn(t *testing.T) {
	now := time.Date(2026, 7, 8, 12, 0, 0, 0, time.UTC)
	store := &memorySandboxStore{
		records: map[string]*SandboxRecord{
			"sandbox-1": rootFSProductTestRecord("sandbox-1", "team-1", SandboxStatusRunning, now),
		},
		lifecycleTxns: map[string]*SandboxLifecycleTxn{
			"txn-1": {
				ID:        "txn-1",
				SandboxID: "sandbox-1",
				Kind:      SandboxLifecycleKindSnapshot,
				Phase:     SandboxLifecyclePhasePublishing,
				UpdatedAt: now.Add(-sandboxRootFSSourceCheckpointLifecycleStaleAfter - time.Second),
			},
		},
	}
	svc := rootFSProductTestService(store)
	svc.clock = fixedClock{now: now}

	err := svc.waitForSandboxLifecycleTxnExit(context.Background(), "sandbox-1")

	require.NoError(t, err)
	txn := store.lifecycleTxns["txn-1"]
	require.NotNil(t, txn)
	assert.Equal(t, SandboxLifecyclePhaseAborted, txn.Phase)
	assert.Equal(t, "stale snapshot transaction", txn.Error)
}

func TestSandboxRootFSProductForkSetsLifecycleExpirations(t *testing.T) {
	claimedAt := time.Date(2026, 6, 18, 12, 0, 0, 0, time.UTC)
	forkedAt := claimedAt.Add(5 * time.Minute)
	source := rootFSProductTestRecord("sandbox-1", "team-1", SandboxStatusPaused, claimedAt)
	source.Config.TTL = int32Ptr(900)
	source.Config.HardTTL = int32Ptr(1800)
	source.ExpiresAt = claimedAt.Add(15 * time.Minute)
	source.HardExpiresAt = claimedAt.Add(30 * time.Minute)
	store := &memorySandboxStore{
		records: map[string]*SandboxRecord{
			"sandbox-1": source,
		},
		rootFSStates: map[string]*SandboxRootFSState{
			"sandbox-1": rootFSProductTestState("sandbox-1", "team-1", "layer-v1"),
		},
	}
	svc := rootFSProductTestService(store)
	svc.clock = fixedClock{now: forkedAt}

	forkResp, err := svc.ForkSandbox(context.Background(), "sandbox-1", "team-1", "user-2", nil)

	require.NoError(t, err)
	require.NotNil(t, forkResp)
	require.NotNil(t, forkResp.Sandbox)
	wantExpiresAt := forkedAt.Add(15 * time.Minute)
	wantHardExpiresAt := forkedAt.Add(30 * time.Minute)
	assert.Equal(t, wantExpiresAt, forkResp.Sandbox.ExpiresAt)
	assert.Equal(t, wantHardExpiresAt, forkResp.Sandbox.HardExpiresAt)
	stored := store.records[forkResp.Sandbox.ID]
	require.NotNil(t, stored)
	assert.Equal(t, wantExpiresAt, stored.ExpiresAt)
	assert.Equal(t, wantHardExpiresAt, stored.HardExpiresAt)
}

func TestSandboxRootFSProductForkRejectsIdentityQuotaBeforeTargetCommit(t *testing.T) {
	now := time.Date(2026, 7, 19, 12, 0, 0, 0, time.UTC)
	store := &memorySandboxStore{
		records: map[string]*SandboxRecord{
			"sandbox-1": rootFSProductTestRecord("sandbox-1", "team-1", SandboxStatusPaused, now),
		},
		rootFSStates: map[string]*SandboxRootFSState{
			"sandbox-1": rootFSProductTestState("sandbox-1", "team-1", "layer-v1"),
		},
	}
	quotaStore := &rejectingForkTeamQuotaCapacityStore{}
	svc := rootFSProductTestService(store)
	svc.teamQuotaStore = quotaStore

	_, err := svc.ForkSandbox(context.Background(), "sandbox-1", "team-1", "user-2", nil)

	require.ErrorIs(t, err, ErrQuotaExceeded)
	require.Len(t, quotaStore.requests, 1)
	assert.Equal(t, int64(1), quotaStore.requests[0].Target[teamquota.KeySandboxIdentityCount])
	assert.Len(t, store.records, 1)
	assert.Len(t, store.rootFSStates, 1)
}

func TestSandboxRootFSProductForkOverridesLifecycleConfig(t *testing.T) {
	claimedAt := time.Date(2026, 6, 18, 12, 0, 0, 0, time.UTC)
	forkedAt := claimedAt.Add(5 * time.Minute)
	source := rootFSProductTestRecord("sandbox-1", "team-1", SandboxStatusPaused, claimedAt)
	source.Config.TTL = int32Ptr(900)
	source.Config.HardTTL = int32Ptr(1800)
	source.ExpiresAt = claimedAt.Add(15 * time.Minute)
	source.HardExpiresAt = claimedAt.Add(30 * time.Minute)
	store := &memorySandboxStore{
		records: map[string]*SandboxRecord{
			"sandbox-1": source,
		},
		rootFSStates: map[string]*SandboxRootFSState{
			"sandbox-1": rootFSProductTestState("sandbox-1", "team-1", "layer-v1"),
		},
	}
	svc := rootFSProductTestService(store)
	svc.clock = fixedClock{now: forkedAt}
	ttl := int32(60)
	hardTTL := int32(120)

	forkResp, err := svc.ForkSandbox(context.Background(), "sandbox-1", "team-1", "user-2", &ForkSandboxRequest{
		Config: &ForkSandboxConfig{
			TTL:     &ttl,
			HardTTL: &hardTTL,
		},
	})

	require.NoError(t, err)
	require.NotNil(t, forkResp)
	require.NotNil(t, forkResp.Sandbox)
	wantExpiresAt := forkedAt.Add(time.Minute)
	wantHardExpiresAt := forkedAt.Add(2 * time.Minute)
	assert.Equal(t, wantExpiresAt, forkResp.Sandbox.ExpiresAt)
	assert.Equal(t, wantHardExpiresAt, forkResp.Sandbox.HardExpiresAt)
	stored := store.records[forkResp.Sandbox.ID]
	require.NotNil(t, stored)
	require.NotNil(t, stored.Config.TTL)
	require.NotNil(t, stored.Config.HardTTL)
	assert.Equal(t, ttl, *stored.Config.TTL)
	assert.Equal(t, hardTTL, *stored.Config.HardTTL)
	assert.Equal(t, wantExpiresAt, stored.ExpiresAt)
	assert.Equal(t, wantHardExpiresAt, stored.HardExpiresAt)
	assert.Equal(t, int32(900), *store.records["sandbox-1"].Config.TTL)
	assert.Equal(t, int32(1800), *store.records["sandbox-1"].Config.HardTTL)
}

func TestSandboxRootFSProductForkCanDisableInheritedLifecycleConfig(t *testing.T) {
	claimedAt := time.Date(2026, 6, 18, 12, 0, 0, 0, time.UTC)
	forkedAt := claimedAt.Add(5 * time.Minute)
	source := rootFSProductTestRecord("sandbox-1", "team-1", SandboxStatusPaused, claimedAt)
	source.Config.TTL = int32Ptr(900)
	source.Config.HardTTL = int32Ptr(1800)
	source.ExpiresAt = claimedAt.Add(15 * time.Minute)
	source.HardExpiresAt = claimedAt.Add(30 * time.Minute)
	store := &memorySandboxStore{
		records: map[string]*SandboxRecord{
			"sandbox-1": source,
		},
		rootFSStates: map[string]*SandboxRootFSState{
			"sandbox-1": rootFSProductTestState("sandbox-1", "team-1", "layer-v1"),
		},
	}
	svc := rootFSProductTestService(store)
	svc.clock = fixedClock{now: forkedAt}
	zero := int32(0)

	forkResp, err := svc.ForkSandbox(context.Background(), "sandbox-1", "team-1", "user-2", &ForkSandboxRequest{
		Config: &ForkSandboxConfig{
			TTL:     &zero,
			HardTTL: &zero,
		},
	})

	require.NoError(t, err)
	require.NotNil(t, forkResp)
	require.NotNil(t, forkResp.Sandbox)
	assert.True(t, forkResp.Sandbox.ExpiresAt.IsZero())
	assert.True(t, forkResp.Sandbox.HardExpiresAt.IsZero())
	stored := store.records[forkResp.Sandbox.ID]
	require.NotNil(t, stored)
	require.NotNil(t, stored.Config.TTL)
	require.NotNil(t, stored.Config.HardTTL)
	assert.Equal(t, int32(0), *stored.Config.TTL)
	assert.Equal(t, int32(0), *stored.Config.HardTTL)
	assert.True(t, stored.ExpiresAt.IsZero())
	assert.True(t, stored.HardExpiresAt.IsZero())
}

func TestSandboxRootFSProductForkRejectsInvalidLifecycleConfig(t *testing.T) {
	now := time.Date(2026, 6, 18, 12, 0, 0, 0, time.UTC)
	source := rootFSProductTestRecord("sandbox-1", "team-1", SandboxStatusPaused, now)
	store := &memorySandboxStore{
		records: map[string]*SandboxRecord{
			"sandbox-1": source,
		},
		rootFSStates: map[string]*SandboxRootFSState{
			"sandbox-1": rootFSProductTestState("sandbox-1", "team-1", "layer-v1"),
		},
	}
	svc := rootFSProductTestService(store)
	ttl := int32(120)
	hardTTL := int32(60)

	_, err := svc.ForkSandbox(context.Background(), "sandbox-1", "team-1", "user-2", &ForkSandboxRequest{
		Config: &ForkSandboxConfig{
			TTL:     &ttl,
			HardTTL: &hardTTL,
		},
	})

	require.ErrorIs(t, err, ErrInvalidClaimRequest)
	assert.Len(t, store.records, 1)
}

func TestSandboxRootFSProductForkRejectsHardExpiredSource(t *testing.T) {
	now := time.Date(2026, 6, 18, 12, 0, 0, 0, time.UTC)
	source := rootFSProductTestRecord("sandbox-1", "team-1", SandboxStatusPaused, now.Add(-time.Hour))
	source.HardExpiresAt = now.Add(-time.Second)
	store := &memorySandboxStore{
		records: map[string]*SandboxRecord{
			"sandbox-1": source,
		},
		rootFSStates: map[string]*SandboxRootFSState{
			"sandbox-1": rootFSProductTestState("sandbox-1", "team-1", "layer-v1"),
		},
	}
	svc := rootFSProductTestService(store)
	svc.clock = fixedClock{now: now}

	_, err := svc.ForkSandbox(context.Background(), "sandbox-1", "team-1", "user-2", nil)

	if !apierrors.IsNotFound(err) {
		t.Fatalf("ForkSandbox() error = %v, want not found", err)
	}
	assert.Len(t, store.records, 1)
}

func TestSandboxRootFSProductRejectsExpiredSnapshotExpiration(t *testing.T) {
	now := time.Date(2026, 6, 18, 12, 0, 0, 0, time.UTC)
	store := &memorySandboxStore{
		records: map[string]*SandboxRecord{
			"sandbox-1": rootFSProductTestRecord("sandbox-1", "team-1", SandboxStatusPaused, now),
		},
		rootFSStates: map[string]*SandboxRootFSState{
			"sandbox-1": rootFSProductTestState("sandbox-1", "team-1", "layer-1"),
		},
	}
	svc := rootFSProductTestService(store)
	svc.clock = fixedClock{now: now}

	_, err := svc.CreateSandboxRootFSSnapshot(context.Background(), "sandbox-1", "team-1", &CreateSandboxRootFSSnapshotRequest{
		Name:      "expired",
		ExpiresAt: now.Add(-time.Second),
	})

	require.ErrorIs(t, err, ErrRootFSSnapshotExpired)
	assert.Empty(t, store.rootFSSnapshots)
}

func TestSandboxRootFSProductEnforcesTeamOwnership(t *testing.T) {
	now := time.Now().UTC()
	store := &memorySandboxStore{
		records: map[string]*SandboxRecord{
			"sandbox-1": rootFSProductTestRecord("sandbox-1", "team-1", SandboxStatusPaused, now),
		},
		rootFSStates: map[string]*SandboxRootFSState{
			"sandbox-1": rootFSProductTestState("sandbox-1", "team-1", "layer-1"),
		},
	}
	svc := rootFSProductTestService(store)

	_, err := svc.CreateSandboxRootFSSnapshot(context.Background(), "sandbox-1", "team-2", nil)
	require.True(t, apierrors.IsForbidden(err), "error = %v", err)
}

func (s *memorySandboxStore) CreateRootFSSnapshot(_ context.Context, req *CreateRootFSSnapshotRequest) (*RootFSSnapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.rootFSSnapshots == nil {
		s.rootFSSnapshots = make(map[string]*RootFSSnapshot)
	}
	state := s.rootFSStates[req.SandboxID]
	if state == nil || state.LayerID == "" {
		return nil, ErrRootFSFilesystemNotFound
	}
	record := s.records[req.SandboxID]
	if record == nil {
		return nil, ErrSandboxRecordNotFound
	}
	snapshot := &RootFSSnapshot{
		ID:              req.SnapshotID,
		FilesystemID:    req.SandboxID,
		TeamID:          record.TeamID,
		SourceSandboxID: req.SandboxID,
		HeadLayerID:     state.LayerID,
		Name:            req.Name,
		Description:     req.Description,
		CreatedAt:       time.Now().UTC(),
		ExpiresAt:       req.ExpiresAt,
	}
	s.rootFSSnapshots[snapshot.ID] = cloneRootFSSnapshotForTest(snapshot)
	return cloneRootFSSnapshotForTest(snapshot), nil
}

func (s *memorySandboxStore) ListRootFSSnapshots(_ context.Context, req *ListRootFSSnapshotsRequest) ([]*RootFSSnapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var snapshots []*RootFSSnapshot
	for _, snapshot := range s.rootFSSnapshots {
		if snapshot == nil || snapshot.SourceSandboxID != req.SandboxID {
			continue
		}
		if req.TeamID != "" && snapshot.TeamID != req.TeamID {
			continue
		}
		snapshots = append(snapshots, cloneRootFSSnapshotForTest(snapshot))
	}
	return snapshots, nil
}

func (s *memorySandboxStore) GetRootFSSnapshot(_ context.Context, snapshotID, teamID string) (*RootFSSnapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	snapshot := s.rootFSSnapshots[snapshotID]
	if snapshot == nil || (teamID != "" && snapshot.TeamID != teamID) {
		return nil, ErrRootFSSnapshotNotFound
	}
	return cloneRootFSSnapshotForTest(snapshot), nil
}

func (s *memorySandboxStore) DeleteRootFSSnapshot(_ context.Context, snapshotID, teamID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	snapshot := s.rootFSSnapshots[snapshotID]
	if snapshot == nil || (teamID != "" && snapshot.TeamID != teamID) {
		return ErrRootFSSnapshotNotFound
	}
	delete(s.rootFSSnapshots, snapshotID)
	return nil
}

func (s *memorySandboxStore) DeleteRootFSSnapshotWithQuota(
	ctx context.Context,
	snapshotID string,
	teamID string,
	_ teamquota.CapacityTxStore,
) error {
	return s.DeleteRootFSSnapshot(ctx, snapshotID, teamID)
}

func (s *memorySandboxStore) ForkRootFSFilesystem(_ context.Context, req *ForkRootFSFilesystemRequest) (*RootFSFilesystem, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sourceState := s.rootFSStates[req.SourceSandboxID]
	if sourceState == nil || sourceState.LayerID == "" {
		return nil, ErrRootFSFilesystemNotFound
	}
	target := s.records[req.TargetSandboxID]
	if target == nil {
		return nil, ErrSandboxRecordNotFound
	}
	targetTeamID := req.TargetTeamID
	if targetTeamID == "" {
		targetTeamID = target.TeamID
	}
	state := cloneSandboxRootFSState(sourceState)
	state.SandboxID = req.TargetSandboxID
	state.TeamID = targetTeamID
	if s.rootFSStates == nil {
		s.rootFSStates = make(map[string]*SandboxRootFSState)
	}
	s.rootFSStates[req.TargetSandboxID] = state
	if s.rootFSFilesystems == nil {
		s.rootFSFilesystems = make(map[string]*RootFSFilesystem)
	}
	filesystem := &RootFSFilesystem{
		ID:                 req.TargetSandboxID,
		TeamID:             targetTeamID,
		SourceFilesystemID: req.SourceSandboxID,
		HeadLayerID:        sourceState.LayerID,
		CreatedAt:          time.Now().UTC(),
		UpdatedAt:          time.Now().UTC(),
	}
	s.rootFSFilesystems[filesystem.ID] = cloneRootFSFilesystemForTest(filesystem)
	return cloneRootFSFilesystemForTest(filesystem), nil
}

func (s *memorySandboxStore) RestoreRootFSFromSnapshot(_ context.Context, req *RestoreRootFSFromSnapshotRequest) (*RootFSFilesystem, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	snapshot := s.rootFSSnapshots[req.SnapshotID]
	if snapshot == nil || (req.TeamID != "" && snapshot.TeamID != req.TeamID) {
		return nil, ErrRootFSSnapshotNotFound
	}
	target := s.records[req.SandboxID]
	if target == nil {
		return nil, ErrSandboxRecordNotFound
	}
	if s.rootFSStates == nil {
		s.rootFSStates = make(map[string]*SandboxRootFSState)
	}
	s.rootFSStates[req.SandboxID] = rootFSProductTestState(req.SandboxID, target.TeamID, snapshot.HeadLayerID)
	filesystem := &RootFSFilesystem{
		ID:                 req.SandboxID,
		TeamID:             target.TeamID,
		SourceFilesystemID: snapshot.FilesystemID,
		HeadLayerID:        snapshot.HeadLayerID,
		CreatedAt:          time.Now().UTC(),
		UpdatedAt:          time.Now().UTC(),
	}
	return filesystem, nil
}

func rootFSProductTestService(store *memorySandboxStore) *SandboxService {
	return &SandboxService{
		sandboxStore:   store,
		teamQuotaStore: &permissiveTeamQuotaCapacityStore{},
		clock:          systemTime{},
		logger:         zap.NewNop(),
	}
}

func rootFSProductTestRecord(id, teamID, status string, now time.Time) *SandboxRecord {
	return &SandboxRecord{
		ID:                id,
		TeamID:            teamID,
		UserID:            "user-1",
		TemplateID:        "template-1",
		TemplateName:      "template-1",
		TemplateNamespace: "template-default",
		ClusterID:         "default",
		Status:            status,
		TemplateSpec:      v1alpha1.SandboxTemplateSpec{},
		CreatedAt:         now,
		UpdatedAt:         now,
	}
}

func rootFSProductTestState(sandboxID, teamID, layerID string) *SandboxRootFSState {
	return &SandboxRootFSState{
		LayerID:           layerID,
		SandboxID:         sandboxID,
		TeamID:            teamID,
		RuntimeGeneration: 1,
		Runtime:           "runc",
		BaseImageRef:      "docker.io/library/busybox:1.36",
		BaseImageDigest:   "sha256:base",
		Snapshotter:       "overlayfs",
		DiffDigest:        "sha256:" + layerID,
		DiffObjectKey:     "rootfs/" + layerID + ".tar",
		CreatedAt:         time.Now().UTC(),
		UpdatedAt:         time.Now().UTC(),
	}
}

func cloneRootFSSnapshotForTest(snapshot *RootFSSnapshot) *RootFSSnapshot {
	if snapshot == nil {
		return nil
	}
	clone := *snapshot
	return &clone
}

func cloneRootFSFilesystemForTest(filesystem *RootFSFilesystem) *RootFSFilesystem {
	if filesystem == nil {
		return nil
	}
	clone := *filesystem
	return &clone
}

var _ SandboxRootFSProductStore = (*memorySandboxStore)(nil)

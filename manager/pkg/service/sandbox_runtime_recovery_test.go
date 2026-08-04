package service

import (
	"context"
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/controller"
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

func TestReconcileSandboxRuntimeCreatesDurableLostRecoveryAfterStrongAbsence(t *testing.T) {
	store := runtimeRecoveryStore("sandbox-1", "pod-1", 3, SandboxDesiredStateActive)
	enqueuer := &recordingPauseEnqueuer{}
	svc := &SandboxService{
		k8sClient:     fake.NewSimpleClientset(),
		sandboxStore:  store,
		pauseEnqueuer: enqueuer,
		config:        SandboxServiceConfig{ClusterID: "cluster-a"},
		logger:        zap.NewNop(),
	}

	require.NoError(t, svc.ReconcileSandboxRuntime(context.Background(), "sandbox-1"))
	txn := activeLifecycleTxnForTest(store, "sandbox-1")
	require.NotNil(t, txn)
	assert.Equal(t, SandboxLifecycleKindPause, txn.Kind)
	assert.Equal(t, SandboxLifecycleSourceLost, txn.Source)
	assert.Equal(t, int64(3), txn.FromGeneration)
	assert.Equal(t, "default", txn.FromPodNamespace)
	assert.Equal(t, "pod-1", txn.FromPodName)
	assert.Equal(t, []string{"sandbox-1"}, enqueuer.recoveryCalls)
	assert.Equal(t, SandboxDesiredStateActive, store.records["sandbox-1"].DesiredState)

	require.NoError(t, svc.ReconcileSandboxRuntime(context.Background(), "sandbox-1"))
	assert.Len(t, store.lifecycleTxns, 1, "duplicate reconcile must reuse the active recovery transaction")
}

func TestReconcileSandboxRuntimeDoesNotTreatKubernetesFailureAsAbsence(t *testing.T) {
	store := runtimeRecoveryStore("sandbox-1", "pod-1", 3, SandboxDesiredStateActive)
	client := fake.NewSimpleClientset()
	client.PrependReactor("list", "pods", func(ktesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("apiserver unavailable")
	})
	svc := &SandboxService{
		k8sClient:    client,
		sandboxStore: store,
		config:       SandboxServiceConfig{ClusterID: "cluster-a"},
		logger:       zap.NewNop(),
	}

	err := svc.ReconcileSandboxRuntime(context.Background(), "sandbox-1")
	require.ErrorContains(t, err, "apiserver unavailable")
	assert.Nil(t, activeLifecycleTxnForTest(store, "sandbox-1"))
	assert.Equal(t, SandboxDesiredStateActive, store.records["sandbox-1"].DesiredState)
}

func TestReconcileSandboxRuntimeRepairsSameGenerationProjectionFromKubernetes(t *testing.T) {
	pod := runtimeRecoveryPod("pod-new", "sandbox-1", 3)
	store := runtimeRecoveryStore("sandbox-1", "pod-old", 3, SandboxDesiredStateActive)
	svc := &SandboxService{
		k8sClient:    fake.NewSimpleClientset(pod),
		sandboxStore: store,
		config:       SandboxServiceConfig{ClusterID: "cluster-a"},
		logger:       zap.NewNop(),
	}

	require.NoError(t, svc.ReconcileSandboxRuntime(context.Background(), "sandbox-1"))
	record := store.records["sandbox-1"]
	assert.Equal(t, "pod-new", record.CurrentPodName)
	assert.Equal(t, "default", record.CurrentPodNamespace)
	assert.Equal(t, int64(3), record.RuntimeGeneration)
	assert.Nil(t, activeLifecycleTxnForTest(store, "sandbox-1"))
}

func TestReconcileSandboxRuntimeRejectsUnownedNewerGeneration(t *testing.T) {
	pod := runtimeRecoveryPod("pod-new", "sandbox-1", 4)
	store := runtimeRecoveryStore("sandbox-1", "pod-old", 3, SandboxDesiredStateActive)
	svc := &SandboxService{
		k8sClient:    fake.NewSimpleClientset(pod),
		sandboxStore: store,
		config:       SandboxServiceConfig{ClusterID: "cluster-a"},
		logger:       zap.NewNop(),
	}

	err := svc.ReconcileSandboxRuntime(context.Background(), "sandbox-1")
	require.ErrorContains(t, err, "unowned runtime generation 4")
	assert.Equal(t, "pod-old", store.records["sandbox-1"].CurrentPodName)
	assert.Nil(t, activeLifecycleTxnForTest(store, "sandbox-1"))
}

func TestReconcileSandboxRuntimeRejectsAmbiguousSameGenerationPods(t *testing.T) {
	first := runtimeRecoveryPod("pod-a", "sandbox-1", 3)
	second := runtimeRecoveryPod("pod-b", "sandbox-1", 3)
	store := runtimeRecoveryStore("sandbox-1", "pod-old", 3, SandboxDesiredStateActive)
	svc := &SandboxService{
		k8sClient:    fake.NewSimpleClientset(first, second),
		sandboxStore: store,
		config:       SandboxServiceConfig{ClusterID: "cluster-a"},
		logger:       zap.NewNop(),
	}

	err := svc.ReconcileSandboxRuntime(context.Background(), "sandbox-1")
	require.ErrorContains(t, err, "unowned runtime generation 3")
	assert.Equal(t, "pod-old", store.records["sandbox-1"].CurrentPodName)
	assert.Nil(t, activeLifecycleTxnForTest(store, "sandbox-1"))
}

func TestReconcileSandboxRuntimeDeletesOlderRuntimeBeforeRecovery(t *testing.T) {
	stale := runtimeRecoveryPod("pod-stale", "sandbox-1", 2)
	store := runtimeRecoveryStore("sandbox-1", "pod-missing", 3, SandboxDesiredStateActive)
	enqueuer := &recordingPauseEnqueuer{}
	client := fake.NewSimpleClientset(stale)
	svc := &SandboxService{
		k8sClient:     client,
		sandboxStore:  store,
		pauseEnqueuer: enqueuer,
		config:        SandboxServiceConfig{ClusterID: "cluster-a"},
		logger:        zap.NewNop(),
	}

	require.NoError(t, svc.ReconcileSandboxRuntime(context.Background(), "sandbox-1"))
	assert.True(t, hasPodAction(client.Actions(), "delete", "pod-stale"))
	txn := activeLifecycleTxnForTest(store, "sandbox-1")
	require.NotNil(t, txn)
	assert.Equal(t, SandboxLifecycleSourceLost, txn.Source)
	assert.Equal(t, []string{"sandbox-1"}, enqueuer.recoveryCalls)
}

func TestReconcileLostRuntimeRecoveryRetriesOlderRuntimeDeletion(t *testing.T) {
	stale := runtimeRecoveryPod("pod-stale", "sandbox-1", 2)
	store := runtimeRecoveryStore("sandbox-1", "pod-missing", 3, SandboxDesiredStateActive)
	store.lifecycleTxns = map[string]*SandboxLifecycleTxn{
		"txn-lost": {
			ID:               "txn-lost",
			SandboxID:        "sandbox-1",
			Kind:             SandboxLifecycleKindPause,
			Phase:            SandboxLifecyclePhasePreparing,
			Source:           SandboxLifecycleSourceLost,
			FromGeneration:   3,
			FromPodNamespace: "default",
			FromPodName:      "pod-missing",
		},
	}
	enqueuer := &recordingPauseEnqueuer{}
	client := fake.NewSimpleClientset(stale)
	deleteAttempts := 0
	client.PrependReactor("delete", "pods", func(ktesting.Action) (bool, runtime.Object, error) {
		deleteAttempts++
		if deleteAttempts == 1 {
			return true, nil, errors.New("transient delete failure")
		}
		return false, nil, nil
	})
	svc := &SandboxService{
		k8sClient:     client,
		sandboxStore:  store,
		pauseEnqueuer: enqueuer,
		config:        SandboxServiceConfig{ClusterID: "cluster-a"},
		logger:        zap.NewNop(),
	}

	err := svc.ReconcileSandboxRuntime(context.Background(), "sandbox-1")
	require.ErrorContains(t, err, "transient delete failure")
	assert.Empty(t, enqueuer.recoveryCalls)

	require.NoError(t, svc.ReconcileSandboxRuntime(context.Background(), "sandbox-1"))
	assert.Equal(t, 2, deleteAttempts)
	assert.Equal(t, []string{"sandbox-1"}, enqueuer.recoveryCalls)
	assert.Equal(t, "txn-lost", activeLifecycleTxnID(store, "sandbox-1"))
}

func TestReconcileLostRuntimeRecoveryRejectsUnexpectedSameGenerationPod(t *testing.T) {
	unexpected := runtimeRecoveryPod("pod-unexpected", "sandbox-1", 3)
	store := runtimeRecoveryStore("sandbox-1", "pod-missing", 3, SandboxDesiredStateActive)
	store.lifecycleTxns = map[string]*SandboxLifecycleTxn{
		"txn-lost": {
			ID:               "txn-lost",
			SandboxID:        "sandbox-1",
			Kind:             SandboxLifecycleKindPause,
			Phase:            SandboxLifecyclePhasePreparing,
			Source:           SandboxLifecycleSourceLost,
			FromGeneration:   3,
			FromPodNamespace: "default",
			FromPodName:      "pod-missing",
		},
	}
	enqueuer := &recordingPauseEnqueuer{}
	svc := &SandboxService{
		k8sClient:     fake.NewSimpleClientset(unexpected),
		sandboxStore:  store,
		pauseEnqueuer: enqueuer,
		config:        SandboxServiceConfig{ClusterID: "cluster-a"},
		logger:        zap.NewNop(),
	}

	err := svc.ReconcileSandboxRuntime(context.Background(), "sandbox-1")
	require.ErrorContains(t, err, "unexpected runtime owns the lost generation")
	assert.Empty(t, enqueuer.recoveryCalls)
	assert.Equal(t, "txn-lost", activeLifecycleTxnID(store, "sandbox-1"))
}

func TestReconcileSandboxRuntimeDoesNotAdoptUnclaimedPod(t *testing.T) {
	unclaimed := runtimeRecoveryPod("pod-idle", "sandbox-1", 3)
	unclaimed.Labels[controller.LabelPoolType] = controller.PoolTypeIdle
	store := runtimeRecoveryStore("sandbox-1", "pod-missing", 3, SandboxDesiredStateActive)
	enqueuer := &recordingPauseEnqueuer{}
	svc := &SandboxService{
		k8sClient:     fake.NewSimpleClientset(unclaimed),
		sandboxStore:  store,
		pauseEnqueuer: enqueuer,
		config:        SandboxServiceConfig{ClusterID: "cluster-a"},
		logger:        zap.NewNop(),
	}

	require.NoError(t, svc.ReconcileSandboxRuntime(context.Background(), "sandbox-1"))
	assert.Equal(t, "pod-missing", store.records["sandbox-1"].CurrentPodName)
	txn := activeLifecycleTxnForTest(store, "sandbox-1")
	require.NotNil(t, txn)
	assert.Equal(t, SandboxLifecycleSourceLost, txn.Source)
	assert.Equal(t, []string{"sandbox-1"}, enqueuer.recoveryCalls)
}

func TestReconcileSandboxRuntimeIgnoresAnotherCluster(t *testing.T) {
	store := runtimeRecoveryStore("sandbox-1", "pod-1", 3, SandboxDesiredStateActive)
	store.records["sandbox-1"].ClusterID = "cluster-b"
	svc := &SandboxService{
		k8sClient:    fake.NewSimpleClientset(),
		sandboxStore: store,
		config:       SandboxServiceConfig{ClusterID: "cluster-a"},
		logger:       zap.NewNop(),
	}

	require.NoError(t, svc.ReconcileSandboxRuntime(context.Background(), "sandbox-1"))
	assert.Nil(t, activeLifecycleTxnForTest(store, "sandbox-1"))
}

func TestReconcileSandboxRuntimeHardExpiryDeletesInsteadOfRecovering(t *testing.T) {
	now := time.Date(2026, time.August, 3, 8, 0, 0, 0, time.UTC)
	store := runtimeRecoveryStore("sandbox-1", "pod-1", 3, SandboxDesiredStateActive)
	store.records["sandbox-1"].HardExpiresAt = now.Add(-time.Second)
	svc := &SandboxService{
		k8sClient:    fake.NewSimpleClientset(),
		sandboxStore: store,
		clock:        fixedClock{now: now},
		config:       SandboxServiceConfig{ClusterID: "cluster-a"},
		logger:       zap.NewNop(),
	}

	require.NoError(t, svc.ReconcileSandboxRuntime(context.Background(), "sandbox-1"))
	assert.Equal(t, SandboxDesiredStateDeleted, store.records["sandbox-1"].DesiredState)
	assert.Nil(t, activeLifecycleTxnForTest(store, "sandbox-1"))
}

func TestReconcilePausedSandboxDeletesStalePodWithoutRecovery(t *testing.T) {
	pod := runtimeRecoveryPod("pod-1", "sandbox-1", 3)
	store := runtimeRecoveryStore("sandbox-1", "", 3, SandboxDesiredStatePaused)
	client := fake.NewSimpleClientset(pod)
	svc := &SandboxService{
		k8sClient:    client,
		sandboxStore: store,
		config:       SandboxServiceConfig{ClusterID: "cluster-a"},
		logger:       zap.NewNop(),
	}

	require.NoError(t, svc.ReconcileSandboxRuntime(context.Background(), "sandbox-1"))
	assert.Equal(t, SandboxDesiredStatePaused, store.records["sandbox-1"].DesiredState)
	assert.Nil(t, activeLifecycleTxnForTest(store, "sandbox-1"))
	assert.True(t, hasPodAction(client.Actions(), "delete", "pod-1"))
}

func TestReconcileSandboxRuntimeDoesNotRaceFreshResumeTransaction(t *testing.T) {
	now := time.Date(2026, time.August, 3, 8, 0, 0, 0, time.UTC)
	store := runtimeRecoveryStore("sandbox-1", "", 3, SandboxDesiredStatePaused)
	store.lifecycleTxns = map[string]*SandboxLifecycleTxn{
		"txn-resume": {
			ID:             "txn-resume",
			SandboxID:      "sandbox-1",
			Kind:           SandboxLifecycleKindResume,
			Phase:          SandboxLifecyclePhasePreparing,
			FromGeneration: 3,
			ToGeneration:   4,
			UpdatedAt:      now,
		},
	}
	svc := &SandboxService{
		k8sClient:    fake.NewSimpleClientset(),
		sandboxStore: store,
		clock:        fixedClock{now: now.Add(time.Minute)},
		config:       SandboxServiceConfig{ClusterID: "cluster-a"},
		logger:       zap.NewNop(),
	}

	require.NoError(t, svc.ReconcileSandboxRuntime(context.Background(), "sandbox-1"))
	txn := activeLifecycleTxnForTest(store, "sandbox-1")
	require.NotNil(t, txn)
	assert.Equal(t, "txn-resume", txn.ID)
	assert.Equal(t, SandboxLifecyclePhasePreparing, txn.Phase)
}

func TestReconcileSandboxRuntimeAbortsStaleResumeWithMissingTarget(t *testing.T) {
	now := time.Date(2026, time.August, 3, 8, 0, 0, 0, time.UTC)
	store := runtimeRecoveryStore("sandbox-1", "", 3, SandboxDesiredStatePaused)
	store.records["sandbox-1"].TemplateID = "default"
	store.records["sandbox-1"].TemplateName = "default"
	store.records["sandbox-1"].TemplateNamespace = "tpl-default"
	store.lifecycleTxns = map[string]*SandboxLifecycleTxn{
		"txn-resume": {
			ID:             "txn-resume",
			SandboxID:      "sandbox-1",
			Kind:           SandboxLifecycleKindResume,
			Phase:          SandboxLifecyclePhasePreparing,
			FromGeneration: 3,
			ToGeneration:   4,
			UpdatedAt:      now.Add(-3 * time.Minute),
		},
	}
	svc := &SandboxService{
		k8sClient:    fake.NewSimpleClientset(),
		podLister:    runtimeIdentityPodLister(t),
		sandboxStore: store,
		clock:        fixedClock{now: now},
		config:       SandboxServiceConfig{ClusterID: "cluster-a"},
		logger:       zap.NewNop(),
	}

	err := svc.ReconcileSandboxRuntime(context.Background(), "sandbox-1")
	require.Error(t, err, "replacement creation is expected to fail in the unit fixture")
	assert.Equal(t, SandboxLifecyclePhaseAborted, store.lifecycleTxns["txn-resume"].Phase)
	assert.NotEqual(t, "txn-resume", activeLifecycleTxnID(store, "sandbox-1"))
}

func TestReconcileSandboxRuntimeWaitsForFreshSourceCheckpointTransaction(t *testing.T) {
	now := time.Date(2026, time.August, 3, 8, 0, 0, 0, time.UTC)
	store := runtimeRecoveryStore("sandbox-1", "pod-missing", 3, SandboxDesiredStateActive)
	store.lifecycleTxns = map[string]*SandboxLifecycleTxn{
		"txn-snapshot": {
			ID:               "txn-snapshot",
			SandboxID:        "sandbox-1",
			Kind:             SandboxLifecycleKindSnapshot,
			Phase:            SandboxLifecyclePhasePublishing,
			FromGeneration:   3,
			FromPodNamespace: "default",
			FromPodName:      "pod-missing",
			UpdatedAt:        now,
		},
	}
	enqueuer := &recordingPauseEnqueuer{}
	svc := &SandboxService{
		k8sClient:     fake.NewSimpleClientset(),
		sandboxStore:  store,
		pauseEnqueuer: enqueuer,
		clock:         fixedClock{now: now.Add(time.Minute)},
		config:        SandboxServiceConfig{ClusterID: "cluster-a"},
		logger:        zap.NewNop(),
	}

	require.NoError(t, svc.ReconcileSandboxRuntime(context.Background(), "sandbox-1"))
	txn := activeLifecycleTxnForTest(store, "sandbox-1")
	require.NotNil(t, txn)
	assert.Equal(t, "txn-snapshot", txn.ID)
	assert.Empty(t, enqueuer.recoveryCalls)
}

func TestReconcileSandboxRuntimeRecoversAfterStaleSourceCheckpointTransaction(t *testing.T) {
	now := time.Date(2026, time.August, 3, 8, 0, 0, 0, time.UTC)
	store := runtimeRecoveryStore("sandbox-1", "pod-missing", 3, SandboxDesiredStateActive)
	store.lifecycleTxns = map[string]*SandboxLifecycleTxn{
		"txn-fork": {
			ID:               "txn-fork",
			SandboxID:        "sandbox-1",
			Kind:             SandboxLifecycleKindFork,
			Phase:            SandboxLifecyclePhasePublishing,
			FromGeneration:   3,
			FromPodNamespace: "default",
			FromPodName:      "pod-missing",
			UpdatedAt:        now.Add(-sandboxRootFSSourceCheckpointLifecycleStaleAfter - time.Second),
		},
	}
	enqueuer := &recordingPauseEnqueuer{}
	svc := &SandboxService{
		k8sClient:     fake.NewSimpleClientset(),
		sandboxStore:  store,
		pauseEnqueuer: enqueuer,
		clock:         fixedClock{now: now},
		config:        SandboxServiceConfig{ClusterID: "cluster-a"},
		logger:        zap.NewNop(),
	}

	require.NoError(t, svc.ReconcileSandboxRuntime(context.Background(), "sandbox-1"))
	assert.Equal(t, SandboxLifecyclePhaseAborted, store.lifecycleTxns["txn-fork"].Phase)
	txn := activeLifecycleTxnForTest(store, "sandbox-1")
	require.NotNil(t, txn)
	assert.Equal(t, SandboxLifecycleKindPause, txn.Kind)
	assert.Equal(t, SandboxLifecycleSourceLost, txn.Source)
	assert.Equal(t, []string{"sandbox-1"}, enqueuer.recoveryCalls)
}

func TestReconcileTerminatingSandboxAbortsResumeAndNeverRecovers(t *testing.T) {
	store := runtimeRecoveryStore("sandbox-1", "pod-missing", 3, SandboxDesiredStateTerminating)
	store.lifecycleTxns = map[string]*SandboxLifecycleTxn{
		"txn-resume": {
			ID:             "txn-resume",
			SandboxID:      "sandbox-1",
			Kind:           SandboxLifecycleKindResume,
			Phase:          SandboxLifecyclePhasePreparing,
			FromGeneration: 3,
			ToGeneration:   4,
		},
	}
	enqueuer := &recordingPauseEnqueuer{}
	svc := &SandboxService{
		k8sClient:     fake.NewSimpleClientset(),
		sandboxStore:  store,
		pauseEnqueuer: enqueuer,
		clock:         systemTime{},
		config:        SandboxServiceConfig{ClusterID: "cluster-a"},
		logger:        zap.NewNop(),
	}

	require.NoError(t, svc.ReconcileSandboxRuntime(context.Background(), "sandbox-1"))
	assert.Equal(t, SandboxDesiredStateDeleted, store.records["sandbox-1"].DesiredState)
	assert.Equal(t, SandboxLifecyclePhaseAborted, store.lifecycleTxns["txn-resume"].Phase)
	assert.Empty(t, enqueuer.recoveryCalls)
}

func TestCompleteLostRecoveryPreservesLastCommittedHeadWhenPodIsGone(t *testing.T) {
	store := runtimeRecoveryStore("sandbox-1", "pod-1", 3, SandboxDesiredStateActive)
	store.rootFSStates = map[string]*SandboxRootFSState{
		"sandbox-1": {SandboxID: "sandbox-1", LayerID: "committed-head", RuntimeGeneration: 2},
	}
	store.lifecycleTxns = map[string]*SandboxLifecycleTxn{
		"txn-lost": {
			ID:               "txn-lost",
			SandboxID:        "sandbox-1",
			Kind:             SandboxLifecycleKindPause,
			Phase:            SandboxLifecyclePhasePreparing,
			Source:           SandboxLifecycleSourceLost,
			FromGeneration:   3,
			FromPodNamespace: "default",
			FromPodName:      "pod-1",
		},
	}
	svc := &SandboxService{
		k8sClient:    fake.NewSimpleClientset(),
		podLister:    runtimeIdentityPodLister(t),
		sandboxStore: store,
		clock:        systemTime{},
		logger:       zap.NewNop(),
	}

	require.NoError(t, svc.CompletePausingSandboxRuntime(context.Background(), "sandbox-1"))
	assert.Equal(t, SandboxDesiredStatePaused, store.records["sandbox-1"].DesiredState)
	assert.Equal(t, "committed-head", store.rootFSStates["sandbox-1"].LayerID)
	assert.Equal(t, SandboxLifecyclePhaseCommitted, store.lifecycleTxns["txn-lost"].Phase)
}

func TestTerminateSandboxPersistsIntentBeforePodDeletion(t *testing.T) {
	pod := runtimeRecoveryPod("pod-1", "sandbox-1", 3)
	store := runtimeRecoveryStore("sandbox-1", "pod-1", 3, SandboxDesiredStateActive)
	client := fake.NewSimpleClientset(pod)
	desiredStateAtDelete := ""
	client.PrependReactor("delete", "pods", func(ktesting.Action) (bool, runtime.Object, error) {
		record, err := store.GetSandbox(context.Background(), "sandbox-1")
		require.NoError(t, err)
		desiredStateAtDelete = record.DesiredState
		return false, nil, nil
	})
	svc := &SandboxService{
		k8sClient:    client,
		podLister:    runtimeIdentityPodLister(t, pod),
		sandboxStore: store,
		clock:        systemTime{},
		config:       SandboxServiceConfig{ClusterID: "cluster-a"},
		logger:       zap.NewNop(),
	}

	require.NoError(t, svc.TerminateSandbox(context.Background(), "sandbox-1"))
	assert.Equal(t, SandboxDesiredStateTerminating, desiredStateAtDelete)
	assert.Equal(t, SandboxDesiredStateDeleted, store.records["sandbox-1"].DesiredState)
}

func TestSandboxRecordDeletionScopeRequiresDurableDeleteIntent(t *testing.T) {
	record := &SandboxRecord{
		ID:                  "sandbox-1",
		DesiredState:        SandboxDesiredStateActive,
		CurrentPodNamespace: "default",
		CurrentPodName:      "pod-1",
		RuntimeGeneration:   3,
	}
	assert.True(t, SandboxRecordDeletionIsRuntimeOnly(record, "default", "pod-1", 3))
	record.DesiredState = SandboxDesiredStateTerminating
	assert.False(t, SandboxRecordDeletionIsRuntimeOnly(record, "default", "pod-1", 3))
	record.DesiredState = SandboxDesiredStateDeleted
	assert.False(t, SandboxRecordDeletionIsRuntimeOnly(record, "default", "pod-1", 3))
}

func TestUnexpectedCurrentPodDeletionPreservesSandboxExternalState(t *testing.T) {
	bindings := &deleteRecordingBindingStore{}
	volumes := &recordingSystemVolumeClient{}
	emitter := &recordingDeletionWebhookEmitter{}
	store := runtimeRecoveryStore("sandbox-1", "pod-1", 3, SandboxDesiredStateActive)
	svc := &SandboxService{
		sandboxStore:           store,
		credentialStore:        bindings,
		webhookStateVolumes:    volumes,
		deletionWebhookEmitter: emitter,
		logger:                 zap.NewNop(),
	}

	require.NoError(t, svc.CleanupDeletedSandbox(context.Background(), SandboxLifecycleInfo{
		SandboxID:            "sandbox-1",
		Namespace:            "default",
		PodName:              "pod-1",
		RuntimeGeneration:    3,
		TeamID:               "team-1",
		WebhookURL:           "https://example.test/webhook",
		WebhookStateVolumeID: "webhook-volume-1",
	}))
	assert.Zero(t, bindings.deleteCalls)
	assert.Empty(t, volumes.marked)
	assert.Empty(t, emitter.calls)
	assert.Equal(t, SandboxDesiredStateActive, store.records["sandbox-1"].DesiredState)
}

func TestSandboxRuntimeReconcilerDeleteEventQueuesSandbox(t *testing.T) {
	reconciler := NewSandboxRuntimeReconciler("cluster-a", nil, nil, nil, zap.NewNop())
	pod := runtimeRecoveryPod("pod-1", "sandbox-1", 3)
	reconciler.ResourceEventHandler().DeleteFunc(pod)
	require.Equal(t, 1, reconciler.queue.Len())
	item, shutdown := reconciler.queue.Get()
	require.False(t, shutdown)
	reconciler.queue.Done(item)
	reconciler.queue.Forget(item)
	assert.Equal(t, "sandbox-1", item)
}

func TestSandboxRuntimeReconcilerScansPagesAndQueuesOnlyDrift(t *testing.T) {
	healthy := runtimeRecoveryPod("pod-a", "sandbox-a", 1)
	store := &runtimeReconcileMemoryStore{candidates: []SandboxRuntimeReconcileCandidate{
		{SandboxID: "sandbox-a", DesiredState: SandboxDesiredStateActive, PodNamespace: "default", PodName: "pod-a", RuntimeGeneration: 1},
		{SandboxID: "sandbox-b", DesiredState: SandboxDesiredStateActive, PodNamespace: "default", PodName: "pod-b", RuntimeGeneration: 1},
		{SandboxID: "sandbox-c", DesiredState: SandboxDesiredStateTerminating},
	}}
	reconciler := NewSandboxRuntimeReconciler("cluster-a", store, runtimeIdentityPodLister(t, healthy), nil, zap.NewNop())
	reconciler.pageSize = 2

	reconciler.enqueueDriftCandidates(context.Background())
	assert.Equal(t, []string{"", "sandbox-b"}, store.afterCalls)
	assert.Equal(t, 2, reconciler.queue.Len())
	got := map[string]bool{}
	for reconciler.queue.Len() > 0 {
		item, shutdown := reconciler.queue.Get()
		require.False(t, shutdown)
		got[item] = true
		reconciler.queue.Done(item)
		reconciler.queue.Forget(item)
	}
	assert.Equal(t, map[string]bool{"sandbox-b": true, "sandbox-c": true}, got)
}

type runtimeReconcileMemoryStore struct {
	candidates []SandboxRuntimeReconcileCandidate
	afterCalls []string
}

func (s *runtimeReconcileMemoryStore) ListRuntimeReconcileCandidates(_ context.Context, _ string, after string, limit int) ([]SandboxRuntimeReconcileCandidate, error) {
	s.afterCalls = append(s.afterCalls, after)
	page := make([]SandboxRuntimeReconcileCandidate, 0, limit)
	for _, candidate := range s.candidates {
		if candidate.SandboxID <= after {
			continue
		}
		page = append(page, candidate)
		if len(page) == limit {
			break
		}
	}
	return page, nil
}

func runtimeRecoveryStore(sandboxID, podName string, generation int64, desiredState string) *memorySandboxStore {
	namespace := "default"
	if podName == "" {
		namespace = ""
	}
	return &memorySandboxStore{records: map[string]*SandboxRecord{
		sandboxID: {
			ID:                  sandboxID,
			TeamID:              "team-1",
			UserID:              "user-1",
			ClusterID:           "cluster-a",
			DesiredState:        desiredState,
			CurrentPodNamespace: namespace,
			CurrentPodName:      podName,
			RuntimeGeneration:   generation,
		},
	}}
}

func runtimeRecoveryPod(name, sandboxID string, generation int64) *corev1.Pod {
	pod := rootFSTestPod(name, sandboxID, "team-1")
	pod.UID = types.UID(name + "-uid")
	pod.Annotations[controller.AnnotationRuntimeGeneration] = strconv.FormatInt(generation, 10)
	pod.Finalizers = []string{sandboxCleanupFinalizer}
	pod.CreationTimestamp = metav1.NewTime(time.Now().UTC())
	return pod
}

func hasPodAction(actions []ktesting.Action, verb, name string) bool {
	for _, action := range actions {
		if action.GetVerb() == verb && action.GetResource().Resource == "pods" {
			if named, ok := action.(interface{ GetName() string }); ok && named.GetName() == name {
				return true
			}
		}
	}
	return false
}

func activeLifecycleTxnID(store *memorySandboxStore, sandboxID string) string {
	txn := activeLifecycleTxnForTest(store, sandboxID)
	if txn == nil {
		return ""
	}
	return txn.ID
}

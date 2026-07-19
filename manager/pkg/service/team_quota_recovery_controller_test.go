package service

import (
	"context"
	"errors"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/controller"
	"github.com/sandbox0-ai/sandbox0/pkg/naming"
	"github.com/sandbox0-ai/sandbox0/pkg/teamquota"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

func TestTeamQuotaStartupRecoveryCommitsOwnedHotClaimTransfer(t *testing.T) {
	transfer, pod := recoveryTransferAndActivePod()
	store := newRecoveryQuotaTestStore()
	store.transfers = []teamquota.RecoveryTransfer{transfer}
	client := fake.NewSimpleClientset(pod)
	svc := newTeamQuotaRecoveryTestService(client, store, nil)
	controller := NewTeamQuotaRecoveryController(
		svc,
		store,
		TeamQuotaRecoveryConfig{ClusterID: "cluster-a"},
		zap.NewNop(),
	)

	if err := controller.RecoverStartup(context.Background()); err != nil {
		t.Fatalf("RecoverStartup() error = %v", err)
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.transferCommits != 1 || store.transferAborts != 0 {
		t.Fatalf("transfer transitions = commit %d abort %d", store.transferCommits, store.transferAborts)
	}
	if len(store.transfers) != 0 {
		t.Fatalf("prepared transfer was not removed: %+v", store.transfers)
	}
}

func TestTeamQuotaRecoveryCommitsActiveTransferWhenReplicaSetAlreadyZero(t *testing.T) {
	transfer, pod := recoveryTransferAndActivePod()
	store := newRecoveryQuotaTestStore()
	store.transfers = []teamquota.RecoveryTransfer{transfer}
	client := fake.NewSimpleClientset(pod)
	svc := newTeamQuotaRecoveryTestService(client, store, nil)
	rsName, err := naming.ReplicasetName("cluster-a", "template-a")
	require.NoError(t, err)
	rs, err := client.AppsV1().ReplicaSets("sandbox").
		Get(context.Background(), rsName, metav1.GetOptions{})
	require.NoError(t, err)
	zero := int32(0)
	rs.Spec.Replicas = &zero
	_, err = client.AppsV1().ReplicaSets(rs.Namespace).
		Update(context.Background(), rs, metav1.UpdateOptions{})
	require.NoError(t, err)
	recovery := NewTeamQuotaRecoveryController(
		svc,
		store,
		TeamQuotaRecoveryConfig{ClusterID: "cluster-a"},
		zap.NewNop(),
	)

	terminalized, err := recovery.recoverPreparedTransfer(context.Background(), &transfer)
	require.NoError(t, err)
	require.True(t, terminalized)

	store.mu.Lock()
	assert.Equal(t, 1, store.transferCommits)
	assert.Equal(t, 0, store.transferAborts)
	store.mu.Unlock()
	rs, err = client.AppsV1().ReplicaSets("sandbox").
		Get(context.Background(), rsName, metav1.GetOptions{})
	require.NoError(t, err)
	require.NotNil(t, rs.Spec.Replicas)
	assert.Equal(t, int32(0), *rs.Spec.Replicas)
	assert.NotContains(t, rs.Annotations, controller.AnnotationTeamQuotaWarmPoolTransfers)
}

func TestTeamQuotaRecoveryRetainsTerminatingTransferRuntime(t *testing.T) {
	transfer, pod := recoveryTransferAndActivePod()
	now := metav1.Now()
	pod.DeletionTimestamp = &now
	store := newRecoveryQuotaTestStore()
	store.transfers = []teamquota.RecoveryTransfer{transfer}
	recovery := NewTeamQuotaRecoveryController(
		newTeamQuotaRecoveryTestService(fake.NewSimpleClientset(pod), store, nil),
		store,
		TeamQuotaRecoveryConfig{ClusterID: "cluster-a"},
		zap.NewNop(),
	)

	terminalized, err := recovery.recoverPreparedTransfer(context.Background(), &transfer)
	require.NoError(t, err)
	require.False(t, terminalized)

	store.mu.Lock()
	defer store.mu.Unlock()
	assert.Equal(t, 0, store.transferCommits)
	assert.Equal(t, 0, store.transferAborts)
	require.Len(t, store.transfers, 1)
}

func TestTeamQuotaRecoveryAbortsWhenExactTransferRuntimeIsGoneWithoutMarker(t *testing.T) {
	transfer, _ := recoveryTransferAndActivePod()
	store := newRecoveryQuotaTestStore()
	store.transfers = []teamquota.RecoveryTransfer{transfer}
	recovery := NewTeamQuotaRecoveryController(
		newTeamQuotaRecoveryTestService(fake.NewSimpleClientset(), store, nil),
		store,
		TeamQuotaRecoveryConfig{ClusterID: "cluster-a"},
		zap.NewNop(),
	)

	terminalized, err := recovery.recoverPreparedTransfer(context.Background(), &transfer)
	require.NoError(t, err)
	require.True(t, terminalized)

	store.mu.Lock()
	defer store.mu.Unlock()
	assert.Equal(t, 0, store.transferCommits)
	assert.Equal(t, 1, store.transferAborts)
	assert.Empty(t, store.transfers)
}

func TestTeamQuotaRecoveryTreatsSameNameReplacementAsGone(t *testing.T) {
	transfer, pod := recoveryTransferAndActivePod()
	pod.UID = types.UID("replacement-uid")
	store := newRecoveryQuotaTestStore()
	store.transfers = []teamquota.RecoveryTransfer{transfer}
	recovery := NewTeamQuotaRecoveryController(
		newTeamQuotaRecoveryTestService(fake.NewSimpleClientset(pod), store, nil),
		store,
		TeamQuotaRecoveryConfig{ClusterID: "cluster-a"},
		zap.NewNop(),
	)

	terminalized, err := recovery.recoverPreparedTransfer(context.Background(), &transfer)
	require.NoError(t, err)
	require.True(t, terminalized)

	store.mu.Lock()
	defer store.mu.Unlock()
	assert.Equal(t, 0, store.transferCommits)
	assert.Equal(t, 1, store.transferAborts)
}

func TestTeamQuotaRecoveryCommitsAndReleasesGoneRuntimeAfterMarker(t *testing.T) {
	transfer, _ := recoveryTransferAndActivePod()
	store := newRecoveryQuotaTestStore()
	store.transfers = []teamquota.RecoveryTransfer{transfer}
	client := fake.NewSimpleClientset()
	svc := newTeamQuotaRecoveryTestService(client, store, nil)
	rsName, err := naming.ReplicasetName("cluster-a", "template-a")
	require.NoError(t, err)
	rs, err := client.AppsV1().ReplicaSets("sandbox").
		Get(context.Background(), rsName, metav1.GetOptions{})
	require.NoError(t, err)
	zero := int32(0)
	rs.Spec.Replicas = &zero
	require.NoError(t, setWarmPoolTransferMarkers(
		rs,
		map[string]int32{transfer.Operation.ID: 0},
	))
	_, err = client.AppsV1().ReplicaSets(rs.Namespace).
		Update(context.Background(), rs, metav1.UpdateOptions{})
	require.NoError(t, err)
	recovery := NewTeamQuotaRecoveryController(
		svc,
		store,
		TeamQuotaRecoveryConfig{ClusterID: "cluster-a"},
		zap.NewNop(),
	)

	terminalized, err := recovery.recoverPreparedTransfer(context.Background(), &transfer)
	require.NoError(t, err)
	require.True(t, terminalized)

	store.mu.Lock()
	assert.Equal(t, 1, store.transferCommits)
	assert.Equal(t, 0, store.transferAborts)
	assert.Equal(t, 1, store.releases)
	assert.Equal(t, 1, store.confirms)
	store.mu.Unlock()
	rs, err = client.AppsV1().ReplicaSets("sandbox").
		Get(context.Background(), rsName, metav1.GetOptions{})
	require.NoError(t, err)
	assert.NotContains(t, rs.Annotations, controller.AnnotationTeamQuotaWarmPoolTransfers)
}

func TestTeamQuotaRecoveryRetainsOwnedTransferUntilRuntimeMatchesDestinationTarget(t *testing.T) {
	transfer, pod := recoveryTransferAndActivePod()
	transfer.DestinationTarget = transfer.DestinationTarget.Clone()
	transfer.DestinationTarget[teamquota.KeySandboxMemoryBytes]++
	store := newRecoveryQuotaTestStore()
	store.transfers = []teamquota.RecoveryTransfer{transfer}
	recovery := NewTeamQuotaRecoveryController(
		newTeamQuotaRecoveryTestService(fake.NewSimpleClientset(pod), store, nil),
		store,
		TeamQuotaRecoveryConfig{ClusterID: "cluster-a"},
		zap.NewNop(),
	)

	terminalized, err := recovery.recoverPreparedTransfer(context.Background(), &transfer)
	require.NoError(t, err)
	require.False(t, terminalized)

	store.mu.Lock()
	defer store.mu.Unlock()
	assert.Equal(t, 0, store.transferCommits)
	assert.Equal(t, 0, store.transferAborts)
	require.Len(t, store.transfers, 1)
	assert.Equal(t, transfer.Operation.ID, store.transfers[0].Operation.ID)
}

func TestTeamQuotaRecoveryStopsAfterFullRetainedTransferBatch(t *testing.T) {
	transfer, pod := recoveryTransferAndActivePod()
	transfer.DestinationTarget = transfer.DestinationTarget.Clone()
	transfer.DestinationTarget[teamquota.KeySandboxMemoryBytes]++
	store := newRecoveryQuotaTestStore()
	store.transfers = []teamquota.RecoveryTransfer{transfer}
	recovery := NewTeamQuotaRecoveryController(
		newTeamQuotaRecoveryTestService(fake.NewSimpleClientset(pod), store, nil),
		store,
		TeamQuotaRecoveryConfig{
			ClusterID: "cluster-a",
			BatchSize: 1,
		},
		zap.NewNop(),
	)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- recovery.recoverPreparedTransfers(ctx)
	}()

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(time.Second):
		cancel()
		t.Fatal("retained full transfer batch caused recovery to loop")
	}

	store.mu.Lock()
	defer store.mu.Unlock()
	assert.Equal(t, 1, store.transferListCalls)
	assert.Equal(t, 0, store.transferCommits)
	assert.Equal(t, 0, store.transferAborts)
	require.Len(t, store.transfers, 1)
}

func TestTeamQuotaStartupRecoveryAbortsUnappliedWarmPoolTransfer(t *testing.T) {
	template := newSandboxResourceTestTemplate(t)
	template.Labels = map[string]string{controller.LabelTemplateScope: naming.ScopeTeam}
	template.Annotations = map[string]string{controller.AnnotationTemplateTeamID: "team-a"}
	template.Spec.Pool.MinIdle = 1
	clusterID := naming.ClusterIDOrDefault(template.Spec.ClusterId)
	source, ok := controller.TeamWarmPoolQuotaOwner(template)
	if !ok {
		t.Fatal("team warm-pool owner was not resolved")
	}
	rsName, err := naming.ReplicasetName(clusterID, template.Name)
	if err != nil {
		t.Fatalf("resolve ReplicaSet name: %v", err)
	}
	one := int32(1)
	rs := &appsv1.ReplicaSet{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: template.Namespace,
			Name:      rsName,
			UID:       types.UID("rs-uid"),
		},
		Spec: appsv1.ReplicaSetSpec{Replicas: &one},
	}
	idlePod := newSandboxResourceTestIdlePod(t, template, "idle-a")
	idlePod.UID = types.UID("pod-uid")
	idlePod.Annotations[controller.AnnotationTeamID] = "team-a"
	idlePod.Annotations[controller.AnnotationOwnerKind] = controller.OwnerKindTeamWarmPool
	idlePod.Labels[controller.LabelOwnerKind] = controller.OwnerKindTeamWarmPool
	idlePod.Labels[controller.LabelTemplateLogicalID] = controller.TemplateLogicalID(template)
	idlePod.OwnerReferences = []metav1.OwnerReference{{
		APIVersion: "apps/v1",
		Kind:       "ReplicaSet",
		Name:       rs.Name,
		UID:        rs.UID,
		Controller: ptrTo(true),
	}}
	transfer := teamquota.RecoveryTransfer{
		Operation: teamquota.Operation{ID: "claim-op", Kind: "claim_warm_pool_transfer", Generation: 1},
		Source:    source,
		Destination: teamquota.Owner{
			TeamID:    "team-a",
			Kind:      "sandbox",
			ID:        "sandbox-a",
			ClusterID: clusterID,
		},
		Runtime: teamquota.RuntimeRef{
			Namespace:  idlePod.Namespace,
			Name:       idlePod.Name,
			UID:        string(idlePod.UID),
			Generation: 1,
		},
		CreatedAt: time.Now().Add(-time.Hour),
	}
	store := newRecoveryQuotaTestStore()
	store.transfers = []teamquota.RecoveryTransfer{transfer}
	client := fake.NewSimpleClientset(idlePod, rs)
	svc := newTeamQuotaRecoveryTestService(client, store, []*SandboxRecord(nil))
	svc.templateLister = staticTemplateLister{templates: []*v1alpha1.SandboxTemplate{template}}
	recovery := NewTeamQuotaRecoveryController(
		svc,
		store,
		TeamQuotaRecoveryConfig{ClusterID: clusterID},
		zap.NewNop(),
	)

	if err := recovery.RecoverStartup(context.Background()); err != nil {
		t.Fatalf("RecoverStartup() error = %v", err)
	}
	storedRS, err := client.AppsV1().ReplicaSets(rs.Namespace).Get(
		context.Background(),
		rs.Name,
		metav1.GetOptions{},
	)
	if err != nil {
		t.Fatalf("get restored ReplicaSet: %v", err)
	}
	if storedRS.Spec.Replicas == nil || *storedRS.Spec.Replicas != 1 {
		t.Fatalf("ReplicaSet replicas = %v, want 1", storedRS.Spec.Replicas)
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.transferAborts != 1 || store.transferCommits != 0 {
		t.Fatalf("transfer transitions = commit %d abort %d", store.transferCommits, store.transferAborts)
	}
}

func TestTeamQuotaStartupRecoveryLookupErrorKeepsPreparedTransfer(t *testing.T) {
	transfer, pod := recoveryTransferAndActivePod()
	store := newRecoveryQuotaTestStore()
	store.transfers = []teamquota.RecoveryTransfer{transfer}
	client := fake.NewSimpleClientset(pod)
	client.PrependReactor("get", "pods", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("Kubernetes API unavailable")
	})
	svc := newTeamQuotaRecoveryTestService(client, store, nil)
	recovery := NewTeamQuotaRecoveryController(
		svc,
		store,
		TeamQuotaRecoveryConfig{ClusterID: "cluster-a"},
		zap.NewNop(),
	)

	if err := recovery.RecoverStartup(context.Background()); err == nil {
		t.Fatal("RecoverStartup() error = nil")
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.transferCommits != 0 || store.transferAborts != 0 || len(store.transfers) != 1 {
		t.Fatalf(
			"lookup failure changed transfer: commit %d abort %d transfers %+v",
			store.transferCommits,
			store.transferAborts,
			store.transfers,
		)
	}
}

func TestTeamQuotaStartupRecoveryDoesNotTakeFreshReplicaSaga(t *testing.T) {
	transfer, pod := recoveryTransferAndActivePod()
	transfer.CreatedAt = time.Now()
	allocationOperation := teamquota.Operation{ID: "claim-fresh", Kind: "claim", Generation: 1}
	reconcileAfter := time.Now().Add(time.Minute)
	store := newRecoveryQuotaTestStore()
	store.transfers = []teamquota.RecoveryTransfer{transfer}
	store.allocations = []teamquota.RecoveryAllocation{{
		AllocationID: "allocation-fresh",
		Owner: teamquota.Owner{
			TeamID:    "team-a",
			Kind:      "sandbox",
			ID:        "sandbox-fresh",
			ClusterID: "cluster-a",
		},
		State:          "reserved",
		Operation:      &allocationOperation,
		ReconcileAfter: &reconcileAfter,
	}}
	svc := newTeamQuotaRecoveryTestService(fake.NewSimpleClientset(pod), store, nil)
	recovery := NewTeamQuotaRecoveryController(
		svc,
		store,
		TeamQuotaRecoveryConfig{ClusterID: "cluster-a"},
		zap.NewNop(),
	)

	require.NoError(t, recovery.RecoverStartup(context.Background()))

	store.mu.Lock()
	defer store.mu.Unlock()
	assert.Equal(t, 0, store.transferCommits)
	assert.Equal(t, 0, store.transferAborts)
	assert.Equal(t, 0, store.commits)
	assert.Equal(t, 0, store.aborts)
	require.Len(t, store.transfers, 1)
	require.Len(t, store.allocations, 1)
	assert.NotNil(t, store.allocations[0].Operation)
}

func TestTeamQuotaPeriodicRecoveryRetriesAfterLookupError(t *testing.T) {
	transfer, pod := recoveryTransferAndActivePod()
	transfer.CreatedAt = time.Now().Add(-time.Hour)
	store := newRecoveryQuotaTestStore()
	store.transfers = []teamquota.RecoveryTransfer{transfer}
	store.transferCommitted = make(chan struct{}, 1)
	client := fake.NewSimpleClientset(pod)
	var lookups atomic.Int64
	client.PrependReactor("get", "pods", func(k8stesting.Action) (bool, runtime.Object, error) {
		if lookups.Add(1) == 1 {
			return true, nil, errors.New("temporary lookup failure")
		}
		return false, nil, nil
	})
	svc := newTeamQuotaRecoveryTestService(client, store, nil)
	recovery := NewTeamQuotaRecoveryController(
		svc,
		store,
		TeamQuotaRecoveryConfig{
			ClusterID:  "cluster-a",
			Interval:   5 * time.Millisecond,
			StaleAfter: time.Nanosecond,
		},
		zap.NewNop(),
	)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- recovery.Run(ctx)
	}()
	select {
	case <-store.transferCommitted:
	case <-time.After(time.Second):
		t.Fatal("periodic recovery did not retry the prepared transfer")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Run() error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("periodic recovery did not stop")
	}
	if lookups.Load() < 2 {
		t.Fatalf("runtime lookups = %d, want at least 2", lookups.Load())
	}
}

func TestRecoveryAllocationDueUsesPostgreSQLDecision(t *testing.T) {
	operation := teamquota.Operation{ID: "operation-a", Kind: "claim", Generation: 1}
	futureAccordingToProcess := time.Now().Add(time.Hour)
	if !recoveryAllocationDue(&teamquota.RecoveryAllocation{
		Operation:      &operation,
		ReconcileAfter: &futureAccordingToProcess,
		ReconcileDue:   true,
	}) {
		t.Fatal("database-due allocation was rejected by the process clock")
	}

	pastAccordingToProcess := time.Now().Add(-time.Hour)
	if recoveryAllocationDue(&teamquota.RecoveryAllocation{
		Operation:      &operation,
		ReconcileAfter: &pastAccordingToProcess,
		ReconcileDue:   false,
	}) {
		t.Fatal("database-not-due allocation was accepted by the process clock")
	}
}

func TestTeamQuotaRecoveryCommitsInterruptedSandboxReservation(t *testing.T) {
	_, pod := recoveryTransferAndActivePod()
	target := activeSandboxQuotaTarget(pod)
	operation := teamquota.Operation{ID: "claim-op", Kind: "claim", Generation: 1}
	store := newRecoveryQuotaTestStore()
	store.allocations = []teamquota.RecoveryAllocation{{
		AllocationID: "allocation-a",
		Owner: teamquota.Owner{
			TeamID:    "team-a",
			Kind:      "sandbox",
			ID:        "sandbox-a",
			ClusterID: "cluster-a",
		},
		State:          "reserved",
		Operation:      &operation,
		Runtime:        sandboxTeamQuotaRuntimeRef(pod),
		Committed:      zeroSandboxTeamQuotaTarget(),
		Pending:        target,
		ReconcileAfter: ptrTo(time.Now().Add(-time.Minute)),
		ReconcileDue:   true,
	}}
	client := fake.NewSimpleClientset(pod)
	svc := newTeamQuotaRecoveryTestService(client, store, nil)
	recovery := NewTeamQuotaRecoveryController(
		svc,
		store,
		TeamQuotaRecoveryConfig{ClusterID: "cluster-a"},
		zap.NewNop(),
	)

	if err := recovery.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce() error = %v", err)
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.commits != 1 || store.aborts != 0 {
		t.Fatalf("allocation transitions = commit %d abort %d", store.commits, store.aborts)
	}
	if store.allocations[0].Operation != nil {
		t.Fatalf("allocation operation remains pending: %+v", store.allocations[0])
	}
}

func TestTeamQuotaRecoveryAdoptsSameOwnerReplacementRuntime(t *testing.T) {
	_, pod := recoveryTransferAndActivePod()
	pod.Name = "replacement-pod"
	pod.UID = types.UID("replacement-uid")
	pod.Annotations[controller.AnnotationRuntimeGeneration] = "2"
	observed := activeSandboxQuotaTarget(pod)
	pending := observed.Clone()
	pending[teamquota.KeySandboxMemoryBytes]++
	operation := teamquota.Operation{ID: "claim-op", Kind: "claim", Generation: 2}
	store := newRecoveryQuotaTestStore()
	store.allocations = []teamquota.RecoveryAllocation{{
		AllocationID: "allocation-a",
		Owner: teamquota.Owner{
			TeamID:    "team-a",
			Kind:      "sandbox",
			ID:        "sandbox-a",
			ClusterID: "cluster-a",
		},
		State:     "reserved",
		Operation: &operation,
		Runtime: teamquota.RuntimeRef{
			Namespace:  pod.Namespace,
			Name:       "previous-pod",
			UID:        "previous-uid",
			Generation: 1,
		},
		Committed:      zeroSandboxTeamQuotaTarget(),
		Pending:        pending,
		ReconcileAfter: ptrTo(time.Now().Add(-time.Minute)),
		ReconcileDue:   true,
	}}
	recovery := NewTeamQuotaRecoveryController(
		newTeamQuotaRecoveryTestService(fake.NewSimpleClientset(pod), store, nil),
		store,
		TeamQuotaRecoveryConfig{ClusterID: "cluster-a"},
		zap.NewNop(),
	)
	inventory := &teamQuotaSandboxInventory{
		pods:    map[string]*corev1.Pod{"sandbox-a": pod},
		records: map[string]*SandboxRecord{},
	}

	require.NoError(t, recovery.recoverSandboxAllocation(
		context.Background(),
		inventory,
		&store.allocations[0],
	))

	store.mu.Lock()
	defer store.mu.Unlock()
	assert.Equal(t, 1, store.attaches)
	assert.Equal(t, 1, store.observedCommits)
	assert.Equal(t, 0, store.commits)
	assert.Equal(t, 0, store.aborts)
	assert.True(t, quotaValuesEqual(store.allocations[0].Committed, observed))
	assert.Equal(t, string(pod.UID), store.allocations[0].Runtime.UID)
	assert.Nil(t, store.allocations[0].Operation)
}

func TestTeamQuotaRecoveryAbortsReservedMutationWhenObservedEqualsCommitted(t *testing.T) {
	_, pod := recoveryTransferAndActivePod()
	observed := activeSandboxQuotaTarget(pod)
	pending := observed.Clone()
	pending[teamquota.KeySandboxMemoryBytes]++
	operation := teamquota.Operation{ID: "resize-op", Kind: "resize", Generation: 2}
	store := newRecoveryQuotaTestStore()
	store.allocations = []teamquota.RecoveryAllocation{{
		AllocationID: "allocation-a",
		Owner: teamquota.Owner{
			TeamID:    "team-a",
			Kind:      "sandbox",
			ID:        "sandbox-a",
			ClusterID: "cluster-a",
		},
		State:     "reserved",
		Operation: &operation,
		Runtime: teamquota.RuntimeRef{
			Namespace:  pod.Namespace,
			Name:       "previous-pod",
			UID:        "previous-uid",
			Generation: 1,
		},
		Committed:      observed,
		Pending:        pending,
		ReconcileAfter: ptrTo(time.Now().Add(-time.Minute)),
		ReconcileDue:   true,
	}}
	recovery := NewTeamQuotaRecoveryController(
		newTeamQuotaRecoveryTestService(fake.NewSimpleClientset(pod), store, nil),
		store,
		TeamQuotaRecoveryConfig{ClusterID: "cluster-a"},
		zap.NewNop(),
	)
	inventory := &teamQuotaSandboxInventory{
		pods:    map[string]*corev1.Pod{"sandbox-a": pod},
		records: map[string]*SandboxRecord{},
	}

	require.NoError(t, recovery.recoverSandboxAllocation(
		context.Background(),
		inventory,
		&store.allocations[0],
	))

	store.mu.Lock()
	defer store.mu.Unlock()
	assert.Equal(t, 0, store.attaches)
	assert.Equal(t, 0, store.observedCommits)
	assert.Equal(t, 0, store.commits)
	assert.Equal(t, 1, store.aborts)
	assert.True(t, quotaValuesEqual(store.allocations[0].Committed, observed))
	assert.Nil(t, store.allocations[0].Operation)
}

func TestTeamQuotaRecoveryReleaseLookupErrorDoesNotConfirm(t *testing.T) {
	operation := teamquota.Operation{ID: "delete-op", Kind: "delete_runtime", Generation: 1}
	store := newRecoveryQuotaTestStore()
	store.allocations = []teamquota.RecoveryAllocation{{
		AllocationID: "allocation-a",
		Owner: teamquota.Owner{
			TeamID:    "team-a",
			Kind:      "sandbox",
			ID:        "sandbox-a",
			ClusterID: "cluster-a",
		},
		State:     "releasing",
		Operation: &operation,
		Runtime: teamquota.RuntimeRef{
			Namespace:  "sandbox",
			Name:       "pod-a",
			UID:        "pod-uid",
			Generation: 1,
		},
		Committed:      activeSandboxQuotaTarget(nil),
		Pending:        zeroSandboxTeamQuotaTarget(),
		ReconcileAfter: ptrTo(time.Now().Add(-time.Minute)),
		ReconcileDue:   true,
	}}
	client := fake.NewSimpleClientset()
	client.PrependReactor("get", "pods", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("Kubernetes API unavailable")
	})
	svc := newTeamQuotaRecoveryTestService(client, store, nil)
	recovery := NewTeamQuotaRecoveryController(
		svc,
		store,
		TeamQuotaRecoveryConfig{ClusterID: "cluster-a"},
		zap.NewNop(),
	)

	if err := recovery.RunOnce(context.Background()); err == nil {
		t.Fatal("RunOnce() error = nil")
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.confirms != 0 || store.allocations[0].Operation == nil {
		t.Fatalf("lookup failure released allocation: %+v", store.allocations[0])
	}
}

func TestTeamQuotaStartupRecoveryReleasesOrphanWarmPoolAllocation(t *testing.T) {
	store := newRecoveryQuotaTestStore()
	store.allocations = []teamquota.RecoveryAllocation{{
		AllocationID: "warm-pool-a",
		Owner: teamquota.Owner{
			TeamID:    "team-a",
			Kind:      "warm_pool",
			ID:        "cluster-a/logical-a",
			ClusterID: "cluster-a",
		},
		State: "active",
		Committed: teamquota.Values{
			teamquota.KeySandboxRuntimeCount: 3,
		},
	}}
	client := fake.NewSimpleClientset()
	svc := newTeamQuotaRecoveryTestService(client, store, nil)
	recovery := NewTeamQuotaRecoveryController(
		svc,
		store,
		TeamQuotaRecoveryConfig{ClusterID: "cluster-a"},
		zap.NewNop(),
	)

	if err := recovery.RecoverStartup(context.Background()); err != nil {
		t.Fatalf("RecoverStartup() error = %v", err)
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if len(store.reconciles) != 1 {
		t.Fatalf("reconcile calls = %d, want 1", len(store.reconciles))
	}
	if store.reconciles[0].owner != store.allocations[0].Owner {
		t.Fatalf("reconciled owner = %+v, want %+v", store.reconciles[0].owner, store.allocations[0].Owner)
	}
	if !quotaValuesEqual(store.reconciles[0].target, zeroWarmPoolTeamQuotaTarget()) {
		t.Fatalf("reconciled target = %+v, want zero warm pool target", store.reconciles[0].target)
	}
}

func TestTeamQuotaStartupRecoveryKeepsWarmPoolAllocationWhenReplicaSetExists(t *testing.T) {
	owner := teamquota.Owner{
		TeamID:    "team-a",
		Kind:      "warm_pool",
		ID:        "cluster-a/logical-a",
		ClusterID: "cluster-a",
	}
	store := newRecoveryQuotaTestStore()
	store.allocations = []teamquota.RecoveryAllocation{{
		AllocationID: "warm-pool-a",
		Owner:        owner,
		State:        "active",
		Committed: teamquota.Values{
			teamquota.KeySandboxRuntimeCount: 3,
		},
	}}
	rs := &appsv1.ReplicaSet{
		ObjectMeta: metav1.ObjectMeta{Name: "pool-a", Namespace: "sandbox"},
		Spec: appsv1.ReplicaSetSpec{
			Template: corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{
				Labels: map[string]string{
					controller.LabelTemplateLogicalID: "logical-a",
					controller.LabelPoolType:          controller.PoolTypeIdle,
				},
				Annotations: map[string]string{
					controller.AnnotationTeamID:    "team-a",
					controller.AnnotationOwnerKind: controller.OwnerKindTeamWarmPool,
				},
			}},
		},
	}
	client := fake.NewSimpleClientset(rs)
	svc := newTeamQuotaRecoveryTestService(client, store, nil)
	recovery := NewTeamQuotaRecoveryController(
		svc,
		store,
		TeamQuotaRecoveryConfig{ClusterID: "cluster-a"},
		zap.NewNop(),
	)

	if err := recovery.RecoverStartup(context.Background()); err != nil {
		t.Fatalf("RecoverStartup() error = %v", err)
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if len(store.reconciles) != 0 {
		t.Fatalf("existing ReplicaSet allocation was reconciled: %+v", store.reconciles)
	}
}

func TestTeamQuotaStartupRecoveryListErrorDoesNotReleaseWarmPoolAllocation(t *testing.T) {
	store := newRecoveryQuotaTestStore()
	store.allocations = []teamquota.RecoveryAllocation{{
		AllocationID: "warm-pool-a",
		Owner: teamquota.Owner{
			TeamID:    "team-a",
			Kind:      "warm_pool",
			ID:        "cluster-a/logical-a",
			ClusterID: "cluster-a",
		},
		State: "active",
	}}
	client := fake.NewSimpleClientset()
	client.PrependReactor("list", "replicasets", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("Kubernetes API unavailable")
	})
	svc := newTeamQuotaRecoveryTestService(client, store, nil)
	recovery := NewTeamQuotaRecoveryController(
		svc,
		store,
		TeamQuotaRecoveryConfig{ClusterID: "cluster-a"},
		zap.NewNop(),
	)

	if err := recovery.RecoverStartup(context.Background()); err == nil {
		t.Fatal("RecoverStartup() error = nil")
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if len(store.reconciles) != 0 {
		t.Fatalf("lookup failure released warm-pool allocation: %+v", store.reconciles)
	}
}

func TestTeamQuotaStartupRecoveryRetainsInterruptedWarmPoolScaleWithoutReplicaSet(t *testing.T) {
	operation := teamquota.Operation{ID: "scale-a", Kind: "scale_warm_pool"}
	store := newRecoveryQuotaTestStore()
	store.allocations = []teamquota.RecoveryAllocation{{
		AllocationID: "warm-pool-a",
		Owner: teamquota.Owner{
			TeamID:    "team-a",
			Kind:      "warm_pool",
			ID:        "cluster-a/logical-a",
			ClusterID: "cluster-a",
		},
		State:              "reserved",
		Operation:          &operation,
		OperationBaseState: "active",
		ReconcileAfter:     ptrTo(time.Now().Add(-time.Minute)),
		ReconcileDue:       true,
	}}
	client := fake.NewSimpleClientset()
	svc := newTeamQuotaRecoveryTestService(client, store, nil)
	recovery := NewTeamQuotaRecoveryController(
		svc,
		store,
		TeamQuotaRecoveryConfig{ClusterID: "cluster-a"},
		zap.NewNop(),
	)

	if err := recovery.RecoverStartup(context.Background()); err != nil {
		t.Fatalf("RecoverStartup() error = %v", err)
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.aborts != 0 || store.commits != 0 || len(store.reconciles) != 0 {
		t.Fatalf(
			"ambiguous missing ReplicaSet changed quota: abort %d commit %d reconcile %d",
			store.aborts,
			store.commits,
			len(store.reconciles),
		)
	}
	if store.allocations[0].Operation == nil {
		t.Fatal("ambiguous warm-pool operation was cleared")
	}
}

func TestTeamQuotaStartupRecoveryCommitsAppliedWarmPoolScale(t *testing.T) {
	template, owner, rs := recoveryWarmPoolTemplateAndReplicaSet(t, 2)
	pending, err := testWarmPoolTeamQuotaTarget(template, 2)
	require.NoError(t, err)
	operation := teamquota.Operation{ID: "scale-applied", Kind: "scale_warm_pool"}
	store := newRecoveryQuotaTestStore()
	store.allocations = []teamquota.RecoveryAllocation{{
		AllocationID:       "warm-pool-applied",
		Revision:           1,
		Owner:              owner,
		State:              "reserved",
		Operation:          &operation,
		OperationBaseState: "active",
		Committed:          zeroWarmPoolTeamQuotaTarget(),
		Pending:            pending,
		ReconcileAfter:     ptrTo(time.Now().Add(-time.Minute)),
		ReconcileDue:       true,
	}}
	svc := newTeamQuotaRecoveryTestService(fake.NewSimpleClientset(rs), store, nil)
	svc.templateLister = staticTemplateLister{templates: []*v1alpha1.SandboxTemplate{template}}
	recovery := NewTeamQuotaRecoveryController(
		svc,
		store,
		TeamQuotaRecoveryConfig{ClusterID: owner.ClusterID},
		zap.NewNop(),
	)

	require.NoError(t, recovery.RecoverStartup(context.Background()))

	store.mu.Lock()
	defer store.mu.Unlock()
	assert.Equal(t, 1, store.commits)
	assert.Equal(t, 0, store.aborts)
	require.Len(t, store.allocations, 1)
	assert.Nil(t, store.allocations[0].Operation)
	assert.True(t, quotaValuesEqual(store.allocations[0].Committed, pending))
}

func TestTeamQuotaStartupRecoveryAbortsUnappliedWarmPoolScale(t *testing.T) {
	template, owner, rs := recoveryWarmPoolTemplateAndReplicaSet(t, 1)
	committed, err := testWarmPoolTeamQuotaTarget(template, 1)
	require.NoError(t, err)
	pending, err := testWarmPoolTeamQuotaTarget(template, 2)
	require.NoError(t, err)
	operation := teamquota.Operation{ID: "scale-unapplied", Kind: "scale_warm_pool"}
	store := newRecoveryQuotaTestStore()
	store.allocations = []teamquota.RecoveryAllocation{{
		AllocationID:       "warm-pool-unapplied",
		Revision:           1,
		Owner:              owner,
		State:              "reserved",
		Operation:          &operation,
		OperationBaseState: "active",
		Committed:          committed,
		Pending:            pending,
		ReconcileAfter:     ptrTo(time.Now().Add(-time.Minute)),
		ReconcileDue:       true,
	}}
	svc := newTeamQuotaRecoveryTestService(fake.NewSimpleClientset(rs), store, nil)
	svc.templateLister = staticTemplateLister{templates: []*v1alpha1.SandboxTemplate{template}}
	recovery := NewTeamQuotaRecoveryController(
		svc,
		store,
		TeamQuotaRecoveryConfig{ClusterID: owner.ClusterID},
		zap.NewNop(),
	)

	require.NoError(t, recovery.RecoverStartup(context.Background()))

	store.mu.Lock()
	defer store.mu.Unlock()
	assert.Equal(t, 0, store.commits)
	assert.Equal(t, 1, store.aborts)
	require.Len(t, store.allocations, 1)
	assert.Nil(t, store.allocations[0].Operation)
	assert.True(t, quotaValuesEqual(store.allocations[0].Committed, committed))
}

func TestTeamQuotaRecoverySkipsStaleSandboxInventorySnapshot(t *testing.T) {
	owner := teamquota.Owner{
		TeamID:    "team-a",
		Kind:      "sandbox",
		ID:        "sandbox-a",
		ClusterID: "cluster-a",
	}
	committed := pausedSandboxTeamQuotaTarget()
	store := newRecoveryQuotaTestStore()
	store.allocations = []teamquota.RecoveryAllocation{{
		AllocationID: "sandbox-a",
		Revision:     1,
		Owner:        owner,
		State:        "active",
		Committed:    committed.Clone(),
	}}
	svc := newTeamQuotaRecoveryTestService(fake.NewSimpleClientset(), store, nil)
	recordStore := svc.sandboxStore.(*recoverySandboxRecordStore)
	recordStore.onList = func() {
		store.mu.Lock()
		defer store.mu.Unlock()
		store.allocations[0].Revision++
		store.allocations[0].Committed = committed.Clone()
		store.allocations[0].Committed[teamquota.KeySandboxRuntimeCount] = 1
	}
	recovery := NewTeamQuotaRecoveryController(
		svc,
		store,
		TeamQuotaRecoveryConfig{ClusterID: owner.ClusterID},
		zap.NewNop(),
	)

	require.NoError(t, recovery.RecoverStartup(context.Background()))

	store.mu.Lock()
	defer store.mu.Unlock()
	assert.Empty(t, store.reconciles)
	assert.Equal(t, int64(2), store.allocations[0].Revision)
	assert.Equal(t, int64(1), store.allocations[0].Committed[teamquota.KeySandboxRuntimeCount])
}

func recoveryWarmPoolTemplateAndReplicaSet(
	t *testing.T,
	replicas int32,
) (*v1alpha1.SandboxTemplate, teamquota.Owner, *appsv1.ReplicaSet) {
	t.Helper()
	template := newSandboxResourceTestTemplate(t)
	template.Labels = map[string]string{
		controller.LabelTemplateScope: naming.ScopeTeam,
	}
	template.Annotations = map[string]string{
		controller.AnnotationTemplateTeamID: "team-a",
	}
	owner, ok := controller.TeamWarmPoolQuotaOwner(template)
	require.True(t, ok)
	rsName, err := naming.ReplicasetName(owner.ClusterID, template.Name)
	require.NoError(t, err)
	return template, owner, &appsv1.ReplicaSet{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: template.Namespace,
			Name:      rsName,
		},
		Spec: appsv1.ReplicaSetSpec{
			Replicas: &replicas,
			Template: corev1.PodTemplateSpec{
				Spec: v1alpha1.BuildIdlePodSpec(template),
			},
		},
	}
}

func recoveryTransferAndActivePod() (teamquota.RecoveryTransfer, *corev1.Pod) {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "sandbox",
			Name:      "pod-a",
			UID:       types.UID("pod-uid"),
			Labels: map[string]string{
				controller.LabelPoolType:  controller.PoolTypeActive,
				controller.LabelSandboxID: "sandbox-a",
			},
			Annotations: map[string]string{
				controller.AnnotationSandboxID:         "sandbox-a",
				controller.AnnotationTeamID:            "team-a",
				controller.AnnotationRuntimeGeneration: "1",
			},
		},
		Spec: corev1.PodSpec{Containers: []corev1.Container{{
			Name: "sandbox",
		}}},
	}
	transfer := teamquota.RecoveryTransfer{
		Operation: teamquota.Operation{ID: "transfer-op", Kind: "claim_warm_pool_transfer", Generation: 1},
		Source: teamquota.Owner{
			TeamID:    "team-a",
			Kind:      "warm_pool",
			ID:        "cluster-a/template-a",
			ClusterID: "cluster-a",
		},
		Destination: teamquota.Owner{
			TeamID:    "team-a",
			Kind:      "sandbox",
			ID:        "sandbox-a",
			ClusterID: "cluster-a",
		},
		Runtime: teamquota.RuntimeRef{
			Namespace:  pod.Namespace,
			Name:       pod.Name,
			UID:        string(pod.UID),
			Generation: 1,
		},
		CreatedAt: time.Now().Add(-time.Hour),
	}
	transfer.DestinationTarget = activeSandboxQuotaTarget(pod)
	return transfer, pod
}

func newTeamQuotaRecoveryTestService(
	client *fake.Clientset,
	store *recoveryQuotaTestStore,
	records []*SandboxRecord,
) *SandboxService {
	service := &SandboxService{
		k8sClient:      client,
		clock:          systemTime{},
		logger:         zap.NewNop(),
		teamQuotaStore: store,
		sandboxStore: &recoverySandboxRecordStore{
			memorySandboxStore: &memorySandboxStore{records: map[string]*SandboxRecord{}},
			records:            records,
		},
		templateLister: staticTemplateLister{},
	}
	if store != nil {
		for index := range store.transfers {
			transfer := store.transfers[index]
			if transfer.Source.ID != "cluster-a/template-a" {
				continue
			}
			clusterID := "cluster-a"
			replicas := int32(1)
			template := &v1alpha1.SandboxTemplate{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "sandbox",
					Name:      "template-a",
					Labels: map[string]string{
						controller.LabelTemplateScope:     naming.ScopeTeam,
						controller.LabelTemplateLogicalID: "template-a",
					},
					Annotations: map[string]string{
						controller.AnnotationTemplateTeamID: "team-a",
					},
				},
				Spec: v1alpha1.SandboxTemplateSpec{
					ClusterId: &clusterID,
					Pool:      v1alpha1.PoolStrategy{MinIdle: 1},
					MainContainer: v1alpha1.ContainerSpec{
						Image: "busybox:latest",
					},
				},
			}
			rsName, err := naming.ReplicasetName(clusterID, template.Name)
			if err != nil {
				panic(err)
			}
			rs := &appsv1.ReplicaSet{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: template.Namespace,
					Name:      rsName,
					UID:       types.UID("recovery-rs-uid"),
				},
				Spec: appsv1.ReplicaSetSpec{
					Replicas: &replicas,
					Template: corev1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Labels: map[string]string{
								controller.LabelTemplateLogicalID: "template-a",
								controller.LabelPoolType:          controller.PoolTypeIdle,
								controller.LabelOwnerKind:         controller.OwnerKindTeamWarmPool,
							},
							Annotations: map[string]string{
								controller.AnnotationTeamID:    "team-a",
								controller.AnnotationOwnerKind: controller.OwnerKindTeamWarmPool,
							},
						},
						Spec: v1alpha1.BuildIdlePodSpec(template),
					},
				},
			}
			_, _ = client.AppsV1().ReplicaSets(rs.Namespace).
				Create(context.Background(), rs, metav1.CreateOptions{})
			service.templateLister = staticTemplateLister{
				templates: []*v1alpha1.SandboxTemplate{template},
			}
			break
		}
	}
	return service
}

type recoverySandboxRecordStore struct {
	*memorySandboxStore
	records []*SandboxRecord
	err     error
	onList  func()
}

func (s *recoverySandboxRecordStore) ListTeamQuotaSandboxRecords(
	context.Context,
	string,
) ([]*SandboxRecord, error) {
	if s.err != nil {
		return nil, s.err
	}
	if s.onList != nil {
		s.onList()
	}
	return append([]*SandboxRecord(nil), s.records...), nil
}

type recoveryQuotaTestStore struct {
	mu                sync.Mutex
	transfers         []teamquota.RecoveryTransfer
	allocations       []teamquota.RecoveryAllocation
	transferListCalls int
	transferCommits   int
	transferAborts    int
	attaches          int
	commits           int
	observedCommits   int
	aborts            int
	confirms          int
	reconciles        []recoveryReconcileCall
	transferCommitted chan struct{}
	releases          int
}

type recoveryReconcileCall struct {
	owner  teamquota.Owner
	target teamquota.Values
}

func newRecoveryQuotaTestStore() *recoveryQuotaTestStore {
	return &recoveryQuotaTestStore{}
}

func (s *recoveryQuotaTestStore) ListRecoveryTransfers(
	_ context.Context,
	clusterID string,
	staleAfter time.Duration,
	limit int,
) ([]teamquota.RecoveryTransfer, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.transferListCalls++
	var result []teamquota.RecoveryTransfer
	for _, transfer := range s.transfers {
		if transfer.Source.ClusterID != clusterID && transfer.Destination.ClusterID != clusterID {
			continue
		}
		if transfer.CreatedAt.Add(staleAfter).After(time.Now()) {
			continue
		}
		result = append(result, transfer)
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].CreatedAt.Equal(result[j].CreatedAt) {
			return result[i].Operation.ID < result[j].Operation.ID
		}
		return result[i].CreatedAt.Before(result[j].CreatedAt)
	})
	if limit > 0 && len(result) > limit {
		result = result[:limit]
	}
	return append([]teamquota.RecoveryTransfer(nil), result...), nil
}

func (s *recoveryQuotaTestStore) ListRecoveryAllocations(
	_ context.Context,
	filter teamquota.RecoveryAllocationFilter,
) ([]teamquota.RecoveryAllocation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var result []teamquota.RecoveryAllocation
	for _, allocation := range s.allocations {
		if allocation.Owner.ClusterID != filter.ClusterID ||
			(filter.TeamID != "" && allocation.Owner.TeamID != filter.TeamID) ||
			(filter.OwnerKind != "" && allocation.Owner.Kind != filter.OwnerKind) ||
			allocation.AllocationID <= filter.AfterAllocationID ||
			(allocation.State == "released" && allocation.Operation == nil) {
			continue
		}
		result = append(result, allocation)
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].AllocationID < result[j].AllocationID
	})
	if filter.Limit > 0 && len(result) > filter.Limit {
		result = result[:filter.Limit]
	}
	return append([]teamquota.RecoveryAllocation(nil), result...), nil
}

func (s *recoveryQuotaTestStore) GetRecoveryAllocation(
	_ context.Context,
	owner teamquota.Owner,
) (*teamquota.RecoveryAllocation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := range s.allocations {
		allocation := s.allocations[i]
		if allocation.Owner.TeamID == owner.TeamID &&
			allocation.Owner.Kind == owner.Kind &&
			allocation.Owner.ID == owner.ID {
			return &allocation, nil
		}
	}
	return nil, nil
}

func (s *recoveryQuotaTestStore) ReserveTarget(context.Context, teamquota.ReserveRequest) (*teamquota.Reservation, error) {
	return nil, nil
}

func (s *recoveryQuotaTestStore) ReserveDelta(context.Context, teamquota.DeltaRequest) (*teamquota.Reservation, error) {
	return nil, nil
}

func (s *recoveryQuotaTestStore) AttachRuntime(
	_ context.Context,
	ref teamquota.OperationRef,
	runtime teamquota.RuntimeRef,
) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.attaches++
	if allocation := s.allocationForRef(ref); allocation != nil {
		allocation.Runtime = runtime
	}
	return nil
}

func (s *recoveryQuotaTestStore) Commit(_ context.Context, ref teamquota.OperationRef) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.commits++
	if allocation := s.allocationForRef(ref); allocation != nil {
		allocation.Committed = allocation.Pending.Clone()
		allocation.Pending = nil
		allocation.Operation = nil
		allocation.State = "active"
		allocation.Revision++
	}
	return nil
}

func (s *recoveryQuotaTestStore) CommitObservedExact(
	_ context.Context,
	ref teamquota.OperationRef,
	exact teamquota.Values,
) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.observedCommits++
	if allocation := s.allocationForRef(ref); allocation != nil {
		allocation.Committed = exact.Clone()
		allocation.Pending = nil
		allocation.Operation = nil
		allocation.State = "active"
		allocation.Revision++
	}
	return nil
}

func (s *recoveryQuotaTestStore) Abort(_ context.Context, ref teamquota.OperationRef, _ string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.aborts++
	if allocation := s.allocationForRef(ref); allocation != nil {
		allocation.Pending = nil
		allocation.Operation = nil
		allocation.State = allocation.OperationBaseState
		if allocation.State == "" {
			allocation.State = "active"
		}
		allocation.Revision++
	}
	return nil
}

func (s *recoveryQuotaTestStore) BeginRelease(
	_ context.Context,
	request teamquota.ReleaseRequest,
) (*teamquota.Reservation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.releases++
	return testQuotaReservation(request.Owner, request.Operation, request.Target), nil
}

func (s *recoveryQuotaTestStore) ConfirmRelease(
	_ context.Context,
	ref teamquota.OperationRef,
	_ teamquota.RuntimeRef,
) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.confirms++
	if allocation := s.allocationForRef(ref); allocation != nil {
		allocation.Committed = allocation.Pending.Clone()
		allocation.Pending = nil
		allocation.Operation = nil
		allocation.State = "released"
		allocation.Revision++
	}
	return nil
}

func (s *recoveryQuotaTestStore) ReconcileTarget(
	_ context.Context,
	owner teamquota.Owner,
	target teamquota.Values,
	_ teamquota.RuntimeRef,
) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.reconciles = append(s.reconciles, recoveryReconcileCall{
		owner:  owner,
		target: target.Clone(),
	})
	return nil
}

func (s *recoveryQuotaTestStore) ReconcileTargetIfRevision(
	_ context.Context,
	owner teamquota.Owner,
	target teamquota.Values,
	_ teamquota.RuntimeRef,
	expectedRevision int64,
) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var allocation *teamquota.RecoveryAllocation
	for index := range s.allocations {
		candidate := &s.allocations[index]
		if candidate.Owner.TeamID == owner.TeamID &&
			candidate.Owner.Kind == owner.Kind &&
			candidate.Owner.ID == owner.ID {
			allocation = candidate
			break
		}
	}
	currentRevision := int64(0)
	if allocation != nil {
		currentRevision = allocation.Revision
	}
	if currentRevision != expectedRevision {
		return false, nil
	}
	s.reconciles = append(s.reconciles, recoveryReconcileCall{
		owner:  owner,
		target: target.Clone(),
	})
	if allocation != nil {
		allocation.Revision++
		allocation.Committed = target.Clone()
		allocation.Pending = nil
		allocation.Operation = nil
	}
	return true, nil
}

func (s *recoveryQuotaTestStore) PrepareTransfer(context.Context, teamquota.TransferRequest) (*teamquota.Reservation, error) {
	return nil, nil
}

func (s *recoveryQuotaTestStore) CommitTransfer(_ context.Context, ref teamquota.OperationRef) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.transferCommits++
	s.removeTransfer(ref)
	if s.transferCommitted != nil {
		select {
		case s.transferCommitted <- struct{}{}:
		default:
		}
	}
	return nil
}

func (s *recoveryQuotaTestStore) CommitTransferObservedSource(
	ctx context.Context,
	ref teamquota.OperationRef,
	_ teamquota.Values,
) error {
	return s.CommitTransfer(ctx, ref)
}

func (s *recoveryQuotaTestStore) AbortTransfer(
	_ context.Context,
	ref teamquota.OperationRef,
	_ string,
) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.transferAborts++
	s.removeTransfer(ref)
	return nil
}

func (s *recoveryQuotaTestStore) TransferTarget(context.Context, teamquota.TransferRequest) (*teamquota.Reservation, error) {
	return nil, nil
}

func (s *recoveryQuotaTestStore) allocationForRef(
	ref teamquota.OperationRef,
) *teamquota.RecoveryAllocation {
	for i := range s.allocations {
		allocation := &s.allocations[i]
		if allocation.Owner.TeamID == ref.Owner.TeamID &&
			allocation.Owner.Kind == ref.Owner.Kind &&
			allocation.Owner.ID == ref.Owner.ID &&
			allocation.Operation != nil &&
			allocation.Operation.ID == ref.ID {
			return allocation
		}
	}
	return nil
}

func (s *recoveryQuotaTestStore) removeTransfer(ref teamquota.OperationRef) {
	for i := range s.transfers {
		transfer := s.transfers[i]
		if transfer.Destination.TeamID == ref.Owner.TeamID &&
			transfer.Destination.Kind == ref.Owner.Kind &&
			transfer.Destination.ID == ref.Owner.ID &&
			transfer.Operation.ID == ref.ID {
			s.transfers = append(s.transfers[:i], s.transfers[i+1:]...)
			return
		}
	}
}

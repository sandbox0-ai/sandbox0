package service

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/controller"
	"github.com/sandbox0-ai/sandbox0/pkg/naming"
	"github.com/sandbox0-ai/sandbox0/pkg/teamquota"
	"github.com/sandbox0-ai/sandbox0/pkg/tokenbucket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

func TestTeamWarmPoolHotClaimTransfersQuotaAndReplicaCommitment(t *testing.T) {
	template, pod, rs := newTeamWarmPoolTransferFixture(t)
	store := &recordingCapacityStore{}
	client := fake.NewSimpleClientset(pod.DeepCopy(), rs.DeepCopy())
	svc := &SandboxService{
		k8sClient:      client,
		podLister:      newClaimTestPodLister(t, pod),
		clock:          systemTime{},
		logger:         zap.NewNop(),
		teamQuotaStore: store,
	}

	claimed, admission, err := svc.claimIdlePodWithTeamQuota(context.Background(), template, &ClaimRequest{
		TeamID:            "team-a",
		UserID:            "user-a",
		SandboxID:         "sandbox-a",
		RuntimeGeneration: 1,
	})
	require.NoError(t, err)
	require.NotNil(t, claimed)
	require.NotNil(t, admission)
	assert.True(t, admission.Transfer)
	assert.True(t, admission.Committed)
	assert.Equal(t, controller.PoolTypeActive, claimed.Labels[controller.LabelPoolType])
	assert.Empty(t, claimed.OwnerReferences)

	store.mu.Lock()
	require.Len(t, store.prepared, 1)
	prepared := store.prepared[0]
	assert.Equal(t, "team-a", prepared.Source.TeamID)
	assert.Equal(t, "warm_pool", prepared.Source.Kind)
	assert.Equal(t, "sandbox-a", prepared.Destination.ID)
	assert.Equal(t, int64(1), prepared.SourceDecrease[teamquota.KeySandboxRuntimeCount])
	assert.True(t, quotaValuesEqual(prepared.SourceDecrease, prepared.TransitionReserve))
	assert.Equal(t, int64(1), prepared.DestinationTarget[teamquota.KeySandboxIdentityCount])
	assert.Equal(t, int64(1), prepared.DestinationTarget[teamquota.KeySandboxRuntimeCount])
	assert.Equal(t, 1, store.transferCommits)
	store.mu.Unlock()

	liveRS, err := client.AppsV1().ReplicaSets(rs.Namespace).Get(context.Background(), rs.Name, metav1.GetOptions{})
	require.NoError(t, err)
	require.NotNil(t, liveRS.Spec.Replicas)
	assert.Equal(t, int32(0), *liveRS.Spec.Replicas)
	assert.NotContains(t, liveRS.Annotations, controller.AnnotationTeamQuotaWarmPoolTransfers)

	var updates []string
	for _, action := range client.Actions() {
		if action.GetVerb() == "update" {
			updates = append(updates, action.GetResource().Resource)
		}
	}
	require.GreaterOrEqual(t, len(updates), 2)
	assert.Equal(t, "pods", updates[0])
	assert.Equal(t, "replicasets", updates[1])
}

func TestTeamWarmPoolHotClaimKeepsOldQuotaUntilDownsizeSucceeds(t *testing.T) {
	template, pod, rs := newTeamWarmPoolTransferFixture(t)
	store := newHotClaimResizeCapacityStore()
	client := fake.NewSimpleClientset(pod.DeepCopy(), rs.DeepCopy())
	resizeStarted := make(chan struct{})
	allowResize := make(chan struct{})
	var (
		startOnce   sync.Once
		releaseOnce sync.Once
	)
	defer releaseOnce.Do(func() { close(allowResize) })
	client.PrependReactor("update", "pods", func(action k8stesting.Action) (bool, runtime.Object, error) {
		if action.GetSubresource() != "resize" {
			return false, nil, nil
		}
		startOnce.Do(func() { close(resizeStarted) })
		<-allowResize
		return false, nil, nil
	})
	svc := &SandboxService{
		k8sClient:      client,
		podLister:      newClaimTestPodLister(t, pod),
		clock:          systemTime{},
		config:         SandboxServiceConfig{SandboxMemoryPerCPU: "4Gi"},
		logger:         zap.NewNop(),
		teamQuotaStore: store,
	}
	request := &ClaimRequest{
		TeamID:            "team-a",
		UserID:            "user-a",
		SandboxID:         "sandbox-a",
		RuntimeGeneration: 1,
		Config: &SandboxConfig{
			Resources: &SandboxResourceConfig{Memory: "128Mi"},
		},
	}
	type claimResult struct {
		pod       *corev1.Pod
		admission *sandboxTeamQuotaAdmission
		err       error
	}
	result := make(chan claimResult, 1)
	go func() {
		claimed, admission, err := svc.claimIdlePodWithTeamQuota(
			context.Background(),
			template,
			request,
		)
		result <- claimResult{pod: claimed, admission: admission, err: err}
	}()

	select {
	case <-resizeStarted:
	case <-time.After(time.Second):
		t.Fatal("hot claim did not reach blocked resize")
	}

	oldTarget := activeSandboxQuotaTarget(pod)
	limits, err := svc.effectiveSandboxResourceLimits(template, request.Config)
	require.NoError(t, err)
	expectedResizedPod := pod.DeepCopy()
	require.NoError(t, svc.applySandboxResourceLimits(expectedResizedPod, limits))
	resizedTarget := activeSandboxQuotaTarget(expectedResizedPod)
	state := store.snapshot()
	require.Len(t, state.prepared, 1)
	require.Len(t, state.resizeReservations, 1)
	assert.True(t, quotaValuesEqual(oldTarget, state.prepared[0].DestinationTarget))
	assert.True(t, quotaValuesEqual(oldTarget, state.committed))
	assert.True(t, quotaValuesEqual(resizedTarget, state.resizeReservations[0].Target))
	assert.Equal(t, 1, state.transferCommits)
	assert.Equal(t, 0, state.resizeCommits)

	releaseOnce.Do(func() { close(allowResize) })
	select {
	case got := <-result:
		require.NoError(t, got.err)
		require.NotNil(t, got.pod)
		require.NotNil(t, got.admission)
		assert.True(t, got.admission.Committed)
	case <-time.After(time.Second):
		t.Fatal("hot claim did not finish after resize was released")
	}
	state = store.snapshot()
	assert.Equal(t, 1, state.resizeCommits)
	assert.True(t, quotaValuesEqual(resizedTarget, state.committed))
}

func TestTeamWarmPoolHotClaimResizeFailureKeepsOldQuotaCommitted(t *testing.T) {
	template, pod, rs := newTeamWarmPoolTransferFixture(t)
	store := newHotClaimResizeCapacityStore()
	client := fake.NewSimpleClientset(pod.DeepCopy(), rs.DeepCopy())
	client.PrependReactor("update", "pods", func(action k8stesting.Action) (bool, runtime.Object, error) {
		if action.GetSubresource() == "resize" {
			return true, nil, errors.New("forced resize failure")
		}
		return false, nil, nil
	})
	svc := &SandboxService{
		k8sClient:      client,
		podLister:      newClaimTestPodLister(t, pod),
		clock:          systemTime{},
		config:         SandboxServiceConfig{SandboxMemoryPerCPU: "4Gi"},
		logger:         zap.NewNop(),
		teamQuotaStore: store,
	}

	claimed, admission, err := svc.claimIdlePodWithTeamQuota(
		context.Background(),
		template,
		&ClaimRequest{
			TeamID:            "team-a",
			UserID:            "user-a",
			SandboxID:         "sandbox-a",
			RuntimeGeneration: 1,
			Config: &SandboxConfig{
				Resources: &SandboxResourceConfig{Memory: "128Mi"},
			},
		},
	)
	require.ErrorContains(t, err, "forced resize failure")
	require.NotNil(t, claimed)
	require.NotNil(t, admission)
	assert.True(t, admission.Committed)

	state := store.snapshot()
	require.Len(t, state.prepared, 1)
	require.Len(t, state.resizeReservations, 1)
	assert.True(t, quotaValuesEqual(activeSandboxQuotaTarget(pod), state.prepared[0].DestinationTarget))
	assert.True(t, quotaValuesEqual(activeSandboxQuotaTarget(pod), state.committed))
	assert.Equal(t, 1, state.transferCommits)
	assert.Equal(t, 0, state.resizeCommits)
	_, getErr := client.CoreV1().Pods(pod.Namespace).Get(
		context.Background(),
		pod.Name,
		metav1.GetOptions{},
	)
	require.Error(t, getErr)
}

func TestTeamWarmPoolHotClaimAbortsOnDefinitePodUpdateRejection(t *testing.T) {
	template, pod, rs := newTeamWarmPoolTransferFixture(t)
	store := &recordingCapacityStore{}
	client := fake.NewSimpleClientset(pod.DeepCopy(), rs.DeepCopy())
	client.PrependReactor("update", "pods", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, k8serrors.NewConflict(
			schema.GroupResource{Resource: "pods"},
			pod.Name,
			errors.New("stale resource version"),
		)
	})
	svc := &SandboxService{
		k8sClient:      client,
		podLister:      newClaimTestPodLister(t, pod),
		clock:          systemTime{},
		logger:         zap.NewNop(),
		teamQuotaStore: store,
	}

	claimed, admission, err := svc.claimIdlePodWithTeamQuota(context.Background(), template, &ClaimRequest{
		TeamID:            "team-a",
		UserID:            "user-a",
		SandboxID:         "sandbox-a",
		RuntimeGeneration: 1,
	})
	require.NoError(t, err)
	assert.Nil(t, claimed)
	assert.Nil(t, admission)

	store.mu.Lock()
	assert.Equal(t, 1, store.transferAborts)
	assert.Equal(t, 0, store.transferCommits)
	store.mu.Unlock()
	liveRS, getErr := client.AppsV1().ReplicaSets(rs.Namespace).Get(context.Background(), rs.Name, metav1.GetOptions{})
	require.NoError(t, getErr)
	require.NotNil(t, liveRS.Spec.Replicas)
	assert.Equal(t, int32(1), *liveRS.Spec.Replicas)
}

func TestTeamWarmPoolHotClaimConflictDoesNotReuseTransferOperationForColdFallback(t *testing.T) {
	template, pod, rs := newTeamWarmPoolTransferFixture(t)
	store := &operationReuseRejectingCapacityStore{}
	client := fake.NewSimpleClientset(pod.DeepCopy(), rs.DeepCopy())
	client.PrependReactor("update", "pods", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, k8serrors.NewConflict(
			schema.GroupResource{Resource: "pods"},
			pod.Name,
			errors.New("stale resource version"),
		)
	})
	svc := &SandboxService{
		k8sClient:      client,
		podLister:      newClaimTestPodLister(t, pod),
		clock:          systemTime{},
		logger:         zap.NewNop(),
		teamQuotaStore: store,
	}
	request := &ClaimRequest{
		TeamID:            "team-a",
		UserID:            "user-a",
		SandboxID:         "sandbox-a",
		RuntimeGeneration: 1,
		OperationID:       "request-operation-a",
	}

	claimed, admission, err := svc.claimIdlePodWithTeamQuota(
		context.Background(),
		template,
		request,
	)
	require.NoError(t, err)
	assert.Nil(t, claimed)
	assert.Nil(t, admission)

	reservation, err := svc.reserveSandboxTeamQuota(
		context.Background(),
		request,
		template,
		"claim",
	)
	require.NoError(t, err)
	require.NotNil(t, reservation)
	assert.Equal(t, request.OperationID, store.reserveOperationID)
	assert.NotEqual(t, store.transferOperationID, store.reserveOperationID)
	assert.Equal(
		t,
		warmPoolTransferOperationID(request.OperationID, string(pod.UID)),
		store.transferOperationID,
	)
}

func TestTeamWarmPoolHotClaimCommitsAfterAmbiguousAppliedPodUpdate(t *testing.T) {
	template, pod, rs := newTeamWarmPoolTransferFixture(t)
	store := &recordingCapacityStore{}
	client := fake.NewSimpleClientset(pod.DeepCopy(), rs.DeepCopy())
	updateCalls := 0
	client.PrependReactor("update", "pods", func(action k8stesting.Action) (bool, runtime.Object, error) {
		updateCalls++
		if updateCalls > 1 {
			return false, nil, nil
		}
		update := action.(k8stesting.UpdateAction)
		updated := update.GetObject().(*corev1.Pod).DeepCopy()
		err := client.Tracker().Update(
			corev1.SchemeGroupVersion.WithResource("pods"),
			updated,
			updated.Namespace,
		)
		if err != nil {
			t.Fatalf("persist ambiguous pod update: %v", err)
		}
		return true, nil, errors.New("update response timed out")
	})
	svc := &SandboxService{
		k8sClient:      client,
		podLister:      newClaimTestPodLister(t, pod),
		clock:          systemTime{},
		logger:         zap.NewNop(),
		teamQuotaStore: store,
	}

	claimed, admission, err := svc.claimIdlePodWithTeamQuota(
		context.Background(),
		template,
		&ClaimRequest{
			TeamID:            "team-a",
			UserID:            "user-a",
			SandboxID:         "sandbox-a",
			RuntimeGeneration: 1,
		},
	)
	require.NoError(t, err)
	require.NotNil(t, claimed)
	require.NotNil(t, admission)
	assert.True(t, admission.Committed)
	store.mu.Lock()
	defer store.mu.Unlock()
	assert.Equal(t, 1, store.transferCommits)
	assert.Equal(t, 0, store.transferAborts)
}

func TestTeamWarmPoolHotClaimRetainsPreparedTransferAfterAmbiguousPodUpdate(t *testing.T) {
	template, pod, rs := newTeamWarmPoolTransferFixture(t)
	store := &recordingCapacityStore{}
	client := fake.NewSimpleClientset(pod.DeepCopy(), rs.DeepCopy())
	client.PrependReactor("update", "pods", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("update response timed out")
	})
	svc := &SandboxService{
		k8sClient:      client,
		podLister:      newClaimTestPodLister(t, pod),
		clock:          systemTime{},
		logger:         zap.NewNop(),
		teamQuotaStore: store,
	}

	claimed, admission, err := svc.claimIdlePodWithTeamQuota(
		context.Background(),
		template,
		&ClaimRequest{
			TeamID:            "team-a",
			UserID:            "user-a",
			SandboxID:         "sandbox-a",
			RuntimeGeneration: 1,
		},
	)
	require.ErrorContains(t, err, "outcome remains ambiguous")
	assert.Nil(t, claimed)
	assert.Nil(t, admission)

	store.mu.Lock()
	assert.Len(t, store.prepared, 1)
	assert.Equal(t, 0, store.transferCommits)
	assert.Equal(t, 0, store.transferAborts)
	store.mu.Unlock()

	livePod, getErr := client.CoreV1().Pods(pod.Namespace).
		Get(context.Background(), pod.Name, metav1.GetOptions{})
	require.NoError(t, getErr)
	assert.Equal(t, controller.PoolTypeIdle, livePod.Labels[controller.LabelPoolType])
	liveRS, getErr := client.AppsV1().ReplicaSets(rs.Namespace).
		Get(context.Background(), rs.Name, metav1.GetOptions{})
	require.NoError(t, getErr)
	require.NotNil(t, liveRS.Spec.Replicas)
	assert.Equal(t, int32(1), *liveRS.Spec.Replicas)
}

func TestWarmPoolReplicaCommitmentHandlesConcurrentBurstIdempotently(t *testing.T) {
	const burst = 100

	template, _, rs := newTeamWarmPoolTransferFixture(t)
	replicas := int32(burst)
	rs.Spec.Replicas = &replicas
	rs.Spec.Template.Spec = v1alpha1.BuildIdlePodSpec(template)
	rs.ResourceVersion = "1"
	client := fake.NewSimpleClientset(rs.DeepCopy())

	var updateMu sync.Mutex
	client.PrependReactor("update", "replicasets", func(action k8stesting.Action) (bool, runtime.Object, error) {
		updateMu.Lock()
		defer updateMu.Unlock()

		update := action.(k8stesting.UpdateAction)
		candidate := update.GetObject().(*appsv1.ReplicaSet).DeepCopy()
		currentObject, err := client.Tracker().Get(
			appsv1.SchemeGroupVersion.WithResource("replicasets"),
			candidate.Namespace,
			candidate.Name,
		)
		if err != nil {
			return true, nil, err
		}
		current := currentObject.(*appsv1.ReplicaSet)
		if candidate.ResourceVersion != current.ResourceVersion {
			return true, nil, k8serrors.NewConflict(
				schema.GroupResource{Group: "apps", Resource: "replicasets"},
				candidate.Name,
				errors.New("stale resource version"),
			)
		}
		resourceVersion, err := strconv.Atoi(current.ResourceVersion)
		if err != nil {
			return true, nil, err
		}
		candidate.ResourceVersion = strconv.Itoa(resourceVersion + 1)
		if err := client.Tracker().Update(
			appsv1.SchemeGroupVersion.WithResource("replicasets"),
			candidate,
			candidate.Namespace,
		); err != nil {
			return true, nil, err
		}
		return true, candidate.DeepCopy(), nil
	})

	source, ok := teamWarmPoolOwnerForClaim(template)
	require.True(t, ok)
	commitments := make([]*warmPoolReplicaCommitment, burst)
	for index := range commitments {
		commitments[index] = &warmPoolReplicaCommitment{
			namespace:   rs.Namespace,
			name:        rs.Name,
			uid:         string(rs.UID),
			operationID: "transfer-" + strconv.Itoa(index),
			template:    template,
			source:      source,
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	start := make(chan struct{})
	errs := make(chan error, burst)
	var wg sync.WaitGroup
	for _, commitment := range commitments {
		wg.Add(1)
		go func(commitment *warmPoolReplicaCommitment) {
			defer wg.Done()
			<-start
			errs <- (&SandboxService{k8sClient: client}).
				releaseWarmPoolReplicaCommitment(ctx, commitment)
		}(commitment)
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}

	liveRS, err := client.AppsV1().ReplicaSets(rs.Namespace).
		Get(context.Background(), rs.Name, metav1.GetOptions{})
	require.NoError(t, err)
	require.NotNil(t, liveRS.Spec.Replicas)
	assert.Equal(t, int32(0), *liveRS.Spec.Replicas)
	markers, err := warmPoolTransferMarkers(liveRS)
	require.NoError(t, err)
	assert.Len(t, markers, burst)

	updatesBefore := countClientActions(client.Actions(), "update", "replicasets")
	require.NoError(t, (&SandboxService{k8sClient: client}).
		releaseWarmPoolReplicaCommitment(context.Background(), commitments[0]))
	assert.Equal(
		t,
		updatesBefore,
		countClientActions(client.Actions(), "update", "replicasets"),
	)
}

func TestWarmPoolReplicaCommitmentWaitsForReplacementToDrain(t *testing.T) {
	template, _, rs := newTeamWarmPoolTransferFixture(t)
	zero := int32(0)
	rs.Spec.Replicas = &zero
	rs.Spec.Template.Spec = v1alpha1.BuildIdlePodSpec(template)
	replacement := newSandboxResourceTestIdlePod(t, template, "replacement-a")
	replacement.UID = types.UID("replacement-uid")
	replacement.Labels[controller.LabelTemplateLogicalID] = controller.TemplateLogicalID(template)
	replacement.Labels[controller.LabelOwnerKind] = controller.OwnerKindTeamWarmPool
	replacement.Annotations[controller.AnnotationTeamID] = "team-a"
	replacement.Annotations[controller.AnnotationOwnerKind] = controller.OwnerKindTeamWarmPool
	replacement.OwnerReferences = []metav1.OwnerReference{{
		APIVersion: "apps/v1",
		Kind:       "ReplicaSet",
		Name:       rs.Name,
		UID:        rs.UID,
		Controller: ptrTo(true),
	}}
	source, ok := teamWarmPoolOwnerForClaim(template)
	require.True(t, ok)
	commitment := &warmPoolReplicaCommitment{
		namespace:     rs.Namespace,
		name:          rs.Name,
		uid:           string(rs.UID),
		operationID:   "transfer-a",
		replicasAfter: 0,
		template:      template,
		source:        source,
	}
	client := fake.NewSimpleClientset(rs.DeepCopy(), replacement.DeepCopy())
	svc := &SandboxService{k8sClient: client}

	type waitResult struct {
		target teamquota.Values
		err    error
	}
	result := make(chan waitResult, 1)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	go func() {
		target, err := svc.waitForWarmPoolReplicaCommitment(ctx, commitment)
		result <- waitResult{target: target, err: err}
	}()

	select {
	case got := <-result:
		t.Fatalf("wait completed while replacement was still live: target=%v err=%v", got.target, got.err)
	case <-time.After(50 * time.Millisecond):
	}
	require.NoError(t, client.CoreV1().Pods(replacement.Namespace).
		Delete(context.Background(), replacement.Name, metav1.DeleteOptions{}))
	select {
	case got := <-result:
		require.NoError(t, got.err)
		assert.Equal(t, int64(0), got.target[teamquota.KeySandboxRuntimeCount])
	case <-time.After(time.Second):
		t.Fatal("wait did not complete after replacement drained")
	}
}

func TestWarmPoolTransferMarkersRejectInvalidState(t *testing.T) {
	for name, raw := range map[string]string{
		"empty operation":  `{"":0}`,
		"negative replica": `{"transfer-a":-1}`,
	} {
		t.Run(name, func(t *testing.T) {
			_, err := warmPoolTransferMarkers(&appsv1.ReplicaSet{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "sandbox",
					Name:      "warm-rs",
					Annotations: map[string]string{
						controller.AnnotationTeamQuotaWarmPoolTransfers: raw,
					},
				},
			})
			require.Error(t, err)
		})
	}
}

func countClientActions(actions []k8stesting.Action, verb, resource string) int {
	count := 0
	for _, action := range actions {
		if action.GetVerb() == verb && action.GetResource().Resource == resource {
			count++
		}
	}
	return count
}

func TestTeamWarmPoolHotClaimFailsClosedWithoutCapacityStore(t *testing.T) {
	template, pod, rs := newTeamWarmPoolTransferFixture(t)
	client := fake.NewSimpleClientset(pod.DeepCopy(), rs.DeepCopy())
	svc := &SandboxService{
		k8sClient: client,
		podLister: newClaimTestPodLister(t, pod),
		clock:     systemTime{},
		logger:    zap.NewNop(),
	}

	claimed, admission, err := svc.claimIdlePodWithTeamQuota(
		context.Background(),
		template,
		&ClaimRequest{
			TeamID:            "team-a",
			UserID:            "user-a",
			SandboxID:         "sandbox-a",
			RuntimeGeneration: 1,
		},
	)
	require.ErrorIs(t, err, ErrTeamQuotaUnavailable)
	assert.Nil(t, claimed)
	assert.Nil(t, admission)
	livePod, getErr := client.CoreV1().
		Pods(pod.Namespace).
		Get(context.Background(), pod.Name, metav1.GetOptions{})
	require.NoError(t, getErr)
	assert.Equal(t, controller.PoolTypeIdle, livePod.Labels[controller.LabelPoolType])
}

func TestSandboxStartRateAdmissionForPoolClaims(t *testing.T) {
	t.Run("team-owned hot claim defers charge to replenishment", func(t *testing.T) {
		template, pod, rs := newTeamWarmPoolTransferFixture(t)
		requestedTemplateID := projectTeamWarmPoolFixtureForClaim(t, template, pod, rs)
		store := &recordingCapacityStore{}
		rateLimiter := &recordingSandboxStartRateLimiter{
			decision: tokenbucket.Decision{RetryAfter: time.Second},
		}
		client := fake.NewSimpleClientset(pod.DeepCopy(), rs.DeepCopy())
		svc := &SandboxService{
			k8sClient:            client,
			podLister:            newClaimTestPodLister(t, pod),
			templateLister:       staticTemplateLister{templates: []*v1alpha1.SandboxTemplate{template}},
			clock:                systemTime{},
			logger:               zap.NewNop(),
			teamQuotaStore:       store,
			teamQuotaRateLimiter: rateLimiter,
		}

		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()
		_, err := svc.ClaimSandbox(ctx, &ClaimRequest{
			Template: requestedTemplateID,
			TeamID:   "team-a",
			UserID:   "user-a",
		})
		require.Error(t, err)
		assert.Equal(t, 0, rateLimiter.calls)
	})

	t.Run("team-owned cold claim charges once", func(t *testing.T) {
		template, pod, rs := newTeamWarmPoolTransferFixture(t)
		requestedTemplateID := projectTeamWarmPoolFixtureForClaim(t, template, pod, rs)
		rateLimiter := &recordingSandboxStartRateLimiter{
			decision: tokenbucket.Decision{RetryAfter: time.Second},
		}
		client := fake.NewSimpleClientset()
		svc := &SandboxService{
			k8sClient:            client,
			podLister:            newClaimTestPodLister(t),
			templateLister:       staticTemplateLister{templates: []*v1alpha1.SandboxTemplate{template}},
			clock:                systemTime{},
			logger:               zap.NewNop(),
			teamQuotaStore:       &recordingCapacityStore{},
			teamQuotaRateLimiter: rateLimiter,
		}

		_, err := svc.ClaimSandbox(context.Background(), &ClaimRequest{
			Template: requestedTemplateID,
			TeamID:   "team-a",
			UserID:   "user-a",
		})
		require.ErrorIs(t, err, ErrQuotaExceeded)
		assert.Equal(t, 1, rateLimiter.calls)
	})

	t.Run("public hot claim charges before detaching idle pod", func(t *testing.T) {
		template, pod, rs := newTeamWarmPoolTransferFixture(t)
		requestedTemplateID := projectPublicWarmPoolFixtureForClaim(t, template, pod, rs)
		rateLimiter := &recordingSandboxStartRateLimiter{
			decision: tokenbucket.Decision{RetryAfter: time.Second},
		}
		client := fake.NewSimpleClientset(pod.DeepCopy(), rs.DeepCopy())
		svc := &SandboxService{
			k8sClient:            client,
			podLister:            newClaimTestPodLister(t, pod),
			templateLister:       staticTemplateLister{templates: []*v1alpha1.SandboxTemplate{template}},
			clock:                systemTime{},
			logger:               zap.NewNop(),
			teamQuotaStore:       &permissiveTeamQuotaCapacityStore{},
			teamQuotaRateLimiter: rateLimiter,
		}

		_, err := svc.ClaimSandbox(context.Background(), &ClaimRequest{
			Template: requestedTemplateID,
			TeamID:   "team-a",
			UserID:   "user-a",
		})
		require.ErrorIs(t, err, ErrQuotaExceeded)
		assert.Equal(t, 1, rateLimiter.calls)

		livePod, getErr := client.CoreV1().Pods(pod.Namespace).Get(
			context.Background(),
			pod.Name,
			metav1.GetOptions{},
		)
		require.NoError(t, getErr)
		assert.Equal(t, controller.PoolTypeIdle, livePod.Labels[controller.LabelPoolType])
	})

	t.Run("public cold claim consumes only the pre-admitted token", func(t *testing.T) {
		template, pod, rs := newTeamWarmPoolTransferFixture(t)
		requestedTemplateID := projectPublicWarmPoolFixtureForClaim(t, template, pod, rs)
		rateLimiter := &recordingSandboxStartRateLimiter{
			decision: tokenbucket.Decision{Allowed: true},
		}
		svc := &SandboxService{
			k8sClient:            fake.NewSimpleClientset(),
			podLister:            newClaimTestPodLister(t),
			templateLister:       staticTemplateLister{templates: []*v1alpha1.SandboxTemplate{template}},
			clock:                systemTime{},
			logger:               zap.NewNop(),
			teamQuotaStore:       &permissiveTeamQuotaCapacityStore{},
			teamQuotaRateLimiter: rateLimiter,
		}

		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()
		_, err := svc.ClaimSandbox(ctx, &ClaimRequest{
			Template: requestedTemplateID,
			TeamID:   "team-a",
			UserID:   "user-a",
		})
		require.Error(t, err)
		assert.Equal(t, 1, rateLimiter.calls)
	})
}

func projectTeamWarmPoolFixtureForClaim(
	t *testing.T,
	template *v1alpha1.SandboxTemplate,
	pod *corev1.Pod,
	rs *appsv1.ReplicaSet,
) string {
	t.Helper()
	requestedTemplateID := template.Name
	privateName := naming.TemplateNameForCluster(naming.ScopeTeam, "team-a", requestedTemplateID)
	privateNamespace, err := naming.TemplateNamespaceForTeam("team-a")
	require.NoError(t, err)
	template.Labels[controller.LabelTemplateLogicalID] = requestedTemplateID
	template.Name = privateName
	template.Namespace = privateNamespace
	pod.Namespace = privateNamespace
	pod.Labels[controller.LabelTemplateID] = privateName
	pod.Labels[controller.LabelTemplateLogicalID] = requestedTemplateID
	rs.Namespace = privateNamespace
	hash, err := controller.TemplateSpecHash(template)
	require.NoError(t, err)
	pod.Annotations[controller.AnnotationTemplateSpecHash] = hash
	return requestedTemplateID
}

func projectPublicWarmPoolFixtureForClaim(
	t *testing.T,
	template *v1alpha1.SandboxTemplate,
	pod *corev1.Pod,
	rs *appsv1.ReplicaSet,
) string {
	t.Helper()
	requestedTemplateID := template.Name
	publicNamespace, err := naming.TemplateNamespaceForBuiltin(requestedTemplateID)
	require.NoError(t, err)
	template.Name = requestedTemplateID
	template.Namespace = publicNamespace
	template.Labels = map[string]string{controller.LabelTemplateScope: naming.ScopePublic}
	template.Annotations = nil
	pod.Namespace = publicNamespace
	pod.Labels[controller.LabelTemplateID] = requestedTemplateID
	delete(pod.Labels, controller.LabelOwnerKind)
	delete(pod.Annotations, controller.AnnotationTeamID)
	delete(pod.Annotations, controller.AnnotationOwnerKind)
	rs.Namespace = publicNamespace
	hash, err := controller.TemplateSpecHash(template)
	require.NoError(t, err)
	pod.Annotations[controller.AnnotationTemplateSpecHash] = hash
	return requestedTemplateID
}

type recordingSandboxStartRateLimiter struct {
	decision tokenbucket.Decision
	err      error
	calls    int
}

func (l *recordingSandboxStartRateLimiter) Take(
	context.Context,
	string,
	teamquota.Key,
	int64,
) (tokenbucket.Decision, error) {
	l.calls++
	return l.decision, l.err
}

func newTeamWarmPoolTransferFixture(t *testing.T) (*v1alpha1.SandboxTemplate, *corev1.Pod, *appsv1.ReplicaSet) {
	t.Helper()
	template := newSandboxResourceTestTemplate(t)
	template.Labels = map[string]string{controller.LabelTemplateScope: naming.ScopeTeam}
	template.Annotations = map[string]string{controller.AnnotationTemplateTeamID: "team-a"}
	template.Spec.Pool.MinIdle = 1

	replicas := int32(1)
	rs := &appsv1.ReplicaSet{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: template.Namespace,
			Name:      "warm-rs",
			UID:       types.UID("rs-uid"),
		},
		Spec: appsv1.ReplicaSetSpec{Replicas: &replicas},
	}
	pod := newSandboxResourceTestIdlePod(t, template, "idle-a")
	pod.UID = types.UID("pod-uid")
	pod.Annotations[controller.AnnotationTeamID] = "team-a"
	pod.Annotations[controller.AnnotationOwnerKind] = controller.OwnerKindTeamWarmPool
	pod.Labels[controller.LabelOwnerKind] = controller.OwnerKindTeamWarmPool
	pod.OwnerReferences = []metav1.OwnerReference{{
		APIVersion: "apps/v1",
		Kind:       "ReplicaSet",
		Name:       rs.Name,
		UID:        rs.UID,
		Controller: ptrTo(true),
	}}
	return template, pod, rs
}

type recordingCapacityStore struct {
	mu              sync.Mutex
	prepared        []teamquota.TransferRequest
	transferCommits int
	transferAborts  int
	commitErr       error
}

func (s *recordingCapacityStore) ReserveTarget(
	_ context.Context,
	request teamquota.ReserveRequest,
) (*teamquota.Reservation, error) {
	return testQuotaReservation(request.Owner, request.Operation, request.Target), nil
}

func (s *recordingCapacityStore) ReserveDelta(context.Context, teamquota.DeltaRequest) (*teamquota.Reservation, error) {
	panic("unexpected ReserveDelta")
}

func (s *recordingCapacityStore) AttachRuntime(context.Context, teamquota.OperationRef, teamquota.RuntimeRef) error {
	return nil
}

func (s *recordingCapacityStore) Commit(context.Context, teamquota.OperationRef) error {
	return nil
}

func (s *recordingCapacityStore) Abort(context.Context, teamquota.OperationRef, string) error {
	return nil
}

func (s *recordingCapacityStore) BeginRelease(context.Context, teamquota.ReleaseRequest) (*teamquota.Reservation, error) {
	return &teamquota.Reservation{}, nil
}

func (s *recordingCapacityStore) ConfirmRelease(context.Context, teamquota.OperationRef, teamquota.RuntimeRef) error {
	return nil
}

func (s *recordingCapacityStore) ReconcileTarget(context.Context, teamquota.Owner, teamquota.Values, teamquota.RuntimeRef) error {
	panic("unexpected ReconcileTarget")
}

func (s *recordingCapacityStore) PrepareTransfer(_ context.Context, request teamquota.TransferRequest) (*teamquota.Reservation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.prepared = append(s.prepared, request)
	return &teamquota.Reservation{
		Owner:     request.Destination,
		Operation: request.Operation,
		Target:    request.DestinationTarget.Clone(),
		Reserved:  teamquota.Values{teamquota.KeySandboxIdentityCount: 1},
	}, nil
}

func (s *recordingCapacityStore) CommitTransfer(context.Context, teamquota.OperationRef) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.transferCommits++
	return s.commitErr
}

func (s *recordingCapacityStore) CommitTransferObservedSource(
	ctx context.Context,
	ref teamquota.OperationRef,
	_ teamquota.Values,
) error {
	return s.CommitTransfer(ctx, ref)
}

func (s *recordingCapacityStore) AbortTransfer(context.Context, teamquota.OperationRef, string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.transferAborts++
	return nil
}

func (s *recordingCapacityStore) TransferTarget(context.Context, teamquota.TransferRequest) (*teamquota.Reservation, error) {
	panic("unexpected TransferTarget")
}

type operationReuseRejectingCapacityStore struct {
	permissiveTeamQuotaCapacityStore

	transferOperationID string
	reserveOperationID  string
}

func (s *operationReuseRejectingCapacityStore) PrepareTransfer(
	_ context.Context,
	request teamquota.TransferRequest,
) (*teamquota.Reservation, error) {
	s.transferOperationID = request.Operation.ID
	return &teamquota.Reservation{
		Owner:     request.Destination,
		Operation: request.Operation,
		Target:    request.DestinationTarget.Clone(),
	}, nil
}

func (s *operationReuseRejectingCapacityStore) AbortTransfer(
	_ context.Context,
	ref teamquota.OperationRef,
	_ string,
) error {
	if ref.ID != s.transferOperationID {
		return errors.New("unexpected transfer operation")
	}
	return nil
}

func (s *operationReuseRejectingCapacityStore) ReserveTarget(
	_ context.Context,
	request teamquota.ReserveRequest,
) (*teamquota.Reservation, error) {
	s.reserveOperationID = request.Operation.ID
	if request.Operation.ID == s.transferOperationID {
		return nil, &teamquota.OperationConflictError{
			Owner:       request.Owner,
			OperationID: request.Operation.ID,
		}
	}
	return testQuotaReservation(request.Owner, request.Operation, request.Target), nil
}

type hotClaimResizeCapacityStore struct {
	permissiveTeamQuotaCapacityStore

	mu                 sync.Mutex
	prepared           []teamquota.TransferRequest
	resizeReservations []teamquota.ReserveRequest
	pending            map[string]teamquota.Values
	committed          teamquota.Values
	transferCommits    int
	resizeCommits      int
}

type hotClaimResizeCapacityState struct {
	prepared           []teamquota.TransferRequest
	resizeReservations []teamquota.ReserveRequest
	committed          teamquota.Values
	transferCommits    int
	resizeCommits      int
}

func newHotClaimResizeCapacityStore() *hotClaimResizeCapacityStore {
	return &hotClaimResizeCapacityStore{
		pending: make(map[string]teamquota.Values),
	}
}

func (s *hotClaimResizeCapacityStore) PrepareTransfer(
	_ context.Context,
	request teamquota.TransferRequest,
) (*teamquota.Reservation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	request.SourceDecrease = request.SourceDecrease.Clone()
	request.DestinationTarget = request.DestinationTarget.Clone()
	s.prepared = append(s.prepared, request)
	return &teamquota.Reservation{
		Owner:     request.Destination,
		Operation: request.Operation,
		Target:    request.DestinationTarget.Clone(),
	}, nil
}

func (s *hotClaimResizeCapacityStore) CommitTransfer(
	_ context.Context,
	ref teamquota.OperationRef,
) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := range s.prepared {
		if s.prepared[i].Operation.ID == ref.ID {
			s.committed = s.prepared[i].DestinationTarget.Clone()
			s.transferCommits++
			return nil
		}
	}
	return errors.New("prepared transfer not found")
}

func (s *hotClaimResizeCapacityStore) CommitTransferObservedSource(
	ctx context.Context,
	ref teamquota.OperationRef,
	_ teamquota.Values,
) error {
	return s.CommitTransfer(ctx, ref)
}

func (s *hotClaimResizeCapacityStore) ReserveTarget(
	_ context.Context,
	request teamquota.ReserveRequest,
) (*teamquota.Reservation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	request.Target = request.Target.Clone()
	s.resizeReservations = append(s.resizeReservations, request)
	s.pending[request.Operation.ID] = request.Target.Clone()
	return &teamquota.Reservation{
		Owner:     request.Owner,
		Operation: request.Operation,
		State:     "reserved",
		Committed: s.committed.Clone(),
		Target:    request.Target.Clone(),
	}, nil
}

func (s *hotClaimResizeCapacityStore) Commit(
	_ context.Context,
	ref teamquota.OperationRef,
) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	target, ok := s.pending[ref.ID]
	if !ok {
		return errors.New("resize reservation not found")
	}
	s.committed = target.Clone()
	delete(s.pending, ref.ID)
	s.resizeCommits++
	return nil
}

func (s *hotClaimResizeCapacityStore) Abort(
	_ context.Context,
	ref teamquota.OperationRef,
	_ string,
) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.pending, ref.ID)
	return nil
}

func (s *hotClaimResizeCapacityStore) snapshot() hotClaimResizeCapacityState {
	s.mu.Lock()
	defer s.mu.Unlock()
	state := hotClaimResizeCapacityState{
		committed:       s.committed.Clone(),
		transferCommits: s.transferCommits,
		resizeCommits:   s.resizeCommits,
	}
	for _, request := range s.prepared {
		request.SourceDecrease = request.SourceDecrease.Clone()
		request.DestinationTarget = request.DestinationTarget.Clone()
		state.prepared = append(state.prepared, request)
	}
	for _, request := range s.resizeReservations {
		request.Target = request.Target.Clone()
		state.resizeReservations = append(state.resizeReservations, request)
	}
	return state
}

package nodepoollifecycle

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	"github.com/stretchr/testify/require"
)

type fakeStore struct {
	nodes                map[string]sandboxstore.RuntimeNodePoolNodeUsage
	observed             int
	firstObservedAt      time.Time
	actionState          string
	finished             map[string]string
	proofs               map[string]map[string]any
	admissionSlots       []int
	heartbeatReservation func(time.Duration) bool
	durableActions       []*sandboxstore.RuntimeNodeLifecycleAction
	acquired             map[string]*sandboxstore.RuntimeNodeLifecycleAction
	terminalized         map[string]int
}

func ptrTime(value time.Time) *time.Time {
	return &value
}

func cloneLifecycleAction(action *sandboxstore.RuntimeNodeLifecycleAction) *sandboxstore.RuntimeNodeLifecycleAction {
	if action == nil {
		return nil
	}
	copied := *action
	return &copied
}

func (s *fakeStore) ReserveRuntimeNodeLifecycleHeartbeat(_ context.Context, _, _ string, interval time.Duration) (bool, error) {
	if s.heartbeatReservation != nil {
		return s.heartbeatReservation(interval), nil
	}
	return true, nil
}

func (s *fakeStore) GetRuntimeNodePoolSnapshot(context.Context, string) (*sandboxstore.RuntimeNodePoolSnapshot, error) {
	result := &sandboxstore.RuntimeNodePoolSnapshot{}
	for _, node := range s.nodes {
		result.Nodes = append(result.Nodes, node)
	}
	return result, nil
}

func (s *fakeStore) GetRuntimeNodeDrainStatus(_ context.Context, _, instanceID string) (*sandboxstore.RuntimeNodeDrainStatus, error) {
	node, ok := s.nodes[instanceID]
	if !ok {
		return nil, sandboxstore.ErrRuntimeNodeNotFound
	}
	return &sandboxstore.RuntimeNodeDrainStatus{Instance: node}, nil
}

func (s *fakeStore) BeginRuntimeNodeDrain(_ context.Context, _, instanceID, _ string) error {
	node := s.nodes[instanceID]
	node.State = sandboxstore.RuntimeNodeInstanceDraining
	s.nodes[instanceID] = node
	return nil
}

func (s *fakeStore) RevokeRuntimeNode(_ context.Context, _, instanceID, _ string) error {
	delete(s.nodes, instanceID)
	return nil
}

func (s *fakeStore) MarkRuntimeNodeProviderReady(_ context.Context, _, instanceID string, slots int) error {
	s.admissionSlots = append(s.admissionSlots, slots)
	node := s.nodes[instanceID]
	node.ProviderReady = true
	s.nodes[instanceID] = node
	return nil
}

func (s *fakeStore) CompleteReadyRuntimeNodeScaleOutActions(context.Context, string) error {
	return nil
}

func (s *fakeStore) AbandonRuntimeNodeEnrollment(_ context.Context, _, instanceID string) error {
	delete(s.nodes, instanceID)
	return nil
}

func (s *fakeStore) ObserveRuntimeNodeLifecycleAction(_ context.Context, request *sandboxstore.ObserveRuntimeNodeLifecycleActionRequest) (*sandboxstore.RuntimeNodeLifecycleAction, error) {
	s.observed++
	if s.acquired == nil {
		s.acquired = make(map[string]*sandboxstore.RuntimeNodeLifecycleAction)
	}
	action := s.acquired[request.Token]
	if action == nil {
		action = &sandboxstore.RuntimeNodeLifecycleAction{
			Token: request.Token, ProviderInstanceIDs: request.ProviderInstanceIDs,
			FirstObservedAt: s.firstObservedAt, RecoveryDeadlineAt: request.RecoveryDeadline,
		}
		s.acquired[request.Token] = action
	}
	action.ProviderInstanceIDs = request.ProviderInstanceIDs
	action.RecoveryDeadlineAt = request.RecoveryDeadline
	action.State = s.actionState
	return cloneLifecycleAction(action), nil
}

func (s *fakeStore) ListRuntimeNodeLifecycleActions(context.Context, string) ([]*sandboxstore.RuntimeNodeLifecycleAction, error) {
	return s.durableActions, nil
}

func (s *fakeStore) AcquireRuntimeNodeLifecycleAction(_ context.Context, _, token, owner string, _ time.Duration) (*sandboxstore.RuntimeNodeLifecycleAction, error) {
	if s.acquired == nil {
		s.acquired = make(map[string]*sandboxstore.RuntimeNodeLifecycleAction)
	}
	for _, durable := range s.durableActions {
		if durable.Token != token {
			continue
		}
		action := cloneLifecycleAction(durable)
		action.RecoveryOwnerID = owner
		action.RecoveryEpoch++
		action.RecoveryLeaseExpiresAt = ptrTime(time.Now().Add(time.Minute))
		s.acquired[token] = action
		return cloneLifecycleAction(action), nil
	}
	return nil, nil
}

func (s *fakeStore) ObserveRuntimeNodeLifecycleProviderAction(_ context.Context, _, token, _ string, _ int64, present bool) (*sandboxstore.RuntimeNodeLifecycleAction, error) {
	action := s.acquired[token]
	if action == nil {
		return nil, errors.New("fake recovery action is not acquired")
	}
	previousActionAbsentSince := action.ProviderActionAbsentSince
	action.ProviderActionLastObservedAt = nil
	action.ProviderActionAbsentSince = nil
	if !present {
		if previousActionAbsentSince == nil {
			action.ProviderActionAbsentSince = ptrTime(time.Now())
		} else {
			action.ProviderActionAbsentSince = previousActionAbsentSince
		}
	}
	return cloneLifecycleAction(action), nil
}

func (s *fakeStore) ObserveRuntimeNodeLifecycleProviderInstances(_ context.Context, _, token, _ string, _ int64, absent bool) (*sandboxstore.RuntimeNodeLifecycleAction, error) {
	action := s.acquired[token]
	if action == nil || action.ProviderActionAbsentSince == nil {
		return nil, errors.New("fake provider action absence is not observed")
	}
	previousInstanceAbsentSince := action.ProviderInstanceAbsentSince
	action.ProviderInstanceAbsentSince = nil
	if absent {
		if previousInstanceAbsentSince == nil {
			action.ProviderInstanceAbsentSince = ptrTime(time.Now())
		} else {
			action.ProviderInstanceAbsentSince = previousInstanceAbsentSince
		}
	}
	return cloneLifecycleAction(action), nil
}

func (s *fakeStore) TerminalizeRuntimeSlotsForProviderAbsentInstance(_ context.Context, _, instanceID, _, _ string, _ int64, _ []byte) (int, error) {
	if s.terminalized == nil {
		s.terminalized = make(map[string]int)
	}
	s.terminalized[instanceID]++
	if node, ok := s.nodes[instanceID]; ok {
		node.NonterminalSlots = 0
		s.nodes[instanceID] = node
	}
	return 1, nil
}

func (s *fakeStore) BeginRuntimeNodeLifecycleActionCleanup(_ context.Context, _ string) error {
	if s.actionState != "abandoned" {
		s.actionState = "draining"
	}
	return nil
}

func (s *fakeStore) BeginRuntimeNodeLifecycleActionCleanupForOwner(_ context.Context, _, token, owner string, epoch int64) error {
	action := s.acquired[token]
	if action == nil || action.RecoveryOwnerID != owner || action.RecoveryEpoch != epoch {
		return errors.New("fake recovery lease is not current")
	}
	if action.State == "pending" {
		action.State = "draining"
	}
	return nil
}

func (s *fakeStore) CompleteRuntimeNodeLifecycleAction(_ context.Context, token, state string) error {
	return s.CompleteRuntimeNodeLifecycleActionWithProof(context.Background(), token, state,
		map[string]string{"source": "legacy_completion"})
}

func (s *fakeStore) CompleteRuntimeNodeLifecycleActionWithProof(_ context.Context, token, state string, proof any) error {
	if s.finished == nil {
		s.finished = make(map[string]string)
	}
	s.finished[token] = state
	if s.proofs == nil {
		s.proofs = make(map[string]map[string]any)
	}
	if object, ok := proof.(map[string]any); ok {
		s.proofs[token] = object
	}
	return nil
}

func (s *fakeStore) CompleteRuntimeNodeLifecycleActionRecovery(_ context.Context, _, token, owner string, epoch int64, state string, proof any) error {
	action := s.acquired[token]
	if action == nil || action.RecoveryOwnerID != owner || action.RecoveryEpoch != epoch {
		return errors.New("fake recovery lease is not current")
	}
	return s.CompleteRuntimeNodeLifecycleActionWithProof(context.Background(), token, state, proof)
}

type fakeCloud struct {
	actions                     []Action
	completed                   map[string]string
	heartbeats                  int
	protected                   map[string]bool
	deleted                     []string
	inService                   map[string]bool
	attached                    map[string]bool
	heartbeatTimeouts           []time.Duration
	protectionError             error
	heartbeatCountsAtProtection []int
	completionErrors            map[string]error
	routeDeletionError          error
}

func (c *fakeCloud) ElasticInstancesInService(_ context.Context, ids []string) (map[string]bool, error) {
	result := make(map[string]bool, len(ids))
	for _, id := range ids {
		result[id] = c.inService[id]
	}
	return result, nil
}

func (c *fakeCloud) ElasticInstancesAttached(_ context.Context, ids []string) (map[string]bool, error) {
	result := make(map[string]bool, len(ids))
	for _, id := range ids {
		result[id] = c.attached[id]
	}
	return result, nil
}

func (c *fakeCloud) ListPendingLifecycleActions(context.Context) ([]Action, error) {
	return c.actions, nil
}

func (c *fakeCloud) HeartbeatLifecycleAction(_ context.Context, _ Action, timeout time.Duration) error {
	c.heartbeats++
	c.heartbeatTimeouts = append(c.heartbeatTimeouts, timeout)
	return nil
}

func TestLifecyclePollingDoesNotExhaustProviderHeartbeatBudgetAcrossRestarts(t *testing.T) {
	for _, deadline := range []time.Duration{20 * time.Minute, 50 * time.Minute} {
		t.Run(deadline.String(), func(t *testing.T) {
			start := time.Date(2026, 9, 14, 0, 0, 0, 0, time.UTC)
			now, nextRenewal := start, start
			store := &fakeStore{firstObservedAt: start}
			store.heartbeatReservation = func(interval time.Duration) bool {
				if now.Before(nextRenewal) {
					return false
				}
				nextRenewal = now.Add(interval)
				return true
			}
			cloud := &fakeCloud{actions: []Action{{Token: "token", HookID: "out", InstanceIDs: []string{"i-1"}}}}
			var expiresAt time.Time
			for elapsed := time.Duration(0); elapsed <= deadline; elapsed += 10 * time.Second {
				now = start.Add(elapsed)
				// A new worker each pass cannot reset the durable renewal schedule.
				worker, err := New(store, cloud, &fakeNomad{store: store}, Config{
					PoolID: "elastic", ScaleOutHookID: "out", ScaleInHookID: "in", WarmSlotsPerNode: 502,
					ScaleOutEnrollmentTimeout: deadline, Now: func() time.Time { return now },
				})
				require.NoError(t, err)
				before := cloud.heartbeats
				_, err = worker.Reconcile(context.Background())
				require.NoError(t, err)
				if cloud.heartbeats != before {
					expiresAt = now.Add(cloud.heartbeatTimeouts[len(cloud.heartbeatTimeouts)-1])
				}
				require.True(t, now.Before(expiresAt), "provider wait expired at %s", elapsed)
			}
			require.LessOrEqual(t, cloud.heartbeats, sandboxstore.RuntimeNodeLifecycleHeartbeatMaxAttempts-3)
			require.Equal(t, LifecycleAbandon, cloud.completed["token"])
		})
	}
}

func TestLifecycleReadyNodeCompletesWhileHeartbeatIsNotDue(t *testing.T) {
	store := &fakeStore{nodes: map[string]sandboxstore.RuntimeNodePoolNodeUsage{
		"i-1": {ProviderInstanceID: "i-1", PoolKind: sandboxstore.RuntimeNodePoolKindElastic,
			State: sandboxstore.RuntimeNodeInstanceActive, CapacityLive: true, ReadySlots: 8},
	}, heartbeatReservation: func(time.Duration) bool { return false }}
	cloud := &fakeCloud{actions: []Action{{Token: "token", HookID: "out", InstanceIDs: []string{"i-1"}}}}
	worker, _ := testWorker(t, store, cloud)
	result, err := worker.Reconcile(context.Background())
	require.NoError(t, err)
	require.Equal(t, 1, result.Completed)
	require.Zero(t, cloud.heartbeats)
	require.Equal(t, LifecycleContinue, cloud.completed["token"])
}

func (c *fakeCloud) CompleteLifecycleAction(_ context.Context, action Action, result string) error {
	if err := c.completionErrors[action.Token]; err != nil {
		return err
	}
	if c.completed == nil {
		c.completed = make(map[string]string)
	}
	c.completed[action.Token] = result
	return nil
}

func (c *fakeCloud) SetInstancesProtection(_ context.Context, ids []string, protected bool) error {
	c.heartbeatCountsAtProtection = append(c.heartbeatCountsAtProtection, c.heartbeats)
	if c.protectionError != nil {
		return c.protectionError
	}
	if c.protected == nil {
		c.protected = make(map[string]bool)
	}
	for _, id := range ids {
		c.protected[id] = protected
	}
	return nil
}

func (c *fakeCloud) DeleteAllocationRoutes(_ context.Context, instanceID, _ string) error {
	c.deleted = append(c.deleted, instanceID)
	return c.routeDeletionError
}

type fakeNomad struct {
	store  *fakeStore
	fenced []string
	purged []string
}

func (n *fakeNomad) FenceAndStopWarmAllocations(_ context.Context, nodeID string) error {
	n.fenced = append(n.fenced, nodeID)
	for id, node := range n.store.nodes {
		if node.NodeID == nodeID {
			node.NonterminalSlots = 0
			n.store.nodes[id] = node
		}
	}
	return nil
}

func (*fakeNomad) NodeHasNonterminalAllocations(context.Context, string) (bool, error) {
	return false, nil
}

func (*fakeNomad) NodeHasNonWarmNonterminalAllocations(context.Context, string) (bool, error) {
	return false, nil
}

func (n *fakeNomad) PurgeNode(_ context.Context, nodeID string) error {
	n.purged = append(n.purged, nodeID)
	return nil
}

func testWorker(t *testing.T, store *fakeStore, cloud *fakeCloud) (*Worker, *fakeNomad) {
	t.Helper()
	nomad := &fakeNomad{store: store}
	worker, err := New(store, cloud, nomad, Config{
		PoolID: "elastic", ScaleOutHookID: "out", ScaleInHookID: "in",
		WarmSlotsPerNode: 8, Interval: time.Second, HeartbeatTimeout: 30 * time.Second,
	})
	require.NoError(t, err)
	return worker, nomad
}

func TestScaleOutCompletesOnlyAfterCapacityAndAllWarmSlots(t *testing.T) {
	store := &fakeStore{nodes: map[string]sandboxstore.RuntimeNodePoolNodeUsage{
		"i-1": {ProviderInstanceID: "i-1", PoolKind: sandboxstore.RuntimeNodePoolKindElastic,
			State: sandboxstore.RuntimeNodeInstanceActive, CapacityLive: true, ReadySlots: 7},
	}}
	cloud := &fakeCloud{actions: []Action{{Token: "token", HookID: "out", InstanceIDs: []string{"i-1"}}}}
	worker, _ := testWorker(t, store, cloud)

	result, err := worker.Reconcile(context.Background())
	require.NoError(t, err)
	require.Equal(t, 0, result.Completed)
	require.Empty(t, cloud.completed)

	node := store.nodes["i-1"]
	node.ReadySlots = 8
	store.nodes["i-1"] = node
	result, err = worker.Reconcile(context.Background())
	require.NoError(t, err)
	require.Equal(t, 1, result.Completed)
	require.Equal(t, LifecycleContinue, cloud.completed["token"])
	require.Equal(t, "completed", store.finished["token"])
	require.True(t, store.nodes["i-1"].ProviderReady)
}

func TestHighDensityScaleOutWaitsForConfiguredCarrierInventory(t *testing.T) {
	store := &fakeStore{nodes: map[string]sandboxstore.RuntimeNodePoolNodeUsage{
		"i-1": {ProviderInstanceID: "i-1", PoolKind: sandboxstore.RuntimeNodePoolKindElastic,
			State: sandboxstore.RuntimeNodeInstanceActive, CapacityLive: true},
	}}
	cloud := &fakeCloud{actions: []Action{{Token: "token", HookID: "out", InstanceIDs: []string{"i-1"}}}}
	config := Config{
		PoolID: "elastic", ScaleOutHookID: "out", ScaleInHookID: "in",
		WarmSlotsPerNode: 502, Interval: time.Second, HeartbeatTimeout: 30 * time.Second,
	}
	worker, err := New(store, cloud, &fakeNomad{store: store}, config)
	require.NoError(t, err)
	for _, ready := range []int{0, 8, 501} {
		node := store.nodes["i-1"]
		node.ReadySlots = ready
		store.nodes["i-1"] = node
		result, err := worker.Reconcile(context.Background())
		require.NoError(t, err)
		require.Zero(t, result.Completed)
		require.Empty(t, cloud.completed)
		require.Empty(t, store.admissionSlots)
	}
	node := store.nodes["i-1"]
	node.ReadySlots = 502
	store.nodes["i-1"] = node
	result, err := worker.Reconcile(context.Background())
	require.NoError(t, err)
	require.Equal(t, 1, result.Completed)
	require.Equal(t, LifecycleContinue, cloud.completed["token"])
	require.Equal(t, []int{502}, store.admissionSlots)
	for _, invalid := range []int{-1, 0} {
		config.WarmSlotsPerNode = invalid
		_, err := New(store, cloud, &fakeNomad{store: store}, config)
		require.Error(t, err)
	}
}

func TestScaleOutWaitsForEnrollmentBeforeTimeout(t *testing.T) {
	now := time.Date(2026, 8, 29, 12, 0, 0, 0, time.UTC)
	store := &fakeStore{
		nodes: map[string]sandboxstore.RuntimeNodePoolNodeUsage{
			"i-1": {ProviderInstanceID: "i-1", PoolKind: sandboxstore.RuntimeNodePoolKindElastic,
				State: sandboxstore.RuntimeNodeInstanceEnrolling, AllocationCIDR: "172.27.0.0/26"},
		},
		firstObservedAt: now.Add(-19 * time.Minute),
	}
	cloud := &fakeCloud{actions: []Action{{Token: "token", HookID: "out", InstanceIDs: []string{"i-1"}}}}
	worker, _ := testWorker(t, store, cloud)
	worker.config.Now = func() time.Time { return now }

	result, err := worker.Reconcile(context.Background())
	require.NoError(t, err)
	require.Zero(t, result.Completed)
	require.Contains(t, store.nodes, "i-1")
	require.Empty(t, cloud.deleted)
	require.Empty(t, cloud.completed)
}

func TestStaleEnrollingScaleOutReleasesReservationThenAbandons(t *testing.T) {
	now := time.Date(2026, 8, 29, 12, 0, 0, 0, time.UTC)
	store := &fakeStore{
		nodes: map[string]sandboxstore.RuntimeNodePoolNodeUsage{
			"i-1": {ProviderInstanceID: "i-1", PoolKind: sandboxstore.RuntimeNodePoolKindElastic,
				State: sandboxstore.RuntimeNodeInstanceEnrolling, AllocationCIDR: "172.27.0.0/26"},
		},
		firstObservedAt: now.Add(-20 * time.Minute),
	}
	cloud := &fakeCloud{actions: []Action{{Token: "token", HookID: "out", InstanceIDs: []string{"i-1"}}}}
	worker, _ := testWorker(t, store, cloud)
	worker.config.Now = func() time.Time { return now }

	result, err := worker.Reconcile(context.Background())
	require.NoError(t, err)
	require.Equal(t, 1, result.Completed)
	require.NotContains(t, store.nodes, "i-1")
	require.Equal(t, []string{"i-1"}, cloud.deleted)
	require.Equal(t, "abandoned", store.finished["token"])
	require.Equal(t, LifecycleAbandon, cloud.completed["token"])
}

func TestStaleActiveWarmingScaleOutDrainsThenAbandons(t *testing.T) {
	now := time.Date(2026, 8, 29, 12, 0, 0, 0, time.UTC)
	store := &fakeStore{
		nodes: map[string]sandboxstore.RuntimeNodePoolNodeUsage{
			"i-1": {ProviderInstanceID: "i-1", PoolKind: sandboxstore.RuntimeNodePoolKindElastic,
				State: sandboxstore.RuntimeNodeInstanceActive, NodeID: "node-1",
				AllocationCIDR: "172.27.0.0/26", NonterminalSlots: 8},
		},
		firstObservedAt: now.Add(-21 * time.Minute),
	}
	cloud := &fakeCloud{actions: []Action{{Token: "token", HookID: "out", InstanceIDs: []string{"i-1"}}}}
	worker, nomad := testWorker(t, store, cloud)
	worker.config.Now = func() time.Time { return now }

	result, err := worker.Reconcile(context.Background())
	require.NoError(t, err)
	require.Equal(t, 1, result.Completed)
	require.Equal(t, []string{"node-1"}, nomad.fenced)
	require.Equal(t, []string{"node-1"}, nomad.purged)
	require.Equal(t, []string{"i-1"}, cloud.deleted)
	require.NotContains(t, store.nodes, "i-1")
	require.Equal(t, LifecycleAbandon, cloud.completed["token"])
}

func TestDurableScaleOutRecoversAfterProviderActionAndInstanceDisappear(t *testing.T) {
	now := time.Date(2026, 9, 19, 0, 0, 0, 0, time.UTC)
	store := &fakeStore{
		nodes: map[string]sandboxstore.RuntimeNodePoolNodeUsage{
			"i-1": {
				ProviderInstanceID: "i-1", PoolKind: sandboxstore.RuntimeNodePoolKindElastic,
				State: sandboxstore.RuntimeNodeInstanceActive, NodeID: "node-1",
				AllocationCIDR: "172.28.0.0/23", NonterminalSlots: 368,
			},
		},
		durableActions: []*sandboxstore.RuntimeNodeLifecycleAction{{
			Token: "token", PoolID: "elastic", LifecycleHookID: "out", Transition: TransitionScaleOut,
			State: "pending", ProviderInstanceIDs: []string{"i-1"},
			FirstObservedAt: now.Add(-time.Hour), RecoveryDeadlineAt: now.Add(-time.Minute),
			ProviderActionAbsentSince:   ptrTime(now.Add(-time.Minute)),
			ProviderInstanceAbsentSince: ptrTime(now.Add(-time.Minute)),
		}},
	}
	cloud := &fakeCloud{}
	worker, nomad := testWorker(t, store, cloud)
	worker.config.Now = func() time.Time { return now }
	worker.config.ProviderAbsenceGrace = time.Second

	result, err := worker.Reconcile(context.Background())
	require.NoError(t, err)
	require.Equal(t, 1, result.Recovered)
	require.Equal(t, 1, store.terminalized["i-1"])
	require.NotContains(t, store.nodes, "i-1")
	require.Equal(t, []string{"node-1"}, nomad.purged)
	require.Empty(t, nomad.fenced, "a provider-absent node must not probe or fence its dead client")
	require.Equal(t, []string{"i-1"}, cloud.deleted)
	require.Equal(t, "abandoned", store.finished["token"])
	require.Empty(t, cloud.completed, "a vanished provider action cannot be completed again")
	require.Equal(t, "provider_action_and_instances_absent", store.proofs["token"]["source"])
}

func TestDurableRecoveryDoesNotRunWhileProviderActionIsPresent(t *testing.T) {
	now := time.Date(2026, 9, 19, 0, 0, 0, 0, time.UTC)
	store := &fakeStore{
		nodes: map[string]sandboxstore.RuntimeNodePoolNodeUsage{
			"i-1": {ProviderInstanceID: "i-1", State: sandboxstore.RuntimeNodeInstanceActive},
		},
		durableActions: []*sandboxstore.RuntimeNodeLifecycleAction{{
			Token: "token", LifecycleHookID: "out", Transition: TransitionScaleOut,
			State: "pending", ProviderInstanceIDs: []string{"i-1"},
			FirstObservedAt: now, RecoveryDeadlineAt: now.Add(-time.Minute),
			ProviderActionAbsentSince:   ptrTime(now.Add(-time.Minute)),
			ProviderInstanceAbsentSince: ptrTime(now.Add(-time.Minute)),
		}},
	}
	cloud := &fakeCloud{actions: []Action{{Token: "token", HookID: "out", InstanceIDs: []string{"i-1"}}}}
	worker, _ := testWorker(t, store, cloud)
	worker.config.Now = func() time.Time { return now }

	result, err := worker.Reconcile(context.Background())
	require.NoError(t, err)
	require.Zero(t, result.Recovered)
	require.Contains(t, store.nodes, "i-1")
	require.Empty(t, store.finished)
	require.Nil(t, store.acquired["token"].ProviderActionAbsentSince)
}

func TestDurableRecoveryWaitsUntilEveryProviderInstanceIsAbsent(t *testing.T) {
	now := time.Date(2026, 9, 19, 0, 0, 0, 0, time.UTC)
	store := &fakeStore{
		nodes: map[string]sandboxstore.RuntimeNodePoolNodeUsage{
			"i-1": {ProviderInstanceID: "i-1", State: sandboxstore.RuntimeNodeInstanceActive},
		},
		durableActions: []*sandboxstore.RuntimeNodeLifecycleAction{{
			Token: "token", LifecycleHookID: "out", Transition: TransitionScaleOut,
			State: "pending", ProviderInstanceIDs: []string{"i-1"},
			FirstObservedAt: now.Add(-time.Hour), RecoveryDeadlineAt: now.Add(-time.Minute),
			ProviderActionAbsentSince:   ptrTime(now.Add(-time.Minute)),
			ProviderInstanceAbsentSince: ptrTime(now.Add(-time.Minute)),
		}},
	}
	cloud := &fakeCloud{attached: map[string]bool{"i-1": true}}
	worker, _ := testWorker(t, store, cloud)
	worker.config.Now = func() time.Time { return now }
	worker.config.ProviderAbsenceGrace = time.Second

	result, err := worker.Reconcile(context.Background())
	require.NoError(t, err)
	require.Zero(t, result.Recovered)
	require.Contains(t, store.nodes, "i-1")
	require.Nil(t, store.acquired["token"].ProviderInstanceAbsentSince)
	require.Empty(t, store.finished)
}

func TestDurableRecoveryRefusesActiveLeaseCustody(t *testing.T) {
	now := time.Date(2026, 9, 19, 0, 0, 0, 0, time.UTC)
	store := &fakeStore{
		nodes: map[string]sandboxstore.RuntimeNodePoolNodeUsage{
			"i-1": {
				ProviderInstanceID: "i-1", State: sandboxstore.RuntimeNodeInstanceActive,
				NodeID: "node-1", ActiveLeases: 1,
			},
		},
		durableActions: []*sandboxstore.RuntimeNodeLifecycleAction{{
			Token: "token", LifecycleHookID: "out", Transition: TransitionScaleOut,
			State: "pending", ProviderInstanceIDs: []string{"i-1"},
			FirstObservedAt: now.Add(-time.Hour), RecoveryDeadlineAt: now.Add(-time.Minute),
			ProviderActionAbsentSince:   ptrTime(now.Add(-time.Minute)),
			ProviderInstanceAbsentSince: ptrTime(now.Add(-time.Minute)),
		}},
	}
	cloud := &fakeCloud{}
	worker, _ := testWorker(t, store, cloud)
	worker.config.Now = func() time.Time { return now }
	worker.config.ProviderAbsenceGrace = time.Second

	_, err := worker.Reconcile(context.Background())
	require.ErrorContains(t, err, "unexpectedly owns active leases")
	require.Contains(t, store.nodes, "i-1")
	require.Empty(t, store.finished)
	require.True(t, cloud.protected["i-1"], "lease custody must retain provider protection")
}

func TestDurableScaleInRecoversAfterProviderActionAndInstanceDisappear(t *testing.T) {
	now := time.Date(2026, 9, 19, 0, 0, 0, 0, time.UTC)
	store := &fakeStore{
		nodes: map[string]sandboxstore.RuntimeNodePoolNodeUsage{
			"i-1": {
				ProviderInstanceID: "i-1", State: sandboxstore.RuntimeNodeInstanceActive,
				NodeID: "node-1", AllocationCIDR: "172.28.0.0/23", NonterminalSlots: 102,
			},
		},
		durableActions: []*sandboxstore.RuntimeNodeLifecycleAction{{
			Token: "token", LifecycleHookID: "in", Transition: TransitionScaleIn,
			State: "pending", ProviderInstanceIDs: []string{"i-1"},
			FirstObservedAt: now.Add(-time.Hour), RecoveryDeadlineAt: now.Add(-time.Minute),
			ProviderActionAbsentSince:   ptrTime(now.Add(-time.Minute)),
			ProviderInstanceAbsentSince: ptrTime(now.Add(-time.Minute)),
		}},
	}
	cloud := &fakeCloud{}
	worker, nomad := testWorker(t, store, cloud)
	worker.config.Now = func() time.Time { return now }
	worker.config.ProviderAbsenceGrace = time.Second

	result, err := worker.Reconcile(context.Background())
	require.NoError(t, err)
	require.Equal(t, 1, result.Recovered)
	require.NotContains(t, store.nodes, "i-1")
	require.Equal(t, 1, store.terminalized["i-1"])
	require.Equal(t, []string{"node-1"}, nomad.purged)
	require.Empty(t, nomad.fenced)
	require.Equal(t, []string{"i-1"}, cloud.deleted)
	require.Equal(t, "completed", store.finished["token"])
	require.Empty(t, cloud.completed)
}

func TestStaleScaleOutRefusesNodeWithUnexpectedLease(t *testing.T) {
	now := time.Date(2026, 8, 29, 12, 0, 0, 0, time.UTC)
	store := &fakeStore{
		nodes: map[string]sandboxstore.RuntimeNodePoolNodeUsage{
			"i-1": {ProviderInstanceID: "i-1", PoolKind: sandboxstore.RuntimeNodePoolKindElastic,
				State: sandboxstore.RuntimeNodeInstanceActive, NodeID: "node-1", ActiveLeases: 1},
		},
		firstObservedAt: now.Add(-21 * time.Minute),
	}
	cloud := &fakeCloud{actions: []Action{{Token: "token", HookID: "out", InstanceIDs: []string{"i-1"}}}}
	worker, nomad := testWorker(t, store, cloud)
	worker.config.Now = func() time.Time { return now }

	_, err := worker.Reconcile(context.Background())
	require.ErrorContains(t, err, "unexpectedly owns active leases")
	require.True(t, cloud.protected["i-1"])
	require.Empty(t, cloud.completed)
	require.Empty(t, nomad.fenced)
}

func TestProviderInServiceRecoveryRemovesStuckWarmingFence(t *testing.T) {
	store := &fakeStore{nodes: map[string]sandboxstore.RuntimeNodePoolNodeUsage{
		"i-1": {ProviderInstanceID: "i-1", PoolKind: sandboxstore.RuntimeNodePoolKindElastic,
			State: sandboxstore.RuntimeNodeInstanceActive, CapacityLive: true, ReadySlots: 8},
	}}
	cloud := &fakeCloud{inService: map[string]bool{"i-1": true}}
	worker, _ := testWorker(t, store, cloud)
	_, err := worker.Reconcile(context.Background())
	require.NoError(t, err)
	require.True(t, store.nodes["i-1"].ProviderReady)
}

func TestWarmingNodeIsProtectedBeforeProviderAdmission(t *testing.T) {
	store := &fakeStore{nodes: map[string]sandboxstore.RuntimeNodePoolNodeUsage{
		"i-1": {ProviderInstanceID: "i-1", PoolKind: sandboxstore.RuntimeNodePoolKindElastic,
			State: sandboxstore.RuntimeNodeInstanceActive, CapacityLive: true, ReadySlots: 7},
	}}
	cloud := &fakeCloud{}
	worker, _ := testWorker(t, store, cloud)
	_, err := worker.Reconcile(context.Background())
	require.NoError(t, err)
	require.True(t, cloud.protected["i-1"])
}

func TestScaleOutCleanupStateCannotBeResurrectedByLateReadiness(t *testing.T) {
	store := &fakeStore{
		nodes: map[string]sandboxstore.RuntimeNodePoolNodeUsage{
			"i-1": {ProviderInstanceID: "i-1", PoolKind: sandboxstore.RuntimeNodePoolKindElastic,
				State: sandboxstore.RuntimeNodeInstanceActive, NodeID: "node-1",
				AllocationCIDR: "172.27.0.0/26", CapacityLive: true, ReadySlots: 8,
				NonterminalSlots: 8},
		},
		actionState: "draining",
	}
	cloud := &fakeCloud{actions: []Action{{Token: "token", HookID: "out", InstanceIDs: []string{"i-1"}}}}
	worker, _ := testWorker(t, store, cloud)

	result, err := worker.Reconcile(context.Background())
	require.NoError(t, err)
	require.Equal(t, 1, result.Completed)
	require.Equal(t, LifecycleAbandon, cloud.completed["token"])
	require.NotContains(t, store.nodes, "i-1")
}

func TestBusyScaleInRollsBackBeforeFencing(t *testing.T) {
	store := &fakeStore{nodes: map[string]sandboxstore.RuntimeNodePoolNodeUsage{
		"i-1": {ProviderInstanceID: "i-1", PoolKind: sandboxstore.RuntimeNodePoolKindElastic,
			State: sandboxstore.RuntimeNodeInstanceActive, ActiveLeases: 1, NodeID: "node-1"},
	}}
	cloud := &fakeCloud{actions: []Action{{Token: "token", HookID: "in", InstanceIDs: []string{"i-1"}}}}
	worker, nomad := testWorker(t, store, cloud)

	result, err := worker.Reconcile(context.Background())
	require.NoError(t, err)
	require.Equal(t, 1, result.RolledBack)
	require.Equal(t, LifecycleRollback, cloud.completed["token"])
	require.True(t, cloud.protected["i-1"])
	require.Empty(t, nomad.fenced)
	require.Equal(t, sandboxstore.RuntimeNodeInstanceActive, store.nodes["i-1"].State)
}

func TestIdleScaleInFencesDrainsRevokesThenContinues(t *testing.T) {
	store := &fakeStore{nodes: map[string]sandboxstore.RuntimeNodePoolNodeUsage{
		"i-1": {ProviderInstanceID: "i-1", PoolKind: sandboxstore.RuntimeNodePoolKindElastic,
			State: sandboxstore.RuntimeNodeInstanceActive, NodeID: "node-1", NodeUID: "uid-1",
			AllocationCIDR: "172.27.0.0/26", NonterminalSlots: 8},
	}}
	cloud := &fakeCloud{actions: []Action{{Token: "token", HookID: "in", InstanceIDs: []string{"i-1"}}}}
	worker, nomad := testWorker(t, store, cloud)

	result, err := worker.Reconcile(context.Background())
	require.NoError(t, err)
	require.Equal(t, 1, result.Completed)
	require.Equal(t, []string{"node-1"}, nomad.fenced)
	require.Equal(t, []string{"node-1"}, nomad.purged)
	require.Equal(t, []string{"i-1"}, cloud.deleted)
	require.Equal(t, LifecycleContinue, cloud.completed["token"])
	require.NotContains(t, store.nodes, "i-1")
}

func TestScaleInRetainsIdentityUntilRoutesAreAbsent(t *testing.T) {
	store := &fakeStore{nodes: map[string]sandboxstore.RuntimeNodePoolNodeUsage{
		"i-1": {ProviderInstanceID: "i-1", PoolKind: sandboxstore.RuntimeNodePoolKindElastic,
			State: sandboxstore.RuntimeNodeInstanceActive, NodeID: "node-1", NodeUID: "uid-1",
			AllocationCIDR: "172.27.0.0/26", NonterminalSlots: 8},
	}}
	cloud := &fakeCloud{actions: []Action{{Token: "token", HookID: "in", InstanceIDs: []string{"i-1"}}}, routeDeletionError: ErrAllocationRoutesPending}
	worker, nomad := testWorker(t, store, cloud)
	result, err := worker.Reconcile(t.Context())
	require.NoError(t, err)
	require.Zero(t, result.Completed)
	require.Empty(t, nomad.purged)
	require.Empty(t, cloud.completed)
	require.Equal(t, sandboxstore.RuntimeNodeInstanceDraining, store.nodes["i-1"].State)
	require.Equal(t, "172.27.0.0/26", store.nodes["i-1"].AllocationCIDR)

	cloud.routeDeletionError = nil
	result, err = worker.Reconcile(t.Context())
	require.NoError(t, err)
	require.Equal(t, 1, result.Completed)
	require.Equal(t, []string{"node-1"}, nomad.purged)
	require.Equal(t, LifecycleContinue, cloud.completed["token"])
	require.NotContains(t, store.nodes, "i-1")
}

func TestLifecycleProtectionFailureDoesNotStarvePendingHooks(t *testing.T) {
	store := &fakeStore{nodes: map[string]sandboxstore.RuntimeNodePoolNodeUsage{
		"i-1": {ProviderInstanceID: "i-1", PoolKind: sandboxstore.RuntimeNodePoolKindElastic, State: sandboxstore.RuntimeNodeInstanceActive, CapacityLive: true, ReadySlots: 8},
		"i-2": {ProviderInstanceID: "i-2", PoolKind: sandboxstore.RuntimeNodePoolKindElastic, State: sandboxstore.RuntimeNodeInstanceActive, CapacityLive: true, ReadySlots: 8},
	}}
	cloud := &fakeCloud{actions: []Action{{Token: "one", HookID: "out", InstanceIDs: []string{"i-1"}}, {Token: "two", HookID: "out", InstanceIDs: []string{"i-2"}}}, protectionError: errors.New("IncorrectScalingGroupStatus")}
	worker, _ := testWorker(t, store, cloud)
	result, err := worker.Reconcile(context.Background())
	require.ErrorContains(t, err, "IncorrectScalingGroupStatus")
	require.Equal(t, 2, cloud.heartbeats)
	require.Equal(t, []int{2}, cloud.heartbeatCountsAtProtection)
	require.Equal(t, 2, result.Completed)
	require.Equal(t, LifecycleContinue, cloud.completed["two"])
}

func TestLifecycleActionFailureDoesNotStarveAnotherHook(t *testing.T) {
	store := &fakeStore{nodes: map[string]sandboxstore.RuntimeNodePoolNodeUsage{
		"i-1": {ProviderInstanceID: "i-1", PoolKind: sandboxstore.RuntimeNodePoolKindElastic, State: sandboxstore.RuntimeNodeInstanceActive, CapacityLive: true, ReadySlots: 8},
		"i-2": {ProviderInstanceID: "i-2", PoolKind: sandboxstore.RuntimeNodePoolKindElastic, State: sandboxstore.RuntimeNodeInstanceActive, CapacityLive: true, ReadySlots: 8},
	}}
	cloud := &fakeCloud{actions: []Action{{Token: "one", HookID: "out", InstanceIDs: []string{"i-1"}}, {Token: "two", HookID: "out", InstanceIDs: []string{"i-2"}}}, completionErrors: map[string]error{"one": errors.New("provider unavailable")}}
	worker, _ := testWorker(t, store, cloud)
	result, err := worker.Reconcile(context.Background())
	require.ErrorContains(t, err, "provider unavailable")
	require.Equal(t, 2, cloud.heartbeats)
	require.Equal(t, 1, result.Completed)
	require.Equal(t, LifecycleContinue, cloud.completed["two"])
}

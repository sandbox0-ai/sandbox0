package nodepoollifecycle

import (
	"context"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	"github.com/stretchr/testify/require"
)

type fakeStore struct {
	nodes           map[string]sandboxstore.RuntimeNodePoolNodeUsage
	observed        int
	firstObservedAt time.Time
	actionState     string
	finished        map[string]string
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

func (s *fakeStore) MarkRuntimeNodeProviderReady(_ context.Context, _, instanceID string, _ int) error {
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
	return &sandboxstore.RuntimeNodeLifecycleAction{
		Token: request.Token, State: s.actionState, FirstObservedAt: s.firstObservedAt,
	}, nil
}

func (s *fakeStore) BeginRuntimeNodeLifecycleActionCleanup(_ context.Context, _ string) error {
	if s.actionState != "abandoned" {
		s.actionState = "draining"
	}
	return nil
}

func (s *fakeStore) CompleteRuntimeNodeLifecycleAction(_ context.Context, token, state string) error {
	if s.finished == nil {
		s.finished = make(map[string]string)
	}
	s.finished[token] = state
	return nil
}

type fakeCloud struct {
	actions    []Action
	completed  map[string]string
	heartbeats int
	protected  map[string]bool
	deleted    []string
	inService  map[string]bool
}

func (c *fakeCloud) ElasticInstancesInService(_ context.Context, ids []string) (map[string]bool, error) {
	result := make(map[string]bool, len(ids))
	for _, id := range ids {
		result[id] = c.inService[id]
	}
	return result, nil
}

func (c *fakeCloud) ListPendingLifecycleActions(context.Context) ([]Action, error) {
	return c.actions, nil
}

func (c *fakeCloud) HeartbeatLifecycleAction(context.Context, Action, time.Duration) error {
	c.heartbeats++
	return nil
}

func (c *fakeCloud) CompleteLifecycleAction(_ context.Context, action Action, result string) error {
	if c.completed == nil {
		c.completed = make(map[string]string)
	}
	c.completed[action.Token] = result
	return nil
}

func (c *fakeCloud) SetInstancesProtection(_ context.Context, ids []string, protected bool) error {
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
	return nil
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

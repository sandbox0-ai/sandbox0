package carrierpool

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	"github.com/sandbox0-ai/sandbox0/pkg/nomadinventory"
	"github.com/stretchr/testify/require"
)

type fakeStore struct {
	node                    sandboxstore.RuntimeCarrierNode
	busy                    []string
	ready                   []string
	begun, saved, completed int
}

func (s *fakeStore) HeartbeatRuntimeCarrierController(context.Context, string, time.Duration) error {
	return nil
}
func (s *fakeStore) RecordRuntimeCarrierSurplus(context.Context, string, int) error { return nil }

func (s *fakeStore) ListRuntimeCarrierNodes(context.Context, string) ([]sandboxstore.RuntimeCarrierNode, error) {
	return []sandboxstore.RuntimeCarrierNode{s.node}, nil
}
func (s *fakeStore) BeginRuntimeCarrierResize(_ context.Context, _ sandboxstore.RuntimeCarrierNode, _ int, g, _ []string) (int64, error) {
	s.begun++
	s.saved++
	s.node.Pending = true
	s.node.Revision++
	s.node.Groups = g
	return s.node.Revision, nil
}
func (s *fakeStore) RuntimeCarrierBusyAllocations(context.Context, sandboxstore.RuntimeCarrierNode) ([]string, error) {
	return s.busy, nil
}
func (s *fakeStore) RuntimeCarrierReadyAllocations(context.Context, sandboxstore.RuntimeCarrierNode) ([]string, error) {
	return s.ready, nil
}
func (s *fakeStore) CompleteRuntimeCarrierResize(context.Context, sandboxstore.RuntimeCarrierNode, []string) error {
	s.completed++
	s.node.Pending = false
	return nil
}

type fakeNomad struct {
	allocs     []nomadinventory.Allocation
	applyErr   error
	plans      [][]string
	catalogErr error
}

func (n *fakeNomad) CarrierCatalog(context.Context, string) ([]string, error) {
	return catalog(128), n.catalogErr
}
func (n *fakeNomad) CarrierAllocations(context.Context, string) ([]nomadinventory.Allocation, error) {
	return n.allocs, nil
}
func (n *fakeNomad) ApplyCarrierPlan(_ context.Context, _ string, _ int64, g []string) error {
	n.plans = append(n.plans, g)
	return n.applyErr
}
func testWorker(t *testing.T, s *fakeStore, n *fakeNomad) *Worker {
	t.Helper()
	w, err := New(s, n, Config{ClusterID: "test", Maximum: 128, LowWatermark: 8, Spare: 16, ShrinkAfter: 2 * time.Minute, Interval: time.Second})
	require.NoError(t, err)
	return w
}

func TestUncertainNomadResponseResumesExactDurablePlan(t *testing.T) {
	s := &fakeStore{node: sandboxstore.RuntimeCarrierNode{FreeCPU: 14000, FreeMemory: 56 << 30}}
	n := &fakeNomad{applyErr: errors.New("response lost")}
	w := testWorker(t, s, n)
	_, err := w.Reconcile(t.Context())
	require.Error(t, err)
	require.True(t, s.node.Pending)
	require.Zero(t, s.completed)
	n.applyErr = nil
	materializePlan(s, n)
	_, err = testWorker(t, s, n).Reconcile(t.Context())
	require.NoError(t, err)
	require.Equal(t, 1, s.begun)
	require.Equal(t, 1, s.saved)
	require.Equal(t, n.plans[0], n.plans[1])
	require.Equal(t, 1, s.completed)
}

func TestShrinkRemainsFencedUntilRemovedAllocationStops(t *testing.T) {
	s := &fakeStore{node: sandboxstore.RuntimeCarrierNode{FreeCPU: 14000, FreeMemory: 56 << 30}}
	n := &fakeNomad{allocs: []nomadinventory.Allocation{{ID: "idle", TaskGroup: "warm-100", ClientStatus: "running"}}}
	w := testWorker(t, s, n)
	_, err := w.Reconcile(t.Context())
	require.NoError(t, err)
	require.True(t, s.node.Pending)
	require.Zero(t, s.completed)
	n.allocs[0].ClientStatus = "complete"
	materializePlan(s, n)
	_, err = w.Reconcile(t.Context())
	require.NoError(t, err)
	require.Equal(t, 1, s.completed)
}

func materializePlan(s *fakeStore, n *fakeNomad) {
	for _, g := range s.node.Groups {
		id := "ready-" + g
		s.ready = append(s.ready, id)
		n.allocs = append(n.allocs, nomadinventory.Allocation{ID: id, TaskGroup: g, ClientStatus: "running"})
	}
}

func TestRefillWaitsForRegisteredReadyCapacity(t *testing.T) {
	s := &fakeStore{node: sandboxstore.RuntimeCarrierNode{FreeCPU: 14000, FreeMemory: 56 << 30}}
	n := &fakeNomad{}
	w := testWorker(t, s, n)
	_, err := w.Reconcile(t.Context())
	require.NoError(t, err)
	require.True(t, s.node.Pending)
	require.Zero(t, s.completed)
	materializePlan(s, n)
	ready := s.ready
	s.ready = nil
	_, err = w.Reconcile(t.Context())
	require.NoError(t, err)
	require.Zero(t, s.completed, "Nomad running alone is not registered capacity")
	s.ready = ready
	_, err = w.Reconcile(t.Context())
	require.NoError(t, err)
	require.Equal(t, 1, s.completed)
}

func TestRevokedNodeDoesNotWaitForReplacementCarriers(t *testing.T) {
	s := &fakeStore{node: sandboxstore.RuntimeCarrierNode{Retiring: true}}
	n := &fakeNomad{}
	_, err := testWorker(t, s, n).Reconcile(t.Context())
	require.NoError(t, err)
	require.Equal(t, 1, s.completed)
	require.Len(t, s.node.Groups, 8)
}

func TestRevocationSupersedesInterruptedRefillWithoutRecreatingGuests(t *testing.T) {
	s := &fakeStore{node: sandboxstore.RuntimeCarrierNode{Retiring: true, Pending: true, Revision: 7, Groups: catalog(16)}}
	n := &fakeNomad{}
	_, err := testWorker(t, s, n).Reconcile(t.Context())
	require.NoError(t, err)
	require.Equal(t, 1, s.begun)
	require.Equal(t, int64(8), s.node.Revision)
	require.Equal(t, 1, s.completed)
	require.Len(t, n.plans[0], 8)
}

func TestBusyMissingAllocationFailsClosed(t *testing.T) {
	s := &fakeStore{node: sandboxstore.RuntimeCarrierNode{FreeCPU: 14000, FreeMemory: 56 << 30}, busy: []string{"missing"}}
	n := &fakeNomad{}
	_, err := testWorker(t, s, n).Reconcile(t.Context())
	require.ErrorContains(t, err, "busy allocation missing")
	require.False(t, s.node.Pending)
	require.Empty(t, n.plans)
	require.Zero(t, s.completed)
}

func TestUnmigratedCatalogDoesNotFenceAdmission(t *testing.T) {
	s := &fakeStore{}
	n := &fakeNomad{catalogErr: errors.New("not adaptive")}
	_, err := testWorker(t, s, n).Reconcile(t.Context())
	require.Error(t, err)
	require.Zero(t, s.begun)
}

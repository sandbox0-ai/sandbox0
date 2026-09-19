package carrierpool

import (
	"context"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	"github.com/stretchr/testify/require"
)

func demandNode(id string) sandboxstore.RuntimeCarrierNode {
	return sandboxstore.RuntimeCarrierNode{NodeID: id, PhysicalCPU: 8000, PhysicalMemory: 8 << 30, FreeCPU: 8000, FreeMemory: 8 << 30, Ready: 16, Groups: catalog(16), MaxCarriers: 128,
		ReadyByCompatibility: map[string]int{"std": 14, "priv": 2}, CompatibilityCapacity: map[string]int{"std": 126, "priv": 2}}
}
func TestDemandDistributionConsumesResourcesOnceAcrossNodesAndClasses(t *testing.T) {
	nodes := []sandboxstore.RuntimeCarrierNode{demandNode("a"), demandNode("b")}
	shapes := []sandboxstore.RuntimeNodePoolDemandShape{{CompatibilityDigest: "std", CPUMillicores: 1000, MemoryBytes: 1 << 30, Slots: 12}, {CompatibilityDigest: "priv", CPUMillicores: 1000, MemoryBytes: 1 << 30, Slots: 4}}
	got := distributeDemand(nodes, shapes, Config{StandardDigest: "std", PrivilegedDigest: "priv", Maximum: 128})
	require.Equal(t, map[string]map[string]int{"a": {"standard": 8}, "b": {"standard": 4, "privileged": 2}}, got)
	nodes[0].PhysicalCPU = 2000
	nodes[0].FreeCPU = 64000
	shapes = []sandboxstore.RuntimeNodePoolDemandShape{{CompatibilityDigest: "std", CPUMillicores: 4000, MemoryBytes: 1 << 30, Slots: 10}}
	got = distributeDemand(nodes, shapes, Config{StandardDigest: "std", Maximum: 128})
	require.Equal(t, map[string]map[string]int{"b": {"standard": 2}}, got, "admission headroom cannot make an indivisible request fit physical capacity")
}
func TestDemandRefillsBeforeLowWatermarkAndPreservesBusyCarriers(t *testing.T) {
	groups, err := planDemand(catalog(128), []string{"warm-100"}, 16, 128, 64000, 64<<30, map[string]int{"standard": 60})
	require.NoError(t, err)
	require.Len(t, groups, 63)
	require.Contains(t, groups, "warm-100")
	groups, err = planDemand(catalog(128), []string{"warm-100"}, 16, 32, 64000, 64<<30, map[string]int{"standard": 60})
	require.NoError(t, err)
	require.Len(t, groups, 32)
	groups, err = planDemand(catalog(128), nil, 16, 128, 0, 64<<30, map[string]int{"standard": 60})
	require.NoError(t, err)
	require.Len(t, groups, 8)
}

type plannedStore struct {
	fakeStore
	shapes  []sandboxstore.RuntimeNodePoolDemandShape
	records []*sandboxstore.RuntimeNodePoolDemandRequest
}

func (s *plannedStore) ListRuntimeCarrierDemand(context.Context, string) ([]sandboxstore.RuntimeNodePoolDemandShape, error) {
	return s.shapes, nil
}
func (s *plannedStore) RecordRuntimeNodePoolDemand(_ context.Context, r *sandboxstore.RuntimeNodePoolDemandRequest) error {
	s.records = append(s.records, r)
	return nil
}
func TestPrewarmWindowExpiresAndUsesExistingDemandLedger(t *testing.T) {
	now := time.Now()
	s := &plannedStore{}
	c := Config{ClusterID: "cluster", PoolID: "pool", StandardDigest: "std", Maximum: 128, LowWatermark: 8, Spare: 16, ShrinkAfter: time.Minute, Interval: time.Second,
		PrewarmWindows: []PrewarmWindow{{Name: "daily-work", Start: now.Add(-time.Minute), End: now.Add(time.Minute), Slots: 100, CPUMillicores: 1000, MemoryBytes: 1 << 30, SecurityClass: "standard"}}}
	w, err := New(s, &fakeNomad{}, c)
	require.NoError(t, err)
	_, err = w.demand(t.Context(), nil)
	require.NoError(t, err)
	require.Len(t, s.records, 1)
	require.Equal(t, "carrier-prewarm/daily-work", s.records[0].OperationID)
	require.Equal(t, 100, s.records[0].Slots)
	require.Equal(t, 30*time.Second, s.records[0].TTL)
	w.config.PrewarmWindows[0].End = now.Add(-time.Second)
	_, err = w.demand(t.Context(), nil)
	require.NoError(t, err)
	require.Len(t, s.records, 1)
	c.PrewarmWindows[0].End = c.PrewarmWindows[0].Start
	_, err = New(s, &fakeNomad{}, c)
	require.Error(t, err)
}
func TestDemandTriggersRefillAboveLowWatermark(t *testing.T) {
	n := demandNode("a")
	n.FreeCPU = 64000
	n.FreeMemory = 64 << 30
	n.PhysicalMemory = 64 << 30
	n.Revision = 1
	s := &plannedStore{fakeStore: fakeStore{node: n}, shapes: []sandboxstore.RuntimeNodePoolDemandShape{{CompatibilityDigest: "std", CPUMillicores: 1000, MemoryBytes: 1 << 30, Slots: 60}}}
	nomad := &fakeNomad{}
	w, err := New(s, nomad, Config{ClusterID: "cluster", StandardDigest: "std", PrivilegedDigest: "priv", Maximum: 128, LowWatermark: 8, Spare: 16, ShrinkAfter: time.Minute, Interval: time.Second})
	require.NoError(t, err)
	changed, err := w.Reconcile(t.Context())
	require.NoError(t, err)
	require.Equal(t, 1, changed)
	require.Len(t, s.node.Groups, 62)
}

func TestRuntimeRolloutRefreshesStaleCapacityBeforeDemandRefill(t *testing.T) {
	n := demandNode("fixed")
	n.FreeCPU = 64000
	n.FreeMemory = 64 << 30
	n.PhysicalMemory = 64 << 30
	n.Revision = 7
	n.CompatibilityCapacity = map[string]int{"old-standard": 126, "old-privileged": 2}
	s := &plannedStore{fakeStore: fakeStore{node: n}}
	nomad := &fakeNomad{}
	materializePlan(&s.fakeStore, nomad)
	w, err := New(s, nomad, Config{ClusterID: "cluster", StandardDigest: "std", PrivilegedDigest: "priv", Maximum: 128, LowWatermark: 8, Spare: 16, ShrinkAfter: time.Minute, Interval: time.Second})
	require.NoError(t, err)
	changed, err := w.Reconcile(t.Context())
	require.NoError(t, err)
	require.Equal(t, 1, changed)
	require.Equal(t, map[string]int{"std": 126, "priv": 2}, s.node.CompatibilityCapacity)
	require.ElementsMatch(t, n.Groups, s.node.Groups, "refresh preserves existing membership even above the low watermark")
	require.False(t, s.node.Pending, "existing ready registrations complete the normal resize proof")
	s.shapes = []sandboxstore.RuntimeNodePoolDemandShape{{CompatibilityDigest: "std", CPUMillicores: 150, MemoryBytes: 128 << 20, Slots: 60}}
	changed, err = w.Reconcile(t.Context())
	require.NoError(t, err)
	require.Equal(t, 1, changed)
	require.Len(t, s.node.Groups, 62)
	require.True(t, s.node.Pending, "new groups still require authenticated readiness")
}

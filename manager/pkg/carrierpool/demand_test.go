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

func TestCronPrewarmCalendarAndExpiry(t *testing.T) {
	for _, tc := range []struct {
		name, expression, now, end string
		duration                   time.Duration
		active                     bool
	}{
		{"weekly start", "50 12 * * FRI", "2026-09-25T12:50:00Z", "2026-09-25T13:20:00Z", 30 * time.Minute, true},
		{"weekly before", "50 12 * * FRI", "2026-09-25T12:49:59Z", "", 30 * time.Minute, false},
		{"weekly end", "50 12 * * FRI", "2026-09-25T13:20:00Z", "", 30 * time.Minute, false},
		{"missed weeks", "50 12 * * FRI", "2026-10-23T13:00:00Z", "2026-10-23T13:20:00Z", 30 * time.Minute, true},
		{"hourly UTC despite input offset", "0 * * * *", "2026-09-25T21:05:00+08:00", "2026-09-25T13:10:00Z", 10 * time.Minute, true},
		{"daily across midnight", "55 23 * * *", "2026-09-26T00:05:00Z", "2026-09-26T00:15:00Z", 20 * time.Minute, true},
		{"month boundary", "0 0 1 * *", "2026-10-01T00:05:00Z", "2026-10-01T00:10:00Z", 10 * time.Minute, true},
		{"steps and overlap", "*/5 * * * *", "2026-09-25T12:07:00Z", "2026-09-25T12:10:00Z", 10 * time.Minute, true},
		{"leap day", "0 0 29 FEB *", "2028-02-29T00:05:00Z", "2028-02-29T00:10:00Z", 10 * time.Minute, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := PrewarmWindow{Cron: tc.expression, Duration: tc.duration}
			require.NoError(t, w.compile())
			now, err := time.Parse(time.RFC3339, tc.now)
			require.NoError(t, err)
			end, active := w.activeEnd(now)
			require.Equal(t, tc.active, active)
			if active {
				require.Equal(t, tc.end, end.UTC().Format(time.RFC3339))
			}
		})
	}
}

func TestCronPrewarmRejectsAmbiguousOrUnsupportedSchedules(t *testing.T) {
	for _, expression := range []string{"@every 1m", "@weekly", "0 0 0 * * *", "CRON_TZ=Asia/Shanghai 0 * * * *", "60 * * * *", "0 0 31 FEB *"} {
		w := PrewarmWindow{Cron: expression, Duration: time.Minute}
		require.Error(t, w.compile(), expression)
	}
	for _, w := range []PrewarmWindow{
		{Cron: "* * * * *"}, {Cron: "* * * * *", Duration: 25 * time.Hour},
		{Cron: "* * * * *", Duration: time.Minute, Start: time.Now()},
		{Start: time.Now(), End: time.Now().Add(time.Hour), Duration: time.Minute},
	} {
		require.Error(t, w.compile())
	}
}

func TestCronPrewarmRenewalUsesOneBoundedDemandAndStopsAtExpiry(t *testing.T) {
	s := &plannedStore{}
	w, err := New(s, &fakeNomad{}, Config{ClusterID: "cluster", PoolID: "pool", StandardDigest: "std", Maximum: 128, LowWatermark: 8, Spare: 16, ShrinkAfter: time.Minute, Interval: time.Second,
		PrewarmWindows: []PrewarmWindow{{Name: "batch", Cron: "0 * * * *", Duration: 10 * time.Minute, Slots: 100, CPUMillicores: 150, MemoryBytes: 128 << 20, SecurityClass: "standard"}}})
	require.NoError(t, err)
	now := time.Date(2026, 9, 25, 12, 9, 50, 0, time.UTC)
	_, err = w.demandAt(t.Context(), nil, now)
	require.NoError(t, err)
	require.Len(t, s.records, 1)
	require.Equal(t, 10*time.Second, s.records[0].TTL)
	_, err = w.demandAt(t.Context(), nil, now.Add(10*time.Second))
	require.NoError(t, err)
	require.Len(t, s.records, 1)
	_, err = w.demandAt(t.Context(), nil, now.Add(time.Hour))
	require.NoError(t, err)
	require.Len(t, s.records, 2)
	require.Equal(t, s.records[0].OperationID, s.records[1].OperationID)
	require.Equal(t, 100, s.records[1].Slots)
}

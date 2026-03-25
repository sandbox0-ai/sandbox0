package allocator

import (
	"testing"

	managerv1alpha1 "github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/pkg/template"
	"go.uber.org/zap"
)

func TestComputeAllocationsClampsAgainstSandboxNodesOnly(t *testing.T) {
	alloc := NewAllocator(10, zap.NewNop(), nil)
	tpl := newAllocatorTestTemplate(20, 20)
	clusters := []*template.Cluster{
		{ClusterID: "cluster-a", Enabled: true, Weight: 1},
	}
	summaries := map[string]*ClusterSummary{
		"cluster-a": {
			NodeCount:        5,
			TotalNodeCount:   5,
			SandboxNodeCount: 2,
			TotalPodCount:    7,
		},
	}

	got := alloc.ComputeAllocations(tpl, clusters, summaries)
	if len(got) != 1 {
		t.Fatalf("len(allocations) = %d, want 1", len(got))
	}

	if got[0].MinIdle != 13 || got[0].MaxIdle != 13 {
		t.Fatalf("allocation = (%d,%d), want (13,13)", got[0].MinIdle, got[0].MaxIdle)
	}
}

func TestComputeAllocationsAllowsZeroSandboxCapacity(t *testing.T) {
	alloc := NewAllocator(10, zap.NewNop(), nil)
	tpl := newAllocatorTestTemplate(5, 8)
	clusters := []*template.Cluster{
		{ClusterID: "cluster-a", Enabled: true, Weight: 1},
	}
	summaries := map[string]*ClusterSummary{
		"cluster-a": {
			NodeCount:        4,
			TotalNodeCount:   4,
			SandboxNodeCount: 0,
			TotalPodCount:    0,
		},
	}

	got := alloc.ComputeAllocations(tpl, clusters, summaries)
	if len(got) != 1 {
		t.Fatalf("len(allocations) = %d, want 1", len(got))
	}

	if got[0].MinIdle != 0 || got[0].MaxIdle != 0 {
		t.Fatalf("allocation = (%d,%d), want (0,0)", got[0].MinIdle, got[0].MaxIdle)
	}
}

func TestComputeAllocationsFallsBackToLegacyNodeCount(t *testing.T) {
	alloc := NewAllocator(10, zap.NewNop(), nil)
	tpl := newAllocatorTestTemplate(18, 18)
	clusters := []*template.Cluster{
		{ClusterID: "cluster-a", Enabled: true, Weight: 1},
	}
	summaries := map[string]*ClusterSummary{
		"cluster-a": {
			NodeCount:     2,
			TotalPodCount: 5,
		},
	}

	got := alloc.ComputeAllocations(tpl, clusters, summaries)
	if len(got) != 1 {
		t.Fatalf("len(allocations) = %d, want 1", len(got))
	}

	if got[0].MinIdle != 15 || got[0].MaxIdle != 15 {
		t.Fatalf("allocation = (%d,%d), want (15,15)", got[0].MinIdle, got[0].MaxIdle)
	}
}

func TestComputeAllocationsWithoutSummarySkipsCapacityClamp(t *testing.T) {
	alloc := NewAllocator(10, zap.NewNop(), nil)
	tpl := newAllocatorTestTemplate(12, 20)
	clusters := []*template.Cluster{
		{ClusterID: "cluster-a", Enabled: true, Weight: 1},
		{ClusterID: "cluster-b", Enabled: true, Weight: 3},
	}

	got := alloc.ComputeAllocations(tpl, clusters, nil)
	if len(got) != 2 {
		t.Fatalf("len(allocations) = %d, want 2", len(got))
	}

	if got[0].ClusterID != "cluster-a" || got[0].MinIdle != 3 || got[0].MaxIdle != 5 {
		t.Fatalf("cluster-a allocation = (%s,%d,%d), want (cluster-a,3,5)", got[0].ClusterID, got[0].MinIdle, got[0].MaxIdle)
	}
	if got[1].ClusterID != "cluster-b" || got[1].MinIdle != 9 || got[1].MaxIdle != 15 {
		t.Fatalf("cluster-b allocation = (%s,%d,%d), want (cluster-b,9,15)", got[1].ClusterID, got[1].MinIdle, got[1].MaxIdle)
	}
}

func newAllocatorTestTemplate(minIdle, maxIdle int32) *template.Template {
	return &template.Template{
		TemplateID: "tmpl-a",
		Spec: managerv1alpha1.SandboxTemplateSpec{
			Pool: managerv1alpha1.PoolStrategy{
				MinIdle: minIdle,
				MaxIdle: maxIdle,
			},
		},
	}
}

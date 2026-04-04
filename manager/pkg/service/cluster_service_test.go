package service

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/controller"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
)

func TestGetClusterSummaryCountsSandboxEligibleNodesAndPendingActivePods(t *testing.T) {
	configPath := writeClusterServiceManagerConfig(t, `
default_cluster_id: cluster-a
sandbox_pod_placement:
  node_selector:
    sandbox0.ai/node-role: sandbox
`)
	t.Setenv("CONFIG_PATH", configPath)

	svc := &ClusterService{
		podLister: newClusterServicePodLister(t,
			newClusterServicePod("ns-a", "idle-ready", "template-a", controller.PoolTypeIdle, corev1.PodRunning, true),
			newClusterServicePod("ns-a", "idle-not-ready", "template-a", controller.PoolTypeIdle, corev1.PodRunning, false),
			newClusterServicePod("ns-a", "idle-pending", "template-a", controller.PoolTypeIdle, corev1.PodPending, false),
			newClusterServicePod("ns-a", "active-running", "template-a", controller.PoolTypeActive, corev1.PodRunning, true),
			newClusterServicePod("ns-a", "active-pending-1", "template-a", controller.PoolTypeActive, corev1.PodPending, false),
			newClusterServicePod("ns-a", "active-pending-2", "template-a", controller.PoolTypeActive, corev1.PodPending, false),
			newClusterServicePod("ns-a", "active-failed", "template-a", controller.PoolTypeActive, corev1.PodFailed, false),
		),
		nodeLister: newClusterServiceNodeLister(t,
			newClusterServiceNode("node-sandbox-a", map[string]string{"sandbox0.ai/node-role": "sandbox"}),
			newClusterServiceNode("node-sandbox-b", map[string]string{"sandbox0.ai/node-role": "sandbox"}),
			newClusterServiceNode("node-system", map[string]string{"sandbox0.ai/node-role": "system"}),
		),
		logger: zap.NewNop(),
	}

	summary, err := svc.GetClusterSummary(context.Background())
	if err != nil {
		t.Fatalf("GetClusterSummary() error = %v", err)
	}

	if summary.ClusterID != "cluster-a" {
		t.Fatalf("ClusterID = %q, want %q", summary.ClusterID, "cluster-a")
	}
	if summary.NodeCount != 3 {
		t.Fatalf("NodeCount = %d, want 3", summary.NodeCount)
	}
	if summary.TotalNodeCount != 3 {
		t.Fatalf("TotalNodeCount = %d, want 3", summary.TotalNodeCount)
	}
	if summary.SandboxNodeCount != 2 {
		t.Fatalf("SandboxNodeCount = %d, want 2", summary.SandboxNodeCount)
	}
	if summary.IdlePodCount != 1 {
		t.Fatalf("IdlePodCount = %d, want 1", summary.IdlePodCount)
	}
	if summary.ActivePodCount != 3 {
		t.Fatalf("ActivePodCount = %d, want 3", summary.ActivePodCount)
	}
	if summary.PendingActivePodCount != 2 {
		t.Fatalf("PendingActivePodCount = %d, want 2", summary.PendingActivePodCount)
	}
	if summary.TotalPodCount != 4 {
		t.Fatalf("TotalPodCount = %d, want 4", summary.TotalPodCount)
	}
}

func TestGetClusterSummaryWithoutSandboxSelectorTreatsAllNodesAsEligible(t *testing.T) {
	configPath := writeClusterServiceManagerConfig(t, `
default_cluster_id: cluster-a
`)
	t.Setenv("CONFIG_PATH", configPath)

	svc := &ClusterService{
		podLister: newClusterServicePodLister(t),
		nodeLister: newClusterServiceNodeLister(t,
			newClusterServiceNode("node-a", map[string]string{"sandbox0.ai/node-role": "sandbox"}),
			newClusterServiceNode("node-b", map[string]string{"sandbox0.ai/node-role": "system"}),
		),
		logger: zap.NewNop(),
	}

	summary, err := svc.GetClusterSummary(context.Background())
	if err != nil {
		t.Fatalf("GetClusterSummary() error = %v", err)
	}

	if summary.SandboxNodeCount != 2 {
		t.Fatalf("SandboxNodeCount = %d, want 2", summary.SandboxNodeCount)
	}
}

func TestGetTemplateStatsCountsPendingActivePods(t *testing.T) {
	svc := &ClusterService{
		podLister: newClusterServicePodLister(t,
			newClusterServicePod("ns-a", "idle-ready", "template-a", controller.PoolTypeIdle, corev1.PodRunning, true),
			newClusterServicePod("ns-a", "idle-not-ready", "template-a", controller.PoolTypeIdle, corev1.PodRunning, false),
			newClusterServicePod("ns-a", "idle-pending", "template-a", controller.PoolTypeIdle, corev1.PodPending, false),
			newClusterServicePod("ns-a", "active-running", "template-a", controller.PoolTypeActive, corev1.PodRunning, true),
			newClusterServicePod("ns-a", "active-pending", "template-a", controller.PoolTypeActive, corev1.PodPending, false),
			newClusterServicePod("ns-a", "active-other-template", "template-b", controller.PoolTypeActive, corev1.PodPending, false),
			newClusterServicePod("ns-a", "active-failed", "template-a", controller.PoolTypeActive, corev1.PodFailed, false),
		),
		templateLister: staticTemplateLister{
			templates: []*v1alpha1.SandboxTemplate{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "template-a",
						Namespace: "ns-a",
					},
					Spec: v1alpha1.SandboxTemplateSpec{
						Pool: v1alpha1.PoolStrategy{
							MinIdle: 2,
							MaxIdle: 6,
						},
					},
				},
			},
		},
		logger: zap.NewNop(),
	}

	stats, err := svc.GetTemplateStats(context.Background())
	if err != nil {
		t.Fatalf("GetTemplateStats() error = %v", err)
	}
	if len(stats.Templates) != 1 {
		t.Fatalf("len(Templates) = %d, want 1", len(stats.Templates))
	}

	stat := stats.Templates[0]
	if stat.TemplateID != "template-a" {
		t.Fatalf("TemplateID = %q, want %q", stat.TemplateID, "template-a")
	}
	if stat.IdleCount != 1 {
		t.Fatalf("IdleCount = %d, want 1", stat.IdleCount)
	}
	if stat.ActiveCount != 2 {
		t.Fatalf("ActiveCount = %d, want 2", stat.ActiveCount)
	}
	if stat.PendingActiveCount != 1 {
		t.Fatalf("PendingActiveCount = %d, want 1", stat.PendingActiveCount)
	}
	if stat.MinIdle != 2 || stat.MaxIdle != 6 {
		t.Fatalf("pool = (%d,%d), want (2,6)", stat.MinIdle, stat.MaxIdle)
	}
}

type staticTemplateLister struct {
	templates []*v1alpha1.SandboxTemplate
}

func (l staticTemplateLister) List() ([]*v1alpha1.SandboxTemplate, error) {
	return l.templates, nil
}

func (l staticTemplateLister) Get(namespace, name string) (*v1alpha1.SandboxTemplate, error) {
	for _, template := range l.templates {
		if template.Namespace == namespace && template.Name == name {
			return template, nil
		}
	}
	return nil, nil
}

func writeClusterServiceManagerConfig(t *testing.T, contents string) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "manager.yaml")
	if err := os.WriteFile(path, []byte(contents), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}
	return path
}

func newClusterServicePodLister(t *testing.T, pods ...*corev1.Pod) corelisters.PodLister {
	t.Helper()
	indexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{
		cache.NamespaceIndex: cache.MetaNamespaceIndexFunc,
	})
	for _, pod := range pods {
		if pod == nil {
			continue
		}
		if err := indexer.Add(pod); err != nil {
			t.Fatalf("add pod: %v", err)
		}
	}
	return corelisters.NewPodLister(indexer)
}

func newClusterServiceNodeLister(t *testing.T, nodes ...*corev1.Node) corelisters.NodeLister {
	t.Helper()
	indexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})
	for _, node := range nodes {
		if node == nil {
			continue
		}
		if err := indexer.Add(node); err != nil {
			t.Fatalf("add node: %v", err)
		}
	}
	return corelisters.NewNodeLister(indexer)
}

func newClusterServicePod(namespace, name, templateID, poolType string, phase corev1.PodPhase, ready bool) *corev1.Pod {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      name,
			Labels: map[string]string{
				controller.LabelTemplateID: templateID,
				controller.LabelPoolType:   poolType,
			},
		},
		Status: corev1.PodStatus{Phase: phase},
	}
	if phase == corev1.PodRunning {
		status := corev1.ConditionFalse
		if ready {
			status = corev1.ConditionTrue
		}
		pod.Status.Conditions = []corev1.PodCondition{
			{
				Type:   corev1.PodReady,
				Status: status,
			},
		}
	}
	return pod
}

func newClusterServiceNode(name string, labels map[string]string) *corev1.Node {
	return &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:   name,
			Labels: labels,
		},
	}
}

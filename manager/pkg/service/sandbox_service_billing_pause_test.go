package service

import (
	"context"
	"sort"
	"testing"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/controller"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	"github.com/sandbox0-ai/sandbox0/pkg/ctldapi"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func (s *memorySandboxStore) ListActiveSandboxIDs(_ context.Context, teamID, clusterID, afterSandboxID string, limit int) ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ids := make([]string, 0)
	for sandboxID, record := range s.records {
		if record == nil || record.TeamID != teamID || record.ClusterID != clusterID ||
			record.DesiredState != sandboxstore.SandboxDesiredStateActive || sandboxID <= afterSandboxID {
			continue
		}
		ids = append(ids, sandboxID)
	}
	sort.Strings(ids)
	if limit > 0 && len(ids) > limit {
		ids = ids[:limit]
	}
	return ids, nil
}

func TestPauseActiveSandboxesForTeamOnlyQueuesLocalClusterRuntimes(t *testing.T) {
	pod := runtimeIdentityPod("ns-a", "pod-a", "sandbox-a")
	pod.Annotations[controller.AnnotationRuntimeGeneration] = "4"
	pod.Status.Phase = corev1.PodRunning
	pod.Status.PodIP = "10.0.0.4"
	markRuntimeIdentityPodReady(t, pod)
	store := &memorySandboxStore{records: map[string]*sandboxstore.SandboxRecord{
		"sandbox-a": {
			ID:                  "sandbox-a",
			TeamID:              "team-a",
			ClusterID:           "cluster-a",
			DesiredState:        sandboxstore.SandboxDesiredStateActive,
			CurrentPodName:      "pod-a",
			CurrentPodNamespace: "ns-a",
			RuntimeGeneration:   4,
		},
		"sandbox-b": {
			ID:           "sandbox-b",
			TeamID:       "team-a",
			ClusterID:    "cluster-b",
			DesiredState: sandboxstore.SandboxDesiredStateActive,
		},
	}}
	enqueuer := &recordingPauseEnqueuer{}
	service := &SandboxService{
		k8sClient:     fake.NewSimpleClientset(pod.DeepCopy()),
		podLister:     runtimeIdentityPodLister(t, pod),
		sandboxStore:  store,
		ctldClient:    ctldapi.NewClientWithTimeout(0),
		pauseEnqueuer: enqueuer,
		config: SandboxServiceConfig{
			ClusterID:   "cluster-a",
			CtldEnabled: true,
			ProcdPort:   49983,
		},
		clock:  systemTime{},
		logger: zap.NewNop(),
	}

	result, err := service.PauseActiveSandboxesForTeam(context.Background(), "team-a")
	if err != nil {
		t.Fatalf("PauseActiveSandboxesForTeam() error = %v", err)
	}
	if result.Requested != 1 {
		t.Fatalf("requested = %d, want 1", result.Requested)
	}
	if len(enqueuer.calls) != 1 || enqueuer.calls[0] != "sandbox-a" {
		t.Fatalf("pause queue calls = %#v, want sandbox-a", enqueuer.calls)
	}
	if txn := activeLifecycleTxnForTest(store, "sandbox-a"); txn == nil || txn.Kind != sandboxstore.SandboxLifecycleKindPause || txn.Source != sandboxstore.SandboxLifecycleSourceBilling {
		t.Fatalf("sandbox-a active transaction = %#v, want pause", txn)
	}
	if txn := activeLifecycleTxnForTest(store, "sandbox-b"); txn != nil {
		t.Fatalf("sandbox-b active transaction = %#v, want nil", txn)
	}
}

package service

import (
	"context"
	"testing"
	"time"

	"github.com/sandbox0-ai/infra/manager/pkg/controller"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
)

func newTestPodLister(t *testing.T, pods ...*corev1.Pod) corelisters.PodLister {
	t.Helper()
	indexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{
		cache.NamespaceIndex: cache.MetaNamespaceIndexFunc,
	})
	for _, p := range pods {
		if p == nil {
			continue
		}
		if err := indexer.Add(p); err != nil {
			t.Fatalf("add pod: %v", err)
		}
	}
	return corelisters.NewPodLister(indexer)
}

func TestListSandboxes(t *testing.T) {
	logger := zap.NewNop()
	now := time.Now()

	tests := []struct {
		name            string
		pods            []*corev1.Pod
		req             *ListSandboxesRequest
		expectedCount   int
		expectedIDs     []string
		expectedHasMore bool
	}{
		{
			name: "list all sandboxes for team",
			pods: []*corev1.Pod{
				createTestPod("sandbox-1", "team-a", "template-1", controller.PoolTypeActive, now.Add(-1*time.Hour), now.Add(1*time.Hour), false),
				createTestPod("sandbox-2", "team-a", "template-2", controller.PoolTypeActive, now.Add(-30*time.Minute), now.Add(2*time.Hour), false),
				createTestPod("sandbox-3", "team-b", "template-1", controller.PoolTypeActive, now.Add(-2*time.Hour), now.Add(1*time.Hour), false),
			},
			req: &ListSandboxesRequest{
				TeamID: "team-a",
				Limit:  50,
				Offset: 0,
			},
			expectedCount:   2,
			expectedIDs:     []string{"sandbox-2", "sandbox-1"}, // sorted by created_at desc
			expectedHasMore: false,
		},
		{
			name: "filter by status",
			pods: []*corev1.Pod{
				createTestPodWithPhase("sandbox-1", "team-a", "template-1", controller.PoolTypeActive, now, now, false, corev1.PodRunning),
				createTestPodWithPhase("sandbox-2", "team-a", "template-2", controller.PoolTypeActive, now, now, false, corev1.PodPending),
				createTestPodWithPhase("sandbox-3", "team-a", "template-3", controller.PoolTypeActive, now, now, false, corev1.PodFailed),
			},
			req: &ListSandboxesRequest{
				TeamID: "team-a",
				Status: SandboxStatusRunning,
				Limit:  50,
				Offset: 0,
			},
			expectedCount:   1,
			expectedIDs:     []string{"sandbox-1"},
			expectedHasMore: false,
		},
		{
			name: "filter by template_id",
			pods: []*corev1.Pod{
				createTestPod("sandbox-1", "team-a", "template-1", controller.PoolTypeActive, now, now, false),
				createTestPod("sandbox-2", "team-a", "template-2", controller.PoolTypeActive, now, now, false),
				createTestPod("sandbox-3", "team-a", "template-1", controller.PoolTypeActive, now, now, false),
			},
			req: &ListSandboxesRequest{
				TeamID:     "team-a",
				TemplateID: "template-1",
				Limit:      50,
				Offset:     0,
			},
			expectedCount:   2,
			expectedHasMore: false,
		},
		{
			name: "filter by paused state",
			pods: []*corev1.Pod{
				createTestPod("sandbox-1", "team-a", "template-1", controller.PoolTypeActive, now, now, false),
				createTestPod("sandbox-2", "team-a", "template-1", controller.PoolTypeActive, now, now, true),
				createTestPod("sandbox-3", "team-a", "template-1", controller.PoolTypeActive, now, now, false),
			},
			req: &ListSandboxesRequest{
				TeamID: "team-a",
				Paused: boolPtr(true),
				Limit:  50,
				Offset: 0,
			},
			expectedCount:   1,
			expectedIDs:     []string{"sandbox-2"},
			expectedHasMore: false,
		},
		{
			name: "pagination",
			pods: []*corev1.Pod{
				createTestPod("sandbox-1", "team-a", "template-1", controller.PoolTypeActive, now.Add(-3*time.Hour), now, false),
				createTestPod("sandbox-2", "team-a", "template-1", controller.PoolTypeActive, now.Add(-2*time.Hour), now, false),
				createTestPod("sandbox-3", "team-a", "template-1", controller.PoolTypeActive, now.Add(-1*time.Hour), now, false),
			},
			req: &ListSandboxesRequest{
				TeamID: "team-a",
				Limit:  2,
				Offset: 0,
			},
			expectedCount:   3,
			expectedIDs:     []string{"sandbox-3", "sandbox-2"},
			expectedHasMore: true,
		},
		{
			name: "pagination offset",
			pods: []*corev1.Pod{
				createTestPod("sandbox-1", "team-a", "template-1", controller.PoolTypeActive, now.Add(-3*time.Hour), now, false),
				createTestPod("sandbox-2", "team-a", "template-1", controller.PoolTypeActive, now.Add(-2*time.Hour), now, false),
				createTestPod("sandbox-3", "team-a", "template-1", controller.PoolTypeActive, now.Add(-1*time.Hour), now, false),
			},
			req: &ListSandboxesRequest{
				TeamID: "team-a",
				Limit:  2,
				Offset: 1,
			},
			expectedCount:   3,
			expectedIDs:     []string{"sandbox-2", "sandbox-1"},
			expectedHasMore: false,
		},
		{
			name: "exclude idle pool pods",
			pods: []*corev1.Pod{
				createTestPod("sandbox-1", "team-a", "template-1", controller.PoolTypeActive, now, now, false),
				createTestPod("sandbox-2", "team-a", "template-1", controller.PoolTypeIdle, now, now, false),
			},
			req: &ListSandboxesRequest{
				TeamID: "team-a",
				Limit:  50,
				Offset: 0,
			},
			expectedCount:   1,
			expectedIDs:     []string{"sandbox-1"},
			expectedHasMore: false,
		},
		{
			name: "empty result",
			pods: []*corev1.Pod{
				createTestPod("sandbox-1", "team-b", "template-1", controller.PoolTypeActive, now, now, false),
			},
			req: &ListSandboxesRequest{
				TeamID: "team-a",
				Limit:  50,
				Offset: 0,
			},
			expectedCount:   0,
			expectedIDs:     nil,
			expectedHasMore: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create fake client
			k8sClient := fake.NewSimpleClientset()
			for _, pod := range tt.pods {
				_, err := k8sClient.CoreV1().Pods(pod.Namespace).Create(context.Background(), pod, metav1.CreateOptions{})
				require.NoError(t, err)
			}

			// Create pod lister
			podLister := newTestPodLister(t, tt.pods...)

			// Create service
			svc := &SandboxService{
				k8sClient: k8sClient,
				podLister: podLister,
				clock:     systemTime{},
				logger:    logger,
			}

			// Execute
			resp, err := svc.ListSandboxes(context.Background(), tt.req)
			require.NoError(t, err)
			assert.Equal(t, tt.expectedCount, resp.Count)
			assert.Equal(t, tt.expectedHasMore, resp.HasMore)
			assert.Len(t, resp.Sandboxes, min(tt.expectedCount, tt.req.Limit))

			if tt.expectedIDs != nil {
				var actualIDs []string
				for _, s := range resp.Sandboxes {
					actualIDs = append(actualIDs, s.ID)
				}
				assert.Equal(t, tt.expectedIDs, actualIDs)
			}
		})
	}
}

func createTestPod(name, teamID, templateID, poolType string, createdAt, expiresAt time.Time, paused bool) *corev1.Pod {
	return createTestPodWithPhase(name, teamID, templateID, poolType, createdAt, expiresAt, paused, corev1.PodRunning)
}

func createTestPodWithPhase(name, teamID, templateID, poolType string, createdAt, expiresAt time.Time, paused bool, phase corev1.PodPhase) *corev1.Pod {
	pausedStr := ""
	if paused {
		pausedStr = "true"
	}
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:              name,
			Namespace:         "default",
			CreationTimestamp: metav1.NewTime(createdAt),
			Labels: map[string]string{
				controller.LabelTemplateID: templateID,
				controller.LabelPoolType:   poolType,
				controller.LabelSandboxID:  name,
			},
			Annotations: map[string]string{
				controller.AnnotationTeamID:    teamID,
				controller.AnnotationExpiresAt: expiresAt.Format(time.RFC3339),
				controller.AnnotationPaused:    pausedStr,
			},
		},
		Status: corev1.PodStatus{
			Phase: phase,
		},
	}
}

func boolPtr(b bool) *bool {
	return &b
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

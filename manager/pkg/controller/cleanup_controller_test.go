package controller

import (
	"context"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
)

type staticCleanupClock struct {
	now time.Time
}

func (c staticCleanupClock) Now() time.Time                  { return c.now }
func (c staticCleanupClock) Since(t time.Time) time.Duration { return c.now.Sub(t) }
func (c staticCleanupClock) Until(t time.Time) time.Duration { return t.Sub(c.now) }

type recordingPauseRequester struct {
	calls []string
}

func (r *recordingPauseRequester) RequestPauseSandboxByID(_ context.Context, sandboxID string) error {
	r.calls = append(r.calls, sandboxID)
	return nil
}

func TestCleanupExpiredRequestsPauseDesiredState(t *testing.T) {
	now := time.Date(2026, time.April, 15, 19, 31, 0, 0, time.UTC)
	template := &v1alpha1.SandboxTemplate{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "default",
			Namespace: "tpl-default",
		},
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "sandbox-1",
			Namespace: "tpl-default",
			Labels: map[string]string{
				LabelTemplateID: "default",
				LabelPoolType:   PoolTypeActive,
			},
			Annotations: map[string]string{
				AnnotationExpiresAt: now.Add(-time.Minute).Format(time.RFC3339),
			},
		},
	}
	indexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{
		cache.NamespaceIndex: cache.MetaNamespaceIndexFunc,
	})
	require.NoError(t, indexer.Add(pod))

	pauseRequester := &recordingPauseRequester{}
	recorder := record.NewFakeRecorder(1)
	controller := NewCleanupController(
		nil,
		corelisters.NewPodLister(indexer),
		nil,
		recorder,
		staticCleanupClock{now: now},
		pauseRequester,
		nil,
		zap.NewNop(),
		time.Minute,
	)

	require.NoError(t, controller.cleanupExpired(context.Background(), template))

	assert.Equal(t, []string{"sandbox-1"}, pauseRequester.calls)
	select {
	case event := <-recorder.Events:
		assert.Contains(t, event, "ExpiredPodPauseRequested")
	default:
		t.Fatal("expected pause-requested event")
	}
}

func TestCleanupExpiredSkipsDeletingPod(t *testing.T) {
	now := time.Date(2026, time.April, 15, 19, 31, 0, 0, time.UTC)
	template := &v1alpha1.SandboxTemplate{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "default",
			Namespace: "tpl-default",
		},
	}
	deletedAt := metav1.NewTime(now.Add(-time.Minute))
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "sandbox-1",
			Namespace:         "tpl-default",
			DeletionTimestamp: &deletedAt,
			Labels: map[string]string{
				LabelTemplateID: "default",
				LabelPoolType:   PoolTypeActive,
			},
			Annotations: map[string]string{
				AnnotationExpiresAt: now.Add(-time.Minute).Format(time.RFC3339),
			},
		},
	}
	indexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{
		cache.NamespaceIndex: cache.MetaNamespaceIndexFunc,
	})
	require.NoError(t, indexer.Add(pod))

	pauseRequester := &recordingPauseRequester{}
	recorder := record.NewFakeRecorder(1)
	controller := NewCleanupController(
		nil,
		corelisters.NewPodLister(indexer),
		nil,
		recorder,
		staticCleanupClock{now: now},
		pauseRequester,
		nil,
		zap.NewNop(),
		time.Minute,
	)

	require.NoError(t, controller.cleanupExpired(context.Background(), template))

	assert.Empty(t, pauseRequester.calls)
	select {
	case event := <-recorder.Events:
		t.Fatalf("unexpected event: %s", event)
	default:
	}
}

func TestCleanupExpiredForceDeletesStaleDeletingPod(t *testing.T) {
	now := time.Date(2026, time.April, 15, 19, 31, 0, 0, time.UTC)
	template := &v1alpha1.SandboxTemplate{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "default",
			Namespace: "tpl-default",
		},
	}
	deletedAt := metav1.NewTime(now.Add(-30 * time.Minute))
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "sandbox-1",
			Namespace:         "tpl-default",
			UID:               types.UID("pod-uid-1"),
			DeletionTimestamp: &deletedAt,
			Labels: map[string]string{
				LabelTemplateID: "default",
				LabelPoolType:   PoolTypeActive,
			},
		},
	}
	indexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{
		cache.NamespaceIndex: cache.MetaNamespaceIndexFunc,
	})
	require.NoError(t, indexer.Add(pod))

	client := fake.NewSimpleClientset(pod)
	recorder := record.NewFakeRecorder(1)
	controller := NewCleanupController(
		client,
		corelisters.NewPodLister(indexer),
		nil,
		recorder,
		staticCleanupClock{now: now},
		nil,
		nil,
		zap.NewNop(),
		time.Minute,
	)

	require.NoError(t, controller.cleanupExpired(context.Background(), template))

	_, err := client.CoreV1().Pods("tpl-default").Get(context.Background(), "sandbox-1", metav1.GetOptions{})
	require.True(t, apierrors.IsNotFound(err), "expected stale deleting pod to be force deleted, got %v", err)
	select {
	case event := <-recorder.Events:
		assert.Contains(t, event, "StaleDeletingPodForceDeleted")
	default:
		t.Fatal("expected stale force-delete event")
	}
}

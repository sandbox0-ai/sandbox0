package controller

import (
	"context"
	"testing"

	"github.com/sandbox0-ai/infra/manager/pkg/apis/sandbox0/v1alpha1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	corelisters "k8s.io/client-go/listers/core/v1"
	k8stesting "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
)

func TestBuildPodTemplateIncludesTemplateHash(t *testing.T) {
	pm := &PoolManager{}
	template := &v1alpha1.SandboxTemplate{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "template-a",
			Namespace: "default",
		},
	}

	got, err := pm.buildPodTemplate(template, true, "hash-v1")
	require.NoError(t, err)
	require.NotNil(t, got.Annotations)
	assert.Equal(t, "hash-v1", got.Annotations[AnnotationTemplateSpecHash])
	assert.Equal(t, PoolTypeIdle, got.Labels[LabelPoolType])
}

func TestDrainStaleIdlePodsUsesDeletePreconditions(t *testing.T) {
	template := &v1alpha1.SandboxTemplate{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "template-a",
			Namespace: "default",
		},
	}

	stalePod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "idle-stale",
			Namespace:       "default",
			UID:             types.UID("uid-stale"),
			ResourceVersion: "11",
			Labels: map[string]string{
				LabelTemplateID: "template-a",
				LabelPoolType:   PoolTypeIdle,
			},
			Annotations: map[string]string{
				AnnotationTemplateSpecHash: "old-hash",
			},
		},
	}
	freshPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "idle-fresh",
			Namespace:       "default",
			UID:             types.UID("uid-fresh"),
			ResourceVersion: "12",
			Labels: map[string]string{
				LabelTemplateID: "template-a",
				LabelPoolType:   PoolTypeIdle,
			},
			Annotations: map[string]string{
				AnnotationTemplateSpecHash: "new-hash",
			},
		},
	}

	client := fake.NewSimpleClientset([]runtime.Object{stalePod, freshPod}...)
	podIndexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})
	require.NoError(t, podIndexer.Add(stalePod))
	require.NoError(t, podIndexer.Add(freshPod))
	podLister := corelisters.NewPodLister(podIndexer)

	deleteActions := 0
	client.PrependReactor("delete", "pods", func(action k8stesting.Action) (handled bool, ret runtime.Object, err error) {
		delAction, ok := action.(k8stesting.DeleteAction)
		require.True(t, ok)
		deleteActions++
		opts := delAction.GetDeleteOptions()
		require.NotNil(t, opts.Preconditions)
		require.NotNil(t, opts.Preconditions.UID)
		require.NotNil(t, opts.Preconditions.ResourceVersion)
		assert.Equal(t, types.UID("uid-stale"), *opts.Preconditions.UID)
		assert.Equal(t, "11", *opts.Preconditions.ResourceVersion)
		return false, nil, nil
	})

	pm := &PoolManager{
		k8sClient: client,
		podLister: podLister,
		recorder:  record.NewFakeRecorder(10),
		logger:    zap.NewNop(),
	}

	err := pm.drainStaleIdlePods(context.Background(), template, "new-hash")
	require.NoError(t, err)
	assert.Equal(t, 1, deleteActions)
}

func TestReconcileReplicaSetTemplateUpdatesHash(t *testing.T) {
	template := &v1alpha1.SandboxTemplate{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "template-a",
			Namespace: "default",
		},
	}
	replicas := int32(1)
	rs := &appsv1.ReplicaSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "rs-template-a",
			Namespace: "default",
		},
		Spec: appsv1.ReplicaSetSpec{
			Replicas: &replicas,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						LabelTemplateID: "template-a",
						LabelPoolType:   PoolTypeIdle,
					},
					Annotations: map[string]string{
						AnnotationTemplateSpecHash: "old-hash",
					},
				},
				Spec: corev1.PodSpec{},
			},
		},
	}

	client := fake.NewSimpleClientset(rs)
	pm := &PoolManager{
		k8sClient: client,
		recorder:  record.NewFakeRecorder(10),
		logger:    zap.NewNop(),
	}

	updated, err := pm.reconcileReplicaSetTemplate(context.Background(), template, rs, "new-hash")
	require.NoError(t, err)
	require.NotNil(t, updated.Spec.Template.Annotations)
	assert.Equal(t, "new-hash", updated.Spec.Template.Annotations[AnnotationTemplateSpecHash])
}

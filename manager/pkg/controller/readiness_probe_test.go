package controller

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

func TestManagedReadinessEvaluatorUsesAnnotationConfig(t *testing.T) {
	runner := &fakeReadinessProbeRunner{}
	evaluator := &managedReadinessEvaluator{
		runner: runner,
		now:    time.Now,
		states: make(map[string]managedProbeState),
	}

	pod := managedReadinessTestPod(t, &corev1.Probe{
		ProbeHandler: corev1.ProbeHandler{
			Exec: &corev1.ExecAction{Command: []string{"test", "-f", "/tmp/ready"}},
		},
	})

	failure, err := evaluator.FirstFailure(context.Background(), pod)
	require.NoError(t, err)
	require.Nil(t, failure)
	require.Equal(t, 1, runner.calls)
}

func TestManagedReadinessEvaluatorCachesByPeriod(t *testing.T) {
	now := time.Unix(100, 0)
	runner := &fakeReadinessProbeRunner{}
	evaluator := &managedReadinessEvaluator{
		runner: runner,
		now:    func() time.Time { return now },
		states: make(map[string]managedProbeState),
	}

	pod := managedReadinessTestPod(t, &corev1.Probe{
		ProbeHandler: corev1.ProbeHandler{
			Exec: &corev1.ExecAction{Command: []string{"test", "-f", "/tmp/ready"}},
		},
		PeriodSeconds: 30,
	})

	failure, err := evaluator.FirstFailure(context.Background(), pod)
	require.NoError(t, err)
	require.Nil(t, failure)
	require.Equal(t, 1, runner.calls)

	failure, err = evaluator.FirstFailure(context.Background(), pod)
	require.NoError(t, err)
	require.Nil(t, failure)
	require.Equal(t, 1, runner.calls)
}

func TestManagedReadinessEvaluatorHonorsFailureThresholdWhenAlreadyReady(t *testing.T) {
	runner := &fakeReadinessProbeRunner{
		results: []error{nil, context.DeadlineExceeded, context.DeadlineExceeded},
	}
	evaluator := &managedReadinessEvaluator{
		runner: runner,
		now: func() time.Time {
			return time.Unix(int64(runner.calls+1), 0)
		},
		states: make(map[string]managedProbeState),
	}

	pod := managedReadinessTestPod(t, &corev1.Probe{
		ProbeHandler: corev1.ProbeHandler{
			Exec: &corev1.ExecAction{Command: []string{"test", "-f", "/tmp/ready"}},
		},
		FailureThreshold: 2,
		PeriodSeconds:    1,
	})

	failure, err := evaluator.FirstFailure(context.Background(), pod)
	require.NoError(t, err)
	require.Nil(t, failure)

	failure, err = evaluator.FirstFailure(context.Background(), pod)
	require.NoError(t, err)
	require.Nil(t, failure)

	failure, err = evaluator.FirstFailure(context.Background(), pod)
	require.NoError(t, err)
	require.NotNil(t, failure)
	require.Equal(t, "SidecarNotReady", failure.Reason)
}

type fakeReadinessProbeRunner struct {
	calls   int
	results []error
}

func (r *fakeReadinessProbeRunner) Run(context.Context, *corev1.Pod, string, *corev1.Probe) error {
	r.calls++
	if len(r.results) == 0 {
		return nil
	}
	idx := r.calls - 1
	if idx >= len(r.results) {
		idx = len(r.results) - 1
	}
	return r.results[idx]
}

func managedReadinessTestPod(t *testing.T, probe *corev1.Probe) *corev1.Pod {
	t.Helper()
	raw, err := json.Marshal([]v1alpha1.ManagedSidecarReadinessProbe{{
		Name:  "helper",
		Probe: probe,
	}})
	require.NoError(t, err)
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "sandbox-a",
			Namespace: "default",
			UID:       types.UID("pod-uid"),
			Annotations: map[string]string{
				v1alpha1.ManagedReadinessProbesAnnotation: string(raw),
			},
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{Name: "procd"},
				{Name: "helper"},
			},
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
		},
	}
}

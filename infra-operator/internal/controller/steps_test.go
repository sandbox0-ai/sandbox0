/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"context"
	"errors"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"

	infrav1alpha1 "github.com/sandbox0-ai/sandbox0/infra-operator/api/v1alpha1"
)

func TestRunStepsSetsConditions(t *testing.T) {
	ctx := context.Background()
	reconciler := &Sandbox0InfraReconciler{}
	infra := &infrav1alpha1.Sandbox0Infra{}

	steps := []reconcileStep{
		{
			Name:           "database",
			Run:            func(context.Context) error { return nil },
			ConditionType:  infrav1alpha1.ConditionTypeDatabaseReady,
			SuccessReason:  "DatabaseReady",
			SuccessMessage: "Database is ready",
			ErrorReason:    "DatabaseFailed",
		},
	}

	result, err := reconciler.runSteps(ctx, infra, steps)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if result.RequeueAfter != requeueInterval {
		t.Fatalf("expected requeue interval %v, got %v", requeueInterval, result.RequeueAfter)
	}

	condition := meta.FindStatusCondition(infra.Status.Conditions, infrav1alpha1.ConditionTypeDatabaseReady)
	if condition == nil {
		t.Fatalf("expected database condition to be set")
	} else {
		status := condition.Status
		if status != metav1.ConditionTrue {
			t.Fatalf("expected condition status true, got %v", status)
		}
	}
}

func TestRunStepsErrorStopsAndSetsCondition(t *testing.T) {
	ctx := context.Background()
	reconciler := &Sandbox0InfraReconciler{}
	infra := &infrav1alpha1.Sandbox0Infra{}
	stepErr := errors.New("boom")

	steps := []reconcileStep{
		{
			Name:          "internal-auth",
			Run:           func(context.Context) error { return stepErr },
			ConditionType: infrav1alpha1.ConditionTypeInternalAuthReady,
			ErrorReason:   "KeyGenerationFailed",
			ErrorResult:   &ctrl.Result{},
		},
		{
			Name:           "database",
			Run:            func(context.Context) error { return nil },
			ConditionType:  infrav1alpha1.ConditionTypeDatabaseReady,
			SuccessReason:  "DatabaseReady",
			SuccessMessage: "Database is ready",
			ErrorReason:    "DatabaseFailed",
		},
	}

	result, err := reconciler.runSteps(ctx, infra, steps)
	if !errors.Is(err, stepErr) {
		t.Fatalf("expected error %v, got %v", stepErr, err)
	}
	if result.RequeueAfter != 0 {
		t.Fatalf("expected empty requeue result, got %v", result.RequeueAfter)
	}

	condition := meta.FindStatusCondition(infra.Status.Conditions, infrav1alpha1.ConditionTypeInternalAuthReady)
	if condition == nil {
		t.Fatalf("expected internal auth condition to be set")
	} else {
		status := condition.Status
		if status != metav1.ConditionFalse {
			t.Fatalf("expected condition status false, got %v", status)
		}
	}
}

func TestIsReadyPod(t *testing.T) {
	readyPod := &corev1.Pod{
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			Conditions: []corev1.PodCondition{
				{
					Type:   corev1.PodReady,
					Status: corev1.ConditionTrue,
				},
			},
		},
	}
	if !isReadyPod(readyPod) {
		t.Fatalf("expected running ready pod to be reported as ready")
	}

	notReadyPod := &corev1.Pod{
		Status: corev1.PodStatus{
			Phase: corev1.PodPending,
			Conditions: []corev1.PodCondition{
				{
					Type:   corev1.PodReady,
					Status: corev1.ConditionFalse,
				},
			},
		},
	}
	if isReadyPod(notReadyPod) {
		t.Fatalf("expected pending pod to be reported as not ready")
	}
}
